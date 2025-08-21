

import os, json, pyodbc, boto3, pandas as pd, yaml
from decimal import Decimal, ROUND_HALF_UP

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SECRET_ID  = os.getenv("SECRET_ID", "connection_parameters_sqlserver-dev")

def get_secret():
    sm = boto3.client("secretsmanager", region_name=AWS_REGION)
    return json.loads(sm.get_secret_value(SecretId=SECRET_ID)["SecretString"])

def connect(secret):
    cs = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={secret['host']},{secret.get('port',1433)};"
        f"DATABASE={(secret.get('database') or secret.get('dbname','master'))};"
        f"UID={secret['username']};PWD={secret['password']};"
    )
    return pyodbc.connect(cs, timeout=30)

def ensure_database(cur, dbname):
    cur.execute(f"IF DB_ID(N'{dbname}') IS NULL CREATE DATABASE [{dbname}];")
    cur.execute(f"USE [{dbname}];")

def load_mapping(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def ensure_table(cur, schema, table, columns, primary_key=None):
    # Create schema if missing (dbo exists by default; safe for others)
    cur.execute(f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{schema}')
        EXEC('CREATE SCHEMA [{schema}]');
    """)
    # Build CREATE TABLE
    cols_sql = []
    for col in columns:
        name = col["name"]
        sqlt = col["type"]        # e.g., NVARCHAR(64), INT, DECIMAL(18,2), FLOAT, DATETIME2
        nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
        cols_sql.append(f"[{name}] {sqlt} {nullable}")
    pk_sql = ""
    if primary_key:
        pkcols = ", ".join(f"[{c}]" for c in primary_key)
        pk_sql = f", CONSTRAINT PK_{table} PRIMARY KEY ({pkcols})"
    ddl = f"CREATE TABLE [{schema}].[{table}] ({', '.join(cols_sql)}{pk_sql});"
    cur.execute(f"""
    IF NOT EXISTS (
      SELECT 1 FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    ) {ddl}
    """)

def coerce_value(val, sqltype, scale=None):
    if val is None: return None
    if isinstance(val, str):
        v = val.strip()
        if v == "" or v.lower() in ("nan", "null", "none"): return None
        val = v
    try:
        t = sqltype.lower()
        if t.startswith(("int","bigint","smallint","tinyint")):
            return int(float(val))
        if t.startswith(("decimal","numeric","money","smallmoney")):
            d = Decimal(str(val))
            if "(" in t and "," in t:
                try:
                    sc = int(t.split("(")[1].split(",")[1].split(")")[0])
                    q = Decimal(1).scaleb(-sc)
                    d = d.quantize(q, rounding=ROUND_HALF_UP)
                except Exception:
                    pass
            return d
        if t.startswith(("float","real")):
            return float(val)
        if t.startswith(("bit",)):
            s = str(val).lower()
            return 1 if s in ("1","true","t","yes","y") else 0
        # let nvarchar/varchar/datetime be passed as-is (or parse to datetime if you want)
        return val
    except Exception:
        return None

def load_csv_with_mapping(cn, csv_path, mapping_path):
    m = load_mapping(mapping_path)
    schema = m.get("schema","dbo")
    table  = m["table"]
    cols   = m["columns"]              # [{name,type,nullable?}, ...]
    pkey   = m.get("primary_key")

    cur = cn.cursor()
    # Make sure DB chosen in connect() is the target DB
    # If not, switch: cur.execute("USE [globalpartners];")
    ensure_table(cur, schema, table, cols, pkey)

    # read CSV as strings to control coercion
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    # order and coerce
    col_names = [c["name"] for c in cols]
    sql_types = {c["name"]: c["type"] for c in cols}
    for c in col_names:
        if c not in df.columns: df[c] = None
    rows = []
    for _, r in df[col_names].iterrows():
        rows.append([coerce_value(r[c], sql_types[c]) for c in col_names])

    placeholders = ",".join(["?"]*len(col_names))
    col_list = ",".join(f"[{c}]" for c in col_names)
    ins = f"INSERT INTO [{schema}].[{table}] ({col_list}) VALUES ({placeholders})"

    cur.fast_executemany = True
    try:
        cur.executemany(ins, rows)
    except pyodbc.Error:
        # fallback row-by-row to pinpoint bad data
        cur.fast_executemany = False
        for i, row in enumerate(rows, 1):
            try:
                cur.execute(ins, row)
            except pyodbc.Error as e:
                print(f"Failed at CSV row {i}: {row}")
                raise
    cn.commit()
    cur.close()
if __name__ == "__main__":
    secret = get_secret()

    # 1) Connect to master, ensure target DB, then reconnect to target
    cn = connect({**secret, "database": "master"})
    cur = cn.cursor()
    target_db = secret.get("database") or secret.get("dbname") or "master"
    ensure_database(cur, target_db)
    cur.close(); cn.close()

    cn = connect({**secret, "database": target_db})

    # 2) Load your CSVs (one example)
    #   Expect a mapping file alongside: mapping/order_items.yml
    load_csv_with_mapping(
        cn,
        csv_path="data/order_items.csv",
        mapping_path="mapping/order_items.yml"
    )

    cn.close()
