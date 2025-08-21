


import os, json, re, math
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

import boto3, pyodbc, pandas as pd

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SECRET_ID  = os.getenv("SECRET_ID", "connection_parameters_sqlserver-dev")
DATASET_DIR = os.getenv("DATASET_DIR", "dataset")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "dbo")

# ---------- Secrets / Connection ----------
def get_secret():
    sm = boto3.client("secretsmanager", region_name=AWS_REGION)
    s = json.loads(sm.get_secret_value(SecretId=SECRET_ID)["SecretString"])
    # support either "database" or "dbname"
    if "database" not in s and "dbname" in s:
        s["database"] = s["dbname"]
    return s

def connect(secret, database=None):
    db = database or secret.get("database") or "master"
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={secret['host']},{secret.get('port',1433)};"
        f"DATABASE={db};UID={secret['username']};PWD={secret['password']};"
    )
    return pyodbc.connect(conn_str, timeout=30)

def ensure_database(cur, dbname):
    cur.execute(f"IF DB_ID(N'{dbname}') IS NULL CREATE DATABASE [{dbname}];")
    cur.execute(f"USE [{dbname}];")

# ---------- Type inference ----------
_int_re    = re.compile(r"^[+-]?\d+$")
_dec_re    = re.compile(r"^[+-]?\d+\.\d+$")
_sci_re    = re.compile(r"^[+-]?\d+(\.\d+)?[eE][+-]?\d+$")
_bool_set  = {"0","1","true","false","t","f","yes","no","y","n"}

def _clean_numeric_str(v: str) -> str:
    # remove thousand separators like "1,234.56"
    return v.replace(",", "").strip()

def infer_sql_type(series: pd.Series) -> str:
    # treat empty/NaN as missing, infer from non-empty values
    vals = [str(x).strip() for x in series if str(x).strip() not in ("", "nan", "NaN", "NULL", "None")]
    if not vals:
        return "NVARCHAR(255)"  # default when column is entirely empty

    # BIT?
    if all(v.lower() in _bool_set for v in vals):
        return "BIT"

    # Date/time? 90%+ parsable → DATETIME2
    try:
        parsed = pd.to_datetime(vals, errors="coerce", utc=False, infer_datetime_format=True)
        ratio = parsed.notna().mean()
        if ratio >= 0.90:
            return "DATETIME2"
    except Exception:
        pass

    # Numeric checks
    all_int = True
    all_num = True
    max_abs = 0
    max_scale = 0
    any_sci = False
    for v in vals:
        v2 = _clean_numeric_str(v)
        if _int_re.match(v2):
            try:
                max_abs = max(max_abs, abs(int(v2)))
            except Exception:
                pass
            continue
        if _sci_re.match(v2):
            any_sci = True
            # scientific → prefer FLOAT
            all_int = False
            try:
                f = float(v2)
                if not math.isfinite(f):
                    all_num = False
            except Exception:
                all_num = False
            continue
        if _dec_re.match(v2):
            all_int = False
            # track scale
            try:
                frac = v2.split(".", 1)[1]
                max_scale = max(max_scale, len(frac))
                dv = float(v2)
                if not math.isfinite(dv):
                    all_num = False
            except Exception:
                all_num = False
            continue
        # not numeric
        all_num = False
        all_int = False
        break

    if all_num:
        if all_int:
            # choose INT vs BIGINT based on range
            return "BIGINT" if max_abs > 2147483647 else "INT"
        if any_sci:
            return "FLOAT"
        # DECIMAL(18, scale<=6) by default
        scale = min(max_scale, 6)
        return f"DECIMAL(18,{scale})"

    # Fallback to NVARCHAR based on max observed length (cap at 4000)
    max_len = max(len(x) for x in vals)
    n = min(max(max_len, 32), 4000)
    return f"NVARCHAR({n})" if n < 4000 else "NVARCHAR(MAX)"

# ---------- DDL / DML helpers ----------
def ensure_schema(cur, schema: str):
    cur.execute(f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{schema}')
        EXEC('CREATE SCHEMA [{schema}]');
    """)

def ensure_table(cur, schema: str, table: str, df: pd.DataFrame):
    # infer types for each column
    cols = []
    for c in df.columns:
        sqlt = infer_sql_type(df[c])
        cols.append((c, sqlt))

    col_defs = []
    for name, sqlt in cols:
        col_defs.append(f"[{name}] {sqlt} NULL")
    ddl = f"CREATE TABLE [{schema}].[{table}] (\n  " + ",\n  ".join(col_defs) + "\n);"

    cur.execute(f"""
    IF NOT EXISTS (
      SELECT 1 FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    ) {ddl}
    """)

def coerce_value(val, sqltype: str):
    if val is None:
        return None
    if isinstance(val, str):
        v = val.strip()
        if v == "" or v.lower() in ("nan","null","none"):
            return None
        val = v

    t = sqltype.lower()
    try:
        if t.startswith(("int","bigint","smallint","tinyint")):
            return int(float(_clean_numeric_str(str(val))))
        if t.startswith(("decimal","numeric","money","smallmoney")):
            s = _clean_numeric_str(str(val))
            d = Decimal(s)
            # quantize to declared scale if present
            if "(" in t and "," in t:
                try:
                    sc = int(t.split("(")[1].split(",")[1].split(")")[0])
                    q = Decimal(1).scaleb(-sc)
                    d = d.quantize(q, rounding=ROUND_HALF_UP)
                except Exception:
                    pass
            return d
        if t.startswith(("float","real")):
            return float(_clean_numeric_str(str(val)))
        if t.startswith("bit"):
            s = str(val).strip().lower()
            return 1 if s in ("1","true","t","yes","y") else 0
        if t.startswith(("datetime","date","time")):
            # let pyodbc try string; or convert via pandas to python datetime if desired
            return val
        # NVARCHAR/others
        return str(val)
    except Exception:
        return None

def insert_dataframe(cn, schema: str, table: str, df: pd.DataFrame):
    # use table's existing columns (in order) to build INSERT
    colinfo = pd.read_sql(
        """
        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        cn, params=[schema, table]
    )
    if colinfo.empty:
        raise RuntimeError(f"Table {schema}.{table} has no columns?")

    col_names = colinfo["COLUMN_NAME"].tolist()
    type_map  = {r["COLUMN_NAME"]: r["DATA_TYPE"] + (
        f"({int(r['NUMERIC_PRECISION'])},{int(r['NUMERIC_SCALE'])})"
        if pd.notna(r["NUMERIC_PRECISION"]) and pd.notna(r["NUMERIC_SCALE"])
        else ""
    ) for _, r in colinfo.iterrows()}

    # ensure df has all required cols; extra df cols are ignored
    for c in col_names:
        if c not in df.columns:
            df[c] = None

    rows = []
    for _, r in df[col_names].iterrows():
        rows.append([coerce_value(r[c], type_map[c]) for c in col_names])

    placeholders = ",".join(["?"]*len(col_names))
    col_list = ",".join(f"[{c}]" for c in col_names)
    sql = f"INSERT INTO [{schema}].[{table}] ({col_list}) VALUES ({placeholders})"

    cur = cn.cursor()
    cur.fast_executemany = True
    try:
        cur.executemany(sql, rows)
    except pyodbc.Error:
        # fallback to find offending row
        cur.fast_executemany = False
        for i, row in enumerate(rows, 1):
            try:
                cur.execute(sql, row)
            except pyodbc.Error as e:
                print(f"Insert failed at CSV row {i}: {row}")
                raise
    cn.commit()
    cur.close()

# ---------- Main pipeline ----------
def load_csv_file(cn, csv_path: Path, schema: str):
    table = csv_path.stem  # filename without .csv
    print(f"\nLoading {csv_path.name} -> [{schema}].[{table}]")

    # read everything as string so we fully control coercion
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    # create schema/table if needed
    cur = cn.cursor()
    ensure_schema(cur, schema)
    ensure_table(cur, schema, table, df)
    cur.close()

    # insert data
    insert_dataframe(cn, schema, table, df)
    print(f"Loaded {len(df)} rows into [{schema}].[{table}]")

if __name__ == "__main__":
    secret = get_secret()

    # 1) connect to master, ensure DB, reconnect to target DB
    cn = connect(secret, database="master")
    cur = cn.cursor()
    target_db = secret.get("database") or "master"
    ensure_database(cur, target_db)
    cur.close(); cn.close()

    cn = connect(secret, database=target_db)
    print(f"RDS>>>>Connecting to SQL Server at:\n  Host: {secret['host']}\n  Port: {secret.get('port',1433)}\n  DB:   {target_db}")
    print("Connected to SQL Server.")

    # 2) load all CSVs in dataset/
    data_dir = Path(DATASET_DIR)
    csvs = sorted(p for p in data_dir.glob("*.csv"))
    if not csvs:
        raise SystemExit(f"No CSV files found under {DATASET_DIR}/")

    for csv in csvs:
        load_csv_file(cn, csv, TARGET_SCHEMA)

    cn.close()
    print("\nAll CSVs loaded successfully.")
