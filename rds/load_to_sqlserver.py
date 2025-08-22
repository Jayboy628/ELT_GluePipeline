# rds/load_to_sqlserver.py
import os, json, re, math, warnings, hashlib
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

import boto3, pyodbc, pandas as pd

AWS_REGION    = os.getenv("AWS_REGION", "us-east-1")
SECRET_ID     = os.getenv("SECRET_ID", "connection_parameters_sqlserver-dev")
DATASET_DIR   = os.getenv("DATASET_DIR", "dataset")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "dbo")

# Optional: tell the loader which columns to use for the row-hash by table.
# If a table isn't listed here, the loader will hash ALL of its columns (sorted).
SURR_KEY_COLUMNS = {
    "order_items": ["order_id", "lineitem_id"],
    "order_item_options": ["order_id", "lineitem_id"],
    # "date_dim": ["date_key"]  # add if you like; otherwise full-row hash
}

# ---------- Secrets / Connection ----------
def get_secret():
    sm = boto3.client("secretsmanager", region_name=AWS_REGION)
    s = json.loads(sm.get_secret_value(SecretId=SECRET_ID)["SecretString"])
    if "database" not in s and "dbname" in s:
        s["database"] = s["dbname"]
    return s

def connect(secret: dict, database: str | None = None):
    db = database or secret.get("database") or "master"
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={secret['host']},{secret.get('port',1433)};"
        f"DATABASE={db};UID={secret['username']};PWD={secret['password']};"
    )
    return pyodbc.connect(conn_str, timeout=30)

def ensure_database(cur, dbname: str):
    cur.execute(f"IF DB_ID(N'{dbname}') IS NULL CREATE DATABASE [{dbname}];")
    cur.execute(f"USE [{dbname}];")

# ---------- Type inference ----------
_int_re   = re.compile(r"^[+-]?\d+$")
_dec_re   = re.compile(r"^[+-]?\d+\.\d+$")
_sci_re   = re.compile(r"^[+-]?\d+(\.\d+)?[eE][+-]?\d+$")
_bool_set = {"0","1","true","false","t","f","yes","no","y","n"}

def _clean_numeric_str(v: str) -> str:
    return v.replace(",", "").strip()

def infer_sql_type(series: pd.Series) -> str:
    vals = [str(x).strip() for x in series if str(x).strip() not in ("", "nan", "NaN", "NULL", "None")]
    if not vals:
        return "NVARCHAR(255)"

    # BIT?
    if all(v.lower() in _bool_set for v in vals):
        return "BIT"

    # DATETIME2? (suppress noisy warnings during inference)
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*Could not infer format.*")
            parsed = pd.to_datetime(vals, errors="coerce", utc=False)  # modern pandas
        if parsed.notna().mean() >= 0.90:
            return "DATETIME2"
    except Exception:
        pass

    # Numeric heuristics
    all_int = True
    all_num = True
    any_sci = False
    max_abs = 0
    max_scale = 0
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
            try:
                frac = v2.split(".", 1)[1]
                max_scale = max(max_scale, len(frac))
                dv = float(v2)
                if not math.isfinite(dv):
                    all_num = False
            except Exception:
                all_num = False
            continue
        all_num = False
        all_int = False
        break

    if all_num:
        if all_int:
            return "BIGINT" if max_abs > 2147483647 else "INT"
        if any_sci:
            return "FLOAT"
        return f"DECIMAL(18,{min(max_scale, 6)})"

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
    # Always ensure __pk exists in the DataFrame for DDL creation
    if "__pk" not in df.columns:
        df["__pk"] = ""  # placeholder; type below will be NVARCHAR(64)

    cols = []
    for c in df.columns:
        sqlt = "NVARCHAR(64)" if c == "__pk" else infer_sql_type(df[c])
        cols.append((c, sqlt))

    col_defs = [f"[{name}] {sqlt} NULL" for name, sqlt in cols]
    ddl = f"CREATE TABLE [{schema}].[{table}] (\n  " + ",\n  ".join(col_defs) + "\n);"

    cur.execute(f"""
    IF NOT EXISTS (
      SELECT 1 FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    ) {ddl}
    """)

def fetch_colinfo(cn, schema: str, table: str):
    cur = cn.cursor()
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, (schema, table))
    rows = cur.fetchall()
    info = [
        {
            "COLUMN_NAME": r[0],
            "DATA_TYPE": r[1],
            "NUMERIC_PRECISION": r[2],
            "NUMERIC_SCALE": r[3],
        }
        for r in rows
    ]
    # If table exists but __pk missing, add it and re-read
    if not any(r["COLUMN_NAME"].lower() == "__pk" for r in info):
        cur.execute(f"ALTER TABLE [{schema}].[{table}] ADD [__pk] NVARCHAR(64) NULL;")
        cn.commit()
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (schema, table))
        rows = cur.fetchall()
        info = [
            {
                "COLUMN_NAME": r[0],
                "DATA_TYPE": r[1],
                "NUMERIC_PRECISION": r[2],
                "NUMERIC_SCALE": r[3],
            }
            for r in rows
        ]
    return info

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
            return val
        return str(val)
    except Exception:
        return None

# ---------- Surrogate key ----------
def _norm(v):
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in ("", "null", "none", "nan"):
        return ""
    return s

def add_surrogate_key(df: pd.DataFrame, table: str) -> pd.DataFrame:
    # Choose columns to hash
    cols = SURR_KEY_COLUMNS.get(table, None)
    if cols is None:
        # hash ALL columns in deterministic order
        cols = sorted(df.columns.tolist())
    # avoid self-reference if __pk already present
    cols = [c for c in cols if c != "__pk" and c in df.columns]

    def row_digest(row) -> str:
        parts = [table]  # include table name for extra stability
        for c in cols:
            parts.append(f"{c}={_norm(row.get(c))}")
        s = "|".join(parts)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]  # 32-hex chars

    df = df.copy()
    df["__pk"] = df.apply(row_digest, axis=1)
    return df

def insert_dataframe(cn, schema: str, table: str, df: pd.DataFrame):
    colinfo = fetch_colinfo(cn, schema, table)
    if not colinfo:
        raise RuntimeError(f"Table {schema}.{table} has no columns?")

    col_names = [r["COLUMN_NAME"] for r in colinfo]
    type_map  = {}
    for r in colinfo:
        t = r["DATA_TYPE"]
        if r["NUMERIC_PRECISION"] is not None and r["NUMERIC_SCALE"] is not None:
            t = f"{t}({int(r['NUMERIC_PRECISION'])},{int(r['NUMERIC_SCALE'])})"
        type_map[r["COLUMN_NAME"]] = t

    # Make sure __pk exists in df (add if missing)
    if "__pk" not in df.columns:
        df["__pk"] = ""

    # Ensure df has all target columns; ignore extras
    for c in col_names:
        if c not in df.columns:
            df[c] = None

    rows = []
    for _, r in df[col_names].iterrows():
        rows.append([coerce_value(r[c], type_map[c]) for c in col_names])

    placeholders = ",".join(["?"] * len(col_names))
    col_list = ",".join(f"[{c}]" for c in col_names)
    sql = f"INSERT INTO [{schema}].[{table}] ({col_list}) VALUES ({placeholders})"

    cur = cn.cursor()
    cur.fast_executemany = True
    try:
        cur.executemany(sql, rows)
    except pyodbc.Error:
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
    table = csv_path.stem
    print(f"\nLoading {csv_path.name} -> [{schema}].[{table}]")

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    # Create the surrogate key column BEFORE DDL so the table includes it from day 1
    df = add_surrogate_key(df, table)

    cur = cn.cursor()
    ensure_schema(cur, schema)
    ensure_table(cur, schema, table, df)
    cur.close()

    insert_dataframe(cn, schema, table, df)
    print(f"Loaded {len(df)} rows into [{schema}].[{table}]")

if __name__ == "__main__":
    secret = get_secret()

    cn = connect(secret, database="master")
    cur = cn.cursor()
    target_db = secret.get("database") or "master"
    ensure_database(cur, target_db)
    cur.close(); cn.close()

    cn = connect(secret, database=target_db)
    print(f"RDS>>>>Connecting to SQL Server at:\n  Host: {secret['host']}\n  Port: {secret.get('port',1433)}\n  DB:   {target_db}")
    print("Connected to SQL Server.")

    data_dir = Path(DATASET_DIR)
    csvs = sorted(p for p in data_dir.glob("*.csv"))
    if not csvs:
        raise SystemExit(f"No CSV files found under {DATASET_DIR}/")

    for csv in csvs:
        load_csv_file(cn, csv, TARGET_SCHEMA)

    cn.close()
    print("\nAll CSVs loaded successfully.")
