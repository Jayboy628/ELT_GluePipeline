# rds/load_to_sqlserver.py
import os, json, re, math, warnings, hashlib
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

import boto3, pyodbc, pandas as pd

# ---------------- Globals ----------------
AWS_REGION    = os.getenv("AWS_REGION", "us-east-1")
SECRET_ID     = os.getenv("SECRET_ID", "connection_parameters_sqlserver-dev")
DATASET_DIR   = os.getenv("DATASET_DIR", "dataset")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "dbo")

# ---- Surrogate key config & helpers (table basename -> business key columns) ----
# Matched case/underscore-insensitively. If a table is absent or keys are empty, we hash the full row.
SURR_KEY_COLUMNS = {
    "order_items": ["order_id", "lineitem_id"],
    "order_item_options": ["order_id", "lineitem_id"],
    # "date_dim": ["date_key"],
}

def _norm(v):
    if v is None: return ""
    s = str(v).strip()
    return "" if s.lower() in ("", "null", "none", "nan") else s

def _canon(name: str) -> str:
    # canonical form: lowercase, remove non-alnum (so "line_item_id" == "LineItemID")
    return re.sub(r'[^a-z0-9]', '', name.lower())

def _resolve_key_columns(df_cols, desired_cols):
    canon_map = {_canon(c): c for c in df_cols}
    out = []
    for d in desired_cols:
        c = canon_map.get(_canon(d))
        if c:
            out.append(c)
    return out

def add_surrogate_key(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """
    Build __pk:
      1) hash resolved business-key columns when present,
      2) otherwise hash the entire row (all columns, sorted, excluding __pk).
    Deterministic & stable across loads.
    """
    df = df.copy()
    desired  = SURR_KEY_COLUMNS.get(table, [])
    key_cols = _resolve_key_columns(df.columns, desired) if desired else []
    print(f"[__pk] table={table} desired={desired} resolved={key_cols or '<<none>> (will use full-row hash as needed)'}")

    def row_digest(row) -> str:
        parts = [table]
        key_vals = [(c, _norm(row.get(c))) for c in key_cols]
        use_full_row = (not key_cols) or all(v == "" for _, v in key_vals)
        if use_full_row:
            cols = sorted([c for c in row.index if c != "__pk"])
            parts.extend(f"{c}={_norm(row.get(c))}" for c in cols)
        else:
            for c, v in key_vals:
                parts.append(f"{c}={v}")
        s = "||".join(parts)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()  # 64-hex
    df["__pk"] = df.apply(row_digest, axis=1)
    return df

# ---------------- Secrets / Connection ----------------
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

# ---------------- Type inference ----------------
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
    if all(v.lower() in _bool_set for v in vals):
        return "BIT"
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*Could not infer format.*")
            parsed = pd.to_datetime(vals, errors="coerce", utc=False)
        if parsed.notna().mean() >= 0.90:
            return "DATETIME2"
    except Exception:
        pass

    all_int = True; all_num = True; any_sci = False
    max_abs = 0; max_scale = 0
    for v in vals:
        v2 = _clean_numeric_str(v)
        if _int_re.match(v2):
            try: max_abs = max(max_abs, abs(int(v2)))
            except: pass
            continue
        if _sci_re.match(v2):
            any_sci = True; all_int = False
            try:
                f = float(v2)
                if not math.isfinite(f): all_num = False
            except: all_num = False
            continue
        if _dec_re.match(v2):
            all_int = False
            try:
                frac = v2.split(".", 1)[1]
                max_scale = max(max_scale, len(frac))
                dv = float(v2)
                if not math.isfinite(dv): all_num = False
            except: all_num = False
            continue
        all_num = False; all_int = False; break

    if all_num:
        if all_int:
            return "BIGINT" if max_abs > 2147483647 else "INT"
        if any_sci:
            return "FLOAT"
        return f"DECIMAL(18,{min(max_scale, 6)})"

    max_len = max(len(x) for x in vals)
    n = min(max(max_len, 32), 4000)
    return f"NVARCHAR({n})" if n < 4000 else "NVARCHAR(MAX)"

# ---------------- DDL / DML helpers ----------------
def ensure_schema(cur, schema: str):
    cur.execute(f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{schema}')
        EXEC('CREATE SCHEMA [{schema}]');
    """)

def ensure_table(cur, schema: str, table: str, df: pd.DataFrame):
    if "__pk" not in df.columns:
        df["__pk"] = ""
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
    info = [{"COLUMN_NAME": r[0], "DATA_TYPE": r[1], "NUMERIC_PRECISION": r[2], "NUMERIC_SCALE": r[3]} for r in rows]
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
        info = [{"COLUMN_NAME": r[0], "DATA_TYPE": r[1], "NUMERIC_PRECISION": r[2], "NUMERIC_SCALE": r[3]} for r in rows]
    return info

def coerce_value(val, sqltype: str):
    if val is None: return None
    if isinstance(val, str):
        v = val.strip()
        if v == "" or v.lower() in ("nan","null","none"): return None
        val = v
    t = sqltype.lower()
    try:
        if t.startswith(("int","bigint","smallint","tinyint")):
            return int(float(_clean_numeric_str(str(val))))
        if t.startswith(("decimal","numeric","money","smallmoney")):
            s = _clean_numeric_str(str(val)); d = Decimal(s)
            if "(" in t and "," in t:
                try:
                    sc = int(t.split("(")[1].split(",")[1].split(")")[0])
                    q = Decimal(1).scaleb(-sc); d = d.quantize(q, rounding=ROUND_HALF_UP)
                except: pass
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

    if "__pk" not in df.columns:
        df["__pk"] = ""

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
            except pyodbc.Error:
                print(f"Insert failed at CSV row {i}: {row}")
                raise
    cn.commit()
    cur.close()

# ---------------- Main ----------------
def load_csv_file(cn, csv_path: Path, schema: str):
    table = csv_path.stem
    print(f"\nLoading {csv_path.name} -> [{schema}].[{table}]")
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    # normalize headers: lowercase, trim, spaces→underscore
    df.rename(columns=lambda c: re.sub(r'\s+', '_', str(c).strip().lower()), inplace=True)
    # add surrogate key BEFORE DDL so the table includes it
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
