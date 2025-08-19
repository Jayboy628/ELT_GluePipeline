import sys, json, io, traceback, datetime as dt
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F, Window

# -----------------------
# Job Arguments
# -----------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "LOAD_PATH",          # s3://bucket/prefix/landing/
    "ERROR_PATH",         # s3://bucket/prefix/errors/
    "TABLES_INGEST",      # dbo.table1,dbo.table2
    "CONNECTION_NAME",    # custom-sqlserver-connection (Glue Connection)
    "BOOKMARK_PATH",      # s3://bucket/prefix/bookmarks/
    "WATERMARK_COLUMN",   # e.g. updated_at or modified_date
    "LOAD_MODE",          # incremental | full
    "PRIMARY_KEYS"        # optional; e.g. dbo.table1:id,order_id|dbo.table2:pk
])

LOAD_PATH         = args["LOAD_PATH"].rstrip("/") + "/"
ERROR_PATH        = args["ERROR_PATH"].rstrip("/") + "/"
CONNECTION_NAME   = args["CONNECTION_NAME"]
BOOKMARK_PATH     = args["BOOKMARK_PATH"].rstrip("/") + "/"
WATERMARK_COLUMN  = args["WATERMARK_COLUMN"]
LOAD_MODE         = args["LOAD_MODE"].lower()
TABLES_INGEST     = [t.strip() for t in args["TABLES_INGEST"].split(",") if t.strip()]

# Parse per-table primary keys: "dbo.t1:id|dbo.t2:pk1,pk2"
# STRICT: fail fast on any malformed PRIMARY_KEYS
def parse_primary_keys(s: str) -> dict[str, list[str]]:
    if s is None or not str(s).strip():
        raise ValueError(
            "PRIMARY_KEYS is required and cannot be empty. "
            "Expected format: 'dbo.t1:pk|dbo.t2:pk1,pk2'"
        )

    out: dict[str, list[str]] = {}
    for idx, raw in enumerate(s.split("|"), start=1):
        seg = raw.strip()
        if not seg:
            raise ValueError(f"Empty segment at position {idx}.")
        if ":" not in seg:
            raise ValueError(f"Missing ':' in segment {idx}: {seg!r}")

        t, cols = seg.split(":", 1)
        table = t.strip()
        if not table:
            raise ValueError(f"Empty table name in segment {idx}: {seg!r}")

        pk_list = [c.strip() for c in cols.split(",") if c.strip()]
        if not pk_list:
            raise ValueError(f"No columns provided for table {table!r} (segment {idx}).")

        if len(set(pk_list)) != len(pk_list):
            dupes = [c for c in pk_list if pk_list.count(c) > 1]
            raise ValueError(f"Duplicate PK columns for table {table!r}: {dupes}")

        if table in out:
            raise ValueError(f"Duplicate table entry {table!r} (segment {idx}).")

        out[table] = pk_list

    return out

# -----------------------
# Glue Setup
# -----------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------
# S3 Helpers
# -----------------------
def _split_s3_uri(uri: str):
    assert uri.startswith("s3://"), f"Invalid S3 URI: {uri}"
    bucket_key = uri[5:]
    b = bucket_key.split("/", 1)[0]
    k = "" if "/" not in bucket_key else bucket_key.split("/", 1)[1]
    return b, k

def s3_read_json(uri: str):
    import boto3, botocore
    b, k = _split_s3_uri(uri)
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=b, Key=k)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise

def s3_write_json(uri: str, payload: dict):
    import boto3
    b, k = _split_s3_uri(uri)
    boto3.client("s3").put_object(
        Bucket=b, Key=k,
        Body=json.dumps(payload, default=str).encode("utf-8"),
        ContentType="application/json"
    )

def log_error(table_name: str, err: Exception):
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    payload = {
        "table": table_name,
        "timestamp_utc": ts,
        "error": str(err),
        "traceback": traceback.format_exc()
    }
    key = f"{ERROR_PATH}ingest/{table_name.replace('.', '_')}/{ts}.json"
    s3_write_json(key, payload)

# -----------------------
# Bookmark (high-watermark) per table
# -----------------------
def get_bookmark(table_name: str):
    uri = f"{BOOKMARK_PATH}{table_name.replace('.', '_')}.json"
    doc = s3_read_json(uri)  # your helper already returns None on NoSuchKey
    if not doc or "last_value" not in doc:
        return None
    return doc["last_value"]

def put_bookmark(table_name: str, last_value):
    if hasattr(last_value, "isoformat"):
        last_value = last_value.isoformat()
    uri = f"{BOOKMARK_PATH}{table_name.replace('.', '_')}.json"
    s3_write_json(uri, {
        "last_value": last_value,
        "saved_at_utc": dt.datetime.utcnow().isoformat() + "Z"
    })

# -----------------------
# Build incremental SQL
# -----------------------
def build_query(table_name: str, wm_col: str, last_value):
    # Subquery in dbtable allows predicate pushdown via JDBC
    if LOAD_MODE == "full" or not last_value:
        return f"(SELECT * FROM {table_name}) AS src"
    # If wm is datetime, quote it; if numeric, remove quotes. We’ll be conservative and quote.
    return f"(SELECT * FROM {table_name} WHERE {wm_col} > '{last_value}') AS src"

# -----------------------
# Read from SQL Server
# -----------------------
def read_table_incremental(table_name: str):
    try:
        last = None if LOAD_MODE == "full" else get_bookmark(table_name)
        dbtable_or_query = build_query(table_name, WATERMARK_COLUMN, last)

        dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="sqlserver",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": dbtable_or_query,           # subquery pattern
                "connectionName": CONNECTION_NAME
            },
            transformation_ctx=f"read_{table_name.replace('dbo.', '')}"
        )
        return dyf
    except Exception as e:
        log_error(table_name, e)
        return None

# -----------------------
# Dedupe (optional, per table) and add ingest cols
# -----------------------
def prepare_df(df, table_name: str):
    now = F.current_timestamp()
    df = (df
          .withColumn("source_table", F.lit(table_name))
          .withColumn("ingest_ts_utc", now)
          .withColumn("ingest_date", F.to_date(now)))

    pks = _pk_map.get(table_name)
    if pks and WATERMARK_COLUMN in df.columns:
        w = Window.partitionBy(*pks).orderBy(F.col(WATERMARK_COLUMN).desc_nulls_last())
        df = (df
              .withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))
    return df

# -----------------------
# Write to S3 (partitioned)
# -----------------------
def write_parquet(df, table_name: str):
    output_path = f"{LOAD_PATH}{table_name.replace('dbo.', '')}/"
    # Coalesce small writes (tune as needed)
    df_out = df.coalesce(8)
    dyf_out = DynamicFrame.fromDF(df_out, glueContext, f"{table_name}_out")

    glueContext.write_dynamic_frame.from_options(
        frame=dyf_out,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": output_path,
            "partitionKeys": ["ingest_date", "source_table"]
        },
        format_options={"compression": "snappy"}
    )
    print(f"Wrote {table_name} to {output_path}")

# -----------------------
# Process loop
# -----------------------
overall_max_seen = {}  # table -> max watermark seen this run

for table in TABLES_INGEST:
    dyf = read_table_incremental(table)
    if not dyf:
        print(f"Skip {table}: read failed.")
        continue

    count = dyf.count()
    if count == 0:
        print(f"No new rows for {table}.")
        continue

    df = dyf.toDF()

    # Track new high-watermark
    if WATERMARK_COLUMN in df.columns:
        # Handle types: if timestamp/date, cast to string ISO so we can store consistently
        max_val_row = df.select(F.max(F.col(WATERMARK_COLUMN)).alias("max_wm")).collect()[0]
        overall_max_seen[table] = max_val_row["max_wm"]

    df = prepare_df(df, table)

    # Optional: basic schema evolution safety
    # df = df.select([F.col(c) for c in df.columns])  # placeholder for column ordering/rules

    # Write
    try:
        write_parquet(df, table)
    except Exception as e:
        log_error(table, e)
        continue

# After successful writes, persist bookmarks
for table, max_val in overall_max_seen.items():
    if max_val is not None:
        # stringify timestamp/date for bookmark storage
        if hasattr(max_val, "isoformat"):
            max_val = max_val.isoformat()
        put_bookmark(table, max_val)

job.commit()
