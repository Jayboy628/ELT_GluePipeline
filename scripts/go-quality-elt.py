# go-quality-elt.py — PRODUCTION REFACTOR

import sys
import boto3
import yaml
from datetime import datetime
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import col, lower, trim, lit, when

# --------------------
# Job Setup
# --------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "TRANSFORM_PATH",
    "THRESHOLDS_DICT_PATH",
    "FINAL_PATH",
    "PROCESS_PATH",
    "QA_QUANTITY_PATH",
    "QA_PRICE_PATH",
    "QA_PATH",
    "ERROR_PATH",
])

TRANSFORM_PATH       = args["TRANSFORM_PATH"]
THRESHOLDS_DICT_PATH = args["THRESHOLDS_DICT_PATH"]
FINAL_PATH           = args["FINAL_PATH"]
QA_PATH              = args["QA_PATH"]
ERROR_PATH           = args["ERROR_PATH"]
PROCESS_PATH         = args["PROCESS_PATH"]
QA_QUANTITY_PATH     = args["QA_QUANTITY_PATH"]
QA_PRICE_PATH        = args["QA_PRICE_PATH"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Reasonable Spark defaults for ETL
spark.conf.set("spark.sql.shuffle.partitions", "160")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# --------------------
# Utilities
# --------------------
def s3_join(prefix: str, suffix: str) -> str:
    return f"{prefix.rstrip('/')}/{suffix.lstrip('/')}"

def move_s3_objects(source_path: str, destination_path: str):
    s3 = boto3.client("s3")
    src = urlparse(source_path)
    dst = urlparse(destination_path)
    src_bucket, src_prefix = src.netloc, src.path.lstrip("/")
    dst_bucket, dst_prefix = dst.netloc, dst.path.lstrip("/")
    objs = s3.list_objects_v2(Bucket=src_bucket, Prefix=src_prefix).get("Contents", [])
    for o in objs or []:
        key = o["Key"]
        if key.endswith("/"):
            continue
        new_key = f"{dst_prefix.rstrip('/')}/{key.split('/')[-1]}"
        print(f"Moving: s3://{src_bucket}/{key} → s3://{dst_bucket}/{new_key}")
        s3.copy_object(Bucket=dst_bucket, CopySource={"Bucket": src_bucket, "Key": key}, Key=new_key)
        s3.delete_object(Bucket=src_bucket, Key=key)

def load_yaml_from_s3(s3_path: str) -> dict:
    s3 = boto3.client("s3")
    path_parts = s3_path.replace("s3://", "").split("/", 1)
    bucket, key = path_parts[0], path_parts[1]
    obj = s3.get_object(Bucket=bucket, Key=key)
    return yaml.safe_load(obj["Body"].read())

def normalize_string_columns(df: DataFrame) -> DataFrame:
    # single pass normalization for string columns
    exprs = [lower(trim(F.col(f.name))).alias(f.name) if isinstance(f.dataType, T.StringType) else F.col(f.name)
             for f in df.schema.fields]
    return df.select(*exprs)

# --------------------
# Load Data
# --------------------
df_items            = spark.read.parquet(s3_join(TRANSFORM_PATH, "order_items/"))
df_item_options_raw = spark.read.parquet(s3_join(TRANSFORM_PATH, "order_item_options/"))
df_date_dim_raw     = spark.read.parquet(s3_join(TRANSFORM_PATH, "date_dim/"))

# Normalize strings once
df_items        = normalize_string_columns(df_items)
df_item_options = normalize_string_columns(df_item_options_raw)
df_date_dim     = normalize_string_columns(df_date_dim_raw)

# Normalize join keys
df_items        = df_items.withColumn("lineitem_id", lower(trim(col("lineitem_id"))))
df_item_options = df_item_options.withColumn("lineitem_id", lower(trim(col("lineitem_id"))))
df_date_dim     = df_date_dim.withColumn("date_key", lower(trim(col("date_key"))))

# -------------------------
# BASIC QA (price / quantity)
# -------------------------
def detect_price_issues(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("item_price").isNull() |
        (F.col("item_price") <= 0) |
        (F.col("item_price") == 1) |
        ((F.col("item_price") > 0) & (F.col("item_price") < 1)) |
        (F.col("item_price") > 100)
    )

def detect_quantity_issues(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("item_quantity").isNull() |
        (F.col("item_quantity") == 0) |
        (F.col("item_quantity") == 1) |
        (F.col("item_quantity") > 47)
    )

price_issues_df    = detect_price_issues(df_items).withColumn("issue_type", lit("price_issue"))
quantity_issues_df = detect_quantity_issues(df_items).withColumn("issue_type", lit("quantity_issue"))

# Write QA extracts only if they have rows (avoid extra actions)
if price_issues_df.limit(1).count() > 0:
    price_issues_df.write.mode("overwrite").parquet(s3_join(QA_PRICE_PATH, "price_issues/"))
if quantity_issues_df.limit(1).count() > 0:
    quantity_issues_df.write.mode("overwrite").parquet(s3_join(QA_QUANTITY_PATH, "quantity_issues/"))

# Subtract incrementally (do not restart from the original)
df_items = df_items.subtract(price_issues_df.select(df_items.columns))
df_items = df_items.subtract(quantity_issues_df.select(df_items.columns))

# -------------------------
# Thresholds (broadcast for UDF)
# -------------------------
thresholds_dict       = load_yaml_from_s3(THRESHOLDS_DICT_PATH)
broadcast_thresholds  = spark.sparkContext.broadcast(thresholds_dict)

def flag_row(restaurant_id: str, category: str, item: str, price: float, quantity: int) -> str:
    t = broadcast_thresholds.value
    try:
        rid = (restaurant_id or "").strip().lower()
        cat = (category or "").strip().lower()
        it  = (item or "").strip().lower()
        rules        = t[rid][cat][it]
        price_rules  = rules.get("price", {})
        qty_rules    = rules.get("quantity", {})
    except Exception:
        return "none"

    violations = 0
    if price is not None:
        pmin = price_rules.get("min"); pmax = price_rules.get("max")
        if pmin is not None and pmax is not None and (price < pmin or price > pmax):
            violations += 1
    if quantity is not None:
        qmin = qty_rules.get("min"); qmax = qty_rules.get("max")
        if qmin is not None and qmax is not None and (quantity < qmin or quantity > qmax):
            violations += 1

    return "high" if violations >= 2 else ("low" if violations == 1 else "none")

flag_row_udf = F.udf(flag_row, T.StringType())

# -------------------------
# Expected schemas & helpers
# -------------------------
COLUMN_TYPES = {
    "order_key": "string",
    "date_key": "string",
    "app_name": "string",
    "restaurant_id": "string",
    "user_id": "string",
    "printed_card_number": "string",
    "order_id": "string",
    "lineitem_id": "string",
    "is_loyalty": "boolean",
    "item_category": "string",
    "item_name": "string",
    "size": "string",
    "option_group_name": "string",
    "option_name": "string",
    "option_price": "double",
    "option_quantity": "int",
    "item_price": "double",
    "item_quantity": "int",
    "date": "string",
    "time": "string",
    "severity": "string",
    "flagged": "boolean",
    # date_dim fields:
    "year": "int",
    "month": "int",
    "week": "int",
    "day_of_week": "string",
    "is_weekend": "boolean",
    "is_holiday": "boolean",
    "holiday_name": "string",
}

def enforce_types(df: DataFrame, type_map: dict) -> DataFrame:
    for name, spark_type in type_map.items():
        if name in df.columns:
            df = df.withColumn(name, F.col(name).cast(spark_type))
    return df

def cast_nulltype_to_string(df: DataFrame) -> DataFrame:
    for f in df.schema.fields:
        if isinstance(f.dataType, T.NullType):
            df = df.withColumn(f.name, F.col(f.name).cast("string"))
    return df

def rearrange_columns_typed(df: DataFrame, expected_columns: list) -> DataFrame:
    existing = set(df.columns)
    missing = [c for c in expected_columns if c not in existing]
    for mc in missing:
        df = df.withColumn(mc, F.lit(None).cast(COLUMN_TYPES.get(mc, "string")))
    df = df.select([c for c in expected_columns])
    df = enforce_types(df, COLUMN_TYPES)
    return cast_nulltype_to_string(df)

# -------------------------
# Canonicalize items (drop legacy names if present)
# -------------------------
# If your upstream transform created these canonical fields already, keep them.
# Otherwise, ensure they exist or rename before this step.
expected_columns_1 = [
    "order_key", "date_key", "app_name", "restaurant_id", "user_id",
    "printed_card_number", "order_id", "lineitem_id",
    "is_loyalty", "item_category", "item_name", "size",
    "item_price", "item_quantity", "date", "time",
]
df_items = df_items.drop("item_category", "item_name")
# If you still have legacy names (final_category/item_name_cleaned) rename here:
rename_map = {"final_category": "item_category", "item_name_cleaned": "item_name"}
for old, new in rename_map.items():
    if old in df_items.columns:
        df_items = df_items.withColumnRenamed(old, new)

items_can = rearrange_columns_typed(df_items, expected_columns_1)

# -------------------------
# Flagging BEFORE joins
# -------------------------
items_flagged = (
    items_can
    .withColumn("severity", flag_row_udf(
        col("restaurant_id"), col("item_category"), col("item_name"),
        col("item_price"), col("item_quantity")
    ))
    .withColumn("flagged", col("severity") != lit("none"))
)

# -------------------------
# Join with options (repartition on key to reduce shuffle)
# -------------------------
items_flagged       = items_flagged.repartition(160, "lineitem_id").cache()
df_item_options     = df_item_options.drop("order_id").repartition(160, "lineitem_id")

joined_df_1 = items_flagged.alias("items").join(
    df_item_options.alias("options"), on="lineitem_id", how="left"
)

expected_columns_2 = [
    "order_key", "date_key", "app_name", "restaurant_id", "user_id",
    "printed_card_number", "order_id", "lineitem_id",
    "is_loyalty", "item_category", "item_name", "size",
    "option_group_name", "option_name", "option_price", "option_quantity",
    "item_price", "item_quantity", "date", "time",
    "severity", "flagged",
]
joined_df_1 = rearrange_columns_typed(joined_df_1, expected_columns_2).fillna({
    "option_price": 0.0,
    "option_quantity": 0,
    "option_group_name": "N/A",
    "option_name": "N/A",
})

# -------------------------
# Enrich WITH date_dim (fast, no calendar generation)
# -------------------------
# Expecting date_dim columns: date_key, year, month, week, day_of_week, is_weekend, is_holiday, holiday_name
date_cols = ["date_key", "year", "month", "week", "day_of_week", "is_weekend", "is_holiday", "holiday_name"]
df_date_dim = df_date_dim.select(*[c for c in date_cols if c in df_date_dim.columns])

joined_df_2 = joined_df_1.join(df_date_dim, on="date_key", how="left")

expected_columns_3 = [
    "order_key", "date_key", "app_name", "restaurant_id", "user_id",
    "printed_card_number", "order_id", "lineitem_id",
    "is_loyalty", "item_category", "item_name", "size",
    "option_group_name", "option_name", "option_price", "option_quantity",
    "item_price", "item_quantity", "date", "time",
    "year", "month", "week", "day_of_week", "is_weekend", "is_holiday", "holiday_name",
    "severity", "flagged",
]
joined_df_3 = rearrange_columns_typed(joined_df_2, expected_columns_3).cache()
joined_df_3.count()   # materialize once

# -------------------------
# Writes (coalesce to reduce small files)
# -------------------------
final_out = joined_df_3.filter(col("severity") != "high")
qa_out    = joined_df_3.filter(col("severity") == "high")

final_out.coalesce(64).write.mode("overwrite").parquet(FINAL_PATH.rstrip("/") + "/")
qa_out.coalesce(8).write.mode("overwrite").parquet(QA_PATH.rstrip("/") + "/")

# -------------------------
# Move processed files (optional; set flag during perf tests)
# -------------------------
DO_MOVE = True
if DO_MOVE:
    ts = datetime.now().strftime('%Y%m%d%H%M%S')
    move_s3_objects(s3_join(TRANSFORM_PATH, "order_item_options/"), s3_join(PROCESS_PATH, f"order_item_options{ts}/"))
    move_s3_objects(s3_join(TRANSFORM_PATH, "order_items/"),        s3_join(PROCESS_PATH, f"order_items{ts}/"))
    move_s3_objects(s3_join(TRANSFORM_PATH, "date_dim/"),           s3_join(PROCESS_PATH, f"date_dim{ts}/"))

job.commit()
