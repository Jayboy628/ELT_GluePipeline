# Glue ETL script for transforming and cleaning data from AWS S3 using PySpark

import sys
import os
import re
import yaml
import boto3
import hashlib
from urllib.parse import urlparse
from collections import defaultdict

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, mean, stddev, min as spark_min, max as spark_max,
    array, array_remove, size, regexp_replace, to_timestamp,
    date_format, udf, explode, lit, lower
)
from pyspark.sql.types import StructType, StructField, StringType


# -------------------------
# Parse Job Arguments
# -------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "LOAD_PATH", "TRANSFORM_PATH", "PROCESS_PATH", "ERROR_PATH", "QA_QUANTITY_PATH",
    "QA_PRICE_PATH",
    "COFFEE_SPECIALTY_MAPPING_PATH", "COFFEE_DRIP_MAPPING_PATH",
    "SANDWICH_MAPPING_PATH", "BREAKFAST_MAPPING_PATH",
    "CATEGORY_MAPPING_PATH", "BOWLS_MAPPING_PATH"
])

LOAD_PATH = args["LOAD_PATH"]
TRANSFORM_PATH = args["TRANSFORM_PATH"]
PROCESS_PATH = args["PROCESS_PATH"]
QA_QUANTITY_PATH = args["QA_QUANTITY_PATH"]
QA_PRICE_PATH = args["QA_PRICE_PATH"]
ERROR_PATH = args["ERROR_PATH"]

cat_mapping = {"category": args["CATEGORY_MAPPING_PATH"]}

mapping_paths = {
    "coffee_specialty": args["COFFEE_SPECIALTY_MAPPING_PATH"],
    "coffee_drip": args["COFFEE_DRIP_MAPPING_PATH"],
    "sandwiches": args["SANDWICH_MAPPING_PATH"],
    "breakfast": args["BREAKFAST_MAPPING_PATH"],
    "bowls": args["BOWLS_MAPPING_PATH"]
}

# -------------------------
# Spark & Glue Context Setup
# -------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------
#Load Mappings from S3
# -------------------------
def load_yaml_from_s3(s3_path):
    s3 = boto3.client("s3")
    parsed = urlparse(s3_path)
    obj = s3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    return yaml.safe_load(obj["Body"].read())

# -------------------------------------
# Move processed files from S3
# -------------------------------------

def move_s3_objects(source_path, destination_path):
    s3 = boto3.client("s3")
    src_bucket, src_prefix = urlparse(source_path).netloc, urlparse(source_path).path.lstrip("/")
    dst_bucket, dst_prefix = urlparse(destination_path).netloc, urlparse(destination_path).path.lstrip("/")
    objs = s3.list_objects_v2(Bucket=src_bucket, Prefix=src_prefix).get("Contents", [])
    for o in objs:
        key = o["Key"]
        if key.endswith("/"): continue
        new_key = os.path.join(dst_prefix, os.path.basename(key))
        s3.copy_object(Bucket=dst_bucket, CopySource={"Bucket": src_bucket, "Key": key}, Key=new_key)
        s3.delete_object(Bucket=src_bucket, Key=key)

# -------------------------------------
# Category and Item Name Mappings
# -------------------------------------

# 1 Clean label (remove links etc.)
clean_category_label_udf = udf(
    lambda label: re.sub(r"http\S+", "", label.replace("`", "")).strip()[:30] if isinstance(label, str) else label,
    StringType()
)

# 2 Load and apply category mappings
category_mapping_dict = load_yaml_from_s3(cat_mapping["category"]).get("category", {})
fix_category_udf = udf(
    lambda cat: category_mapping_dict.get(cat.strip(), cat) if cat else None,
    StringType()
)

# 3 Load item name mapping YAMLs
mapping_files = {
    "Breakfast": load_yaml_from_s3(mapping_paths["breakfast"]),
    "Bowls": load_yaml_from_s3(mapping_paths["bowls"]),
    "Sandwiches": load_yaml_from_s3(mapping_paths["sandwiches"]),
    "Coffee Specialty": load_yaml_from_s3(mapping_paths["coffee_specialty"]),
    "Coffee Drip": load_yaml_from_s3(mapping_paths["coffee_drip"])
}

# 4 Build item name mapping dictionary
from collections import defaultdict
item_name_mappings = defaultdict(dict)

def populate_mappings(category_label, source_map):
    for old, new in source_map.get(category_label.title(), source_map).items():
        if isinstance(old, str) and isinstance(new, str):
            item_name_mappings[category_label][old.strip().upper()] = new.strip()

for category, mapping in mapping_files.items():
    populate_mappings(category, mapping)

# 5 Broadcast and define UDF to apply mapping
broadcast_item_mappings = sc.broadcast(dict(item_name_mappings))
fix_name_by_category_udf = udf(
    lambda name, cat: broadcast_item_mappings.value.get(cat.strip(), {}).get(name.strip().upper(), name)
    if name and cat else name,
    StringType()
)

# -------------------------
# ☕ Category Name Cleaning
# -------------------------

def load_cleaning_rules():
    all_rules = []
    for key in ["sandwiches", "bowls", "breakfast", "coffee_drip", "coffee_specialty"]:
        raw_map = load_yaml_from_s3(mapping_paths[key])
        if isinstance(raw_map, dict):
            # Handle nested dicts (e.g., {"Sandwiches": {...}})
            for inner in raw_map.values():
                if isinstance(inner, dict):
                    all_rules += list(inner.items())
                else:
                    all_rules += list(raw_map.items())
                    break  # Exit if already flat
    return all_rules

cleaning_rules = load_cleaning_rules()


# -------------------------
# ☕ Category Name Cleanin
# -------------------------


# Classification function for beverages
def classify_soda_juice_kombucha_water(item_name):
    if item_name is None:
        return None

    name = str(item_name).lower()

    soda_keywords = [
        'coke', 'cola', 'pepsi', 'sprite', 'soda', 'fanta', 
        'dr pepper', 'mountain dew', 'root beer', 'sprint', 'crush'
    ]
    juice_keywords = [
        'juice', 'oj', 'orange juice', 'apple juice', 'cranberry', 
        'jce'
    ]
    drinks_keywords = [
        'drink', 'fountain drinks', 'nesquik chocolate', 'lemonade'
    ]
    energy_drink_keywords = [
        'energy drink', 'red bull', 'monster', 'rockstar', '5-hour energy','gatorade', 'powerade', 'vitamin water', 'energy boost'
    ]
    water_keywords = [
        'water', 'poland', 'dasani', 'smartwater', 'aquafina'
    ]
    kombucha_keywords = ['kombucha']

    if any(kw in name for kw in soda_keywords):
        return 'Sodas'

    elif any(kw in name for kw in juice_keywords):
        return 'Juices'
    
    elif any(kw in name for kw in drinks_keywords):
        return 'Drinks'
    
    elif any(kw in name for kw in energy_drink_keywords):
        return 'Energy Drinks'
    
    elif any(kw in name for kw in water_keywords):
        return 'Water'
    
    elif any(kw in name for kw in kombucha_keywords):
        return 'Kombuchas'
    else:
        return 'Beverages'

# Register as Spark UDF
classify_beverage_udf = udf(classify_soda_juice_kombucha_water, StringType())


# Register UDFs
spark.udf.register("clean_coffee", lambda name: next(
    (c for p, c in cleaning_rules if re.match(p, name.strip(), re.IGNORECASE)), name) if name else name)
spark.udf.register("extract_size", lambda name: re.search(r"(\d+(\.\d+)?\s?(oz|OZ))", name).group(1)
    if name and re.search(r"(\d+(\.\d+)?\s?(oz|OZ))", name) else None)
spark.udf.register("extract_name", lambda name: re.sub(r"\s*\d+.*oz.*$", "", name).strip() if name else name)
spark.udf.register("classify_beverage", lambda name: "Coffee" if name and "coffee" in name.lower() else None)

# new column for size
extract_size_udf = udf(
    lambda name: (
        (
            (size := (
                re.search(r'(\d+(\.\d+)?\s?(oz|OZ))', name) or          # e.g. 12 oz
                re.search(r'\b(\d+/\d+)\b', name) or                    # e.g. 1/2
                re.search(r'(\d+)(\"|\s*in\b)', name, re.IGNORECASE)   # e.g. 8", 6 in
            )) and 
            (
                # Extract size
                size.group(1).replace('"', '').strip(), 
                # Clean the name: remove size + any asterisks or quotes
                re.sub(size.group(0), "", name)
                  .replace("*", "")
                  .replace('"', "")
                  .strip()
            )
        ) if name else (None, None)
    ),
    StructType([
        StructField("size", StringType(), True),
        StructField("cleaned_name", StringType(), True)
    ])
)



#1. Define UDF to generate surrogate key
def generate_pk_oio(order_id, lineitem_id, user_id=None):
    key = f"{order_id or ''}_{lineitem_id or ''}_{user_id or ''}"
    return hashlib.md5(key.encode()).hexdigest()

generate_pk_udf = udf(generate_pk_oio, StringType())

#2. Read and preprocess raw order_items data
# --- [READ] ---
try:
    order_items = spark.read.parquet(f"{LOAD_PATH}order_items/")
    order_item_options = spark.read.parquet(f"{LOAD_PATH}order_item_options/")
    date_dim = spark.read.parquet(f"{LOAD_PATH}date_dim/")
except Exception as e:
    print("Error reading files:", e)
    for f in ["order_items", "order_item_options", "date_dim"]:
        move_s3_objects(f"s3://{LOAD_PATH}{f}/", f"s3://{ERROR_PATH}{f}/")
    raise

# -------------------------
# BASIC QA
# -------------------------

def detect_price_issues(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("item_price") == '') |
        (col("item_price") == 0) |
        (col("item_price") < 0) |
        (col("item_price") == 1) |
        ((col("item_price") > 0) & (col("item_price") < 1)) |
        (col("item_price") > 100)
    )

def detect_quantity_issues(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("item_quantity") == '') |
        (col("item_quantity") == 0) |
        (col("item_quantity") == 1) |
        (col("item_quantity") > 47)
    )

# Detect and write
price_issues_df = detect_price_issues(order_items).withColumn("issue_type", lit("price_issue"))
quantity_issues_df = detect_quantity_issues(order_items).withColumn("issue_type", lit("quantity_issue"))

# Write to S3

if not price_issues_df.rdd.isEmpty():
    price_issues_df.write.mode("overwrite").parquet(f"{QA_PRICE_PATH}price_issues/")
if not quantity_issues_df.rdd.isEmpty():
    quantity_issues_df.write.mode("overwrite").parquet(f"{QA_QUANTITY_PATH}quantity_issues/")

order_items = order_items.subtract(price_issues_df.select(order_items.columns))
order_items = order_items.subtract(quantity_issues_df.select(order_items.columns))


# -------------------------
# CONVERT DATE
# -------------------------
from pyspark.sql.functions import col, to_timestamp, hour, minute, second, date_format

order_items = order_items \
    .withColumn("creation_time_utc", to_timestamp("creation_time_utc")) \
    .withColumn("time", date_format("creation_time_utc", "HH:mm:ss")) \
    .withColumn("date", col("creation_time_utc").cast("date")) \
    .withColumn("date_key", date_format("creation_time_utc", "dd-MM-yyyy"))


order_items = order_items.select([col(c).alias(c.lower()) for c in order_items.columns])



# -------------------------
# Cleaning Order Items
# -------------------------

#3. Begin cleaning pipeline
df_cleaned = order_items \
    .withColumn("printed_card_number", when(col("printed_card_number").isNull(), "00000")
                .otherwise(regexp_replace(col("printed_card_number").cast(StringType()), r"\\.", ""))
                .cast("double")) \
    .withColumn("user_id", when(col("user_id").isNull(), "_guest").otherwise(col("user_id"))) \
    .withColumn("order_id", col("order_id").cast("string")) \
    .withColumn("lineitem_id", col("lineitem_id").cast("string")) \
    .withColumn("item_name", col("item_name").cast("string").alias("item_name")) \
    .withColumn("item_category", col("item_category").cast("string").alias("item_category")) \
    .withColumn("item_name", F.lower(col("item_name"))) \
    .withColumn("item_category", F.lower(col("item_category"))) \
    .withColumn("item_name_cleaned", F.expr("clean_coffee(item_name)")) \
    .withColumn("extracted", extract_size_udf(col("item_name"))) \
    .withColumn("size", F.lower(col("extracted.size"))) \
    .withColumn("item_name", F.expr("extract_name(item_name_cleaned)")) \
    .drop("extracted") \
    .withColumn("item_category", fix_category_udf(clean_category_label_udf(col("item_category")))) \
    .withColumn("item_name", fix_name_by_category_udf(col("item_name"), col("item_category")))

df_cleaned = df_cleaned.withColumn(
    "item_category",
    when(
        col("item_category").rlike("(?i)soda|juice|drink|bottled"),  # case-insensitive match
        classify_beverage_udf(col("item_name"))
    ).otherwise(col("item_category"))
)


# -------------------------
# normalize_columns for Vegan, Vegitarian, Gluten Free
# -------------------------

def normalize_item_name(name):
    if not name:
        return ""

    # Remove asterisk and normalize quotes
    name = name.replace('*', '').replace('“', '"').replace('”', '"').strip()

    # Extract base and modifiers
    parts = [part.strip() for part in name.split("|")]
    base = parts[0].lower()
    modifiers = parts[1] if len(parts) > 1 else ""

    # Extract size and remove from base
    size_match = re.search(r'(\d+("|\s?oz)|\d+/\d+)', base)
    size = size_match.group(1) if size_match else ""
    if size:
        base = base.replace(size, "").strip()

    # Handle vegan in base name
    if "vegan" in base:
        base = base.replace("vegan", "").strip()
        if "VEG" not in modifiers.upper():
            modifiers += ", VEG"
        if "V" not in modifiers.upper():
            modifiers += ", V"

    # Normalize and expand modifiers
    modifier_tokens = [m.strip().upper() for m in modifiers.split(",") if m.strip()]
    expanded_modifiers = []
    for m in modifier_tokens:
        if m == "gl":
            expanded_modifiers.append("GL")
        elif m == "veg":
            expanded_modifiers.append("VEG")
        elif m == "v":
            expanded_modifiers.append("V")
        else:
            expanded_modifiers.append(m)

    # Final tag string
    tag_string = ", ".join(sorted(set(expanded_modifiers)))

    return f"{base} | {tag_string}" if tag_string else base

# Register UDF
normalize_item_name_udf = udf(normalize_item_name, StringType())

df_cleaned = df_cleaned.withColumn("item_name", normalize_item_name_udf(col("item_name")))

# -------------------------
# move item name V, VEP, Vegitarian, Vegan Category
# -------------------------


def assign_category_based_on_tags(df, item_name_col="item_name", category_col="item_category"):
    """
    Reassigns item_category based on V/VEG/GF tags in item_name.

    Rules:
    - If item_name contains '| V, VEG' or '| V, VEG, GF' → vegan option
    - If item_name contains '| VEG' or '| VEG, GF' → vegetarian options

    Args:
        df (DataFrame): Input Spark DataFrame.
        item_name_col (str): Column containing the normalized item name.
        category_col (str): Column to overwrite with new category.

    Returns:
        DataFrame: DataFrame with updated item_category.
    """
    return df.withColumn(
        category_col,
        when(col(item_name_col).contains("| V, VEG"), "vegan options")
        .when(col(item_name_col).contains("| V, VEG, GF"), "vegan options")
        .when(col(item_name_col).contains("| GF, V, VEG"), "vegan options")
        .when(col(item_name_col).contains("| VEG, GF"), "vegetarian options")
        .when(col(item_name_col).contains("| GF, VEG"), "vegetarian options")
        .when(col(item_name_col).contains("| VEG"), "vegetarian options")
        .otherwise(col(category_col))
    )

# Apply item name normalization
df_cleaned = df_cleaned.withColumn("item_name", normalize_item_name_udf(col("item_name")))

# Reassign category based on tags in normalized item name
df_cleaned = assign_category_based_on_tags(df_cleaned)



#4. Add surrogate key using UDF
df_cleaned = df_cleaned.withColumn(
    "order_key",
    generate_pk_udf(col("order_id"), col("lineitem_id"), col("user_id"))
).persist()


def remove_test_data(df):
    """
    Removes rows where item_category or item_name contains 'test' (case-insensitive).
    """
    return df.filter(
        ~(
            col("item_category").rlike("(?i)test") |
            col("item_name").rlike("(?i)test")
        )
    )

df_cleaned = remove_test_data(df_cleaned)


# -------------------------
# Column lowercase
# -------------------------

def normalize_columns(spark_df):
    """
    Renames columns in a PySpark DataFrame by stripping whitespace and converting to lowercase.
    """
    new_columns = [col_name.strip().lower() for col_name in spark_df.columns]
    for old, new in zip(spark_df.columns, new_columns):
        spark_df = spark_df.withColumnRenamed(old, new)
    return spark_df

order_item_options = normalize_columns(order_item_options) 
date_dim = normalize_columns(date_dim)

# --- [WRITE] ---
order_items.write.mode("overwrite").parquet(f"{TRANSFORM_PATH}order_items/")
order_item_options.write.mode("overwrite").parquet(f"{TRANSFORM_PATH}order_item_options/")
date_dim.write.mode("overwrite").parquet(f"{TRANSFORM_PATH}date_dim/")

# --- [MOVE PROCESSED FILES] ---
move_s3_objects(f"s3://{LOAD_PATH}order_items/", f"s3://{PROCESS_PATH}order_items/")
move_s3_objects(f"s3://{LOAD_PATH}order_item_options/", f"s3://{PROCESS_PATH}order_item_options/")
move_s3_objects(f"s3://{LOAD_PATH}date_dim/", f"s3://{PROCESS_PATH}date_dim/")


job.commit()


