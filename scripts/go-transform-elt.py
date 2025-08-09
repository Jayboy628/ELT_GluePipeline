import sys
import re
import yaml
import boto3
from datetime import datetime
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField

# --------------------------------------------------------------------
# 1. Parse Job Arguments
# --------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "LOAD_PATH",
        "TRANSFORM_PATH",
        "PROCESS_PATH",
        "ERROR_PATH",
        "BEVERAGE_REGEX_PATTERNS",
        "SIZE_REGEX_PATTERNS",
        "CATEGORY_REGEX_PATTERN",
    ],
)

LOAD_PATH = args["LOAD_PATH"]
TRANSFORM_PATH = args["TRANSFORM_PATH"]
PROCESS_PATH = args["PROCESS_PATH"]

# --------------------------------------------------------------------
# 2. Spark & Glue Context Setup
# --------------------------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------
# 3. Helper: Load YAML Mappings from S3
# --------------------------------------------------------------------
def load_yaml_from_s3(s3_path):
    s3 = boto3.client("s3")
    parsed = urlparse(s3_path)
    obj = s3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    return yaml.safe_load(obj["Body"].read())

# Load category, beverage and size patterns from YAML
category_yaml = load_yaml_from_s3(args["CATEGORY_REGEX_PATTERN"])
beverage_yaml = load_yaml_from_s3(args["BEVERAGE_REGEX_PATTERNS"])
size_yaml = load_yaml_from_s3(args["SIZE_REGEX_PATTERNS"])

# --------------------------------------------------------------------
# 4. Compile Patterns for Classification
#    Assumes YAML structured like:
#    category_patterns: { "<regex>": "<replacement>", ... }
#    patterns: [ { category: "<cat>", pattern: "<regex>" }, ... ]
#    size_regex_pattern: "<regex>"
# --------------------------------------------------------------------
category_patterns = category_yaml.get("patterns", {})
# Broadcast the dictionary to each executor
category_broadcast = sc.broadcast(category_patterns)

beverage_patterns_list = beverage_yaml.get("patterns", [])
# Pre-compile regex and broadcast
beverage_compiled = [
    (re.compile(item["pattern"], re.IGNORECASE), item["category"])
    for item in beverage_patterns_list
]
beverage_broadcast = sc.broadcast(beverage_compiled)

size_patterns = size_yaml.get("size_patterns", None)
if not size_patterns:
    raise ValueError("size_patterns missing from size YAML")

# --------------------------------------------------------------------
# 5. Define UDFs
# --------------------------------------------------------------------
@F.udf(returnType=StringType())
def clean_category_label_udf(label):
    """
    Remove URLs, backticks and trim; limit to 30 chars.
    """
    if not isinstance(label, str):
        return label
    cleaned = re.sub(r"http\\S+", "", label).replace("`", "").strip()
    return cleaned[:30]

@F.udf(returnType=StringType())
def fix_category_udf(label):
    """
    Apply regex-based category corrections.
    """
    if not isinstance(label, str):
        return label
    label_lower = label.lower().strip()
    for pattern, replacement in category_broadcast.value.items():
        if re.search(pattern, label_lower):
            return replacement
    return label_lower

@F.udf(returnType=StringType())
def classify_beverage_udf(name):
    """
    Classify beverage type (energy drinks, sodas, water, juices & kombuchas).
    Returns None if not a beverage.
    """
    if not isinstance(name, str):
        return None
    for pattern, category in beverage_broadcast.value:
        if pattern.search(name):
            return category
    return None

@F.udf(returnType=StringType())
def clean_item_name_udf(item_name):
    """
    Clean item name: remove non-alphanumerics except '|', '&'; remove '*', 'the',
    redundant spaces and Alltown Fresh variants.
    """
    if not isinstance(item_name, str):
        return item_name
    # Remove non-alphanumeric (keeping |, &, and spaces)
    cleaned = re.sub(r"[^a-zA-Z0-9|&* ]+", "", item_name)
    cleaned = cleaned.replace("*", "").strip().lower()
    cleaned = re.sub(r"\\bthe\\b", "", cleaned).strip()
    cleaned = re.sub(r"\\s+", " ", cleaned)
    # Remove "Alltowns" variants
    cleaned = re.sub(
        r"\\b(alltowns|all\\s*towns|all-town\\s*s|alltown\\s*fresh)\\b",
        "",
        cleaned,
    ).strip()
    return cleaned

# --------------------------------------------------------------------
# 6. Read Data
#    Adjust the read method to your actual data format (CSV, Parquet, etc.).
# --------------------------------------------------------------------
# Example reading from CSV; adjust schema as needed
order_items = spark.read.option("header", True).csv(f"{LOAD_PATH}/order_items/")

# --------------------------------------------------------------------
# 7. Process the DataFrame
# --------------------------------------------------------------------
# a) Clean category labels
df = order_items.withColumn(
    "category_cleaned",
    fix_category_udf(clean_category_label_udf(F.col("item_category"))),
)

# b) Clean item names
df = df.withColumn("item_name_cleaned", clean_item_name_udf(F.col("item_name")))

# c) Extract size using regexp_extract and remove size tokens
df = df.withColumn(
    "size",
    F.regexp_extract(F.col("item_name_cleaned"), size_patterns, 0),
).withColumn(
    "size",
    F.regexp_replace(F.col("size"), "[()]", "").alias("size"),
).withColumn(
    "size",
    F.lower(F.col("size")).alias("size"),
)

df = df.withColumn(
    "item_name_cleaned",
    F.regexp_replace(F.col("item_name_cleaned"), size_patterns, "").alias(
        "item_name_cleaned"
    ),
).withColumn(
    "item_name_cleaned",
    F.regexp_replace(F.col("item_name_cleaned"), "\\s+", " ").alias(
        "item_name_cleaned"
    ),
)

# d) Classify beverages from cleaned names
df = df.withColumn("beverage_type", classify_beverage_udf(F.col("item_name_cleaned")))

# e) Choose final category: beverage category overrides non-beverage
df = df.withColumn(
    "final_category",
    F.when(F.col("beverage_type").isNotNull(), F.col("beverage_type")).otherwise(
        F.col("category_cleaned")
    ),
)

# --------------------------------------------------------------------
# 8. Additional Category Cleanups
#    Combine sandwiches & subs, reassign gluten free etc.
# --------------------------------------------------------------------
# Combine categories (subs → burgers & sandwiches)
df = df.withColumn(
    "final_category",
    F.when(F.col("final_category").isin("sandwiches", "subs"), "burgers & sandwiches")
    .when(F.col("final_category") == "gluten free", "bowls")
    .when(F.col("final_category") == "most popular", "snacks")
    .when(F.col("final_category") == "candy & chocolate", "snacks")
    .when(F.col("final_category").isin("vegetarian options", "vegan options"), "breakfast")
    .otherwise(F.col("final_category")),
)

# f) Reassign bowls, salads, breakfast, burgers/sandwiches based on keywords
df = df.withColumn(
    "final_category",
    F.when(
        (F.col("item_name_cleaned").rlike("\\b(bowl|salad)s?\\b"))
        & (~F.col("item_name_cleaned").rlike("\\bcatering\\b")),
        "bowls",
    )
    .when(
        (F.col("item_name_cleaned").rlike("\\b(maine\\s?killer)\\b"))
        & (~F.col("item_name_cleaned").rlike("\\bcatering\\b")),
        "salads",
    )
    .when(
        (F.col("item_name_cleaned").rlike("\\b(grain)\\b"))
        & (~F.col("item_name_cleaned").rlike("\\bcatering\\b")),
        "breakfast",
    )
    .when(
        (F.col("item_name_cleaned").rlike("\\b(burger|sandwich|falafel)s?\\b"))
        & (~F.col("item_name_cleaned").rlike("\\bcatering\\b")),
        "burgers & sandwiches",
    )
    .otherwise(F.col("final_category")),
)

# g) Remove test data
df = df.filter(
    ~(
        F.col("item_category").rlike("(?i)test")
        | F.col("item_name").rlike("(?i)test")
    )
)

# --------------------------------------------------------------------
# 9. Normalise Column Names to Lowercase
# --------------------------------------------------------------------
def normalize_columns(spark_df):
    new_columns = [c.strip().lower() for c in spark_df.columns]
    for old_col, new_col in zip(spark_df.columns, new_columns):
        spark_df = spark_df.withColumnRenamed(old_col, new_col)
    return spark_df

df = normalize_columns(df)

# --------------------------------------------------------------------
# 9. Move landing file to processing bucket
# --------------------------------------------------------------------

def move_s3_objects(source_path, destination_path):
    s3 = boto3.client("s3")
    src = urlparse(source_path)
    dst = urlparse(destination_path)

    src_bucket, src_prefix = src.netloc, src.path.lstrip("/")
    dst_bucket, dst_prefix = dst.netloc, dst.path.lstrip("/")

    objs = s3.list_objects_v2(Bucket=src_bucket, Prefix=src_prefix).get("Contents", [])
    for o in objs:
        key = o["Key"]
        if key.endswith("/"):
            continue
        new_key = f"{dst_prefix.rstrip('/')}/{key.split('/')[-1]}"
        print(f"Moving: s3://{src_bucket}/{key} → s3://{dst_bucket}/{new_key}")
        s3.copy_object(Bucket=dst_bucket, CopySource={"Bucket": src_bucket, "Key": key}, Key=new_key)
        s3.delete_object(Bucket=src_bucket, Key=key)


# --------------------------------------------------------------------
# 10. Write Output and Commit
# --------------------------------------------------------------------
df.write.mode("overwrite").parquet(f"{TRANSFORM_PATH}/order_items/")

# Optionally move processed files via boto3 after writing; omitted for clarity


# --- [MOVE PROCESSED FILES] ---
timestamp = datetime.now().strftime('%Y%m%d%H%M%S')


move_s3_objects(f"{LOAD_PATH}order_item_options/", f"{PROCESS_PATH}order_item_options{datetime.now().strftime('%Y%m%d%H%M%S')}/")
move_s3_objects(f"{LOAD_PATH}order_items/", f"{PROCESS_PATH}order_items{datetime.now().strftime('%Y%m%d%H%M%S')}/")
move_s3_objects(f"{LOAD_PATH}date_dim/", f"{PROCESS_PATH}date_dim{datetime.now().strftime('%Y%m%d%H%M%S')}/")


job.commit()
