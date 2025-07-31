
import sys
import boto3
import yaml
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from awsglue.job import Job
from pyspark.sql.functions import col, lower, trim, udf, lit, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, DateType


# ---------------------
# Job Setup
# ---------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "TRANSFORM_PATH",
    "THRESHOLDS_DICT_PATH",
    "FINAL_PATH",
    "QA_PATH"
])


TRANSFORM_PATH = args["TRANSFORM_PATH"]
THRESHOLDS_DICT_PATH = args["THRESHOLDS_DICT_PATH"]
FINAL_PATH = args["FINAL_PATH"]
QA_PATH = args["QA_PATH"]
ERROR_PATH = ["ERROR_PATH"]


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------
# Load Data
# ---------------------

order_items = spark.read.parquet(f"{TRANSFORM_PATH}order_items/")
order_item_options = spark.read.parquet(f"{TRANSFORM_PATH}order_item_options/")
date_dim = spark.read.parquet(f"{TRANSFORM_PATH}date_dim/")


# ---------------------
# Normalize Strings
# ---------------------
def normalize_string_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, lower(trim(col(field.name))))
    return df

order_items = normalize_string_columns(order_items)
order_item_options = normalize_string_columns(order_item_options)
date_dims = normalize_string_columns(date_dim)

order_items = order_items.withColumn("lineitem_id", lower(trim(col("lineitem_id"))))
order_item_options = order_item_options.withColumn("lineitem_id", lower(trim(col("lineitem_id"))))
date_dim = date_dim.withColumn("date_key", lower(trim(col("date_key"))))

# -----------------------
# Enhance Date and hour
# -----------------------

def enhance_dim_date(df: DataFrame) -> DataFrame:
    """
    Enhances a dim_date Spark DataFrame with holiday, month, and calendar attributes.

    Args:
        df (DataFrame): Spark DataFrame with at least a 'date_key' column (dd-MM-yyyy format)

    Returns:
        DataFrame: Enhanced Spark DataFrame
    """
    # Step 1: Convert 'date_key' to actual date
    df = df.withColumn("parsed_date", to_date(col("date_key"), "dd-MM-yyyy"))
    min_date = df.selectExpr("min(parsed_date)").first()[0]
    max_date = min_date.replace(year=min_date.year + 5)

    # Step 2: Use pandas to build full calendar range
    full_dates = pd.date_range(start=min_date, end=max_date, freq="D")
    holidays = {
        "01-01": "New Year's Day",
        "07-04": "Independence Day",
        "12-25": "Christmas",
        "11-11": "Veterans Day",
    }
    
    date_df = pd.DataFrame({
        "parsed_date": full_dates,
        "date_key": full_dates.strftime("%d-%m-%Y"),
        "month_name": full_dates.strftime("%B"),
        "day_of_weeks": full_dates.strftime("%A"),
        "is_weekends": full_dates.weekday >= 5
    })

    # Add holidays (fix: use fillna(''))
    date_df["mm-dd"] = date_df["parsed_date"].dt.strftime("%m-%d")
    date_df["is_holiday"] = date_df["mm-dd"].isin(holidays.keys())
    date_df["holiday_name"] = date_df["mm-dd"].map(holidays).fillna("")
    date_df.drop(columns=["mm-dd"], inplace=True)

    # Define schema and convert to Spark DataFrame
    schema = StructType([
        StructField("parsed_date", DateType()),
        StructField("date_key", StringType()),
        StructField("month_name", StringType()),
        StructField("day_of_weeks", StringType()),
        StructField("is_weekends", BooleanType()),
        StructField("is_holidays", BooleanType()),
        StructField("holiday_names", StringType())
    ])

    spark = df.sparkSession
    enhanced_dates = spark.createDataFrame(date_df, schema=schema)

    # Step 3: Join original dim_date with enhanced attributes
    df_enhanced = df.join(enhanced_dates, on="date_key", how="left")
    return df_enhanced



# -----------------------
# Define schema for DataFrame
# -----------------------
schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("item_category", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("item_price", DoubleType(), True),
    StructField("item_quantity", IntegerType(), True),
])

# -----------------------
# Load YAML from S3
# -----------------------
def load_yaml_from_s3(s3_path: str) -> dict:
    s3 = boto3.client("s3")
    path_parts = s3_path.replace("s3://", "").split("/", 1)
    bucket, key = path_parts[0], path_parts[1]
    obj = s3.get_object(Bucket=bucket, Key=key)
    return yaml.safe_load(obj["Body"].read())



thresholds_dict = load_yaml_from_s3(THRESHOLDS_DICT_PATH)

# -----------------------
# Define flagging UDF
# -----------------------

broadcast_thresholds = spark.sparkContext.broadcast(thresholds_dict)

def flag_row(
    restaurant_id: str,
    category: str,
    item: str,
    price: float,
    quantity: int
) -> str:
    thresholds = broadcast_thresholds.value
    try:
        restaurant_id = restaurant_id.strip().lower()
        category = category.strip().lower()
        item = item.strip().lower()

        rules = thresholds[restaurant_id][category][item]
        price_rules = rules.get("price", {})
        quantity_rules = rules.get("quantity", {})
    except (KeyError, AttributeError):
        return "none"

    violations = 0

    if price is not None:
        price_min = price_rules.get("min")
        price_max = price_rules.get("max")
        if price_min is not None and price_max is not None:
            if price < price_min or price > price_max:
                violations += 1

    if quantity is not None:
        qty_min = quantity_rules.get("min")
        qty_max = quantity_rules.get("max")
        if qty_min is not None and qty_max is not None:
            if quantity < qty_min or quantity > qty_max:
                violations += 1

    return "high" if violations >= 2 else "low" if violations == 1 else "none"

# Register UDF with proper return type
flag_row_udf = udf(flag_row, StringType())


# --------------------------------------------------
# Check for columns is there and `rearrange_columns`
# --------------------------------------------------

expected_columns_1 = [
    "order_key", "date_key", "app_name", "restaurant_id", "user_id", "printed_card_number", "order_id", "lineitem_id",
    "is_loyalty", "item_category", "item_name", "size",  "item_price", "item_quantity", "date", "time"
]

expected_columns_2 = [
    "order_key", "date_key", "app_name", "restaurant_id", "user_id", "printed_card_number", "order_id", "lineitem_id",
    "is_loyalty", "item_category", "item_name", "size", "option_group_name","option_name", "option_price", "option_quantity",  "item_price", "item_quantity", "date", "time"
]


expected_columns_3 = [
    "order_key", "date_key", "app_name", "restaurant_id", "user_id", "printed_card_number", "order_id", "lineitem_id",
    "is_loyalty", "item_category", "item_name", "size", "option_group_name","option_name", "option_price", "option_quantity", 
    "item_price", "item_quantity", "time", "date", "month_name",  "day_of_weeks", "is_weekends", "is_holiday", "holiday_name", "holiday_names"
]



def rearrange_columns(df: DataFrame, expected_columns: list) -> DataFrame:
    """
    Ensure all expected columns exist in the Spark DataFrame.
    - Adds any missing ones as nulls
    - Drops extra columns
    - Reorders to exactly match expected_columns

    Parameters:
        df (DataFrame): Input Spark DataFrame
        expected_columns (list): Desired and final column order

    Returns:
        DataFrame: Cleaned and strictly reordered DataFrame
    """
    existing_cols = set(df.columns)
    missing_cols = [col for col in expected_columns if col not in existing_cols]

    # Add missing columns as null
    for col in missing_cols:
        df = df.withColumn(col, lit(None))

    # Reorder and keep only expected columns
    final_df = df.select([col for col in expected_columns])
    
    print("Final columns set to:")
    print(final_df.columns)

    return final_df

# Step 1: Rearrange columns in order_items
order_items = rearrange_columns(order_items, expected_columns_1)



# Step 2: Apply flagging BEFORE any joins or column drops

# Step 2: Apply flagging BEFORE any joins or column drops
order_items_flagged = order_items.withColumn(
    "severity", flag_row_udf(
        col("restaurant_id"),
        col("item_category"),
        col("item_name"),
        col("item_price"),
        col("item_quantity")
    )
).withColumn("flagged", col("severity") != lit("none"))

# Step 3: Join with options
order_item_options_cleaned = order_item_options.drop("order_id")  # Drop to avoid ambiguity
joined_df_1 = order_items_flagged.alias("items").join(
    order_item_options_cleaned.alias("options"),
    on="lineitem_id",
    how="left"
)

# Step 4: Rearrange + keep flagged/severity
# NOTE: Include `severity`, `flagged` if needed in your expected_columns_2
expected_columns_2 += ["severity", "flagged"]  # Append if not already present
joined_df_1 = rearrange_columns(joined_df_1, expected_columns_2)

# Step 5: Fill option nulls
df_filled = joined_df_1.fillna({
    "option_price": 0.0,
    "option_quantity": 0,
    "option_group_name": "N/A",
    "option_name": "N/A"
})

# Step 6: Join with date dim (this should NOT remove columns from earlier)
joined_df_2 = df_filled.alias("items_option").join(
    date_dim.alias("dates"),
    on="date_key",
    how="left"
)

joined_df_2 = enhance_dim_date(joined_df_2)

# Step 7: Final rearrangement (include all expected + flagging)
expected_columns_3 += ["severity", "flagged"]  # Add these too
joined_df_3 = rearrange_columns(joined_df_2, expected_columns_3)



# Step 1: Write ONLY high severity violations to QA_PATH
joined_df_3.filter(col("severity") == "high") \
    .write.mode("overwrite").parquet(QA_PATH)

# Step 2: Write everything else to FINAL_PATH (non-high severity)
joined_df_3.filter(col("severity") != "high") \
    .write.mode("overwrite").parquet(FINAL_PATH)

job.commit()
