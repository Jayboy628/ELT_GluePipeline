import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, lit, when, count, avg, max as spark_max, min, sum as spark_sum,
    datediff, coalesce, lag, first, to_date, to_timestamp, hour,
    weekofyear, year, month, date_format, concat_ws, expr, row_number, countDistinct
)
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# ---------------------------
# 1. Job Initialization
# ---------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "FINAL_PATH", "ERROR_PATH"
])
FINAL_PATH = args["FINAL_PATH"]
ERROR_PATH = args["ERROR_PATH"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------
# 2. Load Order Items Data
# ---------------------------
df_order_items = spark.read.parquet(f"{FINAL_PATH}")


df_order_items = df_order_items.withColumnRenamed("user_id", "customer_id")
# ---------------------------
# 3. Revenue per Order Calculation
# ---------------------------
df_order_items = df_order_items.withColumn(
    "revenue_per_order",
    (coalesce(col("option_price"), lit(0)) * coalesce(col("option_quantity"), lit(0))) +
    (coalesce(col("item_price"), lit(0)) * coalesce(col("item_quantity"), lit(1)))
)

# ---------------------------
# 4. CLV Aggregation
# ---------------------------
df_clv = df_order_items.filter(col("customer_id") != "_guest").groupBy(
    "restaurant_id", "customer_id", "date_key", "is_loyalty"
).agg(
    spark_sum("revenue_per_order").alias("total_revenue"),
    count("order_id").alias("total_orders"),
    spark_max("date").alias("max_date"),
    avg("revenue_per_order").alias("avg_revenue_per_order"),
    min("revenue_per_order").alias("min_revenue_per_order")
)

df_clv.write.mode("overwrite").parquet("s3a://gp-elt-657082399901-dev/metrics/revenue_per_order/")

# ---------------------------
# 5. Tag CLV Buckets by Restaurant
# ---------------------------
def tag_clv_by_restaurant_spark(df: DataFrame, clv_col="total_revenue") -> DataFrame:
    restaurant_ids = [r["restaurant_id"] for r in df.select("restaurant_id").distinct().collect()]
    thresholds = [
        (rid, *df.filter(col("restaurant_id") == rid).approxQuantile(clv_col, [0.2, 0.8], 0.01))
        for rid in restaurant_ids
    ]
    threshold_df = spark.createDataFrame(thresholds, ["restaurant_id", "low_threshold", "high_threshold"])
    return df.join(threshold_df, "restaurant_id").withColumn(
        "clv_tag",
        when(col(clv_col) >= col("high_threshold"), "High CLV")
        .when(col(clv_col) <= col("low_threshold"), "Low CLV")
        .otherwise("Medium CLV")
    ).drop("low_threshold", "high_threshold")

tagged_df = tag_clv_by_restaurant_spark(df_clv)
tagged_df.write.mode("overwrite").parquet("s3a://gp-elt-657082399901-dev/metrics/clv_tagged/")

# ---------------------------
# 6. RFM Analysis and Segmentation
# ---------------------------
def rfm_analysis(df):
    snapshot_date = df.agg(spark_max("date").alias("snapshot")).collect()[0]["snapshot"]
    return df.groupBy("restaurant_id", "customer_id").agg(
        datediff(lit(snapshot_date), spark_max("date")).alias("recency"),
        count("*").alias("frequency"),
        spark_sum("revenue_per_order").alias("monetary"),
        first("is_loyalty").alias("is_loyalty")
    )

def segment_customers(df):
    return df.withColumn(
        "segment",
        when((col("recency") <= 180) & (col("frequency") > 50) & (col("monetary") > 500), "VIP")
        .when((col("recency") <= 180) & (col("frequency") <= 50) & (col("monetary") > 200), "New Customer")
        .when((col("recency") > 180) & (col("frequency") <= 5) & (col("monetary") <= 1000), "Churn Risk")
        .otherwise("Other")
    )

rfm_df = rfm_analysis(df_order_items.filter(col("customer_id") != "_guest"))
segmented_df = segment_customers(rfm_df)
segmented_df.write.mode("overwrite").parquet("s3a://gp-elt-657082399901-dev/metrics/rfm_segmented/")

# ---------------------------
# 7. Customer Activity Profile
# ---------------------------
def customer_activity_profile(df):
    df = df.filter(col("customer_id") != "_guest")
    window_spec = Window.partitionBy("restaurant_id", "customer_id").orderBy("date")

    df = df.withColumn("order_gap", datediff(col("date"), lag("date").over(window_spec)))
    df = df.withColumn("prev_revenue", lag("revenue_per_order").over(window_spec))
    df = df.withColumn("revenue_change_pct", 
        when(col("prev_revenue").isNotNull(), 
             (col("revenue_per_order") - col("prev_revenue")) / col("prev_revenue"))
    )

    snapshot = df.agg(spark_max("date").alias("max_date")).collect()[0]["max_date"]

    profile = df.groupBy("restaurant_id", "customer_id").agg(
        spark_max("date").alias("last_order_date"),
        avg("order_gap").alias("avg_gap_days"),
        avg("revenue_change_pct").alias("avg_pct_change")
    ).withColumn(
        "days_since_last_order", datediff(lit(snapshot), col("last_order_date"))
    ).withColumn(
        "recency_tag", when(col("days_since_last_order") > 45, "inactive").otherwise("active")
    ).withColumn(
        "gap_tag", when(col("avg_gap_days") > 45, "at risk").otherwise("healthy")
    ).withColumn(
        "activity_tag", when((col("recency_tag") == "inactive") | (col("gap_tag") == "at risk"), "at risk").otherwise("active")
    )

    return profile

activity_df = customer_activity_profile(df_order_items)
activity_df.write.mode("overwrite").partitionBy("restaurant_id").parquet("s3a://gp-elt-657082399901-dev/metrics/customer_profile/")

# ---------------------------
# 8. Sales Trends Monitoring
# ---------------------------
from pyspark.sql.functions import lpad

def sales_trends(df, output_path):
    df = df.withColumn("date", to_date("date")).withColumn(
        "revenue",
        coalesce(col("item_price"), lit(0.0)) * coalesce(col("item_quantity"), lit(0)) +
        coalesce(col("option_price"), lit(0.0)) * coalesce(col("option_quantity"), lit(0))
    ).withColumn("year", year("date")).withColumn("month", date_gformat("date", "MMMM")) \
     .withColumn("week", weekofyear("date")) \
     .withColumn("hour", hour(to_timestamp("time", "HH:mm:ss"))) \
     .withColumn("day", col("day_of_weeks"))

    # Daily
    daily = df.groupBy("year", "day", "date", "restaurant_id", "item_category") \
        .agg(spark_sum("revenue").alias("revenue")).withColumn("granularity", lit("daily"))

    # Weekly
    weekly = df.groupBy("year", "week", "restaurant_id", "item_category") \
        .agg(spark_sum("revenue").alias("revenue")) \
        .withColumn("date", expr("date_add(to_date(concat(year, '-01-01')), (week - 1) * 7)")) \
        .withColumn("granularity", lit("weekly"))

    # Monthly
    monthly = df.groupBy("year", "month", "restaurant_id", "item_category") \
        .agg(spark_sum("revenue").alias("revenue")) \
        .withColumn("month_num", month(to_date(concat_ws(" ", "month", "year"), "MMMM yyyy"))) \
        .withColumn("date", to_date(concat_ws("-", "year", "month_num", lit("01")))) \
        .withColumn("granularity", lit("monthly"))

    # Hourly
    hourly = df.groupBy("year", "hour", "day", "date", "restaurant_id", "item_category") \
        .agg(spark_sum("revenue").alias("revenue")).withColumn("granularity", lit("hourly"))

    daily.write.mode("overwrite").partitionBy("restaurant_id").parquet(f"{output_path}daily/")
    weekly.write.mode("overwrite").partitionBy("restaurant_id").parquet(f"{output_path}weekly/")
    monthly.write.mode("overwrite").partitionBy("restaurant_id").parquet(f"{output_path}monthly/")
    hourly.write.mode("overwrite").partitionBy("restaurant_id").parquet(f"{output_path}hourly/")

sales_trends(df_order_items, "s3a://gp-elt-657082399901-dev/metrics/sales_trends/")

# ---------------------------
# 9. Loyalty Program Impact
# ---------------------------
loyal_df = df_order_items.filter(col("customer_id") != "_guest").groupBy(
    "restaurant_id", "is_loyalty", "customer_id"
).agg(
    avg("revenue_per_order").alias("average_spend"),
    count("customer_id").alias("repeat_orders"),
    spark_sum("revenue_per_order").alias("lifetime_value")
)

loyal_df.write.mode("overwrite").parquet("s3a://gp-elt-657082399901-dev/metrics/loyalty_program_impact/")

loyal_summary = loyal_df.groupBy("restaurant_id", "is_loyalty").agg(
    avg("average_spend").alias("avg_spend_per_customer"),
    avg("repeat_orders").alias("avg_repeat_orders"),
    avg("lifetime_value").alias("avg_lifetime_value")
)

loyal_summary.write.mode("overwrite").parquet("s3a://gp-elt-657082399901-dev/metrics/loyalty_program_impact_summary/")

# ---------------------------
# 10. Top Performing Locations
# ---------------------------
top_locations = df_order_items.groupBy("restaurant_id").agg(
    spark_sum("revenue_per_order").alias("total_revenue"),
    avg("revenue_per_order").alias("average_order_value"),
    countDistinct("date").alias("active_days"),
    countDistinct("order_id").alias("total_orders")
).withColumn(
    "orders_per_day", col("total_orders") / col("active_days")
).withColumn(
    "rank", row_number().over(Window.orderBy(col("total_revenue").desc()))
)

top_locations.write.mode("overwrite").parquet("s3a://gp-elt-657082399901-dev/metrics/top_locations/")

# ---------------------------
# 11. Discount Effectiveness
# ---------------------------
df_discount = df_order_items.withColumn("is_discounted", when(col("option_price") < 0, True).otherwise(False))

discount_summary = df_discount.groupBy("restaurant_id", "is_discounted").agg(
    spark_sum("revenue_per_order").alias("total_revenue"),
    countDistinct("order_id").alias("total_orders"),
    avg("revenue_per_order").alias("avg_order_value")
)

discount_summary.write.mode("overwrite").parquet("s3a://gp-elt-657082399901-dev/metrics/discount_effectiveness/")

# ---------------------------
# Done
# ---------------------------
job.commit()
