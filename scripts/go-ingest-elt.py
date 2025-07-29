import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

from awsglue.job import Job
from pyspark.sql import functions as F

# -----------------------
# Job Arguments
# -----------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "LOAD_PATH", "ERROR_PATH", "TABLES_INGEST"])
LOAD_PATH = args["LOAD_PATH"]
ERROR_PATH = args["ERROR_PATH"]
TABLES_INGEST = [t.strip() for t in args["TABLES_INGEST"].split(",")]

# -----------------------
# Glue Setup
# -----------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------
# Read Table Function
# -----------------------
def read_table(table_name):
    try:
        print(f"Reading table: {table_name}")
        df = glueContext.create_dynamic_frame.from_options(
            connection_type="sqlserver",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": table_name,
                "connectionName": "custom-sqlserver-connection"  # Glue connection handles SecretsManager
            },
            transformation_ctx=f"read_{table_name.replace('dbo.', '')}"
        )
        return df
    except Exception as e:
        log_error(f"Failed to read table {table_name}: {str(e)}")
        return None

# -----------------------
# Write to S3 Function
# -----------------------


def write_parquet_to_s3(dynamic_frame, table_name):
    try:
        df = dynamic_frame.toDF().withColumn("source_table", F.lit(table_name))
        df_dyf = DynamicFrame.fromDF(df, glueContext, f"{table_name}_with_source")

        output_path = f"{LOAD_PATH}{table_name.replace('dbo.', '')}/"

        glueContext.write_dynamic_frame.from_options(
            frame=df_dyf,
            connection_type="s3",
            format="parquet",
            connection_options={"path": output_path},
            format_options={"compression": "snappy"}
        )
        print(f"Wrote table {table_name} to {output_path}")
    except Exception as e:
        log_error(f"Failed to write {table_name}: {str(e)}")



# -----------------------
# Error Logging Function
# -----------------------
def log_error(message):
    print(message)
    try:
        import boto3
        s3 = boto3.client("s3")
        bucket, key_prefix = ERROR_PATH.replace("s3://", "").split("/", 1)
        s3.put_object(
            Bucket=bucket,
            Key=f"{key_prefix}ingest-error.log",
            Body=message.encode("utf-8")
        )
    except Exception as e:
        print(f"Failed to write error log to S3: {str(e)}")

# -----------------------
# Main ETL Loop
# -----------------------
for table in TABLES_INGEST:
    dynamic_frame = read_table(table)
    if dynamic_frame and dynamic_frame.count() > 0:
        write_parquet_to_s3(dynamic_frame, table)
    else:
        print(f"Table {table} is empty or unreadable.")

job.commit()
