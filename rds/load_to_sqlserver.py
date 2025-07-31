import boto3
import base64
import json
import pandas as pd
import pyodbc
import os
import time
from botocore.exceptions import ClientError

def get_secret():
    secret_name = "connection_parameters_sqlserver-dev"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        return secret
    except ClientError as e:
        print(f"Error fetching secret: {e}")
        raise e

def load_csv_to_sql(cursor, table_name, df):
    # Create table schema dynamically (basic example)
    cols = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ({cols})")
    
    # Clear existing data (optional)
    cursor.execute(f"DELETE FROM {table_name}")

    # Insert rows
    for index, row in df.iterrows():
        placeholders = ','.join(['?' for _ in row])
        sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(sql, tuple(row))

# def main():
#     try:
#         secret = get_secret()
#         print("Secret loaded.")
#         print("Connecting to SQL Server at:")
#         print(f"    Host: {secret['host']}")
#         print(f"    Port: {secret['port']}")
#         print(f"    DB:   {secret['dbname']}")

#         conn_str = (
#             f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#             f"SERVER={secret['host']},{secret['port']};"
#             f"DATABASE={secret['dbname']};"
#             f"UID={secret['username']};"
#             f"PWD={secret['password']}"
#         )

def main():
    try:
        # ✅ Hardcoded connection values (for testing only)
        host = "gp-sqlserver-dev.ccetvexupnnw.us-east-1.rds.amazonaws.com"
        port = 1433
        dbname = "globalpartners"
        username = "sqladmin"
        password = "SqlPaSS2025"

        print("✅ Using hardcoded SQL Server credentials.")
        print("🔌 Connecting to SQL Server at:")
        print(f"    Host: {host}")
        print(f"    Port: {port}")
        print(f"    DB:   {dbname}")

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host},{port};"
            f"DATABASE={dbname};"
            f"UID={username};"
            f"PWD={password}"
        )


        # Retry logic for connection
        conn = None
        for attempt in range(3):
            try:
                conn = pyodbc.connect(conn_str, timeout=10)
                print("Connected to SQL Server.")
                break
            except Exception as e:
                print(f"Connection attempt {attempt + 1} failed: {e}")
                time.sleep(5)
        if conn is None:
            raise Exception("Failed to connect after 3 attempts.")

        cursor = conn.cursor()

        # Dataset directory
        dataset_path = "dataset"
        files = {
            "order_items": "order_items.csv",
            "order_item_options": "order_item_options.csv",
            "date_dim": "date_dim.csv"
        }

        for table, file in files.items():
            full_path = os.path.join(dataset_path, file)
            print(f"Loading {file} into table [{table}]...")
            df = pd.read_csv(full_path)
            load_csv_to_sql(cursor, table, df)
            conn.commit()
            print(f"Loaded {len(df)} rows into [{table}].")

        cursor.close()
        conn.close()
        print("All tables loaded successfully.")

    except Exception as e:
        print("Script failed due to error:")
        print(e)
        exit(1)

if __name__ == "__main__":
    main()
