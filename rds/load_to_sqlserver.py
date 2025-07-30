import pyodbc
import pandas as pd
import os
import traceback
import boto3
import json

def get_secret():
    secret_name = "connection_parameters_sqlserver-dev"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response["SecretString"])
        return secret
    except Exception as e:
        print("❌ Error fetching secret:", e)
        raise

def load_csv_to_sqlserver(cursor, conn, csv_path, table_name):
    try:
        df = pd.read_csv(csv_path)
        print(f"✅ Loaded {len(df)} rows from {csv_path}")

        # Optional: Drop and recreate table for simplicity
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        create_stmt = f"CREATE TABLE {table_name} ({', '.join([f'[{col}] NVARCHAR(MAX)' for col in df.columns])})"
        cursor.execute(create_stmt)

        # Insert data row by row (for simplicity)
        for _, row in df.iterrows():
            values = "', '".join([str(val).replace("'", "''") for val in row.tolist()])
            cursor.execute(f"INSERT INTO {table_name} VALUES ('{values}')")

        conn.commit()
        print(f"✅ Inserted {len(df)} rows into {table_name}")
    except Exception as e:
        print(f"❌ Failed loading {csv_path} into {table_name}: {e}")
        traceback.print_exc()
        raise

def main():
    try:
        secret = get_secret()

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={secret['host']},{secret['port']};"
            f"DATABASE=master;"
            f"UID={secret['username']};"
            f"PWD={secret['password']}"
        )

        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        print("✅ Connected to SQL Server")

        # Create globalpartners DB if not exists
        cursor.execute("IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'globalpartners') CREATE DATABASE globalpartners")
        cursor.execute("USE globalpartners")

        # Load files
        load_csv_to_sqlserver(cursor, conn, "dataset/order_items.csv", "order_items")
        load_csv_to_sqlserver(cursor, conn, "dataset/order_item_options.csv", "order_item_options")
        load_csv_to_sqlserver(cursor, conn, "dataset/date_dim.csv", "date_dim")

        print("✅ All data loaded successfully!")

    except Exception as err:
        print("🚨 Script failed due to error:")
        traceback.print_exc()
        exit(1)  # Force GitHub Action to fail
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

if __name__ == "__main__":
    main()
