import pandas as pd
import boto3
import pyodbc
import os
import json

def get_secret():
    secret_name = "connection_parameters_sqlserver-dev"
    region_name = "us-east-1"

    client = boto3.client("secretsmanager", region_name=region_name)
    secret_value = client.get_secret_value(SecretId=secret_name)
    return json.loads(secret_value['SecretString'])

def load_csv_to_sql(table_name, csv_path, connection_string):
    df = pd.read_csv(csv_path)

    with pyodbc.connect(connection_string, autocommit=True) as conn:
        cursor = conn.cursor()
        for index, row in df.iterrows():
            columns = ','.join(df.columns)
            placeholders = ','.join(['?'] * len(row))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, *row)

if __name__ == "__main__":
    secret = get_secret()
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={secret['host']},{secret['port']};"
        f"DATABASE={secret['dbname']};UID={secret['username']};"
        f"PWD={secret['password']}"
    )

    files = {
        "order_items": "dataset/order_items.csv",
        "order_item_options": "dataset/order_item_options.csv",
        "date_dim": "dataset/date_dim.csv"
    }

    for table, path in files.items():
        print(f"Loading {path} into {table}")
        load_csv_to_sql(table, path, conn_str)
