import pandas as pd
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# ----------------------------------
# CONFIGURE ASTRA DB CONNECTION
# ----------------------------------
ASTRA_DB_ID = '<your-db-id>'
ASTRA_DB_REGION = '<your-db-region>'
ASTRA_DB_KEYSPACE = 'sales_keyspace'

# Path to the secure connect bundle (unzip it first)
SECURE_BUNDLE_PATH = os.path.join(os.getcwd(), 'secure-connect')

cloud_config = {
    'secure_connect_bundle': SECURE_BUNDLE_PATH
}

auth_provider = PlainTextAuthProvider('your_db_username', 'your_db_password')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# ----------------------------------
# CREATE KEYSPACE & TABLES
# ----------------------------------

# Create keyspace
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {ASTRA_DB_KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
""")
session.set_keyspace(ASTRA_DB_KEYSPACE)

# Bronze table - raw
session.execute("""
    CREATE TABLE IF NOT EXISTS bronze_sales (
        id UUID PRIMARY KEY,
        region TEXT,
        product TEXT,
        quantity INT,
        price DOUBLE,
        date TEXT
    );
""")

# Silver table - cleaned (date converted)
session.execute("""
    CREATE TABLE IF NOT EXISTS silver_sales (
        id UUID PRIMARY KEY,
        region TEXT,
        product TEXT,
        quantity INT,
        price DOUBLE,
        date DATE
    );
""")

# Gold 1: Total sales per region
session.execute("""
    CREATE TABLE IF NOT EXISTS gold_sales_by_region (
        region TEXT,
        total_sales DOUBLE,
        PRIMARY KEY (region)
    );
""")

# Gold 2: Top-selling products
session.execute("""
    CREATE TABLE IF NOT EXISTS gold_top_products (
        product TEXT,
        total_quantity INT,
        PRIMARY KEY (product)
    );
""")

# Gold 3: Monthly trends
session.execute("""
    CREATE TABLE IF NOT EXISTS gold_monthly_sales (
        month TEXT,
        total_sales DOUBLE,
        PRIMARY KEY (month)
    );
""")

# ----------------------------------
# LOAD CSV INTO BRONZE
# ----------------------------------

df = pd.read_csv('./data/sales_100.csv')

import uuid
from datetime import datetime

print("Inserting into Bronze Table...")
for _, row in df.iterrows():
    session.execute("""
        INSERT INTO bronze_sales (id, region, product, quantity, price, date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        uuid.uuid4(),
        row['region'],
        row['product'],
        int(row['quantity']),
        float(row['price']),
        row['date']
    ))

# ----------------------------------
# TRANSFORM DATA INTO SILVER
# ----------------------------------

print("Transforming and inserting into Silver Table...")
for _, row in df.iterrows():
    session.execute("""
        INSERT INTO silver_sales (id, region, product, quantity, price, date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        uuid.uuid4(),
        row['region'],
        row['product'],
        int(row['quantity']),
        float(row['price']),
        datetime.strptime(row['date'], '%Y-%m-%d').date()
    ))

# ----------------------------------
# AGGREGATE & LOAD GOLD TABLES
# ----------------------------------

print("Generating Gold Table 1: Total Sales by Region...")
gold1 = df.groupby('region').apply(lambda x: (x['quantity'] * x['price']).sum()).reset_index()
gold1.columns = ['region', 'total_sales']
for _, row in gold1.iterrows():
    session.execute("""
        INSERT INTO gold_sales_by_region (region, total_sales)
        VALUES (%s, %s)
    """, (row['region'], float(row['total_sales'])))

print("Generating Gold Table 2: Top Selling Products...")
gold2 = df.groupby('product')['quantity'].sum().reset_index()
gold2.columns = ['product', 'total_quantity']
for _, row in gold2.iterrows():
    session.execute("""
        INSERT INTO gold_top_products (product, total_quantity)
        VALUES (%s, %s)
    """, (row['product'], int(row['total_quantity'])))

print("Generating Gold Table 3: Monthly Sales Trends...")
df['month'] = pd.to_datetime(df['date']).dt.to_period('M').astype(str)
gold3 = df.groupby('month').apply(lambda x: (x['quantity'] * x['price']).sum()).reset_index()
gold3.columns = ['month', 'total_sales']
for _, row in gold3.iterrows():
    session.execute("""
        INSERT INTO gold_monthly_sales (month, total_sales)
        VALUES (%s, %s)
    """, (row['month'], float(row['total_sales'])))

print("✅ All data inserted!")
