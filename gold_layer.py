import pandas as pd
import uuid
from astrapy import DataAPIClient

# ---------------------------
# ✅ Configuration
# ---------------------------
ASTRA_DB_API_ENDPOINT = "https://df120433-b842-4f06-9539-9f482061d533-us-east-2.apps.astra.datastax.com"
ASTRA_DB_TOKEN = "AstraCS:NTTSPYuEmZibWIsOOuLLwNtZ:383626916e320bf00fa8457b714b0db85ad63d49ed3f629dd7e4599dc9ed361f"

# ---------------------------
# ✅ Initialize Astra DB Client
# ---------------------------
client = DataAPIClient(ASTRA_DB_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_API_ENDPOINT)

# ---------------------------
# ✅ Load CSV Data
# ---------------------------
csv_path = "./data/sales_100.csv"  # Ensure the correct path to your CSV file
df = pd.read_csv(csv_path)
df.columns = df.columns.str.strip()  # Strip spaces from columns

# ---------------------------
# ✅ Create Gold Tables
# ---------------------------
gold_table_1 = "gold_revenue_by_region"
gold_table_2 = "gold_revenue_by_country"
gold_table_3 = "gold_units_sold_by_item_type"

# Create the Gold collections (only if they don't exist)
if gold_table_1 not in db.list_collection_names():
    db.create_collection(gold_table_1)
if gold_table_2 not in db.list_collection_names():
    db.create_collection(gold_table_2)
if gold_table_3 not in db.list_collection_names():
    db.create_collection(gold_table_3)

# ---------------------------
# ✅ Aggregate Data for Gold Tables
# ---------------------------
# Gold Table 1: Total revenue by region
gold_data_1 = df.groupby("Region")["TotalRevenue"].sum().reset_index()
for _, row in gold_data_1.iterrows():
    doc = {
        "_id": str(uuid.uuid4()),
        "region": row["Region"],
        "total_revenue": row["TotalRevenue"]
    }
    db.get_collection(gold_table_1).insert_one(doc)

# Gold Table 2: Total revenue by country
gold_data_2 = df.groupby("Country")["TotalRevenue"].sum().reset_index()
for _, row in gold_data_2.iterrows():
    doc = {
        "_id": str(uuid.uuid4()),
        "country": row["Country"],
        "total_revenue": row["TotalRevenue"]
    }
    db.get_collection(gold_table_2).insert_one(doc)

# Gold Table 3: Total units sold by item type
gold_data_3 = df.groupby("Item Type")["UnitsSold"].sum().reset_index()
for _, row in gold_data_3.iterrows():
    doc = {
        "_id": str(uuid.uuid4()),
        "item_type": row["Item Type"],
        "total_units_sold": row["UnitsSold"]
    }
    db.get_collection(gold_table_3).insert_one(doc)

print("✅ Aggregated data inserted into Gold tables.")
