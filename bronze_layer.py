import pandas as pd
import uuid
from astrapy import DataAPIClient

# ---------------------------
# ✅ Configuration
# ---------------------------
ASTRA_DB_API_ENDPOINT = "https://df120433-b842-4f06-9539-9f482061d533-us-east-2.apps.astra.datastax.com"
ASTRA_DB_TOKEN = "AstraCS:SrYQyWZxKCWzSEPotDCEXlYA:3e4404e3797e6d92c8f2d0b8da9b67214c2111321028b12a993afc0cfbe7d0f9" # <-- Paste your generated token here

# ---------------------------
# ✅ Initialize Astra DB Client
# ---------------------------
client = DataAPIClient(ASTRA_DB_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_API_ENDPOINT)

# ---------------------------
# ✅ Create / Connect to Bronze Collection
# ---------------------------
collection_name = "bronze_sales"

# Create the collection (only if it doesn't exist)
if collection_name not in db.list_collection_names():
    bronze_collection = db.create_collection(collection_name)
    print(f"Collection '{collection_name}' created.")
else:
    bronze_collection = db.get_collection(collection_name)
    print(f"Collection '{collection_name}' already exists. Using existing collection.")

# ---------------------------
# ✅ Load CSV Data
# ---------------------------
csv_path = "./data/sales_100.csv"  # Make sure this file exists at this path
df = pd.read_csv(csv_path)

# Strip any leading/trailing spaces from column names
df.columns = df.columns.str.strip()

# Print column names to verify
print(f"📦 Loaded {len(df)} rows from CSV. Column names: {df.columns}")

# ---------------------------
# ✅ Insert into Bronze Collection
# ---------------------------
inserted = 0
for _, row in df.iterrows():
    doc = {
        "_id": str(uuid.uuid4()),  # required unique ID
        "region": row["Region"],  # Updated column name
        "country": row["Country"],  # Updated column name
        "item_type": row["Item Type"],  # Updated column name
        "sales_channel": row["Sales Channel"],  # Updated column name
        "order_priority": row["Order Priority"],  # Updated column name
        "order_date": row["Order Date"],  # Updated column name
        "order_id": row["Order ID"],  # Updated column name
        "ship_date": row["Ship Date"],  # Updated column name
        "units_sold": row["UnitsSold"],  # Updated column name
        "unit_price": row["UnitPrice"],  # Updated column name
        "unit_cost": row["UnitCost"],  # Updated column name
        "total_revenue": row["TotalRevenue"],  # Updated column name
        "total_cost": row["TotalCost"],  # Updated column name
        "total_profit": row["TotalProfit"]  # Updated column name
    }
    bronze_collection.insert_one(doc)
    inserted += 1

print(f"✅ Inserted {inserted} documents into Bronze Collection.")
