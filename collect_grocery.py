import os
import pandas as pd
import requests
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME   = os.getenv("DB_NAME", "fooddesert")

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db     = client[DB_NAME]

# ── Load crosswalk ──────────────────────────────────────────
crosswalk_df = pd.DataFrame(list(db["crosswalk"].find())).drop(columns=["_id"], errors="ignore")
NYC_ZIPS     = crosswalk_df["zip_code"].tolist()

# ── NYC county filter ───────────────────────────────────────
NYC_COUNTIES = "('NEW YORK','KINGS','QUEENS','BRONX','RICHMOND')"

COUNTY_TO_BOROUGH = {
    "NEW YORK": "Manhattan",
    "KINGS":    "Brooklyn",
    "QUEENS":   "Queens",
    "BRONX":    "Bronx",
    "RICHMOND": "Staten Island",
}

# ── Membership/wholesale store names ───────────────────────
MEMBERSHIP_STORES = {
    "COSTCO", "BJ'S", "BJS", "SAM'S CLUB", "SAMS CLUB",
    "WHOLESALE CLUB", "COSTCO WHOLESALE"
}

# ── Fetch ───────────────────────────────────────────────────
print("📡 Fetching NY State Retail Food Store data...\n")

all_records = []
offset      = 0
batch_size  = 1000

while True:
    params = {
        "$limit":  batch_size,
        "$offset": offset,
        "$where":  f"county IN {NYC_COUNTIES} AND square_footage IS NOT NULL",
        "$select": "county,dba_name,entity_name,estab_type,zip_code,square_footage,georeference",
    }
    resp = requests.get(
        "https://data.ny.gov/resource/9a8c-vfzj.json",
        params=params,
        timeout=30
    )
    if resp.status_code != 200:
        print(f"   ⚠️  Fetch error {resp.status_code}: {resp.text[:200]}")
        break

    batch = resp.json()
    if not batch:
        break

    all_records.extend(batch)
    offset += batch_size
    print(f"   ... {len(all_records):,} records fetched")

    if len(batch) < batch_size:
        break

print(f"\n✅ Total records: {len(all_records):,}")

# ── Clean ───────────────────────────────────────────────────
grocery_df = pd.DataFrame(all_records)

# Normalize ZIP
grocery_df["zip_code"] = (
    grocery_df["zip_code"].astype(str).str.strip().str.zfill(5).str[:5]
)

# Filter to our NYC ZIPs
grocery_df = grocery_df[grocery_df["zip_code"].isin(NYC_ZIPS)].copy()
print(f"✅ After ZIP filter: {len(grocery_df):,} records")

# Extract lat/lon from georeference
def extract_coords(geo):
    try:
        coords = geo["coordinates"]
        return coords[1], coords[0]  # lat, lon
    except:
        return None, None

grocery_df["latitude"]  = grocery_df["georeference"].apply(lambda g: extract_coords(g)[0] if isinstance(g, dict) else None)
grocery_df["longitude"] = grocery_df["georeference"].apply(lambda g: extract_coords(g)[1] if isinstance(g, dict) else None)
grocery_df.drop(columns=["georeference"], inplace=True)

# Borough from county
grocery_df["borough"] = grocery_df["county"].map(COUNTY_TO_BOROUGH)

# Square footage as numeric
grocery_df["square_footage"] = pd.to_numeric(grocery_df["square_footage"], errors="coerce")

# Store name for classification
grocery_df["store_name"] = grocery_df["dba_name"].fillna(grocery_df["entity_name"]).str.upper().str.strip()

# ── Classify store type ─────────────────────────────────────
def classify_store(row):
    name = row["store_name"] if isinstance(row["store_name"], str) else ""
    sqft = row["square_footage"]

    # Membership/wholesale stores
    if any(m in name for m in MEMBERSHIP_STORES):
        return "membership"

    # Size-based classification
    if pd.notna(sqft):
        if sqft >= 10000:
            return "supermarket"
        elif sqft >= 2000:
            return "mid_grocery"
        else:
            return "small_grocery"

    return "small_grocery"

grocery_df["store_type"] = grocery_df.apply(classify_store, axis=1)

# Join crosswalk for neighborhood
grocery_df.rename(columns={"borough": "borough_county"}, inplace=True)
grocery_df = grocery_df.merge(crosswalk_df, on="zip_code", how="left")
grocery_df["borough"] = grocery_df["borough"].fillna(grocery_df["borough_county"])
grocery_df.drop(columns=["borough_county"], inplace=True)

# ── Summary ─────────────────────────────────────────────────
print(f"\nStore type breakdown:")
print(grocery_df["store_type"].value_counts().to_string())

print(f"\nBorough breakdown:")
print(grocery_df["borough"].value_counts().to_string())

# ── Store to MongoDB ────────────────────────────────────────
col = db["grocery_nonsmap"]
col.drop()
col.insert_many(grocery_df.where(pd.notna(grocery_df), None).to_dict(orient="records"))

print(f"\n💾 grocery_nonsnap: {col.count_documents({}):,} documents stored")
print(f"   Neighborhoods covered: {grocery_df['neighborhood'].nunique()}")
print(f"   ZIP codes covered:     {grocery_df['zip_code'].nunique()}")
print(f"\n✅ collect_grocery.py complete — next: collect_pantries.py")