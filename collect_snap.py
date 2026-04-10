import os
import io
import zipfile
import pandas as pd
import requests
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME   = "fooddesert"

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db     = client[DB_NAME]

# ── Load crosswalk ──────────────────────────────────────────
crosswalk_df = pd.DataFrame(list(db["crosswalk"].find())).drop(columns=["_id"], errors="ignore")
NYC_ZIPS     = crosswalk_df["zip_code"].tolist()

LOCAL_CSV_PATH = os.path.join(os.path.dirname(__file__), "snap_retailers.csv")

ARCGIS_ENDPOINTS = [
    "https://services1.arcgis.com/RLQu0rK7h4kbsBq5/arcgis/rest/services/SNAP_Retailer_Locator/FeatureServer/0/query",
    "https://geodata.fns.usda.gov/arcgis/rest/services/SNAP/SNAP_Retailer_Locator/FeatureServer/0/query",
]

def fetch_from_arcgis(endpoint):
    all_records = []
    offset      = 0
    batch_size  = 1000

    try:
        while True:
            params = {
                "where":             "State='NY'",
                "outFields":         "Store_Name,Store_Type,Address,City,State,Zip5,County,Longitude,Latitude",
                "returnGeometry":    "false",
                "f":                 "json",
                "resultOffset":      offset,
                "resultRecordCount": batch_size,
            }
            resp = requests.get(endpoint, params=params, timeout=30)
            if resp.status_code != 200:
                return None, f"HTTP {resp.status_code}"

            data     = resp.json()
            if "error" in data:
                return None, data["error"].get("message", "Unknown error")

            features = data.get("features", [])
            if not features:
                break

            for f in features:
                all_records.append(f["attributes"])

            offset += batch_size
            if not data.get("exceededTransferLimit", False):
                break

        return all_records, None

    except Exception as e:
        return None, str(e)


snap_df = None

print("📡 Fetching SNAP retailer data from USDA FNS...\n")

for endpoint in ARCGIS_ENDPOINTS:
    print(f"   Trying: {endpoint}")
    records, err = fetch_from_arcgis(endpoint)
    if records:
        print(f"   ✅ Got {len(records):,} NY state records")
        snap_df = pd.DataFrame(records)
        snap_df["zip_code"] = snap_df["Zip5"].astype(str).str.strip().str.zfill(5)
        break
    else:
        print(f"   ⚠️  Failed: {err}")

if snap_df is None:
    if os.path.exists(LOCAL_CSV_PATH):
        print(f"\n📂 Found local CSV: {LOCAL_CSV_PATH}")
        snap_df = pd.read_csv(LOCAL_CSV_PATH, dtype=str, encoding="latin-1")
        snap_df.columns = [c.strip() for c in snap_df.columns]

        zip_col = next(
            (c for c in snap_df.columns
             if c.lower().replace(" ", "").replace("_", "") in ("zip5", "zipcode", "zip")),
            None
        )
        state_col = next(
            (c for c in snap_df.columns if c.lower() in ("state", "st")), None
        )

        print(f"   Columns detected: {list(snap_df.columns)}")
        print(f"   ZIP col: {zip_col} | State col: {state_col}")

        if state_col:
            snap_df = snap_df[snap_df[state_col].str.strip().str.upper() == "NY"].copy()
            print(f"   NY records: {len(snap_df):,}")

        if zip_col:
            snap_df["zip_code"] = snap_df[zip_col].astype(str).str.strip().str.zfill(5).str[:5]
        else:
            print("❌ Could not find ZIP column in CSV. Check column names above.")
            exit(1)

        type_col = next((c for c in snap_df.columns if "type" in c.lower()), None)
        name_col = next((c for c in snap_df.columns if "name" in c.lower() and "store" in c.lower()), snap_df.columns[0])
        if type_col:
            snap_df["Store_Type"] = snap_df[type_col].str.strip()
        if name_col:
            snap_df["Store_Name"] = snap_df[name_col].str.strip()
    else:
        print("\n❌ All automatic fetch methods failed.")
        print("   Please download the CSV manually:")
        print("   1. Go to: https://usda-snap-retailers-usda-fns.hub.arcgis.com/datasets/8b260f9a10b0459aa441ad8588c2251c/explore")
        print("   2. Click Download → CSV")
        print(f"   3. Save as: {LOCAL_CSV_PATH}")
        print("   4. Re-run this script")
        exit(1)

snap_df = snap_df[snap_df["zip_code"].isin(NYC_ZIPS)].copy()
print(f"✅ NYC retailers: {len(snap_df):,}")

snap_df = snap_df.merge(crosswalk_df, on="zip_code", how="left")

healthy_types = {
    "Supermarket", "Grocery Store", "Specialty Store",
    "Farmers' Markets", "Super Store"
}
snap_df["store_type_clean"]    = snap_df.get("Store_Type", pd.Series(dtype=str)).fillna("Unknown")
snap_df["is_healthy_retailer"] = snap_df["store_type_clean"].isin(healthy_types)

col = db["snap_v2"]
col.drop()
col.insert_many(snap_df.where(pd.notna(snap_df), None).to_dict(orient="records"))

print(f"\n💾 snap_v2: {col.count_documents({}):,} documents stored")
print(f"\nStore type breakdown:")
print(snap_df["store_type_clean"].value_counts().to_string())
print(f"\nHealthy retailer ratio:       {snap_df['is_healthy_retailer'].mean()*100:.1f}%")
print(f"Neighborhoods with SNAP data: {snap_df['neighborhood'].nunique()}")
print(f"ZIP codes with SNAP data:     {snap_df['zip_code'].nunique()}")