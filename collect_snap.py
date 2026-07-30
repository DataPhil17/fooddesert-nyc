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

LOCAL_CSV_PATH = os.path.join(os.path.dirname(__file__), "snap_retailers.csv")

ARCGIS_ENDPOINTS = [
    "https://services1.arcgis.com/RLQu0rK7h4kbsBq5/arcgis/rest/services/SNAP_Retailer_Locator/FeatureServer/0/query",
    "https://geodata.fns.usda.gov/arcgis/rest/services/SNAP/SNAP_Retailer_Locator/FeatureServer/0/query",
]

# ── Store type classification ────────────────────────────────
#
# USDA base healthy types — stores primarily selling food for home preparation
BASE_HEALTHY_TYPES = {
    "Supermarket", "Grocery Store", "Super Store", "Farmers' Markets",
}
# Specialty Store is included but filtered by name (see below)

# Non-food specialty store keywords — reclassify these as unhealthy
# even if USDA calls them Specialty Store
NON_FOOD_KEYWORDS = [
    "tobacco", "smoke", "vape", "vapor", "cigar", "cigarette",
    "candy", "sweet", "confection",
    "liquor", "wine", "beer", "spirits", "beverage", "brew",
    "pharmacy", "drug", "rx", "compounding",
    "cosmetic", "beauty", "hair", "nail", "spa",
    "pet", "flower", "gift",
]

# Ethnic grocery / food market keywords — reclassify as healthy
# regardless of USDA store type (catches bodegas with full grocery lines,
# halal markets, ethnic grocers misclassified as Convenience Store)
ETHNIC_GROCERY_KEYWORDS = [
    "halal", "halaal",
    "carniceria", "carneceria",
    "mercado", "supermercado",
    "internacional", "international food", "international market",
    "african market", "african grocery",
    "caribbean market", "caribbean grocery",
    "asian market", "asian grocery", "asian food",
    "indian grocery", "indian market", "indian food store",
    "middle east", "middle eastern",
    "korean market", "korean grocery",
    "chinese market", "chinese grocery",
    "west indian", "east african",
    "tropical market", "tropical grocery",
    "world market", "world food",
    "ethnic food", "ethnic market", "ethnic grocery",
    "butcher", "carnicero",
    "pescaderia", "fish market",
    "produce market", "fruit market", "vegetable market",
    "farmers market", "farm market", "farm stand",
]

def classify_store(row):
    """
    Three-pass classification:
    1. Ethnic grocery override — reclassify as healthy regardless of USDA type
    2. Non-food specialty filter — reclassify obvious non-food specialty stores as unhealthy
    3. USDA base type — use USDA classification for everything else
    """
    name       = str(row.get("Store_Name", "")).lower().strip()
    store_type = str(row.get("store_type_clean", "")).strip()

    # Pass 1: ethnic grocery override (highest priority)
    if any(kw in name for kw in ETHNIC_GROCERY_KEYWORDS):
        return "healthy", "ethnic_grocery_override"

    # Pass 2: non-food specialty filter
    if store_type == "Specialty Store":
        if any(kw in name for kw in NON_FOOD_KEYWORDS):
            return "unhealthy", "non_food_specialty"
        else:
            return "healthy", "specialty_store"

    # Pass 3: USDA base type
    if store_type in BASE_HEALTHY_TYPES:
        return "healthy", store_type
    else:
        return "unhealthy", store_type


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
            data = resp.json()
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

        zip_col   = next((c for c in snap_df.columns if c.lower().replace(" ","").replace("_","") in ("zip5","zipcode","zip")), None)
        state_col = next((c for c in snap_df.columns if c.lower() in ("state","st")), None)
        type_col  = next((c for c in snap_df.columns if "type" in c.lower()), None)
        name_col  = next((c for c in snap_df.columns if "name" in c.lower() and "store" in c.lower()), snap_df.columns[0])

        print(f"   Columns: {list(snap_df.columns)}")
        print(f"   ZIP: {zip_col} | State: {state_col} | Type: {type_col} | Name: {name_col}")

        if state_col:
            snap_df = snap_df[snap_df[state_col].str.strip().str.upper() == "NY"].copy()
            print(f"   NY records: {len(snap_df):,}")

        if not zip_col:
            print("❌ Could not find ZIP column.")
            exit(1)

        snap_df["zip_code"]    = snap_df[zip_col].astype(str).str.strip().str.zfill(5).str[:5]
        snap_df["Store_Type"]  = snap_df[type_col].str.strip() if type_col else "Unknown"
        snap_df["Store_Name"]  = snap_df[name_col].str.strip()
    else:
        print("\n❌ All fetch methods failed and no local CSV found.")
        print(f"   Download from: https://usda-snap-retailers-usda-fns.hub.arcgis.com/datasets/8b260f9a10b0459aa441ad8588c2251c/explore")
        print(f"   Save as: {LOCAL_CSV_PATH}")
        exit(1)

# ── Filter to NYC ───────────────────────────────────────────
snap_df = snap_df[snap_df["zip_code"].isin(NYC_ZIPS)].copy()
print(f"\n✅ NYC retailers: {len(snap_df):,}")

snap_df = snap_df.merge(crosswalk_df, on="zip_code", how="left")

# ── Normalize store type ─────────────────────────────────────
snap_df["store_type_clean"] = (
    snap_df["Store_Type"]
    .fillna("Unknown")
    .str.strip()
    .replace("Farmers and Markets", "Farmers' Markets")
)

# ── Apply three-pass classification ─────────────────────────
print("\n🔍 Applying store classification...")

results = snap_df.apply(classify_store, axis=1)
snap_df["health_class"]  = results.apply(lambda x: x[0])
snap_df["class_reason"]  = results.apply(lambda x: x[1])
snap_df["is_healthy_retailer"] = snap_df["health_class"] == "healthy"
snap_df["is_convenience"]      = snap_df["store_type_clean"] == "Convenience Store"

# ── Summary ─────────────────────────────────────────────────
ethnic_reclassified = (snap_df["class_reason"] == "ethnic_grocery_override").sum()
nonfood_reclassified = (snap_df["class_reason"] == "non_food_specialty").sum()

print(f"\n📊 Classification results:")
print(f"   Ethnic grocery reclassified as healthy: {ethnic_reclassified:,}")
print(f"   Non-food specialty reclassified as unhealthy: {nonfood_reclassified:,}")
print(f"\nStore type breakdown (USDA original):")
print(snap_df["store_type_clean"].value_counts().to_string())
print(f"\nClassification reason breakdown:")
print(snap_df["class_reason"].value_counts().to_string())
print(f"\nHealthy retailer ratio (revised): {snap_df['is_healthy_retailer'].mean()*100:.1f}%")
print(f"Healthy retailer ratio (USDA raw): {snap_df['store_type_clean'].isin(BASE_HEALTHY_TYPES | {'Specialty Store'}).mean()*100:.1f}%")

# ── Store to MongoDB ─────────────────────────────────────────
col = db["snap"]
col.drop()
col.insert_many(snap_df.where(pd.notna(snap_df), None).to_dict(orient="records"))

print(f"\n💾 snap collection: {col.count_documents({}):,} documents stored")
print(f"   Neighborhoods covered: {snap_df['neighborhood'].nunique()}")
print(f"   ZIP codes covered:     {snap_df['zip_code'].nunique()}")
print(f"\n✅ collect_snap.py complete — re-run clean_and_merge.py to update scores")