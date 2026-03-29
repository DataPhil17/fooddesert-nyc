import requests
import time
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv
from config import MONGO_URI, DB_NAME, NEIGHBORHOODS

load_dotenv()

# Connect to MongoDB
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]
collection = db["snap_retailers"]

# USDA SNAP Retailer API
BASE_URL = "https://services1.arcgis.com/RLQu0rK7h4kbsBq5/arcgis/rest/services/snap_retailer_location_data/FeatureServer/0/query"
LIMIT = 1000

# NYC ZIP codes from our neighborhoods
NYC_ZIPS = list(set([n["zip"] for n in NEIGHBORHOODS]))

def fetch_snap_by_zip(zip_code):
    results = []
    offset = 0

    while True:
        params = {
            "where": f"Zip_Code='{zip_code}' AND State='NY'",
            "outFields": "Record_ID,Store_Name,Store_Street_Address,City,State,Zip_Code,County,Store_Type,Latitude,Longitude,Incentive_Program",
            "f": "json",
            "resultRecordCount": LIMIT,
            "resultOffset": offset
        }

        r = requests.get(BASE_URL, params=params)

        if r.status_code != 200:
            print(f"  ✗ Error: {r.status_code}")
            break

        data = r.json()

        if "features" not in data or not data["features"]:
            break

        features = [f["attributes"] for f in data["features"]]
        results.extend(features)

        if len(features) < LIMIT:
            break

        offset += LIMIT
        time.sleep(0.3)

    return results

def collect_all():
    collection.delete_many({})
    print("🗑️  Cleared existing SNAP retailers collection\n")

    all_retailers = []

    for neighborhood in NEIGHBORHOODS:
        zip_code = neighborhood["zip"]
        name = neighborhood["name"]
        print(f"📍 Fetching SNAP retailers for {name} (ZIP {zip_code})...")

        retailers = fetch_snap_by_zip(zip_code)

        for r in retailers:
            r["neighborhood"] = name
            r["zip"] = zip_code

        if retailers:
            all_retailers.extend(retailers)
            print(f"  ✓ Found {len(retailers)} SNAP retailers")
        else:
            print(f"  → No SNAP retailers found")

    # Deduplicate on Record_ID
    seen = set()
    unique_retailers = []
    for r in all_retailers:
        if r["Record_ID"] not in seen:
            seen.add(r["Record_ID"])
            unique_retailers.append(r)

    if unique_retailers:
        collection.insert_many(unique_retailers, ordered=False)

    total = collection.count_documents({})
    print(f"\n✅ SNAP retailer collection complete. Total: {total}")

    print("\n📊 SNAP Retailers by Neighborhood:")
    pipeline = [
        {"$group": {
            "_id": "$neighborhood",
            "total": {"$sum": 1},
            "store_types": {"$addToSet": "$Store_Type"}
        }},
        {"$sort": {"total": -1}}
    ]

    for doc in collection.aggregate(pipeline):
        print(f"  {doc['_id']}: {doc['total']} retailers")

    print("\n📊 SNAP Retailer Types across all neighborhoods:")
    type_pipeline = [
        {"$group": {"_id": "$Store_Type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    for doc in collection.aggregate(type_pipeline):
        print(f"  {doc['_id']}: {doc['count']}")

if __name__ == "__main__":
    collect_all()