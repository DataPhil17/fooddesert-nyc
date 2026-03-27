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
collection = db["inspections"]

# NYC Open Data - Restaurant Inspections
BASE_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
LIMIT = 1000

def fetch_inspections(zip_code):
    results = []
    offset = 0

    while True:
        params = {
            "$where": f"zipcode = '{zip_code}'",
            "$limit": LIMIT,
            "$offset": offset,
        }

        r = requests.get(BASE_URL, params=params)

        if r.status_code != 200:
            print(f"  ✗ Error: {r.status_code}")
            break

        data = r.json()

        if not data:
            break

        results.extend(data)
        print(f"  ✓ Fetched {len(data)} inspections (offset {offset})")
        offset += LIMIT

        if len(data) < LIMIT:
            break

        time.sleep(0.5)

    return results

def collect_all():
    collection.delete_many({})
    print("🗑️  Cleared existing inspections collection\n")

    for neighborhood in NEIGHBORHOODS:
        print(f"\n📍 Fetching inspections for {neighborhood['name']} (ZIP {neighborhood['zip']})...")
        inspections = fetch_inspections(neighborhood["zip"])

        for inspection in inspections:
            inspection["neighborhood"] = neighborhood["name"]

        if inspections:
            collection.insert_many(inspections, ordered=False)

        print(f"  ✅ Stored {len(inspections)} inspections")

    total = collection.count_documents({})
    print(f"\n✅ Inspection data collection complete. Total documents in MongoDB: {total}")

    print("\n📊 Inspections by neighborhood:")
    for name in set([n["name"] for n in NEIGHBORHOODS]):
        count = collection.count_documents({"neighborhood": name})
        print(f"  {name}: {count} inspections")

if __name__ == "__main__":
    collect_all()