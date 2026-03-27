import requests
import time
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv
from config import MONGO_URI, DB_NAME, GOOGLE_API_KEY, NEIGHBORHOODS

load_dotenv()

# Connect to MongoDB
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]
collection = db["places"]

# Search types
SEARCH_TYPES = [
    {"label": "grocery store", "keyword": "grocery store supermarket"},
    {"label": "restaurant", "keyword": "restaurant"},
    {"label": "fast food", "keyword": "fast food"},
    {"label": "bodega", "keyword": "bodega convenience store deli"},
]

def search_places(keyword, neighborhood):
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    results = []
    params = {
        "query": f"{keyword} in {neighborhood['zip']} NYC",
        "key": GOOGLE_API_KEY
    }
    page = 1

    while True:
        r = requests.get(url, params=params)
        data = r.json()

        if data["status"] not in ["OK", "ZERO_RESULTS"]:
            print(f"  ✗ Error: {data['status']}")
            break

        if data["status"] == "ZERO_RESULTS":
            print(f"  → No results found")
            break

        results.extend(data["results"])
        print(f"  ✓ Page {page}: {len(data['results'])} results")

        if "next_page_token" in data:
            page += 1
            time.sleep(3)
            params = {"pagetoken": data["next_page_token"], "key": GOOGLE_API_KEY}
        else:
            break

    return results

def collect_all():
    collection.delete_many({})
    print("🗑️  Cleared existing places collection\n")

    for neighborhood in NEIGHBORHOODS:
        print(f"\n📍 Collecting data for {neighborhood['name']} ({neighborhood['zip']})...")
        for search in SEARCH_TYPES:
            print(f"  Searching {search['label']}s...")
            places = search_places(search["keyword"], neighborhood)
            for place in places:
                place["neighborhood"] = neighborhood["name"]
                place["zip"] = neighborhood["zip"]
                place["search_type"] = search["label"]
                collection.update_one(
                    {"place_id": place["place_id"]},
                    {"$set": place},
                    upsert=True
                )
            print(f"  ✅ Stored {len(places)} {search['label']}s")
        print(f"✓ Done with {neighborhood['name']} ({neighborhood['zip']})")

    total = collection.count_documents({})
    print(f"\n✅ Collection complete. Total documents in MongoDB: {total}")

if __name__ == "__main__":
    collect_all()