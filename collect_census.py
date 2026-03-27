import requests
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv
from config import MONGO_URI, DB_NAME, NEIGHBORHOODS

load_dotenv()

# Connect to MongoDB
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]
collection = db["census"]

# Census ACS 5-Year Estimates 2023
BASE_URL = "https://api.census.gov/data/2023/acs/acs5"

# Variables:
# B01003_001E = total population
# B19013_001E = median household income
# B17001_002E = population below poverty level
def fetch_census(zip_code):
    params = {
        "get": "B01003_001E,B19013_001E,B17001_002E,NAME",
        "for": f"zip code tabulation area:{zip_code}",
    }

    r = requests.get(BASE_URL, params=params)

    if r.status_code != 200:
        print(f"  ✗ Error fetching census data for ZIP {zip_code}: {r.status_code}")
        return None

    data = r.json()
    if len(data) < 2:
        print(f"  ✗ No data for ZIP {zip_code}")
        return None

    headers = data[0]
    values = data[1]
    record = dict(zip(headers, values))
    return record

def collect_all():
    collection.delete_many({})
    print("🗑️  Cleared existing census collection\n")

    for neighborhood in NEIGHBORHOODS:
        zip_code = neighborhood["zip"]
        name = neighborhood["name"]
        print(f"📍 Fetching census data for {name} (ZIP {zip_code})...")

        record = fetch_census(zip_code)
        if record:
            record["neighborhood"] = name
            record["zip"] = zip_code
            record["population"] = int(record.get("B01003_001E", 0))
            record["median_income"] = int(record.get("B19013_001E", 0))
            record["poverty_count"] = int(record.get("B17001_002E", 0))
            collection.insert_one(record)
            print(f"  ✓ Population: {record['population']:,} | Median Income: ${record['median_income']:,} | Poverty Count: {record['poverty_count']:,}")

    total = collection.count_documents({})
    print(f"\n✅ Census data collection complete. Total ZIP records: {total}")

    print("\n📊 Population by neighborhood:")
    pipeline = [
        {"$group": {"_id": "$neighborhood", "total_population": {"$sum": "$population"}, "avg_income": {"$avg": "$median_income"}}},
        {"$sort": {"total_population": -1}}
    ]
    for doc in collection.aggregate(pipeline):
        print(f"  {doc['_id']}: {doc['total_population']:,} people | Avg Income: ${doc['avg_income']:,.0f}")

if __name__ == "__main__":
    collect_all()