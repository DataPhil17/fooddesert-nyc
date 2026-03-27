import requests
import time
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv
from config import MONGO_URI, DB_NAME

load_dotenv()

# Connect to MongoDB
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]
collection = db["arrests"]

# NYC Open Data - NYPD Arrests
BASE_URL = "https://data.cityofnewyork.us/resource/uip8-fykc.json"
LIMIT = 1000

# Bounding boxes for every neighborhood (as strings for Socrata API)
NEIGHBORHOOD_BOUNDS = [
    # === BRONX ===
    {"name": "South Bronx", "zip": "10451", "min_lat": "40.800", "max_lat": "40.830", "min_lng": "-73.940", "max_lng": "-73.910"},
    {"name": "South Bronx", "zip": "10454", "min_lat": "40.800", "max_lat": "40.820", "min_lng": "-73.925", "max_lng": "-73.895"},
    {"name": "South Bronx", "zip": "10455", "min_lat": "40.810", "max_lat": "40.830", "min_lng": "-73.920", "max_lng": "-73.895"},
    {"name": "Highbridge", "zip": "10452", "min_lat": "40.830", "max_lat": "40.850", "min_lng": "-73.930", "max_lng": "-73.905"},
    {"name": "Belmont", "zip": "10457", "min_lat": "40.845", "max_lat": "40.865", "min_lng": "-73.900", "max_lng": "-73.875"},
    {"name": "Riverdale", "zip": "10471", "min_lat": "40.880", "max_lat": "40.910", "min_lng": "-73.930", "max_lng": "-73.900"},

    # === BROOKLYN ===
    {"name": "East New York", "zip": "11207", "min_lat": "40.655", "max_lat": "40.680", "min_lng": "-73.905", "max_lng": "-73.870"},
    {"name": "East New York", "zip": "11208", "min_lat": "40.650", "max_lat": "40.675", "min_lng": "-73.880", "max_lng": "-73.845"},
    {"name": "Bedford-Stuyvesant", "zip": "11221", "min_lat": "40.682", "max_lat": "40.702", "min_lng": "-73.945", "max_lng": "-73.915"},
    {"name": "Bedford-Stuyvesant", "zip": "11233", "min_lat": "40.670", "max_lat": "40.690", "min_lng": "-73.935", "max_lng": "-73.905"},
    {"name": "Crown Heights", "zip": "11213", "min_lat": "40.665", "max_lat": "40.685", "min_lng": "-73.950", "max_lng": "-73.920"},
    {"name": "Clinton Hill / Fort Greene", "zip": "11205", "min_lat": "40.688", "max_lat": "40.702", "min_lng": "-73.970", "max_lng": "-73.950"},
    {"name": "Clinton Hill / Fort Greene", "zip": "11201", "min_lat": "40.695", "max_lat": "40.710", "min_lng": "-73.995", "max_lng": "-73.975"},
    {"name": "Red Hook", "zip": "11231", "min_lat": "40.670", "max_lat": "40.685", "min_lng": "-74.015", "max_lng": "-73.995"},
    {"name": "Park Slope", "zip": "11215", "min_lat": "40.655", "max_lat": "40.675", "min_lng": "-73.995", "max_lng": "-73.975"},
    {"name": "Park Slope", "zip": "11217", "min_lat": "40.675", "max_lat": "40.690", "min_lng": "-73.990", "max_lng": "-73.970"},
    {"name": "Sheepshead Bay", "zip": "11235", "min_lat": "40.585", "max_lat": "40.608", "min_lng": "-73.960", "max_lng": "-73.930"},
    {"name": "Coney Island", "zip": "11224", "min_lat": "40.570", "max_lat": "40.590", "min_lng": "-74.010", "max_lng": "-73.980"},
    {"name": "Bensonhurst", "zip": "11214", "min_lat": "40.595", "max_lat": "40.620", "min_lng": "-74.020", "max_lng": "-73.990"},
    {"name": "Midwood", "zip": "11230", "min_lat": "40.620", "max_lat": "40.640", "min_lng": "-73.975", "max_lng": "-73.945"},

    # === MANHATTAN ===
    {"name": "Washington Heights", "zip": "10032", "min_lat": "40.835", "max_lat": "40.855", "min_lng": "-73.950", "max_lng": "-73.925"},
    {"name": "Washington Heights", "zip": "10033", "min_lat": "40.850", "max_lat": "40.870", "min_lng": "-73.945", "max_lng": "-73.920"},
    {"name": "Harlem", "zip": "10037", "min_lat": "40.808", "max_lat": "40.825", "min_lng": "-73.940", "max_lng": "-73.915"},
    {"name": "Harlem", "zip": "10039", "min_lat": "40.820", "max_lat": "40.840", "min_lng": "-73.945", "max_lng": "-73.920"},
    {"name": "Upper West Side", "zip": "10023", "min_lat": "40.775", "max_lat": "40.795", "min_lng": "-73.995", "max_lng": "-73.970"},
    {"name": "Upper West Side", "zip": "10024", "min_lat": "40.785", "max_lat": "40.805", "min_lng": "-73.980", "max_lng": "-73.955"},
    {"name": "Greenwich Village", "zip": "10003", "min_lat": "40.727", "max_lat": "40.745", "min_lng": "-74.005", "max_lng": "-73.980"},
    {"name": "Chinatown / Lower East Side", "zip": "10002", "min_lat": "40.712", "max_lat": "40.730", "min_lng": "-74.005", "max_lng": "-73.975"},

    # === QUEENS ===
    {"name": "Astoria", "zip": "11102", "min_lat": "40.765", "max_lat": "40.785", "min_lng": "-73.940", "max_lng": "-73.915"},
    {"name": "Astoria", "zip": "11103", "min_lat": "40.760", "max_lat": "40.780", "min_lng": "-73.925", "max_lng": "-73.900"},
    {"name": "Jamaica", "zip": "11432", "min_lat": "40.700", "max_lat": "40.720", "min_lng": "-73.805", "max_lng": "-73.775"},
    {"name": "Jamaica", "zip": "11433", "min_lat": "40.690", "max_lat": "40.710", "min_lng": "-73.790", "max_lng": "-73.760"},
    {"name": "Forest Hills", "zip": "11375", "min_lat": "40.715", "max_lat": "40.735", "min_lng": "-73.860", "max_lng": "-73.835"},
    {"name": "Kew Gardens", "zip": "11415", "min_lat": "40.705", "max_lat": "40.722", "min_lng": "-73.840", "max_lng": "-73.820"},
    {"name": "Middle Village", "zip": "11379", "min_lat": "40.718", "max_lat": "40.735", "min_lng": "-73.885", "max_lng": "-73.860"},
    {"name": "Far Rockaway", "zip": "11691", "min_lat": "40.595", "max_lat": "40.615", "min_lng": "-73.775", "max_lng": "-73.745"},

    # === STATEN ISLAND ===
    {"name": "St. George", "zip": "10301", "min_lat": "40.630", "max_lat": "40.650", "min_lng": "-74.110", "max_lng": "-74.080"},
    {"name": "Tottenville", "zip": "10307", "min_lat": "40.510", "max_lat": "40.530", "min_lng": "-74.260", "max_lng": "-74.230"},
]

def fetch_arrests(neighborhood):
    results = []
    offset = 0

    while True:
        where = (
            f"latitude > '{neighborhood['min_lat']}' AND "
            f"latitude < '{neighborhood['max_lat']}' AND "
            f"longitude > '{neighborhood['min_lng']}' AND "
            f"longitude < '{neighborhood['max_lng']}'"
        )

        params = {
            "$where": where,
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
        print(f"  ✓ Fetched {len(data)} arrests (offset {offset})")
        offset += LIMIT

        if len(data) < LIMIT:
            break

        time.sleep(0.5)

    return results

def collect_all():
    collection.delete_many({})
    print("🗑️  Cleared existing arrests collection\n")

    for neighborhood in NEIGHBORHOOD_BOUNDS:
        print(f"\n📍 Fetching arrests for {neighborhood['name']} (ZIP {neighborhood['zip']})...")
        arrests = fetch_arrests(neighborhood)

        for arrest in arrests:
            arrest["neighborhood"] = neighborhood["name"]
            arrest["zip"] = neighborhood["zip"]

        if arrests:
            collection.insert_many(arrests, ordered=False)

        print(f"  ✅ Stored {len(arrests)} arrests")

    total = collection.count_documents({})
    print(f"\n✅ Crime data collection complete. Total arrests in MongoDB: {total}")

    print("\n📊 Arrests by neighborhood:")
    for n in set([n["name"] for n in NEIGHBORHOOD_BOUNDS]):
        count = collection.count_documents({"neighborhood": n})
        print(f"  {n}: {count} arrests")

if __name__ == "__main__":
    collect_all()