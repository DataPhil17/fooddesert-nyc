import pandas as pd
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv
from config import MONGO_URI, DB_NAME

load_dotenv()

# Connect to MongoDB
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]
collection = db["usda"]

# NYC county names
NYC_COUNTIES = [
    "New York County",   # Manhattan
    "Kings County",      # Brooklyn
    "Queens County",     # Queens
    "Bronx County",      # Bronx
    "Richmond County"    # Staten Island
]

# County to borough mapping
COUNTY_TO_BOROUGH = {
    "New York County": "Manhattan",
    "Kings County": "Brooklyn",
    "Queens County": "Queens",
    "Bronx County": "Bronx",
    "Richmond County": "Staten Island"
}

# Key columns to keep
KEEP_COLS = [
    "CensusTract",
    "State",
    "County",
    "Urban",
    "Pop2010",
    "LILATracts_1And10",      # Official USDA food desert flag (1 mile / 10 miles)
    "LILATracts_halfAnd10",   # Stricter half mile threshold
    "LILATracts_1And20",      # 1 mile / 20 miles threshold
    "LILATracts_Vehicle",     # Vehicle access based flag
    "HUNVFlag",               # High proportion of households with no vehicle
    "LowIncomeTracts",        # Low income flag
    "PovertyRate",            # Poverty rate
    "MedianFamilyIncome",     # Median family income
    "LA1and10",               # Low access at 1 mile flag
    "LAhalfand10",            # Low access at half mile flag
    "LAPOP1_10",              # Population with low access at 1 mile
    "LAPOP05_10",             # Population with low access at half mile
    "lapop1share",            # Share of pop with low access at 1 mile
    "lapophalf",              # Pop with low access at half mile
    "lapophalfshare",         # Share of pop with low access at half mile
    "TractLOWI",              # Low income population count
    "TractKids",              # Kids population
    "TractSeniors",           # Senior population
    "TractSNAP",              # SNAP recipients
    "TractHUNV",              # Households with no vehicle
    "TractBlack",             # Black population
    "TractHispanic",          # Hispanic population
    "TractWhite",             # White population
    "TractAsian",             # Asian population
]

def collect_all():
    collection.delete_many({})
    print("🗑️  Cleared existing USDA collection\n")

    print("📂 Loading USDA Food Access Research Atlas...")
    df = pd.read_excel(
        '/Users/philippe/Downloads/Food Access Research Atlas 2019.xlsx',
        sheet_name='Food Access Research Atlas'
    )

    # Filter to NYC
    nyc_df = df[df['County'].isin(NYC_COUNTIES)].copy()
    print(f"  ✓ Total NYC census tracts: {len(nyc_df)}")

    # Keep only relevant columns
    nyc_df = nyc_df[KEEP_COLS].copy()

    # Add borough field
    nyc_df["borough"] = nyc_df["County"].map(COUNTY_TO_BOROUGH)

    # Convert NaN to None for MongoDB
    nyc_df = nyc_df.where(pd.notnull(nyc_df), None)

    # Insert into MongoDB
    records = nyc_df.to_dict(orient="records")
    collection.insert_many(records)

    total = collection.count_documents({})
    print(f"  ✓ Stored {total} census tract records in MongoDB")

    print("\n📊 USDA Food Desert Summary by Borough:")
    pipeline = [
        {"$group": {
            "_id": "$borough",
            "total_tracts": {"$sum": 1},
            "usda_food_deserts": {"$sum": "$LILATracts_1And10"},
            "low_income_tracts": {"$sum": "$LowIncomeTracts"},
            "avg_poverty_rate": {"$avg": "$PovertyRate"},
            "total_snap": {"$sum": "$TractSNAP"},
            "total_no_vehicle": {"$sum": "$TractHUNV"}
        }},
        {"$sort": {"usda_food_deserts": -1}}
    ]

    for doc in collection.aggregate(pipeline):
        print(f"  {doc['_id']}:")
        print(f"    Total tracts:       {doc['total_tracts']}")
        print(f"    USDA food deserts:  {doc['usda_food_deserts']}")
        print(f"    Low income tracts:  {doc['low_income_tracts']}")
        print(f"    Avg poverty rate:   {doc['avg_poverty_rate']:.1f}%")
        print(f"    SNAP recipients:    {int(doc['total_snap']):,}")
        print(f"    No vehicle HH:      {int(doc['total_no_vehicle']):,}")

    print(f"\n✅ USDA data collection complete")
    print(f"   Total NYC USDA food deserts: {collection.count_documents({'LILATracts_1And10': 1})}")
    print(f"   Total NYC low income tracts: {collection.count_documents({'LowIncomeTracts': 1})}")

if __name__ == "__main__":
    collect_all()