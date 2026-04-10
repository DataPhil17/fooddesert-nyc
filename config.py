# ============================================================
# PHASE 1 — EXPANDED DATA COLLECTION
# NYC Food Desert Analysis (All Neighborhoods)
# ============================================================

import os
import time
import pandas as pd
import requests
from collections import defaultdict
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME   = "fooddesert"
client    = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db        = client[DB_NAME]

# ── CELL A ──────────────────────────────────────────────────
# ZIP ↔ Neighborhood ↔ Borough Crosswalk
# ────────────────────────────────────────────────────────────

ZIP_CROSSWALK = {
    # ── BRONX ──────────────────────────────────────────────
    "10451": ("South Bronx",            "Bronx"),
    "10454": ("South Bronx",            "Bronx"),
    "10455": ("South Bronx",            "Bronx"),
    "10452": ("Highbridge",             "Bronx"),
    "10453": ("Highbridge",             "Bronx"),
    "10456": ("Melrose",                "Bronx"),
    "10457": ("Belmont",                "Bronx"),
    "10458": ("Fordham",                "Bronx"),
    "10459": ("Longwood",               "Bronx"),
    "10460": ("West Farms",             "Bronx"),
    "10461": ("Parkchester",            "Bronx"),
    "10462": ("Parkchester",            "Bronx"),
    "10463": ("Kingsbridge",            "Bronx"),
    "10464": ("City Island",            "Bronx"),
    "10465": ("Throggs Neck",           "Bronx"),
    "10466": ("Wakefield",              "Bronx"),
    "10467": ("Norwood",                "Bronx"),
    "10468": ("University Heights",     "Bronx"),
    "10469": ("Pelham Parkway",         "Bronx"),
    "10470": ("Wakefield",              "Bronx"),
    "10471": ("Riverdale",              "Bronx"),
    "10472": ("Soundview",              "Bronx"),
    "10473": ("Soundview",              "Bronx"),
    "10474": ("Hunts Point",            "Bronx"),
    "10475": ("Co-op City",             "Bronx"),

    # ── BROOKLYN ───────────────────────────────────────────
    "11201": ("Brooklyn Heights",       "Brooklyn"),
    "11203": ("East Flatbush",          "Brooklyn"),
    "11204": ("Borough Park",           "Brooklyn"),
    "11205": ("Clinton Hill",           "Brooklyn"),
    "11206": ("Bushwick",               "Brooklyn"),
    "11207": ("East New York",          "Brooklyn"),
    "11208": ("East New York",          "Brooklyn"),
    "11209": ("Bay Ridge",              "Brooklyn"),
    "11210": ("Flatbush",               "Brooklyn"),
    "11211": ("Williamsburg",           "Brooklyn"),
    "11212": ("Brownsville",            "Brooklyn"),
    "11213": ("Crown Heights",          "Brooklyn"),
    "11214": ("Bensonhurst",            "Brooklyn"),
    "11215": ("Park Slope",             "Brooklyn"),
    "11216": ("Bedford-Stuyvesant",     "Brooklyn"),
    "11217": ("Park Slope",             "Brooklyn"),
    "11218": ("Kensington",             "Brooklyn"),
    "11219": ("Borough Park",           "Brooklyn"),
    "11220": ("Sunset Park",            "Brooklyn"),
    "11221": ("Bedford-Stuyvesant",     "Brooklyn"),
    "11222": ("Greenpoint",             "Brooklyn"),
    "11223": ("Gravesend",              "Brooklyn"),
    "11224": ("Coney Island",           "Brooklyn"),
    "11225": ("Flatbush",               "Brooklyn"),
    "11226": ("Flatbush",               "Brooklyn"),
    "11228": ("Dyker Heights",          "Brooklyn"),
    "11229": ("Marine Park",            "Brooklyn"),
    "11230": ("Midwood",                "Brooklyn"),
    "11231": ("Red Hook",               "Brooklyn"),
    "11232": ("Sunset Park",            "Brooklyn"),
    "11233": ("Bedford-Stuyvesant",     "Brooklyn"),
    "11234": ("Canarsie",               "Brooklyn"),
    "11235": ("Sheepshead Bay",         "Brooklyn"),
    "11236": ("Canarsie",               "Brooklyn"),
    "11237": ("Bushwick",               "Brooklyn"),
    "11238": ("Prospect Heights",       "Brooklyn"),
    "11239": ("East New York",          "Brooklyn"),

    # ── MANHATTAN ──────────────────────────────────────────
    "10001": ("Chelsea",                "Manhattan"),
    "10002": ("Lower East Side",        "Manhattan"),
    "10003": ("East Village",           "Manhattan"),
    "10004": ("Financial District",     "Manhattan"),
    "10005": ("Financial District",     "Manhattan"),
    "10006": ("Financial District",     "Manhattan"),
    "10007": ("Tribeca",                "Manhattan"),
    "10009": ("East Village",           "Manhattan"),
    "10010": ("Gramercy",               "Manhattan"),
    "10011": ("Chelsea",                "Manhattan"),
    "10012": ("SoHo",                   "Manhattan"),
    "10013": ("Tribeca",                "Manhattan"),
    "10014": ("West Village",           "Manhattan"),
    "10016": ("Murray Hill",            "Manhattan"),
    "10017": ("Midtown East",           "Manhattan"),
    "10018": ("Hell's Kitchen",         "Manhattan"),
    "10019": ("Hell's Kitchen",         "Manhattan"),
    "10021": ("Upper East Side",        "Manhattan"),
    "10022": ("Midtown East",           "Manhattan"),
    "10023": ("Upper West Side",        "Manhattan"),
    "10024": ("Upper West Side",        "Manhattan"),
    "10025": ("Morningside Heights",    "Manhattan"),
    "10026": ("Harlem",                 "Manhattan"),
    "10027": ("Harlem",                 "Manhattan"),
    "10028": ("Upper East Side",        "Manhattan"),
    "10029": ("East Harlem",            "Manhattan"),
    "10030": ("Harlem",                 "Manhattan"),
    "10031": ("Hamilton Heights",       "Manhattan"),
    "10032": ("Washington Heights",     "Manhattan"),
    "10033": ("Washington Heights",     "Manhattan"),
    "10034": ("Inwood",                 "Manhattan"),
    "10035": ("East Harlem",            "Manhattan"),
    "10036": ("Hell's Kitchen",         "Manhattan"),
    "10037": ("Harlem",                 "Manhattan"),
    "10038": ("Financial District",     "Manhattan"),
    "10039": ("Harlem",                 "Manhattan"),
    "10040": ("Inwood",                 "Manhattan"),

    # ── QUEENS ─────────────────────────────────────────────
    "11001": ("Floral Park",            "Queens"),
    "11004": ("Glen Oaks",              "Queens"),
    "11005": ("Floral Park",            "Queens"),
    "11101": ("Long Island City",       "Queens"),
    "11102": ("Astoria",                "Queens"),
    "11103": ("Astoria",                "Queens"),
    "11104": ("Sunnyside",              "Queens"),
    "11105": ("Astoria",                "Queens"),
    "11106": ("Astoria",                "Queens"),
    "11354": ("Flushing",               "Queens"),
    "11355": ("Flushing",               "Queens"),
    "11356": ("College Point",          "Queens"),
    "11357": ("Whitestone",             "Queens"),
    "11358": ("Flushing",               "Queens"),
    "11360": ("Bayside",                "Queens"),
    "11361": ("Bayside",                "Queens"),
    "11362": ("Little Neck",            "Queens"),
    "11363": ("Little Neck",            "Queens"),
    "11364": ("Oakland Gardens",        "Queens"),
    "11365": ("Fresh Meadows",          "Queens"),
    "11366": ("Fresh Meadows",          "Queens"),
    "11367": ("Kew Gardens Hills",      "Queens"),
    "11368": ("Corona",                 "Queens"),
    "11369": ("Jackson Heights",        "Queens"),
    "11370": ("Jackson Heights",        "Queens"),
    "11372": ("Jackson Heights",        "Queens"),
    "11373": ("Elmhurst",               "Queens"),
    "11374": ("Rego Park",              "Queens"),
    "11375": ("Forest Hills",           "Queens"),
    "11377": ("Woodside",               "Queens"),
    "11378": ("Maspeth",                "Queens"),
    "11379": ("Middle Village",         "Queens"),
    "11385": ("Ridgewood",              "Queens"),
    "11411": ("Cambria Heights",        "Queens"),
    "11412": ("St. Albans",             "Queens"),
    "11413": ("Springfield Gardens",    "Queens"),
    "11414": ("Howard Beach",           "Queens"),
    "11415": ("Kew Gardens",            "Queens"),
    "11416": ("Ozone Park",             "Queens"),
    "11417": ("Ozone Park",             "Queens"),
    "11418": ("Richmond Hill",          "Queens"),
    "11419": ("Richmond Hill",          "Queens"),
    "11420": ("South Ozone Park",       "Queens"),
    "11421": ("Woodhaven",              "Queens"),
    "11422": ("Rosedale",               "Queens"),
    "11423": ("Hollis",                 "Queens"),
    "11424": ("Jamaica",                "Queens"),
    "11426": ("Bellerose",              "Queens"),
    "11427": ("Queens Village",         "Queens"),
    "11428": ("Queens Village",         "Queens"),
    "11429": ("Queens Village",         "Queens"),
    "11432": ("Jamaica",                "Queens"),
    "11433": ("Jamaica",                "Queens"),
    "11434": ("Jamaica",                "Queens"),
    "11435": ("Jamaica",                "Queens"),
    "11436": ("Jamaica",                "Queens"),
    "11691": ("Far Rockaway",           "Queens"),
    "11692": ("Arverne",                "Queens"),
    "11693": ("Far Rockaway",           "Queens"),
    "11694": ("Rockaway Park",          "Queens"),
    "11697": ("Breezy Point",           "Queens"),

    # ── STATEN ISLAND ──────────────────────────────────────
    "10301": ("St. George",             "Staten Island"),
    "10302": ("Port Richmond",          "Staten Island"),
    "10303": ("Mariners Harbor",        "Staten Island"),
    "10304": ("Stapleton",              "Staten Island"),
    "10305": ("Rosebank",               "Staten Island"),
    "10306": ("New Dorp",               "Staten Island"),
    "10307": ("Tottenville",            "Staten Island"),
    "10308": ("Great Kills",            "Staten Island"),
    "10309": ("Rossville",              "Staten Island"),
    "10310": ("West Brighton",          "Staten Island"),
    "10311": ("Travis",                 "Staten Island"),
    "10312": ("Eltingville",            "Staten Island"),
    "10314": ("Willowbrook",            "Staten Island"),
}

neighborhood_to_zips = defaultdict(list)
for zip_code, (neighborhood, borough) in ZIP_CROSSWALK.items():
    neighborhood_to_zips[neighborhood].append(zip_code)

crosswalk_df = pd.DataFrame([
    {"zip_code": z, "neighborhood": n, "borough": b}
    for z, (n, b) in ZIP_CROSSWALK.items()
])

NYC_ZIPS = list(ZIP_CROSSWALK.keys())

print(f"✅ Crosswalk built")
print(f"   ZIP codes:    {len(ZIP_CROSSWALK):,}")
print(f"   Neighborhoods: {crosswalk_df['neighborhood'].nunique()}")
print(f"   Boroughs:      {crosswalk_df['borough'].nunique()}")
print(f"\nNeighborhoods per borough:")
print(crosswalk_df.drop_duplicates('neighborhood').groupby('borough')['neighborhood'].count().to_string())


# ── CELL B ──────────────────────────────────────────────────
# ACS 2023 Census Data Pull
# ────────────────────────────────────────────────────────────

ACS_YEAR   = "2023"
ACS_VARS   = "B01003_001E,B19013_001E,B17001_002E,B17001_001E"
CENSUS_KEY = os.getenv("CENSUS_API_KEY", "")

def fetch_acs_batch(zip_list, variables, year, api_key=""):
    records   = []
    base      = f"https://api.census.gov/data/{year}/acs/acs5"
    key_param = f"&key={api_key}" if api_key else ""

    for i, zip_code in enumerate(zip_list):
        url = (
            f"{base}?get=NAME,{variables}"
            f"&for=zip%20code%20tabulation%20area:{zip_code}"
            f"{key_param}"
        )
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                rows = resp.json()
                if len(rows) > 1:
                    record = dict(zip(rows[0], rows[1]))
                    records.append(record)
            if i % 50 == 0 and i > 0:
                print(f"   ... {i}/{len(zip_list)} ZIPs fetched")
                time.sleep(1)
        except Exception as e:
            print(f"   ⚠️  ZIP {zip_code} failed: {e}")
            continue

    return records

print("📡 Fetching ACS 2023 data from Census Bureau...")
print(f"   Querying {len(NYC_ZIPS)} ZIP codes — this takes ~2 minutes\n")

raw_acs = fetch_acs_batch(NYC_ZIPS, ACS_VARS, ACS_YEAR, CENSUS_KEY)
print(f"\n✅ Retrieved {len(raw_acs)} ZIP code records from Census")

acs_raw_df = pd.DataFrame(raw_acs)
acs_raw_df.rename(columns={
    "zip code tabulation area": "zip_code",
    "B01003_001E":              "population",
    "B19013_001E":              "median_income",
    "B17001_002E":              "pop_below_poverty",
    "B17001_001E":              "pop_for_poverty_calc",
}, inplace=True)

for col in ["population", "median_income", "pop_below_poverty", "pop_for_poverty_calc"]:
    acs_raw_df[col] = pd.to_numeric(acs_raw_df[col], errors="coerce")

acs_raw_df.loc[acs_raw_df["median_income"] < 0,        "median_income"]        = None
acs_raw_df.loc[acs_raw_df["pop_below_poverty"] < 0,    "pop_below_poverty"]    = None
acs_raw_df.loc[acs_raw_df["pop_for_poverty_calc"] < 0, "pop_for_poverty_calc"] = None

acs_raw_df["poverty_rate"] = (
    acs_raw_df["pop_below_poverty"] / acs_raw_df["pop_for_poverty_calc"] * 100
).round(2)

census_expanded_df        = acs_raw_df.merge(crosswalk_df, on="zip_code", how="left")
census_expanded_df["year"] = int(ACS_YEAR)

print(f"\n📊 ACS Data Summary:")
print(f"   ZIP codes with data:       {census_expanded_df['zip_code'].nunique()}")
print(f"   ZIP codes missing income:  {census_expanded_df['median_income'].isna().sum()}")
print(f"   Avg median income:         ${census_expanded_df['median_income'].mean():,.0f}")
print(f"   Avg poverty rate:          {census_expanded_df['poverty_rate'].mean():.1f}%")
print(f"   Total population covered:  {census_expanded_df['population'].sum():,.0f}")


# ── CELL C ──────────────────────────────────────────────────
# NYC Open Data — Inspections + Farmers Markets
# Note: SNAP is handled separately in collect_snap.py
# ────────────────────────────────────────────────────────────

NYC_TOKEN = os.getenv("NYC_OPEN_DATA_TOKEN", "")

def socrata_fetch(endpoint, params, token="", label=""):
    headers     = {"X-App-Token": token} if token else {}
    limit       = 1000
    offset      = 0
    all_records = []

    while True:
        p    = {**params, "$limit": limit, "$offset": offset}
        resp = requests.get(endpoint, headers=headers, params=p, timeout=30)
        if resp.status_code != 200:
            print(f"   ⚠️  {label} fetch error {resp.status_code}: {resp.text[:200]}")
            break
        batch = resp.json()
        if not batch:
            break
        all_records.extend(batch)
        offset += limit
        if len(batch) < limit:
            break

    print(f"   ✅ {label}: {len(all_records):,} records")
    return all_records

print("📡 Fetching NYC Open Data...\n")

# ── 1. Restaurant Inspections ──────────────────────────────
inspections_raw = socrata_fetch(
    endpoint="https://data.cityofnewyork.us/resource/43nn-pn8j.json",
    params={
        "$where":  "zipcode IS NOT NULL AND grade IS NOT NULL",
        "$select": "camis,dba,boro,zipcode,cuisine_description,grade,grade_date,score"
    },
    token=NYC_TOKEN,
    label="Restaurant Inspections"
)

inspections_expanded_df = pd.DataFrame(inspections_raw)
inspections_expanded_df.rename(columns={"zipcode": "zip_code"}, inplace=True)
inspections_expanded_df["zip_code"] = (
    inspections_expanded_df["zip_code"].astype(str).str.strip().str[:5]
)
inspections_expanded_df = inspections_expanded_df[
    inspections_expanded_df["zip_code"].isin(NYC_ZIPS)
].copy()

inspections_expanded_df["grade_date"] = pd.to_datetime(
    inspections_expanded_df["grade_date"], errors="coerce"
)
inspections_expanded_df = (
    inspections_expanded_df
    .sort_values("grade_date", ascending=False)
    .drop_duplicates(subset=["camis"])
)
inspections_expanded_df = inspections_expanded_df.merge(
    crosswalk_df, on="zip_code", how="left"
)
inspections_expanded_df["is_grade_A"] = (
    inspections_expanded_df["grade"].str.upper() == "A"
)
# Convert grade_date to string so MongoDB can serialize it (NaT → None)
inspections_expanded_df["grade_date"] = inspections_expanded_df["grade_date"].apply(
    lambda x: x.isoformat() if pd.notna(x) else None
)

# ── 2. Farmers Markets ─────────────────────────────────────
# Source: DOHMH Farmers Markets — dataset 8vwk-6iz2
# Fallback: NY State Dept of Agriculture for broader coverage
farmers_raw = socrata_fetch(
    endpoint="https://data.cityofnewyork.us/resource/8vwk-6iz2.json",
    params={},
    token=NYC_TOKEN,
    label="Farmers Markets (NYC DOHMH)"
)

if not farmers_raw:
    farmers_raw = socrata_fetch(
        endpoint="https://data.ny.gov/resource/qq4h-8p86.json",
        params={"$where": "county IN('New York', 'Kings', 'Queens', 'Bronx', 'Richmond')"},
        token="",
        label="Farmers Markets (NY State)"
    )

farmers_expanded_df = pd.DataFrame(farmers_raw) if farmers_raw else pd.DataFrame()

if not farmers_expanded_df.empty:
    # Standardize key columns first
    for src, tgt in [
        (["accepts_ebt", "ebt", "ebt_credit"],          "accepts_ebt"),
        (["open_year_round", "yearround", "year_round"], "open_year_round"),
        (["marketname", "market_name", "name"],          "market_name"),
    ]:
        for s in src:
            if s in farmers_expanded_df.columns:
                farmers_expanded_df[tgt] = farmers_expanded_df[s]
                break

    # Try ZIP column first
    zip_col = next(
        (c for c in farmers_expanded_df.columns if "zip" in c.lower()), None
    )
    if zip_col:
        farmers_expanded_df["zip_code"] = (
            farmers_expanded_df[zip_col].astype(str).str.strip().str[:5]
        )
        farmers_expanded_df = farmers_expanded_df[
            farmers_expanded_df["zip_code"].isin(NYC_ZIPS)
        ].copy()
        farmers_expanded_df = farmers_expanded_df.merge(
            crosswalk_df, on="zip_code", how="left"
        )

    # No ZIP — use lat/lon to find nearest neighborhood centroid
    elif "latitude" in farmers_expanded_df.columns and "longitude" in farmers_expanded_df.columns:
        print("   ℹ️  No ZIP in farmers data — assigning neighborhood via lat/lon proximity")

        # Build neighborhood centroids from census data (avg lat/lon per ZIP group)
        # Use a simple lookup: for each market, find the ZIP whose centroid is closest
        import json

        # Fetch ZIP centroids from Census Tiger (or compute from crosswalk + known coords)
        # Simpler: use NYC borough + community_district to map to neighborhood
        # The DOHMH dataset has a 'borough' field — use borough + street address heuristic

        farmers_expanded_df["latitude"]  = pd.to_numeric(farmers_expanded_df["latitude"],  errors="coerce")
        farmers_expanded_df["longitude"] = pd.to_numeric(farmers_expanded_df["longitude"], errors="coerce")

        # ZIP centroid lookup built from known NYC ZIP centroids
        ZIP_CENTROIDS = {
            "10451": (40.8173, -73.9243), "10452": (40.8340, -73.9221), "10453": (40.8524, -73.9127),
            "10454": (40.8059, -73.9191), "10455": (40.8119, -73.9134), "10456": (40.8290, -73.9083),
            "10457": (40.8454, -73.8988), "10458": (40.8619, -73.8892), "10459": (40.8219, -73.8977),
            "10460": (40.8360, -73.8810), "10461": (40.8440, -73.8453), "10462": (40.8491, -73.8663),
            "10463": (40.8797, -73.9097), "10464": (40.8471, -73.7878), "10465": (40.8212, -73.8298),
            "10466": (40.8976, -73.8462), "10467": (40.8786, -73.8744), "10468": (40.8617, -73.9145),
            "10469": (40.8706, -73.8561), "10470": (40.9005, -73.8616), "10471": (40.9002, -73.9124),
            "10472": (40.8290, -73.8651), "10473": (40.8160, -73.8610), "10474": (40.8085, -73.8895),
            "10475": (40.8741, -73.8275), "11201": (40.6928, -73.9903), "11203": (40.6462, -73.9388),
            "11204": (40.6201, -73.9862), "11205": (40.6940, -73.9677), "11206": (40.7027, -73.9372),
            "11207": (40.6647, -73.8941), "11208": (40.6626, -73.8717), "11209": (40.6201, -74.0298),
            "11210": (40.6283, -73.9454), "11211": (40.7141, -73.9530), "11212": (40.6609, -73.9163),
            "11213": (40.6697, -73.9391), "11214": (40.6096, -73.9968), "11215": (40.6601, -73.9836),
            "11216": (40.6797, -73.9497), "11217": (40.6834, -73.9780), "11218": (40.6393, -73.9742),
            "11219": (40.6313, -73.9966), "11220": (40.6399, -74.0144), "11221": (40.6899, -73.9264),
            "11222": (40.7271, -73.9511), "11223": (40.5986, -73.9729), "11224": (40.5755, -74.0005),
            "11225": (40.6600, -73.9548), "11226": (40.6461, -73.9560), "11228": (40.6167, -74.0124),
            "11229": (40.6024, -73.9390), "11230": (40.6214, -73.9633), "11231": (40.6755, -74.0029),
            "11232": (40.6525, -74.0042), "11233": (40.6780, -73.9168), "11234": (40.6216, -73.9112),
            "11235": (40.5888, -73.9527), "11236": (40.6392, -73.9013), "11237": (40.7027, -73.9175),
            "11238": (40.6773, -73.9646), "11239": (40.6474, -73.8725), "10001": (40.7484, -74.0018),
            "10002": (40.7157, -73.9863), "10003": (40.7317, -73.9891), "10004": (40.7003, -74.0396),
            "10005": (40.7074, -74.0113), "10006": (40.7083, -74.0134), "10007": (40.7135, -74.0078),
            "10009": (40.7263, -73.9779), "10010": (40.7390, -73.9836), "10011": (40.7459, -74.0013),
            "10012": (40.7260, -74.0004), "10013": (40.7191, -74.0062), "10014": (40.7334, -74.0035),
            "10016": (40.7474, -73.9799), "10017": (40.7530, -73.9717), "10018": (40.7557, -73.9926),
            "10019": (40.7654, -73.9861), "10021": (40.7726, -73.9587), "10022": (40.7589, -73.9680),
            "10023": (40.7800, -73.9814), "10024": (40.7870, -73.9764), "10025": (40.7988, -73.9666),
            "10026": (40.8030, -73.9540), "10027": (40.8115, -73.9527), "10028": (40.7775, -73.9503),
            "10029": (40.7940, -73.9437), "10030": (40.8188, -73.9414), "10031": (40.8248, -73.9499),
            "10032": (40.8387, -73.9410), "10033": (40.8502, -73.9343), "10034": (40.8671, -73.9228),
            "10035": (40.7990, -73.9336), "10036": (40.7596, -73.9896), "10037": (40.8129, -73.9388),
            "10038": (40.7081, -74.0029), "10039": (40.8215, -73.9367), "10040": (40.8588, -73.9293),
            "11101": (40.7484, -73.9389), "11102": (40.7724, -73.9302), "11103": (40.7635, -73.9268),
            "11104": (40.7447, -73.9200), "11105": (40.7796, -73.9019), "11106": (40.7598, -73.9336),
            "11354": (40.7677, -73.8330), "11355": (40.7484, -73.8295), "11356": (40.7862, -73.8427),
            "11357": (40.7937, -73.8139), "11358": (40.7571, -73.7981), "11360": (40.7836, -73.7750),
            "11361": (40.7746, -73.7658), "11362": (40.7637, -73.7366), "11363": (40.7754, -73.7483),
            "11364": (40.7477, -73.7527), "11365": (40.7358, -73.7894), "11366": (40.7247, -73.7850),
            "11367": (40.7298, -73.8236), "11368": (40.7482, -73.8633), "11369": (40.7618, -73.8882),
            "11370": (40.7539, -73.8861), "11372": (40.7497, -73.8834), "11373": (40.7375, -73.8760),
            "11374": (40.7256, -73.8609), "11375": (40.7198, -73.8458), "11377": (40.7473, -73.9072),
            "11378": (40.7235, -73.9107), "11379": (40.7197, -73.8826), "11385": (40.7046, -73.9047),
            "11411": (40.6940, -73.7348), "11412": (40.6960, -73.7617), "11413": (40.6693, -73.7566),
            "11414": (40.6579, -73.8483), "11415": (40.7097, -73.8303), "11416": (40.6826, -73.8527),
            "11417": (40.6759, -73.8449), "11418": (40.6988, -73.8260), "11419": (40.6904, -73.8180),
            "11420": (40.6726, -73.8193), "11421": (40.6938, -73.8591), "11422": (40.6613, -73.7392),
            "11423": (40.7108, -73.7681), "11424": (40.7005, -73.8050), "11426": (40.7285, -73.7213),
            "11427": (40.7232, -73.7449), "11428": (40.7162, -73.7407), "11429": (40.7092, -73.7390),
            "11432": (40.7112, -73.7943), "11433": (40.6989, -73.7878), "11434": (40.6825, -73.7743),
            "11435": (40.7017, -73.8072), "11436": (40.6759, -73.7879), "11691": (40.6037, -73.7567),
            "11692": (40.5938, -73.7925), "11693": (40.5955, -73.8134), "11694": (40.5793, -73.8465),
            "11697": (40.5594, -73.9271), "10301": (40.6259, -74.0939), "10302": (40.6335, -74.1355),
            "10303": (40.6324, -74.1624), "10304": (40.6103, -74.0820), "10305": (40.6043, -74.0648),
            "10306": (40.5726, -74.1134), "10307": (40.5114, -74.2497), "10308": (40.5533, -74.1502),
            "10309": (40.5436, -74.2086), "10310": (40.6332, -74.1123), "10311": (40.5938, -74.1784),
            "10312": (40.5502, -74.1718), "10314": (40.6049, -74.1635),
        }

        def find_nearest_zip(lat, lon):
            if pd.isna(lat) or pd.isna(lon):
                return None
            best_zip, best_dist = None, float("inf")
            for z, (zlat, zlon) in ZIP_CENTROIDS.items():
                dist = (lat - zlat)**2 + (lon - zlon)**2
                if dist < best_dist:
                    best_dist = dist
                    best_zip = z
            return best_zip

        farmers_expanded_df["zip_code"] = farmers_expanded_df.apply(
            lambda r: find_nearest_zip(r["latitude"], r["longitude"]), axis=1
        )
        farmers_expanded_df = farmers_expanded_df[
            farmers_expanded_df["zip_code"].isin(NYC_ZIPS)
        ].copy()
        farmers_expanded_df = farmers_expanded_df.merge(
            crosswalk_df, on="zip_code", how="left"
        )
        matched = farmers_expanded_df["neighborhood"].notna().sum()
        print(f"   ✓ Matched {matched} of {len(farmers_expanded_df)} markets to neighborhoods via lat/lon")

print(f"\n✅ NYC Open Data fetched successfully")


# ── CELL D ──────────────────────────────────────────────────
# Store to MongoDB
# ────────────────────────────────────────────────────────────

def upsert_collection(db, collection_name, df):
    col     = db[collection_name]
    col.drop()
    records = df.where(pd.notna(df), None).to_dict(orient="records")
    if records:
        col.insert_many(records)
    print(f"   ✅ {collection_name}: {len(records):,} documents stored")

print("💾 Storing expanded data to MongoDB Atlas...\n")

upsert_collection(db, "census_v2",      census_expanded_df)
upsert_collection(db, "inspections_v2", inspections_expanded_df)
upsert_collection(db, "crosswalk",      crosswalk_df)

if not farmers_expanded_df.empty:
    upsert_collection(db, "farmers_v2", farmers_expanded_df)
else:
    print("   ⚠️  farmers_v2: skipped (empty DataFrame)")

print(f"\n🗄️  MongoDB collections updated:")
for col_name in ["census_v2", "inspections_v2", "farmers_v2", "crosswalk"]:
    try:
        count = db[col_name].count_documents({})
        print(f"   {col_name:<20} {count:>7,} documents")
    except:
        pass


# ── CELL E ──────────────────────────────────────────────────
# Verification
# ────────────────────────────────────────────────────────────

print("=" * 65)
print("PHASE 1 VERIFICATION — EXPANDED DATA COLLECTION")
print("=" * 65)

print(f"\n{'Dataset':<25} {'ZIP codes':>10} {'Neighborhoods':>15} {'Records':>10}")
print("-" * 65)

datasets = {
    "Census (ACS 2023)": census_expanded_df,
    "Inspections":       inspections_expanded_df,
}
if not farmers_expanded_df.empty:
    datasets["Farmers Markets"] = farmers_expanded_df

for name, df in datasets.items():
    zips  = df["zip_code"].nunique()     if "zip_code"     in df.columns else "-"
    hoods = df["neighborhood"].nunique() if "neighborhood" in df.columns else "-"
    print(f"  {name:<23} {str(zips):>10} {str(hoods):>15} {len(df):>10,}")

print("-" * 65)
print(f"\nCrosswalk:")
print(f"  ZIP codes mapped:     {len(crosswalk_df):,}")
print(f"  Unique neighborhoods: {crosswalk_df['neighborhood'].nunique()}")
print(f"  Boroughs covered:     {crosswalk_df['borough'].nunique()}")

print(f"\nBorough breakdown (Census):")
borough_summary = (
    census_expanded_df
    .merge(crosswalk_df[["zip_code", "borough"]].drop_duplicates(), on="zip_code", how="left")
    .groupby("borough_y")
    .agg(zip_codes=("zip_code", "nunique"), total_pop=("population", "sum"))
    .rename(columns={"borough_y": "borough"})
)
for borough, row in borough_summary.iterrows():
    print(f"  {borough:<20} {int(row['zip_codes']):>3} ZIPs   pop: {int(row['total_pop']):>10,}")

print(f"\n✅ Phase 1 complete — run collect_snap.py to finish SNAP collection")