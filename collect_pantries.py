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
            best_zip  = z
    return best_zip

# ── NTA → our neighborhood name mapping ────────────────────
# Handles cases where NTA names differ from our crosswalk names
NTA_TO_NEIGHBORHOOD = {
    "Soundview-Bruckner-Bronx River":                   "Soundview",
    "Hell's Kitchen":                                   "Hell's Kitchen",
    "East Midtown-Turtle Bay":                          "Midtown East",
    "West New Brighton-Silver Lake-Grymes Hill":        "West Brighton",
    "Mariner's Harbor-Arlington-Graniteville":          "Mariners Harbor",
    "Bedford-Stuyvesant (East)":                        "Bedford-Stuyvesant",
    "Bedford-Stuyvesant (West)":                        "Bedford-Stuyvesant",
    "Bushwick (East)":                                  "Bushwick",
    "Bushwick (West)":                                  "Bushwick",
    "Astoria (Central)":                                "Astoria",
    "Astoria (East)-Woodside (North)":                  "Astoria",
    "Astoria (North)-Ditmars-Steinway":                 "Astoria",
    "Carroll Gardens-Cobble Hill-Gowanus-Red Hook":     "Red Hook",
    "Breezy Point-Belle Harbor-Rockaway Park-Broad Channel": "Breezy Point",
    "St. George-New Brighton":                          "St. George",
    "Midtown-Times Square":                             "Hell's Kitchen",
    "East Midtown-Turtle Bay":                          "Midtown East",
    "Midtown South-Flatiron-Union Square":              "Gramercy",
    "Brighton Beach":                                   "Sheepshead Bay",
}

print("📡 Fetching community resource data...\n")

# ── 1. Emergency Food Supply Gap (NTA level) ───────────────
print("   Fetching Emergency Food Supply Gap data...")
gap_resp = requests.get(
    "https://data.cityofnewyork.us/resource/4kc9-zrs2.json",
    params={"$limit": 1000, "$where": "year='2025'"},
    timeout=15
)

gap_df = pd.DataFrame(gap_resp.json())
print(f"   ✅ Food Security data: {len(gap_df):,} NTA records")

# Convert numeric columns
for col in ["food_insecure_percentage", "supply_gap_lbs",
            "unemployment_rate", "vulnerable_population",
            "weighted_score", "rank"]:
    gap_df[col] = pd.to_numeric(gap_df[col], errors="coerce")

# Map NTA names to our neighborhood names
def map_nta_to_neighborhood(nta_name):
    # Check explicit mapping first
    if nta_name in NTA_TO_NEIGHBORHOOD:
        return NTA_TO_NEIGHBORHOOD[nta_name]
    # Try partial match against crosswalk neighborhoods
    our_hoods = crosswalk_df["neighborhood"].unique()
    for hood in our_hoods:
        if hood.lower() in nta_name.lower() or nta_name.lower() in hood.lower():
            return hood
    return None

gap_df["neighborhood"] = gap_df["nta_name"].apply(map_nta_to_neighborhood)

matched   = gap_df["neighborhood"].notna().sum()
unmatched = gap_df["neighborhood"].isna().sum()
print(f"   Matched to neighborhoods: {matched} / {len(gap_df)}")
if unmatched > 0:
    print(f"   Unmatched NTAs:")
    for nta in gap_df[gap_df["neighborhood"].isna()]["nta_name"].tolist():
        print(f"     - {nta}")

# ── 2. GreenThumb Community Gardens ───────────────────────
print("\n   Fetching GreenThumb community gardens...")

all_gardens = []
offset = 0
while True:
    resp = requests.get(
        "https://data.cityofnewyork.us/resource/p78i-pat6.json",
        params={
            "$limit":  1000,
            "$offset": offset,
            "$select": "gardenname,address,borough,zipcode,status,lat,lon"
        },
        timeout=15
    )
    if resp.status_code != 200:
        print(f"   ⚠️  Gardens fetch error {resp.status_code}")
        break
    batch = resp.json()
    if not batch:
        break
    all_gardens.extend(batch)
    offset += 1000
    if len(batch) < 1000:
        break

garden_df = pd.DataFrame(all_gardens)
print(f"   ✅ Community Gardens: {len(garden_df):,} records")

if not garden_df.empty:
    garden_df["lat"] = pd.to_numeric(garden_df.get("lat"), errors="coerce")
    garden_df["lon"] = pd.to_numeric(garden_df.get("lon"), errors="coerce")

    # Filter active/licensed only
    if "status" in garden_df.columns:
        garden_df = garden_df[
            garden_df["status"].str.upper().isin(["ACTIVE", "LICENSED", "LICENSE"])
        ].copy()
        print(f"   Active gardens: {len(garden_df):,}")

    # ZIP from zipcode column or lat/lon
    zip_col = next((c for c in garden_df.columns if "zip" in c.lower()), None)
    if zip_col:
        garden_df["zip_code"] = (
            garden_df[zip_col].astype(str).str.strip().str.zfill(5).str[:5]
        )
    else:
        garden_df["zip_code"] = garden_df.apply(
            lambda r: find_nearest_zip(r["lat"], r["lon"]), axis=1
        )

    garden_df = garden_df[garden_df["zip_code"].isin(NYC_ZIPS)].copy()
    garden_df = garden_df.merge(crosswalk_df, on="zip_code", how="left")
    garden_df["resource_type"] = "community_garden"
    print(f"   Neighborhoods covered: {garden_df['neighborhood'].nunique()}")

# ── Store to MongoDB ────────────────────────────────────────
print("\n💾 Storing to MongoDB...")

# Food security data
col = db["food_security"]
col.drop()
col.insert_many(gap_df.where(pd.notna(gap_df), None).to_dict(orient="records"))
print(f"   ✅ food_security: {col.count_documents({}):,} documents stored")

# Community gardens
if not garden_df.empty:
    col = db["gardens"]
    col.drop()
    col.insert_many(garden_df.where(pd.notna(garden_df), None).to_dict(orient="records"))
    print(f"   ✅ gardens: {col.count_documents({}):,} documents stored")

# ── Verification ────────────────────────────────────────────
print(f"\n📊 Summary:")
print(f"   Food security NTAs:    {len(gap_df):,}")
print(f"   Matched to hoods:      {gap_df['neighborhood'].notna().sum()}")
print(f"   Avg food insecurity:   {gap_df['food_insecure_percentage'].mean()*100:.1f}%")
print(f"   Community gardens:     {len(garden_df):,}")
print(f"\n✅ collect_pantries.py complete — next: collect_yelp.py")