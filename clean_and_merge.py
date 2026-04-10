import pandas as pd
import numpy as np
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv
from config import MONGO_URI, DB_NAME, ZIP_CROSSWALK

load_dotenv()

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db     = client[DB_NAME]

# ── 1. LOAD V2 COLLECTIONS ──────────────────────────────────
print("📦 Loading v2 collections from MongoDB...")

census_df      = pd.DataFrame(list(db["census_v2"].find())).drop(columns=["_id"], errors="ignore")
inspections_df = pd.DataFrame(list(db["inspections_v2"].find())).drop(columns=["_id"], errors="ignore")
farmers_df     = pd.DataFrame(list(db["farmers_v2"].find())).drop(columns=["_id"], errors="ignore")
snap_df        = pd.DataFrame(list(db["snap_v2"].find())).drop(columns=["_id"], errors="ignore")
usda_df        = pd.DataFrame(list(db["usda"].find())).drop(columns=["_id"], errors="ignore")
crosswalk_df   = pd.DataFrame(list(db["crosswalk"].find())).drop(columns=["_id"], errors="ignore")

# Also load original places data for the original 27 neighborhoods
places_df = pd.DataFrame()
try:
    places_df = pd.DataFrame(list(db["places"].find())).drop(columns=["_id"], errors="ignore")
    print(f"  ✓ Places (v1):       {len(places_df):,} records  ← original 27 neighborhoods")
except:
    pass

print(f"  ✓ Census (v2):       {len(census_df):,} ZIP records")
print(f"  ✓ Inspections (v2):  {len(inspections_df):,} records")
print(f"  ✓ Farmers (v2):      {len(farmers_df):,} records")
print(f"  ✓ SNAP (v2):         {len(snap_df):,} records")
print(f"  ✓ Crosswalk:         {len(crosswalk_df):,} ZIP mappings")


# ── 2. CENSUS — aggregate ZIP to neighborhood ───────────────
print("\n🏘️  Aggregating census data by neighborhood...")

census_df["population"]    = pd.to_numeric(census_df["population"],    errors="coerce")
census_df["median_income"] = pd.to_numeric(census_df["median_income"], errors="coerce")
census_df["poverty_rate"]  = pd.to_numeric(census_df["poverty_rate"],  errors="coerce")

census_summary = census_df.groupby("neighborhood").agg(
    total_population  = ("population",    "sum"),
    avg_median_income = ("median_income", "mean"),
    poverty_rate      = ("poverty_rate",  "mean"),
    borough           = ("borough",       "first"),
).reset_index()

census_summary["avg_median_income"] = census_summary["avg_median_income"].round(0)
census_summary["poverty_rate"]      = census_summary["poverty_rate"].round(1)

print(f"  ✓ {len(census_summary)} neighborhoods with census data")


# ── 3. INSPECTIONS — aggregate to neighborhood ──────────────
print("\n🏥 Aggregating inspections by neighborhood...")

inspections_df["is_grade_A"] = inspections_df["is_grade_A"].astype(bool)

grade_counts = inspections_df.groupby("neighborhood").agg(
    total_inspected = ("camis",      "count"),
    grade_A_count   = ("is_grade_A", "sum"),
).reset_index()

grade_counts["pct_grade_A"] = (
    grade_counts["grade_A_count"] / grade_counts["total_inspected"].replace(0, 1) * 100
).round(1)

print(f"  ✓ {len(grade_counts)} neighborhoods with inspection data")


# ── 4. FARMERS MARKETS — aggregate to neighborhood ──────────
print("\n🌽 Aggregating farmers markets by neighborhood...")

for src, tgt in [
    (["accepts_ebt", "ebt", "ebt_credit"],          "accepts_ebt"),
    (["open_year_round", "yearround", "year_round"], "open_year_round"),
    (["marketname", "market_name", "name",
      "facilityname", "facility_name"],              "market_name"),
]:
    for s in src:
        if s in farmers_df.columns:
            farmers_df[tgt] = farmers_df[s]
            break

for col in ["accepts_ebt", "open_year_round", "market_name"]:
    if col not in farmers_df.columns:
        farmers_df[col] = None

# If neighborhood column missing, join crosswalk on ZIP
if "neighborhood" not in farmers_df.columns:
    zip_col = next(
        (c for c in farmers_df.columns
         if c.lower().replace(" ", "").replace("_", "") in ("zip5", "zipcode", "zip", "zipcd")),
        None
    )
    print(f"  ℹ️  No neighborhood column — available columns: {list(farmers_df.columns)}")
    if zip_col:
        farmers_df["zip_code"] = farmers_df[zip_col].astype(str).str.strip().str.zfill(5).str[:5]
        farmers_df = farmers_df.merge(crosswalk_df[["zip_code","neighborhood","borough"]], on="zip_code", how="left")
        print(f"  ✓ Joined crosswalk on {zip_col} — {farmers_df['neighborhood'].notna().sum()} markets matched")
    else:
        print("  ⚠️  No ZIP column found in farmers_v2 — skipping farmers market aggregation")
        farmers_df["neighborhood"] = None

farmers_matched = farmers_df[farmers_df["neighborhood"].notna()].copy()

farmers_summary = farmers_matched.groupby("neighborhood").agg(
    total_farmers_markets = ("market_name",     "count"),
    ebt_accepted_markets  = ("accepts_ebt",     lambda x: x.astype(str).str.upper().isin(["YES","Y","TRUE","1"]).sum()),
    year_round_markets    = ("open_year_round",  lambda x: x.astype(str).str.upper().isin(["YES","Y","TRUE","1"]).sum()),
).reset_index()

print(f"  ✓ {len(farmers_summary)} neighborhoods with farmers market data")


# ── 5. SNAP — aggregate to neighborhood ─────────────────────
print("\n🏪 Aggregating SNAP retailers by neighborhood...")

snap_summary = snap_df.groupby("neighborhood").agg(
    total_snap_retailers     = ("Store_Name", "count"),
    snap_convenience_stores  = ("Store_Type", lambda x: (x == "Convenience Store").sum()),
    snap_grocery_stores      = ("Store_Type", lambda x: (x == "Grocery Store").sum()),
    snap_supermarkets        = ("Store_Type", lambda x: (x == "Supermarket").sum()),
    snap_super_stores        = ("Store_Type", lambda x: (x == "Super Store").sum()),
    snap_specialty_stores    = ("Store_Type", lambda x: (x == "Specialty Store").sum()),
    snap_farmers_markets     = ("Store_Type", lambda x: (x == "Farmers' Markets").sum()),
).reset_index()

snap_summary["snap_healthy_count"] = (
    snap_summary["snap_grocery_stores"]   +
    snap_summary["snap_supermarkets"]     +
    snap_summary["snap_super_stores"]     +
    snap_summary["snap_specialty_stores"] +
    snap_summary["snap_farmers_markets"]
)

snap_summary["snap_healthy_ratio"] = (
    snap_summary["snap_healthy_count"] /
    snap_summary["total_snap_retailers"].replace(0, 1) * 100
).round(1)

print(f"  ✓ {len(snap_summary)} neighborhoods with SNAP data")


# ── 6. PLACES v1 — grocery & fast food counts ───────────────
# Only available for original 27 neighborhoods.
places_summary = pd.DataFrame()
if not places_df.empty and "search_type" in places_df.columns:
    places_df = places_df.drop_duplicates(subset="place_id")
    type_counts = (
        places_df.groupby(["neighborhood", "search_type"])
        .size().unstack(fill_value=0)
    )
    type_counts.columns = [f"{col}_count" for col in type_counts.columns]
    places_summary = type_counts.reset_index()
    print(f"\n🗺️  Places v1 available for {len(places_summary)} original neighborhoods")


# ── 7. USDA — borough-level food desert flags ───────────────
print("\n🗺️  Processing USDA food desert data...")

county_to_borough = {
    "New York County": "Manhattan",
    "Kings County":    "Brooklyn",
    "Queens County":   "Queens",
    "Bronx County":    "Bronx",
    "Richmond County": "Staten Island",
}

usda_nyc = usda_df[usda_df["County"].isin(county_to_borough.keys())].copy()
usda_nyc["borough"] = usda_nyc["County"].map(county_to_borough)

for col in ["LILATracts_1And10", "LowIncomeTracts", "PovertyRate", "TractSNAP", "TractHUNV"]:
    usda_nyc[col] = pd.to_numeric(usda_nyc[col], errors="coerce")

usda_borough = usda_nyc.groupby("borough").agg(
    total_tracts      = ("CensusTract",      "count"),
    usda_food_deserts = ("LILATracts_1And10", "sum"),
    low_income_tracts = ("LowIncomeTracts",   "sum"),
    avg_poverty_rate  = ("PovertyRate",       "mean"),
    total_snap_recip  = ("TractSNAP",         "sum"),
    total_no_vehicle  = ("TractHUNV",         "sum"),
).reset_index()

usda_borough["pct_usda_food_desert"] = (
    usda_borough["usda_food_deserts"] / usda_borough["total_tracts"] * 100
).round(1)

print("  USDA Food Desert Summary by Borough:")
print(usda_borough[["borough","total_tracts","usda_food_deserts","pct_usda_food_desert"]].to_string(index=False))


# ── 8. MERGE ALL SUMMARIES ───────────────────────────────────
print("\n🔗 Merging all summaries...")

# Census is the spine — all 117 neighborhoods
summary_df = census_summary.copy()

summary_df = summary_df.merge(grade_counts,    on="neighborhood", how="left")
summary_df = summary_df.merge(farmers_summary, on="neighborhood", how="left")
summary_df = summary_df.merge(snap_summary,    on="neighborhood", how="left")

if not places_summary.empty:
    summary_df = summary_df.merge(places_summary, on="neighborhood", how="left")

# Fill zeros for neighborhoods missing certain data sources
fill_zeros = [
    "total_farmers_markets", "ebt_accepted_markets", "year_round_markets",
    "total_snap_retailers",  "snap_healthy_ratio",   "snap_healthy_count",
    "snap_convenience_stores","snap_grocery_stores",  "snap_supermarkets",
    "snap_super_stores",     "snap_specialty_stores", "snap_farmers_markets",
    "grade_A_count",         "total_inspected",       "pct_grade_A",
]
for col in fill_zeros:
    if col in summary_df.columns:
        summary_df[col] = summary_df[col].fillna(0)

print(f"  ✓ Merged summary: {len(summary_df)} neighborhoods")

# ── DATA QUALITY FIXES ──────────────────────────────────────────

# 1. Exclude Travis (Staten Island) — 0 population, industrial/airport area
travis_mask = summary_df["neighborhood"] == "Travis"
if travis_mask.any():
    print(f"\n  ⚠️  Excluding Travis (Staten Island) — 0 population, non-residential area")
    summary_df = summary_df[~travis_mask].copy()
    print(f"  ✓ Remaining neighborhoods: {len(summary_df)}")

# 2. Cap Far Rockaway year-round markets if suspiciously high (lat/lon misassignment)
far_rock = summary_df[summary_df["neighborhood"] == "Far Rockaway"]
if not far_rock.empty:
    yr_count = far_rock["year_round_markets"].values[0]
    fm_count = far_rock["total_farmers_markets"].values[0]
    print(f"\n  ℹ️  Far Rockaway farmers markets: {fm_count:.0f} total, {yr_count:.0f} year-round")
    if yr_count > 20:
        cap = summary_df[summary_df["neighborhood"] != "Far Rockaway"]["year_round_markets"].quantile(0.95)
        print(f"  ⚠️  Suspiciously high — capping year_round_markets at 95th percentile ({cap:.0f})")
        summary_df.loc[summary_df["neighborhood"] == "Far Rockaway", "year_round_markets"] = cap
        summary_df.loc[summary_df["neighborhood"] == "Far Rockaway", "total_farmers_markets"] = min(fm_count, cap * 2)


# ── 9. NORMALIZE BY POPULATION ──────────────────────────────
print("\n📐 Normalizing metrics per 10,000 residents...")

pop = summary_df["total_population"].replace(0, 1)

summary_df["snap_retailers_per_10k"]   = (summary_df["total_snap_retailers"]  / pop * 10000).round(2)
summary_df["snap_grocery_per_10k"]     = (summary_df["snap_grocery_stores"]   / pop * 10000).round(2)
summary_df["snap_supermarket_per_10k"] = (summary_df["snap_supermarkets"]     / pop * 10000).round(2)
summary_df["farmers_markets_per_10k"]  = (summary_df["total_farmers_markets"] / pop * 10000).round(4)
summary_df["inspections_per_10k"]      = (summary_df["total_inspected"]       / pop * 10000).round(2)

# Grocery per 10k — use places v1 where available, SNAP proxy elsewhere
if "grocery store_count" in summary_df.columns:
    summary_df["grocery_per_10k"] = (
        summary_df["grocery store_count"].fillna(0) / pop * 10000
    ).round(2)
else:
    summary_df["grocery_per_10k"] = (
        (summary_df["snap_grocery_stores"] +
         summary_df["snap_supermarkets"]   +
         summary_df["snap_super_stores"])  / pop * 10000
    ).round(2)

if "fast food_count" in summary_df.columns:
    summary_df["fastfood_per_10k"] = (
        summary_df["fast food_count"].fillna(0) / pop * 10000
    ).round(2)
else:
    summary_df["fastfood_per_10k"] = 0.0

summary_df["grocery_to_fastfood_ratio"] = (
    summary_df["grocery_per_10k"] /
    summary_df["fastfood_per_10k"].replace(0, 1)
).round(2)


# ── 10. FOOD ACCESS SCORE ────────────────────────────────────
# Recalibrated for 117 neighborhoods using SNAP-based metrics.
# Fast food penalty removed — Google Places only covers 27 hoods.
#
# Weights:
#   snap_grocery_per_10k   × 8.0  — core grocery access per capita
#   snap_healthy_ratio     × 0.4  — share of SNAP retailers that are healthy
#   year_round_markets     × 1.5  — consistent fresh produce access
#   farmers_markets_per_10k× 1.0  — normalized market access
#   pct_grade_A            × 0.2  — food quality signal (inspections)
#   poverty_rate           × 0.3  — structural disadvantage (penalty)
print("\n🧮 Computing Food Access Score...")

summary_df["food_access_score"] = (
    (summary_df["snap_grocery_per_10k"]     * 8.0) +
    (summary_df["snap_healthy_ratio"]       * 0.4) +
    (summary_df["year_round_markets"]       * 1.5) +
    (summary_df["farmers_markets_per_10k"]  * 1.0) +
    (summary_df["pct_grade_A"].fillna(0)    * 0.2) -
    (summary_df["poverty_rate"]             * 0.3)
).round(2)


# ── 11. PERCENTILE-BASED TIER CLASSIFICATION ────────────────
# With 117 neighborhoods, percentile cutoffs are more
# defensible than fixed thresholds.
# Food Desert  = bottom 20th percentile
# At Risk      = 20th–40th percentile
# Good Access  = 40th percentile and above
print("\n🏜️  Classifying food access tiers (percentile-based)...")

p20 = summary_df["food_access_score"].quantile(0.20)
p40 = summary_df["food_access_score"].quantile(0.40)

print(f"  Score thresholds:  Food Desert < {p20:.1f}  |  At Risk {p20:.1f}–{p40:.1f}  |  Good Access ≥ {p40:.1f}")

def classify_access(score):
    if score >= p40:
        return "Good Access"
    elif score >= p20:
        return "At Risk"
    else:
        return "Food Desert"

summary_df["access_tier"]     = summary_df["food_access_score"].apply(classify_access)
summary_df["our_food_desert"] = summary_df["access_tier"] == "Food Desert"

usda_desert_boroughs = usda_borough[usda_borough["usda_food_deserts"] > 0]["borough"].tolist()
summary_df["usda_food_desert"] = summary_df["borough"].isin(usda_desert_boroughs)

print("\n  Access Tier Distribution:")
print(summary_df["access_tier"].value_counts().to_string())

print(f"\n  Food Desert neighborhoods:")
print(
    summary_df[summary_df["our_food_desert"]][
        ["neighborhood", "borough", "food_access_score"]
    ].sort_values("food_access_score").to_string(index=False)
)


# ── 12. FINAL CLEANUP ────────────────────────────────────────
summary_df["access_tier"]     = summary_df["access_tier"].astype(str)
summary_df["usda_food_desert"] = summary_df["usda_food_desert"].astype(bool)
summary_df["our_food_desert"]  = summary_df["our_food_desert"].astype(bool)


# ── 13. SAVE TO MONGODB ──────────────────────────────────────
print("\n💾 Saving summary_v2 to MongoDB...")

db["summary_v2"].delete_many({})
records = summary_df.where(pd.notna(summary_df), None).to_dict(orient="records")
db["summary_v2"].insert_many(records)

print(f"✅ summary_v2 saved: {len(records)} neighborhood records")


# ── 14. VERIFICATION ─────────────────────────────────────────
print("\n📋 Final Summary Table (sorted by Food Access Score):")

display_cols = [
    "neighborhood", "borough", "total_population", "avg_median_income",
    "poverty_rate", "snap_grocery_per_10k", "snap_healthy_ratio",
    "year_round_markets", "pct_grade_A", "food_access_score",
    "access_tier", "usda_food_desert",
]
display_cols = [c for c in display_cols if c in summary_df.columns]

print(summary_df[display_cols].sort_values("food_access_score", ascending=False).to_string(index=False))

print(f"\n📊 Key Statistics:")
print(f"  Total neighborhoods:        {len(summary_df)}")
print(f"  🟢 Good Access:             {(summary_df['access_tier'] == 'Good Access').sum()}")
print(f"  🟡 At Risk:                 {(summary_df['access_tier'] == 'At Risk').sum()}")
print(f"  🔴 Food Desert:             {(summary_df['access_tier'] == 'Food Desert').sum()}")
print(f"  Avg food access score:      {summary_df['food_access_score'].mean():.1f}")
print(f"  Avg poverty rate:           {summary_df['poverty_rate'].mean():.1f}%")
print(f"  Avg SNAP healthy ratio:     {summary_df['snap_healthy_ratio'].mean():.1f}%")
print(f"  Total SNAP retailers:       {summary_df['total_snap_retailers'].sum():,.0f}")
print(f"  Total farmers markets:      {summary_df['total_farmers_markets'].sum():,.0f}")