import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME   = os.getenv("DB_NAME", "fooddesert")

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db     = client[DB_NAME]

# ══════════════════════════════════════════════════════════════
# 1. LOAD ALL COLLECTIONS
# ══════════════════════════════════════════════════════════════
print("📦 Loading collections from MongoDB...")

census_df      = pd.DataFrame(list(db["census"].find())).drop(columns=["_id"], errors="ignore")
inspections_df = pd.DataFrame(list(db["inspections"].find())).drop(columns=["_id"], errors="ignore")
farmers_df     = pd.DataFrame(list(db["farmers"].find())).drop(columns=["_id"], errors="ignore")
snap_df        = pd.DataFrame(list(db["snap"].find())).drop(columns=["_id"], errors="ignore")
grocery_df     = pd.DataFrame(list(db["grocery_nonsnap"].find())).drop(columns=["_id"], errors="ignore")
security_df    = pd.DataFrame(list(db["food_security"].find())).drop(columns=["_id"], errors="ignore")
gardens_df     = pd.DataFrame(list(db["gardens"].find())).drop(columns=["_id"], errors="ignore")
crosswalk_df   = pd.DataFrame(list(db["crosswalk"].find())).drop(columns=["_id"], errors="ignore")

print(f"  ✓ Census:           {len(census_df):,} ZIP records")
print(f"  ✓ Inspections:      {len(inspections_df):,} records")
print(f"  ✓ Farmers Markets:  {len(farmers_df):,} records")
print(f"  ✓ SNAP Retailers:   {len(snap_df):,} records")
print(f"  ✓ Non-SNAP Grocery: {len(grocery_df):,} records")
print(f"  ✓ Food Security:    {len(security_df):,} NTA records")
print(f"  ✓ Gardens:          {len(gardens_df):,} records")
print(f"  ✓ Crosswalk:        {len(crosswalk_df):,} ZIP mappings")


# ══════════════════════════════════════════════════════════════
# 2. CENSUS — aggregate ZIP → neighborhood
# ══════════════════════════════════════════════════════════════
print("\n🏘️  Aggregating census data...")

for col in ["population", "median_income", "poverty_rate"]:
    census_df[col] = pd.to_numeric(census_df[col], errors="coerce")

census_summary = census_df.groupby("neighborhood").agg(
    total_population  = ("population",    "sum"),
    avg_median_income = ("median_income", "mean"),
    poverty_rate      = ("poverty_rate",  "mean"),
    borough           = ("borough",       "first"),
).reset_index()

census_summary["avg_median_income"] = census_summary["avg_median_income"].round(0)
census_summary["poverty_rate"]      = census_summary["poverty_rate"].round(2)

print(f"  ✓ {len(census_summary)} neighborhoods")


# ══════════════════════════════════════════════════════════════
# 3. INSPECTIONS — fast food classification + grade A rate
# ══════════════════════════════════════════════════════════════
print("\n🍔 Aggregating inspections...")

# Fast food cuisine types
FAST_FOOD_CUISINES = {
    "Hamburgers", "Pizza", "Chicken", "Sandwiches", "Hotdogs",
    "Donuts", "Ice Cream, Gelato, Yogurt, Ices",
    "Juice, Smoothies, Fruit Salads", "Pancakes/Waffles",
}

inspections_df["is_grade_A"]  = inspections_df["is_grade_A"].astype(bool)
inspections_df["is_fast_food"] = inspections_df["cuisine_description"].isin(FAST_FOOD_CUISINES)

inspection_summary = inspections_df.groupby("neighborhood").agg(
    total_restaurants = ("camis",        "count"),
    grade_A_count     = ("is_grade_A",   "sum"),
    fast_food_count   = ("is_fast_food", "sum"),
).reset_index()

inspection_summary["pct_grade_A"] = (
    inspection_summary["grade_A_count"] /
    inspection_summary["total_restaurants"].replace(0, 1) * 100
).round(1)

print(f"  ✓ {len(inspection_summary)} neighborhoods")
print(f"  ✓ Fast food establishments flagged: {inspections_df['is_fast_food'].sum():,}")


# ══════════════════════════════════════════════════════════════
# 4. FARMERS MARKETS — year-round count
# ══════════════════════════════════════════════════════════════
print("\n🌽 Aggregating farmers markets...")

for src, tgt in [
    (["open_year_round", "yearround", "year_round"], "open_year_round"),
    (["marketname", "market_name", "name"],          "market_name"),
]:
    for s in src:
        if s in farmers_df.columns:
            farmers_df[tgt] = farmers_df[s]
            break

for col in ["open_year_round", "market_name"]:
    if col not in farmers_df.columns:
        farmers_df[col] = None

farmers_matched = farmers_df[farmers_df["neighborhood"].notna()].copy()

farmers_summary = farmers_matched.groupby("neighborhood").agg(
    total_farmers_markets = ("market_name",     "count"),
    year_round_markets    = ("open_year_round",  lambda x: x.astype(str).str.upper().isin(["YES","Y","TRUE","1"]).sum()),
).reset_index()

# Cap Far Rockaway at 95th percentile (lat/lon misassignment artifact)
cap_95 = farmers_summary["year_round_markets"].quantile(0.95)
farmers_summary.loc[
    farmers_summary["neighborhood"] == "Far Rockaway", "year_round_markets"
] = int(min(
    farmers_summary.loc[farmers_summary["neighborhood"] == "Far Rockaway", "year_round_markets"].values[0],
    cap_95
))

print(f"  ✓ {len(farmers_summary)} neighborhoods")


# ══════════════════════════════════════════════════════════════
# 5. SNAP — healthy/unhealthy classification
# ══════════════════════════════════════════════════════════════
print("\n🏪 Aggregating SNAP retailers...")

# Fix Farmers and Markets naming inconsistency
snap_df["store_type_clean"] = snap_df["store_type_clean"].replace(
    "Farmers and Markets", "Farmers' Markets"
)

healthy_types = {
    "Supermarket", "Grocery Store", "Super Store",
    "Specialty Store", "Farmers' Markets",
}

# Use pre-computed classification from collect_snap.py (includes ethnic grocery override)
# Fall back to store_type_clean if is_healthy_retailer column missing
if "is_healthy_retailer" in snap_df.columns:
    snap_df["is_healthy"] = snap_df["is_healthy_retailer"].astype(bool)
else:
    healthy_types = {"Supermarket", "Grocery Store", "Super Store",
                     "Farmers' Markets", "Specialty Store"}
    snap_df["is_healthy"] = snap_df["store_type_clean"].isin(healthy_types)

snap_df["is_convenience"] = snap_df["store_type_clean"] == "Convenience Store"

snap_summary = snap_df.groupby("neighborhood").agg(
    total_snap           = ("Store_Name",  "count"),
    snap_grocery         = ("is_healthy",  "sum"),
    snap_healthy         = ("is_healthy",  "sum"),
    snap_convenience     = ("is_convenience", "sum"),
).reset_index()

snap_summary["snap_healthy_ratio"]      = (snap_summary["snap_healthy"]     / snap_summary["total_snap"].replace(0,1) * 100).round(1)
snap_summary["snap_convenience_ratio"]  = (snap_summary["snap_convenience"] / snap_summary["total_snap"].replace(0,1) * 100).round(1)

print(f"  ✓ {len(snap_summary)} neighborhoods")


# ══════════════════════════════════════════════════════════════
# 6. NON-SNAP GROCERY — by store type
# ══════════════════════════════════════════════════════════════
print("\n🛒 Aggregating non-SNAP grocery stores...")

grocery_summary = grocery_df.groupby("neighborhood").agg(
    nonsnap_supermarket = ("store_type", lambda x: (x == "supermarket").sum()),
    nonsnap_mid_grocery = ("store_type", lambda x: (x == "mid_grocery").sum()),
    nonsnap_small       = ("store_type", lambda x: (x == "small_grocery").sum()),
    nonsnap_membership  = ("store_type", lambda x: (x == "membership").sum()),
    nonsnap_total       = ("store_type", "count"),
).reset_index()

print(f"  ✓ {len(grocery_summary)} neighborhoods")


# ══════════════════════════════════════════════════════════════
# 7. FOOD SECURITY — NTA → neighborhood mapping
# ══════════════════════════════════════════════════════════════
print("\n🍽️  Aggregating food security data...")

for col in ["food_insecure_percentage", "supply_gap_lbs", "vulnerable_population", "weighted_score"]:
    security_df[col] = pd.to_numeric(security_df[col], errors="coerce")

# Average across NTAs that map to same neighborhood
security_summary = (
    security_df[security_df["neighborhood"].notna()]
    .groupby("neighborhood")
    .agg(
        food_insecure_pct   = ("food_insecure_percentage", "mean"),
        vulnerable_pop_pct  = ("vulnerable_population",    "mean"),
        supply_gap_lbs      = ("supply_gap_lbs",           "sum"),
        food_need_score     = ("weighted_score",            "mean"),
    )
    .reset_index()
)

security_summary["food_insecure_pct"] = (security_summary["food_insecure_pct"] * 100).round(1)
security_summary["vulnerable_pop_pct"] = (security_summary["vulnerable_pop_pct"] * 100).round(1)

print(f"  ✓ {len(security_summary)} neighborhoods matched")


# ══════════════════════════════════════════════════════════════
# 8. COMMUNITY GARDENS
# ══════════════════════════════════════════════════════════════
print("\n🌱 Aggregating community gardens...")

gardens_summary = gardens_df[gardens_df["neighborhood"].notna()].groupby("neighborhood").agg(
    total_gardens = ("gardenname", "count"),
).reset_index()

print(f"  ✓ {len(gardens_summary)} neighborhoods")


# ══════════════════════════════════════════════════════════════
# 9. MERGE ALL SUMMARIES
# ══════════════════════════════════════════════════════════════
print("\n🔗 Merging all summaries...")

# Census is the spine
summary_df = census_summary.copy()

summary_df = summary_df.merge(inspection_summary, on="neighborhood", how="left")
summary_df = summary_df.merge(farmers_summary,    on="neighborhood", how="left")
summary_df = summary_df.merge(snap_summary,       on="neighborhood", how="left")
summary_df = summary_df.merge(grocery_summary,    on="neighborhood", how="left")
summary_df = summary_df.merge(security_summary,   on="neighborhood", how="left")
summary_df = summary_df.merge(gardens_summary,    on="neighborhood", how="left")

# Fill zeros for missing data
fill_zero_cols = [
    "total_restaurants", "grade_A_count", "fast_food_count", "pct_grade_A",
    "total_farmers_markets", "year_round_markets",
    "total_snap", "snap_grocery", "snap_healthy", "snap_convenience",
    "snap_healthy_ratio", "snap_convenience_ratio",
    "nonsnap_supermarket", "nonsnap_mid_grocery", "nonsnap_small",
    "nonsnap_membership", "nonsnap_total",
    "total_gardens",
]
for col in fill_zero_cols:
    if col in summary_df.columns:
        summary_df[col] = summary_df[col].fillna(0)

# Exclude Travis — 0 population, non-residential
summary_df = summary_df[summary_df["neighborhood"] != "Travis"].copy()

print(f"  ✓ {len(summary_df)} neighborhoods after excluding Travis")


# ══════════════════════════════════════════════════════════════
# 10. NORMALIZE BY POPULATION
# ══════════════════════════════════════════════════════════════
print("\n📐 Normalizing per 10,000 residents...")

pop = summary_df["total_population"].replace(0, 1)

# SNAP grocery stores per 10k (core access metric)
summary_df["snap_grocery_per_10k"] = (
    summary_df["snap_grocery"] / pop * 10000
).round(2)

# Non-SNAP grocery per 10k — weighted by store size
summary_df["nonsnap_grocery_per_10k"] = (
    (summary_df["nonsnap_supermarket"] * 1.0 +
     summary_df["nonsnap_mid_grocery"] * 0.6 +
     summary_df["nonsnap_small"]       * 0.3) / pop * 10000
).round(2)

# Membership stores per 10k
summary_df["membership_per_10k"] = (
    summary_df["nonsnap_membership"] / pop * 10000
).round(4)

# Fast food per 10k
summary_df["fastfood_per_10k"] = (
    summary_df["fast_food_count"] / pop * 10000
).round(2)

# Farmers markets per 10k
summary_df["farmers_markets_per_10k"] = (
    summary_df["total_farmers_markets"] / pop * 10000
).round(4)

# Community gardens per 10k
summary_df["gardens_per_10k"] = (
    summary_df["total_gardens"] / pop * 10000
).round(4)

# Food balance ratio — grocery vs fast food
summary_df["food_balance_ratio"] = (
    summary_df["snap_grocery_per_10k"] /
    (summary_df["fastfood_per_10k"] + 1)
).round(2)


# ══════════════════════════════════════════════════════════════
# 11. ECONOMIC ACCESS GAP
# Income-based affordability penalty
# Higher income = lower penalty = better economic access
# ══════════════════════════════════════════════════════════════
print("\n💰 Computing economic access gap...")

def calc_economic_access_gap(median_income):
    if pd.isna(median_income):
        return 1.0
    elif median_income < 35000:
        return 2.0   # severe affordability pressure
    elif median_income < 50000:
        return 1.5   # high pressure
    elif median_income < 75000:
        return 1.0   # moderate pressure
    elif median_income < 100000:
        return 0.5   # low pressure
    else:
        return 0.0   # no penalty

summary_df["economic_access_gap"] = summary_df["avg_median_income"].apply(
    calc_economic_access_gap
)


# ══════════════════════════════════════════════════════════════
# 12. FOOD ACCESS SCORE
# ══════════════════════════════════════════════════════════════
print("\n🧮 Computing Food Access Score...")

summary_df["food_access_score"] = (
    (summary_df["snap_grocery_per_10k"]        * 8.0)   +
    (summary_df["nonsnap_grocery_per_10k"]     * 5.0)   +
    (summary_df["membership_per_10k"]          * 3.0)   +
    (summary_df["snap_convenience_ratio"] / 100 * -8.0) +
    (summary_df["food_balance_ratio"]          * 4.0)   +
    (summary_df["fastfood_per_10k"]            * -0.5)  +
    (summary_df["year_round_markets"]          * 1.0)   +
    (summary_df["farmers_markets_per_10k"]     * 0.5)   +
    (summary_df["gardens_per_10k"]             * 0.25)  +
    (summary_df["pct_grade_A"] / 100           * 0.2)   +
    (summary_df["economic_access_gap"]         * -3.0)
).round(2)

print(f"  Score range: {summary_df['food_access_score'].min():.1f} — {summary_df['food_access_score'].max():.1f}")
print(f"  Mean score:  {summary_df['food_access_score'].mean():.1f}")


# ══════════════════════════════════════════════════════════════
# 13. BACK-CALCULATE THRESHOLDS FROM ARCHETYPES
# ══════════════════════════════════════════════════════════════
print("\n📏 Computing thresholds from archetypes...")

def score_archetype(snap_grocery_per_10k, nonsnap_grocery_per_10k,
                    membership_per_10k, snap_convenience_ratio,
                    food_balance_ratio, year_round_markets,
                    farmers_markets_per_10k, gardens_per_10k,
                    pct_grade_A, economic_access_gap):
    return round(
        (snap_grocery_per_10k         * 8.0)   +
        (nonsnap_grocery_per_10k      * 5.0)   +
        (membership_per_10k           * 3.0)   +
        (snap_convenience_ratio / 100 * -8.0)  +
        (food_balance_ratio           * 4.0)   +
        (year_round_markets           * 1.0)   +
        (farmers_markets_per_10k      * 0.5)   +
        (gardens_per_10k              * 0.25)  +
        (pct_grade_A / 100            * 0.2)   +
        (economic_access_gap          * -1.0), 2
    )

# Food Desert archetype — very poor access
food_desert_score = score_archetype(
    snap_grocery_per_10k    = 1.0,
    nonsnap_grocery_per_10k = 2.0,
    membership_per_10k      = 0.0,
    snap_convenience_ratio  = 45.0,
    food_balance_ratio      = 0.2,
    year_round_markets      = 0.0,
    farmers_markets_per_10k = 0.0,
    gardens_per_10k         = 0.0,
    pct_grade_A             = 75.0,
    economic_access_gap     = 0.5,
)

# At Risk archetype — marginal access
at_risk_score = score_archetype(
    snap_grocery_per_10k    = 2.5,
    nonsnap_grocery_per_10k = 3.0,
    membership_per_10k      = 0.0,
    snap_convenience_ratio  = 40.0,
    food_balance_ratio      = 0.7,
    year_round_markets      = 1.0,
    farmers_markets_per_10k = 0.3,
    gardens_per_10k         = 0.3,
    pct_grade_A             = 78.0,
    economic_access_gap     = 0.5,
)

# Good Access archetype — strong access
good_access_score = score_archetype(
    snap_grocery_per_10k    = 5.0,
    nonsnap_grocery_per_10k = 4.0,
    membership_per_10k      = 0.1,
    snap_convenience_ratio  = 20.0,
    food_balance_ratio      = 2.5,
    year_round_markets      = 4.0,
    farmers_markets_per_10k = 1.5,
    gardens_per_10k         = 1.0,
    pct_grade_A             = 85.0,
    economic_access_gap     = 0.0,
)

print(f"  Food Desert archetype score:  {food_desert_score}")
print(f"  At Risk archetype score:      {at_risk_score}")
print(f"  Good Access archetype score:  {good_access_score}")

# Thresholds sit between archetypes
FOOD_DESERT_THRESHOLD = round((food_desert_score + at_risk_score) / 2, 1)
GOOD_ACCESS_THRESHOLD = round((at_risk_score + good_access_score) / 2, 1)

print(f"\n  → Food Desert threshold:  < {FOOD_DESERT_THRESHOLD}")
print(f"  → At Risk range:          {FOOD_DESERT_THRESHOLD} – {GOOD_ACCESS_THRESHOLD}")
print(f"  → Good Access threshold:  ≥ {GOOD_ACCESS_THRESHOLD}")


# ══════════════════════════════════════════════════════════════
# 14. TIER CLASSIFICATION
# ══════════════════════════════════════════════════════════════
print("\n🏜️  Classifying access tiers...")

def classify_tier(score):
    if pd.isna(score):
        return "Unknown"
    elif score < FOOD_DESERT_THRESHOLD:
        return "Food Desert"
    elif score < GOOD_ACCESS_THRESHOLD:
        return "At Risk"
    else:
        return "Good Access"

summary_df["access_tier"]     = summary_df["food_access_score"].apply(classify_tier)
summary_df["our_food_desert"] = summary_df["access_tier"] == "Food Desert"

# USDA comparison flag — borough level
# USDA designates Staten Island as having food desert tracts
USDA_DESERT_BOROUGHS = {"Staten Island", "Brooklyn"}
summary_df["usda_food_desert"] = summary_df["borough"].isin(USDA_DESERT_BOROUGHS)

print(f"\n  Access Tier Distribution:")
print(summary_df["access_tier"].value_counts().to_string())

print(f"\n  Food Desert neighborhoods:")
fd = summary_df[summary_df["our_food_desert"]][
    ["neighborhood", "borough", "food_access_score"]
].sort_values("food_access_score")
print(fd.to_string(index=False))


# ══════════════════════════════════════════════════════════════
# 15. SAVE TO MONGODB
# ══════════════════════════════════════════════════════════════
print("\n💾 Saving summary to MongoDB...")

summary_df["access_tier"]     = summary_df["access_tier"].astype(str)
summary_df["our_food_desert"] = summary_df["our_food_desert"].astype(bool)
summary_df["usda_food_desert"] = summary_df["usda_food_desert"].astype(bool)

db["summary"].delete_many({})
records = summary_df.where(pd.notna(summary_df), None).to_dict(orient="records")
db["summary"].insert_many(records)

print(f"✅ summary collection saved: {len(records)} neighborhood records")


# ══════════════════════════════════════════════════════════════
# 16. VERIFICATION TABLE
# ══════════════════════════════════════════════════════════════
print("\n📋 Final Summary Table (sorted by Food Access Score):")

display_cols = [
    "neighborhood", "borough", "total_population", "avg_median_income",
    "poverty_rate", "snap_grocery_per_10k", "nonsnap_grocery_per_10k",
    "snap_convenience_ratio", "food_balance_ratio", "year_round_markets",
    "pct_grade_A", "economic_access_gap", "food_access_score",
    "access_tier", "usda_food_desert",
]
display_cols = [c for c in display_cols if c in summary_df.columns]

print(summary_df[display_cols].sort_values(
    "food_access_score", ascending=False
).to_string(index=False))

print(f"\n📊 Key Statistics:")
print(f"  Total neighborhoods:    {len(summary_df)}")
print(f"  🟢 Good Access:         {(summary_df['access_tier'] == 'Good Access').sum()}")
print(f"  🟡 At Risk:             {(summary_df['access_tier'] == 'At Risk').sum()}")
print(f"  🔴 Food Desert:         {(summary_df['access_tier'] == 'Food Desert').sum()}")
print(f"  Score range:            {summary_df['food_access_score'].min():.1f} – {summary_df['food_access_score'].max():.1f}")
print(f"  Avg food insecurity:    {summary_df['food_insecure_pct'].mean():.1f}%")
print(f"  Avg SNAP healthy ratio: {summary_df['snap_healthy_ratio'].mean():.1f}%")
print(f"  Total SNAP retailers:   {summary_df['total_snap'].sum():,.0f}")
print(f"  Total gardens:          {summary_df['total_gardens'].sum():,.0f}")
print(f"\n  Thresholds used:")
print(f"  Food Desert < {FOOD_DESERT_THRESHOLD} | At Risk {FOOD_DESERT_THRESHOLD}–{GOOD_ACCESS_THRESHOLD} | Good Access ≥ {GOOD_ACCESS_THRESHOLD}")