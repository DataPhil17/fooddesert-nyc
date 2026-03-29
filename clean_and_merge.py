import pandas as pd
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv
from config import MONGO_URI, DB_NAME

load_dotenv()

# Connect to MongoDB
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]

# ── 1. LOAD COLLECTIONS INTO DATAFRAMES ──────────────────────────────────────
print("📦 Loading collections from MongoDB...")

places_df = pd.DataFrame(list(db["places"].find())).drop(columns=["_id"], errors="ignore")
inspections_df = pd.DataFrame(list(db["inspections"].find())).drop(columns=["_id"], errors="ignore")
census_df = pd.DataFrame(list(db["census"].find())).drop(columns=["_id"], errors="ignore")
usda_df = pd.DataFrame(list(db["usda"].find())).drop(columns=["_id"], errors="ignore")
farmers_df = pd.DataFrame(list(db["farmers_markets"].find())).drop(columns=["_id"], errors="ignore")
snap_df = pd.DataFrame(list(db["snap_retailers"].find())).drop(columns=["_id"], errors="ignore")

print(f"  ✓ Places:          {len(places_df):,} records")
print(f"  ✓ Inspections:     {len(inspections_df):,} records")
print(f"  ✓ Census:          {len(census_df):,} records")
print(f"  ✓ USDA:            {len(usda_df):,} records")
print(f"  ✓ Farmers Markets: {len(farmers_df):,} records")
print(f"  ✓ SNAP Retailers:  {len(snap_df):,} records")

# ── 2. AGGREGATE CENSUS BY NEIGHBORHOOD ──────────────────────────────────────
print("\n🏘️  Aggregating census data by neighborhood...")

census_summary = census_df.groupby("neighborhood").agg(
    total_population=("population", "sum"),
    avg_median_income=("median_income", "mean"),
    total_poverty=("poverty_count", "sum")
).reset_index()

census_summary["poverty_rate"] = (
    census_summary["total_poverty"] / census_summary["total_population"] * 100
).round(1)

census_summary["avg_median_income"] = census_summary["avg_median_income"].round(0).astype(int)

# ── 3. CLEAN PLACES DATA ─────────────────────────────────────────────────────
print("\n🧹 Cleaning places data...")

places_df["rating"] = pd.to_numeric(places_df["rating"], errors="coerce")
places_df["user_ratings_total"] = pd.to_numeric(places_df["user_ratings_total"], errors="coerce")
places_df["price_level"] = pd.to_numeric(places_df["price_level"], errors="coerce")
places_df = places_df.drop_duplicates(subset="place_id")
print(f"  ✓ Places after dedup: {len(places_df)}")

# ── 4. AGGREGATE PLACES BY NEIGHBORHOOD ──────────────────────────────────────
print("\n📊 Aggregating places by neighborhood...")

type_counts = places_df.groupby(["neighborhood", "search_type"]).size().unstack(fill_value=0)
type_counts.columns = [f"{col}_count" for col in type_counts.columns]

avg_ratings = places_df.groupby("neighborhood")["rating"].mean().rename("avg_google_rating")

avg_rating_by_type = places_df.groupby(["neighborhood", "search_type"])["rating"].mean().unstack()
avg_rating_by_type.columns = [f"avg_{col}_rating" for col in avg_rating_by_type.columns]

avg_price = places_df.groupby("neighborhood")["price_level"].mean().rename("avg_price_level")

places_summary = pd.concat([type_counts, avg_ratings, avg_rating_by_type, avg_price], axis=1).reset_index()

# ── 5. CLEAN & AGGREGATE INSPECTIONS DATA ────────────────────────────────────
print("\n🏥 Aggregating inspections by neighborhood...")

inspections_df["inspection_date"] = pd.to_datetime(inspections_df["inspection_date"], errors="coerce")
inspections_df = inspections_df.sort_values("inspection_date", ascending=False)
inspections_df = inspections_df.drop_duplicates(subset="camis")
print(f"  ✓ Unique restaurants after dedup: {len(inspections_df)}")

grade_counts = inspections_df.groupby(["neighborhood", "grade"]).size().unstack(fill_value=0)
grade_cols = [c for c in ["A", "B", "C"] if c in grade_counts.columns]
grade_counts = grade_counts[grade_cols].reset_index()
grade_counts.columns = ["neighborhood"] + [f"grade_{c}_count" for c in grade_cols]

if "grade_A_count" in grade_counts.columns:
    total_graded = grade_counts[[c for c in grade_counts.columns if "grade_" in c]].sum(axis=1)
    grade_counts["pct_grade_A"] = (
        grade_counts["grade_A_count"] / total_graded.replace(0, 1) * 100
    ).round(1)

# ── 6. AGGREGATE FARMERS MARKETS BY NEIGHBORHOOD ─────────────────────────────
print("\n🌽 Aggregating farmers markets by neighborhood...")

farmers_matched = farmers_df[farmers_df["neighborhood"].notna()]

farmers_summary = farmers_matched.groupby("neighborhood").agg(
    total_farmers_markets=("marketname", "count"),
    ebt_accepted_markets=("accepts_ebt", lambda x: (x == "Yes").sum()),
    year_round_markets=("open_year_round", lambda x: (x == "Yes").sum())
).reset_index()

farmers_summary["pct_ebt_markets"] = (
    farmers_summary["ebt_accepted_markets"] / farmers_summary["total_farmers_markets"] * 100
).round(1)

print(f"  ✓ Neighborhoods with farmers markets: {len(farmers_summary)}")

# ── 7. AGGREGATE SNAP RETAILERS BY NEIGHBORHOOD ───────────────────────────────
print("\n🏪 Aggregating SNAP retailers by neighborhood...")

snap_summary = snap_df.groupby("neighborhood").agg(
    total_snap_retailers=("Record_ID", "count"),
    snap_convenience_stores=("Store_Type", lambda x: (x == "Convenience Store").sum()),
    snap_grocery_stores=("Store_Type", lambda x: (x == "Grocery Store").sum()),
    snap_supermarkets=("Store_Type", lambda x: (x == "Supermarket").sum()),
    snap_super_stores=("Store_Type", lambda x: (x == "Super Store").sum()),
).reset_index()

snap_summary["snap_healthy_ratio"] = (
    (snap_summary["snap_grocery_stores"] + snap_summary["snap_supermarkets"] + snap_summary["snap_super_stores"]) /
    snap_summary["total_snap_retailers"].replace(0, 1) * 100
).round(1)

print(f"  ✓ Neighborhoods with SNAP data: {len(snap_summary)}")

# ── 8. AGGREGATE USDA DATA BY BOROUGH ────────────────────────────────────────
print("\n🗺️  Aggregating USDA data by borough...")

county_to_borough = {
    "New York County": "Manhattan",
    "Kings County": "Brooklyn",
    "Queens County": "Queens",
    "Bronx County": "Bronx",
    "Richmond County": "Staten Island"
}

borough_map = {
    "South Bronx": "Bronx", "Highbridge": "Bronx", "Belmont": "Bronx", "Riverdale": "Bronx",
    "East New York": "Brooklyn", "Bedford-Stuyvesant": "Brooklyn", "Crown Heights": "Brooklyn",
    "Coney Island": "Brooklyn", "Bensonhurst": "Brooklyn", "Sheepshead Bay": "Brooklyn",
    "Clinton Hill / Fort Greene": "Brooklyn", "Red Hook": "Brooklyn", "Park Slope": "Brooklyn",
    "Midwood": "Brooklyn",
    "Washington Heights": "Manhattan", "Harlem": "Manhattan",
    "Chinatown / Lower East Side": "Manhattan", "Greenwich Village": "Manhattan",
    "Upper West Side": "Manhattan",
    "Astoria": "Queens", "Jamaica": "Queens", "Forest Hills": "Queens",
    "Kew Gardens": "Queens", "Middle Village": "Queens", "Far Rockaway": "Queens",
    "St. George": "Staten Island", "Tottenville": "Staten Island"
}

nyc_counties = list(county_to_borough.keys())
usda_nyc = usda_df[usda_df["County"].isin(nyc_counties)].copy()
usda_nyc["borough"] = usda_nyc["County"].map(county_to_borough)

usda_borough = usda_nyc.groupby("borough").agg(
    total_tracts=("CensusTract", "count"),
    usda_food_deserts=("LILATracts_1And10", "sum"),
    low_income_tracts=("LowIncomeTracts", "sum"),
    avg_poverty_rate=("PovertyRate", "mean"),
    total_snap_recipients=("TractSNAP", "sum"),
    total_no_vehicle=("TractHUNV", "sum")
).reset_index()

usda_borough["pct_usda_food_desert"] = (
    usda_borough["usda_food_deserts"] / usda_borough["total_tracts"] * 100
).round(1)

print("  USDA Food Desert Summary by Borough:")
print(usda_borough[["borough", "total_tracts", "usda_food_deserts", "pct_usda_food_desert", "avg_poverty_rate"]].to_string(index=False))

# ── 9. MERGE ALL SUMMARIES ────────────────────────────────────────────────────
print("\n🔗 Merging all summaries...")

summary_df = places_summary.merge(grade_counts, on="neighborhood", how="left")
summary_df = summary_df.merge(census_summary, on="neighborhood", how="left")
summary_df = summary_df.merge(farmers_summary, on="neighborhood", how="left")
summary_df = summary_df.merge(snap_summary, on="neighborhood", how="left")

# Fill NaN for neighborhoods with no farmers markets
summary_df["total_farmers_markets"] = summary_df["total_farmers_markets"].fillna(0)
summary_df["ebt_accepted_markets"] = summary_df["ebt_accepted_markets"].fillna(0)
summary_df["year_round_markets"] = summary_df["year_round_markets"].fillna(0)

# Add borough
summary_df["borough"] = summary_df["neighborhood"].map(borough_map)

# ── 10. NORMALIZE BY POPULATION ──────────────────────────────────────────────
print("\n📐 Normalizing metrics by population...")

summary_df["grocery_per_10k"] = (
    summary_df["grocery store_count"] / summary_df["total_population"] * 10000
).round(2)

summary_df["fastfood_per_10k"] = (
    summary_df["fast food_count"] / summary_df["total_population"] * 10000
).round(2)

summary_df["bodega_per_10k"] = (
    summary_df["bodega_count"] / summary_df["total_population"] * 10000
).round(2)

summary_df["restaurant_per_10k"] = (
    summary_df["restaurant_count"] / summary_df["total_population"] * 10000
).round(2)

summary_df["snap_retailers_per_10k"] = (
    summary_df["total_snap_retailers"] / summary_df["total_population"] * 10000
).round(2)

summary_df["farmers_markets_per_10k"] = (
    summary_df["total_farmers_markets"] / summary_df["total_population"] * 10000
).round(2)

# ── 11. COMPUTE RATIOS ────────────────────────────────────────────────────────
print("\n📊 Computing food access ratios...")

summary_df["grocery_to_fastfood_ratio"] = (
    summary_df["grocery_per_10k"] / summary_df["fastfood_per_10k"].replace(0, 1)
).round(2)

summary_df["snap_healthy_ratio"] = summary_df["snap_healthy_ratio"].fillna(0)

# ── 12. FOOD ACCESS SCORE ─────────────────────────────────────────────────────
print("\n🧮 Computing Food Access Score...")

summary_df["food_access_score"] = (
    (summary_df["grocery_per_10k"] * 10) +        # Primary access metric
    (summary_df["snap_healthy_ratio"] * 0.5) +     # Quality of food retail
    (summary_df["year_round_markets"] * 1.5) +     # Consistent fresh access
    (summary_df["farmers_markets_per_10k"] * 1) +  # Normalized market access
    (summary_df["pct_grade_A"].fillna(0) * 0.2) -  # Quality signal
    (summary_df["fastfood_per_10k"] * 6) -         # Negative food environment
    (summary_df["poverty_rate"] * 0.3)             # Structural disadvantage
).round(2)

# ── 13. THREE TIER CLASSIFICATION ────────────────────────────────────────────
print("\n🏜️  Classifying food access tiers...")

def classify_access(score):
    if score >= 55:
        return "Good Access"
    elif score >= 40:
        return "At Risk"
    else:
        return "Food Desert"

summary_df["access_tier"] = summary_df["food_access_score"].apply(classify_access)
summary_df["our_food_desert"] = summary_df["access_tier"] == "Food Desert"

# USDA classification by borough
usda_desert_boroughs = usda_borough[usda_borough["usda_food_deserts"] > 0]["borough"].tolist()
summary_df["usda_food_desert"] = summary_df["borough"].apply(
    lambda b: True if b in usda_desert_boroughs else False
)

print("\n  Access Tier Distribution:")
print(summary_df["access_tier"].value_counts().to_string())

# ── 14. SAVE TO MONGODB ───────────────────────────────────────────────────────
print("\n💾 Saving summary to MongoDB...")
# Ensure access_tier is saved as string not bool
summary_df["access_tier"] = summary_df["access_tier"].astype(str)

db["summary"].delete_many({})
records = summary_df.to_dict(orient="records")
db["summary"].insert_many(records)

print(f"✅ Summary collection saved with {len(records)} neighborhood records")

print("\n📋 Final Summary Table:")
print(summary_df[[
    "neighborhood",
    "borough",
    "total_population",
    "avg_median_income",
    "poverty_rate",
    "grocery_per_10k",
    "fastfood_per_10k",
    "bodega_per_10k",
    "grocery_to_fastfood_ratio",
    "total_farmers_markets",
    "year_round_markets",
    "snap_healthy_ratio",
    "pct_grade_A",
    "food_access_score",
    "access_tier",
    "our_food_desert",
    "usda_food_desert"
]].sort_values("food_access_score", ascending=False).to_string(index=False))