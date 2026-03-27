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

places_df = pd.DataFrame(list(db["places"].find()))
arrests_df = pd.DataFrame(list(db["arrests"].find()))
inspections_df = pd.DataFrame(list(db["inspections"].find()))
census_df = pd.DataFrame(list(db["census"].find()))

print(f"  ✓ Places: {len(places_df)} records")
print(f"  ✓ Arrests: {len(arrests_df)} records")
print(f"  ✓ Inspections: {len(inspections_df)} records")
print(f"  ✓ Census: {len(census_df)} records")

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
print(census_summary[["neighborhood", "total_population", "avg_median_income", "poverty_rate"]])

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

# ── 5. CLEAN & AGGREGATE ARRESTS DATA ────────────────────────────────────────
print("\n🚔 Aggregating arrests by neighborhood...")

arrests_df = arrests_df.drop_duplicates(subset="arrest_key")
print(f"  ✓ Arrests after dedup: {len(arrests_df)}")

total_arrests = arrests_df.groupby("neighborhood").size().rename("total_arrests")

top_offenses = arrests_df["ofns_desc"].value_counts().head(5).index.tolist()
offense_counts = arrests_df.groupby(["neighborhood", "ofns_desc"]).size().unstack(fill_value=0)
offense_summary = offense_counts[[o for o in top_offenses if o in offense_counts.columns]].reset_index()

arrests_summary = total_arrests.reset_index().merge(offense_summary, on="neighborhood", how="left")

# ── 6. CLEAN & AGGREGATE INSPECTIONS DATA ────────────────────────────────────
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
    grade_counts["pct_grade_A"] = (grade_counts["grade_A_count"] / total_graded.replace(0, 1) * 100).round(1)

# ── 7. MERGE ALL SUMMARIES ────────────────────────────────────────────────────
print("\n🔗 Merging all summaries...")

summary_df = places_summary.merge(arrests_summary, on="neighborhood", how="left")
summary_df = summary_df.merge(grade_counts, on="neighborhood", how="left")
summary_df = summary_df.merge(census_summary, on="neighborhood", how="left")

# ── 8. NORMALIZE BY POPULATION ───────────────────────────────────────────────
print("\n📐 Normalizing metrics by population...")

summary_df["arrests_per_10k"] = (
    summary_df["total_arrests"] / summary_df["total_population"] * 10000
).round(1)

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

# ── 9. COMPUTE POPULATION-NORMALIZED RATIO ───────────────────────────────────
print("\n📊 Computing population-normalized grocery to fast food ratio...")

summary_df["grocery_to_fastfood_ratio"] = (
    summary_df["grocery_per_10k"] / summary_df["fastfood_per_10k"].replace(0, 1)
).round(2)

# ── 10. FOOD DESERT CLASSIFICATION ───────────────────────────────────────────
print("\n🏜️  Classifying food deserts...")

summary_df["is_food_desert"] = (
    (summary_df["grocery_to_fastfood_ratio"] < 1.0) |
    (summary_df["avg_grocery store_rating"] < 3.5)
)

# ── 11. SAVE TO MONGODB ───────────────────────────────────────────────────────
print("\n💾 Saving summary to MongoDB...")

db["summary"].delete_many({})
records = summary_df.to_dict(orient="records")
db["summary"].insert_many(records)

print(f"✅ Summary collection saved with {len(records)} neighborhood records")

print("\n📋 Final Summary Table:")
print(summary_df[[
    "neighborhood",
    "total_population",
    "avg_median_income",
    "poverty_rate",
    "grocery_per_10k",
    "fastfood_per_10k",
    "bodega_per_10k",
    "grocery_to_fastfood_ratio",
    "avg_google_rating",
    "total_arrests",
    "arrests_per_10k",
    "pct_grade_A",
    "is_food_desert"
]].sort_values("arrests_per_10k", ascending=False).to_string(index=False))