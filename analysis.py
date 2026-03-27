import pandas as pd
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv
from config import MONGO_URI, DB_NAME

load_dotenv()

# Connect to MongoDB
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]

# Load summary collection
summary_df = pd.DataFrame(list(db["summary"].find()))
arrests_df = pd.DataFrame(list(db["arrests"].find()))
places_df = pd.DataFrame(list(db["places"].find()))

# Clean up MongoDB _id field
summary_df = summary_df.drop(columns=["_id"], errors="ignore")
arrests_df = arrests_df.drop(columns=["_id"], errors="ignore")
places_df = places_df.drop(columns=["_id"], errors="ignore")

print("=" * 70)
print("NYC FOOD DESERT & CRIME ANALYSIS")
print("=" * 70)

# ── Q1: WHICH NEIGHBORHOODS QUALIFY AS FOOD DESERTS? ─────────────────────────
print("\n📍 Q1: Food Desert Classification")
print("-" * 70)
print("Definition: grocery_to_fastfood_ratio < 1.0 OR avg grocery rating < 3.5")
print()

q1 = summary_df[[
    "neighborhood",
    "grocery store_count",
    "fast food_count",
    "grocery_to_fastfood_ratio",
    "grocery_per_10k",
    "fastfood_per_10k",
    "avg_grocery store_rating",
    "is_food_desert"
]].sort_values("grocery_to_fastfood_ratio")

q1 = q1.rename(columns={
    "grocery store_count": "grocery_count",
    "avg_grocery store_rating": "avg_grocery_rating"
})

print(q1.to_string(index=False))

food_deserts = summary_df[summary_df["is_food_desert"] == True]["neighborhood"].tolist()
non_deserts = summary_df[summary_df["is_food_desert"] == False]["neighborhood"].tolist()
print(f"\n🏜️  Food Deserts: {', '.join(food_deserts)}")
print(f"✅ Non-Deserts:  {', '.join(non_deserts)}")

# ── Q2: FOOD DESERT VS OVERALL CRIME RATE ────────────────────────────────────
print("\n\n📍 Q2: Food Desert Classification vs Crime Rate")
print("-" * 70)

q2 = summary_df[[
    "neighborhood",
    "is_food_desert",
    "total_arrests",
    "arrests_per_10k",
    "poverty_rate",
    "avg_median_income"
]].sort_values("arrests_per_10k", ascending=False)

print(q2.to_string(index=False))

desert_avg = summary_df[summary_df["is_food_desert"] == True]["arrests_per_10k"].mean()
non_desert_avg = summary_df[summary_df["is_food_desert"] == False]["arrests_per_10k"].mean()

print(f"\n📊 Avg arrests per 10k — Food Deserts:     {desert_avg:.1f}")
print(f"📊 Avg arrests per 10k — Non-Food Deserts: {non_desert_avg:.1f}")
print(f"📊 Food deserts have {desert_avg/non_desert_avg:.1f}x more arrests per capita")

# ── Q3: FAST FOOD TO GROCERY RATIO VS INCOME ─────────────────────────────────
print("\n\n📍 Q3: Fast Food to Grocery Ratio vs Median Income")
print("-" * 70)

q3 = summary_df[[
    "neighborhood",
    "avg_median_income",
    "poverty_rate",
    "grocery_to_fastfood_ratio",
    "grocery_per_10k",
    "fastfood_per_10k"
]].sort_values("avg_median_income")

print(q3.to_string(index=False))

# Correlation between income and ratio
corr = summary_df["avg_median_income"].corr(summary_df["grocery_to_fastfood_ratio"])
print(f"\n📊 Correlation between median income and grocery/fastfood ratio: {corr:.2f}")
print("(1.0 = perfect positive, -1.0 = perfect negative, 0 = no correlation)")

# ── Q4: FOOD DESERT VS TRANSIT ACCESS ────────────────────────────────────────
print("\n\n📍 Q4: Food Desert Classification vs Borough")
print("-" * 70)
print("Note: Transit access data not available via free API.")
print("Reporting borough-level breakdown as proxy for geographic context.\n")

# Add borough manually based on neighborhood
borough_map = {
    "South Bronx": "Bronx",
    "East New York": "Brooklyn",
    "Washington Heights": "Manhattan",
    "Astoria": "Queens",
    "Upper West Side": "Manhattan",
    "Park Slope": "Brooklyn"
}

summary_df["borough"] = summary_df["neighborhood"].map(borough_map)

q4 = summary_df[[
    "neighborhood",
    "borough",
    "is_food_desert",
    "grocery_per_10k",
    "arrests_per_10k",
    "poverty_rate"
]].sort_values("borough")

print(q4.to_string(index=False))

# ── Q5: BOROUGH COMPARISON ───────────────────────────────────────────────────
print("\n\n📍 Q5: Food Environment Comparison Across Boroughs")
print("-" * 70)

q5 = summary_df.groupby("borough").agg(
    neighborhoods=("neighborhood", "count"),
    avg_grocery_ratio=("grocery_to_fastfood_ratio", "mean"),
    avg_grocery_per_10k=("grocery_per_10k", "mean"),
    avg_arrests_per_10k=("arrests_per_10k", "mean"),
    avg_income=("avg_median_income", "mean"),
    avg_poverty_rate=("poverty_rate", "mean"),
    avg_google_rating=("avg_google_rating", "mean"),
    avg_pct_grade_A=("pct_grade_A", "mean")
).reset_index()

q5 = q5.round(2).sort_values("avg_arrests_per_10k", ascending=False)
print(q5.to_string(index=False))

# ── BONUS: TOP OFFENSE TYPES IN FOOD DESERTS VS NON-DESERTS ──────────────────
print("\n\n📍 BONUS: Top 5 Offense Types — Food Deserts vs Non-Food Deserts")
print("-" * 70)

arrests_df = arrests_df.drop_duplicates(subset="arrest_key")

desert_neighborhoods = food_deserts
non_desert_neighborhoods = non_deserts

desert_arrests = arrests_df[arrests_df["neighborhood"].isin(desert_neighborhoods)]
non_desert_arrests = arrests_df[arrests_df["neighborhood"].isin(non_desert_neighborhoods)]

desert_top = desert_arrests["ofns_desc"].value_counts().head(5).reset_index()
desert_top.columns = ["offense", "count_food_deserts"]

non_desert_top = non_desert_arrests["ofns_desc"].value_counts().head(5).reset_index()
non_desert_top.columns = ["offense", "count_non_deserts"]

offense_comparison = desert_top.merge(non_desert_top, on="offense", how="outer").fillna(0)
offense_comparison["count_food_deserts"] = offense_comparison["count_food_deserts"].astype(int)
offense_comparison["count_non_deserts"] = offense_comparison["count_non_deserts"].astype(int)
print(offense_comparison.to_string(index=False))

print("\n\n✅ Analysis complete.")