import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv
import os
import warnings
warnings.filterwarnings("ignore")

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME   = os.getenv("DB_NAME", "fooddesert")

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db     = client[DB_NAME]

summary_df = pd.DataFrame(list(db["summary"].find())).drop(columns=["_id"], errors="ignore")
print(f"Loaded {len(summary_df)} neighborhoods")
print(summary_df["access_tier"].value_counts().to_string())

# Paul Tol colorblind-safe palette
# Distinguishable by deuteranopia, protanopia, and tritanopia
tier_colors = {
    "Good Access": "#0077BB",   # blue
    "At Risk":     "#EE7733",   # orange
    "Food Desert": "#CC3311",   # vermillion
}
borough_order   = ["Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"]
# Borough palette — also colorblind-safe (Tol bright)
borough_palette = {
    "Bronx":         "#AA3377",   # purple
    "Brooklyn":      "#0077BB",   # blue
    "Manhattan":     "#EE7733",   # orange
    "Queens":        "#009988",   # teal
    "Staten Island": "#BBBBBB",   # gray
}
tier_order = ["Food Desert", "At Risk", "Good Access"]

FOOD_DESERT_THRESHOLD = 25.2
GOOD_ACCESS_THRESHOLD = 54.7


# ════════════════════════════════════════════════════════════
# Q1 — Grouped by borough, sorted within borough by score
# ════════════════════════════════════════════════════════════
print("\nGenerating Q1...")

# Sort: borough order first, then score ascending within each borough
borough_sort = {b: i for i, b in enumerate(borough_order)}
q1_df = summary_df.dropna(subset=["food_access_score"]).copy()
q1_df["borough_rank"] = q1_df["borough"].map(borough_sort)
q1_df = q1_df.sort_values(["borough_rank", "food_access_score"], ascending=[True, True])

fig, ax = plt.subplots(figsize=(14, 26))
fig.suptitle(
    "Q1: NYC Neighborhood Food Access Score Rankings\n"
    "Grouped by Borough — Sorted Low to High Within Each Group",
    fontsize=14, fontweight="bold"
)

bar_colors = [tier_colors[t] for t in q1_df["access_tier"]]
bars = ax.barh(range(len(q1_df)), q1_df["food_access_score"],
               color=bar_colors, height=0.75)

# Borough dividers and labels
current_borough = None
borough_start   = 0
y_positions     = list(range(len(q1_df)))

for i, (_, row) in enumerate(q1_df.iterrows()):
    if row["borough"] != current_borough:
        if current_borough is not None:
            # Divider line
            ax.axhline(y=i - 0.5, color="#cccccc", linewidth=1.2, linestyle="-")
            # Borough label on the right side
            mid = (borough_start + i - 1) / 2
            ax.text(ax.get_xlim()[1] if ax.get_xlim()[1] > 0 else 170,
                    mid, current_borough,
                    va="center", ha="left", fontsize=8, fontweight="bold",
                    color=borough_palette.get(current_borough, "#333333"),
                    style="italic")
        current_borough = row["borough"]
        borough_start   = i

# Last borough label
if current_borough:
    mid = (borough_start + len(q1_df) - 1) / 2
    ax.text(170, mid, current_borough,
            va="center", ha="left", fontsize=8, fontweight="bold",
            color=borough_palette.get(current_borough, "#333333"),
            style="italic")

# Threshold lines
ax.axvline(x=GOOD_ACCESS_THRESHOLD, color="#0077BB", linestyle="--",
           linewidth=1.5, alpha=0.9)
ax.axvline(x=FOOD_DESERT_THRESHOLD, color="#EE7733", linestyle="--",
           linewidth=1.5, alpha=0.9)

# Neighborhood labels on y axis
ax.set_yticks(range(len(q1_df)))
ax.set_yticklabels(q1_df["neighborhood"], fontsize=6.5)

# Score labels on bars
for i, (bar, val) in enumerate(zip(bars, q1_df["food_access_score"])):
    if pd.notna(val):
        ax.text(val + 0.8, i, f"{val:.1f}",
                va="center", fontsize=5.5, fontweight="bold", color="#333333")

ax.set_xlabel("Food Access Score", fontsize=11)
ax.set_xlim(0, 175)
ax.set_title(
    f"Food Desert <{FOOD_DESERT_THRESHOLD}  |  "
    f"At Risk {FOOD_DESERT_THRESHOLD}–{GOOD_ACCESS_THRESHOLD}  |  "
    f"Good Access ≥{GOOD_ACCESS_THRESHOLD}  "
    f"(thresholds back-calculated from archetypes)",
    fontsize=8.5
)

legend_elements = [
    Patch(facecolor="#0077BB", label=f"Good Access — {(summary_df['access_tier']=='Good Access').sum()} neighborhoods"),
    Patch(facecolor="#EE7733", label=f"At Risk — {(summary_df['access_tier']=='At Risk').sum()} neighborhoods"),
    Patch(facecolor="#CC3311", label=f"Food Desert — {(summary_df['access_tier']=='Food Desert').sum()} neighborhoods"),
]
ax.legend(handles=legend_elements, fontsize=8, loc="lower right")
plt.tight_layout()
plt.savefig("q1_food_access_score.png", dpi=150, bbox_inches="tight")
plt.close()
print("Q1 saved")


# ════════════════════════════════════════════════════════════
# Q2 — Scatter: Food Access Score vs Poverty Rate
# ════════════════════════════════════════════════════════════
print("Generating Q2...")

fig, axes = plt.subplots(1, 2, figsize=(18, 8))
fig.suptitle(
    "Q2: Food Access Score vs Socioeconomic Indicators\n"
    "High poverty does not predict food desert status in NYC — density matters more",
    fontsize=13, fontweight="bold"
)

plot_df = summary_df.dropna(subset=["food_access_score", "poverty_rate", "avg_median_income"])

# ── Left: Score vs Poverty Rate ─────────────────────────────
for tier in tier_order:
    sub = plot_df[plot_df["access_tier"] == tier]
    axes[0].scatter(
        sub["poverty_rate"],
        sub["food_access_score"],
        c=tier_colors[tier],
        label=f"{tier} (n={len(sub)})",
        alpha=0.8, s=60, edgecolors="white", linewidths=0.5, zorder=3
    )

# Threshold lines
axes[0].axhline(y=GOOD_ACCESS_THRESHOLD, color="#0077BB", linestyle="--",
                linewidth=1.2, alpha=0.7, label=f"Good Access threshold ({GOOD_ACCESS_THRESHOLD})")
axes[0].axhline(y=FOOD_DESERT_THRESHOLD, color="#EE7733", linestyle="--",
                linewidth=1.2, alpha=0.7, label=f"Food Desert threshold ({FOOD_DESERT_THRESHOLD})")

# Annotate notable neighborhoods
notable = [
    ("South Bronx",   "right"),
    ("Hunts Point",   "right"),
    ("Murray Hill",   "left"),
    ("Bayside",       "left"),
    ("Sunset Park",   "right"),
    ("Longwood",      "right"),
]
for name, ha in notable:
    row = plot_df[plot_df["neighborhood"] == name]
    if not row.empty:
        axes[0].annotate(
            name,
            xy=(row["poverty_rate"].values[0], row["food_access_score"].values[0]),
            xytext=(8 if ha == "right" else -8, 0),
            textcoords="offset points",
            fontsize=7, ha=ha, va="center", color="#333333",
            arrowprops=dict(arrowstyle="-", color="#aaaaaa", lw=0.5)
        )

axes[0].set_xlabel("Poverty Rate (%)", fontsize=11)
axes[0].set_ylabel("Food Access Score", fontsize=11)
axes[0].set_title(
    "Score vs Poverty Rate\n"
    "Key finding: high-poverty neighborhoods can score high\n"
    "if they have dense walkable grocery infrastructure",
    fontsize=9
)
axes[0].legend(fontsize=8, loc="upper right")
axes[0].grid(True, alpha=0.2)

# ── Right: Score vs Median Income ───────────────────────────
for tier in tier_order:
    sub = plot_df[plot_df["access_tier"] == tier]
    axes[1].scatter(
        sub["avg_median_income"] / 1000,
        sub["food_access_score"],
        c=tier_colors[tier],
        label=f"{tier} (n={len(sub)})",
        alpha=0.8, s=60, edgecolors="white", linewidths=0.5, zorder=3
    )

axes[1].axhline(y=GOOD_ACCESS_THRESHOLD, color="#0077BB", linestyle="--",
                linewidth=1.2, alpha=0.7)
axes[1].axhline(y=FOOD_DESERT_THRESHOLD, color="#EE7733", linestyle="--",
                linewidth=1.2, alpha=0.7)

for name, ha in notable:
    row = plot_df[plot_df["neighborhood"] == name]
    if not row.empty:
        axes[1].annotate(
            name,
            xy=(row["avg_median_income"].values[0] / 1000, row["food_access_score"].values[0]),
            xytext=(8 if ha == "right" else -8, 0),
            textcoords="offset points",
            fontsize=7, ha=ha, va="center", color="#333333",
            arrowprops=dict(arrowstyle="-", color="#aaaaaa", lw=0.5)
        )

axes[1].set_xlabel("Median Household Income ($000s)", fontsize=11)
axes[1].set_ylabel("Food Access Score", fontsize=11)
axes[1].set_title(
    "Score vs Median Income\n"
    "Key finding: high income does not guarantee good access\n"
    "(Murray Hill: $153k income, Food Desert score)",
    fontsize=9
)
axes[1].legend(fontsize=8, loc="upper right")
axes[1].grid(True, alpha=0.2)

plt.tight_layout()
plt.savefig("q2_score_vs_socioeconomic.png", dpi=150, bbox_inches="tight")
plt.close()
print("Q2 saved")


# ════════════════════════════════════════════════════════════
# Q3 — Score Components by Access Tier
# ════════════════════════════════════════════════════════════
print("Generating Q3...")

q3_df = summary_df.groupby("access_tier").agg(
    avg_snap_grocery    = ("snap_grocery_per_10k",    "mean"),
    avg_nonsnap_grocery = ("nonsnap_grocery_per_10k", "mean"),
    avg_conv_ratio      = ("snap_convenience_ratio",  "mean"),
    avg_food_balance    = ("food_balance_ratio",      "mean"),
    avg_year_round      = ("year_round_markets",      "mean"),
    avg_pct_grade_A     = ("pct_grade_A",             "mean"),
    avg_income          = ("avg_median_income",       "mean"),
    avg_econ_gap        = ("economic_access_gap",     "mean"),
).reindex(tier_order).reset_index()

fig, axes = plt.subplots(2, 4, figsize=(22, 11))
fig.suptitle(
    "Q3: What Variables Drive Food Desert Status?\n"
    "Average metric values for each access tier across 116 NYC neighborhoods",
    fontsize=14, fontweight="bold"
)

colors_q3 = ["#CC3311", "#EE7733", "#0077BB"]  # Paul Tol colorblind-safe

def labeled_bar(ax, labels, values, title, ylabel, fmt="{:.2f}", note=None):
    bars = ax.bar(labels, values, color=colors_q3, width=0.5)
    ax.set_title(title, fontsize=8.5, fontweight="bold", pad=6)
    ax.set_ylabel(ylabel, fontsize=7.5)
    ax.tick_params(axis="x", labelsize=8)
    ax.tick_params(axis="y", labelsize=7)
    for bar, val in zip(bars, values):
        if pd.notna(val):
            offset = max(val * 0.03, 0.3)
            ax.text(bar.get_x() + bar.get_width()/2, val + offset,
                    fmt.format(val), ha="center", fontsize=8, fontweight="bold")
    if note:
        ax.text(0.5, -0.22, note, transform=ax.transAxes,
                fontsize=6.5, ha="center", color="#666666", style="italic")

labeled_bar(axes[0,0], q3_df["access_tier"], q3_df["avg_snap_grocery"],
            "SNAP Grocery Stores per 10k\n(incl. ethnic grocery reclassification)",
            "Count per 10k residents",
            note="Higher = better access")

labeled_bar(axes[0,1], q3_df["access_tier"], q3_df["avg_nonsnap_grocery"],
            "Non-SNAP Grocery per 10k\n(size-weighted: supermarket ×1.0, mid ×0.6, small ×0.3)",
            "Weighted count per 10k",
            note="Higher = better access")

labeled_bar(axes[0,2], q3_df["access_tier"], q3_df["avg_conv_ratio"],
            "Convenience Store % of SNAP Retailers\n(formula penalty term)",
            "% of SNAP stores that are convenience stores",
            fmt="{:.1f}%",
            note="Higher % = worse food environment (more bodegas, fewer grocers)")

labeled_bar(axes[0,3], q3_df["access_tier"], q3_df["avg_food_balance"],
            "Food Balance Ratio\n(SNAP grocery per 10k ÷ fast food per 10k)",
            "Ratio (>1 = more groceries than fast food)",
            fmt="{:.2f}",
            note="Higher ratio = healthier food environment balance")

labeled_bar(axes[1,0], q3_df["access_tier"], q3_df["avg_year_round"],
            "Year-Round Farmers Markets\n(raw count per neighborhood)",
            "Average count",
            note="Higher = more reliable fresh produce access year-round")

labeled_bar(axes[1,1], q3_df["access_tier"], q3_df["avg_pct_grade_A"],
            "% Grade A Health Inspections\n(NYC-specific — not portable to other cities)",
            "% of restaurants with Grade A",
            fmt="{:.1f}%",
            note="NYC DOHMH letter grade system only. Higher = better food safety")

labeled_bar(axes[1,2], q3_df["access_tier"], q3_df["avg_income"],
            "Avg Median Household Income\nNote: high-poverty ≠ food desert in dense NYC",
            "Median income ($)",
            fmt="${:,.0f}",
            note="Poverty rate is a poor predictor of food access in dense urban areas")

labeled_bar(axes[1,3], q3_df["access_tier"], q3_df["avg_econ_gap"],
            "Economic Access Gap\n(income-based affordability penalty)",
            "Gap score (0 = no barrier, 2 = severe barrier)",
            fmt="{:.2f}",
            note="0 = income comfortably supports food costs  |  2 = severe affordability pressure")

plt.tight_layout(rect=[0, 0.02, 1, 1])
plt.savefig("q3_score_components.png", dpi=150, bbox_inches="tight")
plt.close()
print("Q3 saved")


# ════════════════════════════════════════════════════════════
# Q4 — Borough Comparison (population-weighted)
# ════════════════════════════════════════════════════════════
print("Generating Q4...")

# Population-weighted average score per borough
def pop_weighted_mean(group, value_col, weight_col):
    return (group[value_col] * group[weight_col]).sum() / group[weight_col].sum()

q4_rows = []
for borough in borough_order:
    sub = summary_df[summary_df["borough"] == borough].dropna(subset=["food_access_score", "total_population"])
    if sub.empty:
        continue
    q4_rows.append({
        "borough":           borough,
        "avg_score_popwt":   pop_weighted_mean(sub, "food_access_score", "total_population"),
        "avg_snap_grocery":  pop_weighted_mean(sub, "snap_grocery_per_10k", "total_population"),
        "avg_conv_ratio":    pop_weighted_mean(sub, "snap_convenience_ratio", "total_population"),
        "avg_poverty":       pop_weighted_mean(sub, "poverty_rate", "total_population"),
        "food_deserts":      sub["our_food_desert"].sum(),
        "at_risk":           (sub["access_tier"] == "At Risk").sum(),
        "good_access":       (sub["access_tier"] == "Good Access").sum(),
        "n_neighborhoods":   len(sub),
        "total_population":  sub["total_population"].sum(),
    })

q4_df = pd.DataFrame(q4_rows)

fig, axes = plt.subplots(2, 2, figsize=(18, 13))
fig.suptitle(
    "Q4: Food Access Across NYC Boroughs\n"
    "Population-weighted averages — borough sizes noted (neighborhood counts vary)",
    fontsize=14, fontweight="bold"
)

colors_boro = [borough_palette[b] for b in q4_df["borough"]]
x     = range(len(q4_df))
width = 0.25

# Chart 1 — Population-weighted avg score
bars = axes[0,0].bar(q4_df["borough"], q4_df["avg_score_popwt"], color=colors_boro)
axes[0,0].axhline(y=GOOD_ACCESS_THRESHOLD, color="#0077BB", linestyle="--",
                  linewidth=1.5, alpha=0.8, label=f"Good Access ({GOOD_ACCESS_THRESHOLD})")
axes[0,0].axhline(y=FOOD_DESERT_THRESHOLD, color="#EE7733", linestyle="--",
                  linewidth=1.5, alpha=0.8, label=f"Food Desert ({FOOD_DESERT_THRESHOLD})")
axes[0,0].set_title(
    "Population-Weighted Avg Food Access Score\n"
    "(weighted by neighborhood population — not neighborhood count)",
    fontweight="bold", fontsize=9
)
axes[0,0].set_ylabel("Avg Food Access Score")
axes[0,0].tick_params(axis="x", rotation=15)
axes[0,0].legend(fontsize=8)
for bar, row in zip(bars, q4_df.itertuples()):
    axes[0,0].text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height() + 0.5,
        f"{row.avg_score_popwt:.1f}\n(n={row.n_neighborhoods})",
        ha="center", fontsize=7.5, fontweight="bold"
    )

# Chart 2 — Tier distribution
axes[0,1].bar([i - width for i in x], q4_df["food_deserts"],
              width, label="Food Desert", color="#CC3311")
axes[0,1].bar([i         for i in x], q4_df["at_risk"],
              width, label="At Risk",     color="#EE7733")
axes[0,1].bar([i + width for i in x], q4_df["good_access"],
              width, label="Good Access", color="#0077BB")
axes[0,1].set_xticks(list(x))
axes[0,1].set_xticklabels(q4_df["borough"], rotation=15)
axes[0,1].set_title(
    "Access Tier Distribution by Borough\n"
    "Note: Queens has 40 neighborhoods vs Bronx 19 — raw counts reflect this",
    fontweight="bold", fontsize=9
)
axes[0,1].set_ylabel("Number of Neighborhoods")
axes[0,1].legend(fontsize=9)
for i, row in enumerate(q4_df.itertuples()):
    axes[0,1].text(i, row.food_deserts + row.at_risk + row.good_access + 0.2,
                   f"n={row.n_neighborhoods}", ha="center", fontsize=7.5, color="#555555")

# Chart 3 — Population-weighted SNAP grocery per 10k
bars = axes[1,0].bar(q4_df["borough"], q4_df["avg_snap_grocery"], color=colors_boro)
axes[1,0].set_title(
    "Population-Weighted SNAP Grocery Stores per 10k\n"
    "(includes 457 ethnic grocery reclassifications)",
    fontweight="bold", fontsize=9
)
axes[1,0].set_ylabel("Weighted avg count per 10k residents")
axes[1,0].tick_params(axis="x", rotation=15)
for bar, val in zip(bars, q4_df["avg_snap_grocery"]):
    axes[1,0].text(bar.get_x() + bar.get_width()/2, val + 0.05,
                   f"{val:.2f}", ha="center", fontsize=9, fontweight="bold")

# Chart 4 — Poverty rate vs convenience ratio (population-weighted)
ax4  = axes[1,1]
ax4b = ax4.twinx()
bars1 = ax4.bar([i - width/2 for i in x], q4_df["avg_poverty"],
                width, label="Avg Poverty Rate (%)", color="#9b59b6", alpha=0.8)
bars2 = ax4b.bar([i + width/2 for i in x], q4_df["avg_conv_ratio"],
                 width, label="Convenience Store %\nof SNAP retailers", color="#CC3311", alpha=0.6)
ax4.set_xticks(list(x))
ax4.set_xticklabels(q4_df["borough"], rotation=15)
ax4.set_title(
    "Poverty Rate vs Convenience Store Dominance\n"
    "(population-weighted | higher convenience % = worse food environment)",
    fontweight="bold", fontsize=9
)
ax4.set_ylabel("Poverty Rate (%)", color="#9b59b6")
ax4b.set_ylabel("Convenience Store % of SNAP retailers", color="#CC3311")
ax4.legend(loc="upper left", fontsize=8)
ax4b.legend(loc="upper right", fontsize=8)

plt.tight_layout()
plt.savefig("q4_borough_comparison.png", dpi=150, bbox_inches="tight")
plt.close()
print("Q4 saved")

print("\nAll charts saved:")
print("  q1_food_access_score.png")
print("  q2_score_vs_socioeconomic.png")
print("  q3_score_components.png")
print("  q4_borough_comparison.png")

import subprocess
subprocess.run(["open",
    "q1_food_access_score.png",
    "q2_score_vs_socioeconomic.png",
    "q3_score_components.png",
    "q4_borough_comparison.png"
])