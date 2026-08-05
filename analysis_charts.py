import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Patch
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv
import os
import warnings
warnings.filterwarnings("ignore")

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"), tlsCAFile=certifi.where())
db     = client[os.getenv("DB_NAME", "fooddesert")]

summary_df = pd.DataFrame(list(db["summary"].find())).drop(columns=["_id"], errors="ignore")
print(f"Loaded {len(summary_df)} neighborhoods")
print(summary_df["access_tier"].value_counts().to_string())

tier_colors = {"Good Access":"#0077BB","At Risk":"#EE7733","Food Desert":"#CC3311"}
borough_order = ["Bronx","Brooklyn","Manhattan","Queens","Staten Island"]
borough_palette = {
    "Bronx":"#AA3377","Brooklyn":"#0077BB","Manhattan":"#EE7733",
    "Queens":"#009988","Staten Island":"#555555",
}
tier_order = ["Food Desert","At Risk","Good Access"]
FOOD_DESERT_THRESHOLD = 25.2
GOOD_ACCESS_THRESHOLD = 54.7


# ════════════════════════════════════════════════════════════
# Q1
# ════════════════════════════════════════════════════════════
print("\nGenerating Q1...")

borough_sort = {b:i for i,b in enumerate(borough_order)}
q1_df = summary_df.dropna(subset=["food_access_score"]).copy()
q1_df["borough_rank"] = q1_df["borough"].map(borough_sort)
q1_df = q1_df.sort_values(["borough_rank","food_access_score"],ascending=[True,True]).reset_index(drop=True)
n = len(q1_df)

# Tall figure — generous height per bar so labels never overlap
fig, ax = plt.subplots(figsize=(18, n * 0.38 + 3))
fig.suptitle(
    "Q1: NYC Neighborhood Food Access Score Rankings\n"
    "Grouped by Borough — Sorted Low to High Within Each Group",
    fontsize=14, fontweight="bold", y=0.998
)

bar_colors = [tier_colors[t] for t in q1_df["access_tier"]]
# Wider bars — height=0.82
bars = ax.barh(range(n), q1_df["food_access_score"], color=bar_colors, height=0.82)

# Borough dividers and side labels
current_borough = None
borough_start   = 0
for i, row in q1_df.iterrows():
    if row["borough"] != current_borough:
        if current_borough is not None:
            ax.axhline(y=i - 0.5, color="#cccccc", linewidth=1.2)
            mid = (borough_start + i - 1) / 2
            ax.text(192, mid, current_borough, va="center", ha="left",
                    fontsize=9, fontweight="bold", style="italic",
                    color=borough_palette.get(current_borough,"#333333"))
        current_borough = row["borough"]
        borough_start   = i
# Last borough
mid = (borough_start + n - 1) / 2
ax.text(192, mid, current_borough, va="center", ha="left",
        fontsize=9, fontweight="bold", style="italic",
        color=borough_palette.get(current_borough,"#333333"))

ax.axvline(x=GOOD_ACCESS_THRESHOLD, color="#0077BB", linestyle="--", linewidth=1.5, alpha=0.9)
ax.axvline(x=FOOD_DESERT_THRESHOLD, color="#EE7733", linestyle="--", linewidth=1.5, alpha=0.9)

ax.set_yticks(range(n))
ax.set_yticklabels(q1_df["neighborhood"], fontsize=8, fontweight="500")
ax.tick_params(axis="y", pad=5)

for i, (bar, val) in enumerate(zip(bars, q1_df["food_access_score"])):
    if pd.notna(val):
        ax.text(val + 0.8, i, f"{val:.1f}", va="center", fontsize=7, fontweight="bold", color="#333333")

ax.set_xlabel("Food Access Score", fontsize=12)
ax.set_xlim(0, 190)
ax.set_title(
    f"Food Desert <{FOOD_DESERT_THRESHOLD}  |  At Risk {FOOD_DESERT_THRESHOLD}–{GOOD_ACCESS_THRESHOLD}  |  Good Access ≥{GOOD_ACCESS_THRESHOLD}  (thresholds back-calculated from archetypes)",
    fontsize=9
)

legend_elements = [
    Patch(facecolor="#0077BB", label=f"Good Access — {(summary_df['access_tier']=='Good Access').sum()} neighborhoods"),
    Patch(facecolor="#EE7733", label=f"At Risk — {(summary_df['access_tier']=='At Risk').sum()} neighborhoods"),
    Patch(facecolor="#CC3311", label=f"Food Desert — {(summary_df['access_tier']=='Food Desert').sum()} neighborhoods"),
]
ax.legend(handles=legend_elements, fontsize=9, loc="lower right")

# Generous margins so nothing clips
plt.subplots_adjust(left=0.20, right=0.87, top=0.97, bottom=0.03)
plt.savefig("q1_food_access_score.png", dpi=150, bbox_inches="tight")
plt.close()
print("Q1 saved")


# ════════════════════════════════════════════════════════════
# Q2
# ════════════════════════════════════════════════════════════
print("Generating Q2...")

fig, axes = plt.subplots(1, 2, figsize=(24, 12))
fig.suptitle(
    "Q2: Food Access Score vs Socioeconomic Indicators\n"
    "High poverty does not predict food desert status in NYC — store density matters more",
    fontsize=14, fontweight="bold"
)

plot_df = summary_df.dropna(subset=["food_access_score","poverty_rate","avg_median_income"])
notable = [
    ("South Bronx","right"),("Hunts Point","right"),("Murray Hill","left"),
    ("Bayside","left"),("Sunset Park","right"),("Longwood","right"),
    ("Eltingville","left"),("Throggs Neck","left"),
]

for ax_idx, (xcol, xlabel, title) in enumerate([
    ("poverty_rate","Poverty Rate (%)","Score vs Poverty Rate\nHigh-poverty neighborhoods can score high with dense grocery infrastructure"),
    ("avg_median_income","Median Household Income ($000s)","Score vs Median Income\nHigh income does not guarantee good access (Murray Hill: $153k, Food Desert)"),
]):
    ax = axes[ax_idx]
    for tier in tier_order:
        sub = plot_df[plot_df["access_tier"]==tier]
        xv = sub[xcol]/(1000 if xcol=="avg_median_income" else 1)
        ax.scatter(xv, sub["food_access_score"], c=tier_colors[tier],
                   label=f"{tier} (n={len(sub)})", alpha=0.8, s=90,
                   edgecolors="white", linewidths=0.6, zorder=3)
    ax.axhline(y=GOOD_ACCESS_THRESHOLD, color="#0077BB", linestyle="--", linewidth=1.2, alpha=0.7)
    ax.axhline(y=FOOD_DESERT_THRESHOLD, color="#EE7733", linestyle="--", linewidth=1.2, alpha=0.7)
    for name, ha in notable:
        row = plot_df[plot_df["neighborhood"]==name]
        if not row.empty:
            xval = row[xcol].values[0]/(1000 if xcol=="avg_median_income" else 1)
            ax.annotate(name,
                xy=(xval, row["food_access_score"].values[0]),
                xytext=(10 if ha=="right" else -10, 0),
                textcoords="offset points", fontsize=9, ha=ha, va="center",
                color="#333333", arrowprops=dict(arrowstyle="-",color="#aaaaaa",lw=0.6))
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_ylabel("Food Access Score", fontsize=12)
    ax.set_title(title, fontsize=10)
    ax.legend(fontsize=9, loc="upper right")
    ax.grid(True, alpha=0.2)
    ax.tick_params(labelsize=10)

plt.tight_layout()
plt.savefig("q2_score_vs_socioeconomic.png", dpi=150, bbox_inches="tight")
plt.close()
print("Q2 saved")


# ════════════════════════════════════════════════════════════
# Q3 — fixed label spacing
# ════════════════════════════════════════════════════════════
print("Generating Q3...")

q3_df = summary_df.groupby("access_tier").agg(
    avg_snap_grocery    =("snap_grocery_per_10k","mean"),
    avg_nonsnap_grocery =("nonsnap_grocery_per_10k","mean"),
    avg_conv_ratio      =("snap_convenience_ratio","mean"),
    avg_food_balance    =("food_balance_ratio","mean"),
    avg_year_round      =("year_round_markets","mean"),
    avg_pct_grade_A     =("pct_grade_A","mean"),
    avg_income          =("avg_median_income","mean"),
    avg_econ_gap        =("economic_access_gap","mean"),
).reindex(tier_order).reset_index()

fig, axes = plt.subplots(2, 4, figsize=(28, 16))
fig.suptitle(
    "Q3: What Variables Drive Food Desert Status?\n"
    "Average metric values for each access tier across 116 NYC neighborhoods",
    fontsize=15, fontweight="bold"
)
colors_q3 = ["#CC3311","#EE7733","#0077BB"]

def labeled_bar(ax, labels, values, title, ylabel, fmt="{:.2f}", note=None):
    bars = ax.bar(labels, values, color=colors_q3, width=0.5)
    ax.set_title(title, fontsize=9.5, fontweight="bold", pad=12)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.tick_params(axis="x", labelsize=9.5)
    ax.tick_params(axis="y", labelsize=8.5)
    ymax = max(v for v in values if pd.notna(v))
    ax.set_ylim(0, ymax * 1.30)  # extra headroom so labels don't hit title
    for bar, val in zip(bars, values):
        if pd.notna(val):
            ax.text(bar.get_x() + bar.get_width()/2,
                    val + ymax * 0.04,          # consistent offset as % of range
                    fmt.format(val),
                    ha="center", fontsize=9.5, fontweight="bold")
    if note:
        ax.text(0.5, -0.20, note, transform=ax.transAxes,
                fontsize=7.5, ha="center", color="#666666", style="italic")

labeled_bar(axes[0,0], q3_df["access_tier"], q3_df["avg_snap_grocery"],
    "SNAP Grocery Stores per 10k\n(incl. ethnic grocery reclassification)",
    "Count per 10k residents", note="Higher = better access")

labeled_bar(axes[0,1], q3_df["access_tier"], q3_df["avg_nonsnap_grocery"],
    "Non-SNAP Grocery per 10k\n(size-weighted: supermarket x1.0, mid x0.6, small x0.3)",
    "Weighted count per 10k", note="Higher = better access")

labeled_bar(axes[0,2], q3_df["access_tier"], q3_df["avg_conv_ratio"],
    "Convenience Store % of SNAP Retailers\n(formula penalty term)",
    "% of SNAP stores — convenience stores", fmt="{:.1f}%",
    note="Higher % = worse food environment (more bodegas, fewer grocers)")

labeled_bar(axes[0,3], q3_df["access_tier"], q3_df["avg_food_balance"],
    "Food Balance Ratio\n(SNAP grocery per 10k / fast food per 10k)",
    "Ratio (>1 = more groceries than fast food)",
    note="Higher ratio = healthier food environment balance")

labeled_bar(axes[1,0], q3_df["access_tier"], q3_df["avg_year_round"],
    "Year-Round Farmers Markets\n(raw count per neighborhood)",
    "Average count", note="Higher = more reliable fresh produce access year-round")

labeled_bar(axes[1,1], q3_df["access_tier"], q3_df["avg_pct_grade_A"],
    "% Grade A Health Inspections\n(NYC-specific — not portable to other cities)",
    "% of restaurants with Grade A", fmt="{:.1f}%",
    note="NYC DOHMH letter grade system only. Higher = better food safety")

labeled_bar(axes[1,2], q3_df["access_tier"], q3_df["avg_income"],
    "Avg Median Household Income\n(note: high poverty ≠ food desert in dense NYC)",
    "Median income ($)", fmt="${:,.0f}",
    note="Poverty rate is a poor predictor of food access in dense urban areas")

labeled_bar(axes[1,3], q3_df["access_tier"], q3_df["avg_econ_gap"],
    "Economic Access Gap\n(income-based affordability penalty)",
    "Gap score (0 = no barrier, 2 = severe barrier)", fmt="{:.2f}",
    note="0 = income supports food costs  |  2 = severe affordability pressure")

plt.tight_layout(rect=[0,0.03,1,1], h_pad=4.0, w_pad=3.0)
plt.savefig("q3_score_components.png", dpi=150, bbox_inches="tight")
plt.close()
print("Q3 saved")


# ════════════════════════════════════════════════════════════
# Q4 — fixed layout, no overlapping labels
# ════════════════════════════════════════════════════════════
print("Generating Q4...")

def pop_weighted_mean(group, vc, wc):
    return (group[vc]*group[wc]).sum()/group[wc].sum()

q4_rows = []
for borough in borough_order:
    sub = summary_df[summary_df["borough"]==borough].dropna(
        subset=["food_access_score","total_population"])
    if sub.empty: continue
    q4_rows.append({
        "borough":         borough,
        "avg_score":       pop_weighted_mean(sub,"food_access_score","total_population"),
        "avg_snap_grocery":pop_weighted_mean(sub,"snap_grocery_per_10k","total_population"),
        "avg_conv_ratio":  pop_weighted_mean(sub,"snap_convenience_ratio","total_population"),
        "avg_poverty":     pop_weighted_mean(sub,"poverty_rate","total_population"),
        "food_deserts":    sub["our_food_desert"].sum(),
        "at_risk":         (sub["access_tier"]=="At Risk").sum(),
        "good_access":     (sub["access_tier"]=="Good Access").sum(),
        "n":               len(sub),
        "population":      sub["total_population"].sum(),
    })

q4_df = pd.DataFrame(q4_rows)
x     = range(len(q4_df))
width = 0.24
colors_boro = [borough_palette[b] for b in q4_df["borough"]]

fig, axes = plt.subplots(2, 2, figsize=(24, 16))
fig.suptitle(
    "Q4: Food Access Across NYC Boroughs\n"
    "Population-weighted averages — neighborhood counts noted per borough",
    fontsize=15, fontweight="bold"
)

# Chart 1 — avg score
bars = axes[0,0].bar(q4_df["borough"], q4_df["avg_score"], color=colors_boro, width=0.55)
axes[0,0].axhline(y=GOOD_ACCESS_THRESHOLD, color="#0077BB", linestyle="--",
                  linewidth=1.5, alpha=0.8, label=f"Good Access ({GOOD_ACCESS_THRESHOLD})")
axes[0,0].axhline(y=FOOD_DESERT_THRESHOLD, color="#EE7733", linestyle="--",
                  linewidth=1.5, alpha=0.8, label=f"Food Desert ({FOOD_DESERT_THRESHOLD})")
axes[0,0].set_title("Population-Weighted Avg Food Access Score", fontweight="bold", fontsize=12)
axes[0,0].set_ylabel("Avg Food Access Score", fontsize=11)
axes[0,0].tick_params(axis="x", rotation=15, labelsize=11)
axes[0,0].legend(fontsize=10)
ymax0 = q4_df["avg_score"].max()
axes[0,0].set_ylim(0, ymax0 * 1.25)
for bar, row in zip(bars, q4_df.itertuples()):
    axes[0,0].text(bar.get_x()+bar.get_width()/2,
                   bar.get_height() + ymax0*0.03,
                   f"{row.avg_score:.1f}\n(n={row.n})",
                   ha="center", fontsize=9.5, fontweight="bold")

# Chart 2 — tier distribution
b1 = axes[0,1].bar([i-width for i in x], q4_df["food_deserts"],
                   width, label="Food Desert", color="#CC3311")
b2 = axes[0,1].bar([i      for i in x], q4_df["at_risk"],
                   width, label="At Risk",     color="#EE7733")
b3 = axes[0,1].bar([i+width for i in x], q4_df["good_access"],
                   width, label="Good Access", color="#0077BB")
axes[0,1].set_xticks(list(x))
axes[0,1].set_xticklabels(q4_df["borough"], rotation=15, fontsize=11)
axes[0,1].set_title("Access Tier Distribution by Borough\nQueens has 40 neighborhoods vs Bronx 19",
                     fontweight="bold", fontsize=11)
axes[0,1].set_ylabel("Number of Neighborhoods", fontsize=11)
axes[0,1].legend(fontsize=10)
ymax1 = (q4_df["food_deserts"]+q4_df["at_risk"]+q4_df["good_access"]).max()
axes[0,1].set_ylim(0, ymax1 * 1.20)
for i, row in enumerate(q4_df.itertuples()):
    total = row.food_deserts + row.at_risk + row.good_access
    axes[0,1].text(i, total + ymax1*0.04, f"n={row.n}",
                   ha="center", fontsize=9.5, color="#555555")

# Chart 3 — SNAP grocery per 10k
bars = axes[1,0].bar(q4_df["borough"], q4_df["avg_snap_grocery"],
                     color=colors_boro, width=0.55)
axes[1,0].set_title("Population-Weighted SNAP Grocery per 10k\n(includes 457 ethnic grocery reclassifications)",
                     fontweight="bold", fontsize=11)
axes[1,0].set_ylabel("Weighted avg count per 10k residents", fontsize=11)
axes[1,0].tick_params(axis="x", rotation=15, labelsize=11)
ymax2 = q4_df["avg_snap_grocery"].max()
axes[1,0].set_ylim(0, ymax2 * 1.20)
for bar, val in zip(bars, q4_df["avg_snap_grocery"]):
    axes[1,0].text(bar.get_x()+bar.get_width()/2,
                   val + ymax2*0.03,
                   f"{val:.2f}", ha="center", fontsize=10, fontweight="bold")

# Chart 4 — poverty vs convenience — separate y axes, no overlap
ax4  = axes[1,1]
ax4b = ax4.twinx()

# Keep bars narrower so they don't overlap with twin axis
bars1 = ax4.bar([i-width/2 for i in x], q4_df["avg_poverty"],
                width*0.9, label="Avg Poverty Rate (%)", color="#AA3377", alpha=0.85)
bars2 = ax4b.bar([i+width/2 for i in x], q4_df["avg_conv_ratio"],
                 width*0.9, label="Convenience Store %", color="#CC3311", alpha=0.65)

ax4.set_xticks(list(x))
ax4.set_xticklabels(q4_df["borough"], rotation=15, fontsize=11)
ax4.set_title("Poverty Rate vs Convenience Store Dominance\n(population-weighted)",
              fontweight="bold", fontsize=11)
ax4.set_ylabel("Poverty Rate (%)", color="#AA3377", fontsize=11)
ax4b.set_ylabel("Convenience Store % of SNAP retailers", color="#CC3311", fontsize=11)

# Separate legends, no overlap
ax4.legend(loc="upper left", fontsize=9)
ax4b.legend(loc="upper right", fontsize=9)

# Extra y headroom so value labels don't clip
ymax_p = q4_df["avg_poverty"].max()
ymax_c = q4_df["avg_conv_ratio"].max()
ax4.set_ylim(0, ymax_p * 1.30)
ax4b.set_ylim(0, ymax_c * 1.30)

for bar, val in zip(bars1, q4_df["avg_poverty"]):
    ax4.text(bar.get_x()+bar.get_width()/2,
             val + ymax_p*0.03,
             f"{val:.1f}%", ha="center", fontsize=9, fontweight="bold", color="#AA3377")
for bar, val in zip(bars2, q4_df["avg_conv_ratio"]):
    ax4b.text(bar.get_x()+bar.get_width()/2,
              val + ymax_c*0.03,
              f"{val:.1f}%", ha="center", fontsize=9, fontweight="bold", color="#CC3311")

plt.tight_layout(h_pad=4.0, w_pad=3.0)
plt.savefig("q4_borough_comparison.png", dpi=150, bbox_inches="tight")
plt.close()
print("Q4 saved")

print("\nAll charts saved.")
import subprocess
subprocess.run(["open",
    "q1_food_access_score.png",
    "q2_score_vs_socioeconomic.png",
    "q3_score_components.png",
    "q4_borough_comparison.png"
])