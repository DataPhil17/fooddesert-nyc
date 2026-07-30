cat > README.md << 'EOF'
# NYC Food Desert Analysis

An expanded analysis of food access across 116 NYC neighborhoods, challenging the USDA's distance-based food desert definition with a density and quality-based alternative scoring methodology.

## Core Thesis
The USDA defines food deserts using a distance-based metric that fails in dense urban environments. This project builds a custom **Food Access Score** using SNAP retailer density, retail quality classification, food environment balance, and economic access to produce a more nuanced picture of food access across all five NYC boroughs.

## Data Sources
| Source | What | Collection |
|---|---|---|
| ACS 2023 Census API | Population, income, poverty by ZIP | `census` |
| USDA SNAP Retailer CSV | 8,457 authorized NYC retailers | `snap` |
| NYC DOHMH Inspections | 26,014 restaurant health grades | `inspections` |
| NYC DOHMH Farmers Markets | 1,362 market locations | `farmers` |
| NY State Dept of Agriculture | 8,677 non-SNAP grocery stores | `grocery_nonsnap` |
| NYC Emergency Food Supply Gap | Food insecurity % by NTA (2025) | `food_security` |
| NYC GreenThumb | 546 active community gardens | `gardens` |

## Food Access Score Formula

score = (snap_grocery_per_10k × 8.0)
+ (nonsnap_grocery_per_10k × 5.0)
+ (membership_per_10k × 3.0)
- (convenience_ratio / 100 × 8.0)
+ (food_balance_ratio × 4.0)
+ (year_round_markets × 1.0)
+ (farmers_markets_per_10k × 0.5)
+ (gardens_per_10k × 0.25)
+ (pct_grade_A / 100 × 0.2)
- (economic_access_gap × 1.0)

Thresholds are back-calculated from archetypes rather than chosen arbitrarily:
- **Food Desert** — score < 25.2 (7 neighborhoods)
- **At Risk** — score 25.2–54.7 (51 neighborhoods)
- **Good Access** — score ≥ 54.7 (58 neighborhoods)

## Key Findings
- High poverty rate does not predict food desert status in dense NYC neighborhoods
- 457 ethnic grocery stores (halal markets, carnicerías, African grocers) were reclassified from Convenience Store to healthy retail — USDA's classification underrepresents culturally specific food infrastructure
- Murray Hill (median income $153k) scores as a Food Desert due to near-absence of SNAP-authorized grocery stores — wealthy neighborhoods with boutique food retail are missed by SNAP-based metrics
- 536 additional SNAP-authorized grocery stores are needed citywide for full remediation to Good Access

## Scripts
| Script | Purpose |
|---|---|
| `config.py` | Crosswalk + Census + Inspections + Farmers Markets |
| `collect_snap.py` | USDA SNAP retailer data with ethnic grocery reclassification |
| `collect_grocery.py` | Non-SNAP grocery stores from NY State |
| `collect_pantries.py` | Food security data + community gardens |
| `clean_and_merge.py` | Aggregation, scoring, tier classification |
| `analysis_charts.py` | Q1–Q4 analysis charts |
| `build_map.py` | Interactive Folium choropleth map |

## Setup
```bash
git clone https://github.com/DataPhil17/fooddesert-nyc.git
cd fooddesert-nyc
python3 -m venv .venv
source .venv/bin/activate
pip install pymongo certifi python-dotenv pandas requests numpy matplotlib seaborn folium
```

Create a `.env` file:
MONGO_URI=your_mongodb_atlas_connection_string
DB_NAME=fooddesert
CENSUS_API_KEY=your_census_api_key

Run in order:
```bash
python config.py
python collect_snap.py      # requires snap_retailers.csv — see script for download link
python collect_grocery.py
python collect_pantries.py
python clean_and_merge.py
python analysis_charts.py
python build_map.py
```

## Tech Stack
Python · MongoDB Atlas · Folium · Matplotlib · Pandas · NYC Open Data · USDA FNS · ACS Census API
EOF