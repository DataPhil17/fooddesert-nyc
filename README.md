# 🗽 NYC Food Desert Analysis

A data pipeline and analysis project that proposes and tests an alternative food desert classification methodology for dense urban environments — exposing the limitations of the USDA's distance-based definition and offering a more defensible framework for identifying communities without meaningful access to nutritious food.

> **Thesis:** The USDA's distance-based definition of food deserts systematically undercounts food insecurity in dense urban environments. In NYC, proximity to a supermarket does not equal access — quality, affordability, and the ratio of healthy to unhealthy food options must be considered together.

---

## 📊 Live Notebook
View the full analysis notebook with charts and interactive map:
[fooddesert_analysis.ipynb](./fooddesert_analysis.ipynb)

---

## 🔍 Research Questions
1. How do NYC neighborhoods rank on food access and what tier do they fall into?
2. Where does the USDA definition fail NYC?
3. What variables are driving food desert status?
4. How does food access vary across the five boroughs?

---

## 📦 Data Sources
| Source | Data | Method |
|---|---|---|
| Google Places API | Food establishments (grocery, fast food, restaurants, bodegas) | REST API |
| NYC Open Data — Restaurant Inspections | Health inspection grades | Socrata API |
| U.S. Census ACS 5-Year Estimates | Population, income, poverty rates | Census API |
| USDA Food Access Research Atlas (2019) | Federal food desert classification by census tract | Excel download |
| NYC Open Data — Farmers Markets | Market locations, EBT acceptance, year-round status | Socrata API |
| USDA SNAP Retailer Locator | Authorized SNAP retailers by ZIP code and store type | ArcGIS API |
| NYC GeoJSON Boundaries | ZIP code boundary polygons for interactive map | Public GeoJSON |

All raw data is stored in **MongoDB Atlas** as JSON documents before any cleaning or transformation.

---

## 🏙️ Neighborhoods Studied
27 neighborhoods across all five NYC boroughs selected to represent a spectrum of income levels, food access conditions, and demographic profiles — from South Bronx and East New York to Park Slope and Tottenville.

---

## 🧮 Food Access Score
Rather than the USDA's single 1-mile threshold, this project constructs a composite **Food Access Score** from eight weighted variables:

| Variable | Weight | Direction |
|---|---|---|
| Grocery stores per 10k | ×10 | ➕ |
| Fast food per 10k | ×6 | ➖ |
| SNAP healthy retailer ratio | ×0.5 | ➕ |
| Year-round farmers markets | ×1.5 | ➕ |
| Farmers markets per 10k | ×1 | ➕ |
| % Grade A inspections | ×0.2 | ➕ |
| % EBT-accepting markets | ×0.1 | ➕ |
| Poverty rate | ×0.3 | ➖ |

Neighborhoods are assigned to one of three tiers:
- 🟢 **Good Access** — score ≥ 55
- 🟡 **At Risk** — score 40–54
- 🔴 **Food Desert** — score < 40

---

## 🛠️ Tech Stack
- **Python 3.13**
- **MongoDB Atlas** — NoSQL cloud database for raw and processed data storage
- **pandas** — data cleaning, merging, and aggregation
- **matplotlib / seaborn** — data visualization
- **folium** — interactive choropleth map with ZIP code boundaries and traffic light classification
- **Google Places API** — food establishment data collection
- **NYC Open Data Socrata API** — restaurant inspection data
- **U.S. Census ACS API** — demographic data
- **USDA APIs** — food desert atlas and SNAP retailer data
- **Jupyter Notebook** — end-to-end analysis and presentation

---

## 📁 Project Structure
```
fooddesert/
├── config.py                    # Environment variable loading
├── collect_places.py            # Google Places API collection
├── collect_inspections.py       # NYC restaurant inspections collection
├── collect_census.py            # Census ACS demographic collection
├── collect_usda.py              # USDA Food Access Research Atlas
├── collect_farmers_markets.py   # NYC farmers markets collection
├── collect_snap.py              # USDA SNAP retailer collection
├── clean_and_merge.py           # Data cleaning, merging, normalization
├── fooddesert_analysis.ipynb    # Full analysis notebook
├── fooddesert_map.html          # Interactive neighborhood map
├── .env                         # API keys (not included in repo)
├── .gitignore
└── README.md
```

---

## ⚙️ Setup & Installation

### 1. Clone the repository
```bash
git clone https://github.com/DataPhil17/fooddesert-nyc.git
cd fooddesert-nyc
```

### 2. Create and activate virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies
```bash
pip install pymongo requests certifi pandas matplotlib seaborn folium jupyter python-dotenv openpyxl
```

### 4. Set up environment variables
Create a `.env` file in the root directory:
```
MONGO_URI=your_mongodb_connection_string
GOOGLE_API_KEY=your_google_places_api_key
```

### 5. Download USDA Food Access Research Atlas
Download the Excel file from:
`https://www.ers.usda.gov/data-products/food-access-research-atlas/download-the-data/`
Save as `usda_food_atlas.xlsx` in the project root.

### 6. Run data collection scripts in order
```bash
python3 collect_places.py
python3 collect_inspections.py
python3 collect_census.py
python3 collect_usda.py
python3 collect_farmers_markets.py
python3 collect_snap.py
python3 clean_and_merge.py
```

### 7. Launch the notebook
```bash
jupyter notebook fooddesert_analysis.ipynb
```

---

## 📈 Key Findings
- **The Bronx, Manhattan, and Queens have zero USDA food deserts** — yet South Bronx scores 27.39 on our scale with a 35.5% poverty rate
- **Staten Island accounts for 28 of NYC's 33 USDA food deserts** — the wealthiest, most suburban borough dominates the federal list
- **7 neighborhoods are Food Deserts, 13 are At Risk, 7 have Good Access** across our 27 neighborhoods studied
- **43% of all SNAP retailers are convenience stores** — food assistance dollars are being spent at establishments that primarily sell processed goods
- **South Bronx has 42 farmers markets but only 1 is year-round** — seasonal access cannot substitute for consistent food infrastructure
- **Income does not reliably predict food access** — Greenwich Village scores At Risk despite a median income of 153k

---

## ⚠️ Limitations
- Google Places caps at 60 results per query — full establishment coverage cannot be verified
- Only 27 of NYC's 300+ neighborhoods were studied — findings are illustrative rather than exhaustive
- ZIP codes approximate neighborhood boundaries — some cover multiple distinct areas
- USDA Food Access Research Atlas data is from 2019

---

## 👤 Author
**Philippe Louis Jr.**
- GitHub: [@DataPhil17](https://github.com/DataPhil17)
- LinkedIn: [Philippe Louis Jr.](https://www.linkedin.com/in/plouis-chm/)

---

## 📄 License
This project is open source and available under the [MIT License](LICENSE).