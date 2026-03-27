# 🗽 NYC Food Deserts & Crime Analysis

A data pipeline and analysis project investigating the relationship between food access quality and crime statistics across 27 New York City neighborhoods spanning all five boroughs.

> **Thesis:** Food insecurity is a social determinant of crime. People denied access to a healthy environment cannot sustain a healthy lifestyle.

---

## 📊 Live Notebook
View the full analysis notebook with charts and interactive map:
[fooddesert_analysis.html](./fooddesert_analysis.html)

---

## 🔍 Research Questions
1. Which NYC neighborhoods qualify as food deserts based on the density and rating of grocery stores relative to fast food chains?
2. Is there a correlation between food desert classification and overall crime rate?
3. How does the fast food to grocery ratio change as neighborhood median income decreases?
4. How do food desert neighborhoods compare across boroughs?
5. How do food environment conditions in NYC compare across all five boroughs?

---

## 📦 Data Sources
| Source | Data | Method |
|---|---|---|
| Google Places API | Food establishments (grocery, fast food, restaurants, bodegas) | REST API |
| NYC Open Data — NYPD Arrests | Arrest records with coordinates | Socrata API |
| NYC Open Data — Restaurant Inspections | Health inspection grades | Socrata API |
| U.S. Census ACS 5-Year Estimates | Population, income, poverty rates | Census API |
| NYC GeoJSON Boundaries | ZIP code boundary polygons | Public GeoJSON |

All raw data is stored in **MongoDB Atlas** as JSON documents before cleaning or transformation.

---

## 🗺️ Neighborhoods Studied
27 neighborhoods across all five NYC boroughs selected to represent a spectrum of income levels, food access conditions, and demographic profiles — from South Bronx and East New York to Park Slope and Tottenville.

---

## 🛠️ Tech Stack
- **Python 3.13**
- **MongoDB Atlas** — NoSQL cloud database for raw and processed data storage
- **pandas** — data cleaning, merging, and aggregation
- **matplotlib / seaborn** — data visualization
- **folium** — interactive choropleth map with ZIP code boundaries
- **Google Places API** — food establishment data collection
- **NYC Open Data Socrata API** — crime and inspection data
- **U.S. Census ACS API** — demographic data
- **Jupyter Notebook** — end-to-end analysis and presentation

---

## 📁 Project Structure
```
fooddesert/
├── config.py                  # Environment variable loading
├── collect_places.py          # Google Places API collection
├── collect_crime.py           # NYPD arrests collection
├── collect_inspections.py     # NYC restaurant inspections collection
├── collect_census.py          # Census ACS demographic collection
├── clean_and_merge.py         # Data cleaning, merging, normalization
├── analysis.py                # Research question analysis
├── fooddesert_analysis.ipynb  # Full analysis notebook
├── fooddesert_map.html        # Interactive neighborhood map
├── .env                       # API keys (not included in repo)
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
pip install pymongo requests certifi pandas matplotlib seaborn folium jupyter python-dotenv
```

### 4. Set up environment variables
Create a `.env` file in the root directory:
```
MONGO_URI=your_mongodb_connection_string
GOOGLE_API_KEY=your_google_places_api_key
```

### 5. Run data collection scripts in order
```bash
python3 collect_places.py
python3 collect_crime.py
python3 collect_inspections.py
python3 collect_census.py
python3 clean_and_merge.py
```

### 6. Launch the notebook
```bash
jupyter notebook fooddesert_analysis.ipynb
```

---

## 📈 Key Findings
- **23 of 27 neighborhoods** studied qualify as food deserts
- Food desert neighborhoods average **505.6 arrests per 10,000 residents** vs 392.5 for non-deserts
- Income correlation with grocery access is only **0.02** — wealth alone does not predict food access in NYC
- **South Bronx** has the worst food access (ratio 0.50) and highest crime rate (1,379 per 10k) with a 35.5% poverty rate
- **Bodegas are substituting for grocery stores** in the most underserved neighborhoods, providing processed goods rather than fresh produce
- Health inspection grades (85-97% Grade A across all neighborhoods) do not reflect food access disparity

---

## ⚠️ Limitations
- Google Places caps at 60 results per query — full establishment coverage cannot be verified
- Only 27 of NYC's 300+ neighborhoods were studied
- ZIP codes approximate neighborhood boundaries and some cover multiple distinct areas
- NYPD arrest data reflects policing patterns as much as actual crime rates
- Correlation does not imply causation

---

## 👤 Author
**Philippe Louis Jr.**
- GitHub: [@DataPhil17](https://github.com/DataPhil17)
- LinkedIn: [Philippe Louis Jr.](https://www.linkedin.com/in/plouis-chm/)

---

## 📄 License
This project is open source and available under the [MIT License](LICENSE).