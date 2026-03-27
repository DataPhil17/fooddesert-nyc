import certifi
from dotenv import load_dotenv
import os

load_dotenv()

# MongoDB
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "fooddesert"

# Google
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

NEIGHBORHOODS = [
    # === BRONX ===
    {"name": "South Bronx", "zip": "10451"},
    {"name": "South Bronx", "zip": "10454"},
    {"name": "South Bronx", "zip": "10455"},
    {"name": "Highbridge", "zip": "10452"},
    {"name": "Belmont", "zip": "10457"},
    {"name": "Riverdale", "zip": "10471"},

    # === BROOKLYN ===
    {"name": "East New York", "zip": "11207"},
    {"name": "East New York", "zip": "11208"},
    {"name": "Bedford-Stuyvesant", "zip": "11221"},
    {"name": "Bedford-Stuyvesant", "zip": "11233"},
    {"name": "Crown Heights", "zip": "11213"},
    {"name": "Clinton Hill / Fort Greene", "zip": "11205"},
    {"name": "Clinton Hill / Fort Greene", "zip": "11201"},
    {"name": "Red Hook", "zip": "11231"},
    {"name": "Park Slope", "zip": "11215"},
    {"name": "Park Slope", "zip": "11217"},
    {"name": "Sheepshead Bay", "zip": "11235"},
    {"name": "Coney Island", "zip": "11224"},
    {"name": "Bensonhurst", "zip": "11214"},
    {"name": "Midwood", "zip": "11230"},

    # === MANHATTAN ===
    {"name": "Washington Heights", "zip": "10032"},
    {"name": "Washington Heights", "zip": "10033"},
    {"name": "Harlem", "zip": "10037"},
    {"name": "Harlem", "zip": "10039"},
    {"name": "Upper West Side", "zip": "10023"},
    {"name": "Upper West Side", "zip": "10024"},
    {"name": "Greenwich Village", "zip": "10003"},
    {"name": "Chinatown / Lower East Side", "zip": "10002"},

    # === QUEENS ===
    {"name": "Astoria", "zip": "11102"},
    {"name": "Astoria", "zip": "11103"},
    {"name": "Jamaica", "zip": "11432"},
    {"name": "Jamaica", "zip": "11433"},
    {"name": "Forest Hills", "zip": "11375"},
    {"name": "Kew Gardens", "zip": "11415"},
    {"name": "Middle Village", "zip": "11379"},
    {"name": "Far Rockaway", "zip": "11691"},

    # === STATEN ISLAND ===
    {"name": "St. George", "zip": "10301"},
    {"name": "Tottenville", "zip": "10307"},
]

CERT_PATH = certifi.where()