import os
import json
import requests
import pandas as pd
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME   = os.getenv("DB_NAME", "fooddesert")

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db     = client[DB_NAME]

# Output directory — Observable Framework data folder
OUT_DIR = os.path.join(
    os.path.dirname(__file__),
    "observable-site", "src", "data"
)
os.makedirs(OUT_DIR, exist_ok=True)
print(f"Output directory: {OUT_DIR}\n")


# ── 1. Summary — neighborhood scores and metadata ───────────
print("Exporting summary.json...")
summary = list(db["summary"].find({}, {"_id": 0}))

# Clean for JSON serialization
for doc in summary:
    for k, v in doc.items():
        if hasattr(v, "item"):       # numpy types
            doc[k] = v.item()
        elif v != v:                 # NaN check
            doc[k] = None

with open(os.path.join(OUT_DIR, "summary.json"), "w") as f:
    json.dump(summary, f)
print(f"  {len(summary)} neighborhoods exported")


# ── 2. SNAP stores — healthy retailers with coordinates ──────
print("\nExporting snap_stores.json...")
snap = list(db["snap"].find(
    {"is_healthy_retailer": True},
    {"_id": 0, "Store_Name": 1, "store_type_clean": 1,
     "class_reason": 1, "neighborhood": 1, "borough": 1,
     "Latitude": 1, "Longitude": 1}
))

# Filter to records with valid coordinates in NYC bounds
stores = []
for s in snap:
    try:
        lat = float(s.get("Latitude") or 0)
        lon = float(s.get("Longitude") or 0)
        if 40.4 < lat < 41.0 and -74.3 < lon < -73.6:
            stores.append({
                "name":        s.get("Store_Name", "Unknown"),
                "type":        s.get("store_type_clean", "Unknown"),
                "reason":      s.get("class_reason", ""),
                "neighborhood":s.get("neighborhood", ""),
                "borough":     s.get("borough", ""),
                "lat":         round(lat, 6),
                "lon":         round(lon, 6),
            })
    except:
        continue

with open(os.path.join(OUT_DIR, "snap_stores.json"), "w") as f:
    json.dump(stores, f)
print(f"  {len(stores)} healthy SNAP retailers exported")


# ── 3. Placement data — intervention recommendations ─────────
print("\nExporting placement.json...")

import math

FOOD_DESERT_THRESHOLD = 25.2
GOOD_ACCESS_THRESHOLD = 54.7

ZIP_CENTROIDS = {
    "10451":(40.8173,-73.9243),"10452":(40.8340,-73.9221),"10453":(40.8524,-73.9127),
    "10454":(40.8059,-73.9191),"10455":(40.8119,-73.9134),"10456":(40.8290,-73.9083),
    "10457":(40.8454,-73.8988),"10458":(40.8619,-73.8892),"10459":(40.8219,-73.8977),
    "10460":(40.8360,-73.8810),"10461":(40.8440,-73.8453),"10462":(40.8491,-73.8663),
    "10463":(40.8797,-73.9097),"10464":(40.8471,-73.7878),"10465":(40.8212,-73.8298),
    "10466":(40.8976,-73.8462),"10467":(40.8786,-73.8744),"10468":(40.8617,-73.9145),
    "10469":(40.8706,-73.8561),"10470":(40.9005,-73.8616),"10471":(40.9002,-73.9124),
    "10472":(40.8290,-73.8651),"10473":(40.8160,-73.8610),"10474":(40.8085,-73.8895),
    "10475":(40.8741,-73.8275),"11201":(40.6928,-73.9903),"11203":(40.6462,-73.9388),
    "11204":(40.6201,-73.9862),"11205":(40.6940,-73.9677),"11206":(40.7027,-73.9372),
    "11207":(40.6647,-73.8941),"11208":(40.6626,-73.8717),"11209":(40.6201,-74.0298),
    "11210":(40.6283,-73.9454),"11211":(40.7141,-73.9530),"11212":(40.6609,-73.9163),
    "11213":(40.6697,-73.9391),"11214":(40.6096,-73.9968),"11215":(40.6601,-73.9836),
    "11216":(40.6797,-73.9497),"11217":(40.6834,-73.9780),"11218":(40.6393,-73.9742),
    "11219":(40.6313,-73.9966),"11220":(40.6399,-74.0144),"11221":(40.6899,-73.9264),
    "11222":(40.7271,-73.9511),"11223":(40.5986,-73.9729),"11224":(40.5755,-74.0005),
    "11225":(40.6600,-73.9548),"11226":(40.6461,-73.9560),"11228":(40.6167,-74.0124),
    "11229":(40.6024,-73.9390),"11230":(40.6214,-73.9633),"11231":(40.6755,-74.0029),
    "11232":(40.6525,-74.0042),"11233":(40.6780,-73.9168),"11234":(40.6216,-73.9112),
    "11235":(40.5888,-73.9527),"11236":(40.6392,-73.9013),"11237":(40.7027,-73.9175),
    "11238":(40.6773,-73.9646),"11239":(40.6474,-73.8725),"10001":(40.7484,-74.0018),
    "10002":(40.7157,-73.9863),"10003":(40.7317,-73.9891),"10004":(40.7003,-74.0396),
    "10005":(40.7074,-74.0113),"10006":(40.7083,-74.0134),"10007":(40.7135,-74.0078),
    "10009":(40.7263,-73.9779),"10010":(40.7390,-73.9836),"10011":(40.7459,-74.0013),
    "10012":(40.7260,-74.0004),"10013":(40.7191,-74.0062),"10014":(40.7334,-74.0035),
    "10016":(40.7474,-73.9799),"10017":(40.7530,-73.9717),"10018":(40.7557,-73.9926),
    "10019":(40.7654,-73.9861),"10021":(40.7726,-73.9587),"10022":(40.7589,-73.9680),
    "10023":(40.7800,-73.9814),"10024":(40.7870,-73.9764),"10025":(40.7988,-73.9666),
    "10026":(40.8030,-73.9540),"10027":(40.8115,-73.9527),"10028":(40.7775,-73.9503),
    "10029":(40.7940,-73.9437),"10030":(40.8188,-73.9414),"10031":(40.8248,-73.9499),
    "10032":(40.8387,-73.9410),"10033":(40.8502,-73.9343),"10034":(40.8671,-73.9228),
    "10035":(40.7990,-73.9336),"10036":(40.7596,-73.9896),"10037":(40.8129,-73.9388),
    "10038":(40.7081,-74.0029),"10039":(40.8215,-73.9367),"10040":(40.8588,-73.9293),
    "11101":(40.7484,-73.9389),"11102":(40.7724,-73.9302),"11103":(40.7635,-73.9268),
    "11104":(40.7447,-73.9200),"11105":(40.7796,-73.9019),"11106":(40.7598,-73.9336),
    "11354":(40.7677,-73.8330),"11355":(40.7484,-73.8295),"11356":(40.7862,-73.8427),
    "11357":(40.7937,-73.8139),"11358":(40.7571,-73.7981),"11360":(40.7836,-73.7750),
    "11361":(40.7746,-73.7658),"11362":(40.7637,-73.7366),"11363":(40.7754,-73.7483),
    "11364":(40.7477,-73.7527),"11365":(40.7358,-73.7894),"11366":(40.7247,-73.7850),
    "11367":(40.7298,-73.8236),"11368":(40.7482,-73.8633),"11369":(40.7618,-73.8882),
    "11370":(40.7539,-73.8861),"11372":(40.7497,-73.8834),"11373":(40.7375,-73.8760),
    "11374":(40.7256,-73.8609),"11375":(40.7198,-73.8458),"11377":(40.7473,-73.9072),
    "11378":(40.7235,-73.9107),"11379":(40.7197,-73.8826),"11385":(40.7046,-73.9047),
    "11411":(40.6940,-73.7348),"11412":(40.6960,-73.7617),"11413":(40.6693,-73.7566),
    "11414":(40.6579,-73.8483),"11415":(40.7097,-73.8303),"11416":(40.6826,-73.8527),
    "11417":(40.6759,-73.8449),"11418":(40.6988,-73.8260),"11419":(40.6904,-73.8180),
    "11420":(40.6726,-73.8193),"11421":(40.6938,-73.8591),"11422":(40.6613,-73.7392),
    "11423":(40.7108,-73.7681),"11424":(40.7005,-73.8050),"11426":(40.7285,-73.7213),
    "11427":(40.7232,-73.7449),"11428":(40.7162,-73.7407),"11429":(40.7092,-73.7390),
    "11432":(40.7112,-73.7943),"11433":(40.6989,-73.7878),"11434":(40.6825,-73.7743),
    "11435":(40.7017,-73.8072),"11436":(40.6759,-73.7879),"11691":(40.6037,-73.7567),
    "11692":(40.5938,-73.7925),"11693":(40.5955,-73.8134),"11694":(40.5793,-73.8465),
    "11697":(40.5594,-73.9271),"10301":(40.6259,-74.0939),"10302":(40.6335,-74.1355),
    "10303":(40.6324,-74.1624),"10304":(40.6103,-74.0820),"10305":(40.6043,-74.0648),
    "10306":(40.5726,-74.1134),"10307":(40.5114,-74.2497),"10308":(40.5533,-74.1502),
    "10309":(40.5436,-74.2086),"10310":(40.6332,-74.1123),"10311":(40.5938,-74.1784),
    "10312":(40.5502,-74.1718),"10314":(40.6049,-74.1635),
}

crosswalk = list(db["crosswalk"].find({}, {"_id": 0}))
crosswalk_df = pd.DataFrame(crosswalk)

def stores_to_reach(score, target, pop):
    points = max(0, target - score)
    per_10k = points / 8.0
    return max(1, math.ceil(per_10k * pop / 10000)) if pop > 0 else 1

placement = []
for doc in summary:
    tier  = doc.get("access_tier", "")
    score = doc.get("food_access_score") or 0
    pop   = doc.get("total_population") or 0
    hood  = doc.get("neighborhood", "")

    if tier not in ("Food Desert", "At Risk"):
        continue

    # Find centroid
    zips = crosswalk_df[crosswalk_df["neighborhood"] == hood]["zip_code"].tolist()
    lats, lons = [], []
    for z in zips:
        c = ZIP_CENTROIDS.get(str(z))
        if c:
            lats.append(c[0])
            lons.append(c[1])
    if not lats:
        continue

    placement.append({
        "neighborhood":         hood,
        "borough":              doc.get("borough", ""),
        "current_tier":         tier,
        "current_score":        round(score, 1),
        "population":           int(pop),
        "stores_to_at_risk":    stores_to_reach(score, FOOD_DESERT_THRESHOLD, pop) if tier == "Food Desert" else None,
        "stores_to_good_access":stores_to_reach(score, GOOD_ACCESS_THRESHOLD, pop),
        "lat":                  round(sum(lats)/len(lats), 6),
        "lon":                  round(sum(lons)/len(lons), 6),
    })

with open(os.path.join(OUT_DIR, "placement.json"), "w") as f:
    json.dump(placement, f)
print(f"  {len(placement)} neighborhoods exported")


# ── 4. NTA GeoJSON — fetch and save locally ──────────────────
print("\nFetching NTA GeoJSON from NYC Open Data...")
resp = requests.get(
    "https://data.cityofnewyork.us/resource/9nt8-h7nd.geojson?$limit=300",
    timeout=30
)
nta_geojson = resp.json()

# Score lookup
score_lookup = {d["neighborhood"]: d for d in summary}

NTA_TO_NEIGHBORHOOD = {
    "Soundview-Bruckner-Bronx River":"Soundview","Mott Haven-Port Morris":"South Bronx",
    "Melrose":"Melrose","Morrisania":"Melrose","Highbridge":"Highbridge",
    "Concourse-Concourse Village":"Highbridge","Mount Eden-Claremont (West)":"Highbridge",
    "Claremont Village-Claremont (East)":"Highbridge","Mount Hope":"University Heights",
    "Tremont":"Belmont","Crotona Park East":"West Farms","West Farms-Longwood":"West Farms",
    "Longwood":"Longwood","Hunts Point":"Hunts Point","Fordham":"Fordham","Belmont":"Belmont",
    "Bedford Park":"Norwood","Norwood":"Norwood","Williamsbridge-Olinville":"Wakefield",
    "Wakefield-Woodlawn":"Wakefield","Eastchester-Edenwald-Baychester":"Wakefield",
    "Allerton":"Pelham Parkway","Pelham Parkway":"Pelham Parkway","Morris Park":"Pelham Parkway",
    "Pelham Gardens":"Pelham Parkway","Westchester Square":"Throggs Neck",
    "Throgs Neck-Schuylerville":"Throggs Neck","Castle Hill-Unionport":"Soundview",
    "Parkchester":"Parkchester","Van Nest-Morris Park-Bronxdale":"Parkchester",
    "University Heights-Morris Heights":"University Heights","Kingsbridge Heights":"Kingsbridge",
    "Kingsbridge-Marble Hill":"Kingsbridge","Riverdale":"Riverdale","Co-op City":"Co-op City",
    "City Island":"City Island","Fordham Heights":"Fordham",
    "University Heights (North)-Fordham":"University Heights",
    "University Heights (South)-Morris Heights":"University Heights",
    "Kingsbridge Heights-Van Cortlandt Village":"Kingsbridge",
    "Riverdale-Spuyten Duyvil":"Riverdale","Pelham Bay-Country Club-City Island":"Co-op City",
    "Pelham Parkway-Van Nest":"Pelham Parkway","West Farms":"West Farms",
    "Soundview-Clason Point":"Soundview",
    "Bushwick (East)":"Bushwick","Bushwick (West)":"Bushwick","East Williamsburg":"Bushwick",
    "Bedford-Stuyvesant (East)":"Bedford-Stuyvesant","Bedford-Stuyvesant (West)":"Bedford-Stuyvesant",
    "Crown Heights (North)":"Crown Heights","Crown Heights (South)":"Crown Heights",
    "East New York":"East New York","Spring Creek-Starrett City":"East New York",
    "Cypress Hills":"East New York","East New York (North)":"East New York",
    "East New York-City Line":"East New York","East New York-New Lots":"East New York",
    "East Flatbush-Farragut":"East Flatbush","East Flatbush-Erasmus":"East Flatbush",
    "East Flatbush-Remsen Village":"East Flatbush","East Flatbush-Rugby":"East Flatbush",
    "Flatbush":"Flatbush","Prospect Lefferts Gardens-Wingate":"Flatbush",
    "Flatbush (West)-Ditmas Park-Parkville":"Flatbush","Mapleton-Midwood (West)":"Midwood",
    "Kensington-Ocean Parkway":"Kensington","Kensington":"Kensington",
    "Borough Park":"Borough Park","Bensonhurst (East)":"Bensonhurst",
    "Bensonhurst (West)":"Bensonhurst","Bensonhurst":"Bensonhurst",
    "Gravesend":"Gravesend","Bath Beach":"Gravesend","Gravesend (East)-Homecrest":"Gravesend",
    "Gravesend (South)":"Gravesend","Gravesend (West)":"Gravesend",
    "Sheepshead Bay-Gerritsen Beach-Manhattan Beach":"Sheepshead Bay",
    "Sheepshead Bay-Manhattan Beach-Gerritsen Beach":"Sheepshead Bay",
    "Marine Park-Bergen Beach-Mill Basin":"Marine Park","Marine Park-Mill Basin-Bergen Beach":"Marine Park",
    "Canarsie":"Canarsie","Brownsville":"Brownsville","Ocean Hill":"Brownsville",
    "Sunset Park (East)":"Sunset Park","Sunset Park (West)":"Sunset Park",
    "Sunset Park (Central)":"Sunset Park","Sunset Park (East)-Borough Park (West)":"Sunset Park",
    "Red Hook":"Red Hook","Carroll Gardens-Cobble Hill-Gowanus-Red Hook":"Red Hook",
    "Park Slope-Gowanus":"Park Slope","Windsor Terrace-South Slope":"Park Slope","Park Slope":"Park Slope",
    "Prospect Heights":"Prospect Heights","Clinton Hill":"Clinton Hill","Fort Greene":"Clinton Hill",
    "Brooklyn Heights-Cobble Hill":"Brooklyn Heights","Brooklyn Heights":"Brooklyn Heights",
    "Downtown Brooklyn-DUMBO-Boerum Hill":"Brooklyn Heights",
    "Williamsburg":"Williamsburg","South Williamsburg":"Williamsburg",
    "Greenpoint":"Greenpoint","Dyker Heights":"Dyker Heights","Bay Ridge":"Bay Ridge",
    "Midwood":"Midwood","Madison":"Midwood","Flatlands":"Marine Park",
    "Coney Island-Sea Gate":"Coney Island","Brighton Beach":"Sheepshead Bay",
    "East Harlem (North)":"East Harlem","East Harlem (South)":"East Harlem",
    "Harlem":"Harlem","Central Harlem (North)-Polo Grounds":"Harlem",
    "Harlem (North)":"Harlem","Harlem (South)":"Harlem",
    "Hamilton Heights":"Hamilton Heights","Hamilton Heights-Sugar Hill":"Hamilton Heights",
    "Manhattanville-West Harlem":"Hamilton Heights",
    "Washington Heights (North)":"Washington Heights","Washington Heights (South)":"Washington Heights",
    "Inwood":"Inwood","Morningside Heights":"Morningside Heights",
    "Upper West Side":"Upper West Side","Upper West Side (Central)":"Upper West Side",
    "Upper West Side-Lincoln Square":"Upper West Side","Upper West Side-Manhattan Valley":"Upper West Side",
    "Upper East Side-Carnegie Hill":"Upper East Side","Yorkville":"Upper East Side",
    "Lenox Hill-Roosevelt Island":"Upper East Side",
    "Upper East Side-Lenox Hill-Roosevelt Island":"Upper East Side",
    "Upper East Side-Yorkville":"Upper East Side",
    "Hell's Kitchen":"Hell's Kitchen","Midtown-Times Square":"Hell's Kitchen",
    "East Midtown-Turtle Bay":"Midtown East","Murray Hill-Kip's Bay":"Murray Hill",
    "Murray Hill-Kips Bay":"Murray Hill","Murray Hill-Broadway Flushing":"Flushing",
    "Gramercy":"Gramercy","Midtown South-Flatiron-Union Square":"Gramercy",
    "Stuyvesant Town-Peter Cooper Village":"Gramercy",
    "Chelsea-Hudson Yards":"Chelsea","West Village":"West Village",
    "Greenwich Village":"West Village","SoHo-TriBeCa-Civic Center-Little Italy":"Tribeca",
    "Tribeca-Civic Center":"Tribeca","SoHo":"SoHo","SoHo-Little Italy-Hudson Square":"SoHo",
    "Lower East Side":"Lower East Side","Chinatown-Two Bridges":"Lower East Side",
    "East Village":"East Village","Financial District-Battery Park City":"Financial District",
    "Astoria (North)-Ditmars-Steinway":"Astoria","Astoria (Central)":"Astoria",
    "Astoria (East)-Woodside (North)":"Astoria","Old Astoria-Hallets Point":"Astoria",
    "Long Island City-Hunters Point":"Long Island City",
    "Queensbridge-Ravenswood-Dutch Kills":"Long Island City",
    "Sunnyside":"Sunnyside","Woodside":"Woodside","Jackson Heights":"Jackson Heights",
    "East Elmhurst":"Jackson Heights","Elmhurst":"Elmhurst","Corona":"Corona",
    "North Corona":"Corona","Flushing":"Flushing","Queensboro Hill":"Flushing",
    "East Flushing":"Flushing","Flushing-Willets Point":"Flushing",
    "Pomonok-Electchester-Hillcrest":"Fresh Meadows","Fresh Meadows-Utopia":"Fresh Meadows",
    "Auburndale":"Bayside","Bayside":"Bayside","Bay Terrace-Clearview":"Bayside",
    "Whitestone":"Whitestone","Whitestone-Beechhurst":"Whitestone",
    "College Point":"College Point","Forest Hills":"Forest Hills","Rego Park":"Rego Park",
    "Middle Village":"Middle Village","Glendale":"Middle Village","Ridgewood":"Ridgewood",
    "Maspeth":"Maspeth","Woodhaven":"Woodhaven","Richmond Hill":"Richmond Hill",
    "South Richmond Hill":"Richmond Hill","Kew Gardens":"Kew Gardens",
    "Kew Gardens Hills":"Kew Gardens Hills","Jamaica":"Jamaica",
    "Jamaica Estates-Holliswood":"Jamaica","Baisley Park":"Jamaica","South Jamaica":"Jamaica",
    "Jamaica Hills-Briarwood":"Jamaica","Hollis":"Hollis","St. Albans":"St. Albans",
    "Queens Village":"Queens Village","Cambria Heights":"Cambria Heights",
    "Rosedale":"Rosedale","Laurelton":"Rosedale",
    "Springfield Gardens (North)":"Springfield Gardens",
    "Springfield Gardens (South)-Brookville":"Springfield Gardens",
    "Springfield Gardens (North)-Rochdale Village":"Springfield Gardens",
    "Far Rockaway-Bayswater":"Far Rockaway","Arverne-Edgemere":"Arverne",
    "Rockaway Beach-Arverne-Edgemere":"Arverne",
    "Breezy Point-Belle Harbor-Rockaway Park-Broad Channel":"Breezy Point",
    "Howard Beach":"Howard Beach","Howard Beach-Lindenwood":"Howard Beach",
    "South Ozone Park":"South Ozone Park","Ozone Park":"Ozone Park",
    "Ozone Park (North)":"Ozone Park",
    "Glen Oaks-Floral Park-New Hyde Park":"Glen Oaks","Floral Park-Bellerose":"Floral Park",
    "Bellerose":"Bellerose","Oakland Gardens":"Oakland Gardens",
    "Oakland Gardens-Hollis Hills":"Oakland Gardens",
    "Little Neck":"Little Neck","Douglaston-Little Neck":"Little Neck",
    "St. George-New Brighton":"St. George","Port Richmond":"Port Richmond",
    "Mariners Harbor-Arlington-Graniteville":"Mariners Harbor",
    "Mariner's Harbor-Arlington-Graniteville":"Mariners Harbor",
    "Stapleton-Rosebank":"Stapleton","Rosebank":"Rosebank",
    "Rosebank-Shore Acres-Park Hill":"Rosebank",
    "Tompkinsville-Stapleton-Clifton-Fox Hills":"Stapleton",
    "New Brighton-Silver Lake":"West Brighton",
    "West New Brighton-Silver Lake-Grymes Hill":"West Brighton",
    "Westerleigh-Castleton Corners":"West Brighton",
    "New Dorp-Midland Beach":"New Dorp","Oakwood-Richmondtown":"New Dorp",
    "Todt Hill-Emerson Hill-Lighthouse Hill-Manor Heights":"New Dorp",
    "Grasmere-Arrochar-South Beach-Dongan Hills":"Rosebank",
    "Great Kills":"Great Kills","Great Kills-Eltingville":"Great Kills",
    "Eltingville-Annadale-Prince's Bay":"Eltingville",
    "Eltingville-Annadale-Prince Bay":"Eltingville",
    "Annadale-Huguenot-Prince's Bay-Woodrow":"Rossville",
    "Arden Heights-Rossville":"Rossville","Rossville-Woodrow":"Rossville",
    "Tottenville-Charleston":"Tottenville","Willowbrook":"Willowbrook",
    "New Springville-Willowbrook-Bulls Head-Travis":"Willowbrook",
}

# Annotate GeoJSON with scores
for feature in nta_geojson["features"]:
    props   = feature["properties"]
    nta_nm  = props.get("ntaname", "")
    nta_type= props.get("ntatype", "0")
    hood    = NTA_TO_NEIGHBORHOOD.get(nta_nm)
    data    = score_lookup.get(hood, {}) if hood else {}

    props["neighborhood"]           = hood or nta_nm
    props["nta_type"]               = nta_type
    props["food_access_score"]      = data.get("food_access_score")
    props["access_tier"]            = data.get("access_tier", "Unknown")
    props["total_population"]       = data.get("total_population")
    props["avg_median_income"]      = data.get("avg_median_income")
    props["poverty_rate"]           = data.get("poverty_rate")
    props["snap_grocery_per_10k"]   = data.get("snap_grocery_per_10k")
    props["snap_healthy_ratio"]     = data.get("snap_healthy_ratio")
    props["snap_convenience_ratio"] = data.get("snap_convenience_ratio")
    props["year_round_markets"]     = data.get("year_round_markets")
    props["total_snap"]             = data.get("total_snap")
    props["borough"]                = data.get("borough", props.get("boroname", ""))

with open(os.path.join(OUT_DIR, "nta.geojson"), "w") as f:
    json.dump(nta_geojson, f)
print(f"  {len(nta_geojson['features'])} NTA features exported")

print("\nAll data exported:")
print(f"  {OUT_DIR}/summary.json")
print(f"  {OUT_DIR}/snap_stores.json")
print(f"  {OUT_DIR}/placement.json")
print(f"  {OUT_DIR}/nta.geojson")
print("\nNext: run npm run dev in observable-site and open http://localhost:3000/map")