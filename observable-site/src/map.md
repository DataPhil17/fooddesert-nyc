# Map

## Exploring Food Access Across NYC

Click any neighborhood to see its full food access profile. Use the layer control in the top right to toggle between the neighborhood choropleth, SNAP grocery store locations, and proposed intervention sites.

```html
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<div id="nyc-map" style="height:680px;width:100%;border-radius:8px;border:1px solid #C4B49A;margin:16px 0;"></div>
```

```js
// Load Leaflet from CDN
await import("https://unpkg.com/leaflet@1.9.4/dist/leaflet.js");
const L = window.L;

// Color palette — Paul Tol colorblind-safe
const TIER_COLORS  = {"Good Access":"#0077BB","At Risk":"#EE7733","Food Desert":"#CC3311","Unknown":"#CCCCCC"};
const TIER_BORDERS = {"Good Access":"#005588","At Risk":"#994400","Food Desert":"#880000","Unknown":"#999999"};
const STORE_COLORS = {
  "Supermarket":             {color:"#004488", radius:5},
  "Super Store":             {color:"#004488", radius:5},
  "Grocery Store":           {color:"#33BBEE", radius:4},
  "Farmers' Markets":        {color:"#009988", radius:4},
  "ethnic_grocery_override": {color:"#EE7733", radius:4},
  "Specialty Store":         {color:"#BBBBBB", radius:3},
};

const fmt  = (v, d=1) => v != null ? Number(v).toFixed(d) : "N/A";
const fmtI = (v)      => v != null ? Number(v).toLocaleString() : "N/A";
const fmtD = (v)      => v != null ? "$" + Number(v).toLocaleString(undefined, {maximumFractionDigits:0}) : "N/A";

// Initialize map
const map = L.map("nyc-map", {center:[40.7128,-74.0060], zoom:11});
L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  attribution: "&copy; OpenStreetMap &copy; CARTO",
  subdomains: "abcd",
  maxZoom: 19
}).addTo(map);

const neighborhoodLayer = L.layerGroup().addTo(map);
const storeLayer        = L.layerGroup();
const placementLayer    = L.layerGroup();

// Load NTA GeoJSON
const geojson = await FileAttachment("data/nta.geojson").json();

L.geoJSON(geojson, {
  style: f => {
    const tier = f.properties.access_tier || "Unknown";
    const t    = f.properties.nta_type || "0";
    if (t !== "0") return {fillColor:"#E0E8E0", fillOpacity:0.25, color:"#CCCCCC", weight:0.3};
    return {
      fillColor:   TIER_COLORS[tier]  || "#CCCCCC",
      fillOpacity: tier === "Unknown" ? 0.2 : 0.78,
      color:       TIER_BORDERS[tier] || "#999999",
      weight: 1.8
    };
  },
  onEachFeature: (f, layer) => {
    const p = f.properties;
    if (p.nta_type !== "0") return;
    const tier = p.access_tier || "Unknown";
    const tc   = tier === "Food Desert" ? "#CC3311" : tier === "At Risk" ? "#EE7733" : "#0077BB";

    layer.bindTooltip(
      `<strong>${p.neighborhood}</strong><br>${p.borough || ""} — Score: ${fmt(p.food_access_score)}`,
      {sticky: true}
    );

    layer.bindPopup(`
      <div style="font-family:Arial,sans-serif;width:255px;padding:4px;">
        <div style="font-size:15px;font-weight:700;color:#2C2416;margin-bottom:3px;">${p.neighborhood}</div>
        <div style="font-size:11px;color:#6B5C48;margin-bottom:7px;">${p.borough || ""}</div>
        <div style="margin-bottom:9px;">
          <span style="background:${tc};color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;">${tier}</span>
          <span style="float:right;font-size:13px;font-weight:700;color:#2C2416;">Score: ${fmt(p.food_access_score)}</span>
        </div>
        <table style="width:100%;font-size:11px;border-collapse:collapse;">
          <tr style="border-bottom:1px solid #EDE6DC;">
            <td style="padding:3px 4px;color:#6B5C48;">Population</td>
            <td style="padding:3px 4px;text-align:right;font-weight:600;">${fmtI(p.total_population)}</td>
          </tr>
          <tr style="border-bottom:1px solid #EDE6DC;background:#F5F0EB;">
            <td style="padding:3px 4px;color:#6B5C48;">Median Income</td>
            <td style="padding:3px 4px;text-align:right;font-weight:600;">${fmtD(p.avg_median_income)}</td>
          </tr>
          <tr style="border-bottom:1px solid #EDE6DC;">
            <td style="padding:3px 4px;color:#6B5C48;">Poverty Rate</td>
            <td style="padding:3px 4px;text-align:right;font-weight:600;">${fmt(p.poverty_rate)}%</td>
          </tr>
          <tr style="border-bottom:1px solid #EDE6DC;background:#F5F0EB;">
            <td style="padding:3px 4px;color:#6B5C48;">SNAP Grocery/10k</td>
            <td style="padding:3px 4px;text-align:right;font-weight:600;">${fmt(p.snap_grocery_per_10k, 2)}</td>
          </tr>
          <tr style="border-bottom:1px solid #EDE6DC;">
            <td style="padding:3px 4px;color:#6B5C48;">Healthy Retailer %</td>
            <td style="padding:3px 4px;text-align:right;font-weight:600;">${fmt(p.snap_healthy_ratio)}%</td>
          </tr>
          <tr style="border-bottom:1px solid #EDE6DC;background:#F5F0EB;">
            <td style="padding:3px 4px;color:#6B5C48;">Convenience Store %</td>
            <td style="padding:3px 4px;text-align:right;font-weight:600;">${fmt(p.snap_convenience_ratio)}%</td>
          </tr>
          <tr>
            <td style="padding:3px 4px;color:#6B5C48;">Year-Round Markets</td>
            <td style="padding:3px 4px;text-align:right;font-weight:600;">${p.year_round_markets != null ? Math.round(p.year_round_markets) : "0"}</td>
          </tr>
        </table>
      </div>`, {maxWidth:275}
    );

    layer.on("mouseover", e => e.target.setStyle({fillOpacity:0.92, weight:2.5}));
    layer.on("mouseout",  e => e.target.setStyle({fillOpacity: tier==="Unknown"?0.2:0.78, weight:1.8}));
  }
}).addTo(neighborhoodLayer);

// Load SNAP stores
const stores = await FileAttachment("data/snap_stores.json").json();
for (const s of stores) {
  const key   = s.reason === "ethnic_grocery_override" ? "ethnic_grocery_override" : s.type;
  const style = STORE_COLORS[key] || {color:"#BBBBBB", radius:3};
  const label = s.reason === "ethnic_grocery_override" ? "Ethnic Grocery (reclassified)" : s.type;
  L.circleMarker([s.lat, s.lon], {
    radius: style.radius, color: style.color,
    fillColor: style.color, fillOpacity: 0.82, weight: 0.8
  })
  .bindTooltip(`${s.name} — ${label}`, {sticky:true})
  .addTo(storeLayer);
}

// Load placement markers
const placements = await FileAttachment("data/placement.json").json();
for (const p of placements) {
  const isFD = p.current_tier === "Food Desert";
  const c = isFD ? "#CC3311"
    : p.stores_to_good_access >= 5 ? "#EE7733"
    : p.stores_to_good_access >= 3 ? "#C46010"
    : "#9C8C78";

  const stageHtml = isFD
    ? `<tr style="background:#FFF0EE;">
         <td style="padding:4px;color:#CC3311;font-weight:700;">Stage 1 — Escape Food Desert</td>
         <td style="padding:4px;text-align:right;font-weight:700;color:#CC3311;">${p.stores_to_at_risk} store${p.stores_to_at_risk > 1 ? "s" : ""}</td>
       </tr>
       <tr style="background:#E8F4E8;">
         <td style="padding:4px;color:#006600;font-weight:700;">Stage 2 — Good Access</td>
         <td style="padding:4px;text-align:right;font-weight:700;color:#006600;">${p.stores_to_good_access} total</td>
       </tr>`
    : `<tr style="background:#E8F4E8;">
         <td style="padding:4px;color:#006600;font-weight:700;">Stores to Good Access</td>
         <td style="padding:4px;text-align:right;font-weight:700;color:#006600;">${p.stores_to_good_access} store${p.stores_to_good_access > 1 ? "s" : ""}</td>
       </tr>`;

  const icon = L.divIcon({
    html: `<div style="color:${c};font-size:20px;line-height:1;filter:drop-shadow(0 1px 2px rgba(0,0,0,0.4));">&#9733;</div>`,
    iconSize:[20,20], iconAnchor:[10,10], className:""
  });

  const tooltip = isFD
    ? `${p.neighborhood} (Food Desert) — Stage 1: ${p.stores_to_at_risk} | Stage 2: ${p.stores_to_good_access} total`
    : `${p.neighborhood} (At Risk) — ${p.stores_to_good_access} store${p.stores_to_good_access > 1 ? "s" : ""} to Good Access`;

  L.marker([p.lat, p.lon], {icon})
    .bindTooltip(tooltip, {sticky:true})
    .bindPopup(`
      <div style="font-family:Arial,sans-serif;width:265px;padding:4px;">
        <div style="font-size:14px;font-weight:700;color:#2C2416;margin-bottom:4px;">&#9733; ${p.neighborhood}</div>
        <div style="font-size:11px;color:#6B5C48;margin-bottom:8px;">${p.borough}</div>
        <table style="width:100%;font-size:11px;border-collapse:collapse;">
          <tr style="border-bottom:1px solid #EDE6DC;">
            <td style="padding:3px 4px;color:#6B5C48;">Current Score</td>
            <td style="padding:3px 4px;text-align:right;font-weight:600;color:${c};">${p.current_score}</td>
          </tr>
          <tr style="border-bottom:1px solid #EDE6DC;background:#F5F0EB;">
            <td style="padding:3px 4px;color:#6B5C48;">Population</td>
            <td style="padding:3px 4px;text-align:right;font-weight:600;">${fmtI(p.population)}</td>
          </tr>
          ${stageHtml}
        </table>
        <div style="margin-top:8px;font-size:9px;color:#9C8C78;">Each store = +8 pts per 10k residents</div>
      </div>`, {maxWidth:285}
    )
    .addTo(placementLayer);
}

// Legend
const legend = L.control({position:"bottomleft"});
legend.onAdd = () => {
  const d = L.DomUtil.create("div");
  d.innerHTML = `
    <div style="background:rgba(245,240,235,0.96);border:1px solid #C4B49A;border-radius:8px;
                padding:14px 16px;font-family:Arial,sans-serif;min-width:190px;
                box-shadow:0 2px 8px rgba(0,0,0,0.12);">
      <div style="font-size:11px;font-weight:700;color:#2C2416;margin-bottom:8px;">NYC Food Desert Analysis</div>
      <div style="font-size:9px;font-weight:600;color:#6B5C48;margin-bottom:5px;text-transform:uppercase;letter-spacing:0.05em;">Access Tier</div>
      <div style="display:flex;align-items:center;margin-bottom:3px;">
        <div style="width:12px;height:12px;border-radius:2px;background:#0077BB;margin-right:7px;flex-shrink:0;"></div>
        <span style="font-size:10px;color:#2C2416;">Good Access (&#8805;54.7)</span>
      </div>
      <div style="display:flex;align-items:center;margin-bottom:3px;">
        <div style="width:12px;height:12px;border-radius:2px;background:#EE7733;margin-right:7px;flex-shrink:0;"></div>
        <span style="font-size:10px;color:#2C2416;">At Risk (25.2&#8211;54.7)</span>
      </div>
      <div style="display:flex;align-items:center;margin-bottom:10px;">
        <div style="width:12px;height:12px;border-radius:2px;background:#CC3311;margin-right:7px;flex-shrink:0;"></div>
        <span style="font-size:10px;color:#2C2416;">Food Desert (&lt;25.2)</span>
      </div>
      <div style="font-size:9px;font-weight:600;color:#6B5C48;margin-bottom:5px;text-transform:uppercase;letter-spacing:0.05em;">SNAP Stores</div>
      <div style="display:flex;align-items:center;margin-bottom:3px;">
        <div style="width:11px;height:11px;border-radius:50%;background:#004488;margin-right:7px;flex-shrink:0;"></div>
        <span style="font-size:10px;color:#2C2416;">Supermarket</span>
      </div>
      <div style="display:flex;align-items:center;margin-bottom:3px;">
        <div style="width:9px;height:9px;border-radius:50%;background:#33BBEE;margin-right:7px;flex-shrink:0;"></div>
        <span style="font-size:10px;color:#2C2416;">Grocery Store</span>
      </div>
      <div style="display:flex;align-items:center;margin-bottom:3px;">
        <div style="width:9px;height:9px;border-radius:50%;background:#EE7733;margin-right:7px;flex-shrink:0;"></div>
        <span style="font-size:10px;color:#2C2416;">Ethnic Grocery</span>
      </div>
      <div style="display:flex;align-items:center;margin-bottom:10px;">
        <div style="width:9px;height:9px;border-radius:50%;background:#009988;margin-right:7px;flex-shrink:0;"></div>
        <span style="font-size:10px;color:#2C2416;">Farmers Market</span>
      </div>
      <div style="font-size:9px;font-weight:600;color:#6B5C48;margin-bottom:5px;text-transform:uppercase;letter-spacing:0.05em;">Intervention Stars</div>
      <div style="display:flex;align-items:center;margin-bottom:3px;">
        <span style="color:#CC3311;font-size:14px;margin-right:6px;">&#9733;</span>
        <span style="font-size:10px;color:#2C2416;">Food Desert (urgent)</span>
      </div>
      <div style="display:flex;align-items:center;margin-bottom:3px;">
        <span style="color:#EE7733;font-size:14px;margin-right:6px;">&#9733;</span>
        <span style="font-size:10px;color:#2C2416;">At Risk, 5+ stores</span>
      </div>
      <div style="display:flex;align-items:center;">
        <span style="color:#9C8C78;font-size:14px;margin-right:6px;">&#9733;</span>
        <span style="font-size:10px;color:#2C2416;">At Risk, 1&#8211;4 stores</span>
      </div>
      <div style="margin-top:10px;padding-top:8px;border-top:1px solid #EDE6DC;font-size:9px;color:#9C8C78;line-height:1.4;">
        116 neighborhoods &middot; ACS 2023 &middot; USDA SNAP
      </div>
    </div>`;
  return d;
};
legend.addTo(map);

// Layer control
L.control.layers({}, {
  "Neighborhood Access Tiers": neighborhoodLayer,
  "SNAP Grocery Stores":       storeLayer,
  "Proposed Grocery Locations":placementLayer,
}, {position:"topright", collapsed:false}).addTo(map);
```

---

**Layer Guide**

Toggle layers using the control in the top right corner of the map.

- **Neighborhood Access Tiers** — choropleth colored by food access score. Click any neighborhood for its full profile.
- **SNAP Grocery Stores** — every healthy SNAP-authorized retailer plotted by location. Toggle on to visualize density differences between neighborhoods.
- **Proposed Grocery Locations** — star markers for all 58 Food Desert and At Risk neighborhoods with intervention recommendations.