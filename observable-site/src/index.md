# NYC Food Desert Analysis

## Rethinking Food Access in New York City

The United States Department of Agriculture defines a food desert as any low-income census tract where a significant portion of residents live more than one mile from a supermarket. In a city where most residents travel by foot or subway, that definition misses the point entirely.

According to the U.S. Census Bureau's 2024 American Community Survey, **56.7% of New York City households have no car** — making distance to a supermarket a poor proxy for food access in an environment built around walking and transit. A neighborhood with ten bodegas within walking distance is not food-secure simply because those bodegas are nearby. And a neighborhood where the only nearby grocery stores are priced beyond what residents can afford does not have good food access simply because stores exist.

This analysis examines food access across **116 New York City neighborhoods** using a custom Food Access Score built from seven data sources. Rather than measuring distance to the nearest supermarket, it measures the density, quality, and affordability of the food retail environment — the factors that actually determine whether residents can access healthy food in an urban context.

---

## Key Findings

<div style="display:grid; grid-template-columns: repeat(3, 1fr); gap:16px; margin: 24px 0;">

<div style="background:#EDE6DC; border:1px solid #C4B49A; border-left: 4px solid #CC3311; border-radius:8px; padding:20px;">
  <div style="font-size:36px; font-weight:700; color:#CC3311;">13</div>
  <div style="font-size:14px; color:#4A3828; font-weight:600; margin-top:4px;">Food Desert Neighborhoods</div>
  <div style="font-size:12px; color:#6B5C48; margin-top:6px;">Score below 25.2 — critically low grocery access</div>
</div>

<div style="background:#EDE6DC; border:1px solid #C4B49A; border-left: 4px solid #EE7733; border-radius:8px; padding:20px;">
  <div style="font-size:36px; font-weight:700; color:#EE7733;">46</div>
  <div style="font-size:14px; color:#4A3828; font-weight:600; margin-top:4px;">At Risk Neighborhoods</div>
  <div style="font-size:12px; color:#6B5C48; margin-top:6px;">Score 25.2–54.7 — marginal access with measurable gaps</div>
</div>

<div style="background:#EDE6DC; border:1px solid #C4B49A; border-left: 4px solid #0077BB; border-radius:8px; padding:20px;">
  <div style="font-size:36px; font-weight:700; color:#0077BB;">57</div>
  <div style="font-size:14px; color:#4A3828; font-weight:600; margin-top:4px;">Good Access Neighborhoods</div>
  <div style="font-size:12px; color:#6B5C48; margin-top:6px;">Score above 54.7 — adequate grocery density and quality</div>
</div>

</div>

---

## Why the USDA Definition Falls Short

Distance-based food desert definitions were designed for rural and suburban contexts where car ownership is common and supermarkets are sparse. In New York City — where the majority of residents travel by foot or subway — proximity alone is an inadequate measure of food access.

This analysis measures what actually matters: how many authorized food retailers serve residents per capita, what proportion of those retailers sell healthy food, whether the food environment is balanced between grocery stores and fast food, and whether prices are accessible given local incomes.

---

## A Note on Methodology

This project uses the USDA SNAP retailer authorization database as its primary data source, which captures stores authorized to accept SNAP benefits. This intentionally anchors the analysis to the food access reality of low-income residents — the population most affected by food desert conditions.

457 ethnic grocery stores — halal markets, carnecerias, African grocers, produce markets — were reclassified from the USDA's generic "Convenience Store" category to healthy retail. These stores represent real food access infrastructure that standard classification systems systematically undercount.

Thresholds separating Food Desert, At Risk, and Good Access tiers are back-calculated from defined archetypes rather than chosen to produce a target distribution.

---

## Data Sources

| Source | Records | Coverage |
|---|---|---|
| ACS 2023 5-Year Estimates | 173 ZIP codes | Population, income, poverty |
| USDA SNAP Retailer Locator | 8,457 stores | All authorized NYC retailers |
| NYC DOHMH Restaurant Inspections | 26,014 restaurants | Health grades, cuisine type |
| NYC DOHMH Farmers Markets | 1,362 markets | Location, EBT acceptance, seasonality |
| NY State Dept of Agriculture | 8,677 stores | All licensed food retailers |
| NYC Emergency Food Supply Gap | 197 NTAs | Food insecurity rates (2025) |
| NYC GreenThumb | 546 gardens | Active community gardens |

---

*Use the navigation to explore the methodology, findings, interactive map, and intervention analysis.*

<div style="font-size:11px; color:#9C8C78; margin-top:16px;">
Car ownership statistic: U.S. Census Bureau, American Community Survey 2024 1-Year Estimates, via Bloomberg City Lab (September 2025).
</div>
