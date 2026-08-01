# Methodology

## Overview

The Food Access Score is a composite metric designed to measure the quality, density, and affordability of the food retail environment in each NYC neighborhood. It is computed at the ZIP code level and aggregated to neighborhoods using a crosswalk of 173 ZIP codes mapped to 116 neighborhoods across all five boroughs.

---

## The Food Access Score

Each neighborhood receives a score based on eleven weighted variables. Higher scores indicate better food access.

<div style="background:#EDE6DC; border:1px solid #C4B49A; border-radius:8px; padding:20px; margin:20px 0;">

<div style="font-size:14px; font-weight:700; color:#2C2416; margin-bottom:16px;">Food Access Score Formula</div>

<table style="width:100%; border-collapse:collapse; font-size:13px;">
  <thead>
    <tr style="border-bottom:2px solid #C4B49A;">
      <th style="text-align:left; padding:7px 10px; color:#6B5C48; font-size:11px; text-transform:uppercase; letter-spacing:0.05em;">Variable</th>
      <th style="text-align:center; padding:7px 10px; color:#6B5C48; font-size:11px; text-transform:uppercase; letter-spacing:0.05em;">Weight</th>
      <th style="text-align:left; padding:7px 10px; color:#6B5C48; font-size:11px; text-transform:uppercase; letter-spacing:0.05em;">What it measures</th>
    </tr>
  </thead>
  <tbody>
    <tr style="border-bottom:1px solid #C4B49A; background:#F5F0EB;">
      <td style="padding:8px 10px; color:#2C2416; font-weight:600; font-family:monospace;">snap_grocery_per_10k</td>
      <td style="padding:8px 10px; text-align:center; color:#0077BB; font-weight:700;">+8.0</td>
      <td style="padding:8px 10px; color:#4A3828;">SNAP grocery store density per capita</td>
    </tr>
    <tr style="border-bottom:1px solid #C4B49A;">
      <td style="padding:8px 10px; color:#2C2416; font-weight:600; font-family:monospace;">nonsnap_grocery_per_10k</td>
      <td style="padding:8px 10px; text-align:center; color:#0077BB; font-weight:700;">+5.0</td>
      <td style="padding:8px 10px; color:#4A3828;">Non-SNAP grocery access, size-weighted</td>
    </tr>
    <tr style="border-bottom:1px solid #C4B49A; background:#F5F0EB;">
      <td style="padding:8px 10px; color:#2C2416; font-weight:600; font-family:monospace;">membership_per_10k</td>
      <td style="padding:8px 10px; text-align:center; color:#0077BB; font-weight:700;">+3.0</td>
      <td style="padding:8px 10px; color:#4A3828;">Warehouse stores — Costco, BJ's (paywall discount)</td>
    </tr>
    <tr style="border-bottom:1px solid #C4B49A;">
      <td style="padding:8px 10px; color:#2C2416; font-weight:600; font-family:monospace;">convenience_ratio / 100</td>
      <td style="padding:8px 10px; text-align:center; color:#CC3311; font-weight:700;">−8.0</td>
      <td style="padding:8px 10px; color:#4A3828;">Convenience store dominance penalty</td>
    </tr>
    <tr style="border-bottom:1px solid #C4B49A; background:#F5F0EB;">
      <td style="padding:8px 10px; color:#2C2416; font-weight:600; font-family:monospace;">food_balance_ratio</td>
      <td style="padding:8px 10px; text-align:center; color:#0077BB; font-weight:700;">+4.0</td>
      <td style="padding:8px 10px; color:#4A3828;">Grocery vs. fast food balance</td>
    </tr>
    <tr style="border-bottom:1px solid #C4B49A;">
      <td style="padding:8px 10px; color:#2C2416; font-weight:600; font-family:monospace;">year_round_markets</td>
      <td style="padding:8px 10px; text-align:center; color:#0077BB; font-weight:700;">+1.0</td>
      <td style="padding:8px 10px; color:#4A3828;">Reliable year-round fresh produce access</td>
    </tr>
    <tr style="border-bottom:1px solid #C4B49A; background:#F5F0EB;">
      <td style="padding:8px 10px; color:#2C2416; font-weight:600; font-family:monospace;">farmers_markets_per_10k</td>
      <td style="padding:8px 10px; text-align:center; color:#0077BB; font-weight:700;">+0.5</td>
      <td style="padding:8px 10px; color:#4A3828;">Normalized market access per capita</td>
    </tr>
    <tr style="border-bottom:1px solid #C4B49A;">
      <td style="padding:8px 10px; color:#2C2416; font-weight:600; font-family:monospace;">gardens_per_10k</td>
      <td style="padding:8px 10px; text-align:center; color:#0077BB; font-weight:700;">+0.25</td>
      <td style="padding:8px 10px; color:#4A3828;">Community gardens (partial credit)</td>
    </tr>
    <tr style="border-bottom:1px solid #C4B49A; background:#F5F0EB;">
      <td style="padding:8px 10px; color:#2C2416; font-weight:600; font-family:monospace;">pct_grade_A / 100</td>
      <td style="padding:8px 10px; text-align:center; color:#0077BB; font-weight:700;">+0.2</td>
      <td style="padding:8px 10px; color:#4A3828;">Food quality signal — NYC DOHMH Grade A inspections</td>
    </tr>
    <tr>
      <td style="padding:8px 10px; color:#2C2416; font-weight:600; font-family:monospace;">economic_access_gap</td>
      <td style="padding:8px 10px; text-align:center; color:#CC3311; font-weight:700;">−3.0</td>
      <td style="padding:8px 10px; color:#4A3828;">Affordability penalty — income vs. food price mismatch</td>
    </tr>
  </tbody>
</table>

<div style="margin-top:12px; font-size:11px; color:#6B5C48;">
  Blue weights are additive (better access). Red weights are penalties (worse access).
  The convenience store penalty and economic access gap are the two strongest barriers in the formula,
  reflecting that both the quality of available retail and residents' ability to afford it are
  fundamental constraints on food access.
</div>
</div>

---

## Variable Definitions

**snap_grocery_per_10k** — SNAP-authorized grocery stores, supermarkets, and super stores per 10,000 residents. This is the single highest-weighted variable because it directly measures physical food access for the residents most at risk of food insecurity. Ethnic grocery stores reclassified from the USDA Convenience Store category are included here.

**nonsnap_grocery_per_10k** — All licensed food retailers not in the SNAP database, weighted by store size. Supermarkets receive full weight (x1.0), mid-size grocers receive partial weight (x0.6), and small stores receive minimal weight (x0.3). Lower weight than SNAP stores reflects that non-SNAP retailers are not universally accessible to all residents.

**membership_per_10k** — Warehouse and membership stores (Costco, BJ's) per 10,000 residents. Given the lowest weight among grocery terms because the membership fee represents a meaningful barrier for lower-income households.

**convenience_ratio** — The percentage of SNAP-authorized retailers classified as convenience stores. This is one of the two strongest penalty terms in the formula at −8.0. A neighborhood where 70% of its SNAP retail options are bodegas and corner stores has a fundamentally different food access reality than one where 70% are grocery stores.

**food_balance_ratio** — SNAP grocery stores per 10k divided by fast food restaurants per 10k plus one. Values above 1.0 indicate more grocery options than fast food. This term captures balance rather than penalizing fast food outright — the food balance ratio already encodes fast food's effect on the food environment.

**year_round_markets** — Count of farmers markets operating year-round in the neighborhood. Seasonal markets are excluded because intermittent availability does not constitute reliable food access infrastructure.

**farmers_markets_per_10k** — Total farmers market locations per 10,000 residents, normalizing for neighborhood population size.

**gardens_per_10k** — Active GreenThumb community gardens per 10,000 residents. Weighted at 0.25 because not all registered gardens produce food accessible to the community.

**pct_grade_A** — Percentage of inspected food establishments with a New York City Department of Health Grade A rating. This variable is specific to NYC's letter-grade inspection system and is not portable to analyses of other cities.

**economic_access_gap** — An income-based affordability penalty ranging from 0 to 2, weighted at −3.0 — the strongest penalty term in the formula alongside convenience ratio. Neighborhoods with median household income above $100,000 receive no penalty. Neighborhoods below $35,000 receive the maximum penalty of 2.0. This reflects the reality that physical proximity to food does not constitute access if residents cannot afford to shop there.

---

## Tier Classification

Access tiers are determined by back-calculating thresholds from defined archetypes.

| Tier | Score | Neighborhoods | Description |
|---|---|---|---|
| Food Desert | Below 25.2 | 13 | Critically low grocery access |
| At Risk | 25.2 to 54.7 | 46 | Marginal access with measurable gaps |
| Good Access | 54.7 and above | 57 | Adequate grocery density and quality |

Thresholds are placed at the midpoints between archetype scores rather than set arbitrarily or percentile-based. A neighborhood's classification reflects its actual food access conditions, not its position in the citywide distribution.

---

## Ethnic Grocery Reclassification

The USDA SNAP retailer database frequently classifies halal markets, carnecerias, African grocery stores, fish markets, and produce stands as Convenience Stores despite selling primarily fresh and unprocessed food. This analysis applies a name-based reclassification: any store whose name contains terms associated with ethnic food retail is reclassified to the healthy category regardless of its USDA designation.

**457 stores were reclassified**, representing 5.4% of the total NYC SNAP retailer database. The healthy retailer ratio increased from 45.8% to 46.6% citywide following this adjustment. This reclassification directly addresses the systematic undercounting of culturally specific food infrastructure in standard federal classifications.

---

## Known Limitations

- **SNAP data excludes high-end grocers** — Whole Foods, Trader Joe's, and specialty food stores are not SNAP-authorized, causing some high-income neighborhoods to score lower than intuition suggests.
- **Farmers market seasonal duplicates** — The DOHMH dataset lists some markets multiple times across seasons. The year_round_markets count is more reliable than total_farmers_markets.
- **Grade A inspections are NYC-specific** — The DOHMH letter grade system does not exist in most US cities. This variable contributes minimally to the score (weight 0.2) and would need to be replaced for any adaptation to another geography.
- **Fast food identification relies on cuisine description** — Restaurant inspection records identify fast food by cuisine type, which may undercount some chains listed generically.
- **Community garden variability** — Not all GreenThumb-registered gardens produce food accessible to the broader community. The 0.25 weight reflects this uncertainty.
