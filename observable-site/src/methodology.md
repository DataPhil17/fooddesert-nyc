# Methodology

## Overview

The Food Access Score is a composite metric designed to measure the quality, density, and affordability of the food retail environment in each NYC neighborhood. It is computed at the ZIP code level and aggregated to neighborhoods using a crosswalk of 173 ZIP codes mapped to 116 neighborhoods across all five boroughs.

---

## The Food Access Score

Each neighborhood receives a score based on ten weighted variables. Higher scores indicate better food access.

```
Food Access Score =
    (snap_grocery_per_10k          x 8.0)   -- SNAP grocery store density
  + (nonsnap_grocery_per_10k       x 5.0)   -- Non-SNAP grocery access
  + (membership_per_10k            x 3.0)   -- Warehouse stores (partial credit)
  - (convenience_ratio / 100       x 8.0)   -- Convenience store dominance (penalty)
  + (food_balance_ratio            x 4.0)   -- Grocery vs. fast food balance
  + (year_round_markets            x 1.0)   -- Reliable fresh produce access
  + (farmers_markets_per_10k       x 0.5)   -- Normalized market access
  + (gardens_per_10k               x 0.25)  -- Community garden access
  + (pct_grade_A / 100             x 0.2)   -- Food quality signal
  - (economic_access_gap           x 1.0)   -- Affordability penalty
```

---

## Variable Definitions

**snap_grocery_per_10k** — SNAP-authorized grocery stores, supermarkets, and super stores per 10,000 residents. This is the single highest-weighted variable because it directly measures physical food access for the residents most at risk of food insecurity. Ethnic grocery stores reclassified from the USDA Convenience Store category are included here.

**nonsnap_grocery_per_10k** — All licensed food retailers not in the SNAP database, weighted by store size. Supermarkets receive full weight (x1.0), mid-size grocers receive partial weight (x0.6), and small stores receive minimal weight (x0.3). Lower weight than SNAP stores reflects that non-SNAP retailers are not universally accessible to all residents.

**membership_per_10k** — Warehouse and membership stores (Costco, BJ's) per 10,000 residents. Given the lowest weight among grocery terms because the membership fee represents a meaningful barrier for lower-income households.

**convenience_ratio** — The percentage of SNAP-authorized retailers classified as convenience stores. This is the strongest penalty term in the formula. A neighborhood where 70% of its SNAP retail options are bodegas and corner stores has a fundamentally different food access reality than one where 70% are grocery stores.

**food_balance_ratio** — SNAP grocery stores per 10k divided by fast food restaurants per 10k plus one. Values above 1.0 indicate more grocery options than fast food. This term captures balance rather than penalizing fast food outright.

**year_round_markets** — Count of farmers markets operating year-round in the neighborhood. Seasonal markets are excluded because intermittent availability does not constitute reliable food access infrastructure.

**farmers_markets_per_10k** — Total farmers market locations per 10,000 residents, normalizing for neighborhood population size.

**gardens_per_10k** — Active GreenThumb community gardens per 10,000 residents. Weighted at 0.25 because not all registered gardens produce food accessible to the community.

**pct_grade_A** — Percentage of inspected food establishments with a New York City Department of Health Grade A rating. This variable is specific to NYC's letter-grade inspection system and is not portable to analyses of other cities.

**economic_access_gap** — An income-based affordability penalty ranging from 0 to 2. Neighborhoods with median household income above $100,000 receive no penalty. Neighborhoods below $35,000 receive the maximum penalty of 2.0.

---

## Tier Classification

Access tiers are determined by back-calculating thresholds from defined archetypes.

| Tier | Threshold | Neighborhoods |
|---|---|---|
| Food Desert | Score below 25.2 | 7 |
| At Risk | Score 25.2 to 54.7 | 51 |
| Good Access | Score 54.7 and above | 58 |

---

## Ethnic Grocery Reclassification

The USDA SNAP retailer database frequently classifies halal markets, carnecerias, African grocery stores, fish markets, and produce stands as Convenience Stores despite selling primarily fresh and unprocessed food. This analysis applies a name-based reclassification, moving 457 stores to the healthy category. The healthy retailer ratio increased from 45.8% to 46.6% citywide following this adjustment.

---

## Known Limitations

- SNAP data excludes high-end grocery retailers, causing some high-income neighborhoods to score lower than intuition suggests
- The DOHMH farmers market dataset has seasonal duplicates — year_round_markets is more reliable than total count
- Grade A inspection scores are NYC-specific and not portable to other geographies
- Fast food identification relies on cuisine description from inspection records and may undercount some chains
