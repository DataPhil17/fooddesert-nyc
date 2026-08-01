# Findings

## Research Questions

This analysis was structured around four research questions, each examined using the full dataset of 116 NYC neighborhoods.

---

## Q1: How do NYC neighborhoods rank on food access?

Neighborhoods are ranked by Food Access Score within each borough. The highest-scoring neighborhoods — Sunset Park, Longwood, Hunts Point, Corona, and Jackson Heights — share a common profile: high population density, immigrant-heavy commercial corridors, and a large number of SNAP-authorized ethnic grocery stores per capita.

The lowest-scoring neighborhoods — Breezy Point, Eltingville, Floral Park, Bayside, and Throggs Neck — are predominantly lower-density, outer-borough communities with sparse grocery infrastructure and high rates of car dependence.

![Food Access Score Rankings](./q1_food_access_score.png)

---

## Q2: Does poverty predict food desert status?

One of the most significant findings of this analysis is that poverty rate is a poor predictor of food access in New York City. Neighborhoods with the highest poverty rates — South Bronx, Longwood, Hunts Point — score well on food access because they have dense walkable grocery infrastructure. Neighborhoods with the lowest poverty rates — Bayside, Eltingville, Murray Hill — score poorly because they lack SNAP-authorized grocery density.

Murray Hill illustrates this paradox most clearly. With a median household income of $153,000, it scores as a Food Desert due to a near-absence of SNAP-authorized grocery stores. Residents shop at non-SNAP specialty and gourmet retailers not captured by SNAP data — exposing a fundamental limitation of SNAP-based metrics in high-income urban neighborhoods.

![Score vs. Socioeconomic Indicators](./q2_score_vs_socioeconomic.png)

---

## Q3: What variables drive food desert classification?

Examining average metric values across the three access tiers reveals several important patterns. Good Access neighborhoods have significantly higher SNAP grocery density per capita. The poverty rate finding is counterintuitive but methodologically sound: dense immigrant neighborhoods have both high poverty rates and dense grocery infrastructure.

The economic access gap chart shows that Food Desert neighborhoods have lower affordability penalties on average — because most Food Desert neighborhoods are higher-income outer-borough communities where income is not the limiting factor. Access to food is constrained by physical infrastructure, not purchasing power.

![Score Component Analysis](./q3_score_components.png)

---

## Q4: How does food access vary across boroughs?

Borough-level analysis uses population-weighted averages to account for the fact that boroughs contain very different numbers of neighborhoods — Queens has 40 neighborhoods in this analysis while the Bronx has 19.

The Bronx scores highest on SNAP grocery density, driven by the concentration of ethnic grocery stores in neighborhoods like Fordham, Norwood, Highbridge, and University Heights. Queens shows the highest internal variation of any borough, spanning from some of the highest-scoring neighborhoods (Jackson Heights, Corona, Elmhurst) to five of the seven Food Desert neighborhoods.

![Borough Comparison](./q4_borough_comparison.png)

---

## Summary

| Finding | Implication |
|---|---|
| Poverty rate does not predict food access | Income-based USDA criteria miss the most vulnerable urban neighborhoods |
| Dense immigrant neighborhoods score highest | Ethnic grocery infrastructure is systematically undercounted by standard classifications |
| High-income neighborhoods can score as Food Deserts | SNAP-only analysis misses boutique and specialty food retail |
| Queens has the most internal variation | Borough-level policy is too coarse — neighborhood-level targeting is necessary |
| 536 additional stores needed citywide | Full remediation to Good Access requires substantial sustained investment |
