# Findings

## Research Questions

This analysis was structured around four research questions, each examined using the full dataset of 116 NYC neighborhoods.

---

## Q1: How do NYC neighborhoods rank on food access?

Neighborhoods are ranked by Food Access Score within each borough. The highest-scoring neighborhoods — Sunset Park, Longwood, Hunts Point, Corona, and Jackson Heights — share a common profile: high population density, immigrant-heavy commercial corridors, and a large number of SNAP-authorized ethnic grocery stores per capita.

The lowest-scoring neighborhoods — Breezy Point, Eltingville, Floral Park, Bayside, and Throggs Neck — are predominantly lower-density, outer-borough communities with sparse grocery infrastructure and high rates of car dependence.

Of 116 scored neighborhoods, 13 qualify as Food Deserts, 46 are At Risk, and 57 have Good Access.

![Food Access Score Rankings](./q1_food_access_score.png)

---

## Q2: Does poverty predict food desert status?

One of the most significant findings of this analysis is that poverty rate is a poor predictor of food access in New York City. Neighborhoods with the highest poverty rates — South Bronx, Longwood, Hunts Point — score well on food access because they have dense walkable grocery infrastructure. Neighborhoods with the lowest poverty rates — Bayside, Eltingville, Murray Hill — score poorly because they lack SNAP-authorized grocery density.

Murray Hill illustrates this most clearly. With a median household income of $153,000, it scores as a Food Desert because this analysis measures food acAP benefits — residents with higher incomes shop at non-SNAP specialty and gourmet retailers, but SNAP-eligible residents in the same neighborhood have critically limited authorized options. This is not a flaw in the methodology; it is the methodology working correctly.

![Score vs. Socioeconomic Indicators](./q2_score_vs_socioeconomic.png)

---

## Q3: What variables drive food desert classification?

Examining average metric values across the three access tiers reveals several important patterns.

Good Access neighborhoods have significantly higher SNAP grocery density per capita — 5.85 stores per 10,000 residents on average compared to 1.09 for Food Desert neighborhoods. The convenience store percentage is counterintuitively highest in Good Access neighborhoods, but this reflects that dense neighborhoods have more total SNAP retailers overall — the absolute grocery count still dominates.

The poverty rate finding is striking: Food Desert neighborhoods have higher average median incomes than Good Access neighborhoods. This is because most Food Desert neighborhoods in our analysis are suburban, car-dependent outer-borough communities with lower poverty but sparse grocery infrastructure. Dense immigrant neighborhoods have both high poverty rates and dense grocery infrastructure — the opposite of what the USDA's income-based definition assumes.

The economic access gap shows that Food Desert neighborhoods have lower affordability barriers on average because they tend to be higher-income — but their physical grocery access is poor. Good Access neighborhoods face higher affordability pressure but have abundant physical options nearby.

![Score Component Analysis](./q3_score_components.png)

---

## Q4: How does food access vary across boroughs?

Borough-level analysis uses population-weighted averages to account for the fact that boroughs contain very different numbers of neighborhoods — Queens has 40 neighborhoods in this analysis while the Bronx has 19.

The Bronx scores highest on SNAP grocery density, driven by the concentration of ethnic grocery stores in neighborhoods like Fordham, Norwood, Highbridge, and University Heights. The ethnic grocery reclassification had a particularly strong effect in the Bronx, where many stores in dense immigrant commercial corridors were moved from the Convenience Store category to healthy retail.

Queens shows the highest internal variation of any borough, containing both some of the highest-scoring neighborhoods (Jackson Heights, Corona, Elmhurst) and the most Food Desert neighborhoods of any borough. This reflects the geographic diversity of Queens — dense immigrant neighborhoods alongside sprawling low-density suburban communities.

![Borough Comparison](./q4_borough_comparison.png)

---

## Summary

| Finding | Implication |
|---|---|
| Poverty rate does not predict food access | Income-based USDA criteria miss the most vulnerable urban neighborhoods |
| Dense immigrant neighborhoods score highest | Ethnic grocery infrastructure is systematically undercounted by standard classifications |
| High-income neighborhoods can score as Food Deserts | SNAP-only analysis misses boutique and specialty food retail |
| Queens has the most internal variation | Borough-level policy is too coarse — neighborhood-level targeting is necessary |
| 59 neighborhoods need intervention | Full remediation to Good Access requires sustained investment across 13 Food Deserts and 46 At Risk neighborhoods |
