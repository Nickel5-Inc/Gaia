# Weather Regional Scoring System Research
## Köppen Climate Classification-Based Reward Distribution

### Executive Summary
This document explores transitioning from our current global variable-based scoring system to a regional scoring system using Köppen climate classifications. The proposed system would award all rewards for a specific climate region to the top-performing miner in that region, potentially creating more specialized and accurate regional forecasting.

---

## Current Scoring System Analysis

### Current Methodology
- **Scope**: Global scoring across all grid points
- **Metrics**: RMSE, Bias, ACC (Anomaly Correlation Coefficient), Skill Score per variable/pressure level
- **Variables**: Temperature (2m, pressure levels), precipitation, humidity, wind components, etc.
- **Aggregation**: Weighted global averages with latitude weighting
- **Timeline**: Day-1 QC scoring + progressive ERA5 final scoring (Days 1-10)

### Current Strengths
1. **Comprehensive Coverage**: Evaluates performance across all global regions equally
2. **Variable Diversity**: Multiple meteorological variables and pressure levels
3. **Statistical Rigor**: Well-established meteorological metrics (ACC, skill scores)
4. **Computational Efficiency**: Vectorized operations across global grids
5. **Progressive Scoring**: Immediate feedback + longer-term validation

### Current Limitations
1. **Global Averaging**: Excellent tropical performance can mask poor polar performance
2. **Uniform Weighting**: All regions treated equally despite varying forecasting difficulty
3. **No Regional Specialization**: No incentive for miners to excel in specific climate zones
4. **Climate Bias**: Easier-to-predict regions may dominate scores

---

## Köppen Climate Classification Integration

### Köppen System Overview
The Köppen climate classification uses a three-letter code:
- **First letter**: Main climate group (A=tropical, B=dry, C=temperate, D=continental, E=polar)
- **Second letter**: Precipitation pattern (f=fully humid, m=monsoonal, s=summer dry, w=winter dry)
- **Third letter**: Temperature characteristics (a=hot summer, b=warm summer, c=cool summer, d=very cold winter, h=hot, k=cold)

### Proposed Focus: First Two Letters (Seasonal Precipitation Subgroups)
This would create ~26 distinct climate regions:

#### Tropical (A)
- **Af**: Tropical rainforest (Amazon, Congo Basin, SE Asia)
- **Am**: Tropical monsoon (parts of India, SE Asia, West Africa)
- **Aw**: Tropical savanna (parts of Africa, South America, Australia)

#### Dry (B)
- **BWh**: Hot desert (Sahara, Arabian Peninsula, SW USA)
- **BWk**: Cold desert (Central Asia, high-altitude deserts)
- **BSh**: Hot semi-arid (parts of Africa, Australia, Mexico)
- **BSk**: Cold semi-arid (Great Plains, parts of Argentina)

#### Temperate (C)
- **Cfa**: Humid subtropical (SE USA, parts of China, Argentina)
- **Cfb**: Oceanic (Western Europe, parts of Chile, NZ)
- **Cfc**: Subpolar oceanic (coastal Alaska, southern Chile)
- **Csa**: Mediterranean hot summer (Southern California, parts of Mediterranean)
- **Csb**: Mediterranean warm summer (Northern California, parts of Mediterranean)
- **Csc**: Mediterranean cool summer (coastal regions)
- **Cwa**: Humid subtropical (parts of China, northern India)
- **Cwb**: Subtropical highland (parts of Mexico, East Africa)
- **Cwc**: Cold subtropical highland (high altitude regions)

#### Continental (D)
- **Dfa**: Hot summer humid continental (Midwest USA, parts of Eastern Europe)
- **Dfb**: Warm summer humid continental (Great Lakes, Scandinavia)
- **Dfc**: Subarctic (Canada, Siberia, northern Scandinavia)
- **Dfd**: Extremely cold subarctic (eastern Siberia)
- **Dsa/Dsb/Dsc/Dsd**: Mediterranean-influenced continental variants
- **Dwa/Dwb/Dwc/Dwd**: Monsoon-influenced continental variants

#### Polar (E)
- **ET**: Tundra (northern Canada, northern Russia, Antarctica edges)
- **EF**: Ice cap (Greenland, Antarctica)

---

## Regional Scoring Methodology

### Proposed System Architecture

#### 1. Grid Point Classification
```
For each global grid point (lat, lon):
  1. Determine Köppen classification (Af, Am, Aw, BWh, etc.)
  2. Group by first two letters only
  3. Assign to regional scoring bucket
```

#### 2. Regional Performance Calculation
```
For each climate region R and miner M:
  1. Extract all grid points belonging to region R
  2. Calculate regional metrics for miner M:
     - RMSE_R = sqrt(mean((forecast_R - truth_R)²))
     - ACC_R = correlation(forecast_anomaly_R, truth_anomaly_R)
     - Skill_R = 1 - RMSE_R/RMSE_climatology_R
  3. Compute composite regional score: S_R_M
```

#### 3. Winner-Takes-All Regional Rewards
```
For each climate region R:
  1. Rank all miners by regional score S_R_M
  2. Award full regional weight W_R to top miner
  3. All other miners receive 0 weight for region R
```

#### 4. Global Weight Aggregation
```
For each miner M:
  Final_Weight_M = Σ(W_R) for all regions R where miner M ranked #1
```

### Regional Weight Distribution Options

#### Option A: Area-Weighted
- **Weight ∝ Geographic area** of climate region
- **Pros**: Reflects spatial coverage importance
- **Cons**: May over-represent large but meteorologically simple regions (e.g., polar)

#### Option B: Forecasting Difficulty-Weighted  
- **Weight ∝ Historical forecasting difficulty** (inverse of typical skill scores)
- **Pros**: Rewards excellence in challenging regions
- **Cons**: Requires baseline difficulty assessment

#### Option C: Economic Impact-Weighted
- **Weight ∝ Population density** or economic activity in region
- **Pros**: Reflects real-world importance of accurate forecasts
- **Cons**: May neglect important but sparsely populated climate zones

#### Option D: Uniform Weighting
- **Weight = 1/N** for N climate regions
- **Pros**: Simple, treats all climate types equally
- **Cons**: Doesn't reflect varying importance or difficulty

---

## Computational Efficiency Considerations

### Current System Performance
- **Global Metrics**: O(N) where N = total grid points (~1M for global 0.25° grid)
- **Variable Processing**: Vectorized xarray operations
- **Memory**: Moderate (full global grids in memory)

### Regional System Challenges

#### Challenge 1: Grid Point Classification
- **Need**: Fast Köppen classification for each grid point
- **Solution**: Pre-computed Köppen classification grid (static file)
- **Cost**: One-time setup, negligible runtime overhead

#### Challenge 2: Regional Metric Calculation  
- **Current**: Single global calculation
- **Regional**: 26 separate regional calculations
- **Complexity**: O(26 × N/26) = O(N) - same order, but higher constant

#### Challenge 3: Memory Overhead
- **Additional Storage**: Köppen classification grid, regional masks
- **Memory Impact**: ~26 boolean masks for grid point selection

### Proposed Optimization Strategies

#### 1. Pre-computed Regional Masks
```python
# One-time setup
regional_masks = {
    'Af': boolean_grid_for_tropical_rainforest,
    'Am': boolean_grid_for_tropical_monsoon,
    # ... for all 26 regions
}

# Runtime usage
for region, mask in regional_masks.items():
    regional_forecast = global_forecast.where(mask)
    regional_truth = global_truth.where(mask)
    regional_score = compute_metrics(regional_forecast, regional_truth)
```

#### 2. Parallel Regional Processing
```python
# Process all regions concurrently
regional_tasks = [
    compute_regional_score(region, mask, forecast, truth)
    for region, mask in regional_masks.items()
]
regional_scores = await asyncio.gather(*regional_tasks)
```

#### 3. Pre-computed Regional Climatologies
```python
# Pre-compute regional climatologies for skill score baselines
regional_climatologies = {
    'Af': precomputed_tropical_rainforest_climatology,
    'Am': precomputed_tropical_monsoon_climatology,
    # ... for all regions
}
```

#### 4. Sparse Regional Processing
```python
# Only process regions where miners have submitted forecasts
active_regions = {region for region in regions if has_forecast_data(region)}
# Skip empty regions to save computation
```

---

## Implementation Pathway

### Phase 1: Köppen Classification Setup
1. **Obtain Köppen Classification Data**
   - Source: World Bank Climate Change Knowledge Portal or similar
   - Format: NetCDF/GeoTIFF at 0.25° resolution to match forecast grids
   - Validation: Cross-reference with multiple sources

2. **Create Regional Mask System**
   - Generate boolean masks for each of 26 regions
   - Optimize mask storage (compressed, memory-mapped)
   - Validate geographic coverage and overlap

3. **Regional Climatology Computation**
   - Use ERA5 historical data (1979-2020) to compute regional climatologies
   - Separate climatologies for each region and variable
   - Store as pre-computed reference for skill scores

### Phase 2: Parallel Scoring System
1. **Develop Regional Metric Calculator**
   - Extend existing scoring functions to work on regional subsets
   - Maintain compatibility with current metrics (RMSE, ACC, skill score)
   - Add regional aggregation logic

2. **Winner-Takes-All Logic**
   - Implement regional ranking system
   - Design weight distribution mechanism
   - Add regional performance tracking

### Phase 3: Integration & Testing
1. **Hybrid Testing Period**
   - Run both systems in parallel
   - Compare global vs regional rankings
   - Analyze impact on miner behavior

2. **Gradual Transition**
   - Phase in regional scoring over multiple epochs
   - Monitor for unintended consequences
   - Adjust regional weights based on performance

---

## Potential Benefits

### 1. Climate Specialization Incentive
- **Miners may specialize** in specific climate regions where they excel
- **Regional expertise development** rather than global mediocrity
- **Improved accuracy** in challenging climate zones

### 2. Geographic Diversity
- **Ensures coverage** of all climate types
- **Prevents neglect** of difficult-to-predict regions
- **Better global forecast ensemble** through regional specialists

### 3. Real-World Relevance
- **Climate-specific accuracy** is more meaningful than global averages
- **Aligned with actual forecasting challenges** (tropical vs polar prediction)
- **Potential for region-specific model development**

### 4. Reduced Score Manipulation
- **Harder to game** 26 separate regional competitions
- **Regional specialization** requires genuine expertise
- **Winner-takes-all** reduces incentive to barely beat mediocre global performance

---

## Potential Challenges & Risks

### 1. Reduced Competition
- **Only 26 winners** instead of gradual ranking
- **May discourage** miners who can't achieve regional dominance
- **Risk of regional monopolies** if one miner consistently dominates a region

### 2. Regional Bias
- **Some regions easier** to predict than others (stable tropical vs volatile continental)
- **Uneven reward distribution** if region difficulties vary significantly
- **May incentivize targeting** only the easiest regions

### 3. Data Sparsity
- **Some regions have fewer** verification grid points (polar, mountain regions)
- **Statistical significance** may be lower for smaller regions
- **Increased noise** in regional metrics

### 4. Implementation Complexity
- **26x increase** in scoring calculations
- **Regional mask management** and validation overhead
- **Debugging and monitoring** becomes more complex

### 5. Transition Period Challenges
- **Miner adaptation** to new scoring system
- **Potential gaming** as miners learn regional boundaries
- **Validation** of regional classification accuracy

---

## Alternative Approaches

### 1. Hybrid Regional-Global System
- **70% regional scoring** + 30% global scoring
- **Maintains global competition** while incentivizing regional excellence
- **Easier transition** from current system

### 2. Regional Bonus System
- **Base global scoring** + regional excellence bonuses
- **Top 3 miners per region** get bonus weights
- **Preserves current system** while adding regional incentives

### 3. Adaptive Regional Weighting
- **Difficulty-adjusted** regional weights based on historical performance
- **Weight increases** for regions with consistently poor forecasts
- **Automatic balancing** of regional competition

### 4. Temporal Regional Rotation
- **Different regions featured** each epoch/month
- **Prevents regional monopolies** through rotation
- **Maintains global coverage** over time

---

## Research Questions & Next Steps

### 1. Köppen Classification Validation
- **How accurate** are current Köppen classifications for our forecast grids?
- **Should we use** updated climate classifications based on recent data?
- **How stable** are regional boundaries over forecast timescales?

### 2. Regional Difficulty Assessment  
- **Which regions** are inherently harder to predict?
- **How much variation** exists in regional forecast skill?
- **What's the optimal** regional weight distribution?

### 3. Computational Feasibility
- **What's the actual performance** impact of 26x regional calculations?
- **Can we optimize** regional processing to match current performance?
- **How does memory usage** scale with regional processing?

### 4. Miner Behavior Modeling
- **How would miners adapt** to regional competition?
- **Would regional specialization** actually improve forecast quality?
- **What's the risk** of regional market manipulation?

### 5. System Design Decisions
- **Should regions be weighted** equally or by difficulty/importance?
- **How many miners** should receive rewards per region?
- **Should there be** fallback global scoring for uncontested regions?

---

## Conclusion

The Köppen climate classification-based regional scoring system represents a fundamental shift from global averaging to regional specialization. While it offers compelling benefits including climate-specific expertise development and improved coverage of challenging regions, it also introduces significant implementation complexity and potential risks.

**Key Success Factors:**
1. **Accurate regional classification** and mask generation
2. **Computationally efficient** regional processing
3. **Balanced regional weighting** to prevent gaming
4. **Careful transition planning** to maintain network stability

**Recommended Approach:**
Start with a hybrid system that maintains global scoring while adding regional bonuses, then gradually transition to full regional scoring based on observed miner behavior and system performance.

The regional approach could significantly improve forecast quality in specialized climate zones, but requires careful design and implementation to realize these benefits while avoiding potential pitfalls. 