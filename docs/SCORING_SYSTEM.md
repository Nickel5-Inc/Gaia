# Gaia Validator Scoring & Weight System

## Overview

The Gaia validator uses a **hybrid scoring system** that allows miners to succeed through two pathways:
1. **Excellence Path**: Achieve top-tier performance in a single task
2. **Diversity Path**: Demonstrate competent performance across multiple tasks

This system prevents gaming while preserving choice for miners who want to specialize or generalize.

## Task Overview & Time Windows

### Geomagnetic Task
- **Scoring Frequency**: Hourly (24 scores per day)
- **Data Availability**: Immediate after prediction submission
- **Scoring Window**: 24 hours of recent predictions
- **Expected Predictions During Immunity**: ~72 predictions (3 days × 24 hours)

### Soil Moisture Task  
- **Scoring Frequency**: Variable (based on data processing cycles)
- **Data Availability**: 3-day processing delay after submission
- **Scoring Window**: 7 days to account for processing delays
- **Expected Predictions During Immunity**: 0-1 scores (due to processing delay)

### Weather Task
- **Scoring Frequency**: Daily predictions with dual timeline
  - **Day 1**: GFS similarity score (immediate)
  - **Day 10**: Final predictive accuracy score
- **Data Availability**: Day 1 scores immediate, Day 10 scores after forecast period
- **Scoring Window**: 14 days to capture full prediction lifecycle
- **Expected Predictions During Immunity**: 3 day-1 scores, 0 day-10 scores

## Base Task Weight Allocations

- **Weather**: 70% (0.70) - Primary task due to complexity and importance
- **Geomagnetic**: 15% (0.15) - Secondary task, frequent scoring
- **Soil Moisture**: 15% (0.15) - Secondary task, longer processing time

## Excellence Pathway

The excellence pathway allows miners to achieve competitive weights through **exceptional single-task performance**. This path rewards specialization and domain expertise while preventing low-effort gaming through high performance barriers.

### Requirements & Thresholds

#### Top 15% Performance Requirement
- Miner must rank in the **85th percentile or higher** within their chosen task
- Percentile calculated against all miners with valid scores in that task during the evaluation window
- Dynamic threshold: Performance requirement scales with actual competition level

#### Minimum Sample Size
- **10 miners minimum** required for reliable percentile calculation
- If fewer than 10 miners participate, fallback to absolute score thresholds
- Prevents gaming through artificial scarcity of competition

#### Baseline Performance Gate
- Must exceed task-specific baseline performance (existing anti-gaming measure)
- Excellence pathway builds on top of existing baseline requirements
- No excellence weights awarded for below-baseline performance regardless of percentile

### Technical Implementation

#### Percentile Calculation Method
```python
def calculate_excellence_eligibility(miner_score, all_task_scores, task_name):
    # Filter valid scores (above baseline, non-zero, non-NaN)
    valid_scores = [score for score in all_task_scores 
                   if score > baseline_thresholds[task_name] and score > 0]
    
    if len(valid_scores) < 10:
        # Fallback to absolute threshold if insufficient sample
        return miner_score > excellence_fallback_thresholds[task_name]
    
    # Calculate 85th percentile threshold
    percentile_85 = np.percentile(valid_scores, 85)
    
    return miner_score >= percentile_85
```

#### Weight Calculation Formula
```python
def calculate_excellence_weight(miner_score, task_name, completeness_factor):
    if not is_excellence_eligible(miner_score, task_name):
        return 0.0  # No excellence weight if not in top 15%
    
    base_weight = task_base_weights[task_name]  # 0.70, 0.15, or 0.15
    excellence_weight = miner_score * base_weight * completeness_factor
    
    return excellence_weight
```

### Task-Specific Excellence Analysis

#### Geomagnetic Excellence
- **Advantages**: Hourly scoring provides many opportunities to demonstrate consistency
- **Challenges**: Typically flat score distribution makes top 15% competitive
- **Strategy**: Focus on model optimization and consistent participation
- **Typical threshold**: ~0.90-0.95 score range for top 15%

#### Weather Excellence  
- **Advantages**: High base weight (70%) makes excellence very valuable
- **Challenges**: Longer feedback cycles and dual scoring timeline complexity
- **Strategy**: Optimize for both day-1 similarity and day-10 accuracy
- **Typical threshold**: Variable based on forecast difficulty periods

#### Soil Moisture Excellence
- **Advantages**: Lower competition due to processing delays and complexity
- **Challenges**: Fewer scoring opportunities, longer development cycles
- **Strategy**: Focus on model sophistication and regional optimization
- **Typical threshold**: ~0.70-0.80 score range depending on data conditions

### Competitive Dynamics

#### Excellence Threshold Fluctuation
- Thresholds vary based on **actual miner performance distribution**
- High-skill periods → higher thresholds required
- Low-skill periods → lower thresholds sufficient
- **Real-time adaptation** prevents gaming through strategic timing

#### Excellence Sustainability  
- Must **maintain top 15% performance consistently**
- Single excellent score insufficient - requires sustained performance
- Performance evaluated over rolling windows (24h geo, 7d soil, 14d weather)

### Strategic Considerations for Excellence Miners

#### Resource Allocation
- Can focus 100% effort on single task optimization
- No requirement to participate in other tasks
- Maximum ROI on specialized knowledge and infrastructure

#### Risk Factors
- **Competition scaling**: More skilled miners → higher excellence threshold
- **Single point of failure**: Poor performance in chosen task → zero excellence weight
- **Market timing**: Must adapt to changing performance landscapes

#### Excellence vs Diversity Tradeoffs
```python
# Example comparison: Geo specialist decision
geo_excellence_weight = 0.92 * 0.15 * 1.0 = 0.138  # If achieve 85th percentile
geo_diversity_weight = 0.89 * 0.15 * 1.05 * 0.85 = 0.120  # If broader participation

# Excellence provides higher ceiling but requires top-tier performance
```

## Diversity Pathway

The diversity pathway rewards miners who demonstrate **competent performance across multiple tasks**. This path provides more stable weight potential and reduces single-task risk while requiring broader resource allocation and knowledge.

### Requirements & Philosophy

#### Multi-Task Participation
- Miners participate in **any combination** of 2-3 tasks
- No requirement for excellence in any single task
- Rewards **consistency and breadth** over specialization depth

#### Performance Evaluation Method
- Each task scored independently using **sigmoid curve** based on percentile ranking
- Sigmoid provides **smooth scaling** rather than harsh threshold effects
- **Above-median performance rewarded**, below-median gently penalized

#### Diversification Incentive
- **Multi-task bonus** scales with number of tasks attempted
- Encourages broader participation while maintaining quality standards

### Sigmoid Multiplier System

#### Mathematical Foundation
The sigmoid curve creates smooth performance scaling that rewards competence without harsh penalties:

```python
def calculate_diversity_multiplier(percentile_rank):
    """
    Sigmoid curve for diversity path performance scaling
    - Smooth transitions prevent cliff effects
    - Rewards above-median performance  
    - Gentle penalties for below-median performance
    """
    min_multiplier = 0.3   # 30% weight for very poor performance
    max_multiplier = 1.2   # 120% bonus for excellent performance  
    center_point = 35      # Inflection at 35th percentile
    steepness = 0.08       # Moderate curve steepness
    
    sigmoid_value = 1 / (1 + math.exp(-steepness * (percentile_rank - center_point)))
    multiplier = min_multiplier + (max_multiplier - min_multiplier) * sigmoid_value
    
    return multiplier
```

#### Detailed Sigmoid Performance Table

| Percentile | Multiplier | Weight Effect | Strategic Interpretation |
|------------|------------|---------------|-------------------------|
| 5th | 0.31 | 31% weight | Severely poor performance |
| 10th | 0.35 | 35% weight | Significant penalty - needs improvement |
| 20th | 0.48 | 48% weight | Below average - moderate penalty |
| 25th | 0.55 | 55% weight | Bottom quartile - gentle penalty |
| 30th | 0.64 | 64% weight | Approaching competence |
| 35th | 0.75 | 75% weight | **Inflection point** - decent performance |
| 40th | 0.84 | 84% weight | Above inflection - good performance |
| 50th | 0.95 | 95% weight | Median performance - nearly full weight |
| 60th | 1.03 | 103% weight | Above average - small bonus |
| 70th | 1.10 | 110% weight | Good performance - meaningful bonus |
| 80th | 1.15 | 115% weight | Very good performance - strong bonus |
| 90th | 1.18 | 118% weight | Excellent performance - maximum bonus |

### Multi-Task Bonus Structure

#### Bonus Rationale
- **Single task (1)**: 0.85× multiplier - encourages diversification
- **Two tasks (2)**: 1.00× multiplier - baseline competent diversification
- **Three tasks (3)**: 1.15× multiplier - rewards full platform engagement

#### Strategic Implications
```python
# Trade-off analysis for miners
single_task_ceiling = 1.0 * 0.85 = 0.85  # Max possible with 1 task
two_task_ceiling = 2.0 * 1.00 = 2.00     # Max possible with 2 tasks  
three_task_ceiling = 3.0 * 1.15 = 3.45   # Max possible with 3 tasks

# But achieving maximum requires excellent performance across all tasks
```

### Technical Implementation

#### Task Contribution Calculation
```python
def calculate_diversity_weight(miner_scores, all_miner_scores):
    task_contributions = {}
    
    for task_name in ['weather', 'geomagnetic', 'soil']:
        if task_name in miner_scores and not np.isnan(miner_scores[task_name]):
            # Calculate percentile rank for this miner in this task
            task_scores = get_valid_task_scores(all_miner_scores, task_name)
            
            if len(task_scores) >= 5:  # Minimum sample size
                percentile_rank = calculate_percentile_rank(
                    miner_scores[task_name], task_scores
                )
                sigmoid_multiplier = calculate_diversity_multiplier(percentile_rank)
                completeness_factor = get_completeness_factor(miner_scores, task_name)
                
                task_contributions[task_name] = (
                    miner_scores[task_name] * 
                    task_base_weights[task_name] * 
                    sigmoid_multiplier * 
                    completeness_factor
                )
    
    # Apply multi-task bonus
    num_tasks = len(task_contributions)
    multi_task_bonus = 0.7 + (num_tasks * 0.15)  # 0.85, 1.0, 1.15
    
    return sum(task_contributions.values()) * multi_task_bonus
```

### Strategic Analysis Examples

#### Example 1: Conservative Two-Task Approach
```python
# Geo + Soil specialist avoiding weather complexity
geo_score = 0.89  # 60th percentile → 1.03× multiplier
soil_score = 0.72  # 45th percentile → 0.89× multiplier

geo_contribution = 0.89 * 0.15 * 1.03 * 1.0 = 0.138
soil_contribution = 0.72 * 0.15 * 0.89 * 1.0 = 0.096
multi_task_bonus = 1.00  # 2 tasks

total_weight = (0.138 + 0.096) * 1.00 = 0.234
```

#### Example 2: Aggressive Three-Task Approach  
```python
# Full platform engagement with mixed performance
weather_score = 0.81  # 55th percentile → 0.98× multiplier
geo_score = 0.87     # 40th percentile → 0.84× multiplier  
soil_score = 0.69    # 65th percentile → 1.07× multiplier

weather_contribution = 0.81 * 0.70 * 0.98 * 1.0 = 0.556
geo_contribution = 0.87 * 0.15 * 0.84 * 1.0 = 0.110
soil_contribution = 0.69 * 0.15 * 1.07 * 1.0 = 0.111
multi_task_bonus = 1.15  # 3 tasks

total_weight = (0.556 + 0.110 + 0.111) * 1.15 = 0.894
```

#### Example 3: Unbalanced Performance Risk
```python
# Strong weather, weak geo, decent soil
weather_score = 0.92  # 80th percentile → 1.15× multiplier
geo_score = 0.78     # 20th percentile → 0.48× multiplier (penalty!)
soil_score = 0.74    # 50th percentile → 0.95× multiplier

weather_contribution = 0.92 * 0.70 * 1.15 * 1.0 = 0.742
geo_contribution = 0.78 * 0.15 * 0.48 * 1.0 = 0.056  # Severely penalized
soil_contribution = 0.74 * 0.15 * 0.95 * 1.0 = 0.105
multi_task_bonus = 1.15  # 3 tasks

total_weight = (0.742 + 0.056 + 0.105) * 1.15 = 1.038

# Note: Poor geo performance significantly drags down total despite strong weather
```

### Diversity Pathway Advantages

#### Risk Distribution
- **No single point of failure** - poor performance in one task doesn't eliminate all weight
- **Stable income potential** - consistent moderate performance across tasks
- **Adaptability** - can shift focus based on competitive landscape changes

#### Competitive Positioning
- **Lower barriers to entry** - doesn't require top-tier specialization
- **Sustainable long-term strategy** - less vulnerable to skill arms races in single tasks
- **Defensive against gaming** - harder for coordinated attacks to dominate multiple tasks

#### Resource Efficiency
- **Complementary knowledge** - skills and data sources may overlap between tasks
- **Diversified infrastructure** - computational resources used across multiple prediction types
- **Learning synergies** - atmospheric science knowledge applies to weather, geo, and climate-related soil patterns

## Completeness Factors

Completeness factors prevent gaming through sparse submissions by requiring **consistent participation** over time. They measure how many predictions a miner submitted compared to the total scoring opportunities available during the evaluation window.

### Purpose & Anti-Gaming Design

**Without completeness factors**, miners could:
- Submit only 1-2 "lucky" predictions and achieve high average scores
- Game the system by cherry-picking optimal prediction timing
- Achieve unfairly high weights with minimal effort during immunity periods

**With completeness factors**, miners must:
- Demonstrate sustained engagement with prediction tasks
- Build meaningful performance history before receiving full weight potential
- Compete fairly based on consistent performance, not statistical anomalies

### Task-Specific Implementation

#### Geomagnetic Task Completeness
- **Expected predictions**: Number of available hourly scoring periods (typically 18-24 in recent window)
- **Calculation window**: 24-hour rolling window
- **Example scenario**: If 18 scoring periods occurred in the last 24 hours, a miner needs ≥6 predictions (30%) for full weight

```python
geo_expected = len(geomagnetic_results)  # Number of scoring periods
geo_actual = np.sum(miner_geo_predictions > 0)  # Non-zero predictions
geo_completeness_ratio = geo_actual / max(geo_expected, 1)
```

#### Soil Moisture Task Completeness
- **Expected predictions**: Number of available soil scoring cycles (typically 2-3 in recent window)
- **Calculation window**: 7-day rolling window to account for processing delays
- **Example scenario**: If 3 soil scoring cycles occurred, a miner needs ≥1 prediction (33%) for full weight

```python
soil_expected = len(soil_results)  # Number of scoring cycles
soil_actual = np.sum(miner_soil_predictions > 0)  # Non-zero predictions  
soil_completeness_ratio = soil_actual / max(soil_expected, 1)
```

#### Weather Task Completeness (Future Implementation)
- **Expected predictions**: Separate tracking for day-1 and day-10 scores
- **Day-1 completeness**: Based on recent daily prediction submissions
- **Day-10 completeness**: Based on predictions that have matured to final scoring
- **Dual evaluation**: May require different thresholds for immediate vs mature scores

### Threshold-Based Scaling System

Rather than linear penalties, completeness uses a **threshold + gentle scaling** approach:

#### 30% Threshold Design
- **Above 30%**: Full weight potential (factor = 1.0)
- **Below 30%**: Graduated penalty using square root scaling
- **Rationale**: 30% represents meaningful participation while being achievable for new miners

#### Mathematical Formula
```python
def calculate_completeness_factor(actual_predictions, expected_predictions):
    if expected_predictions == 0:
        return 1.0  # No penalty if no opportunities existed
    
    completeness_ratio = actual_predictions / expected_predictions
    threshold = 0.30
    
    if completeness_ratio >= threshold:
        return 1.0  # Full weight for adequate participation
    else:
        # Square root scaling for gentler penalty curve
        return (completeness_ratio / threshold) ** 0.5
```

#### Penalty Curve Examples

| Predictions | Ratio | Factor | Interpretation |
|-------------|-------|--------|----------------|
| 18/18 | 100% | 1.00 | Perfect participation |
| 12/18 | 67% | 1.00 | Good participation (above threshold) |
| 6/18 | 33% | 1.00 | Adequate participation (above threshold) |
| 5/18 | 28% | 0.96 | Slight penalty (just below threshold) |
| 3/18 | 17% | 0.73 | Moderate penalty |
| 2/18 | 11% | 0.61 | Significant penalty |
| 1/18 | 6% | 0.45 | Heavy penalty (prevents gaming) |

### Integration with Scoring Pathways

#### Excellence Pathway Integration
```python
# Completeness applied directly to final score
excellence_weight = (miner_score * task_base_weight * completeness_factor)

# Example: Geo specialist with 85th percentile performance
base_score = 0.92
task_weight = 0.15
completeness = 0.73  # Only 3/18 predictions

final_excellence_weight = 0.92 × 0.15 × 0.73 = 0.101
# vs full participation: 0.92 × 0.15 × 1.0 = 0.138
```

#### Diversity Pathway Integration
```python
# Completeness applied to each task contribution before diversity bonus
task_contribution = (miner_score * task_base_weight * 
                    sigmoid_multiplier * completeness_factor)

# Example: Multi-task miner with varying participation
geo_contrib = 0.89 × 0.15 × 1.05 × 1.00 = 0.140  # Full geo participation
soil_contrib = 0.72 × 0.15 × 0.88 × 0.61 = 0.058  # Limited soil participation (2/3)

diversity_weight = (0.140 + 0.058) × 1.00 = 0.198  # 2-task bonus
```

### Time Window Interactions

#### During Immunity Period (First 3 Days)

**Geomagnetic completeness**:
- ~72 prediction opportunities (3 days × 24 hours)
- 30% threshold = 22 predictions needed for full factor
- New miners can achieve full completeness within immunity period

**Soil completeness**:
- 0-1 scoring opportunities (due to 3-day processing delay)
- Completeness factor typically 1.0 (insufficient data for meaningful penalty)
- Real completeness evaluation begins post-immunity

**Weather completeness**:
- 3 day-1 prediction opportunities during immunity
- 30% threshold = 1 prediction needed for full factor
- Day-10 scores won't be available until post-immunity

#### Post-Immunity Steady State

**Rolling window evaluation**:
- Each task maintains its specific evaluation window
- Completeness calculated on recent performance only
- Historical immunity-period performance gradually phases out

### Edge Cases & Fallbacks

#### Insufficient Scoring Opportunities
```python
if expected_predictions < 3:
    # Not enough data for meaningful completeness evaluation
    completeness_factor = 1.0  # No penalty applied
```

#### New Task Activation
- When new tasks launch, initial completeness requirements may be reduced
- Grace period for miners to adapt to new prediction requirements
- Gradual scaling as task participation normalizes

#### System Maintenance Periods
- Completeness calculations account for validator downtime
- Expected predictions adjusted for actual system availability
- No penalties for periods when predictions were impossible

### Strategic Implications

#### For Excellence Path Miners
- Must maintain consistent participation in chosen specialty task
- Cannot rely on sporadic "lucky" predictions for high weights
- Completeness becomes critical during competitive periods

#### For Diversity Path Miners  
- Uneven participation across tasks creates strategic tradeoffs
- Better to fully participate in 2 tasks than poorly participate in 3
- Completeness penalties can offset multi-task bonuses

#### Gaming Prevention Analysis
- **Attack vector**: Mass registration with minimal effort
- **Prevention**: 30% threshold requires meaningful participation
- **Economic impact**: Increases minimum effort cost, reduces attack profitability
- **Legitimate miner protection**: Reasonable threshold achievable by honest participants

## Dynamic Geomagnetic Sigmoid

To address the typically flat distribution of geomagnetic scores, a dynamic sigmoid is applied:

### Parameters
- **Center (x₀)**: Mean of all valid geomagnetic scores
- **Steepness (k)**: Adaptive based on score distribution spread
  - Tight clustering → Higher steepness (amplify small differences)
  - Wide spread → Lower steepness (less aggressive)

### Calculation
```python
geo_mean = np.mean(valid_geo_scores)
geo_std = np.std(valid_geo_scores)
sigmoid_k = max(10, min(30, 20 / max(geo_std, 0.01)))
sigmoid_transform = 1 / (1 + exp(-sigmoid_k * (score - geo_mean)))
```

## Final Weight Selection

Each miner receives the **maximum** of their excellence and diversity pathway weights:

```python
final_weight = max(excellence_weight, diversity_weight)
```

This allows miners to optimize for either specialization or generalization based on their strengths and preferences.

## Strategic Guidance for Miners

### During 3-Day Immunity Period

**For Geomagnetic Specialists:**
- ✅ **Focus intensively on geomagnetic predictions** (72 opportunities)
- ✅ **Aim for top 15% performance** to unlock excellence pathway
- ⚠️ **Consider basic weather participation** for day-1 scores as insurance
- ❌ **Don't ignore other tasks entirely** - may need diversity fallback

**For Soil Specialists:**
- ⚠️ **Cannot ignore geomagnetic entirely** (soil scores won't be available)
- ✅ **Submit soil predictions immediately** (3-day processing delay)
- ✅ **Maintain geomagnetic participation** for diversity pathway eligibility
- ✅ **Plan for post-immunity soil scoring** when processed scores arrive

**For Weather Specialists:**
- ✅ **Focus on weather day-1 scores** during immunity
- ✅ **Maintain geomagnetic participation** as backup scoring source
- ⚠️ **Plan for day-10 score maturation** post-immunity
- ✅ **Build consistent prediction patterns** for completeness factors

**For Generalists:**
- ✅ **Participate in all available tasks** during immunity
- ✅ **Focus on consistency over perfection** in each task
- ✅ **Build prediction history** for all tasks where possible
- ✅ **Aim for above-median performance** in multiple tasks

### Post-Immunity Strategy

**Excellence Path Miners:**
- Must maintain **top 15% performance** consistently
- Can focus resources on single task optimization
- Should monitor competition levels and adjust if needed

**Diversity Path Miners:**
- Should aim for **above-median performance** in all participated tasks
- Benefit from **consistent multi-task participation**
- Can leverage **multi-task bonuses** for competitive advantage

### Economic Considerations

**Registration Fee Recovery:**
- Current fees: 0.1-0.3 TAO (recoverable in 1+ days as top performer)
- Excellence pathway: Requires genuine skill development
- Diversity pathway: Requires broader resource allocation
- Gaming attacks: No longer economically viable due to high performance requirements

### Risk Management

**For New Miners:**
- **Diversify early**: Don't put all resources into one task during immunity
- **Monitor percentiles**: Track your relative performance in real-time
- **Build history**: Consistent participation more important than perfect scores
- **Plan transitions**: Understand when different scores become available

**For Established Miners:**
- **Defend excellence**: Top 15% positions require ongoing optimization
- **Consider diversity**: Multi-task approach may offer more stable weights
- **Adapt to competition**: Performance requirements scale with miner participation
- **Monitor gaming**: Report coordinated attacks through proper channels

## Technical Implementation Notes

### Percentile Calculations
- Minimum 5 miners required for reliable percentiles
- Fallback to individual scoring if insufficient sample size
- Rolling windows prevent stale performance comparisons

### Score Aggregation
- Time-weighted averaging with exponential decay
- Recent performance weighted more heavily than historical
- Completeness factors applied before pathway selection

### Weight Normalization
- Final weights normalized across all miners
- Additional transformations applied for network consensus
- Anti-gaming measures remain active throughout process

---

*This scoring system is designed to reward genuine performance while preventing gaming attacks. Miners should focus on developing real predictive capabilities rather than attempting to exploit system mechanics.* 