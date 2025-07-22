# Miner Performance Stats Enhancement Plan

## Executive Summary

The goal is to enhance the `miner_performance_stats` table to provide a complete trace of miner performance from individual task predictions through to final on-chain weights and incentives. This system should comprehensively track the entire scoring pipeline and provide transparency into how individual task performance contributes to final validator weights and chain consensus rewards.

## Current System Analysis

### 🔍 **Existing Data Flow Understanding**

1. **Task Score Generation**: Individual task scores are stored in `score_table` (weather, geomagnetic, soil_moisture_region_*)
2. **Weight Calculation Process**: 
   - Scores are aggregated in `_calc_task_weights()` → `_perform_weight_calculations_sync()`
   - **Excellence Pathway**: Top 15% performers in any single task get maximum weight 
   - **Diversity Pathway**: Multi-task performers get scaled weights with bonuses
   - **Final Weight**: `max(excellence_weight, diversity_weight)` per miner
   - **Transformations Applied**: Sigmoid curves, rank-based fallbacks, normalization
3. **Chain Submission**: Final weights submitted via `FiberWeightSetter.set_weights()`
4. **Chain Consensus**: Bittensor network runs consensus to determine final `incentive` values
5. **Node Data Sync**: `handle_miner_deregistration_loop()` syncs chain data to `node_table`

### 📊 **Current Table Schema Analysis**

**Strengths:**
- Comprehensive task-specific metrics (weather, soil, geomagnetic)
- Performance trends and rankings
- JSON fields for detailed metrics storage
- Proper indexing and constraints

**Gaps:**
- No traceability from individual scores to final weights
- Missing pathway information (excellence vs diversity)
- No chain consensus data integration
- Redundant `id` field when `miner_uid` could be primary
- No intermediate calculation values stored

## Enhancement Strategy

### 🎯 **Core Objectives**

1. **Complete Pipeline Traceability**: Track every step from task scores → validator weights → chain incentives
2. **Pathway Transparency**: Record which scoring pathway (excellence/diversity) was used
3. **Intermediate Value Storage**: Capture all calculation steps for debugging and analysis
4. **Chain Integration**: Include final consensus results and weight submission data
5. **Real-time Updates**: Sync with main scoring cycle to avoid duplicate work
6. **Automatic Cleanup**: Remove deregistered miner data automatically

### 🗂️ **Table Schema Modifications**

#### **Columns to Remove:**
- `id` - Use `miner_uid` as primary key instead

#### **Columns to Add:**

**Weight Calculation Pipeline:**
- `submitted_weight` (DOUBLE PRECISION) - Final weight submitted to chain by this validator
- `raw_calculated_weight` (DOUBLE PRECISION) - Pre-normalization weight from scoring algorithm
- `excellence_weight` (DOUBLE PRECISION) - Weight from excellence pathway calculation
- `diversity_weight` (DOUBLE PRECISION) - Weight from diversity pathway calculation
- `scoring_pathway` (VARCHAR(20)) - Which pathway was selected: 'excellence', 'diversity', 'none'
- `pathway_details` (JSONB) - Detailed breakdown of pathway calculation

**Chain Consensus Integration:**
- `incentive` (DOUBLE PRECISION) - Final incentive value from chain consensus
- `consensus_rank` (INTEGER) - Miner's rank based on final incentive values
- `weight_submission_block` (BIGINT) - Block number when weights were submitted
- `consensus_block` (BIGINT) - Block number when consensus was calculated

**Task Weight Contributions:**
- `weather_weight_contribution` (DOUBLE PRECISION) - How much weather scores contributed to final weight
- `geomagnetic_weight_contribution` (DOUBLE PRECISION) - How much geomagnetic scores contributed
- `soil_weight_contribution` (DOUBLE PRECISION) - How much soil scores contributed
- `multi_task_bonus` (DOUBLE PRECISION) - Bonus applied for multi-task participation

**Performance Analysis:**
- `percentile_rank_weather` (DOUBLE PRECISION) - Percentile rank in weather task (0-100)
- `percentile_rank_geomagnetic` (DOUBLE PRECISION) - Percentile rank in geomagnetic task (0-100)  
- `percentile_rank_soil` (DOUBLE PRECISION) - Percentile rank in soil task (0-100)
- `excellence_qualified_tasks` (TEXT[]) - Array of tasks where miner qualified for excellence pathway
- `validator_hotkey` (TEXT) - Which validator calculated these stats (for multi-validator environments)

#### **Modified Primary Key:**
```sql
ALTER TABLE miner_performance_stats DROP CONSTRAINT miner_performance_stats_pkey;
ALTER TABLE miner_performance_stats DROP COLUMN id;
ALTER TABLE miner_performance_stats ADD PRIMARY KEY (miner_uid, period_start, period_end, period_type);
```

### 🔧 **Integration Points**

#### **1. Main Scoring Integration** (`validator.py`)
**Location**: `_perform_weight_calculations_sync()` and `_calc_task_weights()`

**Integration Strategy**:
- Capture weight calculation data during the existing sync process
- Extract pathway decisions, intermediate values, and final weights
- Store data before final weight submission to chain
- **No duplicate work** - piggyback on existing calculations

**Key Data Points to Capture**:
```python
# From _perform_weight_calculations_sync()
performance_data = {
    'raw_calculated_weight': weights_final[idx],
    'excellence_weight': excellence_weight,
    'diversity_weight': diversity_weight, 
    'scoring_pathway': pathway,
    'weather_weight_contribution': weather_contribution,
    'percentile_rank_weather': get_percentile_rank(w_s, 'weather'),
    'excellence_qualified_tasks': qualified_tasks,
    'pathway_details': detailed_breakdown_json
}
```

#### **2. Chain Data Integration** (`handle_miner_deregistration_loop()`)
**Location**: When syncing `node_table` with chain data

**Integration Strategy**:
- Update performance stats with latest `incentive` values from chain
- Record consensus block numbers and submission details
- Calculate consensus rankings based on final incentive values

#### **3. Deregistration Cleanup** 
**Location**: `handle_miner_deregistration_loop()` - already implemented!

**Current Implementation**: The deregistration loop already calls:
```python
# Step 2.1: Performance stats cleanup for hotkey changes
await self.performance_calculator.cleanup_specific_miner(uid, old_hotkey)

# Step 2.5: Bulk cleanup for deregistered miners  
await self.performance_calculator.cleanup_deregistered_miners()
```

### 🏗️ **Implementation Plan**

#### **Phase 1: Schema Enhancement** (Week 1)
1. **Database Migration Script**:
   - Add new columns with appropriate defaults
   - Modify primary key constraints  
   - Update indexes for optimal query performance
   - Backward compatibility for existing data

2. **Update `MinerPerformanceCalculator`**:
   - Modify data models to include new fields
   - Update `save_performance_stats()` method
   - Enhance cleanup methods if needed

#### **Phase 2: Weight Calculation Integration** (Week 2)
1. **Modify `_perform_weight_calculations_sync()`**:
   - Extract pathway decisions and intermediate calculations
   - Capture per-task contributions and percentile ranks
   - Store data in structured format for performance stats

2. **Update `_calc_task_weights()`**:
   - Pass additional metadata to sync function
   - Trigger performance stats update with enhanced data
   - Ensure no performance degradation to weight calculation

#### **Phase 3: Chain Integration** (Week 3)  
1. **Enhance `handle_miner_deregistration_loop()`**:
   - Update performance stats with latest incentive values
   - Record consensus timing and block information
   - Calculate consensus-based rankings

2. **Add Chain Data Validation**:
   - Verify weight submissions were successful
   - Track consensus delay and timing metrics
   - Handle edge cases (failed submissions, fork scenarios)

#### **Phase 4: Testing & Validation** (Week 4)
1. **End-to-End Testing**:
   - Verify complete data flow from scores to incentives
   - Test deregistration cleanup workflows
   - Validate performance impact on main scoring loop

2. **Data Quality Checks**:
   - Ensure all calculation steps are captured accurately
   - Verify pathway logic is correctly recorded
   - Test edge cases (no scores, tied pathways, etc.)

### 📋 **Updated Data Flow**

```
1. Task Execution → Individual Scores (score_table)
                 ↓
2. Score Aggregation → Raw weights + pathway decisions (_perform_weight_calculations_sync)
                    ↓  
3. Weight Processing → Final normalized weights + submission (FiberWeightSetter)
                    ↓
4. Chain Consensus → Final incentive values (Bittensor network)
                   ↓ 
5. Node Sync → Updated node_table with incentives (handle_miner_deregistration_loop)
             ↓
6. Performance Stats → Complete pipeline record (miner_performance_stats)
```

### 🧹 **Cleanup & Maintenance**

**Automatic Deregistration Handling** (Already Working!):
- `cleanup_specific_miner()` - for hotkey changes
- `cleanup_deregistered_miners()` - for bulk cleanup  
- Triggered automatically in deregistration loop

**Performance Monitoring**:
- Track table size and query performance
- Monitor main scoring loop impact
- Alert on calculation failures or data gaps

### 🎛️ **Configuration Options**

```python
PERFORMANCE_STATS_CONFIG = {
    'enable_detailed_pathways': True,          # Capture pathway decisions
    'enable_chain_integration': True,          # Sync with chain consensus  
    'enable_realtime_updates': True,           # Update during scoring cycle
    'retention_days': 30,                      # How long to keep historical data
    'cleanup_frequency_hours': 4,              # How often to cleanup deregistered miners
    'performance_impact_threshold': 0.1        # Max allowed scoring loop slowdown (seconds)
}
```

### 📊 **Expected Benefits**

1. **Complete Transparency**: Full visibility into scoring pipeline from start to finish
2. **Debug Capabilities**: Ability to trace any miner's performance calculation steps  
3. **Analytics Power**: Rich data for identifying scoring patterns and validator behavior
4. **Automated Maintenance**: Self-cleaning system that handles miner lifecycle changes
5. **Multi-Validator Support**: Ready for environments with multiple validators

### ⚡ **Performance Considerations**

- **Minimal Impact**: Piggyback on existing calculations, no duplicate work
- **Efficient Storage**: Use JSONB for complex data, proper indexing
- **Batch Operations**: Group database updates to minimize overhead
- **Memory Management**: Clean up calculation artifacts promptly

### 🔐 **Risk Mitigation**

- **Backward Compatibility**: Graceful handling of missing fields during migration
- **Failure Isolation**: Performance stats failures won't impact main scoring  
- **Data Validation**: Input sanitization and constraint checking
- **Rollback Plan**: Database migration can be reversed if issues arise

---

## Next Steps

1. **Review & Approval**: Stakeholder review of enhancement plan
2. **Database Migration**: Create and test migration scripts  
3. **Code Implementation**: Phased rollout as outlined above
4. **Testing & Validation**: Comprehensive testing in staging environment
5. **Production Deployment**: Gradual rollout with monitoring

This plan provides a comprehensive solution for tracking miner performance from individual task scores through to final chain incentives, creating complete transparency and powerful analytics capabilities while maintaining system performance and reliability. 