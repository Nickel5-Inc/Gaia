# Weather Forecast Component Scores Schema Design

## Overview
We need to store detailed component scores for full transparency in weather forecast scoring. Each forecast has multiple timesteps, variables, and metrics that contribute to the final score.

## Proposed Table: `weather_forecast_component_scores`

This table will store individual metric values for each variable, lead time, and scoring type (day1 vs ERA5).

### Schema Design

```sql
CREATE TABLE weather_forecast_component_scores (
    id SERIAL PRIMARY KEY,
    
    -- Foreign Keys for relationships
    run_id INTEGER NOT NULL REFERENCES weather_forecast_runs(id) ON DELETE CASCADE,
    response_id INTEGER NOT NULL REFERENCES weather_miner_responses(id) ON DELETE CASCADE,
    miner_uid INTEGER NOT NULL REFERENCES node_table(uid) ON DELETE CASCADE,
    miner_hotkey VARCHAR(255) NOT NULL,
    
    -- Scoring context
    score_type VARCHAR(50) NOT NULL, -- 'day1' or 'era5'
    lead_hours INTEGER NOT NULL, -- 6, 12, 24, 48, 72, etc.
    valid_time_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Variable identification
    variable_name VARCHAR(20) NOT NULL, -- '2t', 'msl', 't', 'u', 'v', 'q', 'z', '10u', '10v'
    pressure_level INTEGER, -- NULL for surface variables, 500/850 for pressure levels
    
    -- Component Metrics (all stored as FLOAT)
    rmse FLOAT, -- Root Mean Square Error
    mse FLOAT, -- Mean Square Error  
    acc FLOAT, -- Anomaly Correlation Coefficient (-1 to 1)
    skill_score FLOAT, -- Skill score vs reference (0 to 1, can be negative)
    skill_score_gfs FLOAT, -- Skill score vs GFS specifically
    skill_score_climatology FLOAT, -- Skill score vs climatology
    bias FLOAT, -- Mean bias
    mae FLOAT, -- Mean Absolute Error (if calculated)
    
    -- Sanity check results
    climatology_check_passed BOOLEAN,
    pattern_correlation FLOAT,
    pattern_correlation_passed BOOLEAN,
    
    -- Penalties applied
    clone_penalty FLOAT DEFAULT 0,
    quality_penalty FLOAT DEFAULT 0,
    
    -- Final weighted contribution
    weighted_score FLOAT, -- This variable's contribution to final score
    variable_weight FLOAT, -- Weight used for this variable
    
    -- Metadata
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    calculation_duration_ms INTEGER, -- How long this metric took to calculate
    
    -- Indexes for efficient querying
    UNIQUE(response_id, score_type, lead_hours, variable_name, pressure_level),
    INDEX idx_wfcs_run_id (run_id),
    INDEX idx_wfcs_miner_uid (miner_uid),
    INDEX idx_wfcs_miner_hotkey (miner_hotkey),
    INDEX idx_wfcs_score_type_lead (score_type, lead_hours),
    INDEX idx_wfcs_variable (variable_name, pressure_level),
    INDEX idx_wfcs_valid_time (valid_time_utc),
    INDEX idx_wfcs_calculated_at (calculated_at)
);
```

## Benefits of This Design

1. **Full Transparency**: Every component metric is stored and queryable
2. **Efficient Cascading Deletes**: Foreign keys ensure cleanup when runs/responses are deleted
3. **Deregistration Handling**: Can easily delete all scores for a miner via miner_uid FK
4. **Time-based Cleanup**: Indexed calculated_at allows efficient cleanup of old data
5. **Flexible Querying**: Can aggregate scores by variable, lead time, or miner
6. **Progressive Scoring Support**: Can track how scores evolve as more ERA5 data becomes available

## Query Examples

### Get all component scores for a specific forecast run
```sql
SELECT * FROM weather_forecast_component_scores 
WHERE run_id = ? 
ORDER BY miner_uid, lead_hours, variable_name;
```

### Get average RMSE by variable across all miners for a run
```sql
SELECT variable_name, pressure_level, AVG(rmse) as avg_rmse
FROM weather_forecast_component_scores
WHERE run_id = ? AND score_type = 'era5'
GROUP BY variable_name, pressure_level;
```

### Track score progression for a miner
```sql
SELECT lead_hours, AVG(skill_score) as avg_skill, AVG(acc) as avg_acc
FROM weather_forecast_component_scores
WHERE miner_uid = ? AND run_id = ?
GROUP BY lead_hours
ORDER BY lead_hours;
```

### Clean up old data
```sql
DELETE FROM weather_forecast_component_scores
WHERE calculated_at < NOW() - INTERVAL '30 days';
```

## Alternative Design: JSONB Column

Instead of individual columns for each metric, we could use a JSONB column:

```sql
CREATE TABLE weather_forecast_component_scores_jsonb (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES weather_forecast_runs(id) ON DELETE CASCADE,
    response_id INTEGER NOT NULL REFERENCES weather_miner_responses(id) ON DELETE CASCADE,
    miner_uid INTEGER NOT NULL REFERENCES node_table(uid) ON DELETE CASCADE,
    miner_hotkey VARCHAR(255) NOT NULL,
    
    score_type VARCHAR(50) NOT NULL,
    lead_hours INTEGER NOT NULL,
    valid_time_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    variable_name VARCHAR(20) NOT NULL,
    pressure_level INTEGER,
    
    -- All metrics in JSONB
    metrics JSONB NOT NULL,
    /* Example content:
    {
        "rmse": 2.5,
        "mse": 6.25,
        "acc": 0.85,
        "skill_score": 0.72,
        "skill_score_gfs": 0.68,
        "bias": -0.3,
        "sanity_checks": {
            "climatology_passed": true,
            "pattern_correlation": 0.92
        }
    }
    */
    
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(response_id, score_type, lead_hours, variable_name, pressure_level)
);

-- Create indexes on JSONB fields for common queries
CREATE INDEX idx_wfcs_jsonb_rmse ON weather_forecast_component_scores_jsonb ((metrics->>'rmse'));
CREATE INDEX idx_wfcs_jsonb_acc ON weather_forecast_component_scores_jsonb ((metrics->>'acc'));
CREATE INDEX idx_wfcs_jsonb_skill ON weather_forecast_component_scores_jsonb ((metrics->>'skill_score'));
```

## Recommendation

I recommend the **first approach with individual columns** because:
1. Better type safety and validation
2. Easier to query and aggregate
3. Better performance for numerical operations
4. Clearer schema documentation
5. Easier to add constraints if needed

The JSONB approach would be better if:
- We expect metrics to vary significantly between scoring types
- We need to add new metrics frequently without schema changes
- We want maximum flexibility

## Implementation Steps

1. Create the table via Alembic migration
2. Update scoring functions to insert component scores
3. Update aggregation functions to use component scores
4. Add API endpoints to query component scores for transparency
5. Add cleanup jobs for old component scores
