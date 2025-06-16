# Deterministic Job ID System

## Overview

The deterministic job ID system ensures that validator replicas generate identical job IDs for the same tasks, preventing job tracking inconsistencies during database synchronization.

## Key Principles

1. **Use SCHEDULED timestamps** - Never use processing time (`datetime.now()`)
2. **Use predetermined events** - GFS model times, SMAP satellite times, prediction targets
3. **Include context** - Miner hotkey, validator hotkey, job type

## Usage Examples

### Weather Tasks
```python
from gaia.tasks.base.deterministic_job_id import DeterministicJobID

# ✅ CORRECT: Use the scheduled GFS initialization time
job_id = DeterministicJobID.generate_weather_job_id(
    gfs_init_time=gfs_scheduled_time,  # From GFS model schedule
    miner_hotkey=miner_hotkey,
    validator_hotkey=validator_hotkey,
    job_type="forecast"
)

# ❌ WRONG: Using current processing time
job_id = DeterministicJobID.generate_weather_job_id(
    gfs_init_time=datetime.now(timezone.utc),  # Different on each node!
    miner_hotkey=miner_hotkey,
    validator_hotkey=validator_hotkey,
    job_type="forecast"
)
```

### Geomagnetic Tasks
```python
# Use the scheduled hourly prediction target time
job_id = DeterministicJobID.generate_geomagnetic_job_id(
    query_time=prediction_target_time,  # Scheduled hour (e.g., 14:00:00 UTC)
    miner_hotkey=miner_hotkey,
    validator_hotkey=validator_hotkey
)
```

### Soil Moisture Tasks
```python
# Use the SMAP satellite target time
job_id = DeterministicJobID.generate_soil_moisture_job_id(
    target_time=smap_target_time,  # Scheduled SMAP time
    miner_hotkey=miner_hotkey,
    validator_hotkey=validator_hotkey,
    region_bbox=bbox_string  # Optional for spatial differentiation
)
```

### Generic Task IDs
```python
# For scoring and other task-level operations
task_id = DeterministicJobID.generate_task_id(
    task_name="weather",
    target_time=scheduled_target_time,
    additional_context="optional_context"
)
```

## Migration Status

### ✅ Implemented
- **Weather Task**: Updated to use deterministic job IDs in `miner_execute` and `handle_initiate_fetch`
- **Soil Moisture Task**: Already uses deterministic timestamp-based task IDs
- **Geomagnetic Task**: Already uses deterministic timestamp-based task IDs

### 📋 Implementation Details

#### Weather Task Changes
- `miner_execute()`: Generates deterministic job ID from GFS init time
- `handle_initiate_fetch()`: Uses deterministic ID for fetch jobs
- Validates existing job IDs match deterministic generation

#### Task ID Patterns
- **Weather**: `weather_job_forecast_20250611180000_{miner_hotkey}_{validator_hotkey}`
- **Soil**: `{smap_target_timestamp}` (already deterministic)
- **Geomagnetic**: `{prediction_target_timestamp}` (already deterministic)

## Benefits

1. **Replica Sync Safety**: Jobs maintain consistency after database overwrites
2. **No Job Loss**: Eliminates orphaned jobs during validator failover
3. **Deterministic Behavior**: Same inputs always produce same job IDs
4. **Cross-validator Coordination**: Multiple validators can coordinate same jobs
5. **Debuggable**: Job IDs contain meaningful timestamp information

## Timestamp Normalization

The system automatically normalizes timestamps to ensure consistency:

```python
# Automatic handling of:
- Timezone conversion to UTC
- Microsecond removal
- Optional rounding to nearest second
```

## Backward Compatibility

The system gracefully handles existing random job IDs while transitioning to deterministic ones. Logs will show when existing IDs don't match deterministic generation.

## Troubleshooting

### Job ID Mismatches
If you see warnings about job ID mismatches:
```
⚠️ Existing job ID abc123 doesn't match deterministic ID def456
```
This indicates jobs created before the deterministic upgrade. They will continue to work normally.

### Invalid Timestamps
Ensure scheduled timestamps are:
- UTC timezone
- Actual scheduled times (not processing times)
- Consistent across all validator nodes 