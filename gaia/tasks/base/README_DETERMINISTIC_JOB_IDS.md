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
# NOTE: validator_hotkey is no longer included in job ID to allow reuse across validators
# NOTE: We use "forecast" job_type for both main forecast and fetch phases to unify job IDs
job_id = DeterministicJobID.generate_weather_job_id(
    gfs_init_time=gfs_scheduled_time,  # From GFS model schedule
    miner_hotkey=miner_hotkey,
    validator_hotkey=validator_hotkey,  # Kept for backwards compatibility
    job_type="forecast"  # Unified type for both forecast and fetch phases
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
- **Weather**: `weather_job_forecast_{timestamp}_{miner_hotkey}` (unified forecast type, validator_hotkey removed for cross-validator reuse)
- **Soil**: `{smap_target_timestamp}` (already deterministic)
- **Geomagnetic**: `{prediction_target_timestamp}` (already deterministic)

## Benefits

1. **Replica Sync Safety**: Jobs maintain consistency after database overwrites
2. **No Job Loss**: Eliminates orphaned jobs during validator failover
3. **Deterministic Behavior**: Same inputs always produce same job IDs
4. **Cross-validator Coordination**: Multiple validators can coordinate same jobs
5. **Cross-validator Job Reuse**: Weather jobs can be reused across different validators for the same GFS timestep/miner
6. **Unified Job Types**: Weather tasks use a single "forecast" job type to prevent job ID fragmentation between phases
7. **Database Sync Resilience**: Automatic fallback and recovery mechanisms handle hourly database overwrites
8. **Zero Work Loss**: Miners never lose scoring opportunities due to job ID mismatches
9. **Self-Healing**: System automatically reconciles job IDs and updates records as needed
10. **Debuggable**: Job IDs contain meaningful timestamp information

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

## Database Synchronization Resilience

### The Challenge

In the current system, validator nodes overwrite their databases hourly to match the primary node. This creates a distributed systems challenge where:

1. **Multiple validators** create jobs with potentially different IDs
2. **Database overwrites** can cause job ID mismatches  
3. **Miners lose scoring opportunities** when their work can't be found
4. **Verification fails** due to missing job records

### Resilience Features

The system now implements comprehensive fallback strategies to handle database synchronization scenarios:

#### 1. **Multi-Strategy Job Lookup**
- **Primary**: Exact job ID match
- **Fallback 1**: Same GFS timestep + validator  
- **Fallback 2**: Same GFS timestep + any validator
- **No timestamp tolerance**: GFS timestep must match exactly for weather data validity

#### 2. **Alternative Job Discovery**
- **Timestamp Extraction**: Parse GFS time from deterministic job IDs
- **Cross-Validator Mapping**: Match jobs across different validators for same GFS timestep
- **Exact Timestep Requirement**: Only jobs with identical GFS times are considered valid

#### 3. **Automatic Job ID Reconciliation**
- **Miner Side**: `find_job_by_alternative_methods()`
- **Validator Side**: `reconcile_job_id_for_validator()`
- **Verification**: Enhanced `verify_miner_response()` with fallback logic

#### 4. **Health Monitoring**
- **Job ID Health Checks**: `check_job_id_health()`
- **Mismatch Detection**: Track database sync issues
- **Proactive Monitoring**: Identify problems before they cause failures

### Implementation Details

```python
# Miner fallback lookup
async def find_job_by_alternative_methods(task_instance, job_id, miner_hotkey):
    # Try GFS timestamp extraction
    # Match jobs with exact same GFS timestep only
    # Return equivalent job if found

# Validator reconciliation  
async def reconcile_job_id_for_validator(task_instance, miner_job_id, miner_hotkey, gfs_init_time):
    # Generate expected job ID
    # Check for equivalent jobs
    # Return reconciled ID

# Enhanced verification with resilience
async def verify_miner_response(task_instance, run_details, response_details):
    # Primary token request
    # Fallback job discovery on failure
    # Auto-update job IDs in database
```

### Benefits

1. **Zero Job Loss**: Miners don't lose work due to database sync
2. **Automatic Recovery**: System self-heals from job ID mismatches  
3. **Cross-Validator Compatibility**: Jobs work regardless of validator source
4. **Scoring Preservation**: Miners maintain scoring opportunities
5. **Transparent Operation**: Fallbacks happen automatically with logging

### Monitoring

The system logs all resilience actions:
- `Database sync resilience: Found equivalent job X for missing job Y`
- `Database sync reconciliation: Using expected job ID Z`
- `Used fallback job ID due to database sync`

Use `check_job_id_health()` for proactive monitoring of job ID consistency. 

## Testing Strategy for Database Sync Resilience

### Overview

Testing the database synchronization resilience requires multi-node validation since the issues only manifest in distributed environments with multiple validators and hourly database overwrites.

### Test Environment Setup

#### **Testnet Configuration**
```bash
# Validator A (Primary)
VALIDATOR_A_HOTKEY="5GTA..."
VALIDATOR_A_DATABASE="primary_validator_db"

# Validator B (Replica) 
VALIDATOR_B_HOTKEY="5HTB..."
VALIDATOR_B_DATABASE="replica_validator_db"

# Test Miner
MINER_HOTKEY="5GMT..."
MINER_DATABASE="test_miner_db"
```

#### **Required Test Components**
1. **2+ Validators** - Running simultaneously on testnet
2. **1+ Test Miners** - Connected to both validators
3. **Database Sync Simulation** - Hourly overwrites between validators
4. **Monitoring Tools** - Log aggregation and job ID tracking
5. **Test Scenarios** - Comprehensive edge case coverage

### Test Scenarios

#### **Scenario 1: Basic Deterministic Job ID Generation**
```bash
# Test that both validators generate identical job IDs
OBJECTIVE: Verify job ID consistency across validators
STEPS:
1. Start both validators simultaneously
2. Initiate weather task at same GFS timestep
3. Capture job IDs generated by each validator
4. Verify job IDs are identical

EXPECTED: Job IDs match exactly
VALIDATION: Check logs for deterministic ID generation
```

#### **Scenario 2: Cross-Validator Job Reuse**
```bash
# Test miner can reuse jobs across different validators
OBJECTIVE: Verify cross-validator job compatibility
STEPS:
1. Validator A creates job for GFS timestep T
2. Miner completes job and stores results
3. Validator B requests same GFS timestep T
4. Verify miner reuses existing job

EXPECTED: Miner returns existing job, no duplicate work
VALIDATION: Check miner logs for job reuse messages
```

#### **Scenario 3: Database Overwrite Simulation**
```bash
# Simulate hourly database sync causing job ID mismatches
OBJECTIVE: Test resilience to database synchronization
SETUP:
1. Validator A creates jobs: JobA1, JobA2, JobA3
2. Miner completes jobs, stores results
3. Simulate database overwrite: Copy Validator B's DB to Validator A
4. Validator A now has different job IDs: JobB1, JobB2, JobB3
5. Validator A requests verification from miner

EXPECTED: System uses fallback lookup, finds equivalent jobs
VALIDATION: Check for "Database sync resilience" log messages
```

#### **Scenario 4: Verification Resilience Testing**
```bash
# Test miner verification when validator DB is out of sync
OBJECTIVE: Ensure miners never lose scoring opportunities
STEPS:
1. Miner completes job_id="weather_job_forecast_20250718180000_minerX"
2. Validator DB gets overwritten, now expects different job_id
3. Validator requests verification for the new job_id
4. Miner's fallback system should find equivalent job

EXPECTED: Verification succeeds using fallback mechanisms
VALIDATION: Check verify_miner_response fallback logic
```

#### **Scenario 5: Multi-Validator Competition**
```bash
# Test behavior with multiple validators scoring same miner
OBJECTIVE: Verify scoring preservation across validators
STEPS:
1. Start 3 validators (A, B, C) 
2. All request same GFS timestep from same miner
3. Simulate database overwrites between validators
4. Each validator attempts to score miner's work
5. Verify all validators can score the miner

EXPECTED: Miner gets scored by all validators despite DB differences
VALIDATION: Check scoring success in all validator databases
```

### Test Monitoring & Validation

#### **Key Log Messages to Monitor**
```bash
# Positive indicators
"Generated deterministic job ID: weather_job_forecast_.*"
"Found existing job .* from same validator"
"Found existing job .* from different validator - enabling cross-validator reuse"
"Database sync resilience: Found equivalent job .* for missing job"
"Database sync reconciliation: Using expected job ID"

# Warning indicators (expected during sync issues)
"Database sync resilience: Using equivalent job .* for missing job"
"Used fallback job ID due to database sync"

# Error indicators (should not occur)
"Job not found for job_id: .* even after fallback attempts"
"No reconciliation possible for job ID"
```

#### **Automated Test Validation**
```python
# Test validation script
async def validate_job_id_resilience():
    # 1. Check job ID determinism
    validator_a_jobs = await get_validator_jobs(validator_a)
    validator_b_jobs = await get_validator_jobs(validator_b)
    assert_job_ids_match(validator_a_jobs, validator_b_jobs)
    
    # 2. Check miner job reuse
    miner_jobs = await get_miner_jobs(test_miner)
    assert_no_duplicate_gfs_timesteps(miner_jobs)
    
    # 3. Check fallback activation
    fallback_logs = grep_logs("Database sync resilience")
    assert len(fallback_logs) > 0  # Should activate during testing
    
    # 4. Check scoring preservation
    scoring_results = await get_scoring_results()
    assert_all_miners_scored(scoring_results)
```

### Test Data Collection

#### **Metrics to Track**
```yaml
job_id_consistency:
  - identical_ids_across_validators: true/false
  - deterministic_generation_success_rate: percentage

job_reuse_efficiency:
  - cross_validator_reuse_count: number
  - duplicate_work_prevented: number
  - job_reuse_success_rate: percentage

resilience_activation:
  - fallback_lookup_activations: number
  - successful_job_recoveries: number
  - failed_fallback_attempts: number

scoring_preservation:
  - miners_scored_successfully: number
  - scoring_failures_due_to_job_ids: number
  - scoring_success_rate: percentage
```

#### **Database Health Monitoring**
```python
# Run periodically during tests
health_reports = []
for validator in [validator_a, validator_b]:
    report = await check_job_id_health(validator.weather_task)
    health_reports.append(report)
    
# Analyze patterns
sync_issues = sum(r["stats"]["sync_issues_24h"] for r in health_reports)
unique_validators = max(r["stats"]["unique_validators_24h"] for r in health_reports)
```

### Test Execution Plan

#### **Phase 1: Basic Functionality (Day 1)**
1. Deploy 2 validators on testnet
2. Run Scenarios 1-2 (deterministic IDs, job reuse)
3. Validate basic cross-validator functionality

#### **Phase 2: Resilience Testing (Day 2-3)**
1. Implement database sync simulation
2. Run Scenarios 3-4 (database overwrites, verification)
3. Test fallback mechanisms under stress

#### **Phase 3: Competition Testing (Day 4-5)**
1. Deploy 3+ validators
2. Run Scenario 5 (multi-validator competition)
3. Test scoring preservation at scale

#### **Phase 4: Long-term Stability (Day 6-7)**
1. 48-hour continuous run
2. Hourly database sync simulation
3. Monitor for edge cases and degradation

### Success Criteria

✅ **Deterministic Job IDs**: 100% consistency across validators
✅ **Job Reuse**: >95% cross-validator job reuse rate
✅ **Resilience**: >98% successful fallback recovery rate
✅ **Scoring**: 0% miners lose scoring due to job ID issues
✅ **Health**: <5% job ID health warnings during normal operation

### Failure Analysis

If tests fail, investigate:
1. **Job ID Generation**: Check timestamp consistency
2. **Database State**: Verify sync simulation accuracy  
3. **Fallback Logic**: Debug alternative lookup methods
4. **Timing Issues**: Check for race conditions
5. **Network Partitions**: Test validator communication 