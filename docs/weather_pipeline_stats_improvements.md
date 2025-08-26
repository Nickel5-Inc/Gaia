# Weather Pipeline Statistics Recording Improvements

## Overview
This document identifies opportunities to improve statistics recording throughout the weather task pipeline. Each substep should record detailed information about its execution, errors, and retries to the appropriate database tables.

## Key Database Tables for Statistics

### 1. `weather_forecast_stats` 
- Primary table for per-miner, per-run forecast statistics
- Tracks overall forecast status and scores
- **New columns added:**
  - `current_forecast_stage`: Detailed pipeline stage
  - `current_forecast_status`: Status within current stage  
  - `last_error_message`: Most recent error
  - `retries_remaining`: Number of retries left
  - `next_scheduled_retry`: Next retry timestamp
  - `avg_rmse`, `avg_acc`, `avg_skill_score`: Component averages

### 2. `weather_forecast_steps`
- Event log for step/substep progress
- Tracks retries and errors per step
- Records latency and context

### 3. `weather_miner_responses`
- Tracks miner communication status
- Records job IDs and verification status
- Has `inference_started_at` timestamp

### 4. `weather_forecast_component_scores`
- Detailed per-variable, per-lead-time metrics
- Stores RMSE, ACC, skill scores
- Tracks penalties and weights

### 5. `miner_stats`
- Aggregated statistics across all runs
- Success/failure ratios
- Performance trends

## Current Gaps and Improvements Needed

### 1. **Query Miner Step** (`query_miner_step.py`)

**Current State:**
- Records basic response status in `weather_miner_responses`
- Limited error detail recording
- No update to `weather_forecast_stats` pipeline tracking

**Improvements Needed:**
```python
# After each query attempt:
async def update_query_stats(db, run_id, miner_uid, miner_hotkey, status, error=None):
    # Update weather_forecast_stats
    await stats_manager.update_forecast_stats(
        run_id=run_id,
        miner_uid=miner_uid,
        miner_hotkey=miner_hotkey,
        status="inference_requested",
        current_forecast_stage="inference_requested",
        current_forecast_status=status,
        last_error_message=error,
        retries_remaining=3 - retry_count if error else None,
        next_scheduled_retry=next_retry_time if error else None
    )
    
    # Log to weather_forecast_steps
    await log_step(
        db,
        run_id=run_id,
        miner_uid=miner_uid,
        miner_hotkey=miner_hotkey,
        step_name="query",
        substep="initiate_fetch",
        status=status,
        error_json={"error": error, "retry_count": retry_count} if error else None,
        latency_ms=response_time_ms
    )
```

### 2. **Poll Miner Step** (`poll_miner_step.py`)

**Current State:**
- Updates `weather_miner_responses` status
- No detailed progress tracking
- Limited error categorization

**Improvements Needed:**
```python
# Track each polling attempt
async def record_poll_attempt(db, response_id, attempt, status, progress=None):
    # Update inference progress
    await db.execute("""
        UPDATE weather_miner_responses
        SET last_polled_time = NOW(),
            status = :status
        WHERE id = :id
    """, {"id": response_id, "status": status})
    
    # Update forecast stats with progress
    await stats_manager.update_forecast_stats(
        current_forecast_stage=f"inference_running_{progress}%" if progress else "inference_running",
        current_forecast_status="polling",
        hosting_latency_ms=poll_response_time_ms
    )
    
    # Log polling attempt
    await log_step(
        step_name="poll",
        substep=f"attempt_{attempt}",
        context={"progress": progress, "job_id": job_id}
    )
```

### 3. **Seed Step** (`seed_step.py`)

**Current State:**
- Creates initial `weather_forecast_stats` records
- Basic status tracking

**Improvements Needed:**
```python
# Track GFS download status and errors
async def update_gfs_download_stats(db, run_id, status, error=None):
    # Update all miners for this run
    await db.execute("""
        UPDATE weather_forecast_stats
        SET current_forecast_stage = 'gfs_fetch',
            current_forecast_status = :status,
            last_error_message = :error,
            updated_at = NOW()
        WHERE run_id = :run_id
    """, {"run_id": run_id, "status": status, "error": error})
    
    # Log GFS download attempt
    if error:
        await log_step(
            step_name="seed",
            substep="gfs_download",
            status="failed",
            error_json={"error": error, "gfs_init": gfs_init_time}
        )
```

### 4. **Day1 Scoring Step** (`day1_step.py`)

**Current State:**
- Updates `forecast_score_initial` 
- Basic success/failure logging

**Improvements Needed:**
```python
# Record detailed scoring metrics
async def record_day1_scoring(db, run_id, miner_uid, result):
    # Extract component scores
    component_scores = []
    for variable, metrics in result.get("variable_scores", {}).items():
        component_scores.append({
            "run_id": run_id,
            "response_id": response_id,
            "miner_uid": miner_uid,
            "miner_hotkey": miner_hotkey,
            "score_type": "day1",
            "lead_hours": 24,
            "variable_name": variable,
            "rmse": metrics.get("rmse"),
            "acc": metrics.get("acc"),
            "skill_score": metrics.get("skill_score"),
            "weighted_score": metrics.get("weighted_score"),
            "variable_weight": metrics.get("weight"),
            "calculation_duration_ms": scoring_time_ms
        })
    
    # Batch insert component scores
    if component_scores:
        await db.execute_many(
            insert(weather_forecast_component_scores_table),
            component_scores
        )
    
    # Calculate averages for forecast stats
    avg_rmse = np.mean([s["rmse"] for s in component_scores if s["rmse"]])
    avg_acc = np.mean([s["acc"] for s in component_scores if s["acc"]])
    avg_skill = np.mean([s["skill_score"] for s in component_scores if s["skill_score"]])
    
    # Update forecast stats with averages
    await stats_manager.update_forecast_stats(
        avg_rmse=avg_rmse,
        avg_acc=avg_acc,
        avg_skill_score=avg_skill,
        current_forecast_stage="day1_scored",
        current_forecast_status="completed"
    )
```

### 5. **ERA5 Scoring Step** (`era5_step.py`)

**Current State:**
- Updates individual ERA5 scores
- Basic completeness tracking

**Improvements Needed:**
```python
# Track progressive ERA5 scoring
async def update_era5_progress(db, run_id, miner_uid, lead_hours, scores):
    # Record component scores for this timestep
    for variable, metrics in scores.items():
        await db.execute(
            insert(weather_forecast_component_scores_table).values(
                score_type="era5",
                lead_hours=lead_hours,
                valid_time_utc=gfs_init + timedelta(hours=lead_hours),
                # ... component metrics
            ).on_conflict_do_update(...)
        )
    
    # Update current stage in forecast stats
    completed_hours = [24, 48, 72, ...]  # hours with scores
    next_hour = next((h for h in ERA5_HOURS if h not in completed_hours), None)
    
    await stats_manager.update_forecast_stats(
        current_forecast_stage=f"era5_scoring_{lead_hours}h",
        current_forecast_status="scoring" if next_hour else "completed",
        era5_completeness=len(completed_hours) / 10
    )
```

### 6. **Miner Communication** (`miner_communication.py`)

**Current State:**
- Basic success/failure returns
- Limited error detail

**Improvements Needed:**
```python
# Track communication attempts and errors
async def log_communication_attempt(db, miner_uid, miner_hotkey, request_type, result):
    # Update miner_stats communication metrics
    if result.get("success"):
        await db.execute("""
            UPDATE miner_stats
            SET hosting_successes = hosting_successes + 1,
                avg_hosting_latency_ms = 
                    (avg_hosting_latency_ms * hosting_successes + :latency) / 
                    (hosting_successes + 1)
            WHERE miner_uid = :uid
        """, {"uid": miner_uid, "latency": result.get("response_time_ms")})
    else:
        # Track failure
        error_type = categorize_error(result.get("error"))
        await db.execute("""
            UPDATE miner_stats
            SET hosting_failures = hosting_failures + 1,
                common_errors = jsonb_set(
                    COALESCE(common_errors, '{}'::jsonb),
                    ARRAY[:error_type],
                    COALESCE(common_errors->:error_type, '0')::int + 1
                )
            WHERE miner_uid = :uid
        """, {"uid": miner_uid, "error_type": error_type})
    
    # Log to weather_forecast_steps
    await log_step(
        step_name="communication",
        substep=request_type,
        status="success" if result.get("success") else "failed",
        latency_ms=result.get("response_time_ms"),
        error_json={"error": result.get("error")} if not result.get("success") else None
    )
```

### 7. **Orchestrator** (`orchestrator.py`)

**Current State:**
- Basic run status updates
- Limited visibility into orchestration decisions

**Improvements Needed:**
```python
# Track orchestration decisions and timing
async def log_orchestration_decision(db, run_id, decision, reason):
    # Update run-level status
    await db.execute("""
        UPDATE weather_forecast_runs
        SET status = :status,
            error_message = :reason if :status = 'failed' else NULL
        WHERE id = :run_id
    """, {"run_id": run_id, "status": decision, "reason": reason})
    
    # Log orchestration event
    await db.execute("""
        INSERT INTO validator_job_logs (job_id, level, message)
        VALUES (:job_id, 'INFO', :message)
    """, {
        "job_id": current_job_id,
        "message": f"Orchestration: {decision} - {reason}"
    })
```

## Implementation Priority

1. **High Priority (Immediate)**
   - Add error recording to `weather_forecast_stats` in all failure paths
   - Implement retry tracking with `retries_remaining` and `next_scheduled_retry`
   - Add pipeline stage tracking (`current_forecast_stage`, `current_forecast_status`)

2. **Medium Priority (Next Sprint)**
   - Implement component score recording in scoring steps
   - Add communication metrics to `miner_stats`
   - Implement progressive ERA5 tracking

3. **Low Priority (Future)**
   - Add detailed context logging to `weather_forecast_steps`
   - Implement orchestration decision logging
   - Add performance profiling metrics

## Helper Functions to Add

```python
# In weather_stats_manager.py
async def update_pipeline_status(
    self,
    run_id: int,
    miner_uid: int,
    stage: str,
    status: str,
    error: Optional[str] = None,
    retry_info: Optional[Dict] = None
):
    """Update detailed pipeline tracking columns."""
    update_data = {
        "current_forecast_stage": stage,
        "current_forecast_status": status,
        "updated_at": datetime.now(timezone.utc)
    }
    
    if error:
        update_data["last_error_message"] = error
    
    if retry_info:
        update_data["retries_remaining"] = retry_info.get("retries_remaining")
        update_data["next_scheduled_retry"] = retry_info.get("next_retry_time")
    
    stmt = update(weather_forecast_stats_table).where(
        and_(
            weather_forecast_stats_table.c.run_id == run_id,
            weather_forecast_stats_table.c.miner_uid == miner_uid
        )
    ).values(**update_data)
    
    await self.db.execute(stmt)

async def record_component_scores(
    self,
    run_id: int,
    response_id: int,
    miner_uid: int,
    miner_hotkey: str,
    score_type: str,
    lead_hours: int,
    variable_scores: Dict[str, Dict]
):
    """Batch insert component scores for a scoring operation."""
    rows = []
    for variable, metrics in variable_scores.items():
        rows.append({
            "run_id": run_id,
            "response_id": response_id,
            "miner_uid": miner_uid,
            "miner_hotkey": miner_hotkey,
            "score_type": score_type,
            "lead_hours": lead_hours,
            "variable_name": variable,
            "pressure_level": metrics.get("pressure_level"),
            "rmse": metrics.get("rmse"),
            "acc": metrics.get("acc"),
            "skill_score": metrics.get("skill_score"),
            "bias": metrics.get("bias"),
            "mae": metrics.get("mae"),
            "weighted_score": metrics.get("weighted_score"),
            "variable_weight": metrics.get("weight"),
            "calculated_at": datetime.now(timezone.utc)
        })
    
    if rows:
        await self.db.execute_many(
            insert(weather_forecast_component_scores_table),
            rows
        )
```

## Testing Checklist

- [ ] Verify error messages are recorded in `weather_forecast_stats`
- [ ] Check retry counters decrement properly
- [ ] Confirm pipeline stages update correctly
- [ ] Validate component scores are recorded
- [ ] Test aggregation updates `miner_stats`
- [ ] Verify step logging captures all events
- [ ] Check latency measurements are accurate
- [ ] Confirm error categorization works
