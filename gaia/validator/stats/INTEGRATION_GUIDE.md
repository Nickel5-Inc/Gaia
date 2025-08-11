# Weather Stats Integration Guide

This guide shows how to integrate the weather stats tracking system into the existing validator workflow with minimal changes and zero duplicate calculations.

## Key Principles

1. **Zero Duplicate Calculations** - We only capture data that's already been calculated
2. **Minimal Performance Impact** - Hooks are async and non-blocking
3. **Easy to Integrate** - Just add a few lines at existing database write points
4. **Memory Efficient** - Automatic cache management and batch processing

## Integration Points

### 1. Validator Initialization

In `GaiaValidator.__init__` or after database initialization:

```python
# Add import
from gaia.validator.stats.weather_stats_hooks import WeatherStatsHooks

# After database initialization
self.weather_stats_hooks = WeatherStatsHooks(
    database_manager=self.database_manager,
    validator_hotkey=self.validator_wallet.hotkey.ss58_address
)
```

### 2. When Miner Responses Are Received

In `weather_task.py` (around line 2196), after inserting response:

```python
# After existing code:
await task_instance.db_manager.execute(insert_resp_query, {...})

# Add hook:
if hasattr(validator, 'weather_stats_hooks'):
    await validator.weather_stats_hooks.on_response_inserted(
        run_id=run_id,
        miner_uid=uid_for_hotkey,
        miner_hotkey=miner_hotkey,
        job_id=job_id,
        status="accepted"
    )
```

### 3. After Verification Completes

In `weather_logic.py` (around line 1239), after updating verification status:

```python
# After UPDATE weather_miner_responses SET verification_passed...
await task_instance.db_manager.execute(
    "UPDATE weather_miner_responses SET verification_passed = :verified...",
    {...}
)

# Add hook:
if hasattr(validator, 'weather_stats_hooks'):
    await validator.weather_stats_hooks.on_verification_complete(
        run_id=run_id,
        response_id=response_id,
        miner_uid=miner_uid,
        miner_hotkey=miner_hotkey,
        verification_passed=verification_passed,
        error_msg=error_message,
        fetch_latency_ms=fetch_time_ms  # if tracked
    )
```

### 4. After ERA5 Scores Are Inserted

In `weather_logic.py` (around line 2396), after batch inserting scores:

```python
# After inserting to weather_miner_scores
for metric_record in all_metrics_for_db:
    await task_instance.db_manager.execute(insert_query, metric_record)

# Add hook:
if hasattr(validator, 'weather_stats_hooks') and all_metrics_for_db:
    scores_batch = [
        {
            "miner_uid": mr["miner_uid"],
            "miner_hotkey": mr["miner_hotkey"],
            "score_type": mr["score_type"],
            "score": mr["score"],
            "lead_hours": mr.get("lead_hours")
        }
        for mr in all_metrics_for_db
    ]
    await validator.weather_stats_hooks.on_scores_inserted(
        run_id=run_id,
        scores_batch=scores_batch
    )
```

### 5. After Day 1 Scoring

In `weather_workers.py` (around line 2090), after Day 1 scores:

```python
# After inserting Day1 scores
if db_update_tasks:
    await asyncio.gather(*db_update_tasks)

# Add hook:
if hasattr(validator, 'weather_stats_hooks') and day1_scores:
    scores_batch = [
        {
            "miner_uid": score["miner_uid"],
            "miner_hotkey": score["miner_hotkey"],
            "score_type": "gfs_rmse",
            "score": score["score"]
        }
        for score in day1_scores
    ]
    await validator.weather_stats_hooks.on_scores_inserted(
        run_id=run_id,
        scores_batch=scores_batch
    )
```

### 6. When Run Completes

In `weather_workers.py` (around line 2605), after updating run status:

```python
# After UPDATE weather_forecast_runs SET final_scoring_attempted_time...
await task_instance.db_manager.execute(
    "UPDATE weather_forecast_runs SET final_scoring_attempted_time = :now...",
    {...}
)

# Add hook:
if hasattr(validator, 'weather_stats_hooks'):
    await validator.weather_stats_hooks.on_run_scoring_complete(run_id)
```

### 7. Pass Hooks to Weather Task

When creating the weather task:

```python
# Pass validator reference to weather task
weather_task.validator = self
```

Or alternatively, access hooks through validator:

```python
# In weather task code
if hasattr(self.validator, 'weather_stats_hooks'):
    await self.validator.weather_stats_hooks.on_response_inserted(...)
```

## Backfilling Existing Data

To populate stats from existing `weather_miner_scores`:

```python
async def backfill_weather_stats(validator):
    """Backfill weather stats from existing data."""
    if not hasattr(validator, 'weather_stats_hooks'):
        logger.error("Weather stats hooks not initialized")
        return
    
    # Get recent runs
    query = """
        SELECT id FROM weather_forecast_runs 
        WHERE run_initiation_time > NOW() - INTERVAL '7 days'
        ORDER BY run_initiation_time DESC
    """
    runs = await validator.database_manager.fetch_all(query)
    
    for run in runs:
        logger.info(f"Backfilling stats for run {run['id']}")
        await validator.weather_stats_hooks.extract_scores_from_existing_data(
            run_id=run['id'],
            limit=5000
        )
        await asyncio.sleep(1)  # Don't overwhelm database
    
    logger.info("Backfill complete")
```

## Memory Management

Periodically clear the cache:

```python
# After each run or daily
if hasattr(self, 'weather_stats_hooks'):
    self.weather_stats_hooks.clear_cache()
```

## Benefits

1. **Efficient** - Piggybacks on existing database operations
2. **Non-invasive** - Errors in hooks don't affect main workflow  
3. **Automatic** - Rankings and aggregations happen automatically
4. **Scalable** - Batch processing and cache management prevent memory issues
5. **Complete** - Tracks all aspects of weather forecasting performance

## Data Flow

```
Miner Response → Insert to DB → Hook: on_response_inserted()
     ↓
Verification → Update DB → Hook: on_verification_complete()
     ↓
Day 1 Scoring → Insert Scores → Hook: on_scores_inserted()
     ↓
ERA5 Scoring → Insert Scores → Hook: on_scores_inserted()
     ↓
Run Complete → Update Status → Hook: on_run_scoring_complete()
     ↓
Stats Aggregated → Rankings Calculated → Ready for API
```

## API Data Access

The `MinerScoreSender` automatically pulls from the new stats tables:

```python
# In send_to_gaia(), weather stats are now included:
weather_data = await self.fetch_weather_stats(miner["uid"], miner["hotkey"])
payload = {
    "minerHotKey": miner["hotkey"],
    "minerColdKey": miner["coldkey"],
    "minerUID": miner["uid"],
    "weatherStats": weather_data["stats"],
    "weatherRecentRuns": weather_data["recent_runs"]
}
```

Stats are sent hourly to the web server automatically.
