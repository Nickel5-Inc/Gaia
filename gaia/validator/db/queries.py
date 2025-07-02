import asyncpg
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ========== Weather Forecast Runs ==========

async def create_weather_forecast_run(
    pool: asyncpg.Pool,
    target_forecast_time_utc: datetime,
    gfs_init_time_utc: datetime,
    gfs_input_metadata: Optional[Dict[str, Any]] = None
) -> int:
    """Creates a new weather forecast run and returns its ID."""
    sql = """
        INSERT INTO weather_forecast_runs 
        (run_initiation_time, target_forecast_time_utc, gfs_init_time_utc, gfs_input_metadata, status)
        VALUES ($1, $2, $3, $4, 'pending')
        RETURNING id
    """
    run_id = await pool.fetchval(
        sql, 
        datetime.now(timezone.utc), 
        target_forecast_time_utc, 
        gfs_init_time_utc, 
        gfs_input_metadata
    )
    logger.info(f"Created weather forecast run {run_id}")
    return run_id


async def update_weather_run_status(
    pool: asyncpg.Pool,
    run_id: int,
    status: str,
    error_message: Optional[str] = None,
    completion_time: Optional[datetime] = None
) -> None:
    """Updates the status of a weather forecast run."""
    sql = """
        UPDATE weather_forecast_runs
        SET status = $1, error_message = $2, completion_time = $3
        WHERE id = $4
    """
    await pool.execute(sql, status, error_message, completion_time, run_id)
    logger.debug(f"Updated run {run_id} status to {status}")


async def get_weather_run_by_id(pool: asyncpg.Pool, run_id: int) -> Optional[Dict[str, Any]]:
    """Retrieves a weather forecast run by ID."""
    sql = """
        SELECT id, run_initiation_time, target_forecast_time_utc, gfs_init_time_utc,
               gfs_input_metadata, status, completion_time, error_message
        FROM weather_forecast_runs
        WHERE id = $1
    """
    row = await pool.fetchrow(sql, run_id)
    return dict(row) if row else None


async def get_pending_weather_runs(pool: asyncpg.Pool, limit: int = 10) -> List[Dict[str, Any]]:
    """Retrieves pending weather forecast runs."""
    sql = """
        SELECT id, run_initiation_time, target_forecast_time_utc, gfs_init_time_utc,
               gfs_input_metadata, status
        FROM weather_forecast_runs
        WHERE status = 'pending'
        ORDER BY run_initiation_time ASC
        LIMIT $1
    """
    rows = await pool.fetch(sql, limit)
    return [dict(row) for row in rows]


# ========== Weather Miner Responses ==========

async def create_weather_miner_response(
    pool: asyncpg.Pool,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    job_id: Optional[str] = None,
    input_hash_miner: Optional[str] = None,
    input_hash_validator: Optional[str] = None
) -> int:
    """Creates a new weather miner response record."""
    sql = """
        INSERT INTO weather_miner_responses 
        (run_id, miner_uid, miner_hotkey, response_time, job_id, 
         input_hash_miner, input_hash_validator, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'received')
        RETURNING id
    """
    response_id = await pool.fetchval(
        sql, run_id, miner_uid, miner_hotkey, datetime.now(timezone.utc),
        job_id, input_hash_miner, input_hash_validator
    )
    logger.debug(f"Created miner response {response_id} for run {run_id}")
    return response_id


async def update_weather_miner_response(
    pool: asyncpg.Pool,
    response_id: int,
    **updates: Any
) -> None:
    """Updates a weather miner response with given fields."""
    if not updates:
        return
    
    # Build dynamic SQL for the fields to update
    fields = []
    values = []
    param_count = 1
    
    for field, value in updates.items():
        fields.append(f"{field} = ${param_count}")
        values.append(value)
        param_count += 1
    
    sql = f"""
        UPDATE weather_miner_responses
        SET {', '.join(fields)}
        WHERE id = ${param_count}
    """
    values.append(response_id)
    
    await pool.execute(sql, *values)
    logger.debug(f"Updated miner response {response_id}")


async def get_weather_miner_responses_by_run(
    pool: asyncpg.Pool, 
    run_id: int
) -> List[Dict[str, Any]]:
    """Retrieves all miner responses for a given run."""
    sql = """
        SELECT id, run_id, miner_uid, miner_hotkey, response_time, job_id,
               kerchunk_json_url, verification_hash_computed, verification_hash_claimed,
               verification_passed, status, error_message, input_hash_miner,
               input_hash_validator, input_hash_match
        FROM weather_miner_responses
        WHERE run_id = $1
        ORDER BY response_time ASC
    """
    rows = await pool.fetch(sql, run_id)
    return [dict(row) for row in rows]


async def get_verified_weather_responses(
    pool: asyncpg.Pool, 
    run_id: int
) -> List[Dict[str, Any]]:
    """Retrieves verified miner responses for scoring."""
    sql = """
        SELECT id, run_id, miner_uid, miner_hotkey, verification_hash_computed,
               kerchunk_json_url, target_netcdf_url_template
        FROM weather_miner_responses
        WHERE run_id = $1 AND verification_passed = true AND status = 'verified'
        ORDER BY response_time ASC
    """
    rows = await pool.fetch(sql, run_id)
    return [dict(row) for row in rows]


# ========== Weather Scoring ==========

async def create_weather_miner_score(
    pool: asyncpg.Pool,
    response_id: int,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    score_type: str,
    score: Optional[float] = None,
    metrics: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None,
    lead_hours: Optional[int] = None,
    variable_level: Optional[str] = None,
    valid_time_utc: Optional[datetime] = None
) -> int:
    """Creates a weather miner score record."""
    sql = """
        INSERT INTO weather_miner_scores 
        (response_id, run_id, miner_uid, miner_hotkey, score_type, calculation_time,
         score, metrics, error_message, lead_hours, variable_level, valid_time_utc)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING id
    """
    score_id = await pool.fetchval(
        sql, response_id, run_id, miner_uid, miner_hotkey, score_type,
        datetime.now(timezone.utc), score, metrics, error_message,
        lead_hours, variable_level, valid_time_utc
    )
    logger.debug(f"Created score {score_id} for response {response_id}")
    return score_id


async def get_weather_scores_by_run(
    pool: asyncpg.Pool, 
    run_id: int, 
    score_type: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Retrieves weather scores for a run, optionally filtered by score type."""
    base_sql = """
        SELECT id, response_id, run_id, miner_uid, miner_hotkey, score_type,
               calculation_time, score, metrics, error_message, lead_hours,
               variable_level, valid_time_utc
        FROM weather_miner_scores
        WHERE run_id = $1
    """
    
    if score_type:
        sql = base_sql + " AND score_type = $2 ORDER BY calculation_time ASC"
        rows = await pool.fetch(sql, run_id, score_type)
    else:
        sql = base_sql + " ORDER BY calculation_time ASC"
        rows = await pool.fetch(sql, run_id)
    
    return [dict(row) for row in rows]


# ========== Weather Scoring Jobs (for restart resilience) ==========

async def create_weather_scoring_job(
    pool: asyncpg.Pool,
    run_id: int,
    score_type: str
) -> int:
    """Creates a weather scoring job for restart resilience."""
    sql = """
        INSERT INTO weather_scoring_jobs (run_id, score_type, status)
        VALUES ($1, $2, 'queued')
        ON CONFLICT (run_id, score_type) DO NOTHING
        RETURNING id
    """
    job_id = await pool.fetchval(sql, run_id, score_type)
    if job_id:
        logger.debug(f"Created scoring job {job_id} for run {run_id}, type {score_type}")
    return job_id


async def get_pending_scoring_jobs(
    pool: asyncpg.Pool, 
    score_type: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Retrieves pending scoring jobs."""
    base_sql = """
        SELECT id, run_id, score_type, status, created_at
        FROM weather_scoring_jobs
        WHERE status = 'queued'
    """
    
    if score_type:
        sql = base_sql + " AND score_type = $1 ORDER BY created_at ASC"
        rows = await pool.fetch(sql, score_type)
    else:
        sql = base_sql + " ORDER BY created_at ASC"
        rows = await pool.fetch(sql)
    
    return [dict(row) for row in rows]


async def update_scoring_job_status(
    pool: asyncpg.Pool,
    job_id: int,
    status: str,
    error_message: Optional[str] = None
) -> None:
    """Updates a scoring job status."""
    now = datetime.now(timezone.utc)
    
    if status == 'running':
        sql = """
            UPDATE weather_scoring_jobs
            SET status = $1, started_at = $2
            WHERE id = $3
        """
        await pool.execute(sql, status, now, job_id)
    elif status in ('completed', 'failed'):
        sql = """
            UPDATE weather_scoring_jobs
            SET status = $1, completed_at = $2, error_message = $3
            WHERE id = $4
        """
        await pool.execute(sql, status, now, error_message, job_id)
    else:
        sql = """
            UPDATE weather_scoring_jobs
            SET status = $1, error_message = $2
            WHERE id = $3
        """
        await pool.execute(sql, status, error_message, job_id)


# ========== Weather Ensemble Forecasts ==========

async def create_weather_ensemble_forecast(
    pool: asyncpg.Pool,
    forecast_run_id: int,
    ensemble_path: Optional[str] = None,
    ensemble_kerchunk_path: Optional[str] = None
) -> int:
    """Creates a weather ensemble forecast record."""
    sql = """
        INSERT INTO weather_ensemble_forecasts 
        (forecast_run_id, ensemble_path, ensemble_kerchunk_path, status)
        VALUES ($1, $2, $3, 'pending')
        RETURNING id
    """
    ensemble_id = await pool.fetchval(sql, forecast_run_id, ensemble_path, ensemble_kerchunk_path)
    logger.debug(f"Created ensemble forecast {ensemble_id} for run {forecast_run_id}")
    return ensemble_id


async def update_ensemble_forecast(
    pool: asyncpg.Pool,
    ensemble_id: int,
    **updates: Any
) -> None:
    """Updates an ensemble forecast with given fields."""
    if not updates:
        return
    
    # Build dynamic SQL for the fields to update
    fields = []
    values = []
    param_count = 1
    
    for field, value in updates.items():
        fields.append(f"{field} = ${param_count}")
        values.append(value)
        param_count += 1
    
    sql = f"""
        UPDATE weather_ensemble_forecasts
        SET {', '.join(fields)}
        WHERE id = ${param_count}
    """
    values.append(ensemble_id)
    
    await pool.execute(sql, *values)
    logger.debug(f"Updated ensemble forecast {ensemble_id}")


# ========== General Cleanup Operations ==========

async def cleanup_old_weather_runs(
    pool: asyncpg.Pool,
    retention_days: int
) -> int:
    """Removes old weather forecast runs and related data."""
    cutoff_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff_date = cutoff_date.replace(day=cutoff_date.day - retention_days)
    
    sql = """
        DELETE FROM weather_forecast_runs
        WHERE run_initiation_time < $1
    """
    result = await pool.execute(sql, cutoff_date)
    deleted_count = int(result.split()[-1]) if result.startswith('DELETE') else 0
    
    if deleted_count > 0:
        logger.info(f"Cleaned up {deleted_count} old weather runs older than {retention_days} days")
    
    return deleted_count


# ========== Health and Monitoring ==========

async def get_database_stats(pool: asyncpg.Pool) -> Dict[str, Any]:
    """Retrieves basic database statistics for monitoring."""
    stats = {}
    
    # Weather runs stats
    sql = """
        SELECT 
            COUNT(*) as total_runs,
            COUNT(*) FILTER (WHERE status = 'pending') as pending_runs,
            COUNT(*) FILTER (WHERE status = 'completed') as completed_runs,
            COUNT(*) FILTER (WHERE status = 'failed') as failed_runs
        FROM weather_forecast_runs
        WHERE run_initiation_time >= NOW() - INTERVAL '7 days'
    """
    row = await pool.fetchrow(sql)
    stats['weather_runs_last_7_days'] = dict(row)
    
    # Miner response stats
    sql = """
        SELECT 
            COUNT(*) as total_responses,
            COUNT(*) FILTER (WHERE verification_passed = true) as verified_responses,
            COUNT(DISTINCT miner_hotkey) as unique_miners
        FROM weather_miner_responses
        WHERE response_time >= NOW() - INTERVAL '7 days'
    """
    row = await pool.fetchrow(sql)
    stats['miner_responses_last_7_days'] = dict(row)
    
    return stats