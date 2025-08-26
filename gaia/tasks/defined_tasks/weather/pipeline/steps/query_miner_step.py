"""
Query individual miner to initiate forecast inference.
This step runs in parallel for each miner after GFS data is ready.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline.miner_communication import query_miner_for_weather
from gaia.tasks.defined_tasks.weather.schemas.weather_outputs import WeatherTaskStatus
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from gaia.tasks.defined_tasks.weather.pipeline.steps.step_logger import log_failure, log_success

from gaia.utils.custom_logger import get_logger
logger = get_logger(__name__)


async def run_query_miner_job(
    db: ValidatorDatabaseManager,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    validator_hotkey: str,
    validator: Optional[Any] = None,
) -> bool:
    """
    Query a single miner to initiate forecast inference.
    
    This job:
    1. Sends initiate-fetch request to miner
    2. Records response in weather_miner_responses
    3. Enqueues polling job if accepted
    
    Returns:
        True if successful (miner accepted or explicitly rejected)
        False if error occurred (will retry)
    """
    try:
        # Get run details
        run = await db.fetch_one(
            """
            SELECT id, gfs_init_time_utc, target_forecast_time_utc, status
            FROM weather_forecast_runs
            WHERE id = :run_id
            """,
            {"run_id": run_id}
        )
        
        if not run:
            logger.error(f"[Run {run_id}] Run not found")
            return True  # Don't retry for missing run
            
        gfs_init = run["gfs_init_time_utc"]
        gfs_t_minus_6 = gfs_init - timedelta(hours=6)
        
        logger.info(f"[Run {run_id}] Querying miner {miner_hotkey[:8]}...{miner_hotkey[-8:]} (UID {miner_uid})")
        
        # CRITICAL: Validate that node_table hotkey matches job parameter
        # If they don't match, it indicates a serious issue in metagraph sync or job creation
        node_check = await db.fetch_one(
            "SELECT hotkey, last_updated FROM node_table WHERE uid = :uid",
            {"uid": miner_uid}
        )
        if node_check and node_check["hotkey"] != miner_hotkey:
            logger.error(
                f"[METAGRAPH SYNC ISSUE] Run {run_id}, Miner UID {miner_uid}: "
                f"node_table has {node_check['hotkey'][:8]}...{node_check['hotkey'][-8:]} "
                f"but job parameter is {miner_hotkey[:8]}...{miner_hotkey[-8:]}. "
                f"node_table last_updated: {node_check.get('last_updated')}. "
                f"This indicates either stale metagraph data or incorrect job creation!"
            )
        elif not node_check:
            logger.error(f"[METAGRAPH SYNC ISSUE] Miner UID {miner_uid} not found in node_table!")
        
        # Check if we already have a response for this miner
        existing = await db.fetch_one(
            """
            SELECT id, status, job_id, miner_hotkey FROM weather_miner_responses
            WHERE run_id = :run_id AND miner_uid = :miner_uid
            """,
            {"run_id": run_id, "miner_uid": miner_uid}
        )
        
        if existing:
            # CRITICAL: Validate miner workflow isolation in existing responses
            if existing.get("miner_hotkey") != miner_hotkey:
                logger.error(
                    f"[ISOLATION VIOLATION] Existing response for run {run_id}, miner UID {miner_uid} "
                    f"has hotkey {existing.get('miner_hotkey', 'NULL')[:8]}... but job specifies "
                    f"{miner_hotkey[:8]}... Potential crossover detected."
                )
                return False
                
            if existing["status"] not in ["created", "failed"]:
                logger.info(
                    f"[Run {run_id}] Miner {miner_uid} already has response with status {existing['status']} "
                    f"(job_id: {existing.get('job_id', 'N/A')}), skipping duplicate query"
                )
                return True
            
        # Query the miner using the new communication module
        try:
            result = await query_miner_for_weather(
                validator,
                miner_hotkey,
                forecast_start_time=gfs_init,
                previous_step_time=gfs_t_minus_6,
                validator_hotkey=validator_hotkey,
                db_manager=db,  # Pass the db instance from the worker
            )
            
            # CRITICAL: Verify the responding miner's hotkey matches what we expected
            if result and result.get("success") and result.get("data", {}).get("job_id"):
                job_id = result["data"]["job_id"]
                
                # The job_id should be deterministically generated using the actual miner's hotkey
                # If the responding miner has a different hotkey, the job_id pattern won't match
                logger.info(
                    f"[Run {run_id}] Received job_id {job_id} from miner at UID {miner_uid}, "
                    f"expected hotkey {miner_hotkey[:8]}...{miner_hotkey[-8:]}"
                )
                
                # Additional verification: check if this job_id was created by a different miner
                # by looking at existing responses with this job_id
                job_id_check = await db.fetch_one(
                    """
                    SELECT miner_uid, miner_hotkey FROM weather_miner_responses 
                    WHERE job_id = :job_id AND miner_uid != :expected_uid
                    LIMIT 1
                    """,
                    {"job_id": job_id, "expected_uid": miner_uid}
                )
                
                if job_id_check:
                    logger.error(
                        f"[HOTKEY VERIFICATION FAILED] Miner UID {miner_uid} (hotkey {miner_hotkey[:8]}...{miner_hotkey[-8:]}) "
                        f"returned job_id {job_id} that belongs to different miner UID {job_id_check['miner_uid']} "
                        f"(hotkey {job_id_check['miner_hotkey'][:8]}...{job_id_check['miner_hotkey'][-8:]}). "
                        f"This indicates same IP:port serving multiple miners - marking as failed."
                    )
                    
                    # Record this as a failed response due to hotkey mismatch
                    await _record_miner_response(
                        db, run_id, miner_uid, miner_hotkey,
                        status="failed",
                        error_message=f"Hotkey verification failed - job_id {job_id} belongs to different miner"
                    )
                    return True  # Don't retry, this is a permanent configuration issue
            
            if not result:
                logger.warning(
                    f"[Run {run_id}] No response from miner {miner_hotkey[:8]} (UID {miner_uid}). "
                    f"Miner may be offline or has not registered IP/port via fiber-post-ip process."
                )
                # Record failure
                await _record_miner_response(
                    db, run_id, miner_uid, miner_hotkey,
                    status="failed",
                    error_message="No response - miner offline or IP/port not registered"
                )
                # Update pipeline status
                stats = WeatherStatsManager(db, validator_hotkey)
                await stats.update_pipeline_status(
                    run_id=run_id,
                    miner_uid=miner_uid,
                    stage="inference_requested",
                    status="failed",
                    error="No response - miner offline or IP/port not registered"
                )
                await log_failure(
                    db,
                    run_id=run_id,
                    miner_uid=miner_uid,
                    miner_hotkey=miner_hotkey,
                    step_name="query",
                    substep="initiate_fetch",
                    error_json={"error": "No response - miner offline or IP/port not registered. Please ensure miner has completed fiber-post-ip process."}
                )
                return True  # Don't retry connection failures
            
            # Check if request was successful
            if not result.get("success"):
                error_msg = result.get("error", "Unknown error")
                status_code = result.get("status_code")
                
                logger.warning(
                    f"[Run {run_id}] Failed to query miner {miner_hotkey[:8]}: {error_msg}"
                    f" (HTTP {status_code})" if status_code else ""
                )
                
                # Record failure with details
                await _record_miner_response(
                    db, run_id, miner_uid, miner_hotkey,
                    status="failed",
                    error_message=f"{error_msg} (HTTP {status_code})" if status_code else error_msg
                )
                
                # Update pipeline status
                stats = WeatherStatsManager(db, validator_hotkey)
                await stats.update_pipeline_status(
                    run_id=run_id,
                    miner_uid=miner_uid,
                    stage="inference_requested",
                    status="error",
                    error=f"{error_msg} (HTTP {status_code})" if status_code else error_msg
                )
                await log_failure(
                    db,
                    run_id=run_id,
                    miner_uid=miner_uid,
                    miner_hotkey=miner_hotkey,
                    step_name="query",
                    substep="initiate_fetch",
                    error_json={"error": error_msg, "status_code": status_code},
                    latency_ms=int(result.get("response_time", 0) * 1000) if result else None
                )
                
                # Retry on network errors (no status code means connection failed)
                if not status_code:
                    return False  # Retry network errors
                return True  # Don't retry HTTP errors
            
            # Parse successful response
            miner_response = result.get("data", {})
            
            # Fast-path: Miner already completed inference, skip directly to scoring
            if (
                isinstance(miner_response, dict)
                and miner_response.get("status") in [
                    WeatherTaskStatus.FETCH_COMPLETED,
                    "completed",
                    "forecast_ready",
                ]
                and miner_response.get("job_id")
            ):
                job_id = miner_response.get("job_id")
                zarr_url = miner_response.get("zarr_store_url") or miner_response.get("kerchunk_json_url")
                claimed_hash = miner_response.get("verification_hash") or miner_response.get("manifest_hash")
                logger.info(
                    f"[Run {run_id}] Miner {miner_hotkey[:8]} completed on initial query. Scheduling Day1 scoring immediately.\n"
                    f"  Job ID: {job_id}\n  Zarr: {zarr_url}\n  Hash: {claimed_hash}"
                )
                # Record forecast-ready status and metadata
                await db.execute(
                    """
                    INSERT INTO weather_miner_responses (run_id, miner_uid, miner_hotkey, response_time, job_id, status, kerchunk_json_url, verification_hash_claimed, job_accepted_at)
                    VALUES (:run_id, :miner_uid, :miner_hotkey, NOW(), :job_id, 'forecast_ready', :url, :hash, NOW())
                    ON CONFLICT (run_id, miner_uid) DO UPDATE SET
                        status = EXCLUDED.status,
                        job_id = EXCLUDED.job_id,
                        kerchunk_json_url = COALESCE(EXCLUDED.kerchunk_json_url, weather_miner_responses.kerchunk_json_url),
                        verification_hash_claimed = COALESCE(EXCLUDED.verification_hash_claimed, weather_miner_responses.verification_hash_claimed),
                        job_accepted_at = COALESCE(weather_miner_responses.job_accepted_at, NOW()),
                        response_time = COALESCE(weather_miner_responses.response_time, NOW())
                    """,
                    {
                        "run_id": run_id,
                        "miner_uid": miner_uid,
                        "miner_hotkey": miner_hotkey,
                        "job_id": job_id,
                        "url": zarr_url,
                        "hash": claimed_hash,
                    },
                )
                # Enqueue Day1 scoring singleton immediately
                singleton_key = f"day1_score_run_{run_id}_miner_{miner_uid}"
                job_id_created = await db.enqueue_singleton_job(
                    singleton_key=singleton_key,
                    job_type="weather.day1",
                    payload={
                        "run_id": run_id,
                        "miner_uid": miner_uid,
                        "miner_hotkey": miner_hotkey,
                        # We need current response_id; fetch latest
                        # It will be looked up in day1 step run_item
                        "response_id": (await db.fetch_one(
                            "SELECT id FROM weather_miner_responses WHERE run_id = :rid AND miner_uid = :uid ORDER BY id DESC LIMIT 1",
                            {"rid": run_id, "uid": miner_uid},
                        ))["id"],
                        "job_id": job_id,
                    },
                    priority=60,
                    run_id=run_id,
                    miner_uid=miner_uid,
                )
                if job_id_created:
                    logger.info(f"[Run {run_id}] ✓ Created Day1 singleton job {job_id_created} for miner {miner_uid} (fast-path)")
                return True

            # Check if miner accepted
            if (
                isinstance(miner_response, dict)
                and miner_response.get("status") == WeatherTaskStatus.FETCH_ACCEPTED
                and miner_response.get("job_id")
            ):
                # CRITICAL: Log where this miner hotkey came from for debugging
                logger.info(
                    f"[Run {run_id}] Recording response for miner UID {miner_uid} "
                    f"with hotkey {miner_hotkey[:8]}...{miner_hotkey[-8:]} "
                    f"(source: job parameter from node_table)"
                )
                
                # Record acceptance with timing info
                response_id = await _record_miner_response(
                    db, run_id, miner_uid, miner_hotkey,
                    status="fetch_initiated",
                    job_id=miner_response.get("job_id"),
                    response_time_ms=int(result.get("response_time", 0) * 1000),
                    job_accepted_at=datetime.now(timezone.utc),  # Track job acceptance time
                )
                

                
                logger.info(
                    f"[Run {run_id}] ✓ Miner {miner_hotkey[:8]} accepted"
                    f"\n  Job ID: {miner_response.get('job_id')}"
                    f"\n  Response time: {result.get('response_time', 0):.2f}s"
                )
                
                # The miner should immediately start inference, so update status
                await db.execute(
                    """
                    UPDATE weather_miner_responses
                    SET status = 'inference_running', inference_started_at = NOW()
                    WHERE id = :id
                    """,
                    {"id": response_id}
                )
                
                # Update pipeline status to show inference started
                stats = WeatherStatsManager(db, validator_hotkey)
                await stats.update_pipeline_status(
                    run_id=run_id,
                    miner_uid=miner_uid,
                    stage="inference_running",
                    status="in_progress"
                )
                await log_success(
                    db,
                    run_id=run_id,
                    miner_uid=miner_uid,
                    miner_hotkey=miner_hotkey,
                    step_name="query",
                    substep="initiate_fetch",
                    latency_ms=int(result.get("response_time", 0) * 1000)
                )
                
                # Enqueue polling job to check when inference is complete
                await db.enqueue_validator_job(
                    job_type="weather.poll_miner",
                    payload={
                        "run_id": run_id,
                        "miner_uid": miner_uid,
                        "miner_hotkey": miner_hotkey,
                        "response_id": response_id,
                        "job_id": miner_response.get("job_id"),
                        "attempt": 1,
                    },
                    priority=70,
                    run_id=run_id,
                    miner_uid=miner_uid,
                    response_id=response_id,
                    scheduled_at=datetime.now(timezone.utc) + timedelta(minutes=5),  # First poll in 5 minutes
                )
                
                return True
                
            else:
                # Miner rejected or returned unexpected status
                status = miner_response.get("status", "unknown")
                message = miner_response.get("message", "No message")
                
                # Check if this is a hotkey verification failure
                if status == WeatherTaskStatus.FETCH_REJECTED and "hotkey verification failed" in message.lower():
                    expected_hk = miner_response.get("expected_hotkey", "unknown")
                    actual_hk = miner_response.get("actual_hotkey", "unknown")
                    logger.error(
                        f"[Run {run_id}] HOTKEY VERIFICATION FAILED for UID {miner_uid}:"
                        f"\n  Expected: {expected_hk[:8]}...{expected_hk[-8:] if len(expected_hk) > 8 else expected_hk}"
                        f"\n  Actual: {actual_hk[:8]}...{actual_hk[-8:] if len(actual_hk) > 8 else actual_hk}"
                        f"\n  This UID likely points to a stale/incorrect miner registration."
                    )
                    
                    # Mark this miner UID as having stale registration
                    await _record_miner_response(
                        db, run_id, miner_uid, miner_hotkey,
                        status="failed",
                        error_message=f"Hotkey verification failed - UID {miner_uid} has stale registration"
                    )
                    return True  # Don't retry, this is a permanent configuration issue
                else:
                    logger.warning(
                        f"[Run {run_id}] Miner {miner_hotkey[:8]} rejected or unexpected response"
                        f"\n  Status: {status}"
                        f"\n  Message: {message}"
                    )
                    
                    await _record_miner_response(
                        db, run_id, miner_uid, miner_hotkey,
                        status="failed",
                        error_message=f"Status: {status}, Message: {message}"
                    )
                    return True  # Don't retry rejections
                
        except Exception as e:
            logger.error(f"[Run {run_id}] Error querying miner {miner_hotkey[:8]}: {e}")
            # This is a real error, return False to retry
            return False
            
    except Exception as e:
        logger.error(f"[Run {run_id}] Query miner job failed: {e}", exc_info=True)
        return False


async def _record_miner_response(
    db: ValidatorDatabaseManager,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    status: str,
    job_id: Optional[str] = None,
    error_message: Optional[str] = None,
    response_time_ms: Optional[int] = None,
    job_accepted_at: Optional[datetime] = None,
) -> int:
    """Record or update miner response in database."""
    
    # CRITICAL: Check for existing response with different hotkey (data corruption detection)
    existing_check = await db.fetch_one(
        "SELECT miner_hotkey, job_id FROM weather_miner_responses WHERE run_id = :run_id AND miner_uid = :uid",
        {"run_id": run_id, "uid": miner_uid}
    )
    
    if existing_check and existing_check["miner_hotkey"] != miner_hotkey:
        logger.error(
            f"[DATA CORRUPTION] Run {run_id}, UID {miner_uid}: "
            f"Attempting to record response with hotkey {miner_hotkey[:8]}...{miner_hotkey[-8:]} "
            f"but existing record has {existing_check['miner_hotkey'][:8]}...{existing_check['miner_hotkey'][-8:]} "
            f"(existing job_id: {existing_check.get('job_id', 'N/A')}). "
            f"This indicates serious miner workflow crossover!"
        )
        # Don't update the hotkey if it would cause corruption
        miner_hotkey = existing_check["miner_hotkey"]
        logger.warning(f"[DATA CORRUPTION] Using existing hotkey to prevent further corruption")
    
    result = await db.fetch_one(
        """
        INSERT INTO weather_miner_responses
        (run_id, miner_uid, miner_hotkey, response_time, status, job_id, error_message, job_accepted_at)
        VALUES (:run_id, :uid, :hk, :resp_time, :status, :job_id, :error, :job_accepted)
        ON CONFLICT (run_id, miner_uid) DO UPDATE SET
        response_time = EXCLUDED.response_time,
        status = EXCLUDED.status,
        job_id = COALESCE(EXCLUDED.job_id, weather_miner_responses.job_id),
        error_message = EXCLUDED.error_message,
        job_accepted_at = COALESCE(EXCLUDED.job_accepted_at, weather_miner_responses.job_accepted_at)
        RETURNING id
        """,
        {
            "run_id": run_id,
            "uid": miner_uid,
            "hk": miner_hotkey,
            "resp_time": datetime.now(timezone.utc),
            "status": status,
            "job_id": job_id,
            "error": error_message,
            "job_accepted": job_accepted_at,
        }
    )
    return result["id"] if result else 0
