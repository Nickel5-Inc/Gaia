"""
Weather Validation Lifecycle Orchestrator.

This module contains the main orchestration logic for weather validation runs.
It coordinates database operations, miner interactions, and compute tasks
through the IO-Engine.
"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from gaia.validator.db import queries as db
from gaia.validator.db.connection import get_db_pool
from gaia.validator.utils.ipc_types import WorkUnit

logger = logging.getLogger(__name__)


async def orchestrate_full_weather_run(io_engine) -> None:
    """
    Orchestrates a complete weather validation run.
    
    This is the main entry point that coordinates all the steps needed
    for a weather validation cycle.
    
    Args:
        io_engine: The IOEngine instance for database and compute access
    """
    run_id = None
    pool = await get_db_pool()
    
    try:
        # Step 1: Create a new weather forecast run
        logger.info("Creating new weather forecast run")
        
        # Calculate GFS times (T0 and T-6)
        now = datetime.now(timezone.utc)
        # Typical GFS runs are at 00, 06, 12, 18 UTC
        # Round down to the most recent 6-hour boundary
        gfs_hour = (now.hour // 6) * 6
        gfs_init_time = now.replace(hour=gfs_hour, minute=0, second=0, microsecond=0)
        
        # Target forecast time is typically 240 hours (10 days) ahead
        target_forecast_time = gfs_init_time + timedelta(hours=240)
        
        # Create metadata
        gfs_metadata = {
            "model": "GFS",
            "resolution": 0.25,
            "variables": ["2t", "10u", "10v", "msl", "t", "u", "v", "q", "z"],
            "init_time": gfs_init_time.isoformat(),
            "forecast_horizon_hours": 240
        }
        
        run_id = await db.create_weather_forecast_run(
            pool=pool,
            target_forecast_time_utc=target_forecast_time,
            gfs_init_time_utc=gfs_init_time,
            gfs_input_metadata=gfs_metadata
        )
        
        logger.info(f"Created weather run {run_id} for GFS init time {gfs_init_time}")
        
        # Step 2: Compute the validator's reference hash
        logger.info(f"Computing validator reference hash for run {run_id}")
        
        validator_hash = await _compute_validator_hash(io_engine, gfs_init_time)
        
        if not validator_hash:
            await db.update_weather_run_status(
                pool, run_id, "failed", 
                error_message="Failed to compute validator reference hash"
            )
            return
        
        logger.info(f"Validator hash computed: {validator_hash[:12]}...")
        
        # Step 3: Query miners for forecasts
        logger.info(f"Querying miners for weather forecasts (run {run_id})")
        
        miner_responses = await _query_miners_for_forecasts(
            io_engine, run_id, gfs_init_time, gfs_metadata, validator_hash
        )
        
        if not miner_responses:
            await db.update_weather_run_status(
                pool, run_id, "failed",
                error_message="No miner responses received"
            )
            return
        
        logger.info(f"Received {len(miner_responses)} miner responses for run {run_id}")
        
        # Step 4: Verify miner hashes and forecasts
        logger.info(f"Verifying miner responses for run {run_id}")
        
        verified_responses = await _verify_miner_responses(
            io_engine, run_id, miner_responses, validator_hash
        )
        
        logger.info(f"Verified {len(verified_responses)} miner responses for run {run_id}")
        
        # Step 5: Queue scoring jobs for later processing
        logger.info(f"Queueing scoring jobs for run {run_id}")
        
        await _queue_scoring_jobs(pool, run_id, verified_responses)
        
        # Step 6: Mark run as completed (scoring will happen separately)
        await db.update_weather_run_status(
            pool, run_id, "completed",
            completion_time=datetime.now(timezone.utc)
        )
        
        logger.info(f"Weather run {run_id} orchestration completed successfully")
        
    except Exception as e:
        logger.error(f"Error in weather run orchestration: {e}", exc_info=True)
        if run_id:
            try:
                await db.update_weather_run_status(
                    pool, run_id, "failed", 
                    error_message=f"Orchestration error: {e}"
                )
            except Exception as db_err:
                logger.error(f"Failed to update run status after error: {db_err}")
        raise


async def _compute_validator_hash(io_engine, gfs_init_time: datetime) -> Optional[str]:
    """
    Computes the validator's reference hash using the compute pool.
    
    Args:
        io_engine: IOEngine instance
        gfs_init_time: GFS initialization time
        
    Returns:
        The computed validator hash or None if failed
    """
    try:
        work_unit = WorkUnit(
            job_id=str(uuid.uuid4()),
            task_name="weather.hash.gfs_compute",
            payload={
                "gfs_t0_run_time_iso": gfs_init_time.isoformat(),
                "cache_dir": io_engine.config.WEATHER.GFS_CACHE_DIR
            },
            timeout_seconds=300  # 5 minutes for hash computation
        )
        
        result = await io_engine.dispatch_and_wait(work_unit)
        
        if result.success:
            return result.result
        else:
            logger.error(f"Validator hash computation failed: {result.error}")
            return None
            
    except Exception as e:
        logger.error(f"Error dispatching validator hash computation: {e}")
        return None


async def _query_miners_for_forecasts(
    io_engine, 
    run_id: int, 
    gfs_init_time: datetime, 
    gfs_metadata: Dict[str, Any],
    validator_hash: str
) -> List[Dict[str, Any]]:
    """
    Queries all available miners for weather forecasts.
    
    Args:
        io_engine: IOEngine instance
        run_id: Weather run ID
        gfs_init_time: GFS initialization time
        gfs_metadata: GFS metadata
        validator_hash: Validator's reference hash
        
    Returns:
        List of miner response records
    """
    pool = await get_db_pool()
    miner_responses = []
    
    try:
        # TODO: This is a placeholder - in the full implementation, this would:
        # 1. Query the substrate network for active miners
        # 2. Send forecast requests to each miner via HTTP
        # 3. Create database records for each response
        
        # For now, create a mock response structure
        mock_miners = [
            {"uid": 1, "hotkey": "mock_miner_1_hotkey"},
            {"uid": 2, "hotkey": "mock_miner_2_hotkey"},
        ]
        
        for miner in mock_miners:
            response_id = await db.create_weather_miner_response(
                pool=pool,
                run_id=run_id,
                miner_uid=miner["uid"],
                miner_hotkey=miner["hotkey"],
                job_id=f"weather_job_{run_id}_{miner['uid']}",
                input_hash_validator=validator_hash
            )
            
            miner_responses.append({
                "response_id": response_id,
                "miner_uid": miner["uid"],
                "miner_hotkey": miner["hotkey"],
                "job_id": f"weather_job_{run_id}_{miner['uid']}"
            })
            
        logger.info(f"Created {len(miner_responses)} miner response records")
        return miner_responses
        
    except Exception as e:
        logger.error(f"Error querying miners: {e}")
        return []


async def _verify_miner_responses(
    io_engine,
    run_id: int,
    miner_responses: List[Dict[str, Any]],
    validator_hash: str
) -> List[Dict[str, Any]]:
    """
    Verifies miner responses by checking hashes and forecast integrity.
    
    Args:
        io_engine: IOEngine instance
        run_id: Weather run ID
        miner_responses: List of miner response records
        validator_hash: Validator's reference hash
        
    Returns:
        List of verified miner responses
    """
    pool = await get_db_pool()
    verified_responses = []
    
    for response in miner_responses:
        try:
            # TODO: In full implementation, this would:
            # 1. Retrieve the miner's forecast URL
            # 2. Verify the forecast hash using compute workers
            # 3. Check forecast data integrity
            # 4. Update the response status in the database
            
            # For now, simulate verification
            logger.info(f"Verifying response {response['response_id']} from miner {response['miner_uid']}")
            
            # Mock verification result (in reality, this would use the compute pool)
            verification_passed = True  # Mock result
            
            await db.update_weather_miner_response(
                pool=pool,
                response_id=response["response_id"],
                verification_passed=verification_passed,
                status="verified" if verification_passed else "verification_failed",
                input_hash_match=True  # Mock - would compare miner hash vs validator hash
            )
            
            if verification_passed:
                verified_responses.append(response)
                
        except Exception as e:
            logger.error(f"Error verifying miner response {response['response_id']}: {e}")
            try:
                await db.update_weather_miner_response(
                    pool=pool,
                    response_id=response["response_id"],
                    status="verification_failed",
                    error_message=str(e)
                )
            except Exception as db_err:
                logger.error(f"Failed to update response status: {db_err}")
    
    return verified_responses


async def _queue_scoring_jobs(
    pool,
    run_id: int,
    verified_responses: List[Dict[str, Any]]
) -> None:
    """
    Queues scoring jobs for verified miner responses.
    
    Args:
        pool: Database connection pool
        run_id: Weather run ID
        verified_responses: List of verified miner responses
    """
    if not verified_responses:
        logger.warning(f"No verified responses to score for run {run_id}")
        return
    
    try:
        # Queue Day-1 scoring job
        await db.create_weather_scoring_job(
            pool=pool,
            run_id=run_id,
            score_type="day1_qc"
        )
        
        # Queue ERA5 final scoring job (will run later when ERA5 data is available)
        await db.create_weather_scoring_job(
            pool=pool,
            run_id=run_id,
            score_type="era5_final"
        )
        
        logger.info(f"Queued scoring jobs for run {run_id}")
        
    except Exception as e:
        logger.error(f"Error queueing scoring jobs for run {run_id}: {e}")
        raise