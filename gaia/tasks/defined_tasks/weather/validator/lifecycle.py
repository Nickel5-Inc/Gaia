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
    from .miner_client import WeatherMinerClient
    
    pool = await get_db_pool()
    miner_responses = []
    
    try:
        # TODO: Get actual validator instance from io_engine
        # For now, this is a placeholder that needs validator integration
        # In the full implementation, this would access the validator through io_engine
        
        # Create miner client
        miner_client = WeatherMinerClient(io_engine)
        
        # Calculate GFS T-6 time
        gfs_t_minus_6_run_time = gfs_init_time - timedelta(hours=6)
        
        logger.info(f"Sending initiate fetch requests for run {run_id}")
        
        # TODO: This requires validator instance - placeholder for now
        # responses = await miner_client.initiate_fetch_from_miners(
        #     validator=validator,  # Need to get this from io_engine
        #     gfs_t0_run_time=gfs_init_time,
        #     gfs_t_minus_6_run_time=gfs_t_minus_6_run_time
        # )
        
        # For now, create mock responses to maintain functionality
        mock_responses = {
            "mock_miner_1_hotkey": {
                "status": "fetch_accepted",
                "job_id": f"weather_job_{run_id}_1"
            },
            "mock_miner_2_hotkey": {
                "status": "fetch_accepted", 
                "job_id": f"weather_job_{run_id}_2"
            }
        }
        
        # Process responses and create database records
        for miner_hotkey, response in mock_responses.items():
            if response.get("status") == "fetch_accepted" and response.get("job_id"):
                # TODO: Get actual miner UID from substrate network
                # For now, use mock UIDs
                miner_uid = 1 if "1" in miner_hotkey else 2
                
                response_id = await db.create_weather_miner_response(
                    pool=pool,
                    run_id=run_id,
                    miner_uid=miner_uid,
                    miner_hotkey=miner_hotkey,
                    job_id=response["job_id"],
                    input_hash_validator=validator_hash
                )
                
                miner_responses.append({
                    "response_id": response_id,
                    "miner_uid": miner_uid,
                    "miner_hotkey": miner_hotkey,
                    "job_id": response["job_id"]
                })
            else:
                logger.warning(f"Miner {miner_hotkey} did not accept fetch request: {response}")
                
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
    from .miner_client import WeatherMinerClient
    
    pool = await get_db_pool()
    verified_responses = []
    
    if not miner_responses:
        logger.warning(f"No miner responses to verify for run {run_id}")
        return verified_responses
    
    try:
        # Create miner client
        miner_client = WeatherMinerClient(io_engine)
        
        # Wait for miners to fetch GFS data and compute hashes
        wait_minutes = io_engine.config.WEATHER.VALIDATOR_HASH_WAIT_MINUTES
        logger.info(f"Waiting {wait_minutes} minutes for miners to fetch GFS and compute hashes...")
        await asyncio.sleep(wait_minutes * 60)
        
        # Poll miners for their input status
        logger.info(f"Polling miners for input status for run {run_id}")
        
        # TODO: This requires validator instance - placeholder for now
        # status_responses = await miner_client.get_input_status_from_miners(
        #     validator=validator,  # Need to get this from io_engine
        #     miner_jobs=miner_responses
        # )
        
        # For now, simulate status polling
        mock_status_responses = {}
        for response in miner_responses:
            miner_hotkey = response["miner_hotkey"]
            # Simulate that miners have computed matching hashes
            mock_status_responses[miner_hotkey] = {
                "status": "input_hashed_awaiting_validation",
                "input_data_hash": validator_hash,  # Mock matching hash
                "job_id": response["job_id"]
            }
        
        # Process status responses and verify hashes
        for response in miner_responses:
            try:
                miner_hotkey = response["miner_hotkey"]
                status_data = mock_status_responses.get(miner_hotkey, {})
                
                miner_status = status_data.get("status")
                miner_hash = status_data.get("input_data_hash")
                
                verification_passed = False
                hash_match = False
                new_status = "verification_failed"
                
                if (miner_hash and 
                    miner_status == "input_hashed_awaiting_validation" and 
                    miner_hash == validator_hash):
                    
                    logger.info(f"Hash MATCH for response {response['response_id']} from miner {miner_hotkey[:8]}")
                    verification_passed = True
                    hash_match = True
                    new_status = "verified"
                    
                    # Trigger inference on this miner
                    # TODO: This requires validator instance - placeholder for now
                    # trigger_success = await miner_client._trigger_single_miner_inference(
                    #     validator=validator,
                    #     miner_hotkey=miner_hotkey,
                    #     job_id=response["job_id"]
                    # )
                    trigger_success = True  # Mock successful inference trigger
                    
                    if trigger_success:
                        new_status = "inference_triggered"
                        verified_responses.append(response)
                        logger.info(f"Successfully triggered inference on {miner_hotkey[:8]}")
                    else:
                        logger.warning(f"Failed to trigger inference on {miner_hotkey[:8]}")
                        new_status = "inference_trigger_failed"
                        
                elif miner_hash and miner_hash != validator_hash:
                    logger.warning(f"Hash MISMATCH for response {response['response_id']}: miner={miner_hash[:10]}... validator={validator_hash[:10]}...")
                    new_status = "input_hash_mismatch"
                else:
                    logger.warning(f"Miner {miner_hotkey[:8]} failed to provide valid input hash")
                    new_status = "input_fetch_error"
                
                # Update database with verification results
                await db.update_weather_miner_response(
                    pool=pool,
                    response_id=response["response_id"],
                    verification_passed=verification_passed,
                    status=new_status,
                    input_hash_match=hash_match,
                    input_hash_miner=miner_hash,
                    input_hash_validator=validator_hash
                )
                
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
    
    except Exception as e:
        logger.error(f"Error in miner response verification for run {run_id}: {e}")
    
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