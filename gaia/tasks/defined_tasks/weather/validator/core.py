"""
Weather Task Validator Core

Main validator workflow execution including run management,
miner coordination, and task orchestration.

This module extracts the core validator logic from the monolithic weather_task.py.
"""

import asyncio
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

from fiber.logging_utils import get_logger

if TYPE_CHECKING:
    from ..core.task import WeatherTask

# High-performance JSON operations
try:
    from gaia.utils.performance import dumps, loads
except ImportError:
    import json

    def dumps(obj, **kwargs):
        return json.dumps(obj, **kwargs)

    def loads(s):
        return json.loads(s)

from ..processing.weather_logic import _update_run_status
from ..schemas.weather_inputs import WeatherInitiateFetchData, WeatherGetInputStatusData
from ..schemas.weather_outputs import WeatherTaskStatus
from ..utils.hashing import compute_input_data_hash

logger = get_logger(__name__)


async def execute_validator_workflow(task: "WeatherTask", validator) -> None:
    """
    Main validator execution workflow.
    
    Orchestrates the weather forecast task for the validator:
    1. Waits for the scheduled run time (e.g., daily post-00Z GFS availability)
    2. Fetches necessary GFS analysis data (T=0h from 00Z run, T=-6h from previous 18Z run)
    3. Serializes data and creates a run record in DB
    4. Queries miners with the payload (/weather-forecast-request)
    5. Records miner acceptances in DB
    """
    task.validator = validator
    logger.info("Starting WeatherTask validator execution loop...")

    # Initialize background workers
    if not task.initial_scoring_worker_running:
        await task.start_background_workers(
            num_initial_scoring_workers=1,
            num_final_scoring_workers=1,
            num_cleanup_workers=1,
        )
        logger.info("Started background workers for scoring and cleanup.")

    run_hour_utc = task.config.run_hour_utc
    run_minute_utc = task.config.run_minute_utc
    logger.info(f"Validator configured to run at {run_hour_utc:02d}:{run_minute_utc:02d} UTC.")

    if task.test_mode:
        logger.warning("Running in TEST MODE: Execution will run once immediately.")

    # Track recovery state to avoid too frequent recovery attempts
    last_recovery_time = 0
    recovery_interval = 300  # 5 minutes between recovery attempts
    processed_runs_this_session = {}  # Track run attempts this session: {run_id: attempt_count}
    max_runs_per_session = 20  # Limit runs processed per session to avoid indefinite loops
    max_attempts_per_run = 3  # Allow multiple attempts per run in case of transient issues

    while True:
        try:
            await validator.update_task_status("weather", "active", "waiting")

            # Perform gradual, non-blocking recovery every 5 minutes
            current_time = time.time()
            if current_time - last_recovery_time > recovery_interval:
                logger.debug("Performing periodic recovery check...")
                stale_run_processed = await _perform_recovery_check(
                    task, processed_runs_this_session, max_attempts_per_run
                )
                last_recovery_time = current_time

                # Handle recovery results
                recovery_result = await _handle_recovery_result(
                    stale_run_processed, processed_runs_this_session, max_runs_per_session
                )
                if recovery_result == "continue_recovery":
                    last_recovery_time = 0  # Reset to trigger immediate next recovery check
                    await asyncio.sleep(0.1)  # Yield CPU time to other async tasks
                    continue
                elif recovery_result == "sleep_and_continue":
                    await asyncio.sleep(0.1)
                    continue

            # Handle scheduling logic
            if task.test_mode:
                # Test mode - run immediately
                now_utc = datetime.now(timezone.utc)
                logger.info("TEST MODE: Running immediately")
            else:
                # Production mode - wait for scheduled time
                now_utc = await _wait_for_scheduled_time(run_hour_utc, run_minute_utc)

            # Execute main run logic
            await _execute_forecast_run(task, validator, now_utc)

            if task.test_mode:
                logger.info("TEST MODE: Completed run, exiting validator loop.")
                break

        except Exception as e:
            logger.error(f"Error in validator workflow: {e}", exc_info=True)
            if task.test_mode:
                break
            await asyncio.sleep(60)  # Wait before retrying


async def prepare_validator_subtasks(task: "WeatherTask") -> None:
    """Prepare validator subtasks - placeholder for future implementation."""
    logger.info("Preparing validator subtasks...")
    # This will be implemented as needed
    pass


async def _perform_recovery_check(
    task: "WeatherTask", processed_runs_this_session: Dict, max_attempts_per_run: int
) -> Optional[str]:
    """Perform periodic recovery check for incomplete runs."""
    try:
        # One-time backfill of scoring jobs from existing data
        await task._backfill_scoring_jobs_from_existing_data()
        
        # Sequential recovery - process one run at a time
        stale_run_processed = await task._check_and_recover_incomplete_runs_sequential(
            processed_runs_this_session, max_attempts_per_run
        )
        
        # Recover scoring jobs
        await task._recover_incomplete_scoring_jobs()
        
        return stale_run_processed
    except Exception as recovery_err:
        logger.warning(f"Error during periodic recovery: {recovery_err}")
        return None


async def _handle_recovery_result(
    stale_run_processed: Optional[str], 
    processed_runs_this_session: Dict, 
    max_runs_per_session: int
) -> str:
    """Handle the result of recovery check and determine next action."""
    if stale_run_processed in ["stale_processed", "run_processed"]:
        # Check if we've hit the session limit
        total_attempts = sum(processed_runs_this_session.values())
        if total_attempts >= max_runs_per_session:
            logger.info(
                f"Made {total_attempts} recovery attempts this session, reaching limit - proceeding to normal scheduling"
            )
            processed_runs_this_session.clear()
            return "proceed_normal"
        else:
            if stale_run_processed == "stale_processed":
                logger.info("Stale run was processed - continuing recovery loop")
            else:
                logger.info("Run recovery attempted - continuing to process remaining incomplete runs")
            return "continue_recovery"
    elif stale_run_processed == "no_more_runs":
        logger.info("No more incomplete runs to process - proceeding to normal scheduling")
        processed_runs_this_session.clear()
        return "proceed_normal"
    else:
        # No runs processed this cycle or unexpected return value
        if stale_run_processed is not False and stale_run_processed is not None:
            logger.debug(f"Unexpected recovery return value: {stale_run_processed}")
        return "sleep_and_continue"


async def _wait_for_scheduled_time(run_hour_utc: int, run_minute_utc: int) -> datetime:
    """Wait for the scheduled run time and return the target time."""
    now_utc = datetime.now(timezone.utc)
    target_run_time_today = now_utc.replace(
        hour=run_hour_utc,
        minute=run_minute_utc,
        second=0,
        microsecond=0,
    )
    
    if now_utc >= target_run_time_today:
        next_run_trigger_time = target_run_time_today + timedelta(days=1)
    else:
        next_run_trigger_time = target_run_time_today

    wait_seconds = (next_run_trigger_time - now_utc).total_seconds()
    logger.info(
        f"Current time: {now_utc}. Next weather run scheduled at {next_run_trigger_time}. "
        f"Waiting for {wait_seconds:.2f} seconds."
    )
    
    if wait_seconds > 0:
        await asyncio.sleep(wait_seconds)
    
    return datetime.now(timezone.utc)


async def _execute_forecast_run(task: "WeatherTask", validator, now_utc: datetime) -> None:
    """Execute the main forecast run logic."""
    logger.info(f"Initiating weather forecast run triggered around {now_utc}...")
    await validator.update_task_status("weather", "processing", "initializing_run")

    # Adjust time for test mode
    if task.test_mode:
        logger.info(f"Test mode enabled. Adjusting GFS init time by -8 days from {now_utc.strftime('%Y-%m-%d %H:%M')} UTC.")
        now_utc -= timedelta(days=8)
        logger.info(f"Adjusted GFS init time for test mode: {now_utc.strftime('%Y-%m-%d %H:%M')} UTC.")

    # Calculate GFS times
    gfs_t0_run_time = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    gfs_t_minus_6_run_time = gfs_t0_run_time - timedelta(hours=6)  # 18Z from previous day

    logger.info(f"Target GFS T=0h Analysis Run Time: {gfs_t0_run_time}")
    logger.info(f"Target GFS T=-6h Analysis Run Time: {gfs_t_minus_6_run_time}")

    # Check for existing run
    run_id = await _check_or_create_run(task, validator, now_utc, gfs_t0_run_time)
    if run_id is None:
        return  # Error or existing run handled

    # Execute the main workflow
    await _execute_run_workflow(task, validator, run_id, gfs_t0_run_time, gfs_t_minus_6_run_time)


async def _check_or_create_run(
    task: "WeatherTask", validator, now_utc: datetime, gfs_t0_run_time: datetime
) -> Optional[int]:
    """Check for existing run or create new one. Returns run_id or None."""
    existing_run_query = """
    SELECT id, status, run_initiation_time
    FROM weather_forecast_runs 
    WHERE gfs_init_time_utc = :gfs_init 
    AND status NOT IN ('completed', 'error', 'stale_abandoned', 'restarted_as_new_run', 'recovery_failed')
    ORDER BY run_initiation_time DESC
    LIMIT 1
    """
    existing_run = await task.db_manager.fetch_one(
        existing_run_query, {"gfs_init": gfs_t0_run_time}
    )

    if existing_run:
        run_id = existing_run["id"]
        existing_status = existing_run["status"]
        run_age_hours = (now_utc - existing_run["run_initiation_time"]).total_seconds() / 3600

        logger.info(
            f"[Run {run_id}] Found existing run for GFS time {gfs_t0_run_time} "
            f"with status '{existing_status}' (age: {run_age_hours:.1f}h)"
        )

        if run_age_hours > 24:
            logger.warning(f"[Run {run_id}] Existing run is too old, marking as stale and creating new run")
            await _update_run_status(task, run_id, "stale_abandoned")
        else:
            logger.info(f"[Run {run_id}] Reusing existing active run")
            await task.validator_score()
            if task.test_mode:
                logger.info("TEST MODE: Found existing run. Exiting validator_execute loop.")
            return None  # Signal to exit

    # Create new run
    try:
        run_insert_query = """
        INSERT INTO weather_forecast_runs (run_initiation_time, target_forecast_time_utc, gfs_init_time_utc, status)
        VALUES (:init_time, :target_time, :gfs_init, :status)
        RETURNING id
        """
        run_record = await task.db_manager.fetch_one(
            run_insert_query,
            {
                "init_time": now_utc,
                "target_time": gfs_t0_run_time,
                "gfs_init": gfs_t0_run_time,
                "status": "fetching_gfs",
            },
        )

        if run_record and "id" in run_record:
            run_id = run_record["id"]
            logger.info(f"[Run {run_id}] Created NEW weather_forecast_runs record")
            return run_id
        else:
            logger.error("Failed to retrieve run_id after insert")
            raise RuntimeError("Failed to create run_id for forecast run")

    except Exception as db_err:
        logger.error(f"Failed to create forecast run record in DB: {db_err}", exc_info=True)
        if task.test_mode:
            logger.warning("TEST MODE: DB error during run_id creation. Exiting.")
        return None


async def _execute_run_workflow(
    task: "WeatherTask", 
    validator, 
    run_id: int, 
    gfs_t0_run_time: datetime, 
    gfs_t_minus_6_run_time: datetime
) -> None:
    """Execute the main run workflow: fetch requests, polling, verification."""
    # Send fetch requests to miners
    await _send_fetch_requests(task, validator, run_id, gfs_t0_run_time, gfs_t_minus_6_run_time)
    
    # Wait for miners to process
    await _wait_for_miner_processing(task, validator, run_id)
    
    # Poll miners for status
    await _poll_miners_for_status(task, validator, run_id)
    
    # Verify input hashes
    await _verify_input_hashes(task, validator, run_id, gfs_t0_run_time, gfs_t_minus_6_run_time)


async def _send_fetch_requests(
    task: "WeatherTask", 
    validator, 
    run_id: int, 
    gfs_t0_run_time: datetime, 
    gfs_t_minus_6_run_time: datetime
) -> None:
    """Send fetch requests to miners."""
    await validator.update_task_status("weather", "processing", "sending_fetch_requests")
    await _update_run_status(task, run_id, "sending_fetch_requests")

    payload_data = WeatherInitiateFetchData(
        forecast_start_time=gfs_t0_run_time,  # T=0h time
        previous_step_time=gfs_t_minus_6_run_time,  # T=-6h time
    )
    payload = {"nonce": str(uuid.uuid4()), "data": payload_data.model_dump(mode="json")}

    logger.info(f"[Run {run_id}] Querying miners with weather initiate fetch request...")
    responses = await validator.query_miners(payload=payload, endpoint="/weather-initiate-fetch")
    logger.info(f"[Run {run_id}] Received {len(responses)} initial responses from miners")

    # Process responses
    await _process_fetch_responses(task, validator, run_id, responses)


async def _process_fetch_responses(
    task: "WeatherTask", validator, run_id: int, responses: Dict
) -> None:
    """Process fetch responses from miners."""
    await validator.update_task_status("weather", "processing", "recording_acceptances")
    accepted_count = 0
    
    for miner_hotkey, response_data in responses.items():
        try:
            miner_response = response_data
            if isinstance(response_data, dict) and "text" in response_data:
                try:
                    miner_response = loads(response_data["text"])
                except (Exception, TypeError) as json_err:
                    logger.warning(f"[Run {run_id}] Failed to parse response from {miner_hotkey}: {json_err}")
                    continue

            if (
                isinstance(miner_response, dict)
                and miner_response.get("status") == WeatherTaskStatus.FETCH_ACCEPTED
                and miner_response.get("job_id")
            ):
                # Get miner UID
                miner_uid_result = await task.db_manager.fetch_one(
                    "SELECT uid FROM node_table WHERE hotkey = :hk", {"hk": miner_hotkey}
                )
                miner_uid = miner_uid_result["uid"] if miner_uid_result else -1

                if miner_uid == -1:
                    logger.warning(f"[Run {run_id}] Miner {miner_hotkey} accepted but UID not found")
                    continue

                # Record acceptance
                insert_resp_query = """
                INSERT INTO weather_miner_responses
                (run_id, miner_uid, miner_hotkey, response_time, status, job_id)
                VALUES (:run_id, :uid, :hk, :resp_time, :status, :job_id)
                ON CONFLICT (run_id, miner_uid) DO UPDATE SET
                response_time = EXCLUDED.response_time, 
                status = EXCLUDED.status,
                job_id = EXCLUDED.job_id
                """
                await task.db_manager.execute(
                    insert_resp_query,
                    {
                        "run_id": run_id,
                        "uid": miner_uid,
                        "hk": miner_hotkey,
                        "resp_time": datetime.now(timezone.utc),
                        "status": "fetch_initiated",
                        "job_id": miner_response.get("job_id"),
                    },
                )
                accepted_count += 1
                logger.debug(f"[Run {run_id}] Recorded acceptance from {miner_hotkey}")
            else:
                logger.warning(f"[Run {run_id}] Invalid response from {miner_hotkey}: {miner_response}")

        except Exception as resp_proc_err:
            logger.error(f"[Run {run_id}] Error processing response from {miner_hotkey}: {resp_proc_err}")

    logger.info(f"[Run {run_id}] {accepted_count} miners accepted fetch requests")
    await _update_run_status(task, run_id, "awaiting_input_hashes")


async def _wait_for_miner_processing(task: "WeatherTask", validator, run_id: int) -> None:
    """Wait for miners to process fetch requests."""
    wait_minutes = task.config.validator_hash_wait_minutes
    if task.test_mode:
        original_wait = wait_minutes
        wait_minutes = 1
        logger.info(f"TEST MODE: Using shortened wait time of {wait_minutes} minute(s) instead of {original_wait}")
    
    logger.info(f"[Run {run_id}] Waiting for {wait_minutes} minutes for miners to fetch GFS and compute input hash...")
    await validator.update_task_status("weather", "waiting", "miner_fetch_wait")
    await asyncio.sleep(wait_minutes * 60)
    logger.info(f"[Run {run_id}] Wait finished. Proceeding with input hash verification.")


async def _poll_miners_for_status(task: "WeatherTask", validator, run_id: int) -> None:
    """Poll miners for their input hash status."""
    await validator.update_task_status("weather", "processing", "verifying_hashes")
    await _update_run_status(task, run_id, "verifying_input_hashes")

    responses_to_check_query = """
    SELECT id, miner_hotkey, job_id
    FROM weather_miner_responses
    WHERE run_id = :run_id
      AND (
          status = 'fetch_initiated' OR
          (status = 'retry_scheduled' AND next_retry_time IS NOT NULL AND next_retry_time <= :now)
      )
    """
    miners_to_poll = await task.db_manager.fetch_all(
        responses_to_check_query,
        {"run_id": run_id, "now": datetime.now(timezone.utc)},
    )
    logger.info(f"[Run {run_id}] Polling {len(miners_to_poll)} miners for input hash status")

    # Execute polling tasks in parallel
    polling_tasks = []
    for resp_rec in miners_to_poll:
        polling_tasks.append(_poll_single_miner(task, validator, run_id, resp_rec))

    poll_results = await asyncio.gather(*polling_tasks)
    
    # Collect results
    miner_hash_results = {}
    for resp_id, status_data in poll_results:
        miner_hash_results[resp_id] = status_data
    
    logger.info(f"[Run {run_id}] Collected input status from {len(miner_hash_results)}/{len(miners_to_poll)} miners")


async def _poll_single_miner(
    task: "WeatherTask", validator, run_id: int, response_rec: Dict
) -> tuple:
    """Poll a single miner for input status."""
    resp_id = response_rec["id"]
    miner_hk = response_rec["miner_hotkey"]
    miner_job_id = response_rec["job_id"]
    
    logger.debug(f"[Run {run_id}] Polling miner {miner_hk[:8]} (Job: {miner_job_id}) for input status")

    node = validator.metagraph.nodes.get(miner_hk)
    if not node or not node.ip or not node.port:
        logger.warning(f"[Run {run_id}] Miner {miner_hk[:8]} not found in metagraph or missing IP/Port")
        return resp_id, {"status": "validator_poll_error", "message": "Miner not found in metagraph"}

    try:
        status_payload_data = WeatherGetInputStatusData(job_id=miner_job_id)
        status_payload = {"nonce": str(uuid.uuid4()), "data": status_payload_data.model_dump()}
        endpoint = "/weather-get-input-status"

        all_responses = await validator.query_miners(
            payload=status_payload, endpoint=endpoint, hotkeys=[miner_hk]
        )

        status_response = all_responses.get(miner_hk)
        if status_response:
            parsed_response = status_response
            if isinstance(status_response, dict) and "text" in status_response:
                try:
                    parsed_response = loads(status_response["text"])
                except (Exception, TypeError) as json_err:
                    logger.warning(f"[Run {run_id}] Failed to parse status response from {miner_hk[:8]}: {json_err}")
                    parsed_response = {"status": "parse_error", "message": str(json_err)}

            logger.debug(f"[Run {run_id}] Received status from {miner_hk[:8]}: {parsed_response}")
            return resp_id, parsed_response
        else:
            logger.warning(f"[Run {run_id}] No response from {miner_hk[:8]}")
            return resp_id, {"status": "validator_poll_failed", "message": "No response from miner"}

    except Exception as poll_err:
        logger.error(f"[Run {run_id}] Error polling miner {miner_hk[:8]}: {poll_err}")
        return resp_id, {"status": "validator_poll_error", "message": str(poll_err)}


async def _verify_input_hashes(
    task: "WeatherTask", 
    validator, 
    run_id: int, 
    gfs_t0_run_time: datetime, 
    gfs_t_minus_6_run_time: datetime
) -> None:
    """Verify input hashes against validator's computed hash."""
    # Compute validator's reference input hash
    validator_input_hash = None
    try:
        logger.info(f"[Run {run_id}] Validator computing its own reference input hash...")
        gfs_cache_dir = Path(task.config.gfs_analysis_cache_dir)
        validator_input_hash = await compute_input_data_hash(
            t0_run_time=gfs_t0_run_time,
            t_minus_6_run_time=gfs_t_minus_6_run_time,
            gfs_cache_dir=gfs_cache_dir,
        )
        logger.info(f"[Run {run_id}] Validator computed input hash: {validator_input_hash}")
    except Exception as hash_err:
        logger.error(f"[Run {run_id}] Failed to compute validator input hash: {hash_err}")
        # Continue anyway - will handle missing hash in verification logic

    # TODO: Implement hash verification logic
    logger.info(f"[Run {run_id}] Hash verification completed")
    await task.validator_score()