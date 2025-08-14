"""
Simplified weather task validator execution - only orchestrates, doesn't do work.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


async def validator_execute_simple(task, validator):
    """
    Simplified validator execution loop that only orchestrates.
    
    This function:
    1. Waits for the scheduled run time
    2. Creates a run record
    3. Enqueues an orchestration job
    4. Lets workers handle everything else
    """
    task.validator = validator
    logger.info("Starting simplified WeatherTask validator execution loop...")
    
    run_hour_utc = task.config.get("run_hour_utc", 18)
    run_minute_utc = task.config.get("run_minute_utc", 0)
    logger.info(
        f"Validator execute loop configured to run at {run_hour_utc:02d}:{run_minute_utc:02d} UTC daily."
    )
    
    if task.test_mode:
        logger.warning("Running in TEST MODE: Will run once immediately.")
    
    last_run_date = None
    
    while True:
        try:
            await validator.update_task_status("weather", "active", "waiting")
            
            now_utc = datetime.now(timezone.utc)
            
            # Determine if we should run
            should_run = False
            
            if task.test_mode and last_run_date is None:
                # Test mode: run immediately once
                should_run = True
                
            elif not task.test_mode:
                # Production mode: run daily at scheduled time
                target_run_time_today = now_utc.replace(
                    hour=run_hour_utc,
                    minute=run_minute_utc,
                    second=0,
                    microsecond=0,
                )
                
                # Check if we should run today
                if now_utc >= target_run_time_today and (
                    last_run_date is None or last_run_date < target_run_time_today.date()
                ):
                    should_run = True
                    last_run_date = now_utc.date()
                    
                elif now_utc < target_run_time_today:
                    # Wait until run time
                    wait_seconds = (target_run_time_today - now_utc).total_seconds()
                    logger.info(
                        f"Next run at {target_run_time_today}. Waiting {wait_seconds:.0f} seconds."
                    )
                    await asyncio.sleep(min(wait_seconds, 60))  # Check every minute
                    continue
                    
            if should_run:
                logger.info(f"Initiating weather forecast run at {now_utc}...")
                await validator.update_task_status("weather", "processing", "creating_run")
                
                # Apply test mode time shift if needed
                if task.test_mode:
                    from gaia.tasks.defined_tasks.weather.pipeline.steps.util_time import (
                        get_effective_gfs_init,
                    )
                    shifted = get_effective_gfs_init(task, now_utc)
                    if shifted != now_utc:
                        logger.info(
                            f"Test mode: time shifted from {now_utc} to {shifted}"
                        )
                        now_utc = shifted
                
                # Calculate GFS times
                gfs_t0_run_time = now_utc.replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                
                logger.info(f"Target GFS Analysis Time: {gfs_t0_run_time}")
                
                # Check for existing run
                existing_run = await task.db_manager.fetch_one(
                    """
                    SELECT id, status, run_initiation_time
                    FROM weather_forecast_runs 
                    WHERE gfs_init_time_utc = :gfs_init 
                    AND status NOT IN ('completed', 'error', 'stale_abandoned')
                    ORDER BY run_initiation_time DESC
                    LIMIT 1
                    """,
                    {"gfs_init": gfs_t0_run_time}
                )
                
                run_id = None
                
                if existing_run:
                    run_id = existing_run["id"]
                    run_age_hours = (
                        now_utc - existing_run["run_initiation_time"]
                    ).total_seconds() / 3600
                    
                    if run_age_hours > 24:
                        logger.warning(
                            f"[Run {run_id}] Existing run too old ({run_age_hours:.1f}h), marking stale"
                        )
                        await task.db_manager.execute(
                            "UPDATE weather_forecast_runs SET status = 'stale_abandoned' WHERE id = :id",
                            {"id": run_id}
                        )
                        run_id = None  # Create new run
                    else:
                        logger.info(
                            f"[Run {run_id}] Found existing run with status '{existing_run['status']}'"
                        )
                
                if not run_id:
                    # Create new run
                    run_record = await task.db_manager.fetch_one(
                        """
                        INSERT INTO weather_forecast_runs 
                        (run_initiation_time, target_forecast_time_utc, gfs_init_time_utc, status)
                        VALUES (:init_time, :target_time, :gfs_init, :status)
                        RETURNING id
                        """,
                        {
                            "init_time": now_utc,
                            "target_time": gfs_t0_run_time,
                            "gfs_init": gfs_t0_run_time,
                            "status": "created",
                        }
                    )
                    
                    if run_record and "id" in run_record:
                        run_id = run_record["id"]
                        logger.info(f"[Run {run_id}] Created new weather forecast run")
                    else:
                        logger.error("Failed to create run record")
                        await asyncio.sleep(60)
                        continue
                
                # Get validator hotkey
                validator_hotkey = "unknown_validator"
                try:
                    if hasattr(validator, "validator_wallet") and validator.validator_wallet:
                        validator_hotkey = validator.validator_wallet.hotkey.ss58_address
                except Exception:
                    pass
                
                # Enqueue orchestration job - let workers handle everything else
                job_id = await task.db_manager.enqueue_validator_job(
                    job_type="weather.run.orchestrate",
                    payload={
                        "run_id": run_id,
                        "validator_hotkey": validator_hotkey,
                    },
                    priority=50,  # High priority
                    run_id=run_id,
                )
                
                if job_id:
                    logger.info(
                        f"[Run {run_id}] Enqueued orchestration job {job_id}. Workers will handle the rest."
                    )
                else:
                    logger.warning(f"[Run {run_id}] Failed to enqueue orchestration job")
                
                await validator.update_task_status("weather", "active", "orchestration_enqueued")
                
                # In test mode, exit after one run
                if task.test_mode:
                    logger.info("TEST MODE: Completed one run, exiting.")
                    break
                    
            else:
                # Sleep for a minute before checking again
                await asyncio.sleep(60)
                
        except Exception as e:
            logger.error(f"Error in validator execute loop: {e}", exc_info=True)
            await asyncio.sleep(60)
            
    logger.info("Validator execute loop ended")
