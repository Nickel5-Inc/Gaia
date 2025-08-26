"""
Run-level orchestration for weather forecasting task.
This module handles the high-level coordination of a forecast run.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
import random
from typing import Optional, Dict, Any

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline.steps.seed_step import seed_forecast_run

from gaia.utils.custom_logger import get_logger
logger = get_logger(__name__)


async def orchestrate_run(
    db: ValidatorDatabaseManager,
    run_id: int,
    validator_hotkey: str = "unknown_validator",
    validator: Optional[Any] = None,
) -> bool:
    """
    Orchestrate a weather forecast run.
    
    This function coordinates the entire run workflow:
    1. Ensure seed data (per-miner rows) exists
    2. Enqueue seed job for GFS download
    3. Once seed completes, enqueue initiate-fetch job
    
    Args:
        db: Database manager
        run_id: The run ID to orchestrate
        validator_hotkey: Validator's hotkey for tracking
        validator: Optional validator instance for context
        
    Returns:
        True if orchestration was successful
    """
    try:
        # Check run exists and get its details
        run = await db.fetch_one(
            """
            SELECT id, status, gfs_init_time_utc, target_forecast_time_utc
            FROM weather_forecast_runs
            WHERE id = :run_id
            """,
            {"run_id": run_id}
        )
        
        if not run:
            logger.error(f"[Run {run_id}] Run not found in database")
            return False
            
        current_status = run["status"]
        logger.info(f"[Run {run_id}] Starting orchestration, current status: {current_status}")
        
        # Step 1: Ensure seed data exists (per-miner rows)
        if current_status in ("created", "seeding"):
            # Check if there's already a seed job for this run
            existing_seed = await db.fetch_one(
                """
                SELECT id, status, next_retry_at 
                FROM validator_jobs 
                WHERE run_id = :run_id 
                AND job_type = 'weather.seed'
                AND status IN ('pending', 'claimed', 'retry_scheduled')
                ORDER BY id DESC
                LIMIT 1
                """,
                {"run_id": run_id}
            )
            
            if existing_seed:
                logger.info(
                    f"[Run {run_id}] Seed job already exists (id={existing_seed['id']}, "
                    f"status={existing_seed['status']}), waiting for it to complete"
                )
                # Don't create duplicate jobs, just return True
                return True
            
            # First check if node_table is populated
            node_count = await db.fetch_one("SELECT COUNT(*) as count FROM node_table")
            if not node_count or node_count["count"] == 0:
                logger.warning(f"[Run {run_id}] Node table is empty, waiting for metagraph sync...")
                # Return False to let the orchestration job retry later
                return False
            
            logger.info(f"[Run {run_id}] Seeding per-miner data...")
            seeded = await seed_forecast_run(db, run_id, validator_hotkey)
            logger.info(f"[Run {run_id}] Seeded {seeded} miners")
            
            if seeded == 0:
                logger.warning(f"[Run {run_id}] No miners seeded despite node_table having entries")
                return False
            
            # Update status to indicate we're downloading GFS
            await db.execute(
                "UPDATE weather_forecast_runs SET status = 'seeding' WHERE id = :run_id",
                {"run_id": run_id}
            )
            
            # The seed_forecast_run function already creates the seed step and enqueues jobs
            # So we just need to wait for the seed job to complete
            logger.info(f"[Run {run_id}] Seed job enqueued, GFS download will be handled by worker")
            
        elif current_status == "gfs_ready":
            # GFS is ready, we can proceed with miner queries
            logger.info(f"[Run {run_id}] GFS ready, enqueueing initiate-fetch job")
            
            # Use singleton to avoid duplicates from multiple worker processes
            singleton_key = f"initiate_fetch_run_{run_id}"
            logger.info(f"[Run {run_id}] Attempting to create initiate-fetch singleton job with key '{singleton_key}'")
            
            job_id = await db.enqueue_singleton_job(
                singleton_key=singleton_key,
                job_type="weather.initiate_fetch", 
                payload={
                    "run_id": run_id,
                    "validator_hotkey": validator_hotkey,
                },
                priority=80,
                run_id=run_id,
            )
            
            if job_id:
                logger.info(f"[Run {run_id}] ✓ Created initiate-fetch singleton job {job_id}")
            else:
                logger.info(f"[Run {run_id}] ✗ Initiate-fetch singleton already exists, skipped (multiprocessing protection worked)")
            
        elif current_status in ("querying_miners", "awaiting_results"):
            # Already in progress, just ensure polling jobs exist
            logger.info(f"[Run {run_id}] Run already in progress, ensuring poll jobs exist")
            await db.enqueue_miner_poll_jobs(limit=1000)
            # REMOVED: Bulk job enqueuing - jobs are created by predecessor steps upon completion
            
        else:
            logger.info(f"[Run {run_id}] Run in status '{current_status}', no orchestration needed")
            
        return True
        
    except Exception as e:
        logger.error(f"[Run {run_id}] Orchestration failed: {e}", exc_info=True)
        return False


async def handle_initiate_fetch_job(
    db: ValidatorDatabaseManager,
    run_id: int,
    validator_hotkey: str,
    validator: Optional[Any] = None,
) -> bool:
    """
    Create individual query jobs for each miner.
    This runs AFTER GFS is ready and creates parallel jobs.
    """
    # CRITICAL: Log when this function is called to track multiprocessing duplicates
    import multiprocessing as mp
    worker_name = mp.current_process().name if mp.current_process() else "unknown-worker"
    logger.info(f"[Run {run_id}] handle_initiate_fetch_job called by worker {worker_name}")
    try:
        # Check that GFS is ready
        run = await db.fetch_one(
            """
            SELECT status, gfs_init_time_utc, target_forecast_time_utc
            FROM weather_forecast_runs
            WHERE id = :run_id
            """,
            {"run_id": run_id}
        )
        
        if not run:
            logger.error(f"[Run {run_id}] Run not found")
            return False
            
        if run["status"] not in ("gfs_ready", "querying_miners"):
            logger.debug(f"[Run {run_id}] Not ready for miner queries, status: {run['status']}")
            
            # Check if there's a pending/scheduled seed job
            seed_job = await db.fetch_one(
                """
                SELECT id, status, next_retry_at 
                FROM validator_jobs 
                WHERE run_id = :run_id 
                AND job_type = 'weather.seed'
                AND status IN ('pending', 'claimed', 'retry_scheduled')
                ORDER BY id DESC
                LIMIT 1
                """,
                {"run_id": run_id}
            )
            
            if seed_job:
                retry_time = seed_job.get('next_retry_at')
                if retry_time:
                    wait_minutes = max(2, min(15, (retry_time - datetime.now(timezone.utc)).total_seconds() / 60))
                else:
                    wait_minutes = 2
                logger.info(
                    f"[Run {run_id}] Seed job {seed_job['id']} is {seed_job['status']}, "
                    f"will check again in {wait_minutes:.1f} minutes"
                )
            else:
                wait_minutes = 5
                logger.warning(f"[Run {run_id}] No active seed job found, will check again in {wait_minutes} minutes")
            
            # Check if there's already a scheduled initiate_fetch job for this run
            next_check = datetime.now(timezone.utc) + timedelta(minutes=wait_minutes)
            
            # Use atomic insert to avoid duplicates
            result = await db.fetch_one(
                """
                INSERT INTO validator_jobs (job_type, priority, status, payload, scheduled_at, run_id)
                SELECT 'weather.initiate_fetch', 90, 'pending', :payload::jsonb, :scheduled_at, :run_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM validator_jobs 
                    WHERE run_id = :run_id 
                    AND job_type = 'weather.initiate_fetch'
                    AND status IN ('pending', 'retry_scheduled')
                    AND scheduled_at > NOW()
                )
                RETURNING id
                """,
                {
                    "payload": json.dumps({
                        "run_id": run_id,
                        "validator_hotkey": validator_hotkey,
                    }),
                    "scheduled_at": next_check,
                    "run_id": run_id
                }
            )
            
            if result:
                logger.info(f"[Run {run_id}] Scheduled initiate_fetch to check again in {wait_minutes} minutes (job {result['id']})")
            else:
                logger.debug(f"[Run {run_id}] Initiate_fetch already scheduled, skipping")
            
            return False
            
        logger.info(f"[Run {run_id}] Creating query jobs for all miners")
        
        # Update status
        await db.execute(
            "UPDATE weather_forecast_runs SET status = 'querying_miners' WHERE id = :run_id",
            {"run_id": run_id}
        )
        
        # Select all active miners (no UID filtering)
        miners = await db.fetch_all(
            """
            SELECT uid, hotkey, ip, port
            FROM node_table 
            WHERE hotkey IS NOT NULL 
            ORDER BY uid
            """
        )
        # Randomize miner order to avoid all validators hitting miners in the same sequence
        try:
            rng = random.Random()
            rng.shuffle(miners)
        except Exception:
            pass
        
        # CRITICAL: Check for duplicate IP/port assignments (same physical miner, different UIDs)
        if len(miners) > 1:
            ip_port_map = {}
            for miner in miners:
                ip_port = f"{miner.get('ip', 'N/A')}:{miner.get('port', 'N/A')}"
                if ip_port in ip_port_map:
                    logger.error(
                        f"[DATABASE CORRUPTION] Multiple UIDs point to same miner instance {ip_port}:"
                        f"\n  UID {ip_port_map[ip_port]['uid']}: hotkey {ip_port_map[ip_port]['hotkey'][:8]}...{ip_port_map[ip_port]['hotkey'][-8:]}"
                        f"\n  UID {miner['uid']}: hotkey {miner['hotkey'][:8]}...{miner['hotkey'][-8:]}"
                        f"\n  This explains why both miners return the same job_id!"
                    )
                else:
                    ip_port_map[ip_port] = miner
                    
                logger.info(
                    f"[Run {run_id}] Miner UID {miner['uid']}: {miner['hotkey'][:8]}...{miner['hotkey'][-8:]} "
                    f"at {ip_port}"
                )
        
        # CRITICAL: Log potential hotkey inconsistencies that cause duplicate requests
        logger.info(f"[Run {run_id}] Retrieved {len(miners)} miners from node_table for query job creation")
        
        if not miners:
            logger.warning(f"[Run {run_id}] No miners found to query")
            await db.execute(
                "UPDATE weather_forecast_runs SET status = 'failed', error_message = 'No miners found' WHERE id = :run_id",
                {"run_id": run_id}
            )
            return True
            
        logger.info(f"[Run {run_id}] Creating {len(miners)} query jobs")
        
        # Create individual query jobs for each miner
        jobs_created = 0
        for miner in miners:
            try:
                # CRITICAL: Log the hotkey we're using from node_table for debugging
                logger.info(
                    f"[Run {run_id}] Creating query job for miner UID {miner['uid']} "
                    f"with hotkey {miner['hotkey'][:8]}...{miner['hotkey'][-8:]} (from node_table)"
                )
                # Check if job already exists or recently completed successfully
                existing = await db.fetch_one(
                    """
                    SELECT id, status FROM validator_jobs
                    WHERE job_type = 'weather.query_miner'
                    AND run_id = :run_id
                    AND miner_uid = :miner_uid
                    AND (
                        status IN ('pending', 'in_progress', 'retry_scheduled')
                        OR (status = 'completed' AND completed_at > NOW() - INTERVAL '10 minutes')
                    )
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    {"run_id": run_id, "miner_uid": miner["uid"]}
                )
                
                if existing:
                    logger.debug(f"[Run {run_id}] Query job already exists for miner {miner['uid']}")
                    continue
                
                # Create query job using singleton to prevent duplicates
                singleton_key = f"query_miner_run_{run_id}_miner_{miner['uid']}"
                job_id = await db.enqueue_singleton_job(
                    singleton_key=singleton_key,
                    job_type="weather.query_miner",
                    payload={
                        "run_id": run_id,
                        "miner_uid": miner["uid"],
                        "miner_hotkey": miner["hotkey"],  # Use node_table as source of truth
                        "validator_hotkey": validator_hotkey,
                        "retry_count": 0,  # Track retry attempts
                    },
                    priority=80,  # High priority for initial queries
                    run_id=run_id,
                    miner_uid=miner["uid"],
                )
                
                if job_id:
                    jobs_created += 1
                    
            except Exception as e:
                logger.error(f"[Run {run_id}] Failed to create query job for miner {miner['uid']}: {e}")
                continue
                
        logger.info(f"[Run {run_id}] Created {jobs_created} query jobs")
        
        # Update run status
        if jobs_created > 0:
            await db.execute(
                "UPDATE weather_forecast_runs SET status = 'awaiting_results' WHERE id = :run_id",
                {"run_id": run_id}
            )
        else:
            await db.execute(
                "UPDATE weather_forecast_runs SET status = 'failed', error_message = 'Failed to create query jobs' WHERE id = :run_id",
                {"run_id": run_id}
            )
            
        return True
        
    except Exception as e:
        logger.error(f"[Run {run_id}] Initiate-fetch job failed: {e}", exc_info=True)
        return False
