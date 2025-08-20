from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
from typing import Optional, Dict, Any
import sqlalchemy as sa

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager

# Use module-level logger for consistency
logger = logging.getLogger(__name__)
from gaia.tasks.defined_tasks.weather.pipeline.scheduler import MinerWorkScheduler
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.tasks.defined_tasks.weather.pipeline.steps import day1_step, era5_step
from gaia.tasks.defined_tasks.weather.pipeline.steps import seed_step
from gaia.tasks.defined_tasks.weather.weather_scoring_mechanism import (
    evaluate_miner_forecast_day1,
)
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from gaia.tasks.defined_tasks.weather.utils.gfs_api import (
    fetch_gfs_analysis_data,
    fetch_gfs_data,
)
from gaia.validator.utils.substrate_manager import get_process_isolated_substrate
from fiber.chain.fetch_nodes import get_nodes_for_netuid
from gaia.validator.weights.weight_service import commit_weights_if_eligible


# verification removed


# REMOVED: process_day1_one and process_era5_one functions that called unused run() methods
# These are replaced by the run_item() pathway through the generic job queue


async def process_one(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Unified worker: claims the next available work, preferring generic queue, then per-miner fallback."""
    # First ensure step jobs are enqueued into the generic queue
    try:
        await db.enqueue_weather_step_jobs(limit=200)
    except Exception:
        pass
    try:
        await db.enqueue_miner_poll_jobs(limit=200)
    except Exception:
        pass
    # Prefer generic queue if available
    # Use the actual worker process name as claimed_by to avoid all showing weather-w/1
    try:
        import multiprocessing as _mp
        _pname = _mp.current_process().name if _mp.current_process() else "weather-w/1"
    except Exception:
        _pname = "weather-w/1"
    job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="weather.")
    if job:
        try:
            # Log concise claim
            try:
                _tag = mp.current_process().name if mp.current_process() else "weather-w?"
                logger.info(
                    f"[{_tag}] claimed job id={job.get('id')} type={job.get('job_type')} run_id={job.get('run_id')} miner_uid={job.get('miner_uid')}"
                )
            except Exception:
                pass
            jtype = job.get("job_type")
            payload = job.get("payload") or {}
            if isinstance(payload, str):
                import json as _json

                try:
                    payload = _json.loads(payload)
                except Exception:
                    payload = {}
            if jtype == "weather.run.orchestrate":
                # High-level run orchestration
                from gaia.tasks.defined_tasks.weather.pipeline.orchestrator import orchestrate_run
                rid = payload.get("run_id")
                vhk = payload.get("validator_hotkey", "unknown_validator")
                if rid:
                    ok = await orchestrate_run(db, int(rid), vhk, validator)
                    if ok:
                        await db.complete_validator_job(job["id"], result={"ok": True})
                    else:
                        await db.fail_validator_job(job["id"], "orchestration failed", schedule_retry_in_seconds=60)
                else:
                    await db.fail_validator_job(job["id"], "missing run_id")
                    ok = False
                return ok
            elif jtype == "weather.initiate_fetch":
                # Create individual query jobs for all miners (runs after seed completes)
                from gaia.tasks.defined_tasks.weather.pipeline.orchestrator import handle_initiate_fetch_job
                rid = payload.get("run_id")
                vhk = payload.get("validator_hotkey", "unknown_validator")
                if rid:
                    ok = await handle_initiate_fetch_job(db, int(rid), vhk, validator)
                    if ok:
                        await db.complete_validator_job(job["id"], result={"ok": True})
                    else:
                        await db.fail_validator_job(job["id"], "initiate-fetch failed", schedule_retry_in_seconds=120)
                else:
                    await db.fail_validator_job(job["id"], "missing run_id")
                    ok = False
                return ok
            elif jtype == "weather.query_miner":
                # Query individual miner to start inference
                from gaia.tasks.defined_tasks.weather.pipeline.steps.query_miner_step import run_query_miner_job
                from datetime import datetime, timezone, timedelta
                
                rid = payload.get("run_id")
                muid = job.get("miner_uid")
                mhk = payload.get("miner_hotkey", "unknown")
                vhk = payload.get("validator_hotkey", "unknown")
                retry_count = payload.get("retry_count", 0)
                
                if rid and muid is not None:
                    ok = await run_query_miner_job(db, int(rid), int(muid), mhk, vhk, validator)
                    if ok:
                        # Query succeeded, reset retry tracking
                        await db.execute(
                            """
                            UPDATE weather_forecast_stats
                            SET retries_remaining = 3,
                                current_forecast_status = 'query_successful',
                                last_error_message = NULL,
                                next_scheduled_retry = NULL
                            WHERE run_id = :run_id AND miner_uid = :miner_uid
                            """,
                            {
                                "run_id": rid,
                                "miner_uid": muid
                            }
                        )
                        await db.complete_validator_job(job["id"], result={"ok": True})
                    else:
                        # Schedule retry with lower priority so first attempts get processed first
                        retry_count = retry_count + 1
                        if retry_count <= 3:
                            backoff = [60, 300, 900][retry_count - 1]  # 1min, 5min, 15min
                            # Lower priority for retries: 85, 90, 95
                            retry_priority = 80 + (retry_count * 5)
                            
                            # Update retries_remaining in weather_forecast_stats
                            retries_remaining = 3 - retry_count
                            next_retry_time = datetime.now(timezone.utc) + timedelta(seconds=backoff)
                            await db.execute(
                                """
                                UPDATE weather_forecast_stats
                                SET retries_remaining = :retries,
                                    next_scheduled_retry = :next_retry,
                                    current_forecast_status = 'retrying',
                                    last_error_message = 'Query failed, retrying...'
                                WHERE run_id = :run_id AND miner_uid = :miner_uid
                                """,
                                {
                                    "retries": retries_remaining,
                                    "next_retry": next_retry_time,
                                    "run_id": rid,
                                    "miner_uid": muid
                                }
                            )
                            
                            # Re-enqueue with updated retry count and lower priority using singleton to prevent duplicates
                            singleton_key = f"query_miner_run_{rid}_miner_{muid}"
                            await db.enqueue_singleton_job(
                                singleton_key=singleton_key,
                                job_type="weather.query_miner",
                                payload={
                                    "run_id": rid,
                                    "miner_uid": muid,
                                    "miner_hotkey": mhk,
                                    "validator_hotkey": vhk,
                                    "retry_count": retry_count,
                                },
                                priority=retry_priority,
                                run_id=rid,
                                miner_uid=muid,
                                scheduled_at=next_retry_time
                            )
                            await db.complete_validator_job(job["id"], result={"retry_scheduled": True})
                            logger.info(
                                f"[Run {rid}] Scheduled retry {retry_count}/3 for miner {muid} "
                                f"in {backoff}s with priority {retry_priority}, {retries_remaining} retries remaining"
                            )
                        else:
                            # All retries exhausted, mark as failed
                            await db.execute(
                                """
                                UPDATE weather_forecast_stats
                                SET retries_remaining = 0,
                                    current_forecast_status = 'failed',
                                    last_error_message = 'Query failed after 3 attempts',
                                    next_scheduled_retry = NULL
                                WHERE run_id = :run_id AND miner_uid = :miner_uid
                                """,
                                {
                                    "run_id": rid,
                                    "miner_uid": muid
                                }
                            )
                            await db.fail_validator_job(job["id"], "Query failed after 3 attempts")
                            logger.warning(f"[Run {rid}] Miner {muid} failed after 3 attempts, marked as failed")
                        ok = False
                else:
                    await db.fail_validator_job(job["id"], "missing run_id or miner_uid")
                    ok = False
                return ok
            elif jtype == "weather.poll_miner":
                # Poll miner for inference status
                from gaia.tasks.defined_tasks.weather.pipeline.steps.poll_miner_step import run_poll_miner_job
                rid = payload.get("run_id")
                muid = job.get("miner_uid")
                mhk = payload.get("miner_hotkey", "unknown")
                resp_id = job.get("response_id") or payload.get("response_id")
                job_id = payload.get("job_id", "unknown")
                attempt = payload.get("attempt", 1)
                if rid and muid is not None and resp_id:
                    ok = await run_poll_miner_job(db, int(rid), int(muid), mhk, int(resp_id), job_id, attempt, validator)
                    if ok:
                        await db.complete_validator_job(job["id"], result={"ok": True})
                    else:
                        await db.fail_validator_job(job["id"], "Poll failed", schedule_retry_in_seconds=60)
                        ok = False
                else:
                    await db.fail_validator_job(job["id"], "missing required fields")
                    ok = False
                return ok
            elif jtype == "weather.seed":
                # Run-level seed job (download_gfs), payload has run_id and a coordinator miner tag
                logger.info(f"[weather.seed] Processing seed job {job['id']} with payload: {payload}")
                rid = payload.get("run_id")
                # Be careful with miner_uid=0 which is falsy but valid
                uid = payload.get("miner_uid")
                if uid is None:
                    uid = payload.get("uid")
                hk = payload.get("miner_hotkey") or payload.get("hk") or "coordinator"
                logger.info(f"[weather.seed] Extracted: run_id={rid}, miner_uid={uid}, miner_hotkey={hk}")
                if rid is not None and uid is not None:
                    logger.info(f"[weather.seed] Calling seed_step.run_item for run {rid}")
                    ok = await seed_step.run_item(
                        db,
                        run_id=int(rid),
                        miner_uid=int(uid),
                        miner_hotkey=str(hk),
                        validator=validator,
                    )
                    logger.info(f"[weather.seed] seed_step.run_item returned: {ok}")
                else:
                    logger.warning(f"[weather.seed] Missing required fields: run_id={rid}, miner_uid={uid}")
                    ok = False
                # Mark run as GFS ready if successful
                if ok:
                    await db.execute(
                        "UPDATE weather_forecast_runs SET status = 'gfs_ready' WHERE id = :run_id",
                        {"run_id": rid}
                    )
                    
                    # Update forecast metadata for all miners
                    import json
                    from datetime import timedelta
                    
                    # Get GFS init time from the run
                    run_info = await db.fetch_one(
                        "SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :run_id",
                        {"run_id": rid}
                    )
                    if run_info and run_info["gfs_init_time_utc"]:
                        gfs_init = run_info["gfs_init_time_utc"]
                        forecast_type = "10day"  # Could be from config
                        initial_scoring_lead_hours = [6, 12]  # From config
                        
                        input_sources = {
                            "gfs": {
                                "init_time": gfs_init.isoformat(),
                                "cycle": gfs_init.strftime("%Hz"),
                                "leads": initial_scoring_lead_hours,
                                "analysis_times": [
                                    (gfs_init + timedelta(hours=0)).isoformat(),
                                    (gfs_init + timedelta(hours=-6)).isoformat()
                                ]
                            },
                            "era5_climatology": {
                                "version": "v1",
                                "years_used": 30
                            }
                        }
                        
                        await db.execute(
                            """
                            UPDATE weather_forecast_stats wfs
                            SET forecast_type = :forecast_type,
                                forecast_input_sources = :input_sources,
                                updated_at = NOW()
                            FROM weather_forecast_runs wfr
                            WHERE wfs.run_id = wfr.id
                            AND wfr.id = :run_id
                            """,
                            {
                                "run_id": rid,
                                "forecast_type": forecast_type,
                                "input_sources": json.dumps(input_sources)
                            }
                        )
                    await db.complete_validator_job(job["id"], result={"ok": True})
                    # Re-enqueue orchestration job to handle the next phase
                    await db.enqueue_validator_job(
                        job_type="weather.run.orchestrate",
                        payload={"run_id": rid, "validator_hotkey": hk},
                        priority=50,
                        run_id=rid,
                    )
                    logger.info(f"[weather.seed] Re-enqueued orchestration job for run {rid} after successful seed")
                else:
                    # Use longer backoff for rate limit errors (15 minutes)
                    # This gives NOAA servers time to reset rate limits
                    await db.fail_validator_job(job["id"], "seed step returned False - likely rate limited", schedule_retry_in_seconds=900)
                return ok
            elif jtype == "weather.day1":
                if all(k in payload for k in ("run_id", "miner_uid", "response_id")):
                    # CRITICAL: Validate job parameters match database constraints for miner isolation
                    if job.get("run_id") and job["run_id"] != payload["run_id"]:
                        logger.error(
                            f"[ISOLATION VIOLATION] Job {job.get('id')} run_id {job['run_id']} "
                            f"doesn't match payload run_id {payload['run_id']}"
                        )
                        await db.fail_validator_job(job["id"], "Job/payload run_id mismatch - isolation violation")
                        return False
                    
                    if job.get("miner_uid") and job["miner_uid"] != payload["miner_uid"]:
                        logger.error(
                            f"[ISOLATION VIOLATION] Job {job.get('id')} miner_uid {job['miner_uid']} "
                            f"doesn't match payload miner_uid {payload['miner_uid']}"
                        )
                        await db.fail_validator_job(job["id"], "Job/payload miner_uid mismatch - isolation violation")
                        return False
                    
                    miner_hotkey = payload.get("miner_hotkey")
                    if not miner_hotkey:
                        row = await db.fetch_one(
                            "SELECT miner_hotkey FROM weather_miner_responses WHERE id = :rid",
                            {"rid": payload["response_id"]},
                        )
                        miner_hotkey = row and row.get("miner_hotkey")
                    ok = await day1_step.run_item(
                        db,
                        run_id=payload["run_id"],
                        miner_uid=payload["miner_uid"],
                        miner_hotkey=miner_hotkey or "",
                        response_id=payload["response_id"],
                        validator=validator,
                    )
                else:
                    # Fallback: No specific job payload, cannot process without run_item parameters
                    logger.warning("Day1 job without specific payload cannot be processed via fallback")
                    ok = False
                
                # Handle the result properly
                if ok:
                    await db.complete_validator_job(job["id"], result={"ok": True})
                else:
                    # Schedule retry with 60 second delay to prevent rate limiting
                    await db.fail_validator_job(job["id"], "Day1 scoring failed", schedule_retry_in_seconds=60)
                return ok
            elif jtype == "weather.era5":
                if all(k in payload for k in ("run_id", "miner_uid", "response_id")):
                    # CRITICAL: Validate job parameters match database constraints for miner isolation
                    if job.get("run_id") and job["run_id"] != payload["run_id"]:
                        logger.error(
                            f"[ISOLATION VIOLATION] Job {job.get('id')} run_id {job['run_id']} "
                            f"doesn't match payload run_id {payload['run_id']}"
                        )
                        await db.fail_validator_job(job["id"], "Job/payload run_id mismatch - isolation violation")
                        return False
                    
                    if job.get("miner_uid") and job["miner_uid"] != payload["miner_uid"]:
                        logger.error(
                            f"[ISOLATION VIOLATION] Job {job.get('id')} miner_uid {job['miner_uid']} "
                            f"doesn't match payload miner_uid {payload['miner_uid']}"
                        )
                        await db.fail_validator_job(job["id"], "Job/payload miner_uid mismatch - isolation violation")
                        return False
                    
                    miner_hotkey = payload.get("miner_hotkey")
                    if not miner_hotkey:
                        row = await db.fetch_one(
                            "SELECT miner_hotkey FROM weather_miner_responses WHERE id = :rid",
                            {"rid": payload["response_id"]},
                        )
                        miner_hotkey = row and row.get("miner_hotkey")
                    ok = await era5_step.run_item(
                        db,
                        run_id=payload["run_id"],
                        miner_uid=payload["miner_uid"],
                        miner_hotkey=miner_hotkey or "",
                        response_id=payload["response_id"],
                        validator=validator,
                    )
                else:
                    # Fallback: No specific job payload, cannot process without run_item parameters
                    logger.warning("ERA5 job without specific payload cannot be processed via fallback")
                    ok = False
                
                # Handle the result properly
                if ok:
                    await db.complete_validator_job(job["id"], result={"ok": True})
                else:
                    # Schedule retry with 60 second delay to prevent rate limiting
                    await db.fail_validator_job(job["id"], "ERA5 scoring failed", schedule_retry_in_seconds=60)
                return ok
            elif jtype == "weather.scoring.day1_qc":
                # Kick off per-miner day1 by ensuring jobs are enqueued
                try:
                    await db.execute(
                        "UPDATE weather_forecast_runs SET status = 'day1_scoring_started' WHERE id = :rid",
                        {"rid": payload.get("run_id")},
                    )
                except Exception:
                    pass
                try:
                    await db.enqueue_weather_step_jobs(limit=500)
                except Exception:
                    pass
                ok = True
            elif jtype == "weather.scoring.era5_final":
                # Signal final scoring attempted; per-miner ERA5 steps will run via scheduler
                try:
                    await db.execute(
                        "UPDATE weather_forecast_runs SET final_scoring_attempted_time = NOW() WHERE id = :rid",
                        {"rid": payload.get("run_id")},
                    )
                except Exception:
                    pass
                ok = True
            else:
                ok = False
            if ok:
                await db.complete_validator_job(job["id"], result={"ok": True})
            else:
                await db.fail_validator_job(job["id"], "step returned False", schedule_retry_in_seconds=900)
            return ok
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    # Try non-weather utility queues next: stats → metagraph → miners → era5 → ops
    job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="stats.")
    if job:
        try:
            try:
                _tag = mp.current_process().name if mp.current_process() else "weather-w?"
                logger.info(
                    f"[{_tag}] claimed job id={job.get('id')} type={job.get('job_type')} run_id={job.get('run_id')} miner_uid={job.get('miner_uid')}"
                )
            except Exception:
                pass
            j = job.get("job_type")
            if j == "stats.aggregate":
                from gaia.tasks.defined_tasks.weather.pipeline.steps.aggregate_step import run_miner_aggregation

                ok = await run_miner_aggregation(db, validator=validator)
            elif j == "stats.subnet_snapshot":
                from gaia.tasks.defined_tasks.weather.pipeline.steps.aggregate_step import compute_subnet_stats

                _ = await compute_subnet_stats(db)
                ok = True
            else:
                ok = False
            if ok:
                await db.complete_validator_job(job["id"], result={"ok": True})
            else:
                await db.fail_validator_job(job["id"], "unknown stats job")
            return ok
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="metagraph.")
    if job:
        try:
            try:
                import logging as _logging, multiprocessing as _mp
                _tag = _mp.current_process().name if _mp.current_process() else "weather-w?"
                _logging.getLogger(__name__).info(
                    f"[{_tag}] claimed job id={job.get('id')} type={job.get('job_type')}"
                )
            except Exception:
                pass
            # Placeholder: actual metagraph sync handled elsewhere; mark completed
            await db.complete_validator_job(job["id"], result={"ok": True})
            return True
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="miners.")
    if job:
        try:
            # Opportunistically ensure polling jobs exist
            try:
                await db.enqueue_miner_poll_jobs(limit=500)
            except Exception:
                pass
            try:
                _tag = mp.current_process().name if mp.current_process() else "weather-w?"
                logger.info(
                    f"[{_tag}] claimed job id={job.get('id')} type={job.get('job_type')} run_id={job.get('run_id')} miner_uid={job.get('miner_uid')}"
                )
            except Exception:
                pass
            j = job.get("job_type")
            if j == "miners.poll_inference_status":
                # Poll a miner for inference progress and reschedule until ready
                payload = job.get("payload") or {}
                if isinstance(payload, str):
                    import json as _json
                    try:
                        payload = _json.loads(payload)
                    except Exception:
                        payload = {}
                run_id = payload.get("run_id")
                miner_uid = payload.get("miner_uid")
                response_id = payload.get("response_id")
                miner_hotkey = payload.get("miner_hotkey")
                if response_id and miner_hotkey:
                    try:
                        from gaia.tasks.defined_tasks.weather.pipeline.miner_communication import poll_miner_job_status
                        status_response = await poll_miner_job_status(
                            validator=validator,
                            miner_hotkey=miner_hotkey,
                            job_id=payload.get("job_id", ""),
                            db_manager=db,
                        )
                        # Minimal interpretation: if not ready, reschedule
                        ready = False
                        if status_response and status_response.get("success"):
                            data = status_response.get("data", {})
                            status = data.get("status", "")
                            ready = status in ["ready", "completed"]
                        if ready:
                            # Mark response as submitted/ready for scoring
                            try:
                                if response_id:
                                    await db.execute(
                                        "UPDATE weather_miner_responses SET status = 'forecast_submitted', last_polled_time = NOW() WHERE id = :rid",
                                        {"rid": response_id},
                                    )
                            except Exception:
                                pass
                            await db.complete_validator_job(job["id"], result={"ready": True})
                            # Ensure a day1 step exists/enqueued for this miner
                            try:
                                await db.enqueue_weather_step_jobs(limit=200)
                            except Exception:
                                pass
                            return True
                        else:
                            # Not ready; reschedule poll in ~8 minutes
                            await db.fail_validator_job(job["id"], "not ready", schedule_retry_in_seconds=480)
                            return True
                    except Exception as e:
                        await db.fail_validator_job(job["id"], f"poll exception: {e}", schedule_retry_in_seconds=600)
                        return False
                await db.fail_validator_job(job["id"], "missing payload fields")
                return False
            elif j == "miners.handle_deregistrations":
                # Fetch current nodes from chain and upsert into node_table
                try:
                    import os as _os
                    netuid = int(_os.getenv("NETUID", "237"))
                    substrate = get_process_isolated_substrate(
                        subtensor_network=_os.getenv("SUBTENSOR_NETWORK", "test"),
                        chain_endpoint=_os.getenv("SUBTENSOR_ADDRESS", "") or "",
                    )
                    nodes = get_nodes_for_netuid(substrate=substrate, netuid=netuid)
                    miners_data = []
                    for n in nodes or []:
                        try:
                            index_val = int(getattr(n, "node_id", None) or getattr(n, "uid", 0))
                            # Compare briefly against current DB snapshot; only enqueue changes
                            existing = await db.fetch_one(
                                "SELECT hotkey, coldkey, ip, ip_type, port, incentive, stake, trust, vtrust, protocol FROM node_table WHERE uid = :u",
                                {"u": index_val},
                            )
                            row = {
                                "index": index_val,
                                "hotkey": getattr(n, "hotkey", None),
                                "coldkey": getattr(n, "coldkey", None),
                                "ip": getattr(n, "ip", None),
                                "ip_type": getattr(n, "ip_type", None),
                                "port": getattr(n, "port", None),
                                "incentive": getattr(n, "incentive", None),
                                "stake": getattr(n, "stake", None),
                                "trust": getattr(n, "trust", None),
                                "vtrust": getattr(n, "validator_trust", None),
                                "protocol": getattr(n, "protocol", None),
                            }
                            if not existing or any(
                                (existing.get(k) != row.get(k)) for k in [
                                    "hotkey","coldkey","ip","ip_type","port","incentive","stake","trust","vtrust","protocol"
                                ]
                            ):
                                miners_data.append(row)
                        except Exception:
                            continue
                    if miners_data:
                        await db.batch_update_miners(miners_data)
                    await db.complete_validator_job(job["id"], result={"updated": len(miners_data)})
                    return True
                except Exception as e:
                    await db.fail_validator_job(job["id"], f"dereg exception: {e}", schedule_retry_in_seconds=300)
                    return False
            elif j == "weights.set":
                # Offloaded weight setting (singleton). Uses validator instance passed from parent if provided.
                try:
                    ok = False
                    if validator is not None:
                        logger.info(f"[WeightSetter] Attempting weight setting for job {job.get('id')}")
                        ok = await commit_weights_if_eligible(validator)
                        if ok:
                            logger.info(f"[WeightSetter] Weight setting successful for job {job.get('id')}")
                        else:
                            logger.info(f"[WeightSetter] Weight setting not eligible yet for job {job.get('id')}")
                    else:
                        logger.warning(f"[WeightSetter] No validator instance provided for job {job.get('id')}")
                        
                    if ok:
                        await db.complete_validator_job(job["id"], result={"ok": True})
                        return True
                    else:
                        # Not eligible yet; back off 2 minutes
                        await db.fail_validator_job(job["id"], "not eligible", schedule_retry_in_seconds=120)
                        return True
                except Exception as e:
                    await db.fail_validator_job(job["id"], f"weights exception: {e}", schedule_retry_in_seconds=180)
                    return False
            else:
                # Unknown miners.* job
                await db.fail_validator_job(job["id"], "unknown miners job")
                return False
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="era5.")
    if job:
        try:
            try:
                import logging as _logging, multiprocessing as _mp
                _tag = _mp.current_process().name if _mp.current_process() else "weather-w?"
                _logging.getLogger(__name__).info(
                    f"[{_tag}] claimed job id={job.get('id')} type={job.get('job_type')}"
                )
            except Exception:
                pass
            # Placeholder: era5 token refresh, etc.
            await db.complete_validator_job(job["id"], result={"ok": True})
            return True
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="ops.")
    if job:
        try:
            try:
                import logging as _logging, multiprocessing as _mp
                _tag = _mp.current_process().name if _mp.current_process() else "weather-w?"
                _logging.getLogger(__name__).info(
                    f"[{_tag}] claimed job id={job.get('id')} type={job.get('job_type')}"
                )
            except Exception:
                pass
            # Placeholder ops (status snapshot, db monitor, plots)
            await db.complete_validator_job(job["id"], result={"ok": True})
            return True
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    # Fallback to per-miner scheduler selection
    sched = MinerWorkScheduler(db)
    item = await sched.claim_next()
    if not item:
        return False
    # REMOVED: Legacy per-miner worker code that used the old run() methods
    # All scoring now goes through the generic job queue with run_item() methods
    logger.warning(f"Legacy per-miner worker cannot process step '{item.step}' - use generic job queue instead")
    return False


