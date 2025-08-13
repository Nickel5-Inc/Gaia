from __future__ import annotations

import asyncio
from typing import Optional, Dict, Any
import sqlalchemy as sa

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
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


async def process_day1_one(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Process one day1 scoring candidate end-to-end for a single miner."""
    return await day1_step.run(db, validator=validator)


async def process_era5_one(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Process one ERA5 scoring candidate for a single miner and update stats incrementally."""
    return await era5_step.run(db, validator=validator)


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
                import logging as _logging, multiprocessing as _mp
                _tag = _mp.current_process().name if _mp.current_process() else "weather-w?"
                _logging.getLogger(__name__).info(
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
            if jtype == "weather.seed":
                # Run-level seed job (download_gfs), payload has run_id and a coordinator miner tag
                rid = payload.get("run_id")
                uid = payload.get("miner_uid") or payload.get("uid")
                hk = payload.get("miner_hotkey") or payload.get("hk") or "coordinator"
                if rid is not None and uid is not None:
                    ok = await seed_step.run_item(
                        db,
                        run_id=int(rid),
                        miner_uid=int(uid),
                        miner_hotkey=str(hk),
                        validator=validator,
                    )
                else:
                    ok = False
                # Regardless of success, complete the job (seed step is idempotent and guarded by advisory lock)
                if ok:
                    await db.complete_validator_job(job["id"], result={"ok": True})
                else:
                    await db.fail_validator_job(job["id"], "seed step returned False", schedule_retry_in_seconds=300)
                return ok
            elif jtype == "weather.day1":
                if all(k in payload for k in ("run_id", "miner_uid", "response_id")):
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
                    ok = await process_day1_one(db, validator=validator)
            elif jtype == "weather.era5":
                if all(k in payload for k in ("run_id", "miner_uid", "response_id")):
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
                    ok = await process_era5_one(db, validator=validator)
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
                import logging as _logging, multiprocessing as _mp
                _tag = _mp.current_process().name if _mp.current_process() else "weather-w?"
                _logging.getLogger(__name__).info(
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
                import logging as _logging, multiprocessing as _mp
                _tag = _mp.current_process().name if _mp.current_process() else "weather-w?"
                _logging.getLogger(__name__).info(
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
                        from gaia.validator.miner.miner_query import get_input_status as _gis
                        status_response = await _gis(validator or WeatherTask(db_manager=db, node_type="validator", test_mode=True), miner_hotkey, job_id=payload.get("job_id", ""))
                        # Minimal interpretation: if not ready, reschedule
                        ready = False
                        if isinstance(status_response, dict):
                            txt = status_response.get("text") or ""
                            ready = "ready" in txt.lower() or "completed" in txt.lower()
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
                        ok = await commit_weights_if_eligible(validator)
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
    # verification removed
    if item.step == "day1":
        return await process_day1_one(db, validator=validator)
    if item.step == "era5":
        return await process_era5_one(db, validator=validator)
    return False


