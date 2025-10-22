from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
from typing import Optional, Dict, Any
import sqlalchemy as sa

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager

# Use custom logger for consistency and proper formatting
from gaia.utils.custom_logger import get_logger
logger = get_logger(__name__)
from gaia.tasks.defined_tasks.weather.pipeline.scheduler import MinerWorkScheduler
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.tasks.defined_tasks.weather.pipeline.steps import day1_step, era5_step
from gaia.tasks.defined_tasks.weather.pipeline.steps import seed_step
from gaia.tasks.defined_tasks.weather.weather_scoring_mechanism import (
    evaluate_miner_forecast_day1,
)
from gaia.tasks.defined_tasks.weather.utils.gfs_api import (
    fetch_gfs_analysis_data,
    fetch_gfs_data,
)
from gaia.validator.utils.substrate_manager import get_process_isolated_substrate
from fiber.chain.fetch_nodes import get_nodes_for_netuid
import os
import re
from pathlib import Path


# verification removed
DB_PAUSE_LOCK_KEY: int = 746227728439  # Must match AutoSyncManager.DB_PAUSE_LOCK_KEY
AUTOSYNC_LOCK_FILE: str = "/tmp/gaia_autosync_lock"


async def _autosync_startup_barrier(db: ValidatorDatabaseManager) -> bool:
    """Return True when DB is ready and AutoSyncManager is not holding the pause lock.

    - If DB is restarting, simple queries will raise; we catch and return False.
    - If AutoSync pause lock is held, pg_try_advisory_lock(...) returns false; we return False.
    - If we obtain the lock, immediately release it and proceed (no pause in effect).
    """
    # File-based global barrier: if AutoSync is still running setup, block workers
    try:
        if Path(AUTOSYNC_LOCK_FILE).exists():
            return False
    except Exception:
        pass
    try:
        # DB readiness probe
        _ = await db.fetch_one("SELECT 1 AS ok")
    except Exception:
        return False
    try:
        row = await db.fetch_one("SELECT pg_try_advisory_lock(:key) AS ok", {"key": DB_PAUSE_LOCK_KEY})
        if not row or not row.get("ok"):
            return False
        # We acquired the lock, so no pause is in effect; release immediately
        try:
            await db.execute("SELECT pg_advisory_unlock(:key)", {"key": DB_PAUSE_LOCK_KEY})
        except Exception:
            pass
        return True
    except Exception:
        return False



async def _handle_metagraph_job(job: Dict[str, Any], db: ValidatorDatabaseManager, validator: Optional[Any]) -> bool:
    """Handle metagraph.* jobs uniformly for normal and utility workers."""
    try:
        try:
            logger.info(
                f"claimed job id={job.get('id')} type={job.get('job_type')}"
            )
        except Exception:
            pass
        # Placeholder: actual metagraph sync handled elsewhere; mark completed
        await db.complete_validator_job(job["id"], result={"ok": True})
        return True
    except Exception as exc:
        await db.fail_validator_job(job["id"], f"exception: {exc}")
        return False


async def _handle_miners_job(job: Dict[str, Any], db: ValidatorDatabaseManager, validator: Optional[Any]) -> bool:
    """Handle miners.* jobs uniformly for normal and utility workers."""
    try:
        # Opportunistically ensure polling jobs exist
        try:
            await db.enqueue_miner_poll_jobs(limit=500)
        except Exception:
            pass

        try:
            job_type = job.get("job_type")
            job_id = job.get("id")
            logger.info(
                f"claimed job id={job_id} type={job_type} run_id={job.get('run_id')} miner_uid={job.get('miner_uid')}"
            )
        except Exception:
            pass

        j = job.get("job_type")
        if j == "miners.poll_inference_status":
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
                # Hard cancel if miner no longer exists in node_table (deregistered or hotkey changed)
                try:
                    # Require BOTH uid and hotkey to match the same row; OR may pass during hotkey churn
                    exists_row = await db.fetch_one(
                        "SELECT 1 FROM node_table WHERE uid = :uid AND hotkey = :hk",
                        {"uid": miner_uid, "hk": miner_hotkey},
                    )
                except Exception:
                    exists_row = None
                if not exists_row:
                    logger.warning(
                        f"[miners.poll_inference_status] Miner not found in node_table (uid={miner_uid}, hk={str(miner_hotkey)[:8]}), cancelling poll job {job.get('id')}"
                    )
                    try:
                        await db.execute(
                            """
                            UPDATE weather_forecast_stats
                            SET current_forecast_status = 'error',
                                last_error_message = 'miner not found in node_table',
                                updated_at = NOW()
                            WHERE run_id = :rid AND miner_uid = :uid
                            """,
                            {"rid": run_id, "uid": miner_uid},
                        )
                    except Exception:
                        pass
                    # Hard-cancel: mark job completed without scheduling future retries
                    await db.complete_validator_job(job["id"], result={"cancelled": "miner_not_found"})
                    return True
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
                        # REMOVED: Bulk job enqueuing - next steps are created by predecessor completion
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
            # Fetch current nodes, clean stale data on hotkey changes, and upsert node_table
            try:
                import os as _os
                netuid = int(_os.getenv("NETUID", "237"))
                logger.info(f"[miners.handle_deregistrations] Starting with netuid={netuid}")
                substrate = get_process_isolated_substrate(
                    subtensor_network=_os.getenv("SUBTENSOR_NETWORK", "test"),
                    chain_endpoint=_os.getenv("SUBTENSOR_ADDRESS", "") or "",
                )
                logger.info(f"[miners.handle_deregistrations] Created substrate interface")
                nodes = get_nodes_for_netuid(substrate=substrate, netuid=netuid)
                logger.info(f"[miners.handle_deregistrations] Retrieved {len(nodes or [])} nodes from chain")

                miners_data = []
                hotkey_change_entries = []  # (uid, old_hotkey, new_hotkey)
                for n in nodes or []:
                    try:
                        index_val = int(getattr(n, "node_id", None) or getattr(n, "uid", 0))
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
                        if existing and existing.get("hotkey") and row["hotkey"] and existing.get("hotkey") != row["hotkey"]:
                            hotkey_change_entries.append((index_val, existing.get("hotkey"), row["hotkey"]))
                            logger.info(
                                f"[miners.handle_deregistrations] Detected hotkey change for UID {index_val}: {existing.get('hotkey')} -> {row['hotkey']}"
                            )
                        miners_data.append(row)
                    except Exception as e:
                        logger.warning(f"[miners.handle_deregistrations] Failed to process node {index_val}: {e}")
                        continue

                if hotkey_change_entries:
                    uids_to_zero = [uid for uid, _, _ in hotkey_change_entries]
                    logger.info(f"[miners.handle_deregistrations] Hotkey changes detected for UIDs {uids_to_zero}; cascading cleanup via node_table delete")
                    try:
                        await db.execute(
                            "DELETE FROM node_table WHERE uid = ANY(:uids)",
                            {"uids": uids_to_zero},
                        )
                    except Exception as e:
                        logger.warning(f"[miners.handle_deregistrations] node_table delete for UIDs {uids_to_zero} failed or partial: {e}")
                    for uid, old_hk, new_hk in hotkey_change_entries:
                        try:
                            logger.info(f"[miners.handle_deregistrations] Cleaning up records tied to old hotkey {old_hk}")
                            try:
                                await db.execute(
                                    "UPDATE validator_jobs SET status = 'canceled', updated_at = NOW(), lease_expires_at = NULL WHERE miner_uid = :uid AND job_type LIKE 'weather.%'",
                                    {"uid": uid},
                                )
                            except Exception as e_cancel:
                                logger.warning(f"[miners.handle_deregistrations] Failed cancelling active jobs for old hotkey {old_hk}: {e_cancel}")
                            try:
                                await db.execute(
                                    "DELETE FROM weather_forecast_stats WHERE miner_uid = :uid",
                                    {"uid": uid},
                                )
                            except Exception as e_zero:
                                logger.warning(f"[miners.handle_deregistrations] Failed zeroing score_table for hotkey {old_hk}: {e_zero}")
                        except Exception as e:
                            logger.warning(f"[miners.handle_deregistrations] Cleanup failed for old hotkey {old_hk}: {e}")

                    try:
                        await db.execute(
                            "UPDATE score_table SET raw_score = 0, normalized_score = 0 WHERE miner_uid = ANY(:uids)",
                            {"uids": uids_to_zero},
                        )
                    except Exception as e:
                        logger.warning(f"[miners.handle_deregistrations] Failed zeroing score_table for UIDs {uids_to_zero}: {e}")

                logger.info(f"[miners.handle_deregistrations] Processed {len(miners_data)} miner updates")
                if miners_data:
                    try:
                        await db.batch_update_miners(miners_data)
                        logger.info(f"[miners.handle_deregistrations] Successfully updated {len(miners_data)} miners in node_table")
                    except Exception as e:
                        logger.warning(f"[miners.handle_deregistrations] Failed batch_update_miners: {e}")
                else:
                    logger.info(f"[miners.handle_deregistrations] No miner updates needed")

                try:
                    existing_uids = {row["uid"] for row in await db.fetch_all("SELECT uid FROM node_table")}
                    final_miners_data = []
                    for row in miners_data:
                        try:
                            if row["index"] in existing_uids:
                                final_miners_data.append(row)
                        except Exception:
                            continue
                    if final_miners_data:
                        await db.batch_update_miners(final_miners_data)
                        logger.info("[miners.handle_deregistrations] Finalized node_table synchronization with current metagraph")
                except Exception as e_final:
                    logger.warning(f"[miners.handle_deregistrations] Final node_table sync skipped due to error: {e_final}")

                await db.complete_validator_job(job["id"], result={"updated": len(miners_data)})
                return True
            except Exception as e:
                await db.fail_validator_job(job["id"], f"dereg exception: {e}", schedule_retry_in_seconds=300)
                return False
        elif j == "miners.weights.set":
            # Weight setting has been moved to main validator process
            # Workers don't have substrate access needed for weight setting
            logger.info(f"[WeightSetter] Weight setting job {job.get('id')} - redirecting to main validator process")
            await db.fail_validator_job(job["id"], "weight setting moved to main validator process", schedule_retry_in_seconds=300)
            return True
        else:
            # Unknown miners.* job
            await db.fail_validator_job(job["id"], "unknown miners job")
            return False
    except Exception as exc:
        await db.fail_validator_job(job["id"], f"exception: {exc}")
        return False


# REMOVED: process_day1_one and process_era5_one functions that called unused run() methods
# These are replaced by the run_item() pathway through the generic job queue


async def process_one(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Unified worker: claims the next available work, preferring generic queue, then per-miner fallback."""
    # REMOVED: Bulk job enqueuing to eliminate dual pathways and race conditions
    # Each pipeline step now creates its successor job upon completion
    try:
        # Strict startup barrier: do nothing while AutoSync holds the DB pause lock or DB is restarting
        if not await _autosync_startup_barrier(db):
            return False
        await db.enqueue_miner_poll_jobs(limit=200)
    except Exception:
        pass
    # Convert pending/retry steps in weather_forecast_steps into validator_jobs (single worker via advisory lock)
    # Throttle: only one designated worker (index 1) performs this, and at a limited cadence
    try:
        import multiprocessing as _mp
        _pname = _mp.current_process().name if _mp.current_process() else ""
        should_enqueue_steps = True
        try:
            _idx: Optional[int] = None
            _total: Optional[int] = None
            m = re.search(r"(?i)\bworker[\-\s_]*([0-9]+)\s*/\s*([0-9]+)\b", _pname or "")
            if m:
                _idx = int(m.group(1))
                _total = int(m.group(2))
            else:
                # Legacy fallback: any 'worker' token with slash
                if "worker" in (_pname or "").lower() and "/" in (_pname or ""):
                    tail = (_pname.lower().split("worker", 1)[1]).lstrip(" -_/")
                    parts = tail.split("/", 1)
                    if len(parts) == 2 and parts[0].isdigit() and parts[1].split("/",1)[0].isdigit():
                        _idx = int(parts[0])
                        _total = int(parts[1].split("/",1)[0])
            if _idx is not None and _idx != 1:
                should_enqueue_steps = False
        except Exception:
            pass
        if should_enqueue_steps:
            from datetime import datetime as _dt, timezone as _tz, timedelta as _td
            interval_s = 60
            try:
                interval_s = int(os.getenv("WEATHER_STEPS_ENQUEUE_INTERVAL_SECONDS", "60"))
            except Exception:
                interval_s = 60
            last_at = getattr(validator, "_last_steps_enqueue_at", None) if validator is not None else None
            now = _dt.now(_tz.utc)
            if last_at is None or (now - last_at).total_seconds() >= interval_s:
                lock = await db.fetch_one("SELECT pg_try_advisory_lock(:k) AS ok", {"k": 0x57465354})  # 'WFST'
                if lock and lock.get("ok"):
                    try:
                        await db.enqueue_weather_step_jobs(limit=2000)
                    finally:
                        try:
                            await db.execute("SELECT pg_advisory_unlock(:k)", {"k": 0x57465354})
                        except Exception:
                            pass
                if validator is not None:
                    try:
                        setattr(validator, "_last_steps_enqueue_at", now)
                    except Exception:
                        pass
    except Exception:
        pass
    # Prefer generic queue if available
    # Use the actual worker process name as claimed_by to avoid all showing weather-w/1
    try:
        import multiprocessing as _mp
        _pname = _mp.current_process().name if _mp.current_process() else "weather-w/1"
    except Exception:
        _pname = "weather-w/1"
    # Only respect explicit utility-only flag; no implicit worker-1 special-casing
    skip_weather = False
    try:
        FORCE_UTILITY_ONLY = globals().get("FORCE_UTILITY_ONLY", False)
        if FORCE_UTILITY_ONLY or os.getenv("UTILITY_ONLY", "").strip() == "1":
            skip_weather = True
    except Exception:
        skip_weather = False
    # Enforce single in-flight job per worker: only claim when none is active
    job = None
    if not skip_weather:
        try:
            inflight = await db.fetch_one(
                """
                SELECT 1 FROM validator_jobs
                WHERE claimed_by = :w AND status = 'in_progress' AND lease_expires_at > NOW()
                LIMIT 1
                """,
                {"w": _pname},
            )
        except Exception:
            inflight = None
        if not inflight:
            # Also guard against legacy claimed_by names that lacked brackets/prefixing in older builds
            legacy_name = _pname
            try:
                # Normalize bracketed names like "[WORKER 1/5]" to plain "worker-1/5"
                if legacy_name.startswith("[") and legacy_name.endswith("]"):
                    legacy_name = legacy_name.strip("[]").lower().replace(" ", "").replace("worker", "worker-")
            except Exception:
                legacy_name = _pname
            job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="weather.", is_utility_worker=False, legacy_worker_name=legacy_name)
    if job:
        try:
            # Log concise claim
            try:
                logger.info(
                    f"claimed job id={job.get('id')} type={job.get('job_type')} run_id={job.get('run_id')} miner_uid={job.get('miner_uid')}"
                )
            except Exception:
                pass
            # Post-claim verification: ensure claimed_by matches this worker and status is in_progress
            try:
                row_verify = await db.fetch_one(
                    "SELECT status, claimed_by FROM validator_jobs WHERE id = :jid",
                    {"jid": job.get("id")},
                )
                if row_verify:
                    logger.info(
                        f"post-claim verify job {job.get('id')}: status={row_verify.get('status')} claimed_by={row_verify.get('claimed_by')} self={_pname}"
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
            # Hard stop: utility-only worker must not process weather jobs even if claimed
            FORCE_UTILITY_ONLY = globals().get("FORCE_UTILITY_ONLY", False)
            if FORCE_UTILITY_ONLY or os.getenv("UTILITY_ONLY", "").strip() == "1":
                try:
                    # Release the job back to the queue immediately (do NOT fail it)
                    await db.execute(
                        """
                        UPDATE validator_jobs
                        SET status = 'pending', claimed_by = NULL, started_at = NULL, lease_expires_at = NULL
                        WHERE id = :jid
                        """,
                        {"jid": job["id"]},
                    )
                except Exception:
                    # As a fallback, schedule a quick retry rather than failing permanently
                    await db.fail_validator_job(job["id"], "utility-only worker released weather job", schedule_retry_in_seconds=2)
                return True
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
            elif jtype == "weather.reconcile_era5_steps":
                try:
                    from scripts.reconcile_weather_era5_steps import reconcile_run
                except Exception:
                    reconcile_run = None
                if reconcile_run is None:
                    await db.fail_validator_job(job["id"], "reconcile module missing")
                    return False
                # Fetch all runs and reconcile
                runs = await db.fetch_all("SELECT id FROM weather_forecast_runs ORDER BY id ASC")
                total = 0
                for r in runs:
                    try:
                        rid = int(r["id"])
                    except Exception:
                        continue
                    try:
                        n = await reconcile_run(db, rid)
                        total += int(n or 0)
                    except Exception:
                        # Continue to next run on error
                        continue
                # Enqueue steps that became due now as part of reconciliation
                try:
                    await db.enqueue_weather_step_jobs(limit=1000)
                except Exception:
                    pass
                await db.complete_validator_job(job["id"], result={"ok": True, "reconciled": total})
                return True
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
                    # Renew lease early to avoid rapid re-claims if this worker stalls
                    try:
                        await db.execute(
                            """
                            UPDATE validator_jobs
                            SET lease_expires_at = NOW() + INTERVAL '10 minutes'
                            WHERE id = :jid
                            """,
                            {"jid": job["id"]},
                        )
                    except Exception:
                        pass
                    # Guard: ensure uid-hotkey pair still exists (deregistration/hotkey change)
                    try:
                        exists_row = await db.fetch_one(
                            "SELECT 1 FROM node_table WHERE uid = :uid AND hotkey = :hk",
                            {"uid": int(muid), "hk": str(mhk)},
                        )
                    except Exception:
                        exists_row = None
                    if not exists_row:
                        logger.warning(
                            f"[weather.query_miner] Miner not found in node_table (uid={muid}, hk={str(mhk)[:8]}), cancelling query job {job.get('id')}"
                        )
                        try:
                            await db.execute(
                                """
                                UPDATE weather_forecast_stats
                                SET current_forecast_status = 'error',
                                    last_error_message = 'miner not found in node_table',
                                    updated_at = NOW()
                                WHERE run_id = :rid AND miner_uid = :uid
                                """,
                                {"rid": rid, "uid": muid},
                            )
                        except Exception:
                            pass
                        await db.complete_validator_job(job["id"], result={"cancelled": "miner_not_found"})
                        return True
                    try:
                        ok = await run_query_miner_job(db, int(rid), int(muid), mhk, vhk, validator)
                    except Exception as e:
                        logger.error(f"run_query_miner_job exception: {e}", exc_info=True)
                        ok = False
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
                    # Guard: ensure uid-hotkey pair still valid
                    try:
                        exists_row = await db.fetch_one(
                            "SELECT 1 FROM node_table WHERE uid = :uid AND hotkey = :hk",
                            {"uid": int(payload["miner_uid"]), "hk": str(miner_hotkey or "")},
                        )
                    except Exception:
                        exists_row = None
                    if not exists_row:
                        logger.warning(
                            f"[weather.day1] Miner not found in node_table (uid={payload['miner_uid']}, hk={str(miner_hotkey)[:8]}), cancelling job {job.get('id')}"
                        )
                        await db.complete_validator_job(job["id"], result={"cancelled": "miner_not_found"})
                        return True
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
                elif ok is None:
                    # Substep scheduled its own retry, complete job to prevent double retries
                    await db.complete_validator_job(job["id"], result={"retry_scheduled_by_substep": True})
                    logger.info(f"[weather.day1] Job {job['id']} completed - substep scheduled own retry")
                else:
                    # Check if failure was due to rate limiting (longer retry delay)
                    # vs other errors (shorter retry delay)
                    retry_delay = 300  # 5 minutes for rate limit issues
                    await db.fail_validator_job(job["id"], "Day1 scoring failed - likely rate limited", schedule_retry_in_seconds=retry_delay)
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
                    # Guard: ensure uid-hotkey pair still valid
                    try:
                        exists_row = await db.fetch_one(
                            "SELECT 1 FROM node_table WHERE uid = :uid AND hotkey = :hk",
                            {"uid": int(payload["miner_uid"]), "hk": str(miner_hotkey or "")},
                        )
                    except Exception:
                        exists_row = None
                    if not exists_row:
                        logger.warning(
                            f"[weather.era5] Miner not found in node_table (uid={payload['miner_uid']}, hk={str(miner_hotkey)[:8]}), cancelling job {job.get('id')}"
                        )
                        await db.complete_validator_job(job["id"], result={"cancelled": "miner_not_found"})
                        return True
                    try:
                        ok = await era5_step.run_item(
                            db,
                            run_id=payload["run_id"],
                            miner_uid=payload["miner_uid"],
                            miner_hotkey=miner_hotkey or "",
                            response_id=payload["response_id"],
                            validator=validator,
                        )
                    except era5_step.DataNotReadyError as e:
                        # Mark job completed; step row is waiting_for_truth and will be re-queued by guard
                        logger.info(f"ERA5 data not ready for run {payload['run_id']} miner {payload['miner_uid']}: {e}")
                        await db.complete_validator_job(job["id"], result={"waiting_for_truth": True})
                        return True
                    except Exception as e:
                        logger.error(f"ERA5 job failed with exception: {e}", exc_info=True)
                        ok = False
                else:
                    # Fallback: No specific job payload, cannot process without run_item parameters
                    logger.warning("ERA5 job without required payload (run_id/miner_uid/response_id); marking completed to prevent retry thrash")
                    await db.complete_validator_job(job["id"], result={"skipped": "invalid_payload"})
                    return True
                
                # Handle the result properly
                if ok:
                    await db.complete_validator_job(job["id"], result={"ok": True})
                elif ok is None:
                    # Substep scheduled its own retry, complete job to prevent double retries
                    await db.complete_validator_job(job["id"], result={"retry_scheduled_by_substep": True})
                    logger.info(f"[weather.era5] Job {job['id']} completed - substep scheduled own retry")
                else:
                    # Hard-fail the job to avoid repeated retries for miner-side data errors
                    await db.fail_validator_job(job["id"], "ERA5 scoring failed - not retrying")
                return ok
            elif jtype == "weather.scoring.day1_qc":
                # Kick off per-miner day1 by ensuring jobs are enqueued
                try:
                    await db.execute(
                        "UPDATE weather_forecast_runs SET status = 'scoring' WHERE id = :rid",
                        {"rid": payload.get("run_id")},
                    )
                except Exception:
                    pass
                # REMOVED: Bulk job enqueuing - day1 jobs are created by poll_miner_step when inference completes
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
    job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="stats.", is_utility_worker=False)
    if job:
        try:
            try:
                logger.info(
                    f"claimed job id={job.get('id')} type={job.get('job_type')} run_id={job.get('run_id')} miner_uid={job.get('miner_uid')}"
                )
            except Exception:
                pass
            j = job.get("job_type")
            if j == "stats.aggregate":
                from gaia.tasks.defined_tasks.weather.pipeline.steps.aggregate_step import run_miner_aggregation

                ok = await run_miner_aggregation(db, validator=validator)
            elif j == "stats.subnet_snapshot":
                from gaia.tasks.defined_tasks.weather.pipeline.steps.aggregate_step import compute_subnet_stats
                try:
                    snapshot = await compute_subnet_stats(db)
                    try:
                        logger.info(f"[SubnetSnapshot] active_miners={snapshot.get('active_miners')}, avg_forecast_score={snapshot.get('avg_forecast_score')}")
                    except Exception:
                        pass
                    ok = True
                except Exception as e:
                    logger.error(f"compute_subnet_stats failed: {e}", exc_info=True)
                    ok = False
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

    job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="metagraph.", is_utility_worker=False)
    if job:
        return await _handle_metagraph_job(job, db, validator)

    job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="miners.", is_utility_worker=False)
    if job:
        return await _handle_miners_job(job, db, validator)

    job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="era5.", is_utility_worker=False)
    if job:
        try:
            try:
                logger.info(
                    f"claimed job id={job.get('id')} type={job.get('job_type')}"
                )
            except Exception:
                pass
            j = job.get("job_type")
            if j == "era5.era5_truth_guard":
                # Guard to check if ERA5 truth is available; if yes, proactively reschedule miners sooner
                payload = job.get("payload") or {}
                rid = payload.get("run_id") or job.get("run_id")
                try:
                    from gaia.tasks.defined_tasks.weather.pipeline.steps.era5_step import _get_era5_truth
                    from gaia.tasks.defined_tasks.weather.pipeline.steps.util_time import get_effective_gfs_init
                    # Use a lightweight WeatherTask for config access
                    from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
                    t = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
                    run_row = await db.fetch_one(
                        "SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :rid",
                        {"rid": rid},
                    )
                    gfs_init = run_row and run_row.get("gfs_init_time_utc")
                    if gfs_init is None:
                        await db.complete_validator_job(job["id"], result={"skipped": "no_run"})
                        return True
                    leads = t.config.get("final_scoring_lead_hours", [24,48,72,96,120,144,168,192,216,240])
                    # Time-gate leads based on delay/buffer
                    now_utc = datetime.now(timezone.utc)
                    delay_days = int(t.config.get("era5_delay_days", 5))
                    buffer_hours = int(t.config.get("era5_buffer_hours", 6))
                    # Test mode: run immediately
                    if getattr(t, "test_mode", False):
                        delay_days = 0
                        buffer_hours = 0
                    def _needed_time(h: int):
                        return gfs_init + timedelta(hours=h) + timedelta(days=delay_days) + timedelta(hours=buffer_hours)
                    time_ready_leads = [h for h in leads if now_utc >= _needed_time(h)]
                    if not time_ready_leads:
                        await db.fail_validator_job(job["id"], "truth_not_ready", schedule_retry_in_seconds=3*3600)
                        return True
                    # Fetch truth just for time-ready leads
                    ds = await _get_era5_truth(t, int(rid), gfs_init, time_ready_leads)
                    if ds is None:
                        await db.fail_validator_job(job["id"], "truth_not_ready", schedule_retry_in_seconds=3*3600)
                        return True
                    # Confirm dataset contains the requested target times
                    try:
                        ds_times = getattr(ds, "indexes", None) and ds.indexes.get("time")
                        if ds_times is None:
                            ds_times = getattr(ds, "time", None) and ds["time"].values
                    except Exception:
                        ds_times = None
                    confirmed = []
                    if ds_times is not None:
                        try:
                            import numpy as np
                            arr = np.array(ds_times)
                            for h in time_ready_leads:
                                target_dt = gfs_init + timedelta(hours=h)
                                dt64 = np.datetime64(target_dt.replace(tzinfo=None))
                                if np.any(arr == dt64):
                                    confirmed.append(h)
                        except Exception:
                            confirmed = time_ready_leads[:]
                    else:
                        confirmed = time_ready_leads[:]
                    if not confirmed:
                        await db.fail_validator_job(job["id"], "truth_times_missing", schedule_retry_in_seconds=3600)
                        return True
                    # Enqueue/mark step rows per miner for these confirmed leads
                    miners = await db.fetch_all(
                        "SELECT DISTINCT miner_uid, miner_hotkey FROM weather_miner_responses WHERE run_id = :rid",
                        {"rid": rid},
                    )
                    from gaia.tasks.defined_tasks.weather.pipeline.steps.step_logger import log_start
                    created = 0
                    for h in confirmed:
                        for m in miners:
                            try:
                                await log_start(
                                    db,
                                    run_id=int(rid),
                                    miner_uid=int(m["miner_uid"]),
                                    miner_hotkey=m.get("miner_hotkey") or "unknown",
                                    step_name="era5",
                                    substep="score",
                                    lead_hours=int(h),
                                )
                                created += 1
                            except Exception:
                                pass
                    await db.complete_validator_job(job["id"], result={"scheduled_leads": confirmed, "rows": created})
                    return True
                except Exception as e:
                    await db.fail_validator_job(job["id"], f"truth_guard_exception: {e}", schedule_retry_in_seconds=6*3600)
                    return False
            elif j == "era5.era5_truth_guard_all":
                # Global guard: scan all unfinished runs and schedule per-lead ERA5 steps when truth is ready
                try:
                    from gaia.tasks.defined_tasks.weather.pipeline.steps.era5_step import _get_era5_truth
                    from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
                    from gaia.tasks.defined_tasks.weather.pipeline.steps.step_logger import log_start
                    t = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
                    runs = await db.fetch_all(
                        """
                        SELECT id, gfs_init_time_utc
                        FROM weather_forecast_runs
                        WHERE status NOT IN ('completed','failed')
                        ORDER BY id
                        """
                    )
                    total_created = 0
                    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                    now_utc = _dt.now(_tz.utc)
                    leads_default = t.config.get("final_scoring_lead_hours", [24,48,72,96,120,144,168,192,216,240])
                    delay_days = int(t.config.get("era5_delay_days", 5))
                    buffer_hours = int(t.config.get("era5_buffer_hours", 6))
                    def _needed_time(gfs_init, h: int):
                        return gfs_init + _td(hours=h) + _td(days=delay_days) + _td(hours=buffer_hours)
                    # Track dates found unavailable this cycle to avoid duplicate failing fetches
                    unavailable_dates = set()  # set of 'YYYY-MM-DD'
                    import numpy as np
                    for run in runs:
                        rid = int(run["id"])
                        gfs_init = run.get("gfs_init_time_utc")
                        if not gfs_init:
                            continue
                        time_ready_leads = [h for h in leads_default if now_utc >= _needed_time(gfs_init, h)]
                        if not time_ready_leads:
                            continue
                        # Skip leads whose date was already found unavailable
                        filtered_leads = []
                        for h in time_ready_leads:
                            dt = gfs_init + _td(hours=h)
                            dkey = dt.strftime("%Y-%m-%d")
                            if dkey in unavailable_dates:
                                continue
                            filtered_leads.append(h)
                        if not filtered_leads:
                            continue
                        ds = await _get_era5_truth(t, rid, gfs_init, filtered_leads)
                        if ds is None:
                            continue
                        try:
                            ds_times = getattr(ds, "indexes", None) and ds.indexes.get("time")
                            if ds_times is None:
                                ds_times = getattr(ds, "time", None) and ds["time"].values
                            arr = np.array(ds_times) if ds_times is not None else None
                        except Exception:
                            arr = None
                        confirmed = []
                        for h in filtered_leads:
                            target_dt = gfs_init + _td(hours=h)
                            ok = True
                            if arr is not None:
                                try:
                                    dt64 = np.datetime64(target_dt.replace(tzinfo=None))
                                    ok = np.any(arr == dt64)
                                except Exception:
                                    ok = False
                            if ok:
                                confirmed.append(h)
                            else:
                                try:
                                    unavailable_dates.add(target_dt.strftime("%Y-%m-%d"))
                                except Exception:
                                    pass
                        if not confirmed:
                            continue
                        miners = await db.fetch_all(
                            "SELECT DISTINCT miner_uid, miner_hotkey FROM weather_miner_responses WHERE run_id = :rid",
                            {"rid": rid},
                        )
                        # Build skip sets to avoid re-scoring already completed leads
                        try:
                            rows_succeeded = await db.fetch_all(
                                """
                                SELECT miner_uid, lead_hours
                                FROM weather_forecast_steps
                                WHERE run_id = :rid AND step_name = 'era5' AND substep = 'score' AND status = 'succeeded'
                                """,
                                {"rid": rid},
                            )
                        except Exception:
                            rows_succeeded = []
                        try:
                            rows_failed = await db.fetch_all(
                                """
                                SELECT miner_uid, lead_hours
                                FROM weather_forecast_steps
                                WHERE run_id = :rid AND step_name = 'era5' AND substep = 'score' AND status = 'failed'
                                """,
                                {"rid": rid},
                            )
                        except Exception:
                            rows_failed = []
                        try:
                            rows_scored = await db.fetch_all(
                                """
                                SELECT miner_uid, lead_hours
                                FROM weather_miner_scores
                                WHERE run_id = :rid AND lead_hours IS NOT NULL AND score_type LIKE 'era5_rmse_%'
                                GROUP BY miner_uid, lead_hours
                                """,
                                {"rid": rid},
                            )
                        except Exception:
                            rows_scored = []
                        succeeded_set = {(int(r.get("miner_uid")), int(r.get("lead_hours"))) for r in rows_succeeded if r and r.get("lead_hours") is not None}
                        failed_set = {(int(r.get("miner_uid")), int(r.get("lead_hours"))) for r in rows_failed if r and r.get("lead_hours") is not None}
                        scored_set = {(int(r.get("miner_uid")), int(r.get("lead_hours"))) for r in rows_scored if r and r.get("lead_hours") is not None}
                        try:
                            logger.info(
                                f"[ERA5 Guard] Run {rid}: confirmed leads {confirmed} — scheduling for {len(miners or [])} miners"
                            )
                        except Exception:
                            pass
                        run_created = 0
                        for h in confirmed:
                            for m in miners:
                                key = (int(m["miner_uid"]), int(h))
                                if key in succeeded_set or key in scored_set or key in failed_set:
                                    continue
                                try:
                                    await log_start(
                                        db,
                                        run_id=rid,
                                        miner_uid=int(m["miner_uid"]),
                                        miner_hotkey=m.get("miner_hotkey") or "unknown",
                                        step_name="era5",
                                        substep="score",
                                        lead_hours=int(h),
                                    )
                                    total_created += 1
                                    run_created += 1
                                except Exception:
                                    pass
                        try:
                            logger.info(
                                f"[ERA5 Guard] Run {rid}: created {run_created} step rows"
                            )
                        except Exception:
                            pass
                    if total_created > 0:
                        await db.complete_validator_job(job["id"], result={"scheduled_rows": total_created})
                    else:
                        # Reschedule in 1 hour to keep polling ERA5 availability
                        await db.fail_validator_job(job["id"], "no_work_now", schedule_retry_in_seconds=3600)
                    return True
                except Exception as e:
                    await db.fail_validator_job(job["id"], f"truth_guard_all_exception: {e}", schedule_retry_in_seconds=6*3600)
                    return False
            else:
                # Placeholder for other era5.* jobs
                await db.complete_validator_job(job["id"], result={"ok": True})
                return True
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    job = await db.claim_validator_job(worker_name=_pname, job_type_prefix="ops.", is_utility_worker=False)
    if job:
        try:
            try:
                logger.info(
                    f"claimed job id={job.get('id')} type={job.get('job_type')}"
                )
            except Exception:
                pass
            j = job.get("job_type")
            if j == "ops.recompute_scores_and_weights":
                try:
                    # Run the recompute pipeline inline using Python APIs
                    from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
                    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
                    # Rebuild combined rows based on stats for recent runs
                    # Find recent run ids
                    runs = await db.fetch_all("SELECT id FROM weather_forecast_runs ORDER BY id DESC LIMIT 32")
                    count = 0
                    for r in runs:
                        rid = int(r["id"]) if r and r.get("id") is not None else None
                        if rid is None:
                            continue
                        # Recompute overall for the run using the same helper as script
                        try:
                            from scripts.recompute_scores_and_weights import recompute_overall_for_run
                        except Exception:
                            recompute_overall_for_run = None
                        if recompute_overall_for_run is not None:
                            await recompute_overall_for_run(db, rid)
                        await task.update_combined_weather_scores(run_id_trigger=rid, force_phase="final")
                        count += 1
                    await db.complete_validator_job(job["id"], result={"recomputed_runs": count})
                    return True
                except Exception as e:
                    await db.fail_validator_job(job["id"], f"recompute exception: {e}", schedule_retry_in_seconds=3600)
                    return False
            else:
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


async def process_one_utility(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Utility-only worker loop: never claims weather.* jobs.

    Still performs step enqueuing (seed/era5) and miner poll job housekeeping,
    but strictly skips claiming any weather.* jobs (including orchestrate/query/poll).
    """
    try:
        # Barrier: ensure AutoSync isn't pausing DB
        if not await _autosync_startup_barrier(db):
            return False
        # Housekeeping
        try:
            await db.enqueue_miner_poll_jobs(limit=200)
        except Exception:
            pass
        # Only worker-1 does periodic step enqueues
        try:
            import multiprocessing as _mp
            _pname = _mp.current_process().name if _mp.current_process() else ""
            should_enqueue_steps = True
            try:
                if "worker-" in _pname and "/" in _pname:
                    _idx = int(_pname.split("worker-")[1].split("/")[0])
                    if _idx != 1:
                        should_enqueue_steps = False
            except Exception:
                pass
            if should_enqueue_steps:
                from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                interval_s = 60
                try:
                    interval_s = int(os.getenv("WEATHER_STEPS_ENQUEUE_INTERVAL_SECONDS", "60"))
                except Exception:
                    interval_s = 60
                last_at = getattr(validator, "_last_steps_enqueue_at", None) if validator is not None else None
                now = _dt.now(_tz.utc)
                if last_at is None or (now - last_at).total_seconds() >= interval_s:
                    lock = await db.fetch_one("SELECT pg_try_advisory_lock(:k) AS ok", {"k": 0x57465354})  # 'WFST'
                    if lock and lock.get("ok"):
                        try:
                            await db.enqueue_weather_step_jobs(limit=2000)
                        finally:
                            try:
                                await db.execute("SELECT pg_advisory_unlock(:k)", {"k": 0x57465354})
                            except Exception:
                                pass
                    if validator is not None:
                        try:
                            setattr(validator, "_last_steps_enqueue_at", now)
                        except Exception:
                            pass
        except Exception:
            pass

        # STRICT: Never claim weather.*
        _pname = None
        try:
            import multiprocessing as _mp
            _pname = _mp.current_process().name if _mp.current_process() else "utility"
        except Exception:
            _pname = "utility"

        # Claim only queues this utility worker actually handles
        for prefix in ("stats.", "metagraph.", "miners."):
            job = await db.claim_validator_job(worker_name=_pname, job_type_prefix=prefix, is_utility_worker=True)
            if job:
                try:
                    try:
                        logger.info(
                            f"claimed job id={job.get('id')} type={job.get('job_type')} run_id={job.get('run_id')} miner_uid={job.get('miner_uid')}"
                        )
                    except Exception:
                        pass
                    # Reuse the same handler block from process_one by delegating back into it
                    # but with a guard to ensure weather.* is not processed. Since we
                    # never claim weather.* here, we can just duplicate minimal handling.
                    jtype = job.get("job_type")
                    payload = job.get("payload") or {}
                    if isinstance(payload, str):
                        import json as _json
                        try:
                            payload = _json.loads(payload)
                        except Exception:
                            payload = {}
                    # Fast path: if somehow a weather.* slips in, release it
                    if isinstance(jtype, str) and jtype.startswith("weather."):
                        try:
                            await db.execute(
                                "UPDATE validator_jobs SET status='pending', claimed_by=NULL, started_at=NULL, lease_expires_at=NULL WHERE id = :jid",
                                {"jid": job["id"]},
                            )
                        except Exception:
                            await db.fail_validator_job(job["id"], "utility-only release", schedule_retry_in_seconds=2)
                        return True

                    if jtype == "stats.aggregate":
                        from gaia.tasks.defined_tasks.weather.pipeline.steps.aggregate_step import run_miner_aggregation

                        ok = await run_miner_aggregation(db, validator=validator)
                        if ok:
                            await db.complete_validator_job(job["id"], result={"ok": True})
                        else:
                            await db.fail_validator_job(job["id"], "stats aggregate failed")
                        return ok
                    if jtype == "stats.subnet_snapshot":
                        from gaia.tasks.defined_tasks.weather.pipeline.steps.aggregate_step import compute_subnet_stats
                        try:
                            snapshot = await compute_subnet_stats(db)
                            try:
                                logger.info(f"[SubnetSnapshot] active_miners={snapshot.get('active_miners')}, avg_forecast_score={snapshot.get('avg_forecast_score')}")
                            except Exception:
                                pass
                            await db.complete_validator_job(job["id"], result={"ok": True})
                            return True
                        except Exception as err:
                            logger.error(f"compute_subnet_stats failed: {err}", exc_info=True)
                            await db.fail_validator_job(job["id"], "stats snapshot failed")
                            return False
                    if isinstance(jtype, str) and jtype.startswith("metagraph."):
                        return await _handle_metagraph_job(job, db, validator)
                    if isinstance(jtype, str) and jtype.startswith("miners."):
                        return await _handle_miners_job(job, db, validator)

                    await db.fail_validator_job(job["id"], "unhandled utility job", schedule_retry_in_seconds=60)
                    return False
                except Exception as e:
                    await db.fail_validator_job(job["id"], f"exception: {e}")
                    return False

        return False
    except Exception:
        return False
