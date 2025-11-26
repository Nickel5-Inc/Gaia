#!/usr/bin/env python3
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import sqlalchemy as sa
import random

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline.steps.step_logger import schedule_retry
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask


EXPECTED_LEADS = [24, 48, 72, 96, 120, 144, 168, 192, 216, 240]


async def _is_miner_registered(db: ValidatorDatabaseManager, uid: int) -> bool:
    row = await db.fetch_one("SELECT 1 FROM node_table WHERE uid = :u AND hotkey IS NOT NULL", {"u": uid})
    return bool(row)


async def reconcile_run(db: ValidatorDatabaseManager, run_id: int) -> int:
    """Backfill missing ERA5 step rows and schedule limited retries respecting policy.

    - Creates missing weather_forecast_steps rows for each expected lead where a miner had a verified/ready response
      and where earlier leads are not exhausted (retry_count < 3).
    - Schedules a retry with next_retry_time >= now+30min for newly created steps.
    - Does not create or enqueue steps for unregistered miners.
    - Skips scheduling if a step is already queued or succeeded.
    """
    inserted = 0

    # Load run gfs_init_time and weather config to evaluate truth availability
    run_row = await db.fetch_one(
        sa.text("SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :rid"),
        {"rid": run_id},
    )
    if not run_row or not run_row.get("gfs_init_time_utc"):
        return 0
    gfs_init = run_row["gfs_init_time_utc"]

    # Use WeatherTask config to mirror guard timing
    wt = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
    delay_days = int(wt.config.get("era5_delay_days", 5))
    buffer_hours = int(wt.config.get("era5_buffer_hours", 6))
    now_utc = datetime.now(timezone.utc)

    def lead_truth_ready(lead_h: int) -> bool:
        ready_at = gfs_init + timedelta(hours=lead_h) + timedelta(days=delay_days) + timedelta(hours=buffer_hours)
        return now_utc >= ready_at

    # Candidate miners: verified or acceptable response states
    candidates = await db.fetch_all(
        sa.text(
            """
            SELECT r.run_id, r.miner_uid, r.miner_hotkey, r.verification_passed, r.status AS response_status, r.id AS response_id
            FROM weather_miner_responses r
            WHERE r.run_id = :rid
              AND COALESCE(r.verification_passed, FALSE) = TRUE
              AND r.status <> 'verification_failed'
            """
        ),
        {"rid": run_id},
    )

    for r in candidates:
        uid = int(r["miner_uid"]) if r.get("miner_uid") is not None else None
        if uid is None:
            continue
        if not await _is_miner_registered(db, uid):
            continue
        # Skip any miner responses that failed manifest verification or are in a failed state
        if r.get("response_status") in {"verification_failed", "failed", "error"}:
            continue

        # Enforce dependency: if any earlier lead exhausted retries, do not create later leads
        exhausted = await db.fetch_one(
            sa.text(
                """
                SELECT 1 FROM weather_forecast_steps s
                WHERE s.run_id = :rid AND s.miner_uid = :uid AND s.step_name='era5' AND s.substep='score'
                  AND s.retry_count >= 3 AND s.status = 'failed'
                LIMIT 1
                """
            ),
            {"rid": run_id, "uid": uid},
        )
        if exhausted:
            continue

        # Determine which leads are already present
        present_rows = await db.fetch_all(
            sa.text(
                """
                SELECT lead_hours, status FROM weather_forecast_steps
                WHERE run_id = :rid AND miner_uid = :uid AND step_name='era5' AND substep='score'
                """
            ),
            {"rid": run_id, "uid": uid},
        )
        present_by_lead = {int(pr["lead_hours"]): pr["status"] for pr in present_rows if pr.get("lead_hours") is not None}

        # Reset stale in_progress steps (orphaned by worker crash/OOM)
        # A step is considered stale if it's in_progress and there's no active job with a valid lease
        for lh, st in list(present_by_lead.items()):
            if st == "in_progress":
                # Check if there's an active job for this step
                active_job = await db.fetch_one(
                    sa.text(
                        """
                        SELECT id FROM validator_jobs 
                        WHERE run_id = :rid AND miner_uid = :uid 
                          AND job_type = 'weather.era5'
                          AND status = 'in_progress'
                          AND lease_expires_at > NOW()
                        LIMIT 1
                        """
                    ),
                    {"rid": run_id, "uid": uid},
                )
                if not active_job:
                    # Check current retry count to decide whether to retry or fail permanently
                    step_info = await db.fetch_one(
                        sa.text(
                            """
                            SELECT retry_count FROM weather_forecast_steps
                            WHERE run_id = :rid AND miner_uid = :uid 
                              AND step_name = 'era5' AND substep = 'score' AND lead_hours = :lh
                            """
                        ),
                        {"rid": run_id, "uid": uid, "lh": int(lh)},
                    )
                    current_retry = int(step_info.get("retry_count") or 0) if step_info else 0
                    max_retries = 3
                    
                    if current_retry >= max_retries:
                        # Max retries exceeded - mark as permanently failed
                        try:
                            await db.execute(
                                sa.text(
                                    """
                                    UPDATE weather_forecast_steps
                                    SET status = 'failed', 
                                        job_id = NULL,
                                        completed_at = NOW(),
                                        error_json = jsonb_build_object(
                                            'type', 'max_retries_exceeded',
                                            'message', 'Step failed after max retries - likely persistent OOM or data issue',
                                            'retry_count', retry_count,
                                            'failed_at', NOW()::text
                                        )
                                    WHERE run_id = :rid AND miner_uid = :uid 
                                      AND step_name = 'era5' AND substep = 'score' AND lead_hours = :lh
                                      AND status = 'in_progress'
                                    """
                                ),
                                {"rid": run_id, "uid": uid, "lh": int(lh)},
                            )
                            present_by_lead[lh] = "failed"
                        except Exception:
                            pass
                    else:
                        # Still have retries left - reset to pending for another attempt
                        try:
                            await db.execute(
                                sa.text(
                                    """
                                    UPDATE weather_forecast_steps
                                    SET status = 'pending', 
                                        job_id = NULL,
                                        retry_count = COALESCE(retry_count, 0) + 1,
                                        error_json = jsonb_build_object(
                                            'type', 'orphaned_step_recovered',
                                            'message', 'Step was in_progress with no active job - likely worker crash',
                                            'recovered_at', NOW()::text
                                        )
                                    WHERE run_id = :rid AND miner_uid = :uid 
                                      AND step_name = 'era5' AND substep = 'score' AND lead_hours = :lh
                                      AND status = 'in_progress'
                                    """
                                ),
                                {"rid": run_id, "uid": uid, "lh": int(lh)},
                            )
                            present_by_lead[lh] = "pending"
                            inserted += 1  # Count as reconciled
                        except Exception:
                            pass

        # Flip not-ready retry_scheduled to waiting_for_truth with precise ready_at
        for lh, st in list(present_by_lead.items()):
            if st == "retry_scheduled" and not lead_truth_ready(lh):
                ready_at = gfs_init + timedelta(hours=lh) + timedelta(days=delay_days) + timedelta(hours=buffer_hours)
                # Add jitter 0..3600 seconds to spread load
                ready_at = ready_at + timedelta(seconds=random.randint(0, 3600))
                try:
                    await db.execute(
                        sa.text(
                            """
                            UPDATE weather_forecast_steps
                            SET status = 'waiting_for_truth', next_retry_time = :nrt
                            WHERE run_id = :rid AND miner_uid = :uid AND step_name='era5' AND substep='score' AND lead_hours = :lh
                            """
                        ),
                        {"rid": run_id, "uid": uid, "lh": int(lh), "nrt": ready_at},
                    )
                    # Reflect change locally to avoid re-scheduling below
                    present_by_lead[lh] = "waiting_for_truth"
                except Exception:
                    pass

        # Also skip leads already scored in stats (avoid duplicate work)
        stats_row = await db.fetch_one(
            sa.text(
                """
                SELECT era5_score_24h, era5_score_48h, era5_score_72h, era5_score_96h,
                       era5_score_120h, era5_score_144h, era5_score_168h, era5_score_192h,
                       era5_score_216h, era5_score_240h
                FROM weather_forecast_stats
                WHERE run_id = :rid AND miner_uid = :uid
                """
            ),
            {"rid": run_id, "uid": uid},
        )
        scored_by_lead: Dict[int, bool] = {}
        if stats_row:
            cols = [24,48,72,96,120,144,168,192,216,240]
            for idx, lh in enumerate(cols):
                val = list(stats_row.values())[idx]
                scored_by_lead[lh] = val is not None

        # Choose only the earliest pending available lead to reduce churn
        pending_available: List[int] = []
        for lh in EXPECTED_LEADS:
            if scored_by_lead.get(lh):
                continue
            if present_by_lead.get(lh) in {"queued", "in_progress", "succeeded", "failed"}:
                continue
            # Also skip if already have too many jobs for this step
            job_count = await db.fetch_one(
                sa.text(
                    """
                    SELECT COUNT(*) as cnt FROM validator_jobs
                    WHERE run_id = :rid AND miner_uid = :uid AND job_type = 'weather.era5'
                    """
                ),
                {"rid": run_id, "uid": uid},
            )
            if job_count and int(job_count.get("cnt", 0)) >= 5:
                continue  # Stop creating jobs after 5 attempts
            if not lead_truth_ready(lh):
                continue
            pending_available.append(lh)

        if pending_available:
            lh = min(pending_available)
            # Check existing retry count for this step before scheduling
            existing_step = await db.fetch_one(
                sa.text(
                    """
                    SELECT retry_count FROM weather_forecast_steps
                    WHERE run_id = :rid AND miner_uid = :uid AND step_name = 'era5' AND substep = 'score' AND lead_hours = :lh
                    """
                ),
                {"rid": run_id, "uid": uid, "lh": lh},
            )
            existing_retry_count = int(existing_step.get("retry_count") or 0) if existing_step else 0
            if existing_retry_count >= 5:
                continue  # Don't reschedule if already retried too many times
            
            # Jitter 0..3600s to prevent thundering herd (no fixed 30 min delay)
            next_time = now_utc + timedelta(seconds=random.randint(0, 3600))
            await schedule_retry(
                db,
                run_id=run_id,
                miner_uid=uid,
                miner_hotkey=r.get("miner_hotkey", "unknown"),
                step_name="era5",
                substep="score",
                lead_hours=lh,
                error_json={"type": "reconciled_missing_step", "message": "scheduled first attempt after availability"},
                retry_count=existing_retry_count + 1,  # Increment existing count, not reset to 1
                next_retry_time=next_time,
            )
            inserted += 1

    # After backfilling steps, enqueue jobs for due steps via helper
    await db.enqueue_weather_step_jobs(limit=500)
    return inserted


async def main():
    db = ValidatorDatabaseManager()
    # Fetch all runs to reconcile
    runs = await db.fetch_all("SELECT id FROM weather_forecast_runs ORDER BY id ASC")
    total = 0
    for r in runs:
        rid = int(r["id"])
        n = await reconcile_run(db, rid)
        print({"run": rid, "reconciled_steps": n})
        total += n
    print({"total_reconciled_steps": total})


if __name__ == "__main__":
    asyncio.run(main())


