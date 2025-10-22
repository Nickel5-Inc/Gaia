#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def _preview(conn, run_id: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Return two lists:
    - jobs_to_reset: validator_jobs rows eligible to reset to pending (failed/cancelled, no active duplicate)
    - miners_to_insert: miners without an active query job whose response is failed
    """
    # Preview jobs to reset
    q_reset = text(
        """
        WITH active AS (
          SELECT DISTINCT miner_uid
          FROM validator_jobs
          WHERE run_id = :rid
            AND job_type = 'weather.query_miner'
            AND status IN ('pending','in_progress','retry_scheduled')
        )
        SELECT v.id, v.miner_uid, v.status, COALESCE(v.result, '{}'::jsonb) AS result
        FROM validator_jobs v
        WHERE v.run_id = :rid
          AND v.job_type = 'weather.query_miner'
          AND (
            v.status = 'failed'
            OR (v.status = 'completed' AND (v.result ->> 'cancelled') = 'miner_not_found')
          )
          AND (v.miner_uid IS NOT NULL)
          AND NOT EXISTS (
            SELECT 1 FROM active a WHERE a.miner_uid = v.miner_uid
          )
        ORDER BY v.miner_uid
        """
    )
    res_reset = await conn.execute(q_reset, {"rid": run_id})
    jobs_to_reset = [dict(r) for r in res_reset.mappings().all()]

    # Preview miners to insert
    q_insert_preview = text(
        """
        SELECT r.miner_uid,
               COALESCE(nt.hotkey, r.miner_hotkey) AS miner_hotkey
        FROM weather_miner_responses r
        LEFT JOIN validator_jobs v
          ON v.run_id = r.run_id
         AND v.miner_uid = r.miner_uid
         AND v.job_type = 'weather.query_miner'
         AND v.status IN ('pending','in_progress','retry_scheduled')
        LEFT JOIN node_table nt ON nt.uid = r.miner_uid
        WHERE r.run_id = :rid
          AND r.status = 'failed'
          AND v.id IS NULL
        ORDER BY r.miner_uid
        """
    )
    res_insert = await conn.execute(q_insert_preview, {"rid": run_id})
    miners_to_insert = [dict(r) for r in res_insert.mappings().all()]

    return jobs_to_reset, miners_to_insert


async def _execute(
    conn,
    *,
    run_id: int,
    validator_hotkey: str,
    do_metagraph_sync: bool,
    insert_priority: int,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {"metagraph_sync": False, "reset_count": 0, "inserted_count": 0}

    if do_metagraph_sync:
        q_meta = text(
            """
            INSERT INTO validator_jobs (singleton_key, job_type, priority, status, payload, scheduled_at)
            SELECT 'metagraph_sync', 'metagraph.sync', 150, 'pending', '{}'::jsonb, NOW()
            WHERE NOT EXISTS (
              SELECT 1 FROM validator_jobs
              WHERE singleton_key = 'metagraph_sync'
                AND status IN ('pending','in_progress','retry_scheduled')
            )
            RETURNING id
            """
        )
        res = await conn.execute(q_meta)
        created = res.mappings().first()
        results["metagraph_sync"] = bool(created)

    # Reset failed/cancelled jobs back to pending, avoiding duplicates per miner
    q_reset = text(
        """
        UPDATE validator_jobs v
        SET status = 'pending',
            scheduled_at = NOW(),
            next_retry_at = NULL,
            last_error = NULL,
            completed_at = NULL
        WHERE v.run_id = :rid
          AND v.job_type = 'weather.query_miner'
          AND (
            v.status = 'failed'
            OR (v.status = 'completed' AND (v.result ->> 'cancelled') = 'miner_not_found')
          )
          AND (v.miner_uid IS NOT NULL)
          AND NOT EXISTS (
            SELECT 1 FROM validator_jobs x
            WHERE x.run_id = :rid
              AND x.job_type = 'weather.query_miner'
              AND x.miner_uid = v.miner_uid
              AND x.status IN ('pending','in_progress','retry_scheduled')
          )
        """
    )
    res_reset = await conn.execute(q_reset, {"rid": run_id})
    results["reset_count"] = res_reset.rowcount or 0

    # Insert new query jobs for failed responses without an active job, using singleton_key to avoid dupes
    q_insert = text(
        """
        INSERT INTO validator_jobs (singleton_key, job_type, priority, status, payload, scheduled_at, run_id, miner_uid)
        SELECT 
          'query_miner_run_'||r.run_id||'_miner_'||r.miner_uid AS singleton_key,
          'weather.query_miner' AS job_type,
          :pri AS priority,
          'pending' AS status,
          jsonb_build_object(
            'run_id', r.run_id,
            'miner_uid', r.miner_uid,
            'miner_hotkey', COALESCE(nt.hotkey, r.miner_hotkey),
            'validator_hotkey', CAST(:vhk AS text)
          ) AS payload,
          NOW() AS scheduled_at,
          r.run_id,
          r.miner_uid
        FROM weather_miner_responses r
        LEFT JOIN node_table nt ON nt.uid = r.miner_uid
        WHERE r.run_id = :rid
          AND r.status = 'failed'
          AND NOT EXISTS (
            SELECT 1 FROM validator_jobs v
            WHERE v.singleton_key = 'query_miner_run_'||r.run_id||'_miner_'||r.miner_uid
              AND v.status IN ('pending','in_progress','retry_scheduled')
          )
        RETURNING id, miner_uid
        """
    )
    res_insert = await conn.execute(q_insert, {"rid": run_id, "vhk": validator_hotkey, "pri": int(insert_priority)})
    inserted_rows = res_insert.mappings().all()
    results["inserted_count"] = len(inserted_rows)

    return results


async def requeue_failed_miners(
    *,
    run_id: int,
    db_url: str,
    execute: bool,
    skip_metagraph_sync: bool,
    validator_hotkey: str,
    priority: int,
    verbose: bool,
) -> None:
    engine = create_async_engine(db_url, future=True)
    try:
        # Dry-run preview
        async with engine.connect() as conn:
            jobs_to_reset, miners_to_insert = await _preview(conn, run_id)
            print(f"[DryRun] Run {run_id}: {len(jobs_to_reset)} jobs eligible to reset, {len(miners_to_insert)} miners eligible for new query jobs")
            if verbose and jobs_to_reset:
                for r in jobs_to_reset[:25]:
                    print(f"  reset_job id={r['id']} miner_uid={r['miner_uid']} status={r['status']} cancelled={r.get('result', {}).get('cancelled')}")
            if verbose and miners_to_insert:
                for m in miners_to_insert[:25]:
                    print(f"  insert_job miner_uid={m['miner_uid']} miner_hotkey={str(m.get('miner_hotkey') or '')[:12]}")

        if not execute:
            print("[DryRun] No changes applied. Use --execute to perform updates.")
            return

        # Execute within a transaction
        async with engine.begin() as conn:
            results = await _execute(
                conn,
                run_id=run_id,
                validator_hotkey=validator_hotkey,
                do_metagraph_sync=(not skip_metagraph_sync),
                insert_priority=priority,
            )
            print(
                f"[Applied] Run {run_id}: metagraph_sync_created={results['metagraph_sync']} "
                f"reset_jobs={results['reset_count']} inserted_jobs={results['inserted_count']}"
            )
    finally:
        await engine.dispose()


async def _discover_latest_failed_run(db_url: str) -> Optional[int]:
    engine = create_async_engine(db_url, future=True)
    try:
        async with engine.connect() as conn:
            res = await conn.execute(
                text("SELECT run_id FROM weather_miner_responses WHERE status = 'failed' ORDER BY run_id DESC LIMIT 1")
            )
            row = res.mappings().first()
            return int(row["run_id"]) if row and row.get("run_id") is not None else None
    finally:
        await engine.dispose()


async def async_main(args: argparse.Namespace) -> None:
    db_url = args.db_url
    rid: Optional[int] = args.run_id
    if rid is None and args.auto_latest_failed:
        rid = await _discover_latest_failed_run(db_url)
        if rid is None:
            print("No run with failed miner responses found.")
            return
        print(f"[Auto] Using latest run with failures: {rid}")
    if rid is None:
        print("Error: run_id is required (or use --auto-latest-failed)")
        return
    await requeue_failed_miners(
        run_id=int(rid),
        db_url=db_url,
        execute=bool(args.execute),
        skip_metagraph_sync=bool(args.skip_metagraph_sync),
        validator_hotkey=str(args.validator_hotkey),
        priority=int(args.priority),
        verbose=bool(args.verbose),
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Requeue failed miner query jobs for a specific run.")
    p.add_argument("run_id", type=int, nargs="?", help="Run ID to operate on")
    p.add_argument(
        "--db-url",
        type=str,
        default=os.environ.get("VALIDATOR_DB_URL", "postgresql+asyncpg://postgres:postgres@localhost/validator_db"),
        help="SQLAlchemy async DB URL for validator DB",
    )
    p.add_argument("--auto-latest-failed", dest="auto_latest_failed", action="store_true", help="Use the latest run_id with failed miner responses")
    p.add_argument("--execute", action="store_true", help="Apply changes (default is dry-run)")
    p.add_argument("--skip-metagraph-sync", action="store_true", help="Do not enqueue metagraph.sync singleton job")
    p.add_argument("--validator-hotkey", type=str, default=os.environ.get("VALIDATOR_HOTKEY", "unknown_validator"))
    p.add_argument("--priority", type=int, default=80, help="Priority for inserted weather.query_miner jobs")
    p.add_argument("--verbose", action="store_true", help="Show candidate details in dry-run")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
