#!/usr/bin/env python3
import asyncio
import argparse
from typing import List, Dict, Any

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask


async def get_active_uid_to_hotkey(db: ValidatorDatabaseManager) -> Dict[int, str]:
    rows = await db.fetch_all(
        "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL AND uid >= 0 AND uid < 256"
    )
    mapping: Dict[int, str] = {}
    for r in rows:
        try:
            uid = int(r["uid"])
            hk = r.get("hotkey")
            if hk:
                mapping[uid] = hk
        except Exception:
            continue
    return mapping


async def has_valid_stats_for_uid_hotkey(
    db: ValidatorDatabaseManager, uid: int, hotkey: str, qc_threshold: float
) -> bool:
    # Check latest row for this uid + hotkey
    rec = await db.fetch_one(
        """
        SELECT overall_forecast_score, forecast_score_initial
        FROM weather_forecast_stats
        WHERE miner_uid = :uid AND miner_hotkey = :hk
        ORDER BY updated_at DESC NULLS LAST
        LIMIT 1
        """,
        {"uid": uid, "hk": hotkey},
    )
    if not rec:
        return False
    overall = rec.get("overall_forecast_score")
    day1 = rec.get("forecast_score_initial")
    if overall is not None and float(overall) > 0.0:
        return True
    if day1 is not None and float(day1) >= qc_threshold:
        return True
    return False


async def score_table_has_nonzero_for_uid(db: ValidatorDatabaseManager, uid: int) -> bool:
    col = f"uid_{uid}_score"
    row = await db.fetch_one(
        f"""
        SELECT COUNT(*) AS cnt
        FROM score_table
        WHERE task_name = 'weather' AND {col} > 0
        """
    )
    cnt = int(row["cnt"]) if row and row.get("cnt") is not None else 0
    return cnt > 0


async def zero_score_table_for_uid(db: ValidatorDatabaseManager, uid: int) -> int:
    col = f"uid_{uid}_score"
    res = await db.execute(
        f"""
        UPDATE score_table
        SET {col} = 0.0
        WHERE task_name = 'weather' AND {col} IS NOT NULL AND {col} != 0.0
        """
    )
    return getattr(res, "rowcount", 0) or 0


async def audit_and_fix(db: ValidatorDatabaseManager, fix: bool) -> Dict[str, Any]:
    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
    qc_threshold = float(task.config.get("day1_binary_threshold", 0.1))

    uid_to_hotkey = await get_active_uid_to_hotkey(db)
    audited: List[Dict[str, Any]] = []
    zeroed_total = 0

    for uid in range(256):
        has_nonzero = await score_table_has_nonzero_for_uid(db, uid)
        current_hk = uid_to_hotkey.get(uid)

        if not has_nonzero:
            continue

        if not current_hk:
            # No active miner occupies this UID → zero
            rc = 0
            if fix:
                rc = await zero_score_table_for_uid(db, uid)
                zeroed_total += rc
            audited.append(
                {"uid": uid, "reason": "no_active_hotkey", "zeroed_rows": rc}
            )
            continue

        valid = await has_valid_stats_for_uid_hotkey(db, uid, current_hk, qc_threshold)
        if not valid:
            rc = 0
            if fix:
                rc = await zero_score_table_for_uid(db, uid)
                zeroed_total += rc
            audited.append(
                {"uid": uid, "reason": "no_matching_stats", "zeroed_rows": rc}
            )

    return {"audited_count": len(audited), "zeroed_total": zeroed_total, "details": audited}


async def main():
    parser = argparse.ArgumentParser(description="Audit and optionally fix score_table weather columns against stats")
    parser.add_argument("--fix", action="store_true", help="Apply zeroing for mismatches")
    args = parser.parse_args()

    db = ValidatorDatabaseManager()
    await db.initialize_database()
    result = await audit_and_fix(db, args.fix)
    print(result)


if __name__ == "__main__":
    asyncio.run(main())


