#!/usr/bin/env python3
"""
Weather Scoring Parameter Sweep
================================

Purpose
-------
Evaluate the effect of day1 clone-penalty unification, strict near-GFS clamp,
and tiered day1 weighting on historical runs. This script fetches recent
component scores and stats from the validator database and recomputes
aggregate signals under different configurations to guide defaults.

Safely runnable on main server (read-only). Uses environment variables for DB
connection and sweep parameter sets. Outputs a concise report and JSON results.

Key metrics
-----------
- day1_clamped_rate: fraction of responses triggering strict clamp
- day1_gfs_like_mean: mean day1 of suspected GFS-like miners after clamp
- no_era5_weight_share: total share of contributions from miners w/o ERA5
- tau_correlation: Kendall tau between day1-only ranks vs final ERA5 ranks
- false_clamp_rate: fraction of miners that later perform well in ERA5 but were clamped

Environment
-----------
- DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD
- SWEEP_GAMMAS (csv), SWEEP_DELTA_SCALES (csv), SWEEP_TIER_FACTORS (csv),
  SWEEP_DAY1_CAPS (csv), SWEEP_CLAMP_FRACS (csv)

Usage
-----
python -m scripts.weather_sweep --runs 10 --out /tmp/weather_sweep.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


def _env_csv(name: str, default_csv: str) -> List[float]:
    raw = os.getenv(name, default_csv)
    vals: List[float] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            vals.append(float(tok))
        except Exception:
            pass
    return vals


async def _create_session() -> async_sessionmaker:
    db = os.getenv("DB_NAME", "validator_db")
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "5432"))
    user = os.getenv("DB_USER", "postgres")
    pw = os.getenv("DB_PASSWORD", "postgres")
    url = f"postgresql+asyncpg://{user}:{pw}@{host}:{port}/{db}"
    engine = create_async_engine(url, pool_size=8, max_overflow=4, pool_timeout=30)
    return async_sessionmaker(engine, expire_on_commit=False)


async def _fetch_recent_runs(session_maker, limit_runs: int, ignore_status: bool = False) -> List[int]:
    async with session_maker() as s:
        if ignore_status:
            sql = "SELECT id FROM weather_forecast_runs ORDER BY id DESC LIMIT :lim"
            params = {"lim": int(limit_runs)}
        else:
            sql = (
                "SELECT id FROM weather_forecast_runs "
                "WHERE status IN (" 
                "'completed','final_scores_compiled','initial_scores_compiled','era5_scoring','day1_scoring_started','day1_scored'" 
                ") ORDER BY id DESC LIMIT :lim"
            )
            params = {"lim": int(limit_runs)}
        rows = (await s.execute(sa.text(sql), params)).fetchall()
        return [int(r.id) for r in rows]


async def _fetch_data_for_runs(session_maker, run_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    data: Dict[int, Dict[str, Any]] = {}
    if not run_ids:
        return data
    async with session_maker() as s:
        for r in run_ids:
            data[r] = {"components": {}, "stats": {}}
            # Component scores (day1 + era5 leads)
            rows_cs = (await s.execute(sa.text(
                """
                SELECT run_id, miner_uid, variable_name, pressure_level, lead_hours,
                       rmse, mse, acc, skill_score, skill_score_gfs, skill_score_climatology,
                       clone_penalty, valid_time_utc
                FROM weather_forecast_component_scores
                WHERE run_id = :rid
                """
            ), {"rid": r})).fetchall()
            for row in rows_cs:
                uid = int(row.miner_uid)
                data[r]["components"].setdefault(uid, []).append(dict(row._mapping))

            # Overall stats per run/miner including era5 combined
            rows_stats = (await s.execute(sa.text(
                """
                SELECT run_id, miner_uid, forecast_score_initial, era5_combined_score
                FROM weather_forecast_stats
                WHERE run_id = :rid
                """
            ), {"rid": r})).fetchall()
            for row in rows_stats:
                uid = int(row.miner_uid)
                data[r]["stats"].setdefault(uid, dict(row._mapping))
    return data


def _is_gfs_like(component_rows: List[Dict[str, Any]], delta_map: Dict[str, float], strict_frac: float) -> bool:
    # Treat as GFS-like if any critical var has implied clone-distance < strict_frac * delta
    # We approximate via clone_penalty>0 OR mse < strict_frac*delta when available
    for row in component_rows:
        var_name = str(row.get("variable_name") or "")
        delta = delta_map.get(var_name)
        if delta is None:
            continue
        cp = row.get("clone_penalty")
        if cp is not None and isinstance(cp, (int, float)) and cp > 0:
            return True
        mse = row.get("mse")
        try:
            if mse is not None and float(mse) < strict_frac * float(delta):
                return True
        except Exception:
            pass
    return False


def _kendall_tau(a: List[int], b: List[int]) -> float:
    # Simple Kendall tau approximation using pairwise comparisons
    # a and b are rank lists aligned by the same set of miners
    n = len(a)
    if n < 2:
        return 0.0
    num_conc = 0
    num_disc = 0
    for i in range(n):
        for j in range(i + 1, n):
            s1 = math.copysign(1, a[i] - a[j]) if a[i] != a[j] else 0
            s2 = math.copysign(1, b[i] - b[j]) if b[i] != b[j] else 0
            if s1 == 0 or s2 == 0:
                continue
            if s1 == s2:
                num_conc += 1
            else:
                num_disc += 1
    denom = num_conc + num_disc
    return (num_conc - num_disc) / denom if denom > 0 else 0.0


def _recompute_scores(
    run_bundle: Dict[str, Any],
    gamma: float,
    delta_scale: float,
    strict_frac: float,
    day1_cap: float,
    tier_factor: float,
    base_W_day1: float = 0.2,
    base_W_era5: float = 0.8,
    default_deltas: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    default_deltas = default_deltas or {"2t": 0.0025, "msl": 400, "10u": 0.01, "10v": 0.01, "z500": 100}
    deltas = {k: v * delta_scale for k, v in default_deltas.items()}

    components: Dict[int, List[Dict[str, Any]]] = run_bundle.get("components", {})
    stats: Dict[int, Dict[str, Any]] = run_bundle.get("stats", {})

    # per-uid signals
    day1_values: Dict[int, float] = {}
    has_era5: Dict[int, bool] = {}
    era5_values: Dict[int, float] = {}

    for uid, rows in components.items():
        # aggregate skill/acc and clone penalty (proxy) across rows
        skills = [r.get("skill_score") for r in rows if r.get("skill_score") is not None]
        accs = [r.get("acc") for r in rows if r.get("acc") is not None]
        avg_skill = float(np.nanmean(skills)) if skills else 0.0
        avg_acc = float(np.nanmean(accs)) if accs else 0.0

        # novelty penalty proxy: use clone_penalty or mse/delta
        penalties: List[float] = []
        for r in rows:
            var_name = str(r.get("variable_name") or "")
            delta = deltas.get(var_name)
            if delta is None:
                continue
            cp = r.get("clone_penalty")
            if isinstance(cp, (int, float)) and cp is not None:
                penalties.append(float(cp))
            else:
                mse = r.get("mse")
                try:
                    if mse is not None and delta > 0:
                        # gamma*(1 - mse/delta) if mse<delta else 0
                        p = gamma * max(0.0, 1.0 - float(mse) / float(delta))
                        penalties.append(p)
                except Exception:
                    pass
        avg_penalty = float(np.nanmean(penalties)) if penalties else 0.0

        # day1 aggregate with penalty and clamp
        day1 = max(0.0, 0.6 * avg_skill + 0.4 * ((avg_acc + 1.0) / 2.0) - avg_penalty)

        # strict clamp if GFS-like
        if _is_gfs_like(rows, deltas, strict_frac):
            day1 = min(day1, day1_cap)

        day1_values[uid] = float(day1)
        srow = stats.get(uid) or {}
        ev = float(srow.get("era5_combined_score") or 0.0)
        era5_values[uid] = ev
        has_era5[uid] = ev > 0

    # compute overall with tiering
    W_day1_eff = {}
    overall = {}
    for uid in stats.keys():
        W1 = base_W_day1 if has_era5.get(uid, False) else min(base_W_day1 * tier_factor, day1_cap)
        W_day1_eff[uid] = W1
        overall[uid] = W1 * day1_values.get(uid, 0.0) + base_W_era5 * era5_values.get(uid, 0.0)

    return {
        "day1": day1_values,
        "era5": era5_values,
        "overall": overall,
        "W_day1_eff": W_day1_eff,
        "has_era5": has_era5,
    }


def _summarize(run_bundle: Dict[str, Any], recomputed: Dict[str, Any]) -> Dict[str, Any]:
    uids = list(run_bundle.get("stats", {}).keys())
    if not uids:
        return {"day1_clamped_rate": 0.0, "no_era5_weight_share": 0.0, "tau_correlation": 0.0, "false_clamp_rate": 0.0}

    day1 = recomputed["day1"]; overall = recomputed["overall"]
    has_era5 = recomputed["has_era5"]; W1 = recomputed["W_day1_eff"]

    # clamped proxy: day1 very small
    clamped = [uid for uid in uids if day1.get(uid, 0.0) <= 0.051]
    day1_clamped_rate = len(clamped) / max(1, len(uids))

    # share of day1 contribution from no-ERA5 uids
    total_contrib = sum(W1.get(uid, 0.0) * day1.get(uid, 0.0) for uid in uids)
    no_era5_contrib = sum(W1.get(uid, 0.0) * day1.get(uid, 0.0) for uid in uids if not has_era5.get(uid, False))
    no_era5_weight_share = (no_era5_contrib / total_contrib) if total_contrib > 0 else 0.0

    # rank correlation between day1-only and final ERA5
    # build aligned vectors
    d1_ranks = np.argsort(np.argsort([-day1.get(uid, 0.0) for uid in uids])).tolist()
    e_ranks = np.argsort(np.argsort([-recomputed["era5"].get(uid, 0.0) for uid in uids])).tolist()
    tau = _kendall_tau(d1_ranks, e_ranks)

    # false clamp: clamped but later high ERA5
    false_clamp = [uid for uid in clamped if recomputed["era5"].get(uid, 0.0) >= 0.5]
    false_clamp_rate = len(false_clamp) / max(1, len(clamped)) if clamped else 0.0

    return {
        "day1_clamped_rate": float(day1_clamped_rate),
        "no_era5_weight_share": float(no_era5_weight_share),
        "tau_correlation": float(tau),
        "false_clamp_rate": float(false_clamp_rate),
    }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=25, help="Number of recent runs to analyze")
    parser.add_argument("--out", type=str, default="weather_sweep.json", help="Output JSON path")
    parser.add_argument("--ignore-status", action="store_true", help="Ignore run status filter when selecting recent runs")
    parser.add_argument("--run-ids", type=str, default="", help="Comma-separated run IDs to analyze (overrides --runs selection)")
    args = parser.parse_args()

    session_maker = await _create_session()
    if args.run_ids.strip():
        run_ids = [int(x) for x in args.run_ids.split(",") if x.strip().isdigit()]
    else:
        run_ids = await _fetch_recent_runs(session_maker, args.runs, ignore_status=args.ignore_status)

    if not run_ids:
        print("No runs found. Try --ignore-status or specify --run-ids, and verify DB_* env vars point to main.")
        with open(args.out, "w") as f:
            json.dump({"sweep": [], "generated_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        return

    bundles = await _fetch_data_for_runs(session_maker, run_ids)

    # Diagnostics for empty data
    empty_runs = [r for r, b in bundles.items() if not b.get("components") and not b.get("stats")]
    if empty_runs:
        print(f"No component/stats rows for runs: {empty_runs[:10]}... (total {len(empty_runs)}).")

    gammas = _env_csv("SWEEP_GAMMAS", "1.0,2.0,3.0")
    delta_scales = _env_csv("SWEEP_DELTA_SCALES", "0.5,1.0,1.5")
    tier_factors = _env_csv("SWEEP_TIER_FACTORS", "0.15,0.2,0.3")
    day1_caps = _env_csv("SWEEP_DAY1_CAPS", "0.05,0.08,0.10")
    clamp_fracs = _env_csv("SWEEP_CLAMP_FRACS", "0.5,0.75")

    results: List[Dict[str, Any]] = []
    for run_id, bundle in bundles.items():
        for gamma in gammas:
            for ds in delta_scales:
                for tf in tier_factors:
                    for cap in day1_caps:
                        for cf in clamp_fracs:
                            recomputed = _recompute_scores(
                                bundle, gamma=gamma, delta_scale=ds, strict_frac=cf, day1_cap=cap, tier_factor=tf
                            )
                            summary = _summarize(bundle, recomputed)
                            results.append({
                                "run_id": run_id,
                                "gamma": gamma,
                                "delta_scale": ds,
                                "tier_factor": tf,
                                "day1_cap": cap,
                                "clamp_frac": cf,
                                **summary,
                            })

    # Aggregate summaries across runs by parameter set
    from collections import defaultdict
    agg: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)
    for row in results:
        key = (row["gamma"], row["delta_scale"], row["tier_factor"], row["day1_cap"], row["clamp_frac"])
        agg[key].append(row)

    final: List[Dict[str, Any]] = []
    for key, rows in agg.items():
        metrics = {
            "day1_clamped_rate": float(np.mean([r["day1_clamped_rate"] for r in rows])),
            "no_era5_weight_share": float(np.mean([r["no_era5_weight_share"] for r in rows])),
            "tau_correlation": float(np.mean([r["tau_correlation"] for r in rows])),
            "false_clamp_rate": float(np.mean([r["false_clamp_rate"] for r in rows])),
        }
        gamma, ds, tf, cap, cf = key
        final.append({
            "gamma": gamma,
            "delta_scale": ds,
            "tier_factor": tf,
            "day1_cap": cap,
            "clamp_frac": cf,
            **metrics,
        })

    out_path = args.out
    with open(out_path, "w") as f:
        json.dump({"sweep": final, "generated_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
    # Print a small top-k summary
    if not final:
        print("Sweep produced no results: data may be missing per-run components or stats. Try increasing --runs or use --run-ids.")
    else:
        final_sorted = sorted(final, key=lambda r: (r["no_era5_weight_share"], -r["tau_correlation"]))
        for r in final_sorted[:10]:
            print(json.dumps(r))


if __name__ == "__main__":
    asyncio.run(main())


