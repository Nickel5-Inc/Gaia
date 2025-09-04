from __future__ import annotations

from typing import Any, Dict, List, Optional


def map_weather_stats_to_api(stats_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not stats_row:
        return {}
    # Pass-through mapping to keep wire stable; schema accepts arbitrary JSON on server side for new fields
    # Only include known keys to avoid leaking unexpected types
    allowed_keys = {
        "miner_rank",
        "avg_forecast_score",
        "successful_forecasts",
        "failed_forecasts",
        "forecast_success_ratio",
        "host_reliability_ratio",
        "avg_day1_score",
        "avg_era5_score",
        "avg_era5_completeness",
        "consecutive_successes",
        "consecutive_failures",
    }
    return {k: stats_row.get(k) for k in allowed_keys}


def map_recent_runs_to_api(recent_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for r in recent_rows or []:
        result.append(
            {
                "run_id": r.get("forecast_run_id"),
                "init_time": r.get("forecast_init_time").isoformat() if r.get("forecast_init_time") else None,
                "status": r.get("forecast_status"),
                "day1_score": r.get("forecast_score_initial"),
                "era5_score": r.get("era5_combined_score"),
                "completeness": r.get("era5_completeness"),
                "hosting": r.get("hosting_status"),
                "latency_ms": r.get("hosting_latency_ms"),
            }
        )
    return result


def build_predictions_payload(
    miner: Dict[str, Any],
    weather_stats: Dict[str, Any],
    recent_runs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "minerHotKey": miner["hotkey"],
        "minerColdKey": miner.get("coldkey"),
        "geomagneticPredictions": [],
        "soilMoisturePredictions": [],
        "weatherStats": weather_stats,
        "weatherRecentRuns": recent_runs,
    }


