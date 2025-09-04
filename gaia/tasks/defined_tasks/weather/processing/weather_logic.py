import asyncio
import gc
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import uuid
import numpy as np
import xarray as xr
import pandas as pd
import fsspec
import jwt
import traceback
import ipaddress
import xskillscore as xs
from typing import TYPE_CHECKING, Any, Optional, Dict, List, Tuple

if TYPE_CHECKING:
    from ..weather_task import WeatherTask

# High-performance JSON operations for weather data
try:
    from gaia.utils.performance import dumps, loads
except ImportError:
    import json

    def dumps(obj, **kwargs):
        return json.dumps(obj, **kwargs)

    def loads(s):
        return json.loads(s)


from ..utils.remote_access import open_verified_remote_zarr_dataset
from ..utils.json_sanitizer import safe_json_dumps_for_db
from ..utils.era5_api import fetch_era5_data
from ..utils.gfs_api import fetch_gfs_data, GFS_SURFACE_VARS, GFS_ATMOS_VARS
from ..utils.variable_maps import AURORA_TO_GFS_VAR_MAP
from ..utils.hashing import compute_verification_hash
from ..weather_scoring.metrics import (
    calculate_rmse,
    calculate_mse_skill_score,
    calculate_acc,
    calculate_bias_corrected_forecast,
    _calculate_latitude_weights,
    perform_sanity_checks,
)
from gaia.utils.custom_logger import get_logger
from ..weather_scoring.scoring import VARIABLE_WEIGHTS
import sqlalchemy as sa
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager

logger = get_logger(__name__)


async def _cleanup_miner_records(
    task_instance: "WeatherTask", miner_hotkey: str, run_id: int
) -> bool:
    """
    Remove all records for a deregistered miner from the current run.

    Args:
        task_instance: WeatherTask instance
        miner_hotkey: The miner's hotkey to clean up
        run_id: The run ID to clean up from

    Returns:
        bool: True if cleanup was successful, False otherwise
    """
    try:
        logger.info(
            f"[MinerCleanup] Starting cleanup for deregistered miner {miner_hotkey} in run {run_id}"
        )

        # Delete miner scores
        await task_instance.db_manager.execute(
            "DELETE FROM weather_miner_scores WHERE run_id = :run_id AND miner_hotkey = :miner_hotkey",
            {"run_id": run_id, "miner_hotkey": miner_hotkey},
        )

        # Delete miner responses
        await task_instance.db_manager.execute(
            "DELETE FROM weather_miner_responses WHERE run_id = :run_id AND miner_hotkey = :miner_hotkey",
            {"run_id": run_id, "miner_hotkey": miner_hotkey},
        )

        logger.info(
            f"[MinerCleanup] Successfully cleaned up records for deregistered miner {miner_hotkey} in run {run_id}"
        )
        return True

    except Exception as e:
        logger.error(
            f"[MinerCleanup] Error cleaning up miner {miner_hotkey} records: {e}",
            exc_info=True,
        )
        return False


async def _cleanup_offline_miner_from_run(
    task_instance: "WeatherTask", miner_hotkey: str, run_id: int
) -> bool:
    """
    Remove a miner's participation from the current run (but keep historical data).
    This is for miners that are still registered but offline/unavailable.

    Args:
        task_instance: WeatherTask instance
        miner_hotkey: The miner's hotkey to remove from current run
        run_id: The run ID to remove miner from

    Returns:
        bool: True if removal was successful, False otherwise
    """
    try:
        logger.info(
            f"[OfflineMinerCleanup] Removing offline miner {miner_hotkey} from run {run_id}"
        )

        # Mark responses as failed rather than deleting them
        await task_instance.db_manager.execute(
            """UPDATE weather_miner_responses 
               SET status = 'miner_offline', error_message = 'Miner offline during scoring' 
               WHERE run_id = :run_id AND miner_hotkey = :miner_hotkey""",
            {"run_id": run_id, "miner_hotkey": miner_hotkey},
        )

        # Delete any pending scores for this run
        await task_instance.db_manager.execute(
            "DELETE FROM weather_miner_scores WHERE run_id = :run_id AND miner_hotkey = :miner_hotkey",
            {"run_id": run_id, "miner_hotkey": miner_hotkey},
        )

        logger.info(
            f"[OfflineMinerCleanup] Successfully removed offline miner {miner_hotkey} from run {run_id}"
        )
        return True

    except Exception as e:
        logger.error(
            f"[OfflineMinerCleanup] Error removing offline miner {miner_hotkey} from run: {e}",
            exc_info=True,
        )
        return False


# Cache for run completion checks to avoid spamming logs
_run_completion_cache = {}
_run_completion_cache_ttl = 60  # seconds

async def _check_run_completion(task_instance: "WeatherTask", run_id: int) -> bool:
    """
    Check if a run should be marked as completed based on miner participation.
    A run is considered complete if:
    1. At least one miner has been successfully scored (even if others are pending)
    2. All verified miners have been processed (for ERA5 scoring)
    3. All miners have either scored or failed

    Args:
        task_instance: WeatherTask instance
        run_id: The run ID to check

    Returns:
        bool: True if run should be completed, False if it should continue
    """
    import time
    
    # Check cache to avoid spamming the same check
    cache_key = f"run_{run_id}"
    now = time.time()
    if cache_key in _run_completion_cache:
        cached_time, cached_result = _run_completion_cache[cache_key]
        if now - cached_time < _run_completion_cache_ttl:
            # Return cached result without logging
            return cached_result
    
    try:
        # Check total miners in run
        total_miners_query = """
        SELECT COUNT(*) as total_count FROM weather_miner_responses 
        WHERE run_id = :run_id
        """
        total_result = await task_instance.db_manager.fetch_one(
            total_miners_query, {"run_id": run_id}
        )
        total_miners = total_result["total_count"] if total_result else 0

        # Check verified miners (those eligible for ERA5 scoring)
        verified_miners_query = """
        SELECT COUNT(*) as verified_count FROM weather_miner_responses 
        WHERE run_id = :run_id AND verification_passed = TRUE
        """
        verified_result = await task_instance.db_manager.fetch_one(
            verified_miners_query, {"run_id": run_id}
        )
        verified_miners = verified_result["verified_count"] if verified_result else 0

        # Check miners with COMPLETE scoring (both day1 AND era5)
        # Only count miners as "scored" if they have era5 scores, not just day1
        scored_miners_query = """
        SELECT COUNT(DISTINCT miner_hotkey) as scored_count 
        FROM weather_miner_scores 
        WHERE run_id = :run_id AND score_type = 'era5_rmse'
        """
        scored_result = await task_instance.db_manager.fetch_one(
            scored_miners_query, {"run_id": run_id}
        )
        scored_miners = scored_result["scored_count"] if scored_result else 0

        # Check miners that failed/offline (including ones that couldn't provide tokens)
        failed_miners_query = """
        SELECT COUNT(*) as failed_count FROM weather_miner_responses 
        WHERE run_id = :run_id AND status IN ('miner_offline', 'verification_failed', 'error', 'failed')
        """
        failed_result = await task_instance.db_manager.fetch_one(
            failed_miners_query, {"run_id": run_id}
        )
        failed_miners = failed_result["failed_count"] if failed_result else 0

        logger.info(
            f"[RunCompletion] Run {run_id}: Total: {total_miners}, Verified: {verified_miners}, Scored: {scored_miners}, Failed: {failed_miners}"
        )
        
        # CRITICAL: Debug what's causing premature run completion
        if total_miners > 0:
            # Get detailed status breakdown
            status_breakdown = await task_instance.db_manager.fetch_all(
                """
                SELECT miner_uid, miner_hotkey, status, verification_passed, job_id
                FROM weather_miner_responses 
                WHERE run_id = :run_id
                ORDER BY miner_uid
                """,
                {"run_id": run_id}
            )
            
            logger.info(f"[RunCompletion] Run {run_id}: Detailed status breakdown:")
            for resp in status_breakdown:
                logger.info(
                    f"  UID {resp['miner_uid']}: status={resp['status']}, "
                    f"verified={resp.get('verification_passed', 'NULL')}, "
                    f"job_id={resp.get('job_id', 'NULL')}, "
                    f"hotkey={resp['miner_hotkey'][:8]}...{resp['miner_hotkey'][-8:]}"
                )

        # NEW: If at least one miner has been FULLY scored (ERA5), consider the run complete
        # This prevents retrying runs that have already produced complete scores
        if scored_miners > 0:
            pending_miners = total_miners - scored_miners - failed_miners
            if pending_miners > 0:
                logger.info(
                    f"[RunCompletion] Run {run_id}: Has {scored_miners} successfully scored miner(s), {pending_miners} pending - marking as complete to prevent retries"
                )
            else:
                logger.info(
                    f"[RunCompletion] Run {run_id}: Has {scored_miners} successfully scored miner(s) - marking as complete"
                )
            # Don't update status here - let the worker handle the appropriate status based on scoring type
            _run_completion_cache[cache_key] = (now, True)
            return True

        # IMPORTANT: For ERA5 scoring, only mark complete when all verified miners are FULLY processed
        # A miner is fully processed when it has BOTH completed inference AND scoring (or definitively failed)
        if verified_miners > 0:
            # Count verified miners that are FULLY processed (have completed the entire pipeline)
            # This includes: scored successfully, or failed/offline after inference completion
            fully_processed_verified_query = """
            SELECT COUNT(DISTINCT mr.miner_hotkey) as processed_count
            FROM weather_miner_responses mr
            WHERE mr.run_id = :run_id AND mr.verification_passed = TRUE
            AND (
                -- Definitely failed states (never completed inference)
                mr.status IN ('miner_offline', 'verification_failed', 'error', 'failed')
                -- OR completed inference and got scored (success path)
                OR EXISTS (
                    SELECT 1 FROM weather_miner_scores wms 
                    WHERE wms.run_id = mr.run_id AND wms.miner_hotkey = mr.miner_hotkey
                    AND wms.score_type = 'era5_rmse'  -- Only count as complete if ERA5 scored
                )
                -- OR completed inference but failed during scoring
                OR (mr.status = 'forecast_ready' AND EXISTS (
                    SELECT 1 FROM validator_jobs vj
                    WHERE vj.job_type = 'weather.era5' 
                    AND vj.run_id = mr.run_id 
                    AND vj.miner_uid = mr.miner_uid
                    AND vj.status = 'failed'
                ))
            )
            """
            processed_result = await task_instance.db_manager.fetch_one(
                fully_processed_verified_query, {"run_id": run_id}
            )
            fully_processed_verified = (
                processed_result["processed_count"] if processed_result else 0
            )

            # Also check for miners still in inference_running state
            running_miners_query = """
            SELECT COUNT(*) as running_count FROM weather_miner_responses 
            WHERE run_id = :run_id AND verification_passed = TRUE
            AND status IN ('inference_running', 'inference_pending')
            """
            running_result = await task_instance.db_manager.fetch_one(
                running_miners_query, {"run_id": run_id}
            )
            running_miners = running_result["running_count"] if running_result else 0

            # Also check for miners that completed inference but haven't been ERA5 scored yet
            awaiting_era5_query = """
            SELECT COUNT(DISTINCT mr.miner_hotkey) as awaiting_count
            FROM weather_miner_responses mr
            WHERE mr.run_id = :run_id AND mr.verification_passed = TRUE
            AND mr.status = 'forecast_ready'
            AND NOT EXISTS (
                SELECT 1 FROM weather_miner_scores wms 
                WHERE wms.run_id = mr.run_id AND wms.miner_hotkey = mr.miner_hotkey
                AND wms.score_type = 'era5_rmse'
            )
            """
            awaiting_era5_result = await task_instance.db_manager.fetch_one(
                awaiting_era5_query, {"run_id": run_id}
            )
            awaiting_era5_miners = awaiting_era5_result["awaiting_count"] if awaiting_era5_result else 0

            logger.info(
                f"[RunCompletion] Run {run_id}: Verified miners - Total: {verified_miners}, "
                f"Fully processed: {fully_processed_verified}, Still running: {running_miners}, "
                f"Awaiting ERA5: {awaiting_era5_miners}"
            )

            # Only mark complete if ALL verified miners are fully processed AND none are still running/awaiting
            if fully_processed_verified >= verified_miners and running_miners == 0 and awaiting_era5_miners == 0:
                if scored_miners == 0:
                    logger.warning(
                        f"[RunCompletion] Run {run_id}: All {verified_miners} verified miners fully processed with no successful scores - marking as complete"
                    )
                    await _update_run_status(
                        task_instance,
                        run_id,
                        "all_miners_failed",
                        "All verified miners failed or went offline",
                    )
                else:
                    logger.info(
                        f"[RunCompletion] Run {run_id}: All {verified_miners} verified miners fully processed with {scored_miners} successful scores - marking as complete"
                    )
                _run_completion_cache[cache_key] = (now, True)
                return True
            else:
                logger.info(
                    f"[RunCompletion] Run {run_id}: Still waiting for miners to complete - not marking as complete yet"
                )
                _run_completion_cache[cache_key] = (now, False)
                return False

        # If all miners have either scored or failed, the run is complete
        if total_miners > 0 and (scored_miners + failed_miners) >= total_miners:
            if scored_miners == 0:
                logger.warning(
                    f"[RunCompletion] Run {run_id}: All miners failed - marking run as completed with no scores"
                )
                await _update_run_status(
                    task_instance,
                    run_id,
                    "all_miners_failed",
                    "All miners failed or went offline",
                )
                _run_completion_cache[cache_key] = (now, True)
                return True
            else:
                logger.info(
                    f"[RunCompletion] Run {run_id}: All miners processed ({scored_miners} scored, {failed_miners} failed)"
                )
                _run_completion_cache[cache_key] = (now, True)
                return True

        _run_completion_cache[cache_key] = (now, False)
        return False

    except Exception as e:
        logger.error(
            f"[RunCompletion] Error checking run completion for run {run_id}: {e}",
            exc_info=True,
        )
        # Don't cache error results
        return False


async def _is_miner_registered(task_instance, miner_hotkey: str) -> bool:
    """
    Check if a miner is still registered in the current metagraph.

    Args:
        task_instance: WeatherTask instance with access to validator
        miner_hotkey: The miner's hotkey to check

    Returns:
        bool: True if miner is registered, False otherwise
    """
    try:
        # Access validator's metagraph through the task instance
        if not hasattr(task_instance, "validator") or not task_instance.validator:
            logger.warning(
                f"Cannot check miner registration for {miner_hotkey}: No validator instance available"
            )
            return True  # Default to True to avoid false positives

        validator = task_instance.validator

        # Check if metagraph is available
        if not hasattr(validator, "metagraph") or not validator.metagraph:
            logger.warning(
                f"Cannot check miner registration for {miner_hotkey}: Metagraph not available"
            )
            return True  # Default to True to avoid false positives

        # Check if metagraph has nodes
        if not hasattr(validator.metagraph, "nodes") or not validator.metagraph.nodes:
            logger.warning(
                f"Cannot check miner registration for {miner_hotkey}: Metagraph nodes not available"
            )
            return True  # Default to True to avoid false positives

        # Check if miner hotkey exists in current metagraph
        is_registered = miner_hotkey in validator.metagraph.nodes

        if not is_registered:
            logger.warning(
                f"Miner {miner_hotkey} not found in current metagraph (may be deregistered)"
            )
        else:
            logger.debug(f"Miner {miner_hotkey} confirmed as registered in metagraph")

        return is_registered

    except Exception as e:
        logger.error(f"Error checking miner registration for {miner_hotkey}: {e}")
        return True  # Default to True to avoid false positives on errors


async def _update_run_status(
    task_instance: "WeatherTask",
    run_id: int,
    status: str,
    error_message: Optional[str] = None,
    gfs_metadata: Optional[dict] = None,
):
    """Helper to update the forecast run status and optionally other fields."""
    logger.info(f"[Run {run_id}] Updating run status to '{status}'.")
    update_fields = ["status = :status"]
    params = {"run_id": run_id, "status": status}

    if error_message is not None:
        update_fields.append("error_message = :error_msg")
        params["error_msg"] = error_message
    if gfs_metadata is not None:
        update_fields.append("gfs_input_metadata = :gfs_meta")
        params["gfs_meta"] = dumps(gfs_metadata, default=str)
    if status in [
        "completed",
        "error",
        "scored",
        "final_scoring_failed",
        "ensemble_failed",
        "initial_scoring_failed",
        "verification_failed",
    ]:
        update_fields.append("completion_time = :comp_time")
        params["comp_time"] = datetime.now(timezone.utc)

    query = f"""
        UPDATE weather_forecast_runs
        SET {', '.join(update_fields)}
        WHERE id = :run_id
    """
    try:
        # Safe query preview handling
        query_str = str(query) if hasattr(query, "__str__") else query
        query_preview = (
            query_str[:200].replace("\n", " ")
            if isinstance(query_str, str)
            else str(query)[:200]
        )
        logger.info(
            f"[Run {run_id}] ABOUT TO EXECUTE update status to '{status}'. Query: {query_preview}"
        )
        await task_instance.db_manager.execute(query, params)
        logger.info(
            f"[Run {run_id}] SUCCESSFULLY EXECUTED update status to '{status}'."
        )
    except asyncio.CancelledError:
        logger.warning(
            f"[Run {run_id}] UPDATE RUN STATUS CANCELLED while trying to set status to '{status}'."
        )
        raise
    except Exception as db_err:
        logger.error(
            f"[Run {run_id}] Failed to update run status to '{status}': {db_err}",
            exc_info=True,
        )


async def build_score_row(
    task_instance: "WeatherTask",
    forecast_run_id: int,
    ground_truth_ds: Optional[xr.Dataset] = None,
):
    """Builds the aggregate score row using FINAL (ERA5) scores."""
    logger.info(
        f"[build_score_row] Building final score row for forecast run {forecast_run_id}"
    )

    try:
        run_query = "SELECT * FROM weather_forecast_runs WHERE id = :run_id"
        run = await task_instance.db_manager.fetch_one(
            run_query, {"run_id": forecast_run_id}
        )
        if not run:
            logger.error(f"[build_score_row] Forecast run {forecast_run_id} not found.")
            return

        scores_query = """
        SELECT ms.miner_hotkey, ms.score as final_score -- Fetch the ERA5 score
        FROM weather_miner_scores ms
        JOIN weather_miner_responses mr ON ms.response_id = mr.id
        WHERE mr.run_id = :run_id
            AND mr.verification_passed = TRUE
            AND ms.score_type = 'era5_rmse' -- Specify final score type
        """

        final_scores = await task_instance.db_manager.fetch_all(
            scores_query, {"run_id": forecast_run_id}
        )

        miner_count = len(final_scores)
        if miner_count == 0:
            logger.warning(
                f"[build_score_row] No final ERA5 scores found for run {forecast_run_id}. Cannot build score row."
            )
            return

        avg_score = sum(s["final_score"] for s in final_scores) / miner_count
        max_score = max(s["final_score"] for s in final_scores)
        min_score = min(s["final_score"] for s in final_scores)
        best_miner = min(final_scores, key=lambda s: s["final_score"])
        best_miner_hotkey = best_miner["miner_hotkey"]

        ensemble_score = None
        ensemble_details = None
        try:
            ensemble_query = """
            SELECT ef.id, ef.ensemble_path, ef.ensemble_kerchunk_path
            FROM weather_ensemble_forecasts ef
            WHERE ef.forecast_run_id = :run_id AND ef.status = 'completed'
            LIMIT 1
            """
            ensemble_details = await task_instance.db_manager.fetch_one(
                ensemble_query, {"run_id": forecast_run_id}
            )

            if ensemble_details and (
                ensemble_details.get("ensemble_path")
                or ensemble_details.get("ensemble_kerchunk_path")
            ):
                logger.info(
                    f"[build_score_row] Found completed ensemble for run {forecast_run_id}. Attempting to score vs ERA5."
                )
                local_ground_truth_ds = ground_truth_ds
                close_gt_later = False

                try:
                    if local_ground_truth_ds is None:
                        logger.info(
                            "[build_score_row] Ground truth not passed, fetching ERA5 for ensemble scoring..."
                        )
                        if not run:
                            run = await task_instance.db_manager.fetch_one(
                                "SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :run_id",
                                {"run_id": forecast_run_id},
                            )

                        if run:
                            sparse_lead_hours_final = task_instance.config.get(
                                "final_scoring_lead_hours", [120, 168]
                            )
                            target_datetimes_final = [
                                run["gfs_init_time_utc"] + timedelta(hours=h)
                                for h in sparse_lead_hours_final
                            ]
                            local_ground_truth_ds = await get_ground_truth_data(
                                task_instance,
                                run["gfs_init_time_utc"],
                                np.array(
                                    target_datetimes_final, dtype="datetime64[ns]"
                                ),
                            )
                            close_gt_later = True
                        else:
                            logger.error(
                                f"[build_score_row] Cannot fetch GT for ensemble, run details missing for {forecast_run_id}"
                            )

                    if local_ground_truth_ds:
                        target_datetimes_for_scoring = [
                            pd.Timestamp(t)
                            .to_pydatetime(warn=False)
                            .replace(tzinfo=timezone.utc)
                            for t in local_ground_truth_ds.time.values
                        ]

                        # logger.info(
                        #     f"[build_score_row] Calling calculate_era5_ensemble_score..."
                        # )
                        # ensemble_score = await calculate_era5_ensemble_score(
                        #     task_instance=task_instance,
                        #     ensemble_details=ensemble_details,
                        #     target_datetimes=target_datetimes_for_scoring,
                        #     ground_truth_ds=local_ground_truth_ds,
                        # )
                        # if ensemble_score is not None:
                        #     logger.info(
                        #         f"[build_score_row] Received ensemble score: {ensemble_score:.4f}"
                        #     )
                        # else:
                        #     logger.warning(
                        #         f"[build_score_row] Ensemble scoring failed or returned None."
                        #     )
                    else:
                        logger.warning(
                            f"[build_score_row] Could not retrieve/receive ground truth data. Cannot score ensemble."
                        )

                except Exception as score_err:
                    logger.error(
                        f"[build_score_row] Error during ensemble scoring call: {score_err}",
                        exc_info=True,
                    )
                finally:
                    if (
                        close_gt_later
                        and local_ground_truth_ds
                        and hasattr(local_ground_truth_ds, "close")
                    ):
                        local_ground_truth_ds.close()
                    gc.collect()
            else:
                logger.info(
                    f"[build_score_row] No completed ensemble found for run {forecast_run_id}. Skipping ensemble scoring."
                )

        except Exception as e_ens_score:
            logger.error(
                f"[build_score_row] Unexpected error during ensemble scoring setup: {e_ens_score}",
                exc_info=True,
            )

        score_data = {
            "task_name": "weather",
            "subtask_name": "forecast",
            "run_id": str(forecast_run_id),
            "run_timestamp": run["gfs_init_time_utc"].isoformat(),
            "avg_score": float(avg_score),
            "max_score": float(max_score),
            "min_score": float(min_score),
            "miner_count": miner_count,
            "best_miner": best_miner_hotkey,
            "ensemble_score": ensemble_score,
            "metadata": {
                "gfs_init_time": run["gfs_init_time_utc"].isoformat(),
                "final_score_miner_count": miner_count,
                "has_ensemble": ensemble_details is not None,
                "ensemble_scored": ensemble_score is not None,
            },
        }

        exists_query = "SELECT id FROM score_table WHERE task_name = 'weather' AND run_id = :run_id"
        existing = await task_instance.db_manager.fetch_one(
            exists_query, {"run_id": str(forecast_run_id)}
        )
        db_params = {
            k: v
            for k, v in score_data.items()
            if k not in ["task_name", "subtask_name", "run_id", "run_timestamp"]
        }
        db_params["metadata"] = dumps(score_data["metadata"])

        if existing:
            update_query = "UPDATE score_table SET avg_score = :avg_score, max_score = :max_score, min_score = :min_score, miner_count = :miner_count, best_miner = :best_miner, ensemble_score = :ensemble_score, metadata = :metadata WHERE id = :id"
            db_params["id"] = existing["id"]
            await task_instance.db_manager.execute(update_query, db_params)
            logger.info(
                f"[build_score_row] Updated final score row for run {forecast_run_id}"
            )
        else:
            insert_query = "INSERT INTO score_table (task_name, subtask_name, run_id, run_timestamp, avg_score, max_score, min_score, miner_count, best_miner, ensemble_score, metadata) VALUES (:task_name, :subtask_name, :run_id, :run_timestamp, :avg_score, :max_score, :min_score, :miner_count, :best_miner, :ensemble_score, :metadata)"
            db_params["task_name"] = score_data["task_name"]
            db_params["subtask_name"] = score_data["subtask_name"]
            db_params["run_id"] = score_data["run_id"]
            db_params["run_timestamp"] = score_data["run_timestamp"]
            await task_instance.db_manager.execute(insert_query, db_params)
            logger.info(
                f"[build_score_row] Inserted final score row for run {forecast_run_id}"
            )

    except Exception as e:
        logger.error(
            f"[build_score_row] Error building final score row for run {forecast_run_id}: {e}",
            exc_info=True,
        )


async def get_ground_truth_data(
    task_instance: "WeatherTask", init_time: datetime, forecast_times: np.ndarray
) -> Optional[xr.Dataset]:
    """
    Fetches ERA5 ground truth data corresponding to the forecast times using the CDS API.

    Args:
        task_instance: The WeatherTask instance.
        init_time: The initialization time of the forecast run (used for logging/context).
        forecast_times: Numpy array of datetime64 objects for the forecast timesteps.

    Returns:
        An xarray.Dataset containing the ERA5 data, or None if retrieval fails.
    """
    logger.info(
        f"Attempting to fetch ERA5 ground truth for {len(forecast_times)} times starting near {init_time}"
    )
    try:
        target_datetimes = [
            pd.Timestamp(ts).to_pydatetime(warn=False) for ts in forecast_times
        ]
        target_datetimes = [
            (
                t.replace(tzinfo=timezone.utc)
                if t.tzinfo is None
                else t.astimezone(timezone.utc)
            )
            for t in target_datetimes
        ]
    except Exception as e:
        logger.error(f"Failed to convert forecast_times to Python datetimes: {e}")
        return None

    era5_cache_dir = Path(task_instance.config.get("era5_cache_dir", "./era5_cache"))
    try:
        # Use progressive fetch for consistency with other ERA5 fetching and better caching
        use_progressive_fetch = task_instance.config.get("progressive_era5_fetch", True)
        if use_progressive_fetch:
            from gaia.tasks.defined_tasks.weather.utils.era5_api import fetch_era5_data_progressive
            ground_truth_ds = await fetch_era5_data_progressive(
                target_times=target_datetimes, cache_dir=era5_cache_dir
            )
        else:
            ground_truth_ds = await fetch_era5_data(
                target_times=target_datetimes, cache_dir=era5_cache_dir
            )
        if ground_truth_ds is None:
            logger.warning("fetch_era5_data returned None. Ground truth unavailable.")
            return None
        else:
            logger.success("✅ Successfully fetched/loaded ERA5 ground truth data.")
            return ground_truth_ds
    except Exception as e:
        logger.error(f"Error occurred during get_ground_truth_data: {e}", exc_info=True)
        return None


async def _trigger_initial_scoring(task_instance: "WeatherTask", run_id: int):
    """Queues a run for initial scoring based on GFS analysis."""
    if not task_instance.initial_scoring_worker_running:
        logger.warning("Initial scoring worker not running. Cannot queue run.")
        await _update_run_status(task_instance, run_id, "initial_scoring_skipped")
        return

    if task_instance.test_mode:
        task_instance.last_test_mode_run_id = run_id
        task_instance.test_mode_run_scored_event.clear()
        logger.info(f"[TestMode] Prepared event for scoring completion of run {run_id}")

    # Create persistent scoring job for restart resilience
    if await task_instance._create_scoring_job(run_id, "day1_qc"):
        logger.info(
            f"Queueing run {run_id} for initial GFS-based scoring with persistent tracking."
        )
        await task_instance.initial_scoring_queue.put(run_id)
        await _update_run_status(task_instance, run_id, "initial_scoring_queued")
    else:
        logger.error(
            f"Failed to create persistent scoring job for run {run_id}. Skipping scoring."
        )
        await _update_run_status(task_instance, run_id, "initial_scoring_failed")


async def _trigger_final_scoring(task_instance: "WeatherTask", run_id: int):
    """Trigger final ERA5 scoring for a specific run."""
    try:
        # Create persistent scoring job for restart resilience
        if await task_instance._create_scoring_job(run_id, "era5_final"):
            logger.info(
                f"[Run {run_id}] Successfully triggered final ERA5 scoring with persistent tracking"
            )
            # Update run status to indicate final scoring is queued/triggered
            await _update_run_status(task_instance, run_id, "final_scoring_triggered")
        else:
            logger.error(
                f"[Run {run_id}] Failed to create persistent final scoring job"
            )
            await _update_run_status(task_instance, run_id, "final_scoring_failed")
    except Exception as e:
        logger.error(
            f"[Run {run_id}] Error triggering final scoring: {e}", exc_info=True
        )
        await _update_run_status(task_instance, run_id, "final_scoring_failed")


async def _request_fresh_token(
    task_instance: "WeatherTask", miner_hotkey: str, job_id: str
) -> Optional[Tuple[str, str, str]]:
    """Requests a fresh JWT, Zarr URL, and manifest_content_hash from the miner."""
    logger.info(
        f"[VerifyLogic] Requesting fresh token/manifest_hash for job {job_id} from miner {miner_hotkey[:12]}..."
    )
    forecast_request_payload = {"nonce": str(uuid.uuid4()), "data": {"job_id": job_id}}
    from gaia.tasks.defined_tasks.weather.pipeline.miner_communication import query_single_miner
    endpoint_to_call = "/weather-kerchunk-request"
    try:
        response_dict = await query_single_miner(
            validator=task_instance.validator,
            miner_hotkey=miner_hotkey,
            endpoint=endpoint_to_call,
            payload=forecast_request_payload,
            timeout=45.0,
            db_manager=task_instance.db_manager,
        )

        # If no response, bail
        if not response_dict or not response_dict.get("success"):
            logger.warning(
                f"[VerifyLogic] No response received from miner {miner_hotkey[:12]} for job {job_id}"
            )
            return None
        if response_dict.get("success"):
            miner_response_data = response_dict.get("data", {})
            miner_status = miner_response_data.get("status")

            if miner_status == "completed":
                token = miner_response_data.get("access_token")
                zarr_store_relative_url = miner_response_data.get("zarr_store_url")
                manifest_content_hash = miner_response_data.get("verification_hash")
                if token and zarr_store_relative_url and manifest_content_hash:
                    # Get IP and port from database
                    db = task_instance.db_manager
                    row = await db.fetch_one(
                        "SELECT ip, port FROM node_table WHERE hotkey = :hk", 
                        {"hk": miner_hotkey}
                    )
                    if not row or not row.get("ip") or not row.get("port"):
                        logger.error(f"Could not find IP/port for miner {miner_hotkey[:12]}")
                        return None
                    
                    ip_val = row["ip"]
                    port_val = row["port"]
                    
                    # Convert integer IP to dotted decimal if needed
                    ip_str = (
                        str(ipaddress.ip_address(int(ip_val)))
                        if isinstance(ip_val, (str, int)) and str(ip_val).isdigit()
                        else ip_val
                    )
                    try:
                        ipaddress.ip_address(ip_str)
                    except ValueError:
                        logger.warning(
                            f"Miner IP {ip_str} not standard, assuming hostname."
                        )
                    miner_base_url = f"https://{ip_str}:{port_val}"
                    full_zarr_url = (
                        miner_base_url.rstrip("/")
                        + "/"
                        + zarr_store_relative_url.lstrip("/")
                    )
                    logger.info(
                        f"[VerifyLogic] Success: Token, URL: {full_zarr_url}, ManifestHash: {manifest_content_hash[:10]}..."
                    )
                    return token, full_zarr_url, manifest_content_hash
                else:
                    logger.warning(
                        f"[VerifyLogic] Miner {miner_hotkey[:12]} responded 'completed' for job {job_id} but missing token/URL/hash"
                    )
            elif miner_status == "processing":
                logger.info(
                    f"[VerifyLogic] Miner {miner_hotkey[:12]} job {job_id} still processing: {miner_response_data.get('message', 'No message')}"
                )
                return None  # This is expected, validator should retry later
            elif miner_status == "error":
                logger.warning(
                    f"[VerifyLogic] Miner {miner_hotkey[:12]} job {job_id} failed: {miner_response_data.get('message', 'No error message')}"
                )
                return None
            elif miner_status == "not_found":
                logger.warning(
                    f"[VerifyLogic] Miner {miner_hotkey[:12]} reports job {job_id} not found"
                )
                return None
            else:
                logger.warning(
                    f"[VerifyLogic] Miner {miner_hotkey[:12]} returned unknown status '{miner_status}' for job {job_id}"
                )
        else:
            logger.warning(
                f"[VerifyLogic] Miner {miner_hotkey[:12]} request failed: {response_dict.get('error', 'Unknown error')} for job {job_id}"
            )
    except Exception as e:
        logger.error(
            f"Unhandled exception in _request_fresh_token for job {job_id}: {e!r}",
            exc_info=True,
        )
    return None


async def get_job_by_gfs_init_time(
    task_instance: "WeatherTask",
    gfs_init_time_utc: datetime,
    validator_hotkey: str = None,
) -> Optional[Dict[str, Any]]:
    """
    Check if a job exists for the given GFS initialization time and validator.
    With the new deterministic job ID system, we prioritize job reuse across validators.

    RESILIENCE: This function implements fallback strategies to handle
    database synchronization scenarios where job IDs might not match exactly.
    All fallbacks require exact GFS timestep matches for weather data validity.

    (Intended for Miner-side usage)

    Args:
        task_instance: WeatherTask instance
        gfs_init_time_utc: GFS initialization time
        validator_hotkey: Optional validator hotkey to filter by specific validator's jobs

    Args:
        task_instance: WeatherTask instance
        gfs_init_time_utc: GFS initialization time
        validator_hotkey: Optional validator hotkey to filter by specific validator's jobs
    """
    if task_instance.node_type != "miner":
        logger.error("get_job_by_gfs_init_time called on non-miner node.")
        return None

    try:
        # Strategy 1: Try to find a job from the same validator (exact GFS timestep match)
        if validator_hotkey:
            validator_specific_query = """
            SELECT id as job_id, status, target_netcdf_path as zarr_store_path, validator_hotkey
            FROM weather_miner_jobs
            WHERE gfs_init_time_utc = :gfs_init_time
            AND validator_hotkey = :validator_hotkey
            ORDER BY id DESC
            LIMIT 1
            """
            job = await task_instance.db_manager.fetch_one(
                validator_specific_query,
                {
                    "gfs_init_time": gfs_init_time_utc,
                    "validator_hotkey": validator_hotkey,
                },
            )

            if job:
                logger.info(
                    f"Found existing job {job['job_id']} for GFS time {gfs_init_time_utc} from same validator {validator_hotkey[:8]}"
                )
                return job

        # Strategy 2: Look for any job with this exact GFS time (cross-validator reuse)
        any_validator_query = """
        SELECT id as job_id, status, target_netcdf_path as zarr_store_path, validator_hotkey
        FROM weather_miner_jobs
        WHERE gfs_init_time_utc = :gfs_init_time
        ORDER BY id DESC
        LIMIT 1
        """
        job = await task_instance.db_manager.fetch_one(
            any_validator_query, {"gfs_init_time": gfs_init_time_utc}
        )

        if job:
            other_validator = job.get("validator_hotkey", "unknown")[:8]
            logger.info(
                f"Found existing job {job['job_id']} for GFS time {gfs_init_time_utc} from different validator {other_validator} - enabling cross-validator reuse"
            )
            return job

        # No additional fallbacks - GFS timestep must match exactly for weather data validity

        return None
    except Exception as e:
        logger.error(
            f"Error checking for existing job with GFS init time {gfs_init_time_utc} and validator {validator_hotkey}: {e}"
        )
        logger.error(
            f"Error checking for existing job with GFS init time {gfs_init_time_utc} and validator {validator_hotkey}: {e}"
        )
        return None


async def update_job_status(
    task_instance: "WeatherTask",
    job_id: str,
    status: str,
    error_message: Optional[str] = None,
):
    """
    Update the status of a job in the miner's database.
    (Intended for Miner-side usage)
    """
    if task_instance.node_type != "miner":
        logger.error("update_job_status called on non-miner node.")
        return False

    logger.info(f"[Job {job_id}] Updating miner job status to '{status}'.")
    try:
        update_fields = ["status = :status", "updated_at = :updated_at"]
        params = {
            "job_id": job_id,
            "status": status,
            "updated_at": datetime.now(timezone.utc),
        }

        if status == "processing":
            update_fields.append(
                "processing_start_time = COALESCE(processing_start_time, :proc_start)"
            )
            params["proc_start"] = datetime.now(timezone.utc)
        elif status == "completed":
            update_fields.append("processing_end_time = :proc_end")
            params["proc_end"] = datetime.now(timezone.utc)

        if error_message:
            update_fields.append("error_message = :error_msg")
            params["error_msg"] = error_message

        query = f"""
        UPDATE weather_miner_jobs
        SET {", ".join(update_fields)}
        WHERE id = :job_id -- Assuming miner job table uses job_id as PK or unique ID
        """

        await task_instance.db_manager.execute(query, params)
        logger.info(f"Updated miner job {job_id} status to {status}")
        return True
    except Exception as e:
        logger.error(f"Error updating miner job status for {job_id}: {e}")
        return False


async def update_job_paths(
    task_instance: "WeatherTask",
    job_id: str,
    netcdf_path: Optional[str] = None,
    kerchunk_path: Optional[str] = None,
    verification_hash: Optional[str] = None,
) -> None:
    """
    Update the file paths and verification hash for a completed job in the miner's database.
    Now stores the Zarr path in both target_netcdf_path and kerchunk_json_path fields for compatibility.
    (Intended for Miner-side usage)
    """
    if task_instance.node_type != "miner":
        logger.error("update_job_paths called on non-miner node.")
        return False

    logger.info(
        f"[Job {job_id}] Updating miner job paths with Zarr store path: {netcdf_path}"
    )
    try:
        query = """
        UPDATE weather_miner_jobs
        SET target_netcdf_path = :zarr_path,
            kerchunk_json_path = :zarr_path,
            verification_hash = :hash,
            updated_at = :updated_at
        WHERE id = :job_id
        """
        params = {
            "job_id": job_id,
            "zarr_path": netcdf_path,
            "hash": verification_hash,
            "updated_at": datetime.now(timezone.utc),
        }
        await task_instance.db_manager.execute(query, params)
        logger.info(
            f"Updated miner job {job_id} with Zarr store path and verification hash"
        )
        return True
    except Exception as e:
        logger.error(f"Error updating miner job paths for {job_id}: {e}")
        return False


async def find_job_by_alternative_methods(
    task_instance: "WeatherTask", job_id: str, miner_hotkey: str
) -> Optional[Dict[str, Any]]:
    """
    Attempts to find a job using alternative methods when the exact job ID is not found.
    This is critical for handling database synchronization scenarios where job IDs might not match.
    Used by both miners and validators for database resilience.

    Args:
        task_instance: WeatherTask instance
        job_id: The original job ID that wasn't found
        miner_hotkey: The miner's hotkey

    Returns:
        Job details if found, None otherwise
    """

    try:
        # Extract timestamp from deterministic job ID if possible
        # Format: weather_job_forecast_YYYYMMDDHHMMSS_miner_hotkey
        job_parts = job_id.split("_")
        if len(job_parts) >= 4 and job_parts[0] == "weather" and job_parts[1] == "job":
            timestamp_str = job_parts[3]
            if len(timestamp_str) == 14:  # YYYYMMDDHHMMSS
                try:
                    gfs_time = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S").replace(
                        tzinfo=timezone.utc
                    )
                    logger.info(f"Extracted GFS time {gfs_time} from job ID {job_id}")

                    # Look for jobs with this GFS time and miner
                    fallback_query = """
                    SELECT id as job_id, status, target_netcdf_path, verification_hash, gfs_init_time_utc, validator_hotkey
                    FROM weather_miner_jobs 
                    WHERE gfs_init_time_utc = :gfs_time
                    ORDER BY validator_request_time DESC
                    LIMIT 1
                    """

                    fallback_job = await task_instance.db_manager.fetch_one(
                        fallback_query, {"gfs_time": gfs_time}
                    )

                    if fallback_job:
                        original_job_id = fallback_job["job_id"]
                        other_validator = fallback_job.get(
                            "validator_hotkey", "unknown"
                        )[:8]
                        logger.warning(
                            f"Database sync resilience: Found equivalent job {original_job_id} "
                            f"for missing job {job_id} (GFS: {gfs_time}) from validator {other_validator}"
                        )
                        return fallback_job

                except ValueError as ve:
                    logger.debug(
                        f"Could not parse timestamp from job ID {job_id}: {ve}"
                    )

        # No additional fallbacks - we need exact GFS timestep match for weather data validity

        return None

    except Exception as e:
        logger.error(f"Error in find_job_by_alternative_methods for job {job_id}: {e}")
        return None


async def verify_miner_response(
    task_instance: "WeatherTask", run_details: Dict, response_details: Dict
):
    """Handles fetching Zarr store info and verifying a single miner response's manifest integrity."""
    run_id = run_details["id"]
    response_id = response_details["id"]
    miner_hotkey = response_details["miner_hotkey"]
    job_id = response_details.get("job_id")

    if not job_id:
        logger.error(
            f"[VerifyLogic, Resp {response_id}] Missing job_id for miner {miner_hotkey}. Cannot verify."
        )
        await task_instance.db_manager.execute(
            "UPDATE weather_miner_responses SET status = 'verification_error', error_message = 'Internal: Missing job_id' WHERE id = :id",
            {"id": response_id},
        )
        return

    logger.info(
        f"[VerifyLogic] Verifying response {response_id} from {miner_hotkey} (Miner Job ID: {job_id})"
    )

    access_token = None
    zarr_store_url = None
    claimed_manifest_content_hash = None
    verified_dataset: Optional[xr.Dataset] = None

    try:
        token_data_tuple = await _request_fresh_token(
            task_instance, miner_hotkey, job_id
        )
        if token_data_tuple is None:
            # RESILIENCE: Try alternative job lookup methods before giving up
            logger.warning(
                f"[VerifyLogic, Resp {response_id}] Primary job lookup failed for {job_id}. Attempting database sync resilience fallback..."
            )

            # Try to find an equivalent job using alternative methods
            fallback_job = await find_job_by_alternative_methods(
                task_instance, job_id, miner_hotkey
            )
            if fallback_job and fallback_job.get("status") == "completed":
                fallback_job_id = fallback_job["job_id"]
                logger.info(
                    f"[VerifyLogic, Resp {response_id}] Found fallback job {fallback_job_id}, retrying token request..."
                )

                # Try token request with the fallback job ID
                token_data_tuple = await _request_fresh_token(
                    task_instance, miner_hotkey, fallback_job_id
                )
                if token_data_tuple:
                    # Update the response record with the correct job ID for future reference
                    await task_instance.db_manager.execute(
                        """
                        UPDATE weather_miner_responses 
                        SET job_id = :correct_job_id, 
                            error_message = 'Used fallback job ID due to database sync' 
                        WHERE id = :id
                    """,
                        {"id": response_id, "correct_job_id": fallback_job_id},
                    )
                    logger.info(
                        f"[VerifyLogic, Resp {response_id}] Database sync resilience successful - updated job ID from {job_id} to {fallback_job_id}"
                    )

            if token_data_tuple is None:
                # Still no luck - miner job is processing, failed, or truly not found
                logger.warning(
                    f"[VerifyLogic, Resp {response_id}] Miner {miner_hotkey} job {job_id} not ready for verification (still processing, failed, or database sync issue). Skipping."
                )
                await task_instance.db_manager.execute(
                    """
                    UPDATE weather_miner_responses 
                    SET status = 'awaiting_miner_completion', 
                        error_message = 'Miner job still processing or not available for verification'
                    WHERE id = :id
                """,
                    {"id": response_id},
                )
                return  # Exit gracefully, this miner won't be scored for this run

        access_token, zarr_store_url, claimed_manifest_content_hash = token_data_tuple
        logger.info(
            f"[VerifyLogic, Resp {response_id}] Unpacked token data. URL: {zarr_store_url}, Manifest Hash: {claimed_manifest_content_hash[:10] if claimed_manifest_content_hash else 'N/A'}..."
        )

        if not all([access_token, zarr_store_url, claimed_manifest_content_hash]):
            err_details = f"Token:Set='{bool(access_token)}', URL:'{zarr_store_url}', ManifestHash:Set='{bool(claimed_manifest_content_hash)}'"
            raise ValueError(
                f"Critical information missing after token request: {err_details}"
            )

        await task_instance.db_manager.execute(
            """
            UPDATE weather_miner_responses
            SET kerchunk_json_url = :url, verification_hash_claimed = :hash, status = 'verifying_manifest'
            WHERE id = :id
        """,
            {
                "id": response_id,
                "url": zarr_store_url,
                "hash": claimed_manifest_content_hash,
            },
        )

        logger.info(
            f"[VerifyLogic, Resp {response_id}] Attempting to open VERIFIED Zarr store: {zarr_store_url}..."
        )

        storage_opts = {
            "headers": {"Authorization": f"Bearer {access_token}"},
            "ssl": False,
        }
        verification_timeout_seconds = task_instance.config.get(
            "verification_timeout_seconds", 300
        )

        verified_dataset = await asyncio.wait_for(
            open_verified_remote_zarr_dataset(
                zarr_store_url=zarr_store_url,
                claimed_manifest_content_hash=claimed_manifest_content_hash,
                miner_hotkey_ss58=miner_hotkey,
                storage_options=storage_opts,
                job_id=job_id,
            ),
            timeout=verification_timeout_seconds,
        )

        verification_succeeded_bool = verified_dataset is not None
        db_status_after_verification = ""
        error_message_for_db = None

        if verification_succeeded_bool:
            db_status_after_verification = "verified_manifest_store_opened"
            logger.info(
                f"[VerifyLogic, Resp {response_id}] Manifest verified & Zarr store opened with on-read checks."
            )
            try:
                logger.info(
                    f"[VerifyLogic, Resp {response_id}] Verified dataset keys: {list(verified_dataset.keys()) if verified_dataset else 'N/A'}"
                )
                # Clear any retry scheduling on success
                await task_instance.db_manager.execute(
                    """
                    UPDATE weather_miner_responses
                    SET retry_count = 0,
                        next_retry_time = NULL
                    WHERE id = :resp_id
                    """,
                    {"resp_id": response_id},
                )
            except Exception as e_ds_ops:
                logger.warning(
                    f"[VerifyLogic, Resp {response_id}] Minor error during initial ops on verified dataset: {e_ds_ops}"
                )
                if error_message_for_db:
                    error_message_for_db += f"; Post-verify op error: {e_ds_ops}"
                else:
                    error_message_for_db = f"Post-manifest op error: {e_ds_ops}"
        else:
            # Schedule retry – exponential backoff capped at 60 min
            existing_retry_rec = await task_instance.db_manager.fetch_one(
                "SELECT retry_count FROM weather_miner_responses WHERE id = :rid",
                {"rid": response_id},
            )
            current_retry_count = (
                existing_retry_rec["retry_count"]
                if existing_retry_rec and existing_retry_rec["retry_count"] is not None
                else 0
            )
            new_retry_count = current_retry_count + 1
            delay_minutes = min(
                5 * (2 ** (new_retry_count - 1)), 60
            )  # 5,10,20,40,60,60...
            next_retry_at = datetime.now(timezone.utc) + timedelta(
                minutes=delay_minutes
            )

            db_status_after_verification = "retry_scheduled"
            error_message_for_db = "Manifest verification failed. Scheduled retry."
            logger.error(
                f"[VerifyLogic, Resp {response_id}] Manifest verification failed. Scheduling retry #{new_retry_count} in {delay_minutes} min."
            )

            await task_instance.db_manager.execute(
                """
                UPDATE weather_miner_responses
                SET retry_count = :rc,
                    next_retry_time = :nrt
                WHERE id = :resp_id
                """,
                {"resp_id": response_id, "rc": new_retry_count, "nrt": next_retry_at},
            )

        await task_instance.db_manager.execute(
            """ 
            UPDATE weather_miner_responses 
            SET verification_passed = :verified, status = :new_status, error_message = COALESCE(:err_msg, error_message)
            WHERE id = :id
        """,
            {
                "id": response_id,
                "verified": verification_succeeded_bool,
                "new_status": db_status_after_verification,
                "err_msg": error_message_for_db,
            },
        )

    except asyncio.TimeoutError:
        logger.error(
            f"[VerifyLogic, Resp {response_id}] Verified open process timed out for {miner_hotkey} (Job: {job_id})."
        )
        await task_instance.db_manager.execute(
            "UPDATE weather_miner_responses SET status = 'verification_timeout', error_message = 'Verified open process timed out', verification_passed = FALSE WHERE id = :id",
            {"id": response_id},
        )
    except Exception as err:
        logger.error(
            f"[VerifyLogic, Resp {response_id}] Error verifying response from {miner_hotkey} (Job: {job_id}): {err!r}",
            exc_info=True,
        )
        await task_instance.db_manager.execute(
            """
            UPDATE weather_miner_responses SET status = 'verification_error', error_message = :msg, verification_passed = FALSE
            WHERE id = :id
        """,
            {
                "id": response_id,
                "msg": f"Outer verify_miner_response error: {str(err)}",
            },
        )
    finally:
        if verified_dataset is not None:
            try:
                verified_dataset.close()
            except Exception:
                pass
        gc.collect()


async def get_run_gfs_init_time(
    task_instance: "WeatherTask", run_id: int
) -> Optional[datetime]:
    run_details_query = (
        "SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :run_id"
    )
    run_record = await task_instance.db_manager.fetch_one(
        run_details_query, {"run_id": run_id}
    )
    if run_record and run_record["gfs_init_time_utc"]:
        return run_record["gfs_init_time_utc"]
    logger.error(f"Could not retrieve GFS init time for run_id: {run_id}")
    return None


async def _batch_insert_component_scores(
    db_manager,
    component_scores: List[Dict],
    batch_size: int = 50  # Typically ~90 records max per miner (9 vars × 10 lead times)
) -> bool:
    """
    Optimized batch insert of component scores into weather_forecast_component_scores table.
    Uses COPY-like performance with executemany and ON CONFLICT DO UPDATE for upserts.
    """
    if not component_scores:
        return True
        
    try:
        # Prepare the insert query with ON CONFLICT for upserts
        insert_query = """
            INSERT INTO weather_forecast_component_scores (
                run_id, response_id, miner_uid, miner_hotkey,
                score_type, lead_hours, valid_time_utc,
                variable_name, pressure_level,
                rmse, mse, acc, skill_score, skill_score_gfs, skill_score_climatology,
                bias, mae,
                climatology_check_passed, pattern_correlation, pattern_correlation_passed,
                clone_penalty, quality_penalty,
                weighted_score, variable_weight,
                calculation_duration_ms
            ) VALUES (
                :run_id, :response_id, :miner_uid, :miner_hotkey,
                :score_type, :lead_hours, :valid_time_utc,
                :variable_name, :pressure_level,
                :rmse, :mse, :acc, :skill_score, :skill_score_gfs, :skill_score_climatology,
                :bias, :mae,
                :climatology_check_passed, :pattern_correlation, :pattern_correlation_passed,
                :clone_penalty, :quality_penalty,
                :weighted_score, :variable_weight,
                :calculation_duration_ms
            )
            ON CONFLICT (response_id, score_type, lead_hours, variable_name, pressure_level)
            DO UPDATE SET
                rmse = EXCLUDED.rmse,
                mse = EXCLUDED.mse,
                acc = EXCLUDED.acc,
                skill_score = EXCLUDED.skill_score,
                skill_score_gfs = EXCLUDED.skill_score_gfs,
                skill_score_climatology = EXCLUDED.skill_score_climatology,
                bias = EXCLUDED.bias,
                mae = EXCLUDED.mae,
                climatology_check_passed = EXCLUDED.climatology_check_passed,
                pattern_correlation = EXCLUDED.pattern_correlation,
                pattern_correlation_passed = EXCLUDED.pattern_correlation_passed,
                clone_penalty = EXCLUDED.clone_penalty,
                quality_penalty = EXCLUDED.quality_penalty,
                weighted_score = EXCLUDED.weighted_score,
                variable_weight = EXCLUDED.variable_weight,
                calculation_duration_ms = EXCLUDED.calculation_duration_ms,
                calculated_at = CURRENT_TIMESTAMP
        """
        
        # Process in batches for optimal performance
        logger.debug(f"Processing {len(component_scores)} component scores in batches of {batch_size}")
        for i in range(0, len(component_scores), batch_size):
            batch = component_scores[i:i + batch_size]
            
            # Ensure all required fields have defaults
            processed_batch = []
            for score in batch:
                processed_score = {
                    'run_id': score.get('run_id'),
                    'response_id': score.get('response_id'),
                    'miner_uid': score.get('miner_uid'),
                    'miner_hotkey': score.get('miner_hotkey'),
                    'score_type': score.get('score_type'),
                    'lead_hours': score.get('lead_hours'),
                    'valid_time_utc': score.get('valid_time_utc'),
                    'variable_name': score.get('variable_name'),
                    'pressure_level': score.get('pressure_level'),
                    'rmse': score.get('rmse'),
                    'mse': score.get('mse'),
                    'acc': score.get('acc'),
                    'skill_score': score.get('skill_score'),
                    'skill_score_gfs': score.get('skill_score_gfs'),
                    'skill_score_climatology': score.get('skill_score_climatology'),
                    'bias': score.get('bias'),
                    'mae': score.get('mae'),
                    'climatology_check_passed': score.get('climatology_check_passed'),
                    'pattern_correlation': score.get('pattern_correlation'),
                    'pattern_correlation_passed': score.get('pattern_correlation_passed'),
                    'clone_penalty': score.get('clone_penalty', 0.0),
                    'quality_penalty': score.get('quality_penalty', 0.0),
                    'weighted_score': score.get('weighted_score'),
                    'variable_weight': score.get('variable_weight'),
                    'calculation_duration_ms': score.get('calculation_duration_ms'),
                }
                processed_batch.append(processed_score)
            
            # Execute batch insert
            logger.debug(f"Executing batch insert for batch {i//batch_size + 1} with {len(processed_batch)} records")
            await db_manager.execute_many(insert_query, processed_batch)
            
        logger.success(f"✅ Successfully inserted {len(component_scores)} component scores in batches of {batch_size}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to batch insert component scores: {e}", exc_info=True)
        return False

async def calculate_era5_miner_score(
    task_instance: "WeatherTask",
    miner_response_rec: Dict,
    target_datetimes: List[datetime],
    era5_truth_ds: xr.Dataset,
    era5_climatology_ds: xr.Dataset,
) -> bool:
    """
    Calculates final scores for a single miner against ERA5 analysis.
    Metrics: MSE (vs ERA5), Bias (vs ERA5), ACC (vs ERA5 anomalies),
             Skill Score (vs ERA5 Climatology as reference).
    Stores results in weather_miner_scores.
    Returns True if scoring was successful, False otherwise.
    """
    miner_hotkey = miner_response_rec["miner_hotkey"]
    miner_uid = miner_response_rec["miner_uid"]
    job_id = miner_response_rec["job_id"]
    run_id = miner_response_rec["run_id"]
    response_id = miner_response_rec["id"]

    logger.info(
        f"[FinalScore] Starting ERA5 scoring for miner {miner_hotkey} (UID: {miner_uid}, Job: {job_id}, Run: {run_id}, RespID: {response_id})"
    )

    gfs_init_time_of_run = await get_run_gfs_init_time(task_instance, run_id)
    if gfs_init_time_of_run is None:
        logger.error(
            f"[FinalScore] Miner {miner_hotkey}: Could not determine GFS init time for run {run_id}. Cannot calculate lead hours accurately."
        )
        return False

    final_scoring_config = {
        "variables_levels_to_score": task_instance.config.get(
            "final_scoring_variables_levels",
            task_instance.config.get("day1_variables_levels_to_score"),
        ),
    }

    if era5_climatology_ds is None:
        logger.error(
            f"[FinalScore] Miner {miner_hotkey}: ERA5 Climatology not available. Cannot calculate ACC or Climatology Skill Score."
        )
        return False

    miner_forecast_ds: Optional[xr.Dataset] = None
    all_metrics_for_db = []
    all_component_scores = []  # NEW: Collect component scores for batch insert

    try:
        # CRITICAL: Debug ERA5 scoring configuration like we did for Day1
        variables_to_score = final_scoring_config.get("variables_levels_to_score", [])
        # Normalize string entries into dict form
        normalized_vars = []
        for vc in variables_to_score:
            if isinstance(vc, str):
                normalized_vars.append({"name": vc})
            elif isinstance(vc, dict):
                # Ensure at least a name field exists
                if "name" in vc:
                    normalized_vars.append(vc)
                else:
                    logger.warning(f"[FinalScore] Ignoring malformed variable config without 'name': {vc}")
            else:
                logger.warning(f"[FinalScore] Ignoring unknown variable config type: {type(vc)} -> {vc}")
        variables_to_score = normalized_vars
        logger.info(
            f"[FinalScore] Miner {miner_hotkey[:8]}...{miner_hotkey[-8:]}: ERA5 scoring configuration:"
            f"\n  Variables to score: {len(variables_to_score)} variables"
            f"\n  Target times: {len(target_datetimes)} time steps"
        )
        
        if not variables_to_score:
            logger.error(
                f"[FinalScore] Miner {miner_hotkey[:8]}...{miner_hotkey[-8:]}: "
                f"No variables configured for ERA5 scoring! This will result in failure. "
                f"Check final_scoring_variables_levels or day1_variables_levels_to_score configuration."
            )
            return False
        
        stored_response_details_query = "SELECT kerchunk_json_url, verification_hash_claimed FROM weather_miner_responses WHERE id = :response_id"
        stored_response_data = await task_instance.db_manager.fetch_one(
            stored_response_details_query, {"response_id": response_id}
        )

        if (
            not stored_response_data
            or not stored_response_data["verification_hash_claimed"]
            or not stored_response_data["kerchunk_json_url"]
        ):
            logger.error(
                f"[FinalScore] Miner {miner_hotkey}: Failed to retrieve stored verification_hash_claimed or Zarr URL for response_id {response_id}. Aborting ERA5 scoring."
            )
            return False

        stored_manifest_hash = stored_response_data["verification_hash_claimed"]
        token_data_tuple = await _request_fresh_token(
            task_instance, miner_hotkey, job_id
        )
        if token_data_tuple is None:
            # Check if miner is still registered before treating this as a critical error
            is_registered = await _is_miner_registered(task_instance, miner_hotkey)
            if not is_registered:
                logger.warning(
                    f"[FinalScore] Miner {miner_hotkey} failed token request and is not in current metagraph - likely deregistered. Cleaning up all records for this miner."
                )
                await _cleanup_miner_records(task_instance, miner_hotkey, run_id)
                # Check if this was the last miner in the run
                await _check_run_completion(task_instance, run_id)
                return False  # Skip this miner gracefully rather than causing worker failure
            else:
                logger.error(
                    f"[FinalScore] Miner {miner_hotkey} failed token request but is still registered in metagraph. This may indicate a miner-side issue or network problem."
                )
                # Remove this miner from current run but don't delete their historical data
                await _cleanup_offline_miner_from_run(
                    task_instance, miner_hotkey, run_id
                )
                # Check if this was the last miner in the run
                await _check_run_completion(task_instance, run_id)
                return False  # Skip this miner gracefully

        access_token, current_zarr_store_url, miner_reported_manifest_hash = token_data_tuple

        logger.info(
            f"[FinalScore] Miner {miner_hotkey}: Using stored manifest hash: {stored_manifest_hash[:10]}... and current Zarr URL: {current_zarr_store_url}"
        )

        # STRICT: Ensure miner-reported manifest hash matches originally claimed/stored hash
        try:
            if miner_reported_manifest_hash and miner_reported_manifest_hash != stored_manifest_hash:
                logger.error(
                    f"[FinalScore] Miner {miner_hotkey}: Manifest hash mismatch. Stored={stored_manifest_hash[:10]}..., MinerNow={miner_reported_manifest_hash[:10]}..."
                )
                await task_instance.db_manager.execute(
                    """
                    UPDATE weather_miner_responses
                    SET status = 'verification_error', error_message = 'Manifest hash mismatch between stored and miner-reported during scoring'
                    WHERE id = :rid
                    """,
                    {"rid": response_id},
                )
                return False
        except Exception:
            # If any DB/log error occurs, fail fast to maintain integrity
            return False

        storage_options = {
            "headers": {"Authorization": f"Bearer {access_token}"},
            "ssl": False,
        }
        verification_timeout_seconds = (
            task_instance.config.get("verification_timeout_seconds", 300) / 2
        )

        # RIGOR: Full rehash verification of remote store before any reads
        from ..utils.hashing import recompute_remote_manifest_content_hash
        from ..utils.hashing import is_rehash_verified, verify_minimal_chunks_and_reconstruct_manifest_hash
        if not is_rehash_verified(current_zarr_store_url, stored_manifest_hash):
            # Efficient path: verify only needed chunks for this scoring window (variables/times/levels), then reconstruct manifest hash
            variables = [vc["name"] for vc in variables_to_score if isinstance(vc, dict) and "name" in vc]
            # Build the times to score for this call
            times_to_score = [pd.Timestamp(dt) for dt in target_datetimes]
            # Levels: if any variable has 'level' == 'all', pass the standard 13 plevs; else None
            levels = None
            try:
                if any(vc.get("level") in ("all", None) and vc.get("name") in ("t","u","v","q","z") for vc in variables_to_score if isinstance(vc, dict)):
                    levels = [1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50]
            except Exception:
                levels = None
            ok_rehash, rehash_details, preopened_ds = await verify_minimal_chunks_and_reconstruct_manifest_hash(
                zarr_store_url=current_zarr_store_url,
                claimed_manifest_content_hash=stored_manifest_hash,
                miner_hotkey_ss58=miner_hotkey,
                variables=variables,
                times=times_to_score,
                levels=levels,
                headers=storage_options.get("headers"),
                job_id=f"{job_id}_final_score_minrehash",
            )
            if not ok_rehash:
                logger.error(
                    f"[FinalScore] Miner {miner_hotkey}: Full rehash verification failed: {rehash_details}"
                )
                await task_instance.db_manager.execute(
                    """
                    UPDATE weather_miner_responses 
                    SET status = 'verification_failed', error_message = 'Full rehash verification failed before scoring'
                    WHERE run_id = :run_id AND miner_hotkey = :miner_hotkey
                    """,
                    {"run_id": run_id, "miner_hotkey": miner_hotkey},
                )
                await _check_run_completion(task_instance, run_id)
                return False

        # Reuse verified open dataset if provided by minimal verifier; otherwise open once here
        if 'preopened_ds' in locals() and preopened_ds is not None:
            miner_forecast_ds = preopened_ds
        else:
            miner_forecast_ds = await asyncio.wait_for(
                open_verified_remote_zarr_dataset(
                    zarr_store_url=current_zarr_store_url,
                    claimed_manifest_content_hash=stored_manifest_hash,
                    miner_hotkey_ss58=miner_hotkey,
                    storage_options=storage_options,
                    job_id=f"{job_id}_final_score_reverify",
                ),
                timeout=verification_timeout_seconds,
            )
        if miner_forecast_ds is None:
            logger.error(
                f"[FinalScore] Failed to open verified Zarr dataset for miner {miner_hotkey} - manifest verification failed"
            )
            # Mark this miner as failed but don't crash the entire batch
            await task_instance.db_manager.execute(
                """UPDATE weather_miner_responses 
                   SET status = 'verification_failed', error_message = 'Manifest verification failed during final scoring'
                   WHERE run_id = :run_id AND miner_hotkey = :miner_hotkey""",
                {"run_id": run_id, "miner_hotkey": miner_hotkey},
            )
            # Check if this was the last miner in the run
            await _check_run_completion(task_instance, run_id)
            return False

        # CRITICAL FIX: Apply variable mapping to miner forecast data for ERA5 scoring consistency
        logger.info(
            f"[FinalScore] Miner {miner_hotkey[:8]}...{miner_hotkey[-8:]}: "
            f"Checking miner forecast variable names for ERA5 scoring..."
        )
        logger.info(f"[FinalScore] Original miner variables: {list(miner_forecast_ds.data_vars)}")
        
        # Check if miner data needs variable mapping (has raw GFS names instead of mapped names)
        raw_gfs_vars = ['tmp2m', 'prmslmsl', 'hgtprs', 'tmpprs', 'ugrd10m', 'vgrd10m', 'ugrdprs', 'vgrdprs', 'spfhprs']
        has_raw_vars = any(var in miner_forecast_ds.data_vars for var in raw_gfs_vars)
        mapped_vars = ['2t', 'msl', 'z', 't', '10u', '10v', 'u', 'v', 'q']
        has_mapped_vars = any(var in miner_forecast_ds.data_vars for var in mapped_vars)
        
        if has_raw_vars and not has_mapped_vars:
            logger.info(f"[FinalScore] Miner data has raw GFS variable names, applying manual mapping...")
            
            # Define the variable mapping from raw GFS names to scoring names
            var_mapping = {
                "tmp2m": "2t",
                "ugrd10m": "10u", 
                "vgrd10m": "10v",
                "prmslmsl": "msl",
                "tmpprs": "t",
                "ugrdprs": "u",
                "vgrdprs": "v", 
                "spfhprs": "q",
                "hgtprs": "z_height",
            }
            
            try:
                # Apply variable renaming
                vars_to_rename = {k: v for k, v in var_mapping.items() if k in miner_forecast_ds.data_vars}
                if vars_to_rename:
                    logger.info(f"[FinalScore] Renaming variables: {vars_to_rename}")
                    miner_forecast_ds = miner_forecast_ds.rename(vars_to_rename)
                    
                    # Convert geopotential height to geopotential if present
                    if "z_height" in miner_forecast_ds.data_vars:
                        logger.info(f"[FinalScore] Converting geopotential height to geopotential...")
                        G = 9.80665  # Standard gravity
                        z_height_var = miner_forecast_ds["z_height"]
                        geopotential = G * z_height_var
                        miner_forecast_ds["z"] = geopotential
                        miner_forecast_ds["z"].attrs = {
                            "units": "m2 s-2",
                            "long_name": "Geopotential",
                            "standard_name": "geopotential"
                        }
                        miner_forecast_ds = miner_forecast_ds.drop_vars("z_height")
                        
                    logger.info(f"[FinalScore] Mapped miner variables: {list(miner_forecast_ds.data_vars)}")
                else:
                    logger.info(f"[FinalScore] No variables found to rename")
                
                # CRITICAL: Apply coordinate renaming for pressure levels (same as Day1 scoring)
                coord_mapping = {
                    "lev": "pressure_level",
                    "plev": "pressure_level", 
                    "latitude": "lat", 
                    "longitude": "lon"
                }
                
                coords_to_rename = {k: v for k, v in coord_mapping.items() if k in miner_forecast_ds.coords}
                if coords_to_rename:
                    logger.info(f"[FinalScore] Renaming miner coordinates: {coords_to_rename}")
                    miner_forecast_ds = miner_forecast_ds.rename(coords_to_rename)
                    
            except Exception as e:
                logger.error(f"[FinalScore] Failed to apply variable/coordinate mapping to miner data: {e}")
                return False
        else:
            logger.info(f"[FinalScore] Miner data already has mapped variable names, no mapping needed")  # Skip this miner gracefully

        # Final scoring uses ERA5-based skill scores only (no GFS operational forecast)
        # GFS data is typically unavailable by the time final scoring runs due to retention limits
        gfs_operational_fcst_ds = None
        logger.debug(
            f"[FinalScore] Miner {miner_hotkey}: Using ERA5-climatology skill scores (GFS operational not used for final scoring)"
        )

        # PRE-LOAD STRATEGY: Download all required data chunks ONCE before processing
        # This prevents duplicate HTTP requests when processing multiple variables
        logger.info(f"[FinalScore] Miner {miner_hotkey}: Pre-loading forecast data to prevent duplicate requests...")
        preload_start_time = time.time()
        
        try:
            variables_needed = [var_config.get("name") for var_config in variables_to_score if isinstance(var_config, dict) and var_config.get("name")]
            variables_in_dataset = [var for var in variables_needed if var in miner_forecast_ds.data_vars]
            
            if variables_in_dataset:
                # Convert target datetimes to proper time coordinates for selection
                time_coords = []
                for dt in target_datetimes:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)
                    time_coords.append(pd.Timestamp(dt))
                
                # Select only the variables and times we need
                miner_subset = miner_forecast_ds[variables_in_dataset].sel(time=time_coords, method="nearest")
                
                # Load all data at once to cache in memory
                await asyncio.to_thread(lambda: miner_subset.load())
                
                # Replace the original dataset with the pre-loaded subset
                miner_forecast_ds = miner_subset
                
                preload_time = time.time() - preload_start_time
                logger.info(f"[FinalScore] Miner {miner_hotkey}: Pre-loaded forecast data in {preload_time:.2f}s - processing will use cached data")
            else:
                logger.warning(f"[FinalScore] Miner {miner_hotkey}: No scoring variables found in dataset")
                
        except Exception as preload_err:
            preload_time = time.time() - preload_start_time
            logger.warning(f"[FinalScore] Miner {miner_hotkey}: Pre-loading failed after {preload_time:.2f}s: {preload_err}")
            logger.warning("Continuing with lazy loading (may result in duplicate requests)")

        if gfs_operational_fcst_ds is None:
            logger.debug(
                f"[FinalScore] Miner {miner_hotkey}: Using ERA5-climatology-based skill scores only (no GFS operational reference in final scoring)"
            )

        for valid_time_dt in target_datetimes:
            logger.info(
                f"[FinalScore] Miner {miner_hotkey}: Processing Valid Time: {valid_time_dt}"
            )
            miner_forecast_lead_slice = None
            era5_truth_lead_slice = None

            try:
                # Robust dtype checking for timezone-aware dtypes
                def _is_integer_dtype(dtype):
                    """Check if dtype is integer, handling timezone-aware dtypes."""
                    try:
                        return np.issubdtype(dtype, np.integer)
                    except TypeError:
                        # Handle timezone-aware dtypes that can't be interpreted by numpy
                        return False

                def _is_datetime_dtype(dtype):
                    """Check if dtype is datetime, handling timezone-aware dtypes."""
                    dtype_str = str(dtype)
                    return (
                        "datetime64" in dtype_str
                        or dtype_str.startswith("<M8")
                        or "datetime" in dtype_str.lower()
                    )

                # Handle miner forecast dataset time dtype with robust checking
                if _is_integer_dtype(miner_forecast_ds.time.dtype):
                    selection_label_miner = int(
                        valid_time_dt.timestamp() * 1_000_000_000
                    )
                elif _is_datetime_dtype(miner_forecast_ds.time.dtype):
                    dtype_str = str(miner_forecast_ds.time.dtype)
                    if "UTC" in dtype_str or "tz" in dtype_str.lower():
                        # Timezone-aware - use timezone-aware timestamp
                        selection_label_miner = valid_time_dt
                    else:
                        # Timezone-naive - remove timezone
                        selection_label_miner = valid_time_dt.replace(tzinfo=None)
                else:
                    selection_label_miner = valid_time_dt

                # Handle ERA5 truth dataset time dtype with robust checking
                if _is_integer_dtype(era5_truth_ds.time.dtype):
                    selection_label_era5 = int(
                        valid_time_dt.timestamp() * 1_000_000_000
                    )
                elif _is_datetime_dtype(era5_truth_ds.time.dtype):
                    dtype_str = str(era5_truth_ds.time.dtype)
                    if "UTC" in dtype_str or "tz" in dtype_str.lower():
                        # Timezone-aware - use timezone-aware timestamp
                        selection_label_era5 = valid_time_dt
                    else:
                        # Timezone-naive - remove timezone
                        selection_label_era5 = valid_time_dt.replace(tzinfo=None)
                else:
                    selection_label_era5 = valid_time_dt

                logger.info(
                    f"[FinalScore info] Miner time dtype: {miner_forecast_ds.time.dtype}, ERA5 time dtype: {era5_truth_ds.time.dtype}"
                )
                logger.info(
                    f"[FinalScore info] Using selection_label_miner: {selection_label_miner} (type: {type(selection_label_miner)}) for miner_forecast_ds"
                )
                logger.info(
                    f"[FinalScore info] Using selection_label_era5: {selection_label_era5} (type: {type(selection_label_era5)}) for era5_truth_ds"
                )

                # Remove duplicate time indices before selection to prevent reindexing errors
                if len(miner_forecast_ds.time) > len(
                    np.unique(miner_forecast_ds.time.values)
                ):
                    logger.debug(
                        f"[FinalScore] Removing duplicate time indices from miner forecast data"
                    )
                    _, unique_indices = np.unique(
                        miner_forecast_ds.time.values, return_index=True
                    )
                    miner_forecast_ds = miner_forecast_ds.isel(
                        time=sorted(unique_indices)
                    )

                if len(era5_truth_ds.time) > len(np.unique(era5_truth_ds.time.values)):
                    logger.debug(
                        f"[FinalScore] Removing duplicate time indices from ERA5 data"
                    )
                    _, unique_indices = np.unique(
                        era5_truth_ds.time.values, return_index=True
                    )
                    era5_truth_ds = era5_truth_ds.isel(time=sorted(unique_indices))

                miner_forecast_lead_slice = miner_forecast_ds.sel(
                    time=selection_label_miner, method="nearest"
                )
                era5_truth_lead_slice = era5_truth_ds.sel(
                    time=selection_label_era5, method="nearest"
                )

                if miner_forecast_lead_slice is not None:
                    miner_forecast_lead_slice = miner_forecast_lead_slice.squeeze(
                        drop=True
                    )
                if era5_truth_lead_slice is not None:
                    era5_truth_lead_slice = era5_truth_lead_slice.squeeze(drop=True)

                miner_time_item = miner_forecast_lead_slice.time.item()
                if isinstance(miner_time_item, (int, float, np.integer, np.floating)):
                    miner_time_actual_utc = pd.Timestamp(
                        miner_time_item, unit="ns", tz="UTC"
                    )
                else:
                    miner_time_actual_utc = (
                        pd.Timestamp(miner_time_item).tz_localize("UTC")
                        if pd.Timestamp(miner_time_item).tzinfo is None
                        else pd.Timestamp(miner_time_item).tz_convert("UTC")
                    )

                era5_time_item = era5_truth_lead_slice.time.item()
                if isinstance(era5_time_item, (int, float, np.integer, np.floating)):
                    era5_time_actual_utc = pd.Timestamp(
                        era5_time_item, unit="ns", tz="UTC"
                    )
                else:
                    era5_time_actual_utc = (
                        pd.Timestamp(era5_time_item).tz_localize("UTC")
                        if pd.Timestamp(era5_time_item).tzinfo is None
                        else pd.Timestamp(era5_time_item).tz_convert("UTC")
                    )

                time_check_failed = False
                if abs((miner_time_actual_utc - valid_time_dt).total_seconds()) > 3600:
                    logger.warning(
                        f"[FinalScore] Miner {miner_hotkey}: Selected miner forecast time {miner_time_actual_utc} "
                        f"too far from target {valid_time_dt}. Skipping this valid_time."
                    )
                    time_check_failed = True

                # Use more lenient tolerance for ERA5 in test mode where data availability may be limited
                era5_time_tolerance = (
                    21600  # 6 hours for ERA5 (more lenient for test mode)
                )
                if (
                    not time_check_failed
                    and abs((era5_time_actual_utc - valid_time_dt).total_seconds())
                    > era5_time_tolerance
                ):
                    logger.warning(
                        f"[FinalScore] Miner {miner_hotkey}: Selected ERA5 truth time {era5_time_actual_utc} "
                        f"too far from target {valid_time_dt} (tolerance: {era5_time_tolerance/3600:.1f}h). Skipping this valid_time."
                    )
                    time_check_failed = True

                if time_check_failed:
                    continue
            except TypeError as te_sel:
                logger.error(
                    f"[FinalScore] Miner {miner_hotkey}: TypeError during data selection for valid time {valid_time_dt}: {te_sel}. This often indicates incompatible time coordinate types. Skipping this time.",
                    exc_info=True,
                )
                continue
            except Exception as e_sel:
                log_msg = f"[FinalScore] Miner {miner_hotkey}: Error during data selection for valid time {valid_time_dt}: {e_sel}. \
                        Miner slice acquired: {miner_forecast_lead_slice is not None}, ERA5 slice acquired: {era5_truth_lead_slice is not None}. Skipping this time."
                logger.error(log_msg)
                continue

            gfs_op_lead_slice = None
            if gfs_operational_fcst_ds is not None:
                try:
                    # Remove duplicate time indices from GFS operational data if needed
                    if len(gfs_operational_fcst_ds.time) > len(
                        np.unique(gfs_operational_fcst_ds.time.values)
                    ):
                        logger.debug(
                            f"[FinalScore] Removing duplicate time indices from GFS operational data"
                        )
                        _, unique_indices = np.unique(
                            gfs_operational_fcst_ds.time.values, return_index=True
                        )
                        gfs_operational_fcst_ds = gfs_operational_fcst_ds.isel(
                            time=sorted(unique_indices)
                        )

                    selection_label_gfs_op = valid_time_dt
                    if np.issubdtype(gfs_operational_fcst_ds.time.dtype, np.integer):
                        selection_label_gfs_op = int(
                            valid_time_dt.timestamp() * 1_000_000_000
                        )
                    elif str(gfs_operational_fcst_ds.time.dtype) == "datetime64[ns]":
                        selection_label_gfs_op = valid_time_dt.replace(tzinfo=None)

                    gfs_op_lead_slice = gfs_operational_fcst_ds.sel(
                        time=selection_label_gfs_op, method="nearest"
                    ).squeeze(drop=True)

                except Exception as e_gfs_sel:
                    logger.warning(
                        f"[FinalScore] Miner {miner_hotkey}: Could not select from GFS operational forecast for {valid_time_dt}: {e_gfs_sel}. Skill vs GFS will be impacted."
                    )
                    gfs_op_lead_slice = None

            # Use normalized list to avoid KeyError on bare string configs
            for var_config in variables_to_score:
                var_name = var_config.get("name")
                if not var_name:
                    logger.warning(f"[FinalScore] Skipping variable with missing name in config: {var_config}")
                    continue
                var_level = var_config.get("level")
                standard_name_for_clim = var_config.get("standard_name", var_name)
                var_key = f"{var_name}{var_level if var_level and var_level != 'all' else ''}"
                
                logger.debug(f"[FinalScore] Processing variable {var_key} at time {valid_time_dt}")

                # Ensure both datetimes are proper datetime objects for subtraction
                if isinstance(gfs_init_time_of_run, (int, float)):
                    gfs_init_dt = datetime.fromtimestamp(gfs_init_time_of_run, tz=timezone.utc)
                else:
                    gfs_init_dt = gfs_init_time_of_run
                
                # Ensure valid_time_dt is also a proper datetime object
                if not isinstance(valid_time_dt, datetime):
                    logger.error(f"[FinalScore] valid_time_dt is not a datetime object: {type(valid_time_dt)} = {valid_time_dt}")
                    continue
                    
                lead_hours = int(
                    (valid_time_dt - gfs_init_dt).total_seconds() / 3600
                )

                db_metric_row_base = {
                    "response_id": response_id,
                    "run_id": run_id,
                    "miner_uid": miner_uid,
                    "miner_hotkey": miner_hotkey,
                    "score_type": None,
                    "score": None,
                    "metrics": {},
                    "calculation_time": datetime.now(timezone.utc),
                    "error_message": None,
                    "lead_hours": lead_hours,
                    "variable_level": var_key,
                    "valid_time_utc": valid_time_dt,
                }

                try:
                    scoring_start_time = time.time()  # Track scoring start time for duration calculation
                    logger.info(
                        f"[FinalScore] Miner {miner_hotkey}: Scoring {var_key} at {valid_time_dt} (Lead: {lead_hours}h)"
                    )

                    # Check available variables and handle variable name mapping
                    logger.info(
                        f"[FinalScore] Available miner variables: {list(miner_forecast_lead_slice.data_vars)}"
                    )
                    logger.info(
                        f"[FinalScore] Available ERA5 variables: {list(era5_truth_lead_slice.data_vars)}"
                    )

                    # Handle variable name mapping for ERA5 datasets
                    era5_var_name = var_name
                    miner_var_name = var_name

                    # Define variable name mappings between config names and dataset names
                    era5_var_mappings = {
                        "2t": [
                            "2t",
                            "t2m",
                            "temperature_2m",
                        ],  # 2-meter temperature alternatives (surface level)
                        "10u": [
                            "10u",
                            "u10",
                            "10m_u_component_of_wind",
                        ],  # 10m U wind alternatives
                        "10v": [
                            "10v", 
                            "v10",
                            "10m_v_component_of_wind",
                        ],  # 10m V wind alternatives
                        "msl": [
                            "msl",
                            "mean_sea_level_pressure",
                            "sp",
                        ],  # mean sea level pressure alternatives
                        "z": ["z", "geopotential"],  # geopotential alternatives
                        "t": [
                            "t",
                            "temperature",
                        ],  # temperature alternatives (pressure level)
                        "u": ["u", "u_component_of_wind"],  # U wind alternatives
                        "v": ["v", "v_component_of_wind"],  # V wind alternatives
                        "q": ["q", "specific_humidity"],  # Specific humidity alternatives
                    }

                    miner_var_mappings = {
                        "2t": ["2t", "temperature_2m"],  # FIXED: Remove "t" to avoid confusion with atmospheric temperature
                        "10u": ["10u", "u10", "10m_u_component_of_wind"],
                        "10v": ["10v", "v10", "10m_v_component_of_wind"],
                        "msl": ["msl", "mean_sea_level_pressure", "sp"],
                        "z": ["z", "geopotential"],
                        "t": ["t", "temperature"],  # Atmospheric temperature only
                        "u": ["u", "u_component_of_wind"],
                        "v": ["v", "v_component_of_wind"], 
                        "q": ["q", "specific_humidity"],
                    }

                    # Find appropriate variable name in ERA5 dataset
                    if var_name in era5_var_mappings:
                        logger.debug(f"[FinalScore] ERA5 variable mapping for '{var_name}': {era5_var_mappings[var_name]}")
                        for alt_name in era5_var_mappings[var_name]:
                            if alt_name in era5_truth_lead_slice.data_vars:
                                era5_var_name = alt_name
                                logger.info(
                                    f"[FinalScore] ERA5: Selected variable '{alt_name}' for config variable '{var_name}'"
                                )
                                # CRITICAL DEBUG: Check what we actually selected
                                selected_var = era5_truth_lead_slice[alt_name]
                                logger.info(f"[FinalScore] ERA5 selected variable info:")
                                logger.info(f"  Variable name: {alt_name}")
                                logger.info(f"  Dimensions: {selected_var.dims}")
                                logger.info(f"  Shape: {selected_var.shape}")
                                logger.info(f"  Attributes: {dict(selected_var.attrs)}")
                                break
                        else:
                            logger.error(
                                f"[FinalScore] No suitable ERA5 variable found for '{var_name}'. Available: {list(era5_truth_lead_slice.data_vars)}"
                            )
                            raise KeyError(
                                f"Variable {var_name} not available in ERA5 dataset"
                            )

                    # Find appropriate variable name in miner dataset
                    if var_name in miner_var_mappings:
                        logger.debug(f"[FinalScore] Miner variable mapping for '{var_name}': {miner_var_mappings[var_name]}")
                        for alt_name in miner_var_mappings[var_name]:
                            if alt_name in miner_forecast_lead_slice.data_vars:
                                miner_var_name = alt_name
                                logger.info(
                                    f"[FinalScore] Miner: Selected variable '{alt_name}' for config variable '{var_name}'"
                                )
                                # CRITICAL DEBUG: Check what we actually selected
                                selected_var = miner_forecast_lead_slice[alt_name]
                                logger.info(f"[FinalScore] Miner selected variable info:")
                                logger.info(f"  Variable name: {alt_name}")
                                logger.info(f"  Dimensions: {selected_var.dims}")
                                logger.info(f"  Shape: {selected_var.shape}")
                                logger.info(f"  Attributes: {dict(selected_var.attrs)}")
                                break
                        else:
                            logger.error(
                                f"[FinalScore] No suitable miner variable found for '{var_name}'. Available: {list(miner_forecast_lead_slice.data_vars)}"
                            )
                            raise KeyError(
                                f"Variable {var_name} not available in miner dataset"
                            )
                    
                    # CRITICAL FIX: Standardize dimension names across all datasets before processing
                    def standardize_pressure_dims(data_array, target_dim_name="pressure_level"):
                        """Standardize pressure level dimension names"""
                        if hasattr(data_array, 'dims'):
                            rename_map = {}
                            for dim in data_array.dims:
                                if dim in ['plev', 'lev', 'level'] and dim != target_dim_name:
                                    rename_map[dim] = target_dim_name
                            if rename_map:
                                logger.debug(f"[FinalScore] Standardizing dimensions: {rename_map}")
                                return data_array.rename(rename_map)
                        return data_array

                    miner_var_da_unaligned = standardize_pressure_dims(miner_forecast_lead_slice[miner_var_name])
                    truth_var_da_unaligned = standardize_pressure_dims(era5_truth_lead_slice[era5_var_name])

                    # ENHANCED DIAGNOSTICS: Check variable selection and attributes
                    logger.info(f"[FinalScore] VARIABLE SELECTION DEBUG for {var_key}:")
                    logger.info(f"  Config variable: '{var_name}'")
                    logger.info(f"  Selected miner variable: '{miner_var_name}'")
                    logger.info(f"  Selected ERA5 variable: '{era5_var_name}'")
                    
                    # Check ERA5 variable attributes for unit/name verification
                    era5_var = era5_truth_lead_slice[era5_var_name]
                    logger.info(f"  ERA5 variable attributes: {dict(era5_var.attrs)}")
                    
                    # Check miner variable attributes
                    miner_var = miner_forecast_lead_slice[miner_var_name]
                    logger.info(f"  Miner variable attributes: {dict(miner_var.attrs)}")

                    # Add detailed diagnostics for potential unit mismatches
                    logger.info(
                        f"[FinalScore] RAW DATA DIAGNOSTICS for {var_key} at {valid_time_dt}:"
                    )

                    # Log data ranges before any processing
                    miner_min, miner_max, miner_mean = (
                        float(miner_var_da_unaligned.min()),
                        float(miner_var_da_unaligned.max()),
                        float(miner_var_da_unaligned.mean()),
                    )
                    truth_min, truth_max, truth_mean = (
                        float(truth_var_da_unaligned.min()),
                        float(truth_var_da_unaligned.max()),
                        float(truth_var_da_unaligned.mean()),
                    )

                    logger.info(
                        f"[FinalScore] Miner {var_key}: range=[{miner_min:.1f}, {miner_max:.1f}], mean={miner_mean:.1f}, units={miner_var_da_unaligned.attrs.get('units', 'unknown')}"
                    )
                    logger.info(
                        f"[FinalScore] ERA5  {var_key}: range=[{truth_min:.1f}, {truth_max:.1f}], mean={truth_mean:.1f}, units={truth_var_da_unaligned.attrs.get('units', 'unknown')}"
                    )

                    # Check for potential unit mismatch indicators
                    if var_name == "z" and var_level == 500:
                        # For z500, geopotential should be ~49000-58000 m²/s²
                        # If it's geopotential height, it would be ~5000-6000 m
                        miner_ratio = (
                            miner_mean / 9.80665
                        )  # If miner is geopotential, this ratio should be ~5000-6000
                        truth_ratio = truth_mean / 9.80665
                        logger.info(
                            f"[FinalScore] z500 UNIT CHECK - If geopotential (m²/s²): miner_mean/g={miner_ratio:.1f}m, truth_mean/g={truth_ratio:.1f}m"
                        )

                        if (
                            miner_mean < 10000
                        ):  # Much smaller than expected geopotential
                            logger.warning(
                                f"[FinalScore] POTENTIAL UNIT MISMATCH: Miner z500 mean ({miner_mean:.1f}) suggests geopotential height (m) rather than geopotential (m²/s²)"
                            )
                        elif truth_mean > 40000 and miner_mean > 40000:
                            logger.info(
                                f"[FinalScore] Unit check OK: Both miner and truth z500 appear to be geopotential (m²/s²)"
                            )

                    elif var_name == "2t":
                        # Temperature should be ~200-320 K
                        if miner_mean < 200 or miner_mean > 350:
                            logger.warning(
                                f"[FinalScore] POTENTIAL UNIT ISSUE: Miner 2t mean ({miner_mean:.1f}) outside expected range for Kelvin"
                            )

                    elif var_name == "msl":
                        # Mean sea level pressure should be ~90000-110000 Pa
                        if miner_mean < 50000 or miner_mean > 150000:
                            logger.warning(
                                f"[FinalScore] POTENTIAL UNIT ISSUE: Miner msl mean ({miner_mean:.1f}) outside expected range for Pa"
                            )

                    # AUTOMATIC UNIT CONVERSION: Convert geopotential height to geopotential if needed
                    if var_name == "z" and miner_mean < 10000 and truth_mean > 40000:
                        logger.warning(
                            f"[FinalScore] AUTOMATIC UNIT CONVERSION: Converting miner z from geopotential height (m) to geopotential (m²/s²)"
                        )
                        miner_var_da_unaligned = miner_var_da_unaligned * 9.80665
                        miner_var_da_unaligned.attrs["units"] = "m2 s-2"
                        miner_var_da_unaligned.attrs["long_name"] = (
                            "Geopotential (auto-converted from height)"
                        )
                        logger.info(
                            f"[FinalScore] After conversion: miner z range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}"
                        )

                    # Check for temperature unit conversions (Celsius to Kelvin)
                    elif (
                        var_name in ["2t", "t"]
                        and miner_mean < 100
                        and truth_mean > 200
                    ):
                        logger.warning(
                            f"[FinalScore] AUTOMATIC UNIT CONVERSION: Converting miner {var_name} from Celsius to Kelvin"
                        )
                        miner_var_da_unaligned = miner_var_da_unaligned + 273.15
                        miner_var_da_unaligned.attrs["units"] = "K"
                        miner_var_da_unaligned.attrs["long_name"] = (
                            f'{miner_var_da_unaligned.attrs.get("long_name", var_name)} (auto-converted from Celsius)'
                        )
                        logger.info(
                            f"[FinalScore] After conversion: miner {var_name} range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}"
                        )

                    # Check for pressure unit conversions (hPa to Pa)
                    elif var_name == "msl" and miner_mean < 2000 and truth_mean > 50000:
                        logger.warning(
                            f"[FinalScore] AUTOMATIC UNIT CONVERSION: Converting miner msl from hPa to Pa"
                        )
                        miner_var_da_unaligned = miner_var_da_unaligned * 100.0
                        miner_var_da_unaligned.attrs["units"] = "Pa"
                        miner_var_da_unaligned.attrs["long_name"] = (
                            "Mean sea level pressure (auto-converted from hPa)"
                        )
                        logger.info(
                            f"[FinalScore] After conversion: miner msl range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}"
                        )

                    clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
                    clim_hour_rounded = (valid_time_dt.hour // 6) * 6
                    climatology_var_da_raw = era5_climatology_ds[
                        standard_name_for_clim
                    ].sel(
                        dayofyear=clim_dayofyear,
                        hour=clim_hour_rounded,
                        method="nearest",
                    )

                    if var_level and var_level != "all":
                        # Handle different pressure level dimension names robustly
                        def _select_pressure_level(data_array, target_level):
                            """Select pressure level handling multiple possible dimension names."""
                            possible_pressure_dims = [
                                "pressure_level",
                                "plev",
                                "level",
                                "levels",
                                "p",
                            ]
                            for dim_name in possible_pressure_dims:
                                if dim_name in data_array.dims:
                                    return data_array.sel(
                                        **{dim_name: target_level}, method="nearest"
                                    ).squeeze(drop=True)
                            raise ValueError(
                                f"No pressure level dimension found in {list(data_array.dims)}. Expected one of: {possible_pressure_dims}"
                            )

                        miner_var_da_selected = _select_pressure_level(
                            miner_var_da_unaligned, var_level
                        )
                        truth_var_da_selected = _select_pressure_level(
                            truth_var_da_unaligned, var_level
                        )
                        # Apply robust pressure level selection to climatology too
                        clim_pressure_dim = None
                        for dim_name in [
                            "pressure_level",
                            "plev",
                            "level",
                            "levels",
                            "p",
                        ]:
                            if dim_name in climatology_var_da_raw.dims:
                                clim_pressure_dim = dim_name
                                break
                        if clim_pressure_dim:
                            climatology_var_da_selected = climatology_var_da_raw.sel(
                                **{clim_pressure_dim: var_level}, method="nearest"
                            ).squeeze(drop=True)
                        else:
                            climatology_var_da_selected = (
                                climatology_var_da_raw.squeeze(drop=True)
                            )
                    else:
                        miner_var_da_selected = miner_var_da_unaligned.squeeze(
                            drop=True
                        )
                        truth_var_da_selected = truth_var_da_unaligned.squeeze(
                            drop=True
                        )
                        climatology_var_da_selected = climatology_var_da_raw.squeeze(
                            drop=True
                        )

                    def _standardize_spatial_dims_final(
                        data_array: xr.DataArray,
                    ) -> xr.DataArray:
                        if not isinstance(data_array, xr.DataArray):
                            return data_array
                        rename_dict = {}
                        for dim_name in data_array.dims:
                            if dim_name.lower() in ("latitude", "lat_0"):
                                rename_dict[dim_name] = "lat"
                            elif dim_name.lower() in ("longitude", "lon_0"):
                                rename_dict[dim_name] = "lon"
                        return (
                            data_array.rename(rename_dict)
                            if rename_dict
                            else data_array
                        )

                    miner_var_da_std = _standardize_spatial_dims_final(
                        miner_var_da_selected
                    )
                    truth_var_da_std = _standardize_spatial_dims_final(
                        truth_var_da_selected
                    )
                    climatology_var_da_std = _standardize_spatial_dims_final(
                        climatology_var_da_selected
                    )

                    target_grid_for_interp = truth_var_da_std
                    if (
                        "lat" in target_grid_for_interp.dims
                        and len(target_grid_for_interp.lat) > 1
                        and target_grid_for_interp.lat.values[0]
                        < target_grid_for_interp.lat.values[-1]
                    ):
                        target_grid_for_interp = await asyncio.to_thread(
                            target_grid_for_interp.isel, lat=slice(None, None, -1)
                        )

                    miner_var_da_aligned = await asyncio.to_thread(
                        miner_var_da_std.interp_like,
                        target_grid_for_interp,
                        method="linear",
                        kwargs={"fill_value": None},
                    )
                    truth_var_da_final = target_grid_for_interp

                    clim_var_to_interpolate = climatology_var_da_std
                    # Apply robust pressure level handling to climatology interpolation
                    if var_level:
                        clim_pressure_dim = None
                        for dim_name in [
                            "pressure_level",
                            "plev",
                            "level",
                            "levels",
                            "p",
                        ]:
                            if dim_name in clim_var_to_interpolate.dims:
                                clim_pressure_dim = dim_name
                                break
                        # If climatology still has pressure dimensions but truth doesn't, select the level
                        truth_has_pressure = any(
                            dim in truth_var_da_final.dims
                            for dim in [
                                "pressure_level",
                                "plev",
                                "level",
                                "levels",
                                "p",
                            ]
                        )
                        if clim_pressure_dim and not truth_has_pressure:
                            clim_var_to_interpolate = clim_var_to_interpolate.sel(
                                **{clim_pressure_dim: var_level}, method="nearest"
                            ).squeeze(drop=True)

                    climatology_da_aligned = await asyncio.to_thread(
                        clim_var_to_interpolate.interp_like,
                        truth_var_da_final,
                        method="linear",
                        kwargs={"fill_value": None},
                    )

                    logger.info(
                        f"[FinalScore info Metric Input] Var: {var_key}, Level: {var_level}"
                    )
                    logger.info(
                        f"[FinalScore info Metric Input] truth_var_da_final -> Shape: {truth_var_da_final.shape}, Dims: {truth_var_da_final.dims}, Coords: {list(truth_var_da_final.coords.keys())}"
                    )
                    logger.info(
                        f"[FinalScore info Metric Input] miner_var_da_aligned -> Shape: {miner_var_da_aligned.shape}, Dims: {miner_var_da_aligned.dims}, Coords: {list(miner_var_da_aligned.coords.keys())}"
                    )
                    logger.info(
                        f"[FinalScore info Metric Input] climatology_da_aligned -> Shape: {climatology_da_aligned.shape}, Dims: {climatology_da_aligned.dims}, Coords: {list(climatology_da_aligned.coords.keys())}"
                    )
                    
                    # CRITICAL: Ensure consistent dimension ordering for pressure level variables
                    if var_level and "pressure_level" in miner_var_da_aligned.dims and "pressure_level" in truth_var_da_final.dims:
                        # Standardize to (lat, lon, pressure_level) order for all arrays
                        target_dims = ("lat", "lon", "pressure_level")
                        
                        # Transpose miner data to match truth data dimension order
                        if miner_var_da_aligned.dims != target_dims:
                            logger.debug(f"[FinalScore] Transposing miner data from {miner_var_da_aligned.dims} to {target_dims}")
                            miner_var_da_aligned = miner_var_da_aligned.transpose(*target_dims)
                        
                        # Transpose truth data if needed
                        if truth_var_da_final.dims != target_dims:
                            logger.debug(f"[FinalScore] Transposing truth data from {truth_var_da_final.dims} to {target_dims}")
                            truth_var_da_final = truth_var_da_final.transpose(*target_dims)
                        
                        # Transpose climatology data if needed
                        if "pressure_level" in climatology_da_aligned.dims and climatology_da_aligned.dims != target_dims:
                            logger.debug(f"[FinalScore] Transposing climatology data from {climatology_da_aligned.dims} to {target_dims}")
                            climatology_da_aligned = climatology_da_aligned.transpose(*target_dims)

                    lat_weights = None
                    if "lat" in truth_var_da_final.dims:
                        one_d_lat_weights = await asyncio.to_thread(
                            _calculate_latitude_weights, truth_var_da_final["lat"]
                        )

                        # CRITICAL: Always create spatial-only weights for xskillscore compatibility
                        # xskillscore expects weights to only have spatial dimensions (lat/lon)
                        # regardless of whether the data has pressure levels or not
                        lat_weights = one_d_lat_weights.expand_dims(
                            {"lon": truth_var_da_final["lon"]}
                        )
                        
                        if var_level is None:  # Surface variable (2t, 10u, 10v, msl)
                            logger.debug(
                                f"[FinalScore] Created spatial weights for surface variable {var_key}: {lat_weights.dims}"
                            )
                        else:  # Pressure level variable (t, u, v, q, z)
                            logger.debug(
                                f"[FinalScore] Created spatial weights for pressure-level variable {var_key}: {lat_weights.dims}"
                            )

                    current_metrics = {}
                    bias_corrected_forecast_da = (
                        await calculate_bias_corrected_forecast(
                            miner_var_da_aligned, truth_var_da_final
                        )
                    )

                    actual_bias_op = miner_var_da_aligned - truth_var_da_final

                    def calculate_mean_scalar_threaded(op):
                        computed_mean = op.mean()
                        if hasattr(computed_mean, "compute"):
                            computed_mean = computed_mean.compute()
                        return float(computed_mean.item())

                    mean_bias_val = await asyncio.to_thread(
                        calculate_mean_scalar_threaded, actual_bias_op
                    )
                    current_metrics["bias"] = mean_bias_val

                    # Compute per-level (or surface) MAE and Bias collapsed over spatial dims
                    try:
                        spatial_dims_bias_mae = [d for d in truth_var_da_final.dims if d.lower() in ("latitude", "longitude", "lat", "lon")]
                        abs_err_da = abs(actual_bias_op)
                        if mse_weights is not None:
                            weighted_abs = await asyncio.to_thread(
                                lambda: (abs_err_da * mse_weights).sum(dim=spatial_dims_bias_mae)
                            )
                            sum_w = await asyncio.to_thread(
                                lambda: mse_weights.sum(dim=spatial_dims_bias_mae)
                            )
                            mae_da = weighted_abs / sum_w

                            weighted_bias = await asyncio.to_thread(
                                lambda: (actual_bias_op * mse_weights).sum(dim=spatial_dims_bias_mae)
                            )
                            bias_da = weighted_bias / sum_w
                        else:
                            mae_da = await asyncio.to_thread(abs_err_da.mean, spatial_dims_bias_mae)
                            bias_da = await asyncio.to_thread(actual_bias_op.mean, spatial_dims_bias_mae)

                        per_level_mae_map = {}
                        per_level_bias_map = {}
                        if hasattr(mae_da, 'dims') and 'pressure_level' in mae_da.dims:
                            for level in mae_da.coords['pressure_level']:
                                per_level_mae_map[int(level.item())] = float(mae_da.sel(pressure_level=level).item())
                                per_level_bias_map[int(level.item())] = float(bias_da.sel(pressure_level=level).item())
                        else:
                            per_level_mae_map[None] = float(mae_da.item())
                            per_level_bias_map[None] = float(bias_da.item())
                    except Exception:
                        per_level_mae_map = {}
                        per_level_bias_map = {}

                    def calculate_raw_mse_preserve_dims(*args, **kwargs):
                        """Calculate MSE preserving pressure level dimensions for per-level processing."""
                        try:
                            res = xs.mse(*args, **kwargs)
                            if hasattr(res, "compute"):
                                res = res.compute()
                            return res  # Return raw result, preserving dimensions
                                
                        except Exception as mse_err:
                            import traceback
                            tb_str = traceback.format_exc()
                            logger.error(
                                f"[FinalScore] ERA5 MSE calculation failed for {var_key}:\n"
                                f"Args shapes: {[getattr(arg, 'shape', 'no shape') for arg in args if hasattr(arg, 'shape')]}\n"
                                f"Args dims: {[getattr(arg, 'dims', 'no dims') for arg in args if hasattr(arg, 'dims')]}\n"
                                f"Kwargs: {list(kwargs.keys())}\n"
                                f"Error: {mse_err}\n"
                                f"Full traceback:\n{tb_str}"
                            )
                            raise

                    # CRITICAL: Use spatial-only weights for xskillscore compatibility
                    # xskillscore expects weights to only have the dimensions being reduced over (lat/lon)
                    # NOT the full data dimensions (which may include pressure_level)
                    mse_weights = lat_weights
                    
                    if lat_weights is not None:
                        # Validate that weights have the correct spatial dimensions for xskillscore
                        data_dims = set(truth_var_da_final.dims)
                        weight_dims = set(lat_weights.dims)
                        spatial_dims = {'lat', 'lon'}
                        
                        # Check if weights have the expected spatial dimensions
                        if weight_dims == spatial_dims:
                            logger.debug(f"[FinalScore] Weights have correct spatial dimensions: {weight_dims}")
                        elif spatial_dims.issubset(weight_dims):
                            # Weights have extra dimensions - this is expected for pressure level variables
                            # but xskillscore needs spatial-only weights
                            extra_dims = weight_dims - spatial_dims
                            logger.debug(
                                f"[FinalScore] Weights have extra dimensions {extra_dims} beyond spatial dims {spatial_dims}"
                                f"\n  This is expected for pressure level variables"
                                f"\n  xskillscore will handle the broadcasting internally"
                            )
                        else:
                            raise ValueError(
                                f"[FinalScore] Weights missing required spatial dimensions. "
                                f"Required: {spatial_dims}, Got: {weight_dims}"
                            )

                    # CRITICAL: Use spatial-only reduction for MSE calculation with weights
                    # This ensures compatibility with spatial-only weights and proper skill score calculation
                    # For pressure level variables, we want to reduce over spatial dims and then aggregate pressure levels
                    spatial_dims = [d for d in truth_var_da_final.dims if d in ['lat', 'lon']]
                    mse_dims = spatial_dims if spatial_dims else None

                    logger.debug(
                        f"[FinalScore] MSE calculation - Data dims: {truth_var_da_final.dims}, MSE dims: {mse_dims}"
                    )
                    logger.debug(
                        f"[FinalScore] Data shapes - Miner: {miner_var_da_aligned.shape}, Truth: {truth_var_da_final.shape}"
                    )
                    logger.debug(
                        f"[FinalScore] Data dimensions - Miner: {miner_var_da_aligned.dims}, Truth: {truth_var_da_final.dims}"
                    )
                    if mse_weights is not None:
                        logger.debug(
                            f"[FinalScore] Weights shape: {mse_weights.shape}, dims: {mse_weights.dims}"
                        )
                        logger.debug(
                            f"[FinalScore] Weights coordinate sizes: {dict(mse_weights.sizes)}"
                        )
                        
                        # VALIDATION: Check for dimension compatibility before MSE calculation
                        truth_dims_set = set(truth_var_da_final.dims)
                        weights_dims_set = set(mse_weights.dims)
                        
                        # Check if weights can be broadcast to truth dimensions
                        # Weights should be a subset of truth dimensions (broadcastable)
                        if not weights_dims_set.issubset(truth_dims_set):
                            logger.warning(
                                f"[FinalScore] DIMENSION MISMATCH detected before MSE calculation:"
                                f"\n  Truth dims: {truth_dims_set}"
                                f"\n  Weights dims: {weights_dims_set}"
                                f"\n  Weights have extra dimensions not in truth data"
                                f"\n  Will attempt MSE without weights to avoid error"
                            )
                            mse_weights = None
                        else:
                            # Additional check: ensure weight shapes are compatible
                            try:
                                # Test broadcast compatibility by attempting a dummy operation
                                _ = truth_var_da_final * mse_weights
                                logger.debug(f"[FinalScore] Weights broadcast compatibility confirmed")
                            except Exception as broadcast_test_err:
                                raise ValueError(
                                    f"[FinalScore] Weights broadcast test failed: {broadcast_test_err}"
                                )

                    # Calculate MSE over spatial dimensions first
                    raw_mse_result = await asyncio.to_thread(
                        calculate_raw_mse_preserve_dims,
                        miner_var_da_aligned,
                        truth_var_da_final,
                        dim=mse_dims,
                        weights=mse_weights,
                        skipna=True,
                    )
                    
                    # Handle pressure level results - calculate individual scores per level
                    if hasattr(raw_mse_result, 'dims') and any(d in ['pressure_level', 'plev', 'lev', 'level'] for d in raw_mse_result.dims):
                        logger.debug(f"[FinalScore] MSE result has pressure dimension, calculating per-level scores: {raw_mse_result.dims}")
                        
                        # Get pressure level dimension name
                        pressure_dim = None
                        for d in raw_mse_result.dims:
                            if d in ['pressure_level', 'plev', 'lev', 'level']:
                                pressure_dim = d
                                break
                        
                        if pressure_dim:
                            # Calculate individual pressure level scores
                            pressure_levels = raw_mse_result.coords[pressure_dim].values
                            logger.debug(f"[FinalScore] Processing {len(pressure_levels)} pressure levels: {pressure_levels}")
                            
                            # Store per-level scores for later processing
                            per_level_mse_scores = {}
                            for level in pressure_levels:
                                level_mse = float(raw_mse_result.sel({pressure_dim: level}).item())
                                per_level_mse_scores[int(level)] = level_mse
                                
                            # Aggregate across pressure levels using mass-weighted thickness
                            levels = raw_mse_result.coords[pressure_dim].values.astype(float)
                            levels_sorted = np.sort(levels)
                            thickness = np.zeros_like(levels_sorted)
                            for i in range(len(levels_sorted)):
                                if i == 0:
                                    thickness[i] = (levels_sorted[i + 1] - levels_sorted[i]) / 2.0
                                elif i == len(levels_sorted) - 1:
                                    thickness[i] = (levels_sorted[i] - levels_sorted[i - 1]) / 2.0
                                else:
                                    thickness[i] = (levels_sorted[i + 1] - levels_sorted[i - 1]) / 2.0
                            thickness_map = {lvl: th for lvl, th in zip(levels_sorted, thickness)}
                            weights_pl = np.array([thickness_map[lvl] for lvl in levels])
                            weights_pl = weights_pl / np.sum(weights_pl)
                            level_vals = np.array([float(raw_mse_result.sel({pressure_dim: l}).item()) for l in levels])
                            raw_mse_val = float(np.sum(weights_pl * level_vals))
                            current_metrics["mse"] = raw_mse_val
                            current_metrics["rmse"] = np.sqrt(raw_mse_val)
                            current_metrics["per_level_mse"] = per_level_mse_scores
                        else:
                            logger.warning(f"[FinalScore] Could not identify pressure dimension in {raw_mse_result.dims}")
                            raw_mse_val = float(raw_mse_result.mean().item())
                            current_metrics["mse"] = raw_mse_val
                            current_metrics["rmse"] = np.sqrt(raw_mse_val)
                    else:
                        # Surface variable - already a scalar
                        raw_mse_val = raw_mse_result
                        current_metrics["mse"] = raw_mse_val
                        current_metrics["rmse"] = np.sqrt(raw_mse_val)

                    rmse_metric_row = {
                        **db_metric_row_base,
                        "metrics": current_metrics.copy(),
                        "score_type": f"era5_rmse_{var_key}_{int(lead_hours)}h",
                        "score": current_metrics["rmse"],
                    }
                    all_metrics_for_db.append(rmse_metric_row)
                    
                    # NEW: Create component scores - individual scores for pressure levels or single score for surface
                    calculation_duration_ms = int((time.time() - scoring_start_time) * 1000)
                    component_scores_for_this_var = []
                    
                    # Check if we have per-level scores (atmospheric variables with level="all")
                    if "per_level_mse" in current_metrics:
                        logger.debug(f"[FinalScore] Creating per-level component scores for {var_name}")
                        # Create individual component scores for each pressure level
                        for pressure_level, level_mse in current_metrics["per_level_mse"].items():
                            level_rmse = np.sqrt(level_mse)
                            component_score = {
                                'run_id': run_id,
                                'response_id': response_id,
                                'miner_uid': miner_uid,
                                'miner_hotkey': miner_hotkey,
                                'score_type': 'era5',
                                'lead_hours': int(lead_hours),
                                'valid_time_utc': valid_time_dt,
                                'variable_name': var_name,
                                'pressure_level': pressure_level,  # Individual pressure level
                                'rmse': level_rmse,
                                'mse': level_mse,
                                'bias': per_level_bias_map.get(pressure_level),
                                'mae': per_level_mae_map.get(pressure_level),
                                'calculation_duration_ms': calculation_duration_ms,
                            }
                            # Will add ACC and skill scores later for each level
                            component_scores_for_this_var = component_scores_for_this_var or []
                            component_scores_for_this_var.append(component_score)
                    else:
                        # Surface variable or single pressure level - create single component score
                        pressure_level_db = None if var_level == "all" or var_level is None else int(var_level)
                        component_score = {
                            'run_id': run_id,
                            'response_id': response_id,
                            'miner_uid': miner_uid,
                            'miner_hotkey': miner_hotkey,
                            'score_type': 'era5',
                            'lead_hours': int(lead_hours),
                            'valid_time_utc': valid_time_dt,
                            'variable_name': var_name,
                            'pressure_level': pressure_level_db,
                            'rmse': current_metrics["rmse"],
                            'mse': raw_mse_val,
                            'bias': mean_bias_val,
                            'mae': per_level_mae_map.get(None),
                            'calculation_duration_ms': calculation_duration_ms,
                        }
                        component_scores_for_this_var = [component_score]

                    # Prepare a default climatology reference for skill score; will be standardized for ACC in PL branch
                    climatology_skill_input = climatology_da_aligned

                    # Calculate ACC - use per-level function for atmospheric variables
                    if "per_level_mse" in current_metrics:
                        # Atmospheric variable - calculate ACC per pressure level
                        logger.debug(f"[FinalScore] ACC Input Data Debug for {var_key}:")
                        logger.debug(f"  miner_var_da_aligned: dims={miner_var_da_aligned.dims}, shape={miner_var_da_aligned.shape}")
                        logger.debug(f"  truth_var_da_final: dims={truth_var_da_final.dims}, shape={truth_var_da_final.shape}")
                        logger.debug(f"  climatology_da_aligned: dims={climatology_da_aligned.dims}, shape={climatology_da_aligned.shape}")
                        logger.debug(f"  mse_weights: dims={mse_weights.dims if mse_weights is not None else None}")
                        
                        # CRITICAL FIX: Ensure all datasets have consistent coordinate names for ACC calculation
                        def ensure_consistent_coords(da):
                            """Ensure all coordinate names are consistent across all datasets."""
                            if hasattr(da, 'dims'):
                                coord_rename_map = {}
                                
                                # Standardize pressure coordinates
                                for dim in da.dims:
                                    if dim in ['plev', 'lev', 'level'] and dim != 'pressure_level':
                                        coord_rename_map[dim] = 'pressure_level'
                                
                                # Standardize spatial coordinates
                                for dim in da.dims:
                                    if dim in ['latitude'] and dim != 'lat':
                                        coord_rename_map[dim] = 'lat'
                                    elif dim in ['longitude'] and dim != 'lon':
                                        coord_rename_map[dim] = 'lon'
                                
                                # Also check coordinate names (not just dimension names)
                                for coord_name in da.coords:
                                    if coord_name in ['plev', 'lev', 'level'] and coord_name != 'pressure_level':
                                        coord_rename_map[coord_name] = 'pressure_level'
                                    elif coord_name in ['latitude'] and coord_name != 'lat':
                                        coord_rename_map[coord_name] = 'lat'
                                    elif coord_name in ['longitude'] and coord_name != 'lon':
                                        coord_rename_map[coord_name] = 'lon'
                                
                                if coord_rename_map:
                                    logger.debug(f"[FinalScore] ACC coord standardization: {coord_rename_map}")
                                    return da.rename(coord_rename_map)
                            return da
                        
                        # Apply consistent coordinate naming to all inputs
                        miner_acc_input = ensure_consistent_coords(miner_var_da_aligned)
                        truth_acc_input = ensure_consistent_coords(truth_var_da_final)
                        climatology_acc_input = ensure_consistent_coords(climatology_da_aligned)
                        # Use the same standardized climatology for skill score reference
                        climatology_skill_input = climatology_acc_input
                        
                        logger.debug(f"[FinalScore] ACC inputs after coord standardization:")
                        logger.debug(f"  miner: dims={miner_acc_input.dims}, coords={list(miner_acc_input.coords.keys())}")
                        logger.debug(f"  truth: dims={truth_acc_input.dims}, coords={list(truth_acc_input.coords.keys())}")
                        logger.debug(f"  climatology: dims={climatology_acc_input.dims}, coords={list(climatology_acc_input.coords.keys())}")
                        
                        from ..weather_scoring.metrics import calculate_acc_by_pressure_level
                        per_level_acc = await calculate_acc_by_pressure_level(
                            miner_acc_input,
                            truth_acc_input,
                            climatology_acc_input,
                            mse_weights,
                        )
                        
                        # Store per-level ACC scores
                        current_metrics["per_level_acc"] = per_level_acc
                        
                        # Aggregate ACC across pressure levels using mass-weighted thickness
                        if per_level_acc:
                            levels = truth_var_da_final.coords['pressure_level'].values.astype(float)
                            levels_sorted = np.sort(levels)
                            thickness = np.zeros_like(levels_sorted)
                            for i in range(len(levels_sorted)):
                                if i == 0:
                                    thickness[i] = (levels_sorted[i + 1] - levels_sorted[i]) / 2.0
                                elif i == len(levels_sorted) - 1:
                                    thickness[i] = (levels_sorted[i] - levels_sorted[i - 1]) / 2.0
                                else:
                                    thickness[i] = (levels_sorted[i + 1] - levels_sorted[i - 1]) / 2.0
                            thickness_map = {lvl: th for lvl, th in zip(levels_sorted, thickness)}
                            weights_pl = np.array([thickness_map[lvl] for lvl in levels])
                            weights_pl = weights_pl / np.sum(weights_pl)
                            acc_vals = np.array([per_level_acc.get(int(l), np.nan) for l in levels])
                            if np.isnan(acc_vals).any():
                                raise ValueError(
                                    f"[FinalScore] ACC aggregation encountered NaN per-level values: {acc_vals}"
                                )
                            acc_val = float(np.sum(weights_pl * acc_vals))
                        else:
                            raise ValueError("[FinalScore] No per-level ACC scores to aggregate")
                        current_metrics["acc"] = acc_val
                        
                        # Update component scores with per-level ACC
                        for component_score in component_scores_for_this_var:
                            pressure_level = component_score['pressure_level']
                            component_score['acc'] = per_level_acc.get(pressure_level, np.nan)
                    else:
                        # Surface variable - single ACC calculation
                        acc_val = await calculate_acc(
                            miner_var_da_aligned,
                            truth_var_da_final,
                            climatology_da_aligned,
                            mse_weights,
                        )
                        current_metrics["acc"] = acc_val
                        
                        # Update component score with ACC
                        for component_score in component_scores_for_this_var:
                            component_score['acc'] = acc_val
                    
                    # Create aggregate ACC metric row for backward compatibility
                    acc_metric_row = {
                        **db_metric_row_base,
                        "metrics": current_metrics.copy(),
                        "score_type": f"era5_acc_{var_key}_{int(lead_hours)}h",
                        "score": acc_val,
                    }
                    all_metrics_for_db.append(acc_metric_row)

                    skill_score_val = None

                    if gfs_op_lead_slice is not None:
                        try:
                            if var_name in gfs_op_lead_slice:
                                gfs_var_da_unaligned = gfs_op_lead_slice[var_name]

                                if var_level:
                                    gfs_pressure_dim = None
                                    for dim_name in [
                                        "pressure_level",
                                        "plev",
                                        "level",
                                        "isobaricInhPa",
                                    ]:
                                        if dim_name in gfs_var_da_unaligned.dims:
                                            gfs_pressure_dim = dim_name
                                            break

                                    if gfs_pressure_dim:
                                        gfs_var_da_selected = gfs_var_da_unaligned.sel(
                                            {gfs_pressure_dim: var_level},
                                            method="nearest",
                                        )
                                    else:
                                        logger.warning(
                                            f"[FinalScore] No pressure dimension found in GFS data for {var_key}. Using surface data."
                                        )
                                        gfs_var_da_selected = gfs_var_da_unaligned
                                else:
                                    gfs_var_da_selected = gfs_var_da_unaligned

                                gfs_var_da_std = _standardize_spatial_dims_final(
                                    gfs_var_da_selected
                                )
                                gfs_var_da_aligned = await asyncio.to_thread(
                                    gfs_var_da_std.interp_like,
                                    truth_var_da_final,
                                    method="linear",
                                    kwargs={"fill_value": None},
                                )

                                # bias correction - fresh calculation for each variable to prevent reuse
                                forecast_bc_da = (
                                    await calculate_bias_corrected_forecast(
                                        miner_var_da_aligned, truth_var_da_final
                                    )
                                )

                                # skill score
                                # For atmospheric variables with multiple pressure levels, compute per-level skill
                                per_level_skill_gfs = None
                                if "per_level_mse" in current_metrics:
                                    from ..weather_scoring.metrics import calculate_mse_skill_score_by_pressure_level
                                    per_level_skill_map = await calculate_mse_skill_score_by_pressure_level(
                                        forecast_bc_da,
                                        truth_var_da_final,
                                        gfs_var_da_aligned,
                                        mse_weights,
                                    )
                                    per_level_skill_gfs = per_level_skill_map
                                    # Aggregate to a single value for the summary metric using thickness weights
                                    try:
                                        levels = truth_var_da_final.coords['pressure_level'].values.astype(float)
                                        levels_sorted = np.sort(levels)
                                        thickness = np.zeros_like(levels_sorted)
                                        for i in range(len(levels_sorted)):
                                            if i == 0:
                                                thickness[i] = (levels_sorted[i + 1] - levels_sorted[i]) / 2.0
                                            elif i == len(levels_sorted) - 1:
                                                thickness[i] = (levels_sorted[i] - levels_sorted[i - 1]) / 2.0
                                            else:
                                                thickness[i] = (levels_sorted[i + 1] - levels_sorted[i - 1]) / 2.0
                                        thickness_map = {lvl: th for lvl, th in zip(levels_sorted, thickness)}
                                        weights_pl = np.array([thickness_map[lvl] for lvl in levels])
                                        weights_pl = weights_pl / np.sum(weights_pl)
                                        skill_vals = np.array([per_level_skill_map.get(int(l), np.nan) for l in levels])
                                        # Replace NaNs with zeros for aggregation safety
                                        skill_vals = np.nan_to_num(skill_vals, nan=0.0)
                                        skill_score_val = float(np.sum(weights_pl * skill_vals))
                                    except Exception:
                                        # Fallback to simple mean if any issue with thickness weights
                                        skill_vals_list = [v for v in per_level_skill_map.values() if np.isfinite(v)]
                                        skill_score_val = float(np.mean(skill_vals_list)) if skill_vals_list else None
                                else:
                                    skill_score_val = await calculate_mse_skill_score(
                                        forecast_bc_da,
                                        truth_var_da_final,
                                        gfs_var_da_aligned,
                                        mse_weights,
                                    )

                                if np.isfinite(skill_score_val):
                                    logger.info(
                                        f"[FinalScore] UID {miner_uid} - Calculated GFS-based skill for {var_key} L{lead_hours}h: {skill_score_val:.4f}"
                                    )
                                    skill_metric_row = {
                                        **db_metric_row_base,
                                        "metrics": current_metrics.copy(),
                                        "score_type": f"era5_skill_gfs_{var_key}_{int(lead_hours)}h",
                                        "score": skill_score_val,
                                    }
                                    all_metrics_for_db.append(skill_metric_row)
                                    
                                    # Add GFS skill score to component scores
                                    if per_level_skill_gfs is not None:
                                        for component_score in component_scores_for_this_var:
                                            pl = component_score['pressure_level']
                                            if pl is not None:
                                                component_score['skill_score_gfs'] = per_level_skill_gfs.get(pl)
                                    else:
                                        for component_score in component_scores_for_this_var:
                                            component_score['skill_score_gfs'] = skill_score_val
                                else:
                                    logger.warning(
                                        f"[FinalScore] UID {miner_uid} - Calculated skill score is non-finite for {var_key} L{lead_hours}h"
                                    )
                                    skill_score_val = None
                            else:
                                logger.warning(
                                    f"[FinalScore] UID {miner_uid} - Variable {var_name} not found in GFS forecast data"
                                )
                        except Exception as e_skill:
                            logger.error(
                                f"[FinalScore] UID {miner_uid} - Error calculating skill score for {var_key} L{lead_hours}h: {e_skill}",
                                exc_info=True,
                            )
                            skill_score_val = None
                    else:
                        logger.debug(
                            f"[FinalScore] UID {miner_uid} - Using ERA5-climatology skill scores (GFS operational not available)"
                        )

                    if skill_score_val is None:
                        try:
                            # bias correction - ALWAYS recalculate for each variable (prevent variable reuse bug)
                            forecast_bc_da = await calculate_bias_corrected_forecast(
                                miner_var_da_aligned, truth_var_da_final
                            )

                            # skill score vs climatology - possibly per pressure level
                            # Enforce strict dimension order to match truth for rigorous scorer
                            if hasattr(climatology_skill_input, 'dims') and hasattr(truth_var_da_final, 'dims'):
                                if set(climatology_skill_input.dims) == set(truth_var_da_final.dims) and tuple(climatology_skill_input.dims) != tuple(truth_var_da_final.dims):
                                    logger.debug(
                                        f"[FinalScore] Aligning climatology dims for skill: {climatology_skill_input.dims} -> {truth_var_da_final.dims}"
                                    )
                                    climatology_skill_input = climatology_skill_input.transpose(*truth_var_da_final.dims)
                            per_level_skill_clim = None
                            if "per_level_mse" in current_metrics:
                                from ..weather_scoring.metrics import calculate_mse_skill_score_by_pressure_level
                                per_level_skill_map = await calculate_mse_skill_score_by_pressure_level(
                                    forecast_bc_da,
                                    truth_var_da_final,
                                    climatology_skill_input,
                                    mse_weights,
                                )
                                per_level_skill_clim = per_level_skill_map
                                # Aggregate with thickness weights
                                try:
                                    levels = truth_var_da_final.coords['pressure_level'].values.astype(float)
                                    levels_sorted = np.sort(levels)
                                    thickness = np.zeros_like(levels_sorted)
                                    for i in range(len(levels_sorted)):
                                        if i == 0:
                                            thickness[i] = (levels_sorted[i + 1] - levels_sorted[i]) / 2.0
                                        elif i == len(levels_sorted) - 1:
                                            thickness[i] = (levels_sorted[i] - levels_sorted[i - 1]) / 2.0
                                        else:
                                            thickness[i] = (levels_sorted[i + 1] - levels_sorted[i - 1]) / 2.0
                                    thickness_map = {lvl: th for lvl, th in zip(levels_sorted, thickness)}
                                    weights_pl = np.array([thickness_map[lvl] for lvl in levels])
                                    weights_pl = weights_pl / np.sum(weights_pl)
                                    skill_vals = np.array([per_level_skill_map.get(int(l), np.nan) for l in levels])
                                    skill_vals = np.nan_to_num(skill_vals, nan=0.0)
                                    skill_score_val = float(np.sum(weights_pl * skill_vals))
                                except Exception:
                                    # Fallback to simple mean
                                    skill_vals_list = [v for v in per_level_skill_map.values() if np.isfinite(v)]
                                    skill_score_val = float(np.mean(skill_vals_list)) if skill_vals_list else None
                            else:
                                skill_score_val = await calculate_mse_skill_score(
                                    forecast_bc_da,
                                    truth_var_da_final,
                                    climatology_skill_input,  # Use standardized climatology when available
                                    mse_weights,
                                )

                            if np.isfinite(skill_score_val):
                                logger.info(
                                    f"[FinalScore] UID {miner_uid} - Calculated CLIM-based skill for {var_key} L{lead_hours}h: {skill_score_val:.4f}"
                                )
                                skill_metric_row = {
                                    **db_metric_row_base,
                                    "metrics": current_metrics.copy(),
                                    "score_type": f"era5_skill_clim_{var_key}_{int(lead_hours)}h",
                                    "score": skill_score_val,
                                }
                                all_metrics_for_db.append(skill_metric_row)
                                
                                # Add climatology skill score to component scores
                                if per_level_skill_clim is not None:
                                    for component_score in component_scores_for_this_var:
                                        pl = component_score['pressure_level']
                                        if pl is not None:
                                            component_score['skill_score_climatology'] = per_level_skill_clim.get(pl)
                                else:
                                    for component_score in component_scores_for_this_var:
                                        component_score['skill_score_climatology'] = skill_score_val
                            else:
                                logger.warning(
                                    f"[FinalScore] UID {miner_uid} - Calculated climatology skill score is non-finite for {var_key} L{lead_hours}h"
                                )
                                skill_score_val = None
                        except Exception as e_clim_skill:
                            logger.error(
                                f"[FinalScore] UID {miner_uid} - Error calculating climatology skill score for {var_key} L{lead_hours}h: {e_clim_skill}",
                                exc_info=True,
                            )
                            skill_score_val = None

                    if skill_score_val is None:
                        logger.warning(
                            f"[FinalScore] UID {miner_uid} - No valid skill score calculated for {var_key} L{lead_hours}h"
                        )

                    skill_score_log_str = (
                        f"{skill_score_val:.3f}"
                        if skill_score_val is not None
                        else "N/A"
                    )
                    logger.info(
                        f"[FinalScore] Miner {miner_hotkey} V:{var_key} L:{int(lead_hours)}h RMSE:{current_metrics.get('rmse', np.nan):.2f} ACC:{current_metrics.get('acc', np.nan):.3f} SKILL:{skill_score_log_str}"
                    )
                    
                    # Finalize and append component scores
                    from ..weather_scoring.scoring import VARIABLE_WEIGHTS
                    base_variable_weight = VARIABLE_WEIGHTS.get(var_name, 0.0)
                    
                    for component_score in component_scores_for_this_var:
                        # Add skill score if available; prefer per-level if present
                        if 'skill_score_gfs' in component_score and component_score.get('skill_score_gfs') is not None:
                            component_score['skill_score'] = component_score['skill_score_gfs']
                        elif 'skill_score_climatology' in component_score and component_score.get('skill_score_climatology') is not None:
                            component_score['skill_score'] = component_score['skill_score_climatology']
                        elif skill_score_val is not None:
                            component_score['skill_score'] = skill_score_val
                        
                        # Add variable weight - for pressure level variables, distribute weight across levels
                        if "per_level_mse" in current_metrics:
                            # Distribute weight equally across all pressure levels
                            num_levels = len(current_metrics["per_level_mse"])
                            component_score['variable_weight'] = base_variable_weight / num_levels
                        else:
                            # Surface variable gets full weight
                            component_score['variable_weight'] = base_variable_weight
                        
                        all_component_scores.append(component_score)

                except KeyError as ke:
                    logger.error(
                        f"[FinalScore] Miner {miner_hotkey}: KeyError scoring {var_key} at {valid_time_dt}: {ke}. This often means the variable was not found in one of the datasets (miner, truth, or climatology).",
                        exc_info=True,
                    )
                    error_metric_row = {
                        **db_metric_row_base,
                        "score_type": f"era5_error_{var_key}_{int(lead_hours)}h",
                        "error_message": f"KeyError: {ke}",
                    }
                    all_metrics_for_db.append(error_metric_row)
                except Exception as e_var_score:
                    import traceback
                    tb_str = traceback.format_exc()
                    error_msg = str(e_var_score)

                    # Treat network/payload issues as hard failures for this miner/run
                    critical_io_signatures = [
                        "ContentLengthError",
                        "ClientPayloadError",
                        "Response payload is not completed",
                        "Not enough data for satisfy content length header",
                        "ClientOSError",
                        "Connection reset by peer",
                    ]
                    if any(sig in error_msg for sig in critical_io_signatures):
                        logger.error(
                            f"[FinalScore] Hard-failing miner {miner_hotkey} run {run_id} due to IO/payload error while scoring {var_key} at {valid_time_dt}: {error_msg}"
                        )
                        return False
                    
                    logger.error(
                        (
                            f"[FinalScore] Miner {miner_hotkey}: Error scoring {var_key} at {valid_time_dt}:\n"
                            f"Error: {error_msg}\n"
                            f"Variable config: {var_config}\n"
                            f"Data shapes - Miner: {getattr(miner_forecast_lead_slice, 'shape', 'unknown')}, "
                            f"Truth: {getattr(era5_truth_lead_slice, 'shape', 'unknown')}\n"
                            f"Lead hours: {lead_hours}\n"
                            f"Full traceback:\n{tb_str}"
                        ),
                        exc_info=True,
                    )
                    
                    # Add debug info for type errors
                    if "unsupported operand type" in error_msg:
                        logger.error("[FinalScore] Type error debug info:")
                        logger.error(f"  - gfs_init_time_of_run type: {type(gfs_init_time_of_run)} = {gfs_init_time_of_run}")
                        logger.error(f"  - valid_time_dt type: {type(valid_time_dt)} = {valid_time_dt}")
                        logger.error(f"  - lead_hours type: {type(lead_hours)} = {lead_hours}")
                        logger.error(f"  - var_key: {var_key}")
                        logger.error(f"  - var_name: {var_name}, var_level: {var_level}")
                    
                    # Check for coordinate/dimension errors and fail fast
                    if any(coord_err in error_msg.lower() for coord_err in ['lat', 'lon', 'pressure_level', 'coordinate', 'dimension', 'keyerror']):
                        logger.error(
                            f"[FinalScore] Miner {miner_hotkey}: CRITICAL coordinate/dimension error for {var_key} - this indicates a fundamental data structure issue",
                        )
                        return False  # Fail the entire ERA5 scoring for this miner
                    
                    error_metric_row = {
                        **db_metric_row_base,
                        "score_type": f"era5_error_{var_key}_{int(lead_hours)}h",
                        "error_message": error_msg,
                        "debug_info": {
                            "var_config": var_config,
                            "miner_shape": getattr(miner_forecast_lead_slice, 'shape', None),
                            "truth_shape": getattr(era5_truth_lead_slice, 'shape', None),
                            "traceback": tb_str
                        }
                    }
                    all_metrics_for_db.append(error_metric_row)
    finally:
        if miner_forecast_ds:
            try:
                miner_forecast_ds.close()
                logger.debug(
                    f"[FinalScore] Closed miner forecast dataset for {miner_hotkey}"
                )
            except Exception:
                pass

        # CRITICAL: Enhanced cleanup per miner for ERA5 scoring
        try:
            # Clear any ERA5-specific intermediate objects created during this miner's evaluation
            miner_specific_objects = [
                "miner_forecast_ds",
                "gfs_operational_fcst_ds",
                "miner_forecast_lead_slice",
                "era5_truth_lead_slice",
                "gfs_op_lead_slice",
            ]

            for obj_name in miner_specific_objects:
                if obj_name in locals():
                    try:
                        obj = locals()[obj_name]
                        if hasattr(obj, "close"):
                            obj.close()
                        del obj
                    except Exception:
                        pass

            # Force cleanup for this miner's processing
            collected = gc.collect()
            logger.debug(
                f"[FinalScore] Cleanup for {miner_hotkey}: collected {collected} objects"
            )

        except Exception as cleanup_err:
            logger.debug(
                f"[FinalScore] Cleanup error for {miner_hotkey}: {cleanup_err}"
            )

        # Final garbage collection
        gc.collect()

    if not all_metrics_for_db:
        logger.warning(
            f"[FinalScore] Miner {miner_hotkey}: No metrics were calculated. This might be due to selection errors for all valid_times or variables."
        )
        return False

    insert_query = """
        INSERT INTO weather_miner_scores 
        (response_id, run_id, miner_uid, miner_hotkey, score_type, score, metrics, calculation_time, error_message, lead_hours, variable_level, valid_time_utc)
        VALUES (:response_id, :run_id, :miner_uid, :miner_hotkey, :score_type, :score, :metrics_json, :calculation_time, :error_message, :lead_hours, :variable_level, :valid_time_utc)
        ON CONFLICT (response_id, score_type, lead_hours, variable_level, valid_time_utc) DO UPDATE SET 
        score = EXCLUDED.score, metrics = EXCLUDED.metrics, calculation_time = EXCLUDED.calculation_time, error_message = EXCLUDED.error_message,
        miner_uid = EXCLUDED.miner_uid, miner_hotkey = EXCLUDED.miner_hotkey, run_id = EXCLUDED.run_id 
    """

    successful_inserts = 0
    for metric_record in all_metrics_for_db:
        params = metric_record.copy()
        params.setdefault("score", None)
        params.setdefault("metrics", {})
        params.setdefault("error_message", None)

        # Use safe JSON serialization to handle infinity/NaN values
        from ..utils.json_sanitizer import safe_json_dumps_for_db

        params["metrics_json"] = safe_json_dumps_for_db(params.pop("metrics"))

        try:
            await task_instance.db_manager.execute(insert_query, params)
            successful_inserts += 1
        except Exception as db_err_indiv:
            logger.error(
                f"[FinalScore] Miner {miner_hotkey}: DB error storing single ERA5 score record ({params.get('score_type')} for {params.get('variable_level')} at {params.get('lead_hours')}h): {db_err_indiv}",
                exc_info=False,
            )  # exc_info=False to avoid too much noise if many fail

    if successful_inserts > 0:
        logger.info(
            f"[FinalScore] Miner {miner_hotkey}: Stored/Updated {successful_inserts}/{len(all_metrics_for_db)} ERA5 metric records to DB."
        )
        
        # NEW: Batch insert component scores for full transparency
        if all_component_scores:
            logger.info(f"[FinalScore] Inserting {len(all_component_scores)} component scores for {miner_hotkey}")
            component_insert_success = await _batch_insert_component_scores(
                task_instance.db_manager,
                all_component_scores,
                batch_size=100
            )
            if not component_insert_success:
                logger.warning(f"[FinalScore] Failed to insert some component scores for {miner_hotkey}")
            else:
                logger.success(f"[FinalScore] ✅ Successfully inserted all component scores for {miner_hotkey}")
        
        return True
    else:
        logger.error(
            f"[FinalScore] Miner {miner_hotkey}: Failed to store any ERA5 metric records to DB."
        )
        return False


async def _calculate_and_store_aggregated_era5_score(
    task_instance: "WeatherTask",
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    response_id: int,
    lead_hours_scored: List[int],
    vars_levels_scored: List[Dict],
) -> Optional[float]:
    """
    Calculates a single aggregated ERA5 score (0-1) for a miner.
    Composition: 50% Skill/Error Component, 50% ACC Component.
    Equal weighting for var/level within each component for now.
    """
    logger.info(
        f"[AggFinalScore] Calculating aggregated ERA5 score for UID {miner_uid}, Run {run_id}"
    )

    # Define clamp before any usage within this function to avoid local variable resolution errors
    def clamp(x: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, x))

    skill_error_component_scores = []
    acc_component_scores = []

    for lead_h in lead_hours_scored:
        for var_config in vars_levels_scored:
            var_name = var_config.get("name")
            if not var_name:
                logger.warning(f"[FinalScore] Skipping aggregation for variable with missing name in config: {var_config}")
                continue
            var_level = var_config.get("level")
            # Match score_type var_key formatting used during scoring: omit 'all'
            var_key = f"{var_name}{var_level if var_level and var_level != 'all' else ''}"

            rmse_score_val = None
            skill_score_val = None
            acc_score_val = None

            # Fetch RMSE
            rmse_score_type = f"era5_rmse_{var_key}_{lead_h}h"
            rmse_rec = await task_instance.db_manager.fetch_one(
                "SELECT score FROM weather_miner_scores WHERE response_id = :resp_id AND score_type = :stype",
                {"resp_id": response_id, "stype": rmse_score_type},
            )
            if (
                rmse_rec
                and rmse_rec["score"] is not None
                and np.isfinite(rmse_rec["score"])
            ):
                rmse_score_val = rmse_rec["score"]

            skill_score_val = None
            skill_score_type_gfs = f"era5_skill_gfs_{var_key}_{lead_h}h"
            skill_rec_gfs = await task_instance.db_manager.fetch_one(
                "SELECT score FROM weather_miner_scores WHERE response_id = :resp_id AND score_type = :stype",
                {"resp_id": response_id, "stype": skill_score_type_gfs},
            )
            if (
                skill_rec_gfs
                and skill_rec_gfs["score"] is not None
                and np.isfinite(skill_rec_gfs["score"])
            ):
                skill_score_val = skill_rec_gfs["score"]
                logger.info(
                    f"[AggFinalScore] UID {miner_uid} - Using GFS-based skill for {var_key} L{lead_h}h: {skill_score_val:.4f}"
                )
            else:
                skill_score_type_clim = f"era5_skill_clim_{var_key}_{lead_h}h"
                skill_rec_clim = await task_instance.db_manager.fetch_one(
                    "SELECT score FROM weather_miner_scores WHERE response_id = :resp_id AND score_type = :stype",
                    {"resp_id": response_id, "stype": skill_score_type_clim},
                )
                if (
                    skill_rec_clim
                    and skill_rec_clim["score"] is not None
                    and np.isfinite(skill_rec_clim["score"])
                ):
                    skill_score_val = skill_rec_clim["score"]
                    logger.info(
                        f"[AggFinalScore] UID {miner_uid} - Using CLIM-based skill for {var_key} L{lead_h}h: {skill_score_val:.4f} (ERA5-climatology skill scoring)"
                    )
                else:
                    logger.warning(
                        f"[AggFinalScore] UID {miner_uid} - No valid climatology skill score found for {var_key} L{lead_h}h."
                    )

            acc_score_type = f"era5_acc_{var_key}_{lead_h}h"
            acc_rec = await task_instance.db_manager.fetch_one(
                "SELECT score FROM weather_miner_scores WHERE response_id = :resp_id AND score_type = :stype",
                {"resp_id": response_id, "stype": acc_score_type},
            )
            if (
                acc_rec
                and acc_rec["score"] is not None
                and np.isfinite(acc_rec["score"])
            ):
                acc_score_val = acc_rec["score"]

            normalized_skill_error_item = 0.0
            if skill_score_val is not None:
                # Defensively clamp skill-based item to [0, 1]
                normalized_skill_error_item = clamp(float(skill_score_val), 0.0, 1.0)
            elif rmse_score_val is not None and rmse_score_val >= 0:
                normalized_skill_error_item = 1.0 / (1.0 + rmse_score_val)
            else:
                logger.warning(
                    f"[AggFinalScore] No valid Skill Score or RMSE for {var_key} at lead {lead_h}h for UID {miner_uid}. Skill/Error item will be 0."
                )
            skill_error_component_scores.append(normalized_skill_error_item)

            normalized_acc_item = 0.0
            if acc_score_val is not None:
                # ACC expected in [-1, 1]; clamp normalized to [0, 1]
                normalized_acc_item = clamp(((float(acc_score_val) + 1.0) / 2.0), 0.0, 1.0)
            else:
                logger.warning(
                    f"[AggFinalScore] No valid ACC for {var_key} at lead {lead_h}h for UID {miner_uid}. ACC item will be 0."
                )
            acc_component_scores.append(normalized_acc_item)

    # PROGRESSIVE SCORING FIX: Use total expected components as denominator (zeros for missing lead times)
    # This ensures scores can only improve over time, not degrade as longer lead times are added
    total_expected_lead_hours = task_instance.config.get(
        "final_scoring_lead_hours", [24, 48, 72, 96, 120, 144, 168, 192, 216, 240]
    )
    total_expected_components_per_type = len(total_expected_lead_hours) * len(
        vars_levels_scored
    )

    # Calculate progressive averages with zeros as placeholders for missing components
    avg_skill_error_score = (
        sum(skill_error_component_scores) / total_expected_components_per_type
        if total_expected_components_per_type > 0
        else 0.0
    )
    avg_acc_score = (
        sum(acc_component_scores) / total_expected_components_per_type
        if total_expected_components_per_type > 0
        else 0.0
    )

    final_score_val = (0.5 * avg_skill_error_score) + (0.5 * avg_acc_score)

    # Compute normalized per-lead ERA5 scores combining skill/acc/rmse if available
    # Then write per-lead normalized scores, averages, and overall_forecast_score (80% ERA5 + 20% Day1)
    try:
        # Fetch Day1 overall score for binary QC blend
        day1_row = await task_instance.db_manager.fetch_one(
            """
            SELECT forecast_score_initial
            FROM weather_forecast_stats
            WHERE run_id = :rid AND miner_uid = :uid
            """,
            {"rid": run_id, "uid": miner_uid},
        )
        day1_overall = float(day1_row["forecast_score_initial"]) if day1_row and day1_row["forecast_score_initial"] is not None else None

        # Compute normalized per-lead scores from weather_miner_scores table
        # We aggregate by lead and variable
        rows = await task_instance.db_manager.fetch_all(
            """
            SELECT lead_hours, score_type, score
            FROM weather_miner_scores
            WHERE run_id = :rid AND miner_uid = :uid AND score IS NOT NULL
              AND (score_type LIKE 'era5_rmse_%' OR score_type LIKE 'era5_acc_%' OR score_type LIKE 'era5_skill_%')
            """,
            {"rid": run_id, "uid": miner_uid},
        )
        per_lead_metrics: Dict[int, Dict[str, List[float]]] = {}
        for r in rows:
            lh = r.get("lead_hours")
            st = r.get("score_type")
            sc = r.get("score")
            if lh is None or st is None or sc is None:
                continue
            bucket = per_lead_metrics.setdefault(int(lh), {"acc": [], "skill": [], "rmse": []})
            if st.startswith("era5_acc_"):
                bucket["acc"].append(float(sc))
            elif st.startswith("era5_skill_"):
                bucket["skill"].append(float(sc))
            elif st.startswith("era5_rmse_"):
                bucket["rmse"].append(float(sc))

        normalized_by_lead: Dict[int, float] = {}
        rmse_ref_by_lead: Dict[int, float] = {}
        # Establish a reference RMSE per lead as median of miner RMSEs for stability when GFS ref not present in table
        # Here we fallback to miner's own median across vars; could be enhanced with network-wide stats
        for lh, met in per_lead_metrics.items():
            rmse_vals = [v for v in met["rmse"] if v is not None]
            if rmse_vals:
                rmse_vals_sorted = sorted(rmse_vals)
                rmse_ref_by_lead[lh] = rmse_vals_sorted[len(rmse_vals_sorted)//2]
            else:
                rmse_ref_by_lead[lh] = 1.0

        for lh, met in per_lead_metrics.items():
            # Normalize components
            acc_vals = [a for a in met["acc"] if a is not None]
            skill_vals = [s for s in met["skill"] if s is not None]
            rmse_vals = [e for e in met["rmse"] if e is not None]

            acc_norm = None
            if acc_vals:
                acc_norm = sum((a + 1.0) / 2.0 for a in acc_vals) / len(acc_vals)

            skill_norm = None
            if skill_vals:
                skill_norm = sum(clamp(s, 0.0, 1.0) for s in skill_vals) / len(skill_vals)

            rmse_norm = None
            if rmse_vals:
                ref = rmse_ref_by_lead.get(lh, 1.0)
                rrmse_list = [(e / ref) if ref > 0 else 1.0 for e in rmse_vals]
                rmse_norm = sum(1.0 / (1.0 + r) for r in rrmse_list) / len(rrmse_list)

            # Combine with weights, prefer skill and acc; use rmse as fallback
            if skill_norm is not None and acc_norm is not None:
                normalized = 0.6 * skill_norm + 0.4 * acc_norm if rmse_norm is None else 0.6 * skill_norm + 0.3 * acc_norm + 0.1 * rmse_norm
            elif skill_norm is not None:
                normalized = skill_norm
            elif acc_norm is not None:
                normalized = acc_norm
            elif rmse_norm is not None:
                normalized = rmse_norm
            else:
                normalized = 0.0
            # Clamp final per-lead normalized score to [0, 1]
            normalized_by_lead[lh] = float(clamp(float(normalized), 0.0, 1.0))

        # Push per-lead normalized writes early, as part of final scoring loop
        stats_mgr = WeatherStatsManager(task_instance.db_manager, validator_hotkey=getattr(task_instance, "validator", None) and getattr(getattr(getattr(task_instance, "validator", None), "validator_wallet", None), "hotkey", None) and getattr(getattr(getattr(task_instance, "validator", None), "validator_wallet", None).hotkey, "ss58_address", None) or "unknown_validator")

        # Compute averages from component table for avg_rmse/avg_acc/avg_skill
        avg_comp = await task_instance.db_manager.fetch_one(
            sa.text(
                """
                SELECT AVG(rmse) AS avg_rmse, AVG(acc) AS avg_acc, AVG(skill_score) AS avg_skill
                FROM weather_forecast_component_scores
                WHERE run_id = :rid AND miner_uid = :uid AND score_type = 'era5'
                """
            ),
            {"rid": run_id, "uid": miner_uid},
        )
        era5_avg_rmse = float(avg_comp["avg_rmse"]) if avg_comp and avg_comp["avg_rmse"] is not None else None
        era5_avg_acc = float(avg_comp["avg_acc"]) if avg_comp and avg_comp["avg_acc"] is not None else None
        era5_avg_skill = float(avg_comp["avg_skill"]) if avg_comp and avg_comp["avg_skill"] is not None else None

        # Aggregate normalized ERA5 with completeness penalty: divide by full expected horizon
        # This prevents partially-complete forecasts from appearing better due to fewer (shorter) leads
        expected_leads = task_instance.config.get("final_scoring_lead_hours", [24,48,72,96,120,144,168,192,216,240])
        available_leads = [h for h in expected_leads if h in normalized_by_lead]
        era5_norm_total = sum(normalized_by_lead[h] for h in available_leads)
        era5_norm_avg = (era5_norm_total / float(len(expected_leads))) if expected_leads else 0.0

        # Binary Day1 QC: day1_pass = 1 if day1_overall >= threshold else 0
        qc_threshold = float(task_instance.config.get("day1_binary_threshold", 0.1))
        day1_pass = 1.0 if (day1_overall is not None and float(day1_overall) >= qc_threshold) else 0.0

        # Blend with configured weights (defaults: 0.95 ERA5, 0.05 Day1-pass)
        W_era5 = task_instance.config.get("weather_score_era5_weight", 0.95)
        W_day1 = task_instance.config.get("weather_score_day1_weight", 0.05)
        try:
            W_era5 = float(W_era5)
            W_day1 = float(W_day1)
        except Exception:
            W_era5, W_day1 = 0.95, 0.05
        if (W_era5 + W_day1) > 0:
            norm = float(W_era5 + W_day1)
            W_era5 /= norm
            W_day1 /= norm

        overall_forecast_score = W_era5 * era5_norm_avg + W_day1 * day1_pass
        overall_forecast_score = clamp(float(overall_forecast_score), 0.0, 1.0)

        await stats_mgr.update_forecast_stats(
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            status="completed" if len(available_leads) == len(expected_leads) else "era5_scoring",
            era5_scores={h: normalized_by_lead.get(h) for h in available_leads},
            avg_rmse=era5_avg_rmse,
            avg_acc=era5_avg_acc,
            avg_skill_score=era5_avg_skill,
            overall_forecast_score=overall_forecast_score,
        )
    except Exception as _agg_err:
        logger.debug(f"[AggFinalScore] Skipped normalized aggregation write due to error: {_agg_err}")

    # Log progressive scoring details
    logger.info(
        f"[AggFinalScore] UID {miner_uid}, Run {run_id}: PROGRESSIVE SCORING - Available: {len(skill_error_component_scores)}/{total_expected_components_per_type} skill/error, {len(acc_component_scores)}/{total_expected_components_per_type} ACC components"
    )
    logger.info(
        f"[AggFinalScore] UID {miner_uid}, Run {run_id}: ProgressiveSkill/Error={avg_skill_error_score:.4f}, ProgressiveACC={avg_acc_score:.4f} => Composite Score: {final_score_val:.4f}"
    )

    agg_score_type = "era5_final_composite_score"
    metrics_for_agg_score = {
        "avg_normalized_skill_error_component": avg_skill_error_score,
        "avg_normalized_acc_component": avg_acc_score,
        "num_skill_error_components_available": len(skill_error_component_scores),
        "num_acc_components_available": len(acc_component_scores),
        "total_expected_components_per_type": total_expected_components_per_type,
        "completion_ratio_skill_error": (
            len(skill_error_component_scores) / total_expected_components_per_type
            if total_expected_components_per_type > 0
            else 0.0
        ),
        "completion_ratio_acc": (
            len(acc_component_scores) / total_expected_components_per_type
            if total_expected_components_per_type > 0
            else 0.0
        ),
        "progressive_scoring_method": "zeros_for_missing_lead_times",
    }

    calc_time = datetime.now(timezone.utc)
    db_params = {
        "response_id": response_id,
        "run_id": run_id,
        "miner_uid": miner_uid,
        "miner_hotkey": miner_hotkey,
        "score_type": agg_score_type,
        "score": final_score_val if np.isfinite(final_score_val) else 0.0,
        "metrics_json": safe_json_dumps_for_db(metrics_for_agg_score),
        "calculation_time": calc_time,
        "error_message": None,
        "lead_hours": -1,  # Use -1 for aggregated scores to fix NULL unique constraint issue
        "variable_level": "aggregated_final",
        "valid_time_utc": calc_time,  # Use calculation time for aggregated scores to ensure uniqueness
    }

    insert_agg_query = """
        INSERT INTO weather_miner_scores 
        (response_id, run_id, miner_uid, miner_hotkey, score_type, score, metrics, calculation_time, error_message, lead_hours, variable_level, valid_time_utc)
        VALUES (:response_id, :run_id, :miner_uid, :miner_hotkey, :score_type, :score, :metrics_json, :calculation_time, :error_message, :lead_hours, :variable_level, :valid_time_utc)
        ON CONFLICT (response_id, score_type, lead_hours, variable_level, valid_time_utc) DO UPDATE SET 
        score = EXCLUDED.score, metrics = EXCLUDED.metrics, calculation_time = EXCLUDED.calculation_time, error_message = EXCLUDED.error_message,
        run_id = EXCLUDED.run_id, miner_uid = EXCLUDED.miner_uid, miner_hotkey = EXCLUDED.miner_hotkey
    """

    try:
        await task_instance.db_manager.execute(insert_agg_query, db_params)
        logger.info(
            f"[AggFinalScore] Stored/Updated final composite ERA5 score for UID {miner_uid}, Run {run_id}, Type: {agg_score_type}"
        )
        return final_score_val
    except Exception as e_db:
        logger.error(
            f"[AggFinalScore] DB error storing final composite ERA5 score for UID {miner_uid}, Run {run_id}: {e_db}",
            exc_info=True,
        )
        return None


async def reconcile_job_id_for_validator(
    task_instance: "WeatherTask",
    miner_job_id: str,
    miner_hotkey: str,
    gfs_init_time: datetime,
) -> str:
    """
    Attempts to reconcile a job ID that might not match due to database synchronization.
    This is used by validators to handle cases where miners return job IDs that don't exist
    in the current validator database due to hourly overwrites.

    Args:
        task_instance: WeatherTask instance
        miner_job_id: The job ID returned by the miner
        miner_hotkey: The miner's hotkey
        gfs_init_time: The GFS initialization time for this job

    Returns:
        The reconciled job ID (either original or a found equivalent)
    """
    if task_instance.node_type != "validator":
        logger.warning("reconcile_job_id_for_validator called on non-validator node.")
        return miner_job_id

    try:
        # Check if we have a job with the same GFS time that should match this miner job
        # Generate what the job ID should be with our current deterministic system
        from gaia.tasks.base.deterministic_job_id import DeterministicJobID

        # Extract miner hotkey from the provided miner_hotkey or try to get it
        expected_job_id = DeterministicJobID.generate_weather_job_id(
            gfs_init_time=gfs_init_time,
            miner_hotkey=miner_hotkey,
            validator_hotkey=(
                task_instance.validator.hotkey
                if hasattr(task_instance, "validator") and task_instance.validator
                else "unknown"
            ),
            job_type="forecast",
        )

        if miner_job_id == expected_job_id:
            # Job ID matches what we expect, no reconciliation needed
            return miner_job_id

        # Check if the expected job ID exists in our validator database
        expected_job_query = """
        SELECT job_id FROM weather_miner_responses 
        WHERE job_id = :expected_job_id
        AND miner_hotkey = :miner_hotkey
        ORDER BY response_time DESC 
        LIMIT 1
        """

        expected_job_exists = await task_instance.db_manager.fetch_one(
            expected_job_query,
            {"expected_job_id": expected_job_id, "miner_hotkey": miner_hotkey},
        )

        if expected_job_exists:
            logger.warning(
                f"Database sync reconciliation: Miner returned job ID {miner_job_id} but we have {expected_job_id} for same GFS time. Using our expected ID."
            )
            return expected_job_id

        # Look for any job with the same GFS init time from this miner
        gfs_time_query = """
        SELECT r.job_id, f.gfs_init_time_utc
        FROM weather_miner_responses r
        JOIN weather_forecast_runs f ON r.run_id = f.id
        WHERE r.miner_hotkey = :miner_hotkey
        AND f.gfs_init_time_utc = :gfs_time
        ORDER BY r.response_time DESC
        LIMIT 1
        """

        gfs_match = await task_instance.db_manager.fetch_one(
            gfs_time_query, {"miner_hotkey": miner_hotkey, "gfs_time": gfs_init_time}
        )

        if gfs_match:
            reconciled_job_id = gfs_match["job_id"]
            logger.warning(
                f"Database sync reconciliation: Found equivalent job {reconciled_job_id} for miner job {miner_job_id} (same GFS time {gfs_init_time})"
            )
            return reconciled_job_id

        # No reconciliation possible, return original
        logger.debug(
            f"No reconciliation possible for job ID {miner_job_id}, using as-is"
        )
        return miner_job_id

    except Exception as e:
        logger.error(
            f"Error in reconcile_job_id_for_validator for job {miner_job_id}: {e}"
        )
        return miner_job_id  # Return original on error


async def check_job_id_health(task_instance: "WeatherTask") -> Dict[str, Any]:
    """
    Performs a health check on job ID consistency to identify potential database synchronization issues.
    This helps detect problems before they cause verification failures.

    Returns:
        Dict with health check results and any issues found
    """
    health_report = {
        "status": "healthy",
        "issues": [],
        "stats": {},
        "recommendations": [],
    }

    try:
        if task_instance.node_type == "miner":
            # Check for orphaned jobs (jobs that might have been created by other validators)
            orphaned_jobs_query = """
            SELECT COUNT(*) as count, 
                   COUNT(DISTINCT validator_hotkey) as validator_count,
                   MIN(validator_request_time) as oldest_request,
                   MAX(validator_request_time) as newest_request
            FROM weather_miner_jobs 
            WHERE validator_request_time >= NOW() - INTERVAL '24 hours'
            """
            orphaned_stats = await task_instance.db_manager.fetch_one(
                orphaned_jobs_query
            )

            health_report["stats"]["total_jobs_24h"] = orphaned_stats.get("count", 0)
            health_report["stats"]["unique_validators_24h"] = orphaned_stats.get(
                "validator_count", 0
            )

            if orphaned_stats.get("validator_count", 0) > 1:
                health_report["issues"].append(
                    {
                        "type": "multiple_validators",
                        "description": f"Jobs from {orphaned_stats['validator_count']} different validators in last 24h",
                        "impact": "Potential for job ID conflicts during database sync",
                    }
                )
                health_report["recommendations"].append(
                    "Enable database sync resilience features"
                )

            # Check for jobs with non-standard job ID formats
            non_standard_jobs_query = """
            SELECT COUNT(*) as count 
            FROM weather_miner_jobs 
            WHERE id NOT LIKE 'weather_job_forecast_%'
            AND validator_request_time >= NOW() - INTERVAL '24 hours'
            """
            non_standard_count = await task_instance.db_manager.fetch_one(
                non_standard_jobs_query
            )

            if non_standard_count.get("count", 0) > 0:
                health_report["issues"].append(
                    {
                        "type": "non_standard_job_ids",
                        "count": non_standard_count["count"],
                        "description": "Jobs with non-deterministic job ID format found",
                        "impact": "These jobs may not be reusable across validators",
                    }
                )
                health_report["status"] = "warning"

        elif task_instance.node_type == "validator":
            # Check for mismatched job IDs in responses
            mismatch_query = """
            SELECT COUNT(*) as count,
                   COUNT(DISTINCT miner_hotkey) as affected_miners
            FROM weather_miner_responses 
            WHERE error_message LIKE '%fallback%' OR error_message LIKE '%database sync%'
            AND response_time >= NOW() - INTERVAL '24 hours'
            """
            mismatch_stats = await task_instance.db_manager.fetch_one(mismatch_query)

            health_report["stats"]["sync_issues_24h"] = mismatch_stats.get("count", 0)
            health_report["stats"]["affected_miners_24h"] = mismatch_stats.get(
                "affected_miners", 0
            )

            if mismatch_stats.get("count", 0) > 0:
                health_report["issues"].append(
                    {
                        "type": "job_id_mismatches",
                        "count": mismatch_stats["count"],
                        "affected_miners": mismatch_stats["affected_miners"],
                        "description": "Job ID mismatches detected (database sync resilience activated)",
                        "impact": "Some jobs required fallback lookup methods",
                    }
                )
                health_report["status"] = "warning"
                health_report["recommendations"].append(
                    "Monitor database synchronization timing"
                )

        # Set overall status
        if len(health_report["issues"]) == 0:
            health_report["status"] = "healthy"
        elif any(
            issue["type"] in ["multiple_validators", "job_id_mismatches"]
            for issue in health_report["issues"]
        ):
            health_report["status"] = "warning"

    except Exception as e:
        health_report["status"] = "error"
        health_report["issues"].append(
            {
                "type": "health_check_error",
                "description": f"Error during health check: {e}",
                "impact": "Unable to assess job ID health",
            }
        )
        logger.error(f"Error in check_job_id_health: {e}")

    return health_report
