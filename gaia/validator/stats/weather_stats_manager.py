"""
Weather Forecast Statistics Manager

This module handles the calculation, aggregation, and storage of weather forecast
statistics for the new weather_forecast_stats and miner_stats tables.
"""

import asyncio
import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from loguru import logger
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.database.validator_schema import (
    weather_forecast_stats_table,
    miner_stats_table,
    weather_forecast_runs_table,
    weather_miner_responses_table,
    weather_miner_scores_table,
)


class WeatherStatsManager:
    """Manages weather forecast statistics and miner performance metrics."""
    
    def __init__(self, database_manager: ValidatorDatabaseManager, validator_hotkey: str):
        """
        Initialize the WeatherStatsManager.
        
        Args:
            database_manager: Database manager instance
            validator_hotkey: Validator's hotkey for tracking
        """
        self.db = database_manager
        self.validator_hotkey = validator_hotkey
        
    @staticmethod
    def generate_forecast_run_id(gfs_init_time: datetime, target_time: datetime) -> str:
        """
        Generate a deterministic forecast run ID that will be consistent across validators.
        
        Args:
            gfs_init_time: GFS initialization time
            target_time: Target forecast time
            
        Returns:
            Deterministic run ID string
        """
        # Create a deterministic ID using GFS init time and target time
        # Format: YYYYMMDD_HH_YYYYMMDD_HH
        gfs_str = gfs_init_time.strftime("%Y%m%d_%H")
        target_str = target_time.strftime("%Y%m%d_%H")
        return f"forecast_{gfs_str}_to_{target_str}"
    
    @staticmethod
    def sanitize_error_message(error_msg: str) -> Dict[str, Any]:
        """
        Sanitize error messages to remove sensitive information.
        
        Args:
            error_msg: Raw error message
            
        Returns:
            Sanitized error data as JSON
        """
        if not error_msg:
            return None
            
        # Patterns to redact
        ip_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
        port_pattern = r':(\d{4,5})\b'
        path_pattern = r'(/[\w/\-\.]+)'
        
        # Redact sensitive information
        sanitized = error_msg
        sanitized = re.sub(ip_pattern, '[IP_REDACTED]', sanitized)
        sanitized = re.sub(port_pattern, ':[PORT_REDACTED]', sanitized)
        
        # Keep only filename, not full paths
        sanitized = re.sub(path_pattern, lambda m: '/' + m.group(1).split('/')[-1], sanitized)
        
        # Categorize the error
        error_type = "unknown"
        if "timeout" in sanitized.lower():
            error_type = "timeout"
        elif "connection" in sanitized.lower():
            error_type = "connection"
        elif "404" in sanitized or "not found" in sanitized.lower():
            error_type = "not_found"
        elif "hash" in sanitized.lower() or "verification" in sanitized.lower():
            error_type = "verification_failed"
        elif "inference" in sanitized.lower():
            error_type = "inference_error"
        
        return {
            "type": error_type,
            "message": sanitized[:500],  # Limit message length
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    async def update_forecast_stats(
        self,
        run_id: int,
        miner_uid: int,
        miner_hotkey: str,
        status: str,
        error_msg: Optional[str] = None,
        initial_score: Optional[float] = None,
        era5_scores: Optional[Dict[int, float]] = None,
        hosting_status: Optional[str] = None,
        hosting_latency_ms: Optional[int] = None,
        avg_rmse: Optional[float] = None,
        avg_acc: Optional[float] = None,
        avg_skill_score: Optional[float] = None,
        overall_forecast_score: Optional[float] = None
    ) -> bool:
        """
        Update or insert weather forecast statistics for a miner.
        
        Args:
            run_id: Weather forecast run ID from weather_forecast_runs table
            miner_uid: Miner's UID
            miner_hotkey: Miner's hotkey
            status: Current status of the forecast
            error_msg: Error message if any
            initial_score: Day 1 GFS comparison score
            era5_scores: Dictionary of lead_hour -> score mappings
            hosting_status: Status of kerchunk hosting
            hosting_latency_ms: Latency for accessing hosted files
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Normalize and validate status
            allowed_statuses = {
                "pending",
                "seeded",
                "input_received",
                "day1_scoring",
                "era5_scoring",
                "completed",
                "failed",
            }
            norm_status = (status or "").strip().lower()
            if norm_status not in allowed_statuses:
                # Map common synonyms
                synonyms = {
                    "received": "input_received",
                    "in_progress": "era5_scoring",
                    "done": "completed",
                    "error": "failed",
                }
                norm_status = synonyms.get(norm_status, "failed" if error_msg else "pending")

            # Get forecast run details
            run_data = await self.db.fetch_one(
                sa.select(
                    weather_forecast_runs_table.c.gfs_init_time_utc,
                    weather_forecast_runs_table.c.target_forecast_time_utc
                ).where(weather_forecast_runs_table.c.id == run_id)
            )
            
            if not run_data:
                logger.error(f"Forecast run {run_id} not found")
                return False
            
            # Generate deterministic forecast_run_id
            forecast_run_id = self.generate_forecast_run_id(
                run_data["gfs_init_time_utc"],
                run_data["target_forecast_time_utc"]
            )
            
            # Prepare the stats record
            stats_data = {
                "miner_uid": miner_uid,
                "miner_hotkey": miner_hotkey,
                "forecast_run_id": forecast_run_id,
                "run_id": run_id,
                "forecast_init_time": run_data["gfs_init_time_utc"],
                "forecast_status": norm_status,
                "current_forecast_stage": norm_status,
                "validator_hotkey": self.validator_hotkey,
                "updated_at": datetime.now(timezone.utc)
            }
            
            # Add error message if present
            if error_msg:
                # Record sanitized JSON and a short text mirror for quick filtering
                sanitized = self.sanitize_error_message(error_msg if isinstance(error_msg, str) else json.dumps(error_msg))
                if sanitized:
                    stats_data["forecast_error_msg"] = sanitized
                    stats_data["last_error_message"] = sanitized.get("message")
            
            # Add initial score
            if initial_score is not None:
                try:
                    stats_data["forecast_score_initial"] = float(initial_score)
                except Exception:
                    pass
            
            # Add ERA5 scores (clamped 0..1) - cumulative across existing row + new inputs
            if era5_scores:
                fixed_leads = [24, 48, 72, 96, 120, 144, 168, 192, 216, 240]
                merged_scores: Dict[int, float] = {}
                try:
                    # Fetch existing per-lead values to accumulate completeness across multiple updates
                    existing_row = await self.db.fetch_one(
                        sa.select(
                            *[getattr(weather_forecast_stats_table.c, f"era5_score_{h}h") for h in fixed_leads]
                        ).where(
                            (weather_forecast_stats_table.c.miner_uid == miner_uid)
                            & (weather_forecast_stats_table.c.forecast_run_id == forecast_run_id)
                        )
                    )
                except Exception:
                    existing_row = None

                for lead_hour in fixed_leads:
                    val = None
                    # Prefer newly provided value
                    if lead_hour in era5_scores:
                        try:
                            val = float(era5_scores[lead_hour])
                        except Exception:
                            val = None
                    # Else fallback to existing stored value
                    if val is None and existing_row is not None:
                        try:
                            val = existing_row.get(f"era5_score_{lead_hour}h")
                        except Exception:
                            val = None
                    if val is not None:
                        clamped = max(0.0, min(1.0, float(val)))
                        merged_scores[lead_hour] = clamped
                        stats_data[f"era5_score_{lead_hour}h"] = clamped

                completeness_count = len(merged_scores)
                total_score = sum(merged_scores.values())
                stats_data["era5_completeness"] = (completeness_count / 10.0)
                stats_data["era5_combined_score"] = (total_score / 10.0)
            
            # Add hosting metrics
            if hosting_status:
                allowed_hosting = {"accessible", "inaccessible", "timeout", "error"}
                h = (hosting_status or "").strip().lower()
                stats_data["hosting_status"] = h if h in allowed_hosting else "error"
            if hosting_latency_ms is not None:
                try:
                    stats_data["hosting_latency_ms"] = max(0, int(hosting_latency_ms))
                except Exception:
                    pass
            
            # Add average component metrics
            if avg_rmse is not None:
                try:
                    stats_data["avg_rmse"] = float(avg_rmse)
                except Exception:
                    pass
            if avg_acc is not None:
                try:
                    stats_data["avg_acc"] = float(avg_acc)
                except Exception:
                    pass
            if avg_skill_score is not None:
                try:
                    stats_data["avg_skill_score"] = float(avg_skill_score)
                except Exception:
                    pass
            if overall_forecast_score is not None:
                # Defensive clamp overall score
                try:
                    overall_val = float(overall_forecast_score)
                except Exception:
                    overall_val = None
                if overall_val is not None:
                    stats_data["overall_forecast_score"] = max(0.0, min(1.0, overall_val))
            
            # Upsert the record
            stmt = insert(weather_forecast_stats_table).values(**stats_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=["miner_uid", "forecast_run_id"],
                set_={
                    k: v for k, v in stats_data.items() 
                    if k not in ["miner_uid", "forecast_run_id", "created_at"]
                }
            )
            
            await self.db.execute(stmt)
            logger.debug(f"Updated forecast stats for miner {miner_uid} run {forecast_run_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating forecast stats: {e}")
            return False
    
    async def calculate_miner_rank(self, run_id: int) -> Dict[int, int]:
        """
        Calculate and update miner ranks for a specific forecast run.
        
        Args:
            run_id: Weather forecast run ID
            
        Returns:
            Dictionary of miner_uid -> rank
        """
        try:
            # Get all miners' combined scores for this run
            query = sa.select(
                weather_forecast_stats_table.c.miner_uid,
                weather_forecast_stats_table.c.era5_combined_score,
                weather_forecast_stats_table.c.forecast_run_id
            ).where(
                weather_forecast_stats_table.c.run_id == run_id
            ).order_by(
                weather_forecast_stats_table.c.era5_combined_score.desc().nullslast()
            )
            
            results = await self.db.fetch_all(query)
            
            ranks = {}
            current_rank = 1
            last_score = None
            same_score_count = 0
            
            for row in results:
                miner_uid = row["miner_uid"]
                score = row["era5_combined_score"]
                
                # Handle tied scores
                if score == last_score:
                    same_score_count += 1
                else:
                    current_rank += same_score_count
                    same_score_count = 1
                    last_score = score
                
                ranks[miner_uid] = current_rank
                
                # Update the rank in the database
                await self.db.execute(
                    sa.update(weather_forecast_stats_table)
                    .where(
                        (weather_forecast_stats_table.c.miner_uid == miner_uid) &
                        (weather_forecast_stats_table.c.forecast_run_id == row["forecast_run_id"])
                    )
                    .values(miner_rank=current_rank)
                )
            
            logger.info(f"Updated ranks for {len(ranks)} miners in run {run_id}")
            return ranks
            
        except Exception as e:
            logger.error(f"Error calculating miner ranks: {e}")
            return {}
    
    async def aggregate_miner_stats(self, miner_uid: Optional[int] = None) -> bool:
        """
        Vectorized aggregation from weather_forecast_stats into miner_stats using SQL.
        """
        try:
            # Derive aggregates in one shot
            where_clause = "WHERE 1=1"
            params = {}
            if miner_uid is not None:
                where_clause = "WHERE wfs.miner_uid = :uid"
                params["uid"] = miner_uid

            # Compute per-row average ERA5 across fixed columns and completeness, then aggregate per miner
            sql = sa.text(
                f"""
                INSERT INTO miner_stats (
                    miner_uid, miner_hotkey, miner_rank,
                    avg_forecast_score, successful_forecasts, failed_forecasts, forecast_success_ratio,
                    hosting_successes, hosting_failures, host_reliability_ratio, avg_hosting_latency_ms,
                    avg_day1_score, avg_era5_score, avg_era5_completeness, best_forecast_score, worst_forecast_score, score_std_dev,
                    last_successful_forecast, last_failed_forecast, first_seen, last_active,
                    total_inference_time, avg_inference_time, error_rate_by_type,
                    validator_hotkey, updated_at
                )
                SELECT
                    wfs.miner_uid,
                    (
                      SELECT w2.miner_hotkey
                      FROM weather_forecast_stats w2
                      WHERE w2.miner_uid = wfs.miner_uid
                      ORDER BY w2.updated_at DESC NULLS LAST
                      LIMIT 1
                    ) AS miner_hotkey,
                    MAX(wfs.miner_rank) AS miner_rank,
                    AVG(wfs.era5_combined_score) AS avg_forecast_score,
                    SUM(CASE WHEN wfs.forecast_status = 'completed' THEN 1 ELSE 0 END) AS successful_forecasts,
                    SUM(CASE WHEN wfs.forecast_status = 'failed' THEN 1 ELSE 0 END) AS failed_forecasts,
                    CASE WHEN COUNT(*) > 0 THEN SUM(CASE WHEN wfs.forecast_status = 'completed' THEN 1 ELSE 0 END)::float / COUNT(*) ELSE 0 END AS forecast_success_ratio,
                    SUM(CASE WHEN wfs.hosting_status = 'accessible' THEN 1 ELSE 0 END) AS hosting_successes,
                    SUM(CASE WHEN wfs.hosting_status IN ('inaccessible','timeout','error') THEN 1 ELSE 0 END) AS hosting_failures,
                    CASE WHEN SUM(CASE WHEN wfs.hosting_status IS NOT NULL THEN 1 ELSE 0 END) > 0
                         THEN SUM(CASE WHEN wfs.hosting_status = 'accessible' THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN wfs.hosting_status IS NOT NULL THEN 1 ELSE 0 END),0)
                         ELSE 0 END AS host_reliability_ratio,
                    AVG(wfs.hosting_latency_ms) AS avg_hosting_latency_ms,
                    AVG(wfs.forecast_score_initial) AS avg_day1_score,
                    AVG((COALESCE(wfs.era5_score_24h,0)+COALESCE(wfs.era5_score_48h,0)+COALESCE(wfs.era5_score_72h,0)+COALESCE(wfs.era5_score_96h,0)+COALESCE(wfs.era5_score_120h,0)+COALESCE(wfs.era5_score_144h,0)+COALESCE(wfs.era5_score_168h,0)+COALESCE(wfs.era5_score_192h,0)+COALESCE(wfs.era5_score_216h,0)+COALESCE(wfs.era5_score_240h,0))/10.0) AS avg_era5_score,
                    AVG((CASE WHEN wfs.era5_score_24h IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN wfs.era5_score_48h IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN wfs.era5_score_72h IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN wfs.era5_score_96h IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN wfs.era5_score_120h IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN wfs.era5_score_144h IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN wfs.era5_score_168h IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN wfs.era5_score_192h IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN wfs.era5_score_216h IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN wfs.era5_score_240h IS NOT NULL THEN 1 ELSE 0 END)/10.0) AS avg_era5_completeness,
                    MAX(wfs.era5_combined_score) AS best_forecast_score,
                    MIN(wfs.era5_combined_score) AS worst_forecast_score,
                    STDDEV(wfs.era5_combined_score) AS score_std_dev,
                    MAX(CASE WHEN wfs.forecast_status = 'completed' THEN wfs.updated_at ELSE NULL END) AS last_successful_forecast,
                    MAX(CASE WHEN wfs.forecast_status = 'failed' THEN wfs.updated_at ELSE NULL END) AS last_failed_forecast,
                    MIN(wfs.created_at) AS first_seen,
                    MAX(wfs.created_at) AS last_active,
                    COALESCE(SUM(wfs.forecast_inference_duration_seconds), 0) AS total_inference_time,
                    CASE WHEN SUM(CASE WHEN wfs.forecast_inference_duration_seconds IS NOT NULL THEN 1 ELSE 0 END) > 0 
                         THEN AVG(wfs.forecast_inference_duration_seconds) 
                         ELSE NULL END AS avg_inference_time,
                    CASE WHEN SUM(CASE WHEN wfs.forecast_status = 'failed' THEN 1 ELSE 0 END) > 0
                         THEN jsonb_build_object(
                             'network_error', SUM(CASE WHEN wfs.last_error_message LIKE '%network%' OR wfs.last_error_message LIKE '%timeout%' THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN wfs.forecast_status = 'failed' THEN 1 ELSE 0 END), 0),
                             'verification_error', SUM(CASE WHEN wfs.last_error_message LIKE '%verification%' OR wfs.last_error_message LIKE '%manifest%' THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN wfs.forecast_status = 'failed' THEN 1 ELSE 0 END), 0),
                             'inference_error', SUM(CASE WHEN wfs.last_error_message LIKE '%inference%' OR wfs.last_error_message LIKE '%processing%' THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN wfs.forecast_status = 'failed' THEN 1 ELSE 0 END), 0),
                             'other_error', SUM(CASE WHEN wfs.last_error_message NOT LIKE '%network%' AND wfs.last_error_message NOT LIKE '%timeout%' AND wfs.last_error_message NOT LIKE '%verification%' AND wfs.last_error_message NOT LIKE '%manifest%' AND wfs.last_error_message NOT LIKE '%inference%' AND wfs.last_error_message NOT LIKE '%processing%' THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN wfs.forecast_status = 'failed' THEN 1 ELSE 0 END), 0)
                         )
                         ELSE NULL END AS error_rate_by_type,
                    :vhk AS validator_hotkey,
                    NOW() AT TIME ZONE 'UTC' AS updated_at
                FROM weather_forecast_stats wfs
                {where_clause}
                GROUP BY wfs.miner_uid
                ON CONFLICT (miner_uid) DO UPDATE SET
                    miner_hotkey = EXCLUDED.miner_hotkey,
                    miner_rank = EXCLUDED.miner_rank,
                    avg_forecast_score = EXCLUDED.avg_forecast_score,
                    successful_forecasts = EXCLUDED.successful_forecasts,
                    failed_forecasts = EXCLUDED.failed_forecasts,
                    forecast_success_ratio = EXCLUDED.forecast_success_ratio,
                    hosting_successes = EXCLUDED.hosting_successes,
                    hosting_failures = EXCLUDED.hosting_failures,
                    host_reliability_ratio = EXCLUDED.host_reliability_ratio,
                    avg_hosting_latency_ms = EXCLUDED.avg_hosting_latency_ms,
                    avg_day1_score = EXCLUDED.avg_day1_score,
                    avg_era5_score = EXCLUDED.avg_era5_score,
                    avg_era5_completeness = EXCLUDED.avg_era5_completeness,
                    best_forecast_score = EXCLUDED.best_forecast_score,
                    worst_forecast_score = EXCLUDED.worst_forecast_score,
                    score_std_dev = EXCLUDED.score_std_dev,
                    last_successful_forecast = EXCLUDED.last_successful_forecast,
                    last_failed_forecast = EXCLUDED.last_failed_forecast,
                    first_seen = EXCLUDED.first_seen,
                    last_active = EXCLUDED.last_active,
                    total_inference_time = EXCLUDED.total_inference_time,
                    avg_inference_time = EXCLUDED.avg_inference_time,
                    error_rate_by_type = EXCLUDED.error_rate_by_type,
                    validator_hotkey = EXCLUDED.validator_hotkey,
                    updated_at = EXCLUDED.updated_at
                """.replace("{where_clause}", where_clause)
            )

            params["vhk"] = self.validator_hotkey
            await self.db.execute(sql, params)
            logger.info("Aggregated miner_stats via SQL")
            return True
        except Exception as e:
            logger.error(f"Error aggregating miner stats (SQL): {e}")
            return False

    async def recompute_era5_rollups_for_run(self, run_id: int) -> bool:
        """Recompute era5_combined_score and era5_completeness from fixed columns for a run."""
        try:
            sql = sa.text(
                """
                UPDATE weather_forecast_stats SET
                  era5_combined_score = (
                    COALESCE(era5_score_24h,0)+COALESCE(era5_score_48h,0)+COALESCE(era5_score_72h,0)+COALESCE(era5_score_96h,0)+
                    COALESCE(era5_score_120h,0)+COALESCE(era5_score_144h,0)+COALESCE(era5_score_168h,0)+COALESCE(era5_score_192h,0)+
                    COALESCE(era5_score_216h,0)+COALESCE(era5_score_240h,0)
                  )/10.0,
                  era5_completeness = (
                    (CASE WHEN era5_score_24h IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN era5_score_48h IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN era5_score_72h IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN era5_score_96h IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN era5_score_120h IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN era5_score_144h IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN era5_score_168h IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN era5_score_192h IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN era5_score_216h IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN era5_score_240h IS NOT NULL THEN 1 ELSE 0 END)
                  )/10.0
                WHERE run_id = :rid
                """
            )
            await self.db.execute(sql, {"rid": run_id})
            return True
        except Exception as e:
            logger.error(f"Error recomputing ERA5 rollups for run {run_id}: {e}")
            return False

    async def update_miner_ranks_for_run(self, run_id: int) -> bool:
        """Update miner_rank for a run using dense_rank over era5_combined_score."""
        try:
            sql = sa.text(
                """
                WITH ranked AS (
                  SELECT id, DENSE_RANK() OVER (ORDER BY era5_combined_score DESC NULLS LAST) AS rnk
                  FROM weather_forecast_stats
                  WHERE run_id = :rid
                )
                UPDATE weather_forecast_stats w
                SET miner_rank = ranked.rnk
                FROM ranked
                WHERE w.id = ranked.id
                """
            )
            await self.db.execute(sql, {"rid": run_id})
            return True
        except Exception as e:
            logger.error(f"Error updating miner ranks for run {run_id}: {e}")
            return False
    
    async def update_consecutive_streaks(self, miner_uid: int) -> None:
        """
        Update consecutive success/failure streaks for a miner.
        
        Args:
            miner_uid: Miner's UID
        """
        try:
            # Get recent forecast results ordered by time
            query = sa.select(
                weather_forecast_stats_table.c.forecast_status,
                weather_forecast_stats_table.c.updated_at
            ).where(
                weather_forecast_stats_table.c.miner_uid == miner_uid
            ).order_by(
                weather_forecast_stats_table.c.updated_at.desc()
            ).limit(100)  # Look at last 100 forecasts
            
            results = await self.db.fetch_all(query)
            
            if not results:
                return
            
            # Calculate streaks
            consecutive_successes = 0
            consecutive_failures = 0
            
            for i, row in enumerate(results):
                if i == 0:  # Most recent forecast
                    if row["forecast_status"] == "completed":
                        consecutive_successes = 1
                    elif row["forecast_status"] == "failed":
                        consecutive_failures = 1
                    else:
                        break  # Not completed or failed yet
                else:
                    if consecutive_successes > 0:
                        if row["forecast_status"] == "completed":
                            consecutive_successes += 1
                        else:
                            break
                    elif consecutive_failures > 0:
                        if row["forecast_status"] == "failed":
                            consecutive_failures += 1
                        else:
                            break
            
            # Update miner_stats
            await self.db.execute(
                sa.update(miner_stats_table)
                .where(miner_stats_table.c.miner_uid == miner_uid)
                .values(
                    consecutive_successes=consecutive_successes,
                    consecutive_failures=consecutive_failures
                )
            )
            
        except Exception as e:
            logger.error(f"Error updating consecutive streaks for miner {miner_uid}: {e}")
    
    async def update_error_analytics(self, miner_uid: int) -> None:
        """
        Analyze and update error patterns for a miner.
        
        Args:
            miner_uid: Miner's UID
        """
        try:
            # Get all error messages for this miner
            query = sa.select(
                weather_forecast_stats_table.c.forecast_error_msg,
                weather_forecast_stats_table.c.forecast_type
            ).where(
                (weather_forecast_stats_table.c.miner_uid == miner_uid) &
                (weather_forecast_stats_table.c.forecast_error_msg.isnot(None))
            )
            
            results = await self.db.fetch_all(query)
            
            if not results:
                return
            
            # Analyze error patterns
            error_counts = {}
            error_by_type = {}
            
            for row in results:
                error_data = row["forecast_error_msg"]
                forecast_type = row["forecast_type"]
                
                if isinstance(error_data, dict):
                    error_type = error_data.get("type", "unknown")
                    error_counts[error_type] = error_counts.get(error_type, 0) + 1
                    
                    if forecast_type not in error_by_type:
                        error_by_type[forecast_type] = {}
                    error_by_type[forecast_type][error_type] = error_by_type[forecast_type].get(error_type, 0) + 1
            
            # Get top 5 most common errors
            common_errors = dict(sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:5])
            
            # Calculate error rates by forecast type
            total_by_type_query = sa.select(
                weather_forecast_stats_table.c.forecast_type,
                sa.func.count().label("total")
            ).where(
                weather_forecast_stats_table.c.miner_uid == miner_uid
            ).group_by(
                weather_forecast_stats_table.c.forecast_type
            )
            
            totals = await self.db.fetch_all(total_by_type_query)
            
            error_rates = {}
            for row in totals:
                forecast_type = row["forecast_type"]
                total = row["total"]
                error_count = sum(error_by_type.get(forecast_type, {}).values())
                error_rates[forecast_type] = error_count / total if total > 0 else 0
            
            # Update miner_stats
            await self.db.execute(
                sa.update(miner_stats_table)
                .where(miner_stats_table.c.miner_uid == miner_uid)
                .values(
                    common_errors=common_errors,
                    error_rate_by_type=error_rates
                )
            )
            
        except Exception as e:
            logger.error(f"Error updating error analytics for miner {miner_uid}: {e}")
    
    async def get_overall_miner_ranks(self) -> Dict[int, int]:
        """
        Calculate overall miner ranks based on average forecast scores.
        
        Returns:
            Dictionary of miner_uid -> overall_rank
        """
        try:
            query = sa.select(
                miner_stats_table.c.miner_uid,
                miner_stats_table.c.avg_forecast_score
            ).order_by(
                miner_stats_table.c.avg_forecast_score.desc().nullslast()
            )
            
            results = await self.db.fetch_all(query)
            
            ranks = {}
            for i, row in enumerate(results, 1):
                ranks[row["miner_uid"]] = i
                
                # Update the rank in the database
                await self.db.execute(
                    sa.update(miner_stats_table)
                    .where(miner_stats_table.c.miner_uid == row["miner_uid"])
                    .values(miner_rank=i)
                )
            
            logger.info(f"Updated overall ranks for {len(ranks)} miners")
            return ranks
            
        except Exception as e:
            logger.error(f"Error calculating overall miner ranks: {e}")
            return {}
    
    async def update_pipeline_status(
        self,
        run_id: int,
        miner_uid: int,
        stage: str,
        status: str,
        error: Optional[str] = None,
        retry_info: Optional[Dict] = None
    ) -> bool:
        """
        Update detailed pipeline tracking columns.
        
        Args:
            run_id: Weather forecast run ID
            miner_uid: Miner's UID
            stage: Current pipeline stage
            status: Status within the stage
            error: Error message if any
            retry_info: Dictionary with retries_remaining and next_retry_time
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get forecast run details to generate forecast_run_id
            run_data = await self.db.fetch_one(
                sa.select(
                    weather_forecast_runs_table.c.gfs_init_time_utc,
                    weather_forecast_runs_table.c.target_forecast_time_utc
                ).where(weather_forecast_runs_table.c.id == run_id)
            )
            
            if not run_data:
                logger.error(f"Forecast run {run_id} not found")
                return False
            
            forecast_run_id = self.generate_forecast_run_id(
                run_data["gfs_init_time_utc"],
                run_data["target_forecast_time_utc"]
            )
            
            # Normalize stage/status
            stage_norm = (stage or "").strip().lower()
            status_norm = (status or "").strip().lower()
            update_data = {
                "current_forecast_stage": stage_norm,
                "current_forecast_status": status_norm,
                "updated_at": datetime.now(timezone.utc)
            }
            
            if error:
                # Store short text and sanitized JSON
                msg = error if isinstance(error, str) else json.dumps(error)
                update_data["last_error_message"] = msg[:500]
                sanitized = self.sanitize_error_message(msg)
                if sanitized:
                    update_data["forecast_error_msg"] = sanitized
            
            if retry_info:
                update_data["retries_remaining"] = retry_info.get("retries_remaining")
                update_data["next_scheduled_retry"] = retry_info.get("next_retry_time")
            
            stmt = sa.update(weather_forecast_stats_table).where(
                sa.and_(
                    weather_forecast_stats_table.c.miner_uid == miner_uid,
                    weather_forecast_stats_table.c.forecast_run_id == forecast_run_id
                )
            ).values(**update_data)
            
            await self.db.execute(stmt)
            logger.debug(f"Updated pipeline status for miner {miner_uid}: {stage}/{status}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating pipeline status: {e}")
            return False
    
    async def record_component_scores(
        self,
        run_id: int,
        response_id: int,
        miner_uid: int,
        miner_hotkey: str,
        score_type: str,
        lead_hours: int,
        valid_time: datetime,
        variable_scores: Dict[str, Dict]
    ) -> bool:
        """
        Batch insert component scores for a scoring operation.
        
        Args:
            run_id: Weather forecast run ID
            response_id: Miner response ID
            miner_uid: Miner's UID
            miner_hotkey: Miner's hotkey
            score_type: Type of scoring ('day1' or 'era5')
            lead_hours: Forecast lead time in hours
            valid_time: Valid time for the forecast
            variable_scores: Dictionary of variable -> metrics
            
        Returns:
            True if successful, False otherwise
        """
        try:
            from gaia.database.validator_schema import weather_forecast_component_scores_table
            
            rows = []
            for variable, metrics in variable_scores.items():
                row = {
                    "run_id": run_id,
                    "response_id": response_id,
                    "miner_uid": miner_uid,
                    "miner_hotkey": miner_hotkey,
                    "score_type": score_type,
                    "lead_hours": lead_hours,
                    "valid_time_utc": valid_time,
                    "variable_name": variable,
                    "pressure_level": metrics.get("pressure_level"),
                    "rmse": metrics.get("rmse"),
                    "mse": metrics.get("mse"),
                    "acc": metrics.get("acc"),
                    "skill_score": metrics.get("skill_score"),
                    "skill_score_gfs": metrics.get("skill_score_gfs"),
                    "skill_score_climatology": metrics.get("skill_score_climatology"),
                    "bias": metrics.get("bias"),
                    "mae": metrics.get("mae"),
                    "climatology_check_passed": metrics.get("climatology_check_passed"),
                    "pattern_correlation": metrics.get("pattern_correlation"),
                    "pattern_correlation_passed": metrics.get("pattern_correlation_passed"),
                    "clone_penalty": metrics.get("clone_penalty", 0),
                    "quality_penalty": metrics.get("quality_penalty", 0),
                    "weighted_score": metrics.get("weighted_score"),
                    "variable_weight": metrics.get("weight"),
                    "calculated_at": datetime.now(timezone.utc),
                    "calculation_duration_ms": metrics.get("calculation_duration_ms")
                }
                rows.append(row)
            
            if rows:
                # Use on_conflict_do_update for idempotency
                for row in rows:
                    stmt = insert(weather_forecast_component_scores_table).values(**row)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[
                            "response_id", "score_type", "lead_hours", 
                            "variable_name", "pressure_level"
                        ],
                        set_={
                            k: v for k, v in row.items()
                            if k not in ["response_id", "score_type", "lead_hours", 
                                        "variable_name", "pressure_level"]
                        }
                    )
                    await self.db.execute(stmt)
                
                logger.debug(f"Recorded {len(rows)} component scores for miner {miner_uid}")
            
            # Calculate and update averages in forecast stats
            if rows:
                avg_rmse = sum(r["rmse"] for r in rows if r["rmse"] is not None) / len([r for r in rows if r["rmse"] is not None]) if any(r["rmse"] is not None for r in rows) else None
                avg_acc = sum(r["acc"] for r in rows if r["acc"] is not None) / len([r for r in rows if r["acc"] is not None]) if any(r["acc"] is not None for r in rows) else None
                avg_skill = sum(r["skill_score"] for r in rows if r["skill_score"] is not None) / len([r for r in rows if r["skill_score"] is not None]) if any(r["skill_score"] is not None for r in rows) else None
                
                # Update forecast stats with averages
                await self.update_forecast_stats(
                    run_id=run_id,
                    miner_uid=miner_uid,
                    miner_hotkey=miner_hotkey,
                    status=f"{score_type}_scored" if score_type == "day1" else "era5_scoring",
                    avg_rmse=avg_rmse,
                    avg_acc=avg_acc,
                    avg_skill_score=avg_skill
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error recording component scores: {e}")
            return False
