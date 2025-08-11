"""
Lightweight hooks for integrating weather stats tracking into the existing validator workflow.

This module provides minimal-overhead integration points that piggyback on existing
database operations to avoid duplicate calculations and queries.
"""

import time
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from loguru import logger
import sqlalchemy as sa

from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from gaia.database.validator_schema import (
    weather_forecast_stats_table,
    weather_miner_scores_table,
    weather_miner_responses_table,
)


class WeatherStatsHooks:
    """Efficient hooks for weather stats tracking that avoid duplicate work."""
    
    def __init__(self, database_manager, validator_hotkey: str):
        """
        Initialize the stats hooks.
        
        Args:
            database_manager: Database manager instance
            validator_hotkey: Validator's hotkey
        """
        self.db = database_manager
        self.stats_manager = WeatherStatsManager(database_manager, validator_hotkey)
        self.validator_hotkey = validator_hotkey
        # Cache to avoid duplicate updates
        self._processed_responses = set()
        self._processed_scores = {}  # run_id -> {miner_uid -> {scores}}
    
    async def on_response_inserted(
        self,
        run_id: int,
        miner_uid: int,
        miner_hotkey: str,
        job_id: Optional[str] = None,
        status: str = "received"
    ) -> None:
        """
        Hook to call right after inserting/updating weather_miner_responses.
        This avoids a separate query to get the same data.
        
        Args:
            run_id: Weather forecast run ID
            miner_uid: Miner's UID
            miner_hotkey: Miner's hotkey
            job_id: Job ID from response
            status: Response status
        """
        try:
            # Skip if we've already processed this
            cache_key = f"{run_id}_{miner_uid}_response"
            if cache_key in self._processed_responses:
                return
            self._processed_responses.add(cache_key)
            
            # Light update - just mark as received
            await self.stats_manager.update_forecast_stats(
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                status="input_received" if job_id else "failed",
                error_msg=None if job_id else "No job_id in response"
            )
            
        except Exception as e:
            logger.debug(f"Stats hook error (non-critical): {e}")
    
    async def on_verification_complete(
        self,
        run_id: int,
        response_id: int,
        miner_uid: int,
        miner_hotkey: str,
        verification_passed: bool,
        error_msg: Optional[str] = None,
        fetch_latency_ms: Optional[int] = None
    ) -> None:
        """
        Hook to call after verification is complete and database is updated.
        
        Args:
            run_id: Weather forecast run ID
            response_id: Response ID
            miner_uid: Miner's UID
            miner_hotkey: Miner's hotkey
            verification_passed: Whether verification passed
            error_msg: Error message if failed
            fetch_latency_ms: Latency for fetching kerchunk (if measured)
        """
        try:
            hosting_status = "accessible" if verification_passed else "inaccessible"
            status = "verified" if verification_passed else "verification_failed"
            
            await self.stats_manager.update_forecast_stats(
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                status=status,
                error_msg=error_msg,
                hosting_status=hosting_status,
                hosting_latency_ms=fetch_latency_ms
            )
            
        except Exception as e:
            logger.debug(f"Stats hook error (non-critical): {e}")
    
    async def on_scores_inserted(
        self,
        run_id: int,
        scores_batch: List[Dict[str, Any]]
    ) -> None:
        """
        Hook to call after inserting scores into weather_miner_scores.
        Processes a batch of scores efficiently.
        
        Args:
            run_id: Weather forecast run ID
            scores_batch: List of score records that were inserted
        """
        try:
            # Group scores by miner
            miner_scores = {}
            for score_record in scores_batch:
                miner_uid = score_record.get("miner_uid")
                if not miner_uid:
                    continue
                    
                if miner_uid not in miner_scores:
                    miner_scores[miner_uid] = {
                        "miner_hotkey": score_record.get("miner_hotkey"),
                        "day1_score": None,
                        "era5_scores": {}
                    }
                
                score_type = score_record.get("score_type", "")
                score_value = score_record.get("score")
                
                if score_type == "gfs_rmse" and score_value is not None:
                    # Day 1 GFS score
                    miner_scores[miner_uid]["day1_score"] = score_value
                elif score_type == "era5_rmse" and score_value is not None:
                    # ERA5 score - extract lead hour
                    lead_hours = score_record.get("lead_hours")
                    if lead_hours:
                        miner_scores[miner_uid]["era5_scores"][lead_hours] = score_value
            
            # Update stats for each miner
            for miner_uid, scores in miner_scores.items():
                # Check cache to avoid duplicate updates
                cache_key = f"{run_id}_{miner_uid}"
                if cache_key not in self._processed_scores:
                    self._processed_scores[cache_key] = {"day1": False, "era5": {}}
                
                # Update Day 1 score if new
                if scores["day1_score"] is not None and not self._processed_scores[cache_key]["day1"]:
                    await self.stats_manager.update_forecast_stats(
                        run_id=run_id,
                        miner_uid=miner_uid,
                        miner_hotkey=scores["miner_hotkey"],
                        status="day1_scored",
                        initial_score=scores["day1_score"]
                    )
                    self._processed_scores[cache_key]["day1"] = True
                
                # Update ERA5 scores
                if scores["era5_scores"]:
                    # Filter out already processed scores
                    new_scores = {
                        h: s for h, s in scores["era5_scores"].items()
                        if h not in self._processed_scores[cache_key]["era5"]
                    }
                    
                    if new_scores:
                        # Determine status based on number of scores
                        total_era5_hours = len(self._processed_scores[cache_key]["era5"]) + len(new_scores)
                        status = "completed" if total_era5_hours >= 10 else "era5_scoring"
                        
                        await self.stats_manager.update_forecast_stats(
                            run_id=run_id,
                            miner_uid=miner_uid,
                            miner_hotkey=scores["miner_hotkey"],
                            status=status,
                            era5_scores=new_scores
                        )
                        
                        # Update cache
                        for h in new_scores:
                            self._processed_scores[cache_key]["era5"][h] = True
                        
                        # If completed, update streaks
                        if status == "completed":
                            await self.stats_manager.update_consecutive_streaks(miner_uid)
            
        except Exception as e:
            logger.debug(f"Stats hook error (non-critical): {e}")
    
    async def on_run_scoring_complete(self, run_id: int) -> None:
        """
        Hook to call when all scoring for a run is complete.
        
        Args:
            run_id: Weather forecast run ID
        """
        try:
            # Mark any miners without complete scores as failed
            incomplete_query = sa.select(
                weather_forecast_stats_table.c.miner_uid,
                weather_forecast_stats_table.c.miner_hotkey
            ).where(
                (weather_forecast_stats_table.c.run_id == run_id) &
                (weather_forecast_stats_table.c.forecast_status.notin_(['completed', 'failed']))
            )
            
            incomplete_miners = await self.db.fetch_all(incomplete_query)
            
            for miner in incomplete_miners:
                await self.stats_manager.update_forecast_stats(
                    run_id=run_id,
                    miner_uid=miner["miner_uid"],
                    miner_hotkey=miner["miner_hotkey"],
                    status="failed",
                    error_msg={"type": "incomplete", "message": "Scoring incomplete"}
                )
                await self.stats_manager.update_error_analytics(miner["miner_uid"])
            
            # Calculate ranks for this run
            await self.stats_manager.calculate_miner_rank(run_id)
            
            # Aggregate all miner stats
            await self.stats_manager.aggregate_miner_stats()
            
            # Update overall ranks
            await self.stats_manager.get_overall_miner_ranks()
            
            # Clear cache for this run to free memory
            keys_to_remove = [k for k in self._processed_scores if k.startswith(f"{run_id}_")]
            for key in keys_to_remove:
                del self._processed_scores[key]
            
            # Clear old response cache entries (keep last 1000)
            if len(self._processed_responses) > 1000:
                self._processed_responses = set(list(self._processed_responses)[-500:])
            
            logger.info(f"Completed stats aggregation for run {run_id}")
            
        except Exception as e:
            logger.error(f"Error in run completion stats: {e}")
    
    async def extract_scores_from_existing_data(
        self,
        run_id: int,
        limit: int = 1000
    ) -> None:
        """
        Extract and populate stats from existing weather_miner_scores data.
        Useful for backfilling or recovery.
        
        Args:
            run_id: Weather forecast run ID
            limit: Maximum number of records to process
        """
        try:
            # Get existing scores that haven't been processed into stats
            query = """
                SELECT DISTINCT ON (wms.miner_uid, wms.score_type, wms.lead_hours)
                    wms.miner_uid,
                    wms.miner_hotkey,
                    wms.score_type,
                    wms.score,
                    wms.lead_hours,
                    wmr.verification_passed,
                    wmr.status as response_status,
                    wmr.error_message
                FROM weather_miner_scores wms
                JOIN weather_miner_responses wmr ON wmr.id = wms.response_id
                WHERE wms.run_id = :run_id
                ORDER BY wms.miner_uid, wms.score_type, wms.lead_hours, wms.calculation_time DESC
                LIMIT :limit
            """
            
            scores = await self.db.fetch_all(query, {"run_id": run_id, "limit": limit})
            
            if scores:
                # Process as a batch
                await self.on_scores_inserted(run_id, [dict(s) for s in scores])
                logger.info(f"Extracted {len(scores)} scores for run {run_id} into stats")
            
        except Exception as e:
            logger.error(f"Error extracting scores: {e}")
    
    def clear_cache(self) -> None:
        """Clear all cached data to free memory."""
        self._processed_responses.clear()
        self._processed_scores.clear()
        logger.debug("Cleared stats hooks cache")
