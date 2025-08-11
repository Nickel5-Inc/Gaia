"""
Integration module for weather stats tracking in the validator workflow.

This module provides integration points for the WeatherStatsManager to be called
from the existing validator weather scoring pipeline.
"""

from typing import Optional, Dict, Any
from datetime import datetime, timezone
from loguru import logger

from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager


class WeatherStatsIntegration:
    """Integration helper for weather stats tracking."""
    
    def __init__(self, database_manager: ValidatorDatabaseManager, validator_hotkey: str):
        """
        Initialize the integration helper.
        
        Args:
            database_manager: Database manager instance
            validator_hotkey: Validator's hotkey
        """
        self.stats_manager = WeatherStatsManager(database_manager, validator_hotkey)
        self.db = database_manager
    
    async def on_miner_response_received(
        self,
        run_id: int,
        miner_uid: int,
        miner_hotkey: str,
        response_data: Dict[str, Any]
    ) -> None:
        """
        Called when a miner response is received for a weather forecast request.
        
        Args:
            run_id: Weather forecast run ID
            miner_uid: Miner's UID
            miner_hotkey: Miner's hotkey
            response_data: Response data from miner including kerchunk URL, job_id, etc.
        """
        try:
            # Update stats to show we received a response
            await self.stats_manager.update_forecast_stats(
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                status="input_received"
            )
            logger.debug(f"Recorded response receipt for miner {miner_uid}")
            
        except Exception as e:
            logger.error(f"Error recording miner response: {e}")
    
    async def on_kerchunk_fetch_attempt(
        self,
        run_id: int,
        miner_uid: int,
        miner_hotkey: str,
        success: bool,
        latency_ms: Optional[int] = None,
        error_msg: Optional[str] = None
    ) -> None:
        """
        Called when attempting to fetch a miner's kerchunk file.
        
        Args:
            run_id: Weather forecast run ID
            miner_uid: Miner's UID
            miner_hotkey: Miner's hotkey
            success: Whether the fetch was successful
            latency_ms: Latency in milliseconds if successful
            error_msg: Error message if failed
        """
        try:
            hosting_status = "accessible" if success else "inaccessible"
            
            await self.stats_manager.update_forecast_stats(
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                status="input_received" if success else "failed",
                error_msg=error_msg if not success else None,
                hosting_status=hosting_status,
                hosting_latency_ms=latency_ms
            )
            
            logger.debug(f"Recorded kerchunk fetch {'success' if success else 'failure'} for miner {miner_uid}")
            
        except Exception as e:
            logger.error(f"Error recording kerchunk fetch attempt: {e}")
    
    async def on_day1_scoring_complete(
        self,
        run_id: int,
        miner_uid: int,
        miner_hotkey: str,
        gfs_score: float,
        error_msg: Optional[str] = None
    ) -> None:
        """
        Called when Day 1 GFS scoring is complete for a miner.
        
        Args:
            run_id: Weather forecast run ID
            miner_uid: Miner's UID
            miner_hotkey: Miner's hotkey
            gfs_score: GFS comparison score
            error_msg: Error message if scoring failed
        """
        try:
            status = "day1_scored" if gfs_score is not None else "failed"
            
            await self.stats_manager.update_forecast_stats(
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                status=status,
                error_msg=error_msg,
                initial_score=gfs_score
            )
            
            logger.debug(f"Recorded Day 1 score {gfs_score:.4f} for miner {miner_uid}")
            
        except Exception as e:
            logger.error(f"Error recording Day 1 scoring: {e}")
    
    async def on_era5_scoring_progress(
        self,
        run_id: int,
        miner_uid: int,
        miner_hotkey: str,
        lead_hour: int,
        score: float
    ) -> None:
        """
        Called when an ERA5 score is calculated for a specific lead time.
        
        Args:
            run_id: Weather forecast run ID
            miner_uid: Miner's UID
            miner_hotkey: Miner's hotkey
            lead_hour: Lead hour (24, 48, 72, etc.)
            score: ERA5 comparison score
        """
        try:
            # Update with single ERA5 score
            era5_scores = {lead_hour: score}
            
            await self.stats_manager.update_forecast_stats(
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                status="era5_scoring",
                era5_scores=era5_scores
            )
            
            logger.debug(f"Recorded ERA5 score {score:.4f} at {lead_hour}h for miner {miner_uid}")
            
        except Exception as e:
            logger.error(f"Error recording ERA5 scoring progress: {e}")
    
    async def on_forecast_complete(
        self,
        run_id: int,
        miner_uid: int,
        miner_hotkey: str,
        all_era5_scores: Dict[int, float],
        final_status: str = "completed"
    ) -> None:
        """
        Called when all scoring is complete for a miner's forecast.
        
        Args:
            run_id: Weather forecast run ID
            miner_uid: Miner's UID
            miner_hotkey: Miner's hotkey
            all_era5_scores: All ERA5 scores by lead hour
            final_status: Final status (completed or failed)
        """
        try:
            # Update with all ERA5 scores and final status
            await self.stats_manager.update_forecast_stats(
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                status=final_status,
                era5_scores=all_era5_scores
            )
            
            # Update consecutive streaks
            await self.stats_manager.update_consecutive_streaks(miner_uid)
            
            # Update error analytics if needed
            if final_status == "failed":
                await self.stats_manager.update_error_analytics(miner_uid)
            
            logger.info(f"Completed forecast tracking for miner {miner_uid} with status {final_status}")
            
        except Exception as e:
            logger.error(f"Error recording forecast completion: {e}")
    
    async def on_run_complete(self, run_id: int) -> None:
        """
        Called when an entire forecast run is complete for all miners.
        
        Args:
            run_id: Weather forecast run ID
        """
        try:
            # Calculate ranks for this run
            await self.stats_manager.calculate_miner_rank(run_id)
            
            # Aggregate all miner stats
            await self.stats_manager.aggregate_miner_stats()
            
            # Update overall ranks
            await self.stats_manager.get_overall_miner_ranks()
            
            logger.info(f"Completed all stats aggregation for run {run_id}")
            
        except Exception as e:
            logger.error(f"Error completing run stats: {e}")
    
    async def get_stats_for_web_server(self, since: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get formatted statistics for sending to the web server.
        
        Args:
            since: Optional datetime to get stats since
            
        Returns:
            Dictionary of statistics ready for web server
        """
        try:
            # Build query for weather_forecast_stats
            import sqlalchemy as sa
            from gaia.database.validator_schema import weather_forecast_stats_table, miner_stats_table
            
            forecast_query = sa.select(weather_forecast_stats_table)
            if since:
                forecast_query = forecast_query.where(
                    weather_forecast_stats_table.c.updated_at >= since
                )
            forecast_query = forecast_query.order_by(
                weather_forecast_stats_table.c.updated_at.desc()
            ).limit(1000)  # Limit to last 1000 records
            
            forecast_stats = await self.db.fetch_all(forecast_query)
            
            # Get miner stats
            miner_query = sa.select(miner_stats_table).order_by(
                miner_stats_table.c.miner_rank.nullslast()
            )
            miner_stats = await self.db.fetch_all(miner_query)
            
            # Format for web server
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "validator_hotkey": self.stats_manager.validator_hotkey,
                "forecast_stats": [
                    {
                        "miner_uid": row["miner_uid"],
                        "miner_hotkey": row["miner_hotkey"],
                        "miner_rank": row["miner_rank"],
                        "forecast_run_id": row["forecast_run_id"],
                        "forecast_init_time": row["forecast_init_time"].isoformat() if row["forecast_init_time"] else None,
                        "forecast_status": row["forecast_status"],
                        "forecast_error_msg": row["forecast_error_msg"],
                        "forecast_score_initial": row["forecast_score_initial"],
                        "era5_scores": {
                            h: row[f"era5_score_{h}h"] 
                            for h in range(24, 241, 24) 
                            if row.get(f"era5_score_{h}h") is not None
                        },
                        "era5_combined_score": row["era5_combined_score"],
                        "era5_completeness": row["era5_completeness"],
                        "forecast_type": row["forecast_type"],
                        "hosting_status": row["hosting_status"],
                        "hosting_latency_ms": row["hosting_latency_ms"],
                        "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None
                    }
                    for row in forecast_stats
                ],
                "miner_stats": [
                    {
                        "miner_uid": row["miner_uid"],
                        "miner_hotkey": row["miner_hotkey"],
                        "miner_rank": row["miner_rank"],
                        "avg_forecast_score": row["avg_forecast_score"],
                        "successful_forecasts": row["successful_forecasts"],
                        "failed_forecasts": row["failed_forecasts"],
                        "forecast_success_ratio": row["forecast_success_ratio"],
                        "hosting_successes": row["hosting_successes"],
                        "hosting_failures": row["hosting_failures"],
                        "host_reliability_ratio": row["host_reliability_ratio"],
                        "avg_hosting_latency_ms": row["avg_hosting_latency_ms"],
                        "avg_day1_score": row["avg_day1_score"],
                        "avg_era5_score": row["avg_era5_score"],
                        "avg_era5_completeness": row["avg_era5_completeness"],
                        "best_forecast_score": row["best_forecast_score"],
                        "worst_forecast_score": row["worst_forecast_score"],
                        "score_std_dev": row["score_std_dev"],
                        "consecutive_successes": row["consecutive_successes"],
                        "consecutive_failures": row["consecutive_failures"],
                        "common_errors": row["common_errors"],
                        "error_rate_by_type": row["error_rate_by_type"],
                        "last_active": row["last_active"].isoformat() if row["last_active"] else None
                    }
                    for row in miner_stats
                ]
            }
            
        except Exception as e:
            logger.error(f"Error getting stats for web server: {e}")
            return {}
