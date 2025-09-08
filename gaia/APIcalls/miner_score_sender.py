import asyncio
import json
import math
import pprint
import traceback
from typing import Dict, Optional

import httpx
from sqlalchemy import text

from gaia.APIcalls.website_api import GaiaCommunicator
from gaia.utils.custom_logger import get_logger
from gaia.validator.database.validator_database_manager import \
    ValidatorDatabaseManager

logger = get_logger(__name__)


class MinerScoreSender:
    def __init__(self, database_manager, api_client=None):
        """
        Initialize the MinerScoreSender.

        Args:
            database_manager: Database manager instance
            api_client: Optional httpx.AsyncClient for API communication
        """
        self.db_manager = database_manager
        self.api_client = api_client
        self._external_api_client = api_client is not None

    async def fetch_active_miners(self) -> list:
        """
        Fetch all active miners with their hotkeys, coldkeys, and UIDs.
        """
        query = "SELECT uid, hotkey, coldkey FROM node_table WHERE hotkey IS NOT NULL"
        results = await self.db_manager.fetch_all(query)
        return [
            {"uid": row["uid"], "hotkey": row["hotkey"], "coldkey": row["coldkey"]}
            for row in results
        ]

    # Geomagnetic and soil moisture processing removed (tasks disabled)
    
    async def fetch_weather_stats(self, miner_uid: int, miner_hotkey: str) -> dict:
        """
        Fetch weather forecast statistics from the new stats tables.
        """
        try:
            # Get aggregated stats from miner_stats table
            stats_query = """
                SELECT 
                    miner_rank,
                    avg_forecast_score,
                    successful_forecasts,
                    failed_forecasts,
                    forecast_success_ratio,
                    host_reliability_ratio,
                    avg_day1_score,
                    avg_era5_score,
                    avg_era5_completeness,
                    consecutive_successes,
                    consecutive_failures
                FROM miner_stats
                WHERE miner_uid = :uid
            """
            stats = await self.db_manager.fetch_one(stats_query, {"uid": miner_uid})
            
            # Get recent forecast runs from weather_forecast_stats
            recent_query = """
                SELECT 
                    forecast_run_id,
                    forecast_init_time,
                    forecast_status,
                    forecast_score_initial,
                    era5_combined_score,
                    era5_completeness,
                    hosting_status,
                    hosting_latency_ms,
                    updated_at
                FROM weather_forecast_stats
                WHERE miner_uid = :uid
                ORDER BY updated_at DESC
                LIMIT 5
            """
            recent_runs = await self.db_manager.fetch_all(recent_query, {"uid": miner_uid})
            
            # Format the data for API
            return {
                "stats": stats if stats else {},
                "recent_runs": [
                    {
                        "run_id": r["forecast_run_id"],
                        "init_time": r["forecast_init_time"].isoformat() if r["forecast_init_time"] else None,
                        "status": r["forecast_status"],
                        "day1_score": r["forecast_score_initial"],
                        "era5_score": r["era5_combined_score"],
                        "completeness": r["era5_completeness"],
                        "hosting": r["hosting_status"],
                        "latency_ms": r["hosting_latency_ms"]
                    }
                    for r in recent_runs
                ] if recent_runs else []
            }
        except Exception as e:
            logger.error(f"Error fetching weather stats for miner {miner_uid}: {e}")
            return {"stats": {}, "recent_runs": []}

    async def send_to_gaia(self):
        try:
            if not self.api_client or self.api_client.is_closed:
                self.api_client = httpx.AsyncClient(
                    timeout=60.0,
                    follow_redirects=True,
                    limits=httpx.Limits(
                        max_connections=100,
                        max_keepalive_connections=20,
                        keepalive_expiry=30,
                    ),
                    transport=httpx.AsyncHTTPTransport(retries=3),
                )
                logger.warning("| MinerScoreSender | Re-created closed API client")

            async with GaiaCommunicator(
                "/Predictions", client=self.api_client
            ) as gaia_communicator:
                active_miners = await self.fetch_active_miners()
                if not active_miners:
                    logger.warning("| MinerScoreSender | No active miners found.")
                    return

                # Semaphore to limit concurrent miner data processing (DB heavy)
                miner_processing_semaphore = asyncio.Semaphore(5)

                async def process_miner_with_semaphore(miner):
                    async with miner_processing_semaphore:
                        try:
                            logger.debug(
                                f"Processing miner {miner['hotkey']} under semaphore"
                            )
                            # Fetch weather stats from new tables
                            weather_data = await self.fetch_weather_stats(
                                miner["uid"], miner["hotkey"]
                            )
                            logger.debug(f"Fetched weather stats for miner {miner['hotkey']}")
                            
                            # Format payload for API
                            payload = {
                                "minerHotKey": miner["hotkey"],
                                "minerColdKey": miner["coldkey"],
                                "minerUID": miner["uid"],
                                "geomagneticPredictions": [],  # Geomagnetic task disabled
                                "soilMoisturePredictions": [],  # Soil moisture task disabled
                                "weatherStats": weather_data["stats"],
                                "weatherRecentRuns": weather_data["recent_runs"]
                            }
                            return payload
                        except Exception as e:
                            logger.error(
                                f"Error processing miner {miner['hotkey']} under semaphore: {str(e)}\n{traceback.format_exc()}"
                            )
                            return None

                # Process miners in smaller batches to reduce memory usage
                batch_size = 50  # Process 50 miners at a time
                successful_sends = 0
                failed_sends = 0

                logger.info(
                    f"Starting to process data for {len(active_miners)} active miners in batches of {batch_size}."
                )

                for batch_start in range(0, len(active_miners), batch_size):
                    batch_end = min(batch_start + batch_size, len(active_miners))
                    batch_miners = active_miners[batch_start:batch_end]

                    logger.info(
                        f"Processing miner batch {batch_start//batch_size + 1}: miners {batch_start+1}-{batch_end}"
                    )

                    # Process this batch
                    batch_tasks = [
                        process_miner_with_semaphore(miner) for miner in batch_miners
                    ]
                    batch_payloads = await asyncio.gather(
                        *batch_tasks, return_exceptions=True
                    )

                    valid_batch_payloads = [
                        p
                        for p in batch_payloads
                        if p is not None and not isinstance(p, Exception)
                    ]
                    logger.info(
                        f"Batch {batch_start//batch_size + 1}: Successfully processed {len(valid_batch_payloads)} miners."
                    )

                    # Send batch data immediately and clean up
                    for i, payload in enumerate(valid_batch_payloads):
                        try:
                            logger.debug(
                                f"Sending data for miner {payload['minerHotKey']} ({i+1}/{len(valid_batch_payloads)})"
                            )
                            await asyncio.wait_for(
                                gaia_communicator.send_data(data=payload),
                                timeout=30,  # Timeout for individual API call
                            )
                            logger.debug(
                                f"Successfully sent data for miner {payload['minerHotKey']}"
                            )
                            successful_sends += 1
                        except asyncio.TimeoutError:
                            logger.error(
                                f"Timeout sending API data for miner {payload['minerHotKey']}"
                            )
                            failed_sends += 1
                        except Exception as e:
                            logger.error(
                                f"Error sending API data for miner {payload['minerHotKey']}: {str(e)}\n{traceback.format_exc()}"
                            )
                            failed_sends += 1
                        finally:
                            await asyncio.sleep(
                                0.2
                            )  # Add a small delay to avoid rate limiting

                    # IMMEDIATE cleanup of batch data
                    try:
                        del batch_tasks
                        del batch_payloads
                        del valid_batch_payloads
                        # Force GC every few batches for large miner sets
                        if (batch_start // batch_size + 1) % 3 == 0:
                            import gc

                            collected = gc.collect()
                            logger.debug(
                                f"Batch cleanup: GC collected {collected} objects"
                            )
                    except Exception as cleanup_err:
                        logger.debug(f"Error during batch cleanup: {cleanup_err}")

                    # Brief pause between batches
                    if batch_end < len(active_miners):
                        await asyncio.sleep(1.0)

                logger.info(
                    f"Completed sending data to Gaia API. Successful: {successful_sends}, Failed: {failed_sends}"
                )

        except Exception as e:
            logger.error(f"Error in send_to_gaia: {str(e)}\n{traceback.format_exc()}")
            raise

    async def run_async(self):
        # Add 1-hour startup delay to prevent immediate execution on validator startup
        logger.info(
            "| MinerScoreSender | Delaying initial execution by 1 hour to avoid startup rush..."
        )
        await asyncio.sleep(3600)  # 1 hour delay

        while True:
            try:
                logger.info(
                    "| MinerScoreSender | Starting hourly process to send scores to Gaia API..."
                )
                await asyncio.wait_for(self.send_to_gaia(), timeout=2700)
                logger.info(
                    "| MinerScoreSender | Completed sending scores. Sleeping for 1 hour..."
                )
                await asyncio.sleep(3600)
            except asyncio.TimeoutError:
                logger.error(
                    "| MinerScoreSender | Process timed out after 45 minutes, restarting..."
                )
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"| MinerScoreSender | ❗ Error in run_async: {e}")
                await asyncio.sleep(30)

    async def cleanup(self):
        try:
            if self.api_client and not self._external_api_client:
                await self.api_client.aclose()
                logger.info("Closed MinerScoreSender API client")
            if self.db_manager and not getattr(self, "_external_db_manager", False):
                await self.db_manager.close_all_connections()
                logger.info("Closed MinerScoreSender database connections")
        except Exception as e:
            logger.error(f"Error cleaning up MinerScoreSender: {e}")
            logger.error(traceback.format_exc())
