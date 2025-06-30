import asyncio
from sqlalchemy import text
from fiber.logging_utils import get_logger
import pprint
import math
import traceback
import httpx
import json
from gaia.APIcalls.website_api import GaiaCommunicator
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from typing import Dict, Optional

def prepare_prediction_field(data_list):
    """
    Convert a list of values into a comma-separated string.

    Args:
        data_list (list): List of prediction values.

    Returns:
        str: Comma-separated string of values.
    """
    return ",".join(map(str, data_list)) if data_list else ""

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
        return [{"uid": row["uid"], "hotkey": row["hotkey"], "coldkey": row["coldkey"]} for row in results]

    def _process_geomagnetic_row_sync(self, row: Dict) -> Optional[Dict]:
        """Synchronous helper to process a single geomagnetic history row."""
        try:
            # Convert to float and check for NaN/Inf
            pred_value = float(row["predicted_value"])
            truth_value = float(row["ground_truth_value"])
            score_value = float(row["score"])

            if all(not math.isnan(v) and not math.isinf(v) for v in [pred_value, truth_value, score_value]):
                return {
                    "predictionId": row["id"],
                    "predictionDate": row["prediction_datetime"].isoformat(),
                    "geomagneticPredictionTargetDate": row["prediction_datetime"].isoformat(),
                    "geomagneticPredictionInputDate": row["prediction_datetime"].isoformat(),
                    "geomagneticPredictedValue": pred_value,
                    "geomagneticGroundTruthValue": truth_value,
                    "geomagneticScore": score_value,
                    "scoreGenerationDate": row["scored_at"].isoformat()
                }
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid numeric value in geo row {row.get('id', 'N/A')}, skipping: {e}")
        return None

    async def fetch_geomagnetic_history(self, miner_hotkey: str) -> list:
        # First clean up NaN values from history
        # cleanup_query = """
        #     DELETE FROM geomagnetic_history 
        #     WHERE miner_hotkey = :miner_hotkey 
        #     AND (
        #         predicted_value IS NULL 
        #         OR ground_truth_value IS NULL 
        #         OR score IS NULL
        #         OR predicted_value::text = 'NaN'
        #         OR ground_truth_value::text = 'NaN'
        #         OR score::text = 'NaN'
        #     )
        # """
        # await self.db_manager.execute(cleanup_query, {"miner_hotkey": miner_hotkey})

        # Then fetch valid records
        query = """
            SELECT id, query_time AS prediction_datetime, predicted_value, ground_truth_value, score, scored_at
            FROM geomagnetic_history
            WHERE miner_hotkey = :miner_hotkey
            AND predicted_value IS NOT NULL 
            AND ground_truth_value IS NOT NULL 
            AND score IS NOT NULL
            AND predicted_value::text != 'NaN'
            AND ground_truth_value::text != 'NaN'
            AND score::text != 'NaN'
            ORDER BY scored_at DESC
            LIMIT 10
        """
        results = await self.db_manager.fetch_all(query, {"miner_hotkey": miner_hotkey})

        if not results:
            return []

        loop = asyncio.get_event_loop()
        # Offload row processing
        processed_rows = await loop.run_in_executor(
            None, 
            lambda rows: [self._process_geomagnetic_row_sync(r) for r in rows], 
            results
        )
        
        # Filter out None results (from rows that failed processing)
        valid_predictions = [p for p in processed_rows if p is not None]
        return valid_predictions

    def _process_soil_row_sync(self, row: Dict) -> Dict:
        """Synchronous helper to process a single soil moisture history row."""
        # Ensure json is imported within the sync function if not globally available in the executor's context
        # import json # Not strictly needed if json is a built-in and standard library.
        return {
            "predictionId": row["id"],
            "predictionDate": row["target_time"].isoformat(),
            "soilPredictionRegionId": row["region_id"],
            "sentinelRegionBounds": json.dumps(row["sentinel_bounds"]) if row["sentinel_bounds"] else "[]",
            "sentinelRegionCrs": row["sentinel_crs"] if row["sentinel_crs"] else 4326,
            "soilPredictionTargetDate": row["target_time"].isoformat(),
            "soilSurfaceRmse": row["surface_rmse"],
            "soilRootzoneRmse": row["rootzone_rmse"],
            "soilSurfacePredictedValues": json.dumps(row["surface_sm_pred"]) if row["surface_sm_pred"] else "[]",
            "soilRootzonePredictedValues": json.dumps(row["rootzone_sm_pred"]) if row["rootzone_sm_pred"] else "[]",
            "soilSurfaceGroundTruthValues": json.dumps(row["surface_sm_truth"]) if row["surface_sm_truth"] else "[]",
            "soilRootzoneGroundTruthValues": json.dumps(row["rootzone_sm_truth"]) if row["rootzone_sm_truth"] else "[]",
            "soilSurfaceStructureScore": row["surface_structure_score"],
            "soilRootzoneStructureScore": row["rootzone_structure_score"],
            "scoreGenerationDate": row["scored_at"].isoformat(),
        }

    async def fetch_soil_moisture_history(self, miner_hotkey: str) -> list:
        query = """
            SELECT soil_moisture_history.id, 
                   soil_moisture_history.target_time,
                   soil_moisture_history.region_id, 
                   COALESCE(soil_moisture_history.sentinel_bounds, soil_moisture_regions.sentinel_bounds) as sentinel_bounds,
                   COALESCE(soil_moisture_history.sentinel_crs, soil_moisture_regions.sentinel_crs) as sentinel_crs,
                   soil_moisture_history.surface_rmse, 
                   soil_moisture_history.rootzone_rmse, 
                   soil_moisture_history.surface_sm_pred,
                   soil_moisture_history.rootzone_sm_pred, 
                   soil_moisture_history.surface_sm_truth,
                   soil_moisture_history.rootzone_sm_truth, 
                   soil_moisture_history.surface_structure_score,
                   soil_moisture_history.rootzone_structure_score, 
                   soil_moisture_history.scored_at
            FROM soil_moisture_history
            LEFT JOIN soil_moisture_regions 
            ON soil_moisture_history.region_id = soil_moisture_regions.id
            WHERE soil_moisture_history.miner_hotkey = :miner_hotkey
            ORDER BY soil_moisture_history.scored_at DESC
            LIMIT 10
        """
        results = await self.db_manager.fetch_all(query, {"miner_hotkey": miner_hotkey})
        
        if not results:
            return []
            
        # import json # Moved to _process_soil_row_sync if strictly needed there.
        loop = asyncio.get_event_loop()
        # Offload row processing (list comprehension equivalent)
        processed_rows = await loop.run_in_executor(
            None,
            lambda rows: [self._process_soil_row_sync(r) for r in rows],
            results
        )
        return processed_rows # Assuming _process_soil_row_sync never returns None, or they are acceptable

    async def send_to_gaia(self):
        try:
            if not self.api_client or self.api_client.is_closed:
                self.api_client = httpx.AsyncClient(
                    timeout=30.0,
                    follow_redirects=True,
                    limits=httpx.Limits(
                        max_connections=100,
                        max_keepalive_connections=20,
                        keepalive_expiry=30,
                    ),
                    transport=httpx.AsyncHTTPTransport(retries=3),
                )
                logger.warning("| MinerScoreSender | Re-created closed API client")

            async with GaiaCommunicator("/Predictions", client=self.api_client) as gaia_communicator:
                active_miners = await self.fetch_active_miners()
                if not active_miners:
                    logger.warning("| MinerScoreSender | No active miners found.")
                    return

                # Semaphore to limit concurrent miner data processing (DB heavy)
                miner_processing_semaphore = asyncio.Semaphore(5)

                async def process_miner_with_semaphore(miner):
                    async with miner_processing_semaphore:
                        try:
                            logger.debug(f"Processing miner {miner['hotkey']} under semaphore")
                            geo_history, soil_history = await asyncio.gather(
                                self.fetch_geomagnetic_history(miner["hotkey"]),
                                self.fetch_soil_moisture_history(miner["hotkey"])
                            )
                            logger.debug(f"Fetched history for miner {miner['hotkey']}")
                            return {
                                "minerHotKey": miner["hotkey"],
                                "minerColdKey": miner["coldkey"],
                                "geomagneticPredictions": geo_history or [],
                                "soilMoisturePredictions": soil_history or []
                            }
                        except Exception as e:
                            logger.error(f"Error processing miner {miner['hotkey']} under semaphore: {str(e)}\n{traceback.format_exc()}")
                            return None

                # Process miners in smaller batches to reduce memory usage
                batch_size = 50  # Process 50 miners at a time
                successful_sends = 0
                failed_sends = 0
                
                logger.info(f"Starting to process data for {len(active_miners)} active miners in batches of {batch_size}.")
                
                for batch_start in range(0, len(active_miners), batch_size):
                    batch_end = min(batch_start + batch_size, len(active_miners))
                    batch_miners = active_miners[batch_start:batch_end]
                    
                    logger.info(f"Processing miner batch {batch_start//batch_size + 1}: miners {batch_start+1}-{batch_end}")
                    
                    # Process this batch
                    batch_tasks = [process_miner_with_semaphore(miner) for miner in batch_miners]
                    batch_payloads = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    
                    valid_batch_payloads = [p for p in batch_payloads if p is not None and not isinstance(p, Exception)]
                    logger.info(f"Batch {batch_start//batch_size + 1}: Successfully processed {len(valid_batch_payloads)} miners.")
                    
                    # Send batch data immediately and clean up
                    for i, payload in enumerate(valid_batch_payloads):
                        try:
                            logger.debug(f"Sending data for miner {payload['minerHotKey']} ({i+1}/{len(valid_batch_payloads)})")
                            await asyncio.wait_for(
                                gaia_communicator.send_data(data=payload),
                                timeout=30 # Timeout for individual API call
                            )
                            logger.debug(f"Successfully sent data for miner {payload['minerHotKey']}")
                            successful_sends += 1
                        except asyncio.TimeoutError:
                            logger.error(f"Timeout sending API data for miner {payload['minerHotKey']}")
                            failed_sends += 1
                        except Exception as e:
                            logger.error(f"Error sending API data for miner {payload['minerHotKey']}: {str(e)}\n{traceback.format_exc()}")
                            failed_sends += 1
                        finally:
                            await asyncio.sleep(0.2) # Add a small delay to avoid rate limiting
                    
                    # IMMEDIATE cleanup of batch data
                    try:
                        del batch_tasks
                        del batch_payloads  
                        del valid_batch_payloads
                        # Force GC every few batches for large miner sets
                        if (batch_start // batch_size + 1) % 3 == 0:
                            import gc
                            collected = gc.collect()
                            logger.debug(f"Batch cleanup: GC collected {collected} objects")
                    except Exception as cleanup_err:
                        logger.debug(f"Error during batch cleanup: {cleanup_err}")
                    
                    # Brief pause between batches
                    if batch_end < len(active_miners):
                        await asyncio.sleep(1.0)
                
                logger.info(f"Completed sending data to Gaia API. Successful: {successful_sends}, Failed: {failed_sends}")

        except Exception as e:
            logger.error(f"Error in send_to_gaia: {str(e)}\n{traceback.format_exc()}")
            raise

    async def run_async(self):
        while True:
            try:
                logger.info("| MinerScoreSender | Starting hourly process to send scores to Gaia API...")
                await asyncio.wait_for(self.send_to_gaia(), timeout=2700)
                logger.info("| MinerScoreSender | Completed sending scores. Sleeping for 1 hour...")
                await asyncio.sleep(3600)
            except asyncio.TimeoutError:
                logger.error("| MinerScoreSender | Process timed out after 45 minutes, restarting...")
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"| MinerScoreSender | ❗ Error in run_async: {e}")
                await asyncio.sleep(30)

    async def cleanup(self):
        try:
            if self.api_client and not self._external_api_client:
                await self.api_client.aclose()
                logger.info("Closed MinerScoreSender API client")
            if self.db_manager and not getattr(self, '_external_db_manager', False):
                await self.db_manager.close_all_connections()
                logger.info("Closed MinerScoreSender database connections")
        except Exception as e:
            logger.error(f"Error cleaning up MinerScoreSender: {e}")
            logger.error(traceback.format_exc())
