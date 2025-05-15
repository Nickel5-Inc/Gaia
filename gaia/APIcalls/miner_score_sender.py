import asyncio
from sqlalchemy import text
from fiber.logging_utils import get_logger
import pprint
import math
import traceback
import httpx
from gaia.APIcalls.website_api import GaiaCommunicator
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager

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
    def __init__(self, database_manager, loop, api_client=None):
        """
        Initialize the MinerScoreSender.

        Args:
            database_manager: Database manager instance
            loop: Asyncio event loop
            api_client: Optional httpx.AsyncClient for API communication
        """
        self.db_manager = database_manager
        self.loop = loop
        self.api_client = api_client
        self._external_api_client = api_client is not None

    async def fetch_active_miners(self) -> list:
        """
        Fetch all active miners with their hotkeys, coldkeys, and UIDs.
        """
        query = "SELECT uid, hotkey, coldkey FROM node_table WHERE hotkey IS NOT NULL"
        results = await self.db_manager.fetch_all(query)
        return [{"uid": row["uid"], "hotkey": row["hotkey"], "coldkey": row["coldkey"]} for row in results]

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

        valid_predictions = []
        for row in results:
            try:
                # Convert to float and check for NaN/Inf
                pred_value = float(row["predicted_value"])
                truth_value = float(row["ground_truth_value"])
                score_value = float(row["score"])

                if all(not math.isnan(v) and not math.isinf(v) for v in [pred_value, truth_value, score_value]):
                    valid_predictions.append({
                        "predictionId": row["id"],
                        "predictionDate": row["prediction_datetime"].isoformat(),
                        "geomagneticPredictionTargetDate": row["prediction_datetime"].isoformat(),
                        "geomagneticPredictionInputDate": row["prediction_datetime"].isoformat(),
                        "geomagneticPredictedValue": pred_value,
                        "geomagneticGroundTruthValue": truth_value,
                        "geomagneticScore": score_value,
                        "scoreGenerationDate": row["scored_at"].isoformat()
                    })
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid numeric value in row {row['id']}, skipping: {e}")
        return valid_predictions

    async def fetch_soil_moisture_history(self, miner_hotkey: str) -> list:
        query = """
            SELECT soil_moisture_history.id, 
                   soil_moisture_history.target_time,
                   soil_moisture_history.region_id, 
                   soil_moisture_predictions.sentinel_bounds, 
                   soil_moisture_predictions.sentinel_crs,
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
            LEFT JOIN soil_moisture_predictions 
            ON soil_moisture_history.region_id = soil_moisture_predictions.region_id
            WHERE soil_moisture_history.miner_hotkey = :miner_hotkey
            ORDER BY soil_moisture_history.scored_at DESC
            LIMIT 10
        """
        results = await self.db_manager.fetch_all(query, {"miner_hotkey": miner_hotkey})
        import json
        return [
            {
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
            for row in results
        ]

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

                # Gather all processed miner data
                logger.info(f"Starting to process data for {len(active_miners)} active miners.")
                miner_data_tasks = [process_miner_with_semaphore(miner) for miner in active_miners]
                all_miner_payloads = await asyncio.gather(*miner_data_tasks, return_exceptions=True)

                valid_payloads = [p for p in all_miner_payloads if p is not None and not isinstance(p, Exception)]
                logger.info(f"Successfully processed data for {len(valid_payloads)} miners.")

                # Send data to Gaia API in batches or sequentially
                # For simplicity, sending one by one with timeout, can be batched if API supports batch endpoints
                # or with another semaphore if sending concurrently is desired.
                successful_sends = 0
                failed_sends = 0
                for i, payload in enumerate(valid_payloads):
                    try:
                        logger.debug(f"Sending data for miner {payload['minerHotKey']} ({i+1}/{len(valid_payloads)})")
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

    def run(self):
        asyncio.run_coroutine_threadsafe(self.run_async(), self.loop)

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
