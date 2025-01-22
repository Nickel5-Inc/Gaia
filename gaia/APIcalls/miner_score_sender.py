import asyncio
from sqlalchemy import text
from fiber.logging_utils import get_logger
import pprint
import math

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
    def __init__(self, database_manager: ValidatorDatabaseManager, loop: asyncio.AbstractEventLoop):
        """
        Initialize the MinerScoreSender class.

        Args:
            database_manager: An instance of ValidatorDatabaseManager.
            loop: The asyncio event loop to use for running tasks.
        """
        self.database_manager = database_manager
        self.loop = loop

    async def fetch_active_miners(self) -> list:
        """
        Fetch all active miners with their hotkeys, coldkeys, and UIDs.
        """
        query = "SELECT uid, hotkey, coldkey FROM node_table WHERE hotkey IS NOT NULL"
        results = await self.database_manager.fetch_all(query)
        return [{"uid": row["uid"], "hotkey": row["hotkey"], "coldkey": row["coldkey"]} for row in results]

    async def fetch_geomagnetic_history(self, miner_hotkey: str) -> list:
        # First clean up NaN values from history
        cleanup_query = """
            DELETE FROM geomagnetic_history 
            WHERE miner_hotkey = :miner_hotkey 
            AND (
                predicted_value IS NULL 
                OR ground_truth_value IS NULL 
                OR score IS NULL
                OR predicted_value::text = 'NaN'
                OR ground_truth_value::text = 'NaN'
                OR score::text = 'NaN'
            )
        """
        await self.database_manager.execute(cleanup_query, {"miner_hotkey": miner_hotkey})
        
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
        results = await self.database_manager.fetch_all(query, {"miner_hotkey": miner_hotkey})
        
        valid_predictions = []
        for row in results:
            try:
                # Convert to float and check for NaN/Inf
                pred_value = float(row["predicted_value"])
                truth_value = float(row["ground_truth_value"])
                score_value = float(row["score"])
                
                if (not math.isnan(pred_value) and not math.isinf(pred_value) and
                    not math.isnan(truth_value) and not math.isinf(truth_value) and
                    not math.isnan(score_value) and not math.isinf(score_value)):
                    
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
                continue
                
        return valid_predictions

    async def fetch_soil_moisture_history(self, miner_hotkey: str) -> list:
        query = """
            SELECT soil_moisture_history.id, 
                   soil_moisture_history.target_time,
                   soil_moisture_history.region_id, 
                   soil_moisture_predictions.sentinel_bounds, 
                   soil_moisture_predictions.sentinel_crs,
                   soil_moisture_history.target_time, 
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
        results = await self.database_manager.fetch_all(query, {"miner_hotkey": miner_hotkey})
        
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
            async with GaiaCommunicator("/Predictions") as gaia_communicator:
                active_miners = await self.fetch_active_miners()
                if not active_miners:
                    logger.warning("| MinerScoreSender | No active miners found.")
                    return

                batch_size = 3
                total_batches = (len(active_miners) + batch_size - 1) // batch_size
                
                for i in range(0, len(active_miners), batch_size):
                    try:
                        batch = active_miners[i:i + batch_size]
                        current_batch = (i // batch_size) + 1
                        logger.info(f"Processing batch {current_batch} of {total_batches}")

                        tasks = []
                        for miner in batch:
                            tasks.append(self.fetch_geomagnetic_history(miner["hotkey"]))
                            tasks.append(self.fetch_soil_moisture_history(miner["hotkey"]))

                        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=120)
                        
                        for j in range(0, len(results), 2):
                            miner = batch[j//2]
                            hotkey = miner["hotkey"]
                            
                            payload = {
                                "minerHotKey": hotkey,
                                "minerColdKey": miner["coldkey"],
                                "geomagneticPredictions": results[j] or [],
                                "soilMoisturePredictions": results[j+1] or []
                            }

                            logger.info(f"Sending data for miner {hotkey} (batch {current_batch}/{total_batches})")
                            #logger.debug(f"Payload:\n{pprint.pformat(payload, indent=2)}") uncomment to see payload
                            
                            try:
                                await asyncio.wait_for(
                                    gaia_communicator.send_data(data=payload),
                                    timeout=30
                                )
                                logger.debug(f"Successfully processed miner {hotkey}")
                            except asyncio.TimeoutError:
                                logger.error(f"Timeout sending data for miner {hotkey}")
                                            
                        await asyncio.sleep(0.5)
                    
                    except asyncio.TimeoutError:
                        logger.error(f"Batch {current_batch} processing timed out, moving to next batch")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing batch {current_batch}: {str(e)}")
                        continue

                logger.info("Completed processing all miners")

        except Exception as e:
            logger.error(f"Error in send_to_gaia: {str(e)}")
            raise

    async def run_async(self):
        """
        Run the process to send geomagnetic and soil moisture scores as asyncio tasks.
        Runs once per hour with automatic restart on failure.
        """
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
        """
        Entry point for running the MinerScoreSender in a thread-safe manner.
        """
        asyncio.run_coroutine_threadsafe(self.run_async(), self.loop)
