import asyncio
from sqlalchemy import text
from fiber.logging_utils import get_logger

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
        self.loop = loop  # Use the passed event loop

    async def fetch_active_miners(self) -> list:
        """
        Fetch all active miners with their hotkeys, coldkeys, and UIDs.
        """
        async with await self.database_manager.get_connection() as session:
            query = text("SELECT uid, hotkey, coldkey FROM node_table WHERE hotkey IS NOT NULL")
            result = await session.execute(query)
            return [{"uid": row[0], "hotkey": row[1], "coldkey": row[2]} for row in result.fetchall()]

    async def fetch_geomagnetic_history(self, miner_hotkey: str) -> list:
        async with await self.database_manager.get_connection() as session:
            query = text("""
                SELECT id, query_time AS prediction_datetime, predicted_value, ground_truth_value, score, scored_at
                FROM geomagnetic_history
                WHERE miner_hotkey = :miner_hotkey
                ORDER BY scored_at DESC
                LIMIT 10
            """)
            result = await session.execute(query, {"miner_hotkey": miner_hotkey})
            return [
                {
                    "predictionId": row[0],
                    "predictionDate": row[1].isoformat(),
                    "geomagneticPredictionTargetDate": row[1].isoformat(),
                    "geomagneticPredictionInputDate": row[1].isoformat(),
                    "geomagneticPredictedValue": row[2],
                    "geomagneticGroundTruthValue": row[3],
                    "geomagneticScore": row[4],
                    "scoreGenerationDate": row[5].isoformat()
                }
                for row in result.fetchall()
            ]

    async def fetch_soil_moisture_history(self, miner_hotkey: str) -> list:
        async with await self.database_manager.get_connection() as session:
            query = text("""
                SELECT soil_moisture_history.id, 
                       soil_moisture_history.target_time AS prediction_datetime, 
                       soil_moisture_history.region_id, 
                       soil_moisture_predictions.sentinel_bounds, 
                       soil_moisture_predictions.sentinel_crs,
                       soil_moisture_history.target_time, 
                       soil_moisture_history.surface_rmse, 
                       soil_moisture_history.rootzone_rmse, 
                       soil_moisture_history.surface_sm_pred AS surface_predicted_values,
                       soil_moisture_history.rootzone_sm_pred AS rootzone_predicted_values, 
                       soil_moisture_history.surface_sm_truth AS surface_ground_truth_values,
                       soil_moisture_history.rootzone_sm_truth AS rootzone_ground_truth_values, 
                       soil_moisture_history.surface_structure_score,
                       soil_moisture_history.rootzone_structure_score, 
                       soil_moisture_history.scored_at
                FROM soil_moisture_history
                LEFT JOIN soil_moisture_predictions 
                ON soil_moisture_history.region_id = soil_moisture_predictions.region_id
                WHERE soil_moisture_history.miner_hotkey = :miner_hotkey
                ORDER BY soil_moisture_history.scored_at DESC
                LIMIT 10
            """)
            result = await session.execute(query, {"miner_hotkey": miner_hotkey})
            return [
                {
                    "predictionId": row[0],
                    "predictionDate": row[1].isoformat(),
                    "soilPredictionRegionId": row[2],
                    "sentinelRegionBounds": row[3],
                    "sentinelRegionCrs": row[4],
                    "soilPredictionTargetDate": row[5].isoformat(),
                    "soilSurfaceRmse": row[6],
                    "soilRootzoneRmse": row[7],
                    "soilSurfacePredictedValues": row[8],
                    "soilRootzonePredictedValues": row[9],
                    "soilSurfaceGroundTruthValues": row[10],
                    "soilRootzoneGroundTruthValues": row[11],
                    "soilSurfaceStructureScore": row[12],
                    "soilRootzoneStructureScore": row[13],
                    "scoreGenerationDate": row[14].isoformat()
                }
                for row in result.fetchall()
            ]

    async def send_to_gaia(self):
        async with GaiaCommunicator("/Predictions") as gaia_communicator:
            active_miners = await self.fetch_active_miners()
            if not active_miners:
                logger.warning("| MinerScoreSender | No active miners found.")
                return

            batch_size = 3
            for i in range(0, len(active_miners), batch_size):
                batch = active_miners[i:i + batch_size]
                tasks = []
                
                for miner in batch:
                    hotkey = miner["hotkey"]
                    coldkey = miner["coldkey"]

                    geomagnetic_predictions = await self.fetch_geomagnetic_history(hotkey)
                    soil_moisture_predictions = await self.fetch_soil_moisture_history(hotkey)

                    payload = {
                        "minerHotKey": hotkey,
                        "minerColdKey": coldkey,
                        "geomagneticPredictions": geomagnetic_predictions or [],
                        "soilSurfacePredictedValues": prepare_prediction_field(
                            [prediction["soilSurfacePredictedValues"] for prediction in soil_moisture_predictions]
                        ),
                        "soilRootzonePredictedValues": prepare_prediction_field(
                            [prediction["soilRootzonePredictedValues"] for prediction in soil_moisture_predictions]
                        ),
                        "soilSurfaceGroundTruthValues": prepare_prediction_field(
                            [prediction["soilSurfaceGroundTruthValues"] for prediction in soil_moisture_predictions]
                        ),
                        "soilRootzoneGroundTruthValues": prepare_prediction_field(
                            [prediction["soilRootzoneGroundTruthValues"] for prediction in soil_moisture_predictions]
                        ),
                    }

                    tasks.append(gaia_communicator.send_data(data=payload))

                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for miner, result in zip(batch, results):
                    if isinstance(result, Exception):
                        logger.error(f"| MinerScoreSender | Failed to send data for {miner['hotkey']}: {result}")
                
                await asyncio.sleep(1)

    async def run_async(self):
        """
        Run the process to send geomagnetic and soil moisture scores as asyncio tasks.
        """
        try:
            logger.info("| MinerScoreSender | Starting the process to send scores to Gaia API...")
            await self.send_to_gaia()  # Call the consolidated method
            logger.info("| MinerScoreSender | Successfully completed sending scores to Gaia API.")
        except Exception as e:
            logger.error(f"| MinerScoreSender | ❗ Error in run_async: {e}")

    def run(self):
        """
        Entry point for running the MinerScoreSender in a thread-safe manner.
        """
        asyncio.run_coroutine_threadsafe(self.run_async(), self.loop)
