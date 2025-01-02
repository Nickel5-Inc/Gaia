import asyncio
import threading
from datetime import timezone, datetime
from sqlalchemy import text
import bittensor as bt

from gaia.APIcalls.website_api import GaiaCommunicator
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager


class MinerScoreSender:
    def __init__(self, database_manager: ValidatorDatabaseManager):
        """
        Initialize the MinerScoreSender class.

        Args:
            database_manager: An instance of ValidatorDatabaseManager.
        """
        self.database_manager = database_manager

    async def fetch_active_miners(self) -> list:
        """
        Fetch all active miners with their hotkeys, coldkeys, and UIDs.

        Returns:
            List of dictionaries with miner details.
        """
        bt.logging.debug("| MinerScoreSender | Fetching active miners...")
        async with await self.database_manager.get_connection() as session:
            query = text("SELECT uid, hotkey, coldkey FROM node_table WHERE hotkey IS NOT NULL")
            result = await session.execute(query)
            return [{"uid": row[0], "hotkey": row[1], "coldkey": row[2]} for row in result.fetchall()]

    async def fetch_geomagnetic_history(self, miner_hotkey: str) -> list:
        """
        Fetch geomagnetic prediction history for a given miner.
        """
        bt.logging.debug(f"| MinerScoreSender | Fetching geomagnetic history for miner {miner_hotkey}...")
        async with await self.database_manager.get_connection() as session:
            query = text("""
                SELECT id, query_time AS prediction_datetime, predicted_value, ground_truth_value, score, scored_at
                FROM geomagnetic_history
                WHERE miner_hotkey = :miner_hotkey
                ORDER BY scored_at DESC
                LIMIT 20
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
        """
        Fetch soil moisture prediction history for a given miner.
        """
        bt.logging.debug(f"| MinerScoreSender | Fetching soil moisture history for miner {miner_hotkey}...")
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
                LIMIT 20
            """)
            result = await session.execute(query, {"miner_hotkey": miner_hotkey})
            return [
                {
                    "predictionId": row[0],
                    "predictionDate": datetime.fromtimestamp(row[1]).isoformat() if isinstance(row[1], int) else row[1].isoformat(),
                    "soilPredictionRegionId": row[2],
                    "sentinelRegionBounds": row[3],
                    "sentinelRegionCrs": row[4],
                    "soilPredictionTargetDate": datetime.fromtimestamp(row[5]).isoformat() if isinstance(row[5], int) else row[5].isoformat(),
                    "soilSurfaceRmse": row[6],
                    "soilRootzoneRmse": row[7],
                    "soilSurfacePredictedValues": row[8],
                    "soilRootzonePredictedValues": row[9],
                    "soilSurfaceGroundTruthValues": row[10],
                    "soilRootzoneGroundTruthValues": row[11],
                    "soilSurfaceStructureScore": row[12],
                    "soilRootzoneStructureScore": row[13],
                    "scoreGenerationDate": datetime.fromtimestamp(row[14]).isoformat() if isinstance(row[14], int) else row[14].isoformat(),
                }
                for row in result.fetchall()
            ]

    async def send_geomagnetic_scores_to_gaia(self):
        """
        Fetch geomagnetic scores and predictions for miners, then send to Gaia API.
        """
        bt.logging.debug("| MinerScoreSender | Sending geomagnetic scores...")
        active_miners = await self.fetch_active_miners()
        if not active_miners:
            bt.logging.warning("| MinerScoreSender | ❗ No active miners found for geomagnetic scores.")
            return

        data_to_send = []
        for miner in active_miners:
            uid, hotkey, coldkey = miner["uid"], miner["hotkey"], miner["coldkey"]
            geomagnetic_predictions = await self.fetch_geomagnetic_history(hotkey)
            if geomagnetic_predictions:
                data_to_send.append({
                    "minerHotKey": hotkey,
                    "minerColdKey": coldkey,
                    "geomagneticPredictions": geomagnetic_predictions
                })

        gaia_communicator = GaiaCommunicator("/Predictions")
        for miner_data in data_to_send:
            try:
                gaia_communicator.send_data(data=miner_data)
            except Exception as e:
                bt.logging.error(f"| MinerScoreSender | ❗ Failed to send geomagnetic data for {miner_data['minerHotKey']}: {e}")

    async def send_soil_scores_to_gaia(self):
        """
        Fetch soil moisture scores and predictions for miners, then send to Gaia API.
        """
        bt.logging.debug("| MinerScoreSender | Sending soil moisture scores...")
        active_miners = await self.fetch_active_miners()
        if not active_miners:
            bt.logging.warning("| MinerScoreSender | ❗ No active miners found for soil scores.")
            return

        data_to_send = []
        for miner in active_miners:
            uid, hotkey, coldkey = miner["uid"], miner["hotkey"], miner["coldkey"]
            soil_moisture_predictions = await self.fetch_soil_moisture_history(hotkey)
            if soil_moisture_predictions:
                data_to_send.append({
                    "minerHotKey": hotkey,
                    "minerColdKey": coldkey,
                    "soilMoisturePredictions": soil_moisture_predictions
                })

        gaia_communicator = GaiaCommunicator("/Predictions")
        for miner_data in data_to_send:
            try:
                gaia_communicator.send_data(data=miner_data)
            except Exception as e:
                bt.logging.error(f"| MinerScoreSender | ❗ Failed to send soil moisture data for {miner_data['minerHotKey']}: {e}")

    async def run_async(self):
        """
        Run the process to send geomagnetic and soil moisture scores as asyncio tasks.
        """
        try:
            bt.logging.info("| MinerScoreSender | Starting the process to send scores to Gaia API...")
            await asyncio.gather(
                self.send_geomagnetic_scores_to_gaia(),
                self.send_soil_scores_to_gaia(),
            )
            bt.logging.info("| MinerScoreSender | Successfully completed sending scores to Gaia API.")
        except Exception as e:
            bt.logging.error(f"| MinerScoreSender | ❗ An error occurred in run_async: {e}")

    def run(self):
        """
        Entry point for running the MinerScoreSender in a thread-safe manner.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if loop.is_running():
            asyncio.run_coroutine_threadsafe(self.run_async(), loop)
        else:
            asyncio.run(self.run_async())

