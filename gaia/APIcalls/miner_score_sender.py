import asyncio
import threading
from datetime import timezone, datetime
from sqlalchemy.exc import SQLAlchemyError
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
        async with await self.database_manager.get_connection() as session:
            query = text("SELECT uid, hotkey, coldkey FROM node_table WHERE hotkey IS NOT NULL")
            result = await session.execute(query)
            return [{"uid": row[0], "hotkey": row[1], "coldkey": row[2]} for row in result.fetchall()]

    async def fetch_geomagnetic_history(self, miner_hotkey: str) -> list:
        async with await self.database_manager.get_connection() as session:
            query = text("""
                SELECT id, prediction_datetime, predicted_value, ground_truth_value, score, scored_at
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
                SELECT id, prediction_datetime, region_id, sentinel_bounds, sentinel_crs,
                       target_date, surface_rmse, rootzone_rmse, surface_predicted_values,
                       rootzone_predicted_values, surface_ground_truth_values,
                       rootzone_ground_truth_values, surface_structure_score,
                       rootzone_structure_score, scored_at
                FROM soil_moisture_history
                WHERE miner_hotkey = :miner_hotkey
                ORDER BY scored_at DESC
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

    async def send_geomagnetic_scores_to_gaia(self) -> None:
        """
        Fetch geomagnetic scores and predictions for miners, then send to Gaia API.
        """
        current_thread = threading.current_thread().name

        # Step 1: Fetch active miners
        active_miners = await self.fetch_active_miners()
        if not active_miners:
            bt.logging.warning(f"| {current_thread} | ❗ No active miners found for geomagnetic scores.")
            return

        data_to_send = []

        # Step 2: Fetch geomagnetic data for each miner
        for miner in active_miners:
            uid, hotkey, coldkey = miner["uid"], miner["hotkey"], miner["coldkey"]

            # Fetch geomagnetic predictions
            geomagnetic_predictions = await self.fetch_geomagnetic_history(hotkey)

            # Prepare the payload
            if geomagnetic_predictions:
                data_to_send.append({
                    "minerHotKey": hotkey,
                    "minerColdKey": coldkey,
                    "geomagneticPredictions": geomagnetic_predictions
                })

        bt.logging.info(f"| {current_thread} | ⛵ Sending {len(data_to_send)} geomagnetic scores to Gaia.")

        # Step 3: Send data to Gaia
        gaia_communicator = GaiaCommunicator("/Predictions")
        for miner_data in data_to_send:
            try:
                gaia_communicator.send_data(data=miner_data)
            except Exception as e:
                bt.logging.error(
                    f"| {current_thread} | ❗ Failed to send geomagnetic data for miner {miner_data['minerHotKey']}: {e}")

    async def send_soil_scores_to_gaia(self) -> None:
        """
        Fetch soil moisture scores and predictions for miners, then send to Gaia API.
        """
        current_thread = threading.current_thread().name

        # Step 1: Fetch active miners
        active_miners = await self.fetch_active_miners()
        if not active_miners:
            bt.logging.warning(f"| {current_thread} | ❗ No active miners found for soil scores.")
            return

        data_to_send = []

        # Step 2: Fetch soil moisture data for each miner
        for miner in active_miners:
            uid, hotkey, coldkey = miner["uid"], miner["hotkey"], miner["coldkey"]

            # Fetch soil moisture predictions
            soil_moisture_predictions = await self.fetch_soil_moisture_history(hotkey)

            # Prepare the payload
            if soil_moisture_predictions:
                data_to_send.append({
                    "minerHotKey": hotkey,
                    "minerColdKey": coldkey,
                    "soilMoisturePredictions": soil_moisture_predictions
                })

        bt.logging.info(f"| {current_thread} | ⛵ Sending {len(data_to_send)} soil moisture scores to Gaia.")

        # Step 3: Send data to Gaia
        gaia_communicator = GaiaCommunicator("/Predictions")
        for miner_data in data_to_send:
            try:
                gaia_communicator.send_data(data=miner_data)
            except Exception as e:
                bt.logging.error(
                    f"| {current_thread} | ❗ Failed to send soil moisture data for miner {miner_data['minerHotKey']}: {e}")

    def run(self):
        """
        Run the process to send geomagnetic and soil moisture scores in separate event loops.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            bt.logging.info("| MainThread | Starting the process to send scores to Gaia API...")

            tasks = [
                self.send_geomagnetic_scores_to_gaia(),
                self.send_soil_scores_to_gaia()
            ]

            bt.logging.info("| MainThread | Initiating geomagnetic and soil moisture score sending tasks...")

            loop.run_until_complete(asyncio.gather(*tasks))

            bt.logging.info("| MainThread | Successfully completed sending scores to Gaia API.")
        except Exception as e:
            bt.logging.error(f"| MainThread | ❗ An error occurred in the run method: {e}")
        finally:
            bt.logging.info("| MainThread | Closing the event loop.")
            loop.close()


if __name__ == "__main__":
    db_manager = ValidatorDatabaseManager(
        database="validator_db",
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres"
    )

    sender = MinerScoreSender(database_manager=db_manager)
    sender.run()
