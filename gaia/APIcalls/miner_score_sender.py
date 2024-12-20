import asyncio
import threading
from datetime import timezone, datetime
from sqlalchemy.exc import SQLAlchemyError
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
        async with self.database_manager.get_connection() as session:
            query = "SELECT uid, hotkey, coldkey FROM node_table WHERE hotkey IS NOT NULL"
            result = await session.execute(query)
            return [{"uid": row[0], "hotkey": row[1], "coldkey": row[2]} for row in result.fetchall()]

    async def fetch_geomagnetic_history(self, miner_hotkey: str) -> list:
        """
        Fetch geomagnetic prediction history.

        Args:
            miner_hotkey: Hotkey of the miner.

        Returns:
            List of dictionaries containing geomagnetic prediction details.
        """
        async with self.database_manager.get_connection() as session:
            query = """
                SELECT id, prediction_datetime, predicted_value, ground_truth_value, score, scored_at
                FROM geomagnetic_history
                WHERE miner_hotkey = :miner_hotkey
                ORDER BY scored_at DESC
                LIMIT 10
            """
            result = await session.execute(query, {"miner_hotkey": miner_hotkey})
            return [
                {
                    "predictionID": row[0],
                    "predictionDateTime": row[1].isoformat(),
                    "metrics": {
                        "geomagneticPredictionTargetDate": row[1].isoformat(),
                        "geomagneticPredictionInput": {"inputDateTime": row[1].isoformat()},
                        "geomagneticPredictedValue": row[2],
                        "geomagneticGroundTruthValue": row[3],
                        "geomagneticScore": row[4]
                    },
                    "scoreGenerationDate": row[5].isoformat()
                }
                for row in result.fetchall()
            ]

    async def fetch_soil_moisture_history(self, miner_hotkey: str) -> list:
        """
        Fetch soil moisture prediction history.

        Args:
            miner_hotkey: Hotkey of the miner.

        Returns:
            List of dictionaries containing soil moisture prediction details.
        """
        async with self.database_manager.get_connection() as session:
            query = """
                SELECT id, prediction_datetime, region_id, sentinel_bounds, sentinel_crs,
                       target_date, surface_rmse, rootzone_rmse, surface_predicted_values,
                       rootzone_predicted_values, surface_ground_truth_values,
                       rootzone_ground_truth_values, surface_structure_score,
                       rootzone_structure_score, scored_at
                FROM soil_moisture_history
                WHERE miner_hotkey = :miner_hotkey
                ORDER BY scored_at DESC
                LIMIT 10
            """
            result = await session.execute(query, {"miner_hotkey": miner_hotkey})
            return [
                {
                    "predictionID": row[0],
                    "predictionDateTime": row[1].isoformat(),
                    "metrics": {
                        "soilPredictionRegionID": row[2],
                        "sentinelRegionBounds": row[3],
                        "sentinelRegionCRS": row[4],
                        "soilPredictionTargetDate": row[5].isoformat(),
                        "soilSurfaceRMSE": row[6],
                        "soilRootzoneRMSE": row[7],
                        "soilSurfacePredictedValues": row[8],
                        "soilRootzonePredictedValues": row[9],
                        "soilSurfaceGroundTruthValues": row[10],
                        "soilRootzoneGroundTruthValues": row[11],
                        "soilSurfaceStructureScore": row[12],
                        "soilRootzoneStructureScore": row[13]
                    },
                    "files": {
                        "soilPredictionInput": "input.tif",
                        "soilPredictionOutput": "output.tif"
                    },
                    "scoreGenerationDate": row[14].isoformat()
                }
                for row in result.fetchall()
            ]

    async def send_miner_scores_to_gaia(self) -> None:
        """
        Fetch miner data, scores, and raw predictions, then send to Gaia API.
        """
        current_thread = threading.current_thread().name

        # Step 1: Fetch active miners
        active_miners = await self.fetch_active_miners()
        if not active_miners:
            bt.logging.warning(f"| {current_thread} | ❗ No active miners found.")
            return

        data_to_send = []

        # Step 2: Fetch geomagnetic and soil moisture data for each miner
        for miner in active_miners:
            uid, hotkey, coldkey = miner["uid"], miner["hotkey"], miner["coldkey"]

            # Fetch predictions
            geomagnetic_predictions = await self.fetch_geomagnetic_history(hotkey)
            soil_moisture_predictions = await self.fetch_soil_moisture_history(hotkey)

            # Get the most recent weight (placeholder logic)
            most_recent_weight = 0.5  # Replace with actual weight logic

            # Prepare the payload
            data_to_send.append({
                "minerUID": uid,
                "minerHotKey": hotkey,
                "minerColdKey": coldkey,
                "predictions": {
                    "geomagnetic_predictions": geomagnetic_predictions,
                    "soil_moisture_predictions": soil_moisture_predictions
                },
                "mostRecentWeight": most_recent_weight
            })

        bt.logging.info(f"| {current_thread} | ⛵ Sending {len(data_to_send)} miner scores to Gaia.")

        # Step 3: Send data to Gaia
        gaia_communicator = GaiaCommunicator("/Validator/Info")
        for miner_data in data_to_send:
            gaia_communicator.send_data(data=miner_data)

    def run(self):
        """
        Run the send miner scores process in a new event loop.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.send_miner_scores_to_gaia())


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
