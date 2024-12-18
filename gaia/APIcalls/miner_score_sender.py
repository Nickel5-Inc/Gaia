import asyncio
import threading
from datetime import timezone, datetime
from sqlalchemy.exc import SQLAlchemyError
import bittensor as bt

from gaia.APIcalls.website_api import GaiaCommunicator
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager  # Is this the correct one?


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

    async def fetch_geomagnetic_history(self, miner_hotkey: str) -> dict:
        """
        Fetch predicted values and scores from geomagnetic_history.

        Args:
            miner_hotkey: Hotkey of the miner.

        Returns:
            Dictionary containing predicted values and scores.
        """
        async with self.database_manager.get_connection() as session:
            query = """
                SELECT predicted_value, score, scored_at 
                FROM geomagnetic_history 
                WHERE miner_hotkey = :miner_hotkey 
                ORDER BY scored_at DESC 
                LIMIT 1
            """
            result = await session.execute(query, {"miner_hotkey": miner_hotkey})
            row = result.fetchone()
            if row:
                return {
                    "predicted_value": row[0],
                    "score": row[1],
                    "scored_at": row[2]
                }
            return {}

    async def fetch_soil_moisture_history(self, miner_hotkey: str) -> dict:
        """
        Fetch scores and structure scores from soil_moisture_history.

        Args:
            miner_hotkey: Hotkey of the miner.

        Returns:
            Dictionary containing surface RMSE, rootzone RMSE, and structure scores.
        """
        async with self.database_manager.get_connection() as session:
            query = """
                SELECT surface_rmse, rootzone_rmse, surface_structure_score, rootzone_structure_score, scored_at 
                FROM soil_moisture_history 
                WHERE miner_hotkey = :miner_hotkey 
                ORDER BY scored_at DESC 
                LIMIT 1
            """
            result = await session.execute(query, {"miner_hotkey": miner_hotkey})
            row = result.fetchone()
            if row:
                return {
                    "surface_rmse": row[0],
                    "rootzone_rmse": row[1],
                    "surface_structure_score": row[2],
                    "rootzone_structure_score": row[3],
                    "scored_at": row[4]
                }
            return {}

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

            # Fetch data from geomagnetic_history
            geomagnetic_data = await self.fetch_geomagnetic_history(hotkey)

            # Fetch data from soil_moisture_history
            soil_moisture_data = await self.fetch_soil_moisture_history(hotkey)

            # Prepare the payload
            data_to_send.append({
                "minerUID": uid,
                "minerHotKey": hotkey,
                "minerColdKey": coldkey,
                "geomagneticPredictedValue": geomagnetic_data.get("predicted_value", 0),
                "geomagneticScore": geomagnetic_data.get("score", 0),
                "soilSurfaceRMSE": soil_moisture_data.get("surface_rmse", 0),
                "soilRootzoneRMSE": soil_moisture_data.get("rootzone_rmse", 0),
                "soilSurfaceStructureScore": soil_moisture_data.get("surface_structure_score", 0),
                "soilRootzoneStructureScore": soil_moisture_data.get("rootzone_structure_score", 0),
                "scoreGenerationDate": geomagnetic_data.get("scored_at") or soil_moisture_data.get("scored_at") or datetime.now(timezone.utc).isoformat()
            })

        bt.logging.info(f"| {current_thread} | ⛵ Sending {len(data_to_send)} miner scores to Gaia.")

        # Step 3: Send data to Gaia
        gaia_communicator = GaiaCommunicator("/Validator/Info")
        for miner_data in data_to_send:
            version = miner_data["scoreGenerationDate"]
            gaia_communicator.send_data(version=version)

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