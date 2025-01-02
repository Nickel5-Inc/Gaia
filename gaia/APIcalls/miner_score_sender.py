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
        bt.logging.debug("| MinerScoreSender | Fetching active miners...")
        async with await self.database_manager.get_connection() as session:
            query = text("SELECT uid, hotkey, coldkey FROM node_table WHERE hotkey IS NOT NULL")
            result = await session.execute(query)
            return [{"uid": row[0], "hotkey": row[1], "coldkey": row[2]} for row in result.fetchall()]

    async def send_geomagnetic_scores_to_gaia(self):
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

    async def send_soil_scores_to_gaia(self):
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
            bt.logging.error(f"| MinerScoreSender | ❗ An error occurred in the run_async method: {e}")

    def run(self):
        """
        Entry point for running the MinerScoreSender in a thread-safe manner.
        """
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If the event loop is already running, schedule run_async
            asyncio.run_coroutine_threadsafe(self.run_async(), loop)
        else:
            # Otherwise, start a new event loop
            asyncio.run(self.run_async())
