import os
import traceback
import argparse
import uvicorn

from dotenv import load_dotenv
from fiber.logging_utils import get_logger
from fiber.miner import server
from fiber.miner.middleware import configure_extra_logging_middleware
from gaia.miner.utils.subnet import factory_router
from gaia.miner.database.miner_database_manager import MinerDatabaseManager

# Load environment variables
load_dotenv("dev.env")


class Miner:
    """
    Miner class that sets up the neuron and processes tasks.
    """

    def __init__(self, args):
        self.args = args
        self.logger = get_logger(__name__)
        self.wallet = args.wallet
        self.hotkey = args.hotkey
        self.netuid = args.netuid
        self.subtensor_chain_endpoint = args.subtensor_chain_endpoint
        self.subtensor_network = args.subtensor_network
        self.database_manager = MinerDatabaseManager()

    def setup_neuron(self) -> bool:
        """
        Set up the miner neuron with necessary configurations and connections.

        Returns:
            bool: True if setup is successful, False otherwise.
        """
        # Add neuron setup logic here if needed
        self.logger.info("Setting up miner neuron...")
        return True

    def run(self):
        """
        Run the miner application with a FastAPI server.
        """
        try:
            self.logger.info("Starting miner server...")
            app = server.factory_app(debug=True)
            app.include_router(factory_router())

            # Change host to "0.0.0.0" to allow external connections
            uvicorn.run(app, host="0.0.0.0", port=33334)

        except Exception as e:
            self.logger.error(f"Error starting miner: {e}")
            self.logger.error(traceback.format_exc())
            raise e

        while True:
            # Main miner loop for processing tasks
            # Listen to routes for new tasks and process them
            pass


if __name__ == "__main__":
    # Argument parser
    parser = argparse.ArgumentParser(description="Start the miner with optional flags.")

    # Wallet and network arguments
    parser.add_argument("--wallet", type=str, help="Name of the wallet to use")
    parser.add_argument("--hotkey", type=str, help="Name of the hotkey to use")
    parser.add_argument("--netuid", type=int, help="Netuid to use")

    # Optional arguments
    parser.add_argument("--use_base_model", action="store_true", help="Enable base model usage")

    # Subtensor arguments
    parser.add_argument("--subtensor_chain_endpoint", type=str, help="Subtensor chain endpoint to use")
    parser.add_argument("--subtensor_network", type=str, default="test", help="Subtensor network to use")

    # Parse arguments and start miner
    args = parser.parse_args()

    miner = Miner(args)
    miner.run()
