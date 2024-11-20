import os
import traceback
from dotenv import load_dotenv
import argparse
import uvicorn
from fiber.logging_utils import get_logger
from fiber.miner import server
from miner.utils.subnet import factory_router 
from fiber.miner.middleware import configure_extra_logging_middleware
from miner.database.miner_database_manager import MinerDatabaseManager

# Load environment variables
load_dotenv("dev.env")

class Miner:
    def __init__(self, args):
        self.args = args
        self.logger = get_logger(__name__)
        self.wallet = args.wallet
        self.hotkey = args.hotkey
        self.netuid = args.netuid
        self.subtensor_chain_endpoint = args.subtensor.chain_endpoint
        self.subtensor_network = args.subtensor.network
        self.database_manager = MinerDatabaseManager()

    def setup_neuron(self) -> bool:
        """
        Set up the neuron with necessary configurations and connections.
        """
        pass

    def run(self):
        try:
            app = server.factory_app(debug=True)
            app.include_router(factory_router())
            # Use the port argument from the CLI
            uvicorn.run(app, host="0.0.0.0", port=self.args.port)
        except Exception as e:
            self.logger.error(f"Error starting miner: {e}")
            self.logger.error(traceback.format_exc())
            raise e

if __name__ == "__main__":
    # Add arguments
    parser = argparse.ArgumentParser(description="Start the miner with optional flags.")
    
    # ID arguments, should overwrite env variables
    parser.add_argument("--wallet", type=str, help="Name of the wallet to use")
    parser.add_argument("--hotkey", type=str, help="Name of the hotkey to use")
    parser.add_argument("--netuid", type=int, help="Netuid to use")

    # Optional arguments
    parser.add_argument("--port", type=int, default=8091, help="Port to run the miner on")
    parser.add_argument('--use_base_model', action='store_true', help='Enable base model usage')

    # Subtensor arguments
    subtensor_group = parser.add_argument_group('subtensor')
    subtensor_group.add_argument("--subtensor.chain_endpoint", type=str, help="Subtensor chain endpoint to use")
    subtensor_group.add_argument("--subtensor.network", type=str, help="Subtensor network to use", default="test")

    # Parse arguments and start the miner
    args = parser.parse_args()
    miner = Miner(args) 
    miner.run()
