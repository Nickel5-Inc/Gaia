import os
import traceback
from dotenv import load_dotenv
import argparse
from fiber import SubstrateInterface
import uvicorn
from fiber.logging_utils import get_logger
from fiber.miner import server
from fiber.miner.core import configuration
from fiber.miner.middleware import configure_extra_logging_middleware
from fiber.chain import chain_utils
from gaia.miner.utils.subnet import factory_router
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
import ssl
import logging
from fiber import logging_utils
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask

# Define max request size (5MB in bytes)
MAX_REQUEST_SIZE = 5 * 1024 * 1024  # 5MB


os.environ["NODE_TYPE"] = "miner"


class Miner:
    """
    Miner class that sets up the neuron and processes tasks.
    """

    def __init__(self, args):
        self.args = args
        self.logger = get_logger(__name__)

        # Load environment variables
        load_dotenv("dev.env")

        # Load wallet and network settings from args or env
        self.wallet = (
            args.wallet if args.wallet else os.getenv("WALLET_NAME", "default")
        )
        self.hotkey = (
            args.hotkey if args.hotkey else os.getenv("HOTKEY_NAME", "default")
        )
        self.netuid = args.netuid if args.netuid else int(os.getenv("NETUID", 237))
        self.port = args.port if args.port else int(os.getenv("PORT", 8091))
        # Load chain endpoint from args or env
        self.subtensor_chain_endpoint = (
            self.args.subtensor.chain_endpoint
            if hasattr(self.args, "subtensor")
            and hasattr(self.args.subtensor, "chain_endpoint")
            else os.getenv(
                "SUBTENSOR_ADDRESS", "wss://test.finney.opentensor.ai:443/"
            )
        )

        self.subtensor_network = (
            self.args.subtensor.network
            if hasattr(self.args, "subtensor")
            and hasattr(self.args.subtensor, "network")
            else os.getenv("SUBTENSOR_NETWORK", "test")
        )

        self.database_manager = MinerDatabaseManager()
        self.soil_task = SoilMoistureTask(
            db_manager=self.database_manager, node_type="miner"
        )

    def setup_neuron(self) -> bool:
        """
        Set up the miner neuron with necessary configurations and connections.
        """
        self.logger.info("Setting up miner neuron...")

        # Add detailed logging for keypair and wallet info
        self.keypair = chain_utils.load_hotkey_keypair(self.wallet, self.hotkey)
        self.logger.debug(
            f"""
Detailed Neuron Configuration:
----------------------------
Wallet Path: {self.wallet}
Hotkey Path: {self.hotkey}
Keypair SS58 Address: {self.keypair.ss58_address}
Keypair Public Key: {self.keypair.public_key}
Subtensor Chain Endpoint: {self.subtensor_chain_endpoint}
Network: {self.subtensor_network}
Port: {self.port}
        """
        )

        # Test signature verification
        test_message = "test_message"
        test_sig = self.keypair.sign(test_message)
        verify_result = self.keypair.verify(test_message, test_sig)
        self.logger.debug(
            f"""
Signature Verification Test:
-------------------------
Test Message: {test_message}
Test Signature: {test_sig}
Verification Result: {verify_result}
        """
        )

        return True

    def run(self):
        """
        Run the miner application with a FastAPI server.
        """
        try:
            if not self.setup_neuron():
                self.logger.error("Failed to setup neuron")
                return

            self.logger.info("Starting miner server...")
            app = server.factory_app(debug=True)

            # Configure app to handle larger requests
            app.body_limit = MAX_REQUEST_SIZE

            app.include_router(factory_router(self))

            if os.getenv("ENV", "dev").lower() == "dev":
                configure_extra_logging_middleware(app)

            # Simplified logging configuration
            log_config = {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "default": {
                        "()": "uvicorn.logging.DefaultFormatter",
                        "fmt": "%(levelprefix)s %(asctime)s | %(message)s",
                        "use_colors": True,
                    },
                    "access": {
                        "()": "uvicorn.logging.AccessFormatter",
                        "fmt": '%(levelprefix)s %(asctime)s | "%(request_line)s" %(status_code)s',
                        "use_colors": True,
                    },
                },
                "handlers": {
                    "default": {
                        "formatter": "default",
                        "class": "logging.StreamHandler",
                        "stream": "ext://sys.stderr",
                    },
                    "access": {
                        "formatter": "access",
                        "class": "logging.StreamHandler",
                        "stream": "ext://sys.stdout",
                    },
                },
                "loggers": {
                    "uvicorn": {"handlers": ["default"], "level": "INFO"},
                    "uvicorn.error": {"handlers": ["default"], "level": "INFO"},
                    "uvicorn.access": {"handlers": ["access"], "level": "INFO"},
                },
            }

            # Set Fiber logging to DEBUG
            fiber_logger = logging_utils.get_logger("fiber")
            fiber_logger.setLevel(logging.DEBUG)

            # Add a file handler for detailed logging
            fh = logging.FileHandler("fiber_debug.log")
            fh.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            fh.setFormatter(formatter)
            fiber_logger.addHandler(fh)

            uvicorn.run(
                app,
                host="0.0.0.0",
                port=self.port,
                log_config=log_config,
                log_level="trace",
            )
        except Exception as e:
            self.logger.error(f"Error starting miner: {e}")
            self.logger.error(traceback.format_exc())
            raise e

        while True:
            # Main miner loop for processing tasks
            # Listen to routes for new tasks and process them
            pass


if __name__ == "__main__":
    # Add arguments
    parser = argparse.ArgumentParser(description="Start the miner with optional flags.")

    # Create a subtensor group
    subtensor_group = parser.add_argument_group("subtensor")

    # Wallet and network arguments
    parser.add_argument("--wallet", type=str, help="Name of the wallet to use")
    parser.add_argument("--hotkey", type=str, help="Name of the hotkey to use")
    parser.add_argument("--netuid", type=int, help="Netuid to use")

    # Optional arguments
    parser.add_argument(
        "--port", type=int, default=8091, help="Port to run the miner on"
    )
    parser.add_argument(
        "--use_base_model", action="store_true", help="Enable base model usage"
    )

    # Subtensor arguments
    subtensor_group.add_argument(
        "--subtensor.chain_endpoint", type=str, help="Subtensor chain endpoint to use"
    )
    subtensor_group.add_argument(
        "--subtensor.network", type=str, default="test", help="Subtensor network to use"
    )

    # Parse arguments and start the miner
    args = parser.parse_args()
    miner = Miner(args)
    miner.run()
