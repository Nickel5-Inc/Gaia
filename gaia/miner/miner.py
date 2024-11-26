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
        self.wallet = args.wallet if args.wallet else os.getenv("WALLET_NAME", "default")
        self.hotkey = args.hotkey if args.hotkey else os.getenv("HOTKEY_NAME", "default")
        self.netuid = args.netuid if args.netuid else int(os.getenv("NETUID", 237))
        self.port = args.port if args.port else int(os.getenv("PORT", 8091))
        # Load chain endpoint from args or env
        self.subtensor_chain_endpoint = (
            self.args.subtensor.chain_endpoint
            if hasattr(self.args, "subtensor") and hasattr(self.args.subtensor, "chain_endpoint")
            else os.getenv("SUBSTRATE_CHAIN_ENDPOINT", "wss://test.finney.opentensor.ai:443/")
        )
        
        self.subtensor_network = (
            self.args.subtensor.network 
            if hasattr(self.args, "subtensor") and hasattr(self.args.subtensor, "network")
            else os.getenv("SUBTENSOR_NETWORK", "test")
        )
        
        self.database_manager = MinerDatabaseManager()

    def setup_neuron(self) -> bool:
        """
        Set up the miner neuron with necessary configurations and connections.
        """
        self.logger.info("Setting up miner neuron...")
        
        # Add detailed logging for keypair and wallet info
        self.keypair = chain_utils.load_hotkey_keypair(self.wallet, self.hotkey)
        self.logger.info(f"Wallet: {self.wallet}")
        self.logger.info(f"Hotkey: {self.hotkey}")
        self.logger.info(f"Keypair SS58 Address: {self.keypair.ss58_address}")
        self.logger.info(f"Subtensor chain endpoint: {self.subtensor_chain_endpoint}")
        
        # Log all environment variables (excluding sensitive data)
        safe_env = {k: v for k, v in os.environ.items() if not any(sensitive in k.lower() for sensitive in ['key', 'password', 'secret'])}
        self.logger.info(f"Environment variables: {safe_env}")
        
        # Log detailed config
        self.config = configuration.factory_config()
        self.logger.info(f"Detailed config:")
        for key, value in vars(self.config).items():
            self.logger.info(f"  {key}: {value}")

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
            app.include_router(factory_router())
            
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
            
            # Add middleware for detailed request logging
            @app.middleware("http")
            async def log_requests(request, call_next):
                # Log request details
                request_body = None
                if request.method in ["POST", "PUT"]:
                    try:
                        request_body = await request.body()
                        self.logger.info(f"Request body: {request_body.decode()}")
                    except Exception as e:
                        self.logger.warning(f"Could not read request body: {e}")

                self.logger.info(f"""
Request Details:
----------------
Method: {request.method}
URL: {request.url}
Client Host: {request.client.host if request.client else 'Unknown'}
Headers: {dict(request.headers)}
                """)
                
                # Process the request
                response = await call_next(request)
                
                # Log response details
                self.logger.info(f"""
Response Details:
----------------
Status: {response.status_code}
Headers: {dict(response.headers)}
                """)
                
                return response
            
            uvicorn.run(
                app,
                host="0.0.0.0",
                port=self.port,
                log_config=log_config,
                log_level="info",
                access_log=True,
                timeout_keep_alive=65,
                proxy_headers=True,
                forwarded_allow_ips="*"
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
