import os
import sys  # Add sys import
import traceback # Add traceback import
from dotenv import load_dotenv
import argparse
from fiber import SubstrateInterface
import uvicorn
from fiber.logging_utils import get_logger
from fiber.encrypted.miner import server
from fiber.encrypted.miner.core import configuration
from fiber.encrypted.miner.middleware import configure_extra_logging_middleware
from fiber.chain import chain_utils, fetch_nodes
from gaia.miner.utils.subnet import factory_router
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
import ssl
import logging
from fiber import logging_utils
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import asyncio
import ipaddress
from typing import Optional
import warnings

# Imports for Alembic check
from alembic.config import Config # Add Alembic import
from alembic import command # Add Alembic import
from alembic.util import CommandError # Add Alembic import

MAX_REQUEST_SIZE = 800 * 1024 * 1024  # 800MB

logger = get_logger(__name__)

os.environ["NODE_TYPE"] = "miner"


class Miner:
    """
    Miner class that sets up the neuron and processes tasks.
    """

    def __init__(self, args):
        self.args = args
        self.logger = get_logger(__name__)
        self.my_public_ip_str: Optional[str] = None
        self.my_public_port: Optional[int] = None
        self.my_public_protocol: Optional[str] = None
        self.my_public_base_url: Optional[str] = None

        # Load environment variables
        load_dotenv(".env")

        # Load wallet and network settings from args or env (CLI > ENV > Default)
        self.wallet = (
            args.wallet if args.wallet is not None else os.getenv("WALLET_NAME", "default")
        )
        self.hotkey = (
            args.hotkey if args.hotkey is not None else os.getenv("HOTKEY_NAME", "default")
        )
        self.netuid = (
            args.netuid if args.netuid is not None else int(os.getenv("NETUID", 237))
        )
        self.port = (
            args.port if args.port is not None else int(os.getenv("PORT", 8091))
        )
        
        # Load chain endpoint from args or env (CLI > ENV > Default)
        # Check if args.subtensor and args.subtensor.chain_endpoint exist and were provided via CLI
        cli_chain_endpoint = None
        if hasattr(args, "subtensor") and hasattr(args.subtensor, "chain_endpoint"):
             cli_chain_endpoint = args.subtensor.chain_endpoint # Will be None if not provided via CLI (due to default=None)

        self.subtensor_chain_endpoint = (
             cli_chain_endpoint if cli_chain_endpoint is not None
             else os.getenv("SUBTENSOR_ADDRESS", "wss://test.finney.opentensor.ai:443/")
        )

        # Load chain network from args or env (CLI > ENV > Default)
        cli_network = None
        if hasattr(args, "subtensor") and hasattr(args.subtensor, "network"):
            cli_network = args.subtensor.network # Will be None if not provided via CLI

        self.subtensor_network = (
             cli_network if cli_network is not None
             else os.getenv("SUBTENSOR_NETWORK", "test")
        )

        logger.info(f"Subtensor network: {self.subtensor_network}")
        logger.info(f"Subtensor chain endpoint: {self.subtensor_chain_endpoint}")
        logger.info(f"Subtensor netuid: {self.netuid}")
        logger.info(f"Subtensor port: {self.port}")
        logger.info(f" Wallet Name: {self.wallet}")
        logger.info(f" Hotkey Name: {self.hotkey}")

        self.database_manager = MinerDatabaseManager()
        self.geomagnetic_task = GeomagneticTask(
            node_type="miner",
            db_manager=self.database_manager
        )
        self.soil_task = SoilMoistureTask(
            db_manager=self.database_manager,
            node_type="miner"
        )
        self.weather_task = WeatherTask(
            db_manager=self.database_manager,
            node_type="miner"
        )
    

    def setup_neuron(self) -> bool:
        """
        Set up the miner neuron with necessary configurations and connections.
        """
        self.logger.info("Setting up miner neuron...")
        try: 
            self.keypair = chain_utils.load_hotkey_keypair(self.wallet, self.hotkey)
            
            config = configuration.factory_config() 
            config.keypair = self.keypair
            config.min_stake_threshold = float(os.getenv("MIN_STAKE_THRESHOLD", 5))
            config.netuid = self.netuid
            config.subtensor_network = self.subtensor_network 
            config.chain_endpoint = self.subtensor_chain_endpoint
            self.config = config
            
            if hasattr(self.weather_task, 'config') and self.weather_task.config is not None:
                self.weather_task.config['netuid'] = self.netuid
                self.weather_task.config['chain_endpoint'] = self.subtensor_chain_endpoint
                if 'miner_public_base_url' not in self.weather_task.config:
                     self.weather_task.config['miner_public_base_url'] = None
            else:
                self.weather_task.config = {
                    'netuid': self.netuid,
                    'chain_endpoint': self.subtensor_chain_endpoint,
                    'miner_public_base_url': None
                }
            self.weather_task.keypair = self.keypair

            self.logger.debug(
                f"""
    Detailed Neuron Configuration Initialized:
    ----------------------------
    Wallet Path: {self.wallet}
    Hotkey Path: {self.hotkey}
    Keypair SS58 Address: {self.keypair.ss58_address}
    Keypair Public Key: {self.keypair.public_key}
    Subtensor Chain Endpoint: {self.subtensor_chain_endpoint}
    Network: {self.subtensor_network}
    FastAPI Port (serving on): {self.port}
    Target Netuid: {self.netuid}
            """
            )

            substrate_for_check = None
            try:
                self.logger.info("MINER_SELF_CHECK: Attempting to fetch own registered axon info...")
                endpoint_to_use = self.config.chain_endpoint 
                
                substrate_for_check = SubstrateInterface(url=endpoint_to_use)
                
                all_nodes = fetch_nodes.get_nodes_for_netuid(substrate_for_check, self.netuid)
                found_own_node = None
                for node_info_from_list in all_nodes:
                    if node_info_from_list.hotkey == self.keypair.ss58_address:
                        found_own_node = node_info_from_list
                        break
                
                if found_own_node:
                    self.logger.info(f"MINER_SELF_CHECK: Found own node info in metagraph. Raw IP from node object: '{found_own_node.ip}', Port: {found_own_node.port}, Protocol: {found_own_node.protocol}")
                    ip_to_convert = found_own_node.ip
                    try:
                        if isinstance(ip_to_convert, str) and ip_to_convert.isdigit():
                            self.my_public_ip_str = str(ipaddress.ip_address(int(ip_to_convert)))
                        elif isinstance(ip_to_convert, int):
                             self.my_public_ip_str = str(ipaddress.ip_address(ip_to_convert))
                        else: 
                             ipaddress.ip_address(ip_to_convert)
                             self.my_public_ip_str = ip_to_convert 
                        
                        self.my_public_port = int(found_own_node.port)
                        self.my_public_protocol = "https" if int(found_own_node.protocol) == 4 else "http"
                        if int(found_own_node.protocol) not in [3,4]:
                             self.logger.warning(f"MINER_SELF_CHECK: Registered axon protocol is {found_own_node.protocol} (int: {int(found_own_node.protocol)}), not HTTP(3) or HTTPS(4). Using '{self.my_public_protocol}'.")

                        self.my_public_base_url = f"{self.my_public_protocol}://{self.my_public_ip_str}:{self.my_public_port}"
                        
                        self.logger.info(f"MINER_SELF_CHECK: Stored Public IP: {self.my_public_ip_str}")
                        self.logger.info(f"MINER_SELF_CHECK: Stored Public Port: {self.my_public_port}")
                        self.logger.info(f"MINER_SELF_CHECK: Stored Public Protocol: {self.my_public_protocol}")
                        self.logger.info(f"MINER_SELF_CHECK: Stored Public Base URL: {self.my_public_base_url}")
                        
                        if hasattr(self.weather_task, 'config') and self.weather_task.config is not None:
                            self.weather_task.config['miner_public_base_url'] = self.my_public_base_url
                            self.logger.info(f"MINER_SELF_CHECK: Updated WeatherTask config with miner_public_base_url: {self.my_public_base_url}")
                        else:
                            self.logger.warning("MINER_SELF_CHECK: WeatherTask.config not found or is None, cannot set miner_public_base_url.")

                    except ValueError as e_ip_conv:
                        self.logger.error(f"MINER_SELF_CHECK: Could not convert/validate IP '{ip_to_convert}' to standard string: {e_ip_conv}. Public URL not set.")
                    except Exception as e_node_parse: 
                        self.logger.error(f"MINER_SELF_CHECK: Error parsing axon details from node_info: {e_node_parse}", exc_info=False)
                else:
                    self.logger.warning(f"MINER_SELF_CHECK: Hotkey {self.keypair.ss58_address} not found in metagraph. Public URL not set.")
            except Exception as e_check:
                self.logger.error(f"MINER_SELF_CHECK: Error fetching own axon info: {e_check}", exc_info=False) 
            finally:
                if substrate_for_check:
                    substrate_for_check.close()
                    self.logger.debug("MINER_SELF_CHECK: Substrate connection for self-check closed.")

            return True
        except Exception as e_outer_setup: 
            self.logger.error(f"Outer error in setup_neuron: {e_outer_setup}", exc_info=True)
            return False

    def run(self):
        """
        Run the miner application with a FastAPI server.
        """
        try:
            if not self.setup_neuron():
                self.logger.error("Failed to setup neuron, exiting...")
                return
            
            if self.my_public_base_url:
                self.logger.info(f"Miner public base URL for Kerchunk determined as: {self.my_public_base_url}")
            else:
                self.logger.warning("Miner public base URL could not be determined. Kerchunk JSONs may use relative paths.")

            self.logger.info("Starting miner server...")
            
            app = server.factory_app(debug=True)
            app.body_limit = MAX_REQUEST_SIZE

            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*on_event is deprecated.*")
                
                @app.on_event("startup")
                async def startup_event():
                    self.logger.info("Initializing database on FastAPI startup...")
                    try:
                        await self.database_manager.ensure_engine_initialized()
                        await self.database_manager.initialize_database()
                        self.logger.info("Database initialization completed successfully")
                    except Exception as e:
                        self.logger.error(f"Failed to initialize database during startup: {e}", exc_info=True)
                        import sys
                        sys.exit(1)

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
                log_level="debug",
            )
        except Exception as e:
            self.logger.error(f"Error starting miner: {e}")
            self.logger.error(traceback.format_exc())
            raise e


if __name__ == "__main__":
    # Load environment variables *before* anything else (like Alembic check)
    load_dotenv(".env")

    # --- Alembic check code ---
    # Copied from validator.py
    try:
        # Use print here as logger might not be fully setup yet
        print("[Startup] Checking database schema version using Alembic...")
        # Construct path relative to this script file to find alembic.ini at project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, "..", "..")) # Assumes script is in gaia/miner/
        alembic_ini_path = os.path.join(project_root, "alembic.ini")

        if not os.path.exists(alembic_ini_path):
            print(f"[Startup] ERROR: alembic.ini not found at expected path: {alembic_ini_path}")
            sys.exit("Alembic configuration not found.")

        alembic_cfg = Config(alembic_ini_path)
        # Ensure alembic.ini is accessible from the miner's execution context
        # If miner runs from a different directory, adjust the path to alembic.ini
        command.upgrade(alembic_cfg, "head")
        print("[Startup] Database schema is up-to-date.")
    except CommandError as e:
         # Use print here as well
         print(f"[Startup] ERROR: Alembic command failed during startup check: {e}")
         print("[Startup] ERROR: Please ensure the database is accessible and migrations are correct.")
         sys.exit("Database migration/check failed.") # Exit directly
    except Exception as e:
        print(f"[Startup] ERROR: Failed to check/upgrade database schema during startup: {e}")
        traceback.print_exc() # Print full traceback for unexpected errors
        sys.exit("Database schema check failed.")
    # --- End Alembic check ---

    logger.info("Miner Alembic check complete. Starting main miner application...") # Keep this log message after the check
    
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Start the miner with optional flags.")
    
    # Wallet and network arguments
    parser.add_argument("--wallet", type=str, default=None, help="Name of the wallet to use (overrides WALLET_NAME env var)")
    parser.add_argument("--hotkey", type=str, default=None, help="Name of the hotkey to use (overrides HOTKEY_NAME env var)")
    parser.add_argument("--netuid", type=int, default=None, help="Netuid to use (overrides NETUID env var)")

    # Optional arguments
    parser.add_argument("--port", type=int, default=None, help="Port to run the miner on (overrides PORT env var)")
    parser.add_argument("--use_base_model", action="store_true", help="Enable base model usage")

    # Create a subtensor group
    subtensor_group = parser.add_argument_group("subtensor")
    # Subtensor arguments
    subtensor_group.add_argument("--subtensor.chain_endpoint", type=str, default=None, help="Subtensor chain endpoint to use (overrides SUBTENSOR_ADDRESS env var)")
    subtensor_group.add_argument("--subtensor.network", type=str, default=None, help="Subtensor network to use (overrides SUBTENSOR_NETWORK env var)")

    # Parse arguments
    args = parser.parse_args()

    # Instantiate and run the miner
    miner = Miner(args)

    try:
        miner.run() # Assuming miner.run() contains the main logic now, replacing placeholder
    except KeyboardInterrupt:
        logger.info("Miner application interrupted. Shutting down...")
    except Exception as e:
        logger.critical(f"Unhandled exception in miner main application: {e}", exc_info=True)
    finally:
        logger.info("Miner application has finished.")

