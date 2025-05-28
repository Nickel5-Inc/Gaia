import os
import sys  # Add sys import
import traceback # Add traceback import
import subprocess # For managing inference server subprocess
import atexit # To ensure inference server is stopped
import time # For health check delays
import httpx # For health checking inference server
from urllib.parse import urlparse # To parse service URL
from dotenv import load_dotenv
import argparse
from fiber import SubstrateInterface
import uvicorn
from fiber.logging_utils import get_logger
from fiber.encrypted.miner import server as fiber_server
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
from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
import asyncio
import ipaddress
from typing import Optional
import warnings
from contextlib import asynccontextmanager # Add this import

# Imports for Alembic check
from alembic.config import Config # Add Alembic import
from alembic import command # Add Alembic import
from alembic.util import CommandError # Add Alembic import

MAX_REQUEST_SIZE = 800 * 1024 * 1024  # 800MB

logger = get_logger(__name__)

os.environ["NODE_TYPE"] = "miner"

# --- Globals for Inference Service Management ---
inference_server_process: Optional[subprocess.Popen] = None
INFERENCE_SERVICE_READY_TIMEOUT = 60  # seconds to wait for inference service
INFERENCE_SERVICE_HEALTH_CHECK_INTERVAL = 3  # seconds between health checks

# --- Helper Functions for Inference Service Management ---
def _is_local_url(service_url: str) -> bool:
    if not service_url:
        return False
    try:
        parsed_url = urlparse(service_url)
        hostname = parsed_url.hostname
        return hostname in ["localhost", "127.0.0.1"]
    except Exception as e:
        logger.error(f"Error parsing WEATHER_INFERENCE_SERVICE_URL '{service_url}': {e}")
        return False

def _get_port_from_url(service_url: str, default_port: int = 8000) -> int:
    try:
        parsed_url = urlparse(service_url)
        port = parsed_url.port
        if port:
            return port
        # If URL is like http://localhost/run_inference (no port), use default for local
        if parsed_url.hostname in ["localhost", "127.0.0.1"] and not port:
            logger.warning(f"No port in local URL '{service_url}', defaulting to {default_port}.")
            return default_port
        elif not port: # Remote URL with no port, this is unusual for http
            logger.error(f"No port found in remote URL '{service_url}' and no default specified for remote. Returning default {default_port} but this may be incorrect.")
            return default_port # Or raise error
        return default_port # Should be caught by if port: above

    except Exception as e:
        logger.error(f"Error extracting port from URL '{service_url}': {e}. Defaulting to {default_port}.")
        return default_port


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
        self.weather_inference_service_url: Optional[str] = None # Added for inference service URL
        self.weather_task = None # Will be initialized in lifespan
        self.weather_runpod_api_key = None

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
            args.port if args.port is not None else int(os.getenv("PORT", 33334))
        )
        self.public_port = (
            args.public_port if args.public_port is not None else int(os.getenv("PUBLIC_PORT", 33333))
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
        logger.info(f"Public port: {self.public_port}")
        logger.info(f"Wallet Name: {self.wallet}")
        logger.info(f"Hotkey Name: {self.hotkey}")

        self.database_manager = MinerDatabaseManager()
        self.geomagnetic_task = GeomagneticTask(
            node_type="miner",
            db_manager=self.database_manager
        )
        self.soil_task = SoilMoistureTask(
            db_manager=self.database_manager,
            node_type="miner"
        )
        
        weather_enabled_env_val = os.getenv("WEATHER_MINER_ENABLED", "false")
        weather_enabled = weather_enabled_env_val.lower() in ["true", "1", "yes"]
        
        self.weather_inference_service_url = os.getenv("WEATHER_INFERENCE_SERVICE_URL")
        runpod_api_key_from_env = os.getenv("CREDENTIAL")
        if runpod_api_key_from_env:
            self.weather_runpod_api_key = runpod_api_key_from_env
            logger.info(f"RunPod API Key loaded from CREDENTIAL env var in __init__.")

        if weather_enabled:
            logger.info("Weather task ENABLED by WEATHER_MINER_ENABLED. Initializing in __init__.")
            weather_task_args = {
                "db_manager": self.database_manager,
                "node_type": "miner",
                "inference_service_url": self.weather_inference_service_url
            }
            if self.weather_runpod_api_key:
                weather_task_args["runpod_api_key"] = self.weather_runpod_api_key
            
            self.weather_task = WeatherTask(**weather_task_args)
            logger.info("WeatherTask basic initialization completed in __init__.")
        else:
            logger.info("Weather task DISABLED by WEATHER_MINER_ENABLED. self.weather_task remains None.")
            self.weather_task = None
    

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
            
            if self.weather_task is not None:
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
    Public Port (posted to chain): {self.public_port}
    Internal Port (for FastAPI): {self.port}
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
                        
                        if self.weather_task is not None:
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
            
            # --- Inference Service Management Functions (modified for Docker context) ---
            async def check_local_inference_service_readiness(service_url: str) -> bool:
                health_url = service_url.replace("/run_inference", "/health") # Construct health URL
                logger.info(f"Checking readiness of local inference service at {health_url}...")
                start_time = time.monotonic()
                while time.monotonic() - start_time < INFERENCE_SERVICE_READY_TIMEOUT:
                    try:
                        # Synchronous HTTP GET for simplicity in this startup phase
                        response = httpx.get(health_url, timeout=5) 
                        if response.status_code == 200:
                            health_data = response.json()
                            model_status = health_data.get("model_status", "unknown")
                            logger.info(f"Local inference service is healthy. Status: {response.status_code}, Model Status: {model_status}")
                            if "ready" in model_status or "loaded" in model_status: # Check for positive model status
                                return True
                            else:
                                logger.warning(f"Inference service at {health_url} is up, but model status is '{model_status}'. Retrying...")
                        else:
                            logger.warning(f"Local inference service at {health_url} not ready (Status: {response.status_code}). Retrying...")
                    except httpx.RequestError as e:
                        logger.warning(f"Error connecting to local inference service at {health_url}: {e}. Retrying...")
                    
                    await asyncio.sleep(INFERENCE_SERVICE_HEALTH_CHECK_INTERVAL) # Use asyncio.sleep in async context
                
                logger.error(f"Local inference service at {health_url} did not become ready within {INFERENCE_SERVICE_READY_TIMEOUT} seconds.")
                return False

            app = fiber_server.factory_app(debug=True)
            app.body_limit = MAX_REQUEST_SIZE

            @app.on_event("startup")
            async def overridden_startup_event():
                nonlocal self # Ensure self is captured if methods like self.database_manager are used
                self.logger.info("Initializing database on application startup (via overridden on_event)...")
                try:
                    await self.database_manager.ensure_engine_initialized()
                    await self.database_manager.initialize_database()
                    self.logger.info("Database initialization completed successfully")
                except Exception as e_db:
                    self.logger.error(f"Failed to initialize database during startup: {e_db}", exc_info=True)
                    # Depending on severity, you might want to sys.exit(1) here
                
                # Weather Inference Service Setup
                weather_enabled_env_val = os.getenv("WEATHER_MINER_ENABLED", "false") 
                self.logger.info(f"DEBUG_WEATHER_ENABLED: Value of WEATHER_MINER_ENABLED from os.getenv: '{weather_enabled_env_val}'")
                weather_enabled_env = weather_enabled_env_val.lower() in ["true", "1", "yes"]
                

                if weather_enabled_env:
                    if self.weather_task is None:
                        self.logger.warning("WeatherTask was None at startup despite WEATHER_MINER_ENABLED being true. Re-initializing. This might indicate an issue if __init__ didn't set it.")
                        weather_task_args = {
                            "db_manager": self.database_manager,
                            "node_type": "miner",
                            "inference_service_url": self.weather_inference_service_url
                        }
                        if self.weather_runpod_api_key:
                            weather_task_args["runpod_api_key"] = self.weather_runpod_api_key
                        self.weather_task = WeatherTask(**weather_task_args)

                    if self.weather_task:
                        weather_inference_type = os.getenv("WEATHER_INFERENCE_TYPE", "local_model").lower()
                        service_url_env = self.weather_inference_service_url

                        if weather_inference_type == "http_service":
                            if not service_url_env:
                                self.logger.error("WEATHER_INFERENCE_TYPE is 'http_service' but WEATHER_INFERENCE_SERVICE_URL is not set. Weather task cannot use inference service.")
                            else:
                                self.logger.info(f"HTTP inference service configured. URL: {self.weather_inference_service_url}")

                                if _is_local_url(service_url_env):
                                    self.logger.info(f"Local inference service URL configured: {service_url_env}. Checking readiness...")
                                    service_ready = await check_local_inference_service_readiness(service_url_env)
                                    if service_ready:
                                        self.logger.info(f"Local inference service is ready.")
                                    else:
                                        self.logger.error(f"Local inference service at {service_url_env} failed to become ready. Weather task may not function correctly.")
                                else:
                                    self.logger.info(f"Remote HTTP inference service URL: {self.weather_inference_service_url}. No local readiness check performed.")
                        elif weather_inference_type == "local_model":
                            self.logger.info(f"Weather inference type is '{weather_inference_type}'. WeatherTask will use its internal model logic.")
                        else: 
                            self.logger.warning(f"Unhandled WEATHER_INFERENCE_TYPE: '{weather_inference_type}'. Defaulting to local model behavior if applicable, or no remote service.")

                        if self.config:
                            if hasattr(self.weather_task, 'config') and self.weather_task.config is not None:
                                self.weather_task.config['netuid'] = self.config.netuid
                                self.weather_task.config['chain_endpoint'] = self.config.chain_endpoint
                                if 'miner_public_base_url' not in self.weather_task.config:
                                    self.weather_task.config['miner_public_base_url'] = self.my_public_base_url
                            else:
                                self.weather_task.config = {
                                    'netuid': self.config.netuid,
                                    'chain_endpoint': self.config.chain_endpoint,
                                    'miner_public_base_url': self.my_public_base_url
                                }
                            self.weather_task.keypair = self.keypair
                            self.logger.info("WeatherTask re-configured with neuron details during startup.")
                        else:
                            self.logger.warning("Miner self.config not found during startup, WeatherTask config might be incomplete.")
                    else:
                        self.logger.error("WeatherTask is still None during startup even though weather_enabled_env is true. This is unexpected.")
                else:
                    self.logger.info("Weather task is disabled (checked in startup event). self.weather_task should be None.")

                yield
                self.logger.info("Application shutting down...")
            
            

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
            logger.info(f"port for fastapi: {self.port}")

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
    load_dotenv(".env", override=True)

    # --- Alembic check code ---
    # No need to set DB_TARGET since we have separate configurations now
    logger.info(f"[Startup] Using miner-specific Alembic configuration")

    try:
        # Use print here as logger might not be fully setup yet
        print("[Startup] Checking database schema version using Alembic...")
        # Construct path relative to this script file to find alembic_miner.ini at project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, "..", "..")) # Assumes script is in gaia/miner/
        alembic_ini_path = os.path.join(project_root, "alembic_miner.ini")

        if not os.path.exists(alembic_ini_path):
            print(f"[Startup] ERROR: alembic_miner.ini not found at expected path: {alembic_ini_path}")
            sys.exit("Alembic miner configuration not found.")

        alembic_cfg = Config(alembic_ini_path)
        print(f"[Startup] Miner: Using Alembic configuration from: {alembic_ini_path}")
        
        # --- Diagnostic Block ---
        try:
            from alembic.script import ScriptDirectory
            script_dir_instance = ScriptDirectory.from_config(alembic_cfg)
            
            print(f"[Startup] DIAGNOSTIC: ScriptDirectory main path: {script_dir_instance.dir}")
            print(f"[Startup] DIAGNOSTIC: ScriptDirectory version_locations: {script_dir_instance.version_locations}")
        except Exception as e_diag:
            print(f"[Startup] DIAGNOSTIC: Error during ScriptDirectory check: {e_diag}")
        # --- End Diagnostic Block ---

        alembic_auto_upgrade = os.getenv("ALEMBIC_AUTO_UPGRADE", "True").lower() in ["true", "1", "yes"]
        if alembic_auto_upgrade:
            print(f"[Startup] ALEMBIC_AUTO_UPGRADE is True. Attempting to upgrade database schema to head...")
            command.upgrade(alembic_cfg, "head")
            print("[Startup] Database schema is up-to-date (or upgrade attempted).")
        else:
            print("[Startup] ALEMBIC_AUTO_UPGRADE is False. Skipping automatic schema upgrade.")

    except CommandError as e:
         # Use print here as well
         print(f"[Startup] ERROR: Alembic command failed during startup check: {e}")

    logger.info("Miner Alembic check complete. Starting main miner application...")
    
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Start the miner with optional flags.")
    
    # Wallet and network arguments
    parser.add_argument("--wallet", type=str, default=None, help="Name of the wallet to use (overrides WALLET_NAME env var)")
    parser.add_argument("--hotkey", type=str, default=None, help="Name of the hotkey to use (overrides HOTKEY_NAME env var)")
    parser.add_argument("--netuid", type=int, default=None, help="Netuid to use (overrides NETUID env var)")

    # Optional arguments
    parser.add_argument("--port", type=int, default=None, help="Port to run the miner on (overrides PORT env var)")
    parser.add_argument("--public-port", type=int, default=None, help="Public port to announce to the chain (overrides PUBLIC_PORT env var)")
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

