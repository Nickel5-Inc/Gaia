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
# Removed asynccontextmanager import - using traditional startup/shutdown events
import time

# Imports for Alembic check
from alembic.config import Config # Add Alembic import
from alembic import command # Add Alembic import
from alembic.util import CommandError # Add Alembic import

MAX_REQUEST_SIZE = 100 * 1024 * 1024  # Reduced from 800MB to 100MB

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
        
        # Memory monitoring for the entire miner process
        self.memory_monitor_task = None
        self.memory_monitor_enabled = os.getenv('MINER_MEMORY_MONITORING_ENABLED', 'true').lower() in ['true', '1', 'yes']
        self.pm2_restart_enabled = os.getenv('MINER_PM2_RESTART_ENABLED', 'true').lower() in ['true', '1', 'yes']

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
        
        # --- Initialize WeatherTask in __init__ ---
        self.weather_inference_service_url = None # Will be determined here
        self.weather_runpod_api_key = None      # Will be determined here
        self.weather_task = None                # Initialize to None

        weather_enabled_env_val = os.getenv("WEATHER_MINER_ENABLED", "false")
        weather_enabled = weather_enabled_env_val.lower() in ["true", "1", "yes"]
        
        # Corrected logging for WEATHER_MINER_ENABLED
        weather_enabled_env_val_for_log = os.getenv("WEATHER_MINER_ENABLED")
        logger.info(f"DEBUG_WEATHER_ENABLED in __init__: Value from os.getenv: '{weather_enabled_env_val_for_log}', Evaluated to: {weather_enabled}")

        
        self.weather_inference_service_url = os.getenv("WEATHER_INFERENCE_SERVICE_URL")
        runpod_api_key_from_env = os.getenv("INFERENCE_SERVICE_API_KEY")
        if runpod_api_key_from_env:
            self.weather_runpod_api_key = runpod_api_key_from_env
            logger.info("RunPod API Key loaded from INFERENCE_SERVICE_API_KEY env var in __init__.")
        else:
            runpod_api_key_from_env = os.getenv("WEATHER_RUNPOD_API_KEY")
            if runpod_api_key_from_env:
                self.weather_runpod_api_key = runpod_api_key_from_env
                logger.info("RunPod API Key loaded from WEATHER_RUNPOD_API_KEY env var in __init__.")
            else:
                logger.info("No RunPod API Key found (checked INFERENCE_SERVICE_API_KEY, WEATHER_RUNPOD_API_KEY) in __init__.")

        if weather_enabled:
            logger.info("Weather task IS ENABLED based on WEATHER_MINER_ENABLED in __init__.")
            
            weather_inference_type = os.getenv("WEATHER_INFERENCE_TYPE", "local_model").lower()
            service_url_env = os.getenv("WEATHER_INFERENCE_SERVICE_URL")

            if weather_inference_type == "http_service":
                if not service_url_env:
                    logger.error("WEATHER_INFERENCE_TYPE is 'http_service' but WEATHER_INFERENCE_SERVICE_URL is not set. Weather task cannot use inference service.")
                    # self.weather_task remains None
                else:
                    self.weather_inference_service_url = service_url_env
                    logger.info(f"HTTP inference service configured in __init__. URL: {self.weather_inference_service_url}")
                    
                    runpod_api_key_from_env = os.getenv("INFERENCE_SERVICE_API_KEY")
                    if runpod_api_key_from_env:
                        self.weather_runpod_api_key = runpod_api_key_from_env
                        logger.info("RunPod API Key loaded from INFERENCE_SERVICE_API_KEY env var in __init__.")
                    else:
                        runpod_api_key_from_env = os.getenv("WEATHER_RUNPOD_API_KEY")
                        if runpod_api_key_from_env:
                            self.weather_runpod_api_key = runpod_api_key_from_env
                            logger.info("RunPod API Key loaded from WEATHER_RUNPOD_API_KEY env var in __init__.")
                        else:
                            logger.info("No RunPod API Key found (checked INFERENCE_SERVICE_API_KEY, WEATHER_RUNPOD_API_KEY) in __init__.")
            
            elif weather_inference_type == "local_model":
                logger.info(f"Weather inference type is '{weather_inference_type}' in __init__. WeatherTask will use internal model logic.")
            else:
                logger.warning(f"Unhandled WEATHER_INFERENCE_TYPE: '{weather_inference_type}' in __init__.")

            # Now, instantiate WeatherTask if it's still considered viable
            weather_task_args = {
                "db_manager": self.database_manager,
                "node_type": "miner",
                "inference_service_url": self.weather_inference_service_url
            }
            if self.weather_runpod_api_key:
                weather_task_args["runpod_api_key"] = self.weather_runpod_api_key
            
            # Load R2 configuration from environment variables
            r2_config = {}
            r2_endpoint_url = os.getenv("R2_ENDPOINT_URL")
            r2_access_key_id = os.getenv("R2_ACCESS_KEY")
            r2_secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY")
            r2_bucket_name = os.getenv("R2_BUCKET")
            
            if all([r2_endpoint_url, r2_access_key_id, r2_secret_access_key, r2_bucket_name]):
                r2_config = {
                    "r2_endpoint_url": r2_endpoint_url,
                    "r2_access_key_id": r2_access_key_id,
                    "r2_secret_access_key": r2_secret_access_key,
                    "r2_bucket_name": r2_bucket_name
                }
                weather_task_args["r2_config"] = r2_config
                self.logger.info("Loaded complete R2 configuration from environment variables during startup.")
            else:
                self.logger.warning("Incomplete R2 configuration found during startup. WeatherTask will proceed without R2 upload capabilities.")
            
            try:
                self.weather_task = WeatherTask(**weather_task_args)
                logger.info("WeatherTask INSTANTIATED in Miner.__init__.")
            except Exception as e_wt_init:
                logger.error(f"Failed to instantiate WeatherTask in Miner.__init__: {e_wt_init}", exc_info=True)
                self.weather_task = None # Ensure it's None on failure
        else:
            logger.info("Weather task IS DISABLED based on WEATHER_MINER_ENABLED in __init__. self.weather_task is None.")
            self.weather_task = None # Explicitly None
        # --- End WeatherTask Initialization in __init__ ---
    

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
            
            if self.weather_task is not None: # Check if weather_task was initialized
                if hasattr(self.weather_task, 'config') and self.weather_task.config is not None:
                    self.weather_task.config['netuid'] = self.netuid
                    self.weather_task.config['chain_endpoint'] = self.subtensor_chain_endpoint
                    if 'miner_public_base_url' not in self.weather_task.config:
                         self.weather_task.config['miner_public_base_url'] = None # Will be set by self-check if possible
                    self.weather_task.keypair = self.keypair
                    logger.info("Miner.setup_neuron: Applied neuron config to existing WeatherTask.")
                else:
                    self.weather_task.config = {
                        'netuid': self.netuid,
                        'chain_endpoint': self.subtensor_chain_endpoint,
                        'miner_public_base_url': None # Will be set by self-check if possible
                    }
                    self.weather_task.keypair = self.keypair
                    logger.info("Miner.setup_neuron: WeatherTask is None, skipping neuron config for it.")

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
                
                # DEBUG: Log all parameters being used for self-check
                self.logger.info("=" * 80)
                self.logger.info("🔍 MINER_SELF_CHECK DEBUG: Parameters being used")
                self.logger.info("=" * 80)
                self.logger.info(f"🔑 Hotkey (self.keypair.ss58_address): '{self.keypair.ss58_address}'")
                self.logger.info(f"📏 Hotkey length: {len(self.keypair.ss58_address)} characters")
                self.logger.info(f"🌐 NetUID (self.netuid): {self.netuid}")
                self.logger.info(f"🔗 Chain endpoint (self.config.chain_endpoint): '{endpoint_to_use}'")
                self.logger.info(f"🧪 Hotkey format check:")
                self.logger.info(f"   - Starts with: '{self.keypair.ss58_address[:10]}...'")
                self.logger.info(f"   - Ends with: '...{self.keypair.ss58_address[-10:]}'")
                self.logger.info(f"   - Has whitespace? {bool(self.keypair.ss58_address.strip() != self.keypair.ss58_address)}")
                self.logger.info("=" * 80)
                
                substrate_for_check = SubstrateInterface(url=endpoint_to_use)
                self.logger.info("✅ MINER_SELF_CHECK: Substrate connection established successfully")
                
                all_nodes = fetch_nodes.get_nodes_for_netuid(substrate_for_check, self.netuid)
                self.logger.info(f"📋 MINER_SELF_CHECK: Retrieved {len(all_nodes)} nodes from metagraph")
                
                # DEBUG: Show sample of nodes for comparison
                if all_nodes:
                    self.logger.info("🔍 MINER_SELF_CHECK: Sample nodes from metagraph (first 3):")
                    for i, node in enumerate(all_nodes[:3]):
                        self.logger.info(f"   [{i}] Hotkey: '{node.hotkey}' (len: {len(node.hotkey)})")
                        self.logger.info(f"       IP: {node.ip}, Port: {node.port}, Protocol: {node.protocol}")
                        # Character-by-character comparison with our hotkey
                        if len(node.hotkey) == len(self.keypair.ss58_address):
                            diff_count = sum(1 for a, b in zip(node.hotkey, self.keypair.ss58_address) if a != b)
                            self.logger.info(f"       Character differences with our hotkey: {diff_count}")
                    
                    if len(all_nodes) > 3:
                        self.logger.info(f"   ... and {len(all_nodes) - 3} more nodes")
                else:
                    self.logger.error("❌ MINER_SELF_CHECK: No nodes found in metagraph!")
                    self.logger.warning(f"MINER_SELF_CHECK: Hotkey {self.keypair.ss58_address} not found in metagraph. Public URL not set.")
                    return True
                found_own_node = None
                self.logger.info(f"🔍 MINER_SELF_CHECK: Searching for exact hotkey match among {len(all_nodes)} nodes...")
                exact_matches = 0
                partial_matches = []
                
                for node_info_from_list in all_nodes:
                    if node_info_from_list.hotkey == self.keypair.ss58_address:
                        found_own_node = node_info_from_list
                        exact_matches += 1
                        self.logger.info(f"✅ MINER_SELF_CHECK: EXACT MATCH FOUND! Node hotkey: '{node_info_from_list.hotkey}'")
                        break
                    # Check for partial matches to help debug
                    elif node_info_from_list.hotkey.strip() == self.keypair.ss58_address.strip():
                        partial_matches.append(("whitespace_difference", node_info_from_list.hotkey))
                    elif len(node_info_from_list.hotkey) == len(self.keypair.ss58_address):
                        diff_count = sum(1 for a, b in zip(node_info_from_list.hotkey, self.keypair.ss58_address) if a != b)
                        if diff_count <= 3:  # Very close match
                            partial_matches.append((f"{diff_count}_char_difference", node_info_from_list.hotkey))
                
                self.logger.info(f"🔍 MINER_SELF_CHECK: Search complete - Exact matches: {exact_matches}")
                
                if not found_own_node and partial_matches:
                    self.logger.warning(f"⚠️ MINER_SELF_CHECK: No exact match found, but found {len(partial_matches)} partial matches:")
                    for match_type, hotkey in partial_matches[:5]:  # Show first 5 partial matches
                        self.logger.warning(f"   - {match_type}: '{hotkey}'")
                elif not found_own_node:
                    self.logger.error(f"❌ MINER_SELF_CHECK: No exact or partial matches found among {len(all_nodes)} nodes")
                    # Show first few hotkeys for debugging
                    self.logger.info("🔍 MINER_SELF_CHECK: First 5 hotkeys in metagraph for comparison:")
                    for i, node in enumerate(all_nodes[:5]):
                        self.logger.info(f"   [{i}] '{node.hotkey}'")
                
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
                                self.logger.info("MINER_SELF_CHECK: WeatherTask is None or WeatherTask.config is None, cannot set miner_public_base_url for it.") # Modified log

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

            # Start memory monitoring in a background thread
            self.logger.info("🔍 Starting miner memory monitoring in background...")
            self._start_memory_monitoring_thread()

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

            # Add required imports for middleware
            from collections import defaultdict
            
            # Simple in-memory rate limiter with proper cleanup
            request_counts = defaultdict(list)
            
            async def cleanup_rate_limiter():
                """Periodic cleanup of rate limiter to prevent memory leaks"""
                while True:
                    try:
                        await asyncio.sleep(300)  # Clean every 5 minutes
                        current_time = time.time()
                        
                        # Clean old requests and remove empty IP entries
                        empty_ips = []
                        for client_ip, requests in request_counts.items():
                            # Remove old requests
                            request_counts[client_ip] = [
                                req_time for req_time in requests 
                                if current_time - req_time < 60
                            ]
                            # Mark empty IPs for removal
                            if not request_counts[client_ip]:
                                empty_ips.append(client_ip)
                        
                        # Remove empty IP entries to prevent memory accumulation
                        for ip in empty_ips:
                            del request_counts[ip]
                        
                        if empty_ips:
                            self.logger.debug(f"Rate limiter cleanup: Removed {len(empty_ips)} inactive IP entries")
                        
                        # Log memory usage periodically
                        active_ips = len(request_counts)
                        if active_ips > 100:  # Log if many IPs are being tracked
                            self.logger.info(f"Rate limiter tracking {active_ips} IP addresses")
                            
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        self.logger.warning(f"Error in rate limiter cleanup: {e}")

            # Create FastAPI app using fiber server factory
            app = fiber_server.factory_app(debug=True)
            app.body_limit = MAX_REQUEST_SIZE
            
            # Traditional startup event handler
            @app.on_event("startup")
            async def startup_event():
                self.logger.info("Initializing database on application startup...")
                try:
                    await self.database_manager.ensure_engine_initialized()
                    await self.database_manager.initialize_database()
                    self.logger.info("Database initialization completed successfully")
                except Exception as e_db:
                    self.logger.error(f"Failed to initialize database during startup: {e_db}", exc_info=True)
                
                # Weather Inference Service Setup
                weather_enabled_env_val = os.getenv("WEATHER_MINER_ENABLED", "false") 
                self.logger.info(f"DEBUG_WEATHER_ENABLED: Value of WEATHER_MINER_ENABLED from os.getenv: '{weather_enabled_env_val}'")
                weather_enabled_env = weather_enabled_env_val.lower() in ["true", "1", "yes"]
                
                if weather_enabled_env:
                    if self.weather_task is None:
                        self.logger.error("WeatherTask was None at startup despite WEATHER_MINER_ENABLED being true. This indicates a problem in __init__ - weather task should already exist!")
                        self.logger.error("Weather functionality will be disabled due to initialization failure")
                    
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
                            
                            # Start background workers for the WeatherTask
                            try:
                                await self.weather_task.start_background_workers()
                                self.logger.info("Started WeatherTask background workers for re-initialized WeatherTask.")
                            except Exception as worker_err:
                                self.logger.error(f"Failed to start WeatherTask background workers for re-initialized task: {worker_err}", exc_info=True)
                        else:
                            self.logger.warning("Miner self.config not found during startup, WeatherTask config might be incomplete.")
                    else:
                        self.logger.error("WeatherTask is still None during startup even though weather_enabled_env is true. This is unexpected.")
                else:
                    self.logger.info("Weather task is disabled (checked in startup event). self.weather_task should be None.")

                # Start rate limiter cleanup task (now that event loop is running)
                cleanup_task = asyncio.create_task(cleanup_rate_limiter())
                app.state.rate_limiter_cleanup = cleanup_task
                self.logger.info("Rate limiter cleanup task started")
                    
            @app.on_event("shutdown") 
            async def shutdown_event():
                self.logger.info("Application shutting down...")
                # Stop memory monitoring on shutdown
                await self.stop_memory_monitoring()
            
            @app.middleware("http")
            async def rate_limit_middleware(request, call_next):
                client_ip = request.client.host
                current_time = time.time()
                
                # Clean old requests for this IP (still needed for immediate cleanup)
                request_counts[client_ip] = [
                    req_time for req_time in request_counts[client_ip] 
                    if current_time - req_time < 60
                ]
                
                # Check rate limit (max 100 requests per minute per IP)
                max_requests_per_minute = int(os.getenv('MINER_RATE_LIMIT_PER_MINUTE', '100'))
                if len(request_counts[client_ip]) >= max_requests_per_minute:
                    self.logger.warning(f"Rate limit exceeded for IP {client_ip}: {len(request_counts[client_ip])} requests in last minute")
                    return JSONResponse(
                        status_code=429,
                        content={"detail": "Rate limit exceeded"}
                    )
                
                # Record this request
                request_counts[client_ip].append(current_time)
                
                response = await call_next(request)
                return response

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
                access_log=True,
                workers=1,  # Keep single worker for simplicity but explicitly set
                limit_concurrency=50,  # Limit concurrent connections
                limit_max_requests=1000,  # Restart worker after N requests to prevent memory leaks
                timeout_keep_alive=30,  # Keep alive timeout
                timeout_graceful_shutdown=30,  # Graceful shutdown timeout
            )
        except Exception as e:
            self.logger.error(f"Error starting miner: {e}")
            self.logger.error(traceback.format_exc())
            raise e

    def _start_memory_monitoring_thread(self):
        """Start memory monitoring in a separate thread to avoid event loop issues."""
        if not self.memory_monitor_enabled:
            self.logger.info("Miner memory monitoring disabled by configuration")
            return
            
        try:
            import threading
            import psutil
            system_memory = psutil.virtual_memory()
            self.logger.info(f"System memory: {system_memory.total / (1024**3):.1f} GB total, {system_memory.available / (1024**3):.1f} GB available")
            
            # Start monitoring in a daemon thread
            monitor_thread = threading.Thread(target=self._memory_monitor_sync_loop, daemon=True)
            monitor_thread.start()
            self.logger.info("✅ Miner memory monitoring started in background thread")
        except ImportError:
            self.logger.warning("psutil not available - memory monitoring disabled")
            self.memory_monitor_enabled = False
        except Exception as e:
            self.logger.error(f"Failed to start miner memory monitoring: {e}")

    def _memory_monitor_sync_loop(self):
        """Synchronous memory monitoring loop that runs in a separate thread."""
        try:
            import psutil
            import time
            import gc
            process = psutil.Process()
            last_log_time = 0
            
            # Configurable thresholds with defaults tuned for miner systems
            warning_threshold_mb = int(os.getenv('MINER_MEMORY_WARNING_THRESHOLD_MB', '8000'))  # 8GB
            emergency_threshold_mb = int(os.getenv('MINER_MEMORY_EMERGENCY_THRESHOLD_MB', '12000'))  # 12GB
            critical_threshold_mb = int(os.getenv('MINER_MEMORY_CRITICAL_THRESHOLD_MB', '14000'))  # 14GB
            
            self.logger.info(f"Memory monitoring thresholds: Warning={warning_threshold_mb}MB, Emergency={emergency_threshold_mb}MB, Critical={critical_threshold_mb}MB")
            self.logger.info(f"PM2 restart enabled: {self.pm2_restart_enabled}")
            if self.pm2_restart_enabled:
                pm2_id = os.getenv('pm2_id', 'not detected')
                self.logger.info(f"PM2 instance ID: {pm2_id}")
            
            while True:
                try:
                    memory_info = process.memory_info()
                    memory_mb = memory_info.rss / (1024 * 1024)
                    
                    system_memory = psutil.virtual_memory()
                    system_percent = system_memory.percent
                    
                    current_time = time.time()
                    
                    # Emergency circuit breakers with pm2 restart capability
                    if memory_mb > critical_threshold_mb:
                        self.logger.error(f"💀 CRITICAL MEMORY: {memory_mb:.1f} MB - OOM imminent! (threshold: {critical_threshold_mb} MB)")
                        self.logger.error("Attempting emergency garbage collection to prevent OOM kill...")
                        try:
                            collected = gc.collect()
                            self.logger.error(f"Emergency GC freed {collected} objects")
                            time.sleep(2)  # Brief pause after emergency GC
                            
                            # Check memory again after GC
                            post_gc_memory = process.memory_info().rss / (1024 * 1024)
                            if post_gc_memory > (critical_threshold_mb * 0.9):  # Still >90% of critical
                                if self.pm2_restart_enabled:
                                    self.logger.error(f"🔄 TRIGGERING PM2 RESTART: Memory still critical after GC ({post_gc_memory:.1f} MB)")
                                    self._trigger_pm2_restart_sync("Critical memory pressure after GC")
                                    return  # Exit the monitoring loop
                                else:
                                    self.logger.error(f"💀 MEMORY CRITICAL BUT PM2 RESTART DISABLED: {post_gc_memory:.1f} MB - system may be killed by OOM")
                            else:
                                self.logger.info(f"✅ Memory reduced to {post_gc_memory:.1f} MB after GC - continuing")
                        except Exception as gc_err:
                            self.logger.error(f"Emergency GC failed: {gc_err}")
                            # If GC fails, definitely restart (if enabled)
                            if self.pm2_restart_enabled:
                                self._trigger_pm2_restart_sync("Emergency GC failed and memory critical")
                                return
                            else:
                                self.logger.error("💀 GC FAILED AND PM2 RESTART DISABLED - system may crash")
                    elif memory_mb > emergency_threshold_mb:
                        self.logger.error(f"🚨 EMERGENCY MEMORY PRESSURE: {memory_mb:.1f} MB - OOM risk HIGH! (threshold: {emergency_threshold_mb} MB)")
                        self.logger.warning("Consider reducing batch sizes or restarting miner to prevent OOM")
                        # Light GC at emergency level
                        try:
                            collected = gc.collect()
                            if collected > 0:
                                self.logger.info(f"Emergency light GC collected {collected} objects")
                        except Exception:
                            pass
                    elif memory_mb > warning_threshold_mb:
                        self.logger.warning(f"🟡 HIGH MEMORY: Miner process using {memory_mb:.1f} MB ({system_percent:.1f}% of system) (threshold: {warning_threshold_mb} MB)")
                        
                    # Regular status logging every 5 minutes
                    if current_time - last_log_time >= 300:  # 5 minutes
                        self.logger.info(f"Miner memory status: {memory_mb:.1f} MB RSS ({system_percent:.1f}% system memory)")
                        last_log_time = current_time
                        
                except Exception as e:
                    self.logger.warning(f"Memory monitoring error: {e}")
                    
                time.sleep(10)  # Check every 10 seconds
                
        except Exception as e:
            self.logger.error(f"Miner memory monitoring thread error: {e}", exc_info=True)

    def _trigger_pm2_restart_sync(self, reason: str):
        """Synchronous version of PM2 restart trigger for use in threads."""
        self.logger.error(f"🔄 TRIGGERING CONTROLLED PM2 RESTART: {reason}")
        
        try:
            # Try graceful shutdown first
            self.logger.info("Attempting graceful shutdown before restart...")
            
            # Force garbage collection one more time
            import gc
            collected = gc.collect()
            self.logger.info(f"Final GC before restart collected {collected} objects")
            
            # Check if we're running under pm2
            pm2_instance_id = os.getenv('pm2_id')
            if pm2_instance_id:
                self.logger.info(f"Running under PM2 instance {pm2_instance_id} - triggering restart...")
                # Use pm2 restart command
                import subprocess
                subprocess.Popen(['pm2', 'restart', pm2_instance_id])
            else:
                self.logger.warning("Not running under PM2 - triggering system exit")
                # If not under pm2, exit gracefully
                import sys
                sys.exit(1)
                
        except Exception as e:
            self.logger.error(f"Error during controlled restart: {e}")
            # Last resort - force exit
            import sys
            sys.exit(1)

    async def start_memory_monitoring(self):
        """Start background memory monitoring for the entire miner process."""
        if not self.memory_monitor_enabled:
            self.logger.info("Miner memory monitoring disabled by configuration")
            return
            
        self.logger.info("🔍 Starting memory monitoring for miner process...")
        try:
            import psutil
            system_memory = psutil.virtual_memory()
            self.logger.info(f"System memory: {system_memory.total / (1024**3):.1f} GB total, {system_memory.available / (1024**3):.1f} GB available")
            
            self.memory_monitor_task = asyncio.create_task(self._memory_monitor_loop())
            self.logger.info("✅ Miner memory monitoring started")
        except ImportError:
            self.logger.warning("psutil not available - memory monitoring disabled")
            self.memory_monitor_enabled = False
        except Exception as e:
            self.logger.error(f"Failed to start miner memory monitoring: {e}")

    async def _memory_monitor_loop(self):
        """Background memory monitoring loop for the miner process."""
        try:
            import psutil
            process = psutil.Process()
            last_log_time = 0
            
            # Configurable thresholds with defaults tuned for miner systems
            warning_threshold_mb = int(os.getenv('MINER_MEMORY_WARNING_THRESHOLD_MB', '8000'))  # 8GB
            emergency_threshold_mb = int(os.getenv('MINER_MEMORY_EMERGENCY_THRESHOLD_MB', '12000'))  # 12GB
            critical_threshold_mb = int(os.getenv('MINER_MEMORY_CRITICAL_THRESHOLD_MB', '14000'))  # 14GB
            
            self.logger.info(f"Memory monitoring thresholds: Warning={warning_threshold_mb}MB, Emergency={emergency_threshold_mb}MB, Critical={critical_threshold_mb}MB")
            self.logger.info(f"PM2 restart enabled: {self.pm2_restart_enabled}")
            if self.pm2_restart_enabled:
                pm2_id = os.getenv('pm2_id', 'not detected')
                self.logger.info(f"PM2 instance ID: {pm2_id}")
            
            while True:
                try:
                    memory_info = process.memory_info()
                    memory_mb = memory_info.rss / (1024 * 1024)
                    
                    system_memory = psutil.virtual_memory()
                    system_percent = system_memory.percent
                    
                    current_time = time.time()
                    
                    # Emergency circuit breakers with pm2 restart capability
                    if memory_mb > critical_threshold_mb:
                        self.logger.error(f"💀 CRITICAL MEMORY: {memory_mb:.1f} MB - OOM imminent! (threshold: {critical_threshold_mb} MB)")
                        self.logger.error("Attempting emergency garbage collection to prevent OOM kill...")
                        try:
                            import gc
                            collected = gc.collect()
                            self.logger.error(f"Emergency GC freed {collected} objects")
                            await asyncio.sleep(2)  # Brief pause after emergency GC
                            
                            # Check memory again after GC
                            post_gc_memory = process.memory_info().rss / (1024 * 1024)
                            if post_gc_memory > (critical_threshold_mb * 0.9):  # Still >90% of critical
                                if self.pm2_restart_enabled:
                                    self.logger.error(f"🔄 TRIGGERING PM2 RESTART: Memory still critical after GC ({post_gc_memory:.1f} MB)")
                                    await self._trigger_pm2_restart("Critical memory pressure after GC")
                                    return  # Exit the monitoring loop
                                else:
                                    self.logger.error(f"💀 MEMORY CRITICAL BUT PM2 RESTART DISABLED: {post_gc_memory:.1f} MB - system may be killed by OOM")
                            else:
                                self.logger.info(f"✅ Memory reduced to {post_gc_memory:.1f} MB after GC - continuing")
                        except Exception as gc_err:
                            self.logger.error(f"Emergency GC failed: {gc_err}")
                            # If GC fails, definitely restart (if enabled)
                            if self.pm2_restart_enabled:
                                await self._trigger_pm2_restart("Emergency GC failed and memory critical")
                                return
                            else:
                                self.logger.error("💀 GC FAILED AND PM2 RESTART DISABLED - system may crash")
                    elif memory_mb > emergency_threshold_mb:
                        self.logger.error(f"🚨 EMERGENCY MEMORY PRESSURE: {memory_mb:.1f} MB - OOM risk HIGH! (threshold: {emergency_threshold_mb} MB)")
                        self.logger.warning("Consider reducing batch sizes or restarting miner to prevent OOM")
                        # Light GC at emergency level
                        try:
                            import gc
                            collected = gc.collect()
                            if collected > 0:
                                self.logger.info(f"Emergency light GC collected {collected} objects")
                        except Exception:
                            pass
                    elif memory_mb > warning_threshold_mb:
                        self.logger.warning(f"🟡 HIGH MEMORY: Miner process using {memory_mb:.1f} MB ({system_percent:.1f}% of system) (threshold: {warning_threshold_mb} MB)")
                        
                    # Regular status logging every 5 minutes
                    if current_time - last_log_time >= 300:  # 5 minutes
                        self.logger.info(f"Miner memory status: {memory_mb:.1f} MB RSS ({system_percent:.1f}% system memory)")
                        last_log_time = current_time
                        
                except Exception as e:
                    self.logger.warning(f"Memory monitoring error: {e}")
                    
                await asyncio.sleep(10)  # Check every 10 seconds
                
        except asyncio.CancelledError:
            self.logger.info("Miner memory monitoring stopped")
        except Exception as e:
            self.logger.error(f"Miner memory monitoring loop error: {e}", exc_info=True)

    async def _trigger_pm2_restart(self, reason: str):
        """Trigger a controlled pm2 restart instead of letting OOM killer take over."""
        if not self.pm2_restart_enabled:
            self.logger.error(f"🚨 PM2 restart disabled - would restart for: {reason}")
            return
            
        self.logger.error(f"🔄 TRIGGERING CONTROLLED PM2 RESTART: {reason}")
        
        try:
            # Try graceful shutdown first
            self.logger.info("Attempting graceful shutdown before restart...")
            
            # Stop any ongoing weather tasks
            if hasattr(self, 'weather_task') and self.weather_task:
                try:
                    await self.weather_task.cleanup_resources()
                    self.logger.info("Weather task cleanup completed")
                except Exception as e:
                    self.logger.warning(f"Error during weather task cleanup: {e}")
            
            # Force garbage collection one more time
            import gc
            collected = gc.collect()
            self.logger.info(f"Final GC before restart collected {collected} objects")
            
            # Check if we're running under pm2
            pm2_instance_id = os.getenv('pm2_id')
            if pm2_instance_id:
                self.logger.info(f"Running under PM2 instance {pm2_instance_id} - triggering restart...")
                # Use pm2 restart command
                import subprocess
                subprocess.Popen(['pm2', 'restart', pm2_instance_id])
            else:
                self.logger.warning("Not running under PM2 - triggering system exit")
                # If not under pm2, exit gracefully
                import sys
                sys.exit(1)
                
        except Exception as e:
            self.logger.error(f"Error during controlled restart: {e}")
            # Last resort - force exit
            import sys
            sys.exit(1)

    async def stop_memory_monitoring(self):
        """Stop memory monitoring gracefully."""
        if self.memory_monitor_task and not self.memory_monitor_task.done():
            self.logger.info("Stopping miner memory monitoring...")
            self.memory_monitor_task.cancel()
            try:
                await self.memory_monitor_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.warning(f"Error stopping memory monitor: {e}")
            self.logger.info("Miner memory monitoring stopped")


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

