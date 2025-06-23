#!/usr/bin/env python3
"""
New Modular Gaia Validator
===========================

This is a complete rewrite of the validator using the modular core manager classes.
It serves as the main entry point and orchestrates all validator functionality through
specialized manager components.

Architecture:
- GaiaValidatorNeuron: Handles substrate, metagraph, and neuron setup
- HTTPClientManager: Manages miner communication and HTTP clients  
- BackgroundWorkersManager: Handles watchdog and background monitoring
- DatabaseOperationsManager: Manages database monitoring and operations
- MemoryManagementManager: Handles memory cleanup and monitoring
- WeightCalculationsManager: Manages scoring and weight setting
- MainExecutionManager: Orchestrates main execution loop
"""

import gc
import logging
import sys
from datetime import datetime, timezone, timedelta
import os
import time
import signal
import tracemalloc
import asyncio
import math
from typing import Any, Optional, List, Dict, Set
from argparse import ArgumentParser

# Set environment and configure tracing
os.environ["NODE_TYPE"] = "validator"

# === WEIGHT TRACING INTEGRATION ===
try:
    import sys
    import os
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if root_dir not in sys.path:
        sys.path.insert(0, root_dir)
    
    import runtime_weight_tracer
    print("🔍 [NEW_VALIDATOR] Weight tracing available - enabling...")
    runtime_weight_tracer.enable_weight_tracing()
    print("✅ [NEW_VALIDATOR] Weight tracing enabled successfully")
except Exception as e:
    print(f"⚠️ [NEW_VALIDATOR] Weight tracing not available: {e}")

# Core imports
from dotenv import load_dotenv
from fiber.logging_utils import get_logger

# Import all core manager classes
from gaia.validator.core.neuron import GaiaValidatorNeuron
from gaia.validator.core.http_client import HTTPClientManager
from gaia.validator.core.background_workers import BackgroundWorkersManager
from gaia.validator.core.database_operations import DatabaseOperationsManager
from gaia.validator.core.memory_management import MemoryManagementManager
from gaia.validator.core.weight_calculations import WeightCalculationsManager
from gaia.validator.core.main_execution import MainExecutionManager

# Import database and task components
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.APIcalls.miner_score_sender import MinerScoreSender
from gaia.validator.basemodel_evaluator import BaseModelEvaluator

# Database setup and utilities
from gaia.validator.utils.db_wipe import handle_db_wipe
from gaia.validator.utils.earthdata_tokens import ensure_valid_earthdata_token
from gaia.validator.sync.auto_sync_manager import get_auto_sync_manager

logger = get_logger(__name__)


class NewGaiaValidator:
    """
    New modular Gaia Validator built using core manager classes.
    This serves as the main orchestrator that delegates responsibilities
    to specialized manager components.
    """
    
    def __init__(self, args):
        """Initialize the new modular validator with all core managers."""
        print("[NEW_VALIDATOR] Starting modular validator initialization")
        
        self.args = args
        self._cleanup_done = False
        self._shutdown_event = asyncio.Event()
        
        # ====== CRITICAL: Business Logic Attributes from Original Validator ======
        # These MUST match exactly for 100% business logic preservation
        self.metagraph = None
        self.config = None
        self.weights = [0.0] * 256
        self.last_set_weights_block = 0
        self.current_block = 0
        self.nodes = {}
        self.last_successful_weight_set = time.time()
        self.last_successful_dereg_check = time.time()
        self.last_successful_db_check = time.time()
        self.last_metagraph_sync = time.time()
        self.validator_uid = None
        
        # Task health tracking (EXACTLY as original)
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
        
        # Timing and configuration attributes (EXACTLY as original)
        self.watchdog_timeout = 3600  # 1 hour default timeout
        self.db_check_interval = 300  # 5 minutes
        self.metagraph_sync_interval = 300  # 5 minutes
        self.max_consecutive_errors = 3
        self.watchdog_running = False
        
        # Memory monitoring configuration (EXACTLY as original)
        self.memory_monitor_enabled = os.getenv('VALIDATOR_MEMORY_MONITORING_ENABLED', 'true').lower() in ['true', '1', 'yes']
        self.pm2_restart_enabled = os.getenv('VALIDATOR_PM2_RESTART_ENABLED', 'true').lower() in ['true', '1', 'yes']
        self.memory_warning_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_WARNING_THRESHOLD_MB', '8000'))
        self.memory_emergency_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_EMERGENCY_THRESHOLD_MB', '10000'))
        self.memory_critical_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_CRITICAL_THRESHOLD_MB', '12000'))
        self.last_memory_log_time = 0
        self.memory_log_interval = 300
        self.last_emergency_gc_time = 0
        self.emergency_gc_cooldown = 60
        
        # Task tracking for proper cleanup (EXACTLY as original)
        self._background_tasks = set()
        self._task_cleanup_lock = asyncio.Lock()
        
        # Memray configuration (EXACTLY as original)
        self.memray_tracker = None
        
        # Stepped Task Weight Schedule (EXACTLY as original)
        self.task_weight_schedule = [
            (datetime(2025, 5, 28, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.50, "geomagnetic": 0.25, "soil": 0.25}),
            (datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.65, "geomagnetic": 0.175, "soil": 0.175}), 
            (datetime(2025, 6, 5, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.80, "geomagnetic": 0.10, "soil": 0.10})
        ]
        
        # Database monitor history (EXACTLY as original)
        self.db_monitor_history = []
        self.db_monitor_history_lock = asyncio.Lock()
        self.DB_MONITOR_HISTORY_MAX_SIZE = 120
        
        # Memory snapshot tracking (EXACTLY as original)
        self.tracemalloc_snapshot1 = None
        
        # Lock for miner table operations (EXACTLY as original)
        self.miner_table_lock = asyncio.Lock()
        
        # Initialize database manager first (required by many components)
        print("[NEW_VALIDATOR] Initializing database manager...")
        self.database_manager = ValidatorDatabaseManager()
        
        # Initialize core neuron manager (handles setup, config, substrate)
        print("[NEW_VALIDATOR] Initializing neuron manager...")
        self.neuron_manager = GaiaValidatorNeuron(self.args)
        
        # Initialize HTTP client manager (handles miner communication)
        print("[NEW_VALIDATOR] Initializing HTTP client manager...")
        self.http_manager = HTTPClientManager()
        
        # Initialize memory management (handles cleanup, monitoring)
        print("[NEW_VALIDATOR] Initializing memory manager...")
        self.memory_manager = MemoryManagementManager()
        
        # Initialize background workers manager (handles watchdog, monitoring)
        print("[NEW_VALIDATOR] Initializing background workers manager...")
        self.workers_manager = BackgroundWorkersManager()
        
        # Initialize database operations manager (handles DB monitoring, plotting)
        print("[NEW_VALIDATOR] Initializing database operations manager...")
        self.db_ops_manager = DatabaseOperationsManager(self.database_manager)
        
        # Initialize weight calculations manager (handles scoring, weight setting)
        print("[NEW_VALIDATOR] Initializing weight calculations manager...")
        self.weight_manager = WeightCalculationsManager()
        
        # Initialize main execution manager (handles main loop, task orchestration)
        print("[NEW_VALIDATOR] Initializing main execution manager...")
        self.main_manager = MainExecutionManager()
        
        # Initialize task instances
        print("[NEW_VALIDATOR] Initializing task instances...")
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
        
        # Initialize additional components
        print("[NEW_VALIDATOR] Initializing additional components...")
        self.basemodel_evaluator = BaseModelEvaluator(
            db_manager=self.database_manager,
            test_mode=getattr(self.args, 'test', False)
        )
        
        # DB Sync components
        self.auto_sync_manager = None
        self.is_source_validator_for_db_sync = os.getenv("IS_SOURCE_VALIDATOR_FOR_DB_SYNC", "False").lower() == "true"
        
        # Configure sync intervals
        if self.args.test:
            self.db_sync_interval_hours = 0.25  # 15 minutes for testing
        else:
            self.db_sync_interval_hours = int(os.getenv("DB_SYNC_INTERVAL_HOURS", "1"))
        
        # MinerScoreSender (will be initialized after HTTP clients)
        self.miner_score_sender = None
        
        # Setup signal handlers for graceful shutdown
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)
        
        print("[NEW_VALIDATOR] Modular validator initialization completed")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signame = signal.Signals(signum).name
        logger.info(f"[NEW_VALIDATOR] Received shutdown signal {signame}")
        if not self._cleanup_done:
            if asyncio.get_event_loop().is_running():
                logger.info("[NEW_VALIDATOR] Setting shutdown event in running loop")
                self._shutdown_event.set()
            else:
                logger.info("[NEW_VALIDATOR] Creating new loop for shutdown")
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                loop.run_until_complete(self._initiate_shutdown())
    
    async def _initiate_shutdown(self):
        """Handle graceful shutdown of the modular validator."""
        if self._cleanup_done:
            logger.info("[NEW_VALIDATOR] Cleanup already completed")
            return
        
        logger.info("[NEW_VALIDATOR] Initiating graceful shutdown sequence...")
        
        try:
            # Set shutdown event
            self._shutdown_event.set()
            
            # Stop all manager components
            logger.info("[NEW_VALIDATOR] Stopping all manager components...")
            
            # Stop background workers first
            if hasattr(self, 'workers_manager'):
                await self.workers_manager.stop_all_workers()
            
            # Stop main execution manager
            if hasattr(self, 'main_manager'):
                await self.main_manager.shutdown()
            
            # Cleanup HTTP manager
            if hasattr(self, 'http_manager'):
                await self.http_manager.cleanup()
            
            # Cleanup neuron manager
            if hasattr(self, 'neuron_manager'):
                await self.neuron_manager.cleanup()
            
            # Cleanup memory manager (this should be last)
            if hasattr(self, 'memory_manager'):
                await self.memory_manager.cleanup()
            
            # Close database connections
            if hasattr(self, 'database_manager'):
                await self.database_manager.close_all_connections()
            
            # Cleanup auto sync manager
            if hasattr(self, 'auto_sync_manager') and self.auto_sync_manager:
                await self.auto_sync_manager.shutdown()
            
            # Create cleanup completion file for auto updater
            try:
                cleanup_file = "/tmp/validator_cleanup_done"
                with open(cleanup_file, "w") as f:
                    f.write(f"Cleanup completed at {time.time()}\n")
                logger.info(f"[NEW_VALIDATOR] Created cleanup completion file: {cleanup_file}")
            except Exception as e_cleanup_file:
                logger.error(f"[NEW_VALIDATOR] Failed to create cleanup completion file: {e_cleanup_file}")
            
            self._cleanup_done = True
            logger.info("[NEW_VALIDATOR] Graceful shutdown sequence completed.")
            
        except Exception as e_shutdown:
            logger.error(f"[NEW_VALIDATOR] Error during shutdown: {e_shutdown}", exc_info=True)
            self._cleanup_done = True
    
    async def setup_and_initialize(self) -> bool:
        """Setup and initialize all components in the correct order."""
        try:
            logger.info("[NEW_VALIDATOR] Starting setup and initialization...")
            
            # 1. Setup neuron (substrate, metagraph, etc.)
            logger.info("[NEW_VALIDATOR] Setting up neuron...")
            if not await self.neuron_manager.setup_neuron():
                logger.error("[NEW_VALIDATOR] Failed to setup neuron")
                return False
            
            # CRITICAL: Sync key attributes from neuron manager to main validator
            # This ensures 100% business logic compatibility
            logger.info("[NEW_VALIDATOR] Syncing critical attributes from neuron manager...")
            self.metagraph = self.neuron_manager.metagraph
            self.config = getattr(self.neuron_manager, 'config', None)
            self.validator_uid = getattr(self.neuron_manager, 'validator_uid', None)
            self.last_metagraph_sync = getattr(self.neuron_manager, 'last_metagraph_sync', time.time())
            self.metagraph_sync_interval = getattr(self.neuron_manager, 'metagraph_sync_interval', 300)
            
            # 2. Initialize database
            logger.info("[NEW_VALIDATOR] Initializing database...")
            await self.database_manager.initialize_database()
            
            # 3. Initialize DB Sync components
            logger.info("[NEW_VALIDATOR] Initializing DB sync components...")
            await self._initialize_db_sync_components()
            
            # 4. Handle database wipe if needed
            logger.info("[NEW_VALIDATOR] Checking for database wipe trigger...")
            await handle_db_wipe(self.database_manager)
            
            # 5. Setup HTTP clients
            logger.info("[NEW_VALIDATOR] Setting up HTTP clients...")
            await self.http_manager.setup_clients()
            
            # 6. Initialize MinerScoreSender now that HTTP clients are ready
            logger.info("[NEW_VALIDATOR] Initializing MinerScoreSender...")
            self.miner_score_sender = MinerScoreSender(
                database_manager=self.database_manager,
                api_client=self.http_manager.api_client
            )
            
            # 7. Initialize baseline models
            logger.info("[NEW_VALIDATOR] Initializing baseline models...")
            await self.basemodel_evaluator.initialize_models()
            
            # 8. Setup memory monitoring
            logger.info("[NEW_VALIDATOR] Setting up memory monitoring...")
            self.memory_manager.setup_monitoring()
            
            # 9. Initialize weight calculations manager with components
            logger.info("[NEW_VALIDATOR] Initializing weight calculations...")
            await self.weight_manager.initialize(
                neuron_manager=self.neuron_manager,
                database_manager=self.database_manager,
                memory_manager=self.memory_manager,
                main_execution_manager=self.main_manager
            )
            
            # 10. Setup background workers
            logger.info("[NEW_VALIDATOR] Setting up background workers...")
            self.workers_manager.setup_workers(
                neuron_manager=self.neuron_manager,
                memory_manager=self.memory_manager,
                http_manager=self.http_manager
            )
            
            logger.info("[NEW_VALIDATOR] Setup and initialization completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"[NEW_VALIDATOR] Error during setup: {e}", exc_info=True)
            return False
    
    async def _initialize_db_sync_components(self):
        """Initialize database sync components."""
        logger.info("[NEW_VALIDATOR] Attempting to initialize DB Sync components...")
        
        db_sync_enabled_str = os.getenv("DB_SYNC_ENABLED", "True")
        if db_sync_enabled_str.lower() != "true":
            logger.info("[NEW_VALIDATOR] DB_SYNC_ENABLED is not 'true'. Database synchronization feature will be disabled.")
            self.auto_sync_manager = None
            return
        
        try:
            logger.info("[NEW_VALIDATOR] Initializing AutoSyncManager...")
            self.auto_sync_manager = await get_auto_sync_manager(test_mode=self.args.test)
            if self.auto_sync_manager:
                logger.info("[NEW_VALIDATOR] ✅ AutoSyncManager initialized successfully")
                
                # Setup AutoSyncManager immediately as one of the first things
                logger.info("[NEW_VALIDATOR] Setting up AutoSyncManager...")
                try:
                    setup_success = await self.auto_sync_manager.setup()
                    if setup_success:
                        logger.info("[NEW_VALIDATOR] ✅ AutoSyncManager setup completed successfully")
                    else:
                        logger.warning("[NEW_VALIDATOR] AutoSyncManager setup failed")
                except Exception as e:
                    logger.error(f"[NEW_VALIDATOR] AutoSyncManager setup error: {e}")
            else:
                logger.warning("[NEW_VALIDATOR] AutoSyncManager failed to initialize")
        except Exception as e:
            logger.warning(f"[NEW_VALIDATOR] AutoSyncManager initialization failed: {e}")
        
        logger.info("[NEW_VALIDATOR] DB Sync initialization completed.")
    
    async def start_all_background_tasks(self) -> List[asyncio.Task]:
        """Start all background tasks and return the task list."""
        logger.info("[NEW_VALIDATOR] Starting all background tasks...")
        
        tasks = []
        
        # Core task execution functions
        task_functions = [
            lambda: self.geomagnetic_task.validator_execute(self),
            lambda: self.soil_task.validator_execute(self),
            lambda: self.weather_task.validator_execute(self),
            lambda: self.main_manager.status_logger(
                neuron_manager=self.neuron_manager,
                memory_manager=self.memory_manager
            ),
            lambda: self.weight_manager.main_scoring(),
            lambda: self.workers_manager.handle_miner_deregistration_loop(
                database_manager=self.database_manager,
                neuron_manager=self.neuron_manager
            ),
            lambda: self._manage_earthdata_token(),
            lambda: self.http_manager.monitor_client_health(),
            lambda: self.memory_manager.periodic_substrate_cleanup(self.neuron_manager),
            lambda: self.memory_manager.aggressive_memory_cleanup(),
        ]
        
        # Add MinerScoreSender task if enabled
        score_sender_on_str = os.getenv("SCORE_SENDER_ON", "False")
        if score_sender_on_str.lower() == "true":
            logger.info("[NEW_VALIDATOR] SCORE_SENDER_ON is True, enabling MinerScoreSender task.")
            task_functions.append(lambda: self.miner_score_sender.run_async())
        
        # Add memory snapshot taker if not using memray
        if not getattr(self, 'memray_active', False):
            task_functions.append(lambda: self.memory_manager.memory_snapshot_taker())
        
        # Create tasks
        for i, task_func in enumerate(task_functions):
            task = asyncio.create_task(task_func())
            task.set_name(f"background_task_{i}")
            tasks.append(task)
        
        logger.info(f"[NEW_VALIDATOR] Started {len(tasks)} background tasks")
        return tasks
    
    async def _manage_earthdata_token(self):
        """Manage Earthdata token refresh."""
        logger.info("[NEW_VALIDATOR] 🌍 Earthdata token management task started")
        
        while not self._shutdown_event.is_set():
            try:
                logger.info("[NEW_VALIDATOR] 🔍 Running Earthdata token check...")
                token = await ensure_valid_earthdata_token()
                if token:
                    logger.info(f"[NEW_VALIDATOR] ✅ Earthdata token check successful. Token: {token[:10]}...")
                else:
                    logger.warning("[NEW_VALIDATOR] ⚠️ Earthdata token check failed")
                
                await asyncio.sleep(86400)  # Check daily
                
            except asyncio.CancelledError:
                logger.info("[NEW_VALIDATOR] 🛑 Earthdata token management task cancelled")
                break
            except Exception as e:
                logger.error(f"[NEW_VALIDATOR] ❌ Error in Earthdata token management: {e}")
                await asyncio.sleep(3600)  # Retry in 1 hour
    
    async def run_main_loop(self):
        """Run the main validator loop."""
        
        async def run_validator_logic():
            try:
                # Setup and initialize all components
                if not await self.setup_and_initialize():
                    logger.error("[NEW_VALIDATOR] Setup failed, exiting...")
                    return
                
                # Start tracemalloc for memory analysis
                logger.info("[NEW_VALIDATOR] Starting tracemalloc for memory analysis...")
                tracemalloc.start(25)
                
                # Start watchdog
                logger.info("[NEW_VALIDATOR] Starting watchdog...")
                await self.workers_manager.start_watchdog()
                
                # Start auto-updater as independent task
                logger.info("[NEW_VALIDATOR] Starting independent auto-updater task...")
                auto_updater_task = asyncio.create_task(self.main_manager.check_for_updates(self))
                
                # Start all background tasks
                background_tasks = await self.start_all_background_tasks()
                
                # Create shutdown waiter
                shutdown_waiter = asyncio.create_task(self._shutdown_event.wait())
                all_tasks = background_tasks + [shutdown_waiter]
                
                # Main monitoring loop
                logger.info("[NEW_VALIDATOR] Entering main monitoring loop...")
                while not self._shutdown_event.is_set():
                    current_tasks = [t for t in all_tasks if not t.done()]
                    
                    if not current_tasks:
                        logger.info("[NEW_VALIDATOR] All tasks completed")
                        if not self._shutdown_event.is_set():
                            self._shutdown_event.set()
                        break
                    
                    done, pending = await asyncio.wait(
                        current_tasks,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    if self._shutdown_event.is_set() or shutdown_waiter.done():
                        logger.info("[NEW_VALIDATOR] Shutdown signaled, breaking main loop")
                        break
                    
                    # Handle completed tasks
                    for task in done:
                        if task in background_tasks:
                            try:
                                result = task.result()
                                logger.warning(f"[NEW_VALIDATOR] Background task {task.get_name()} completed unexpectedly")
                            except asyncio.CancelledError:
                                logger.info(f"[NEW_VALIDATOR] Background task {task.get_name()} was cancelled")
                            except Exception as e:
                                logger.error(f"[NEW_VALIDATOR] Background task {task.get_name()} failed: {e}")
                
                # Cleanup
                logger.info("[NEW_VALIDATOR] Cleaning up tasks...")
                for task in background_tasks:
                    if not task.done():
                        task.cancel()
                
                if shutdown_waiter and not shutdown_waiter.done():
                    shutdown_waiter.cancel()
                
                await asyncio.gather(*(background_tasks + [shutdown_waiter]), return_exceptions=True)
                logger.info("[NEW_VALIDATOR] All tasks cleaned up")
                
            except asyncio.CancelledError:
                logger.info("[NEW_VALIDATOR] Main execution was cancelled")
                # Cancel all tasks
                for task in background_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*background_tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"[NEW_VALIDATOR] Error in main execution: {e}", exc_info=True)
            finally:
                if not self._cleanup_done:
                    await self._initiate_shutdown()
        
        # Run validator logic
        logger.info("[NEW_VALIDATOR] Running validator logic...")
        await run_validator_logic()
    
    # Delegate properties to managers for compatibility with task execution
    @property
    def substrate(self):
        """Delegate substrate access to neuron manager."""
        return self.neuron_manager.substrate if hasattr(self.neuron_manager, 'substrate') else None
    
    @property
    def metagraph(self):
        """Delegate metagraph access to neuron manager."""
        return self.neuron_manager.metagraph if hasattr(self.neuron_manager, 'metagraph') else None
    
    @property
    def keypair(self):
        """Delegate keypair access to neuron manager."""
        return self.neuron_manager.keypair if hasattr(self.neuron_manager, 'keypair') else None
    
    @property
    def netuid(self):
        """Delegate netuid access to neuron manager."""
        return self.neuron_manager.netuid if hasattr(self.neuron_manager, 'netuid') else None
    
    @property
    def miner_client(self):
        """Delegate miner client access to HTTP manager."""
        return self.http_manager.miner_client if hasattr(self.http_manager, 'miner_client') else None
    
    @property
    def api_client(self):
        """Delegate API client access to HTTP manager."""
        return self.http_manager.api_client if hasattr(self.http_manager, 'api_client') else None
    
    # Delegate methods for compatibility with task execution
    async def query_miners(self, payload: Dict, endpoint: str, hotkeys: Optional[List[str]] = None) -> Dict:
        """Delegate miner queries to HTTP manager."""
        return await self.http_manager.query_miners(
            payload=payload,
            endpoint=endpoint,
            hotkeys=hotkeys,
            neuron_manager=self.neuron_manager
        )
    
    async def update_task_status(self, task_name: str, status: str, operation: Optional[str] = None):
        """Delegate task status updates to workers manager."""
        return await self.workers_manager.update_task_status(task_name, status, operation)
    
    # ====== CRITICAL: Missing Method Delegations for Business Logic ======
    # These methods are called by weight calculations and other managers
    
    def _log_memory_usage(self, context: str, threshold_mb: float = 100.0):
        """Delegate memory logging to memory manager."""
        return self.memory_manager._log_memory_usage(context, threshold_mb)
    
    def _aggressive_substrate_cleanup(self, context: str = "substrate_cleanup"):
        """Delegate substrate cleanup to memory manager.""" 
        return self.memory_manager._aggressive_substrate_cleanup(context)
    
    def get_current_task_weights(self) -> Dict[str, float]:
        """Get current task weights based on time schedule (EXACTLY as original)."""
        now_utc = datetime.now(timezone.utc)
        
        # Default to the first set of weights in the schedule if current time is before any scheduled change
        active_weights = self.task_weight_schedule[0][1] 
        
        # Iterate through the schedule to find the latest applicable weights
        for dt_threshold, weights_at_threshold in self.task_weight_schedule:
            if now_utc >= dt_threshold:
                active_weights = weights_at_threshold
            else:
                break 
                
        return active_weights
    
    def _track_background_task(self, task: asyncio.Task, task_name: str = "unnamed"):
        """Track background tasks for cleanup (EXACTLY as original)."""
        self._background_tasks.add(task)
        task.add_done_callback(lambda t: self._background_tasks.discard(t))
        logger.debug(f"Tracking background task: {task_name}")
    
    def custom_serializer(self, obj):
        """Custom JSON serializer for handling datetime objects and bytes (EXACTLY as original)."""
        import base64
        import pandas as pd
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return {
                "_type": "bytes",
                "encoding": "base64",
                "data": base64.b64encode(obj).decode("ascii"),
            }
        raise TypeError(f"Type {type(obj)} not serializable")
    
    async def cleanup_stale_history_on_startup(self):
        """Delegate to background workers manager."""
        if hasattr(self.workers_manager, 'cleanup_stale_history_on_startup'):
            return await self.workers_manager.cleanup_stale_history_on_startup()
    
    async def _sync_metagraph(self):
        """Delegate to neuron manager.""" 
        return await self.neuron_manager._sync_metagraph()
    
    async def _fetch_nodes_managed(self, netuid, force_fresh=False):
        """Delegate to neuron manager."""
        return await self.neuron_manager._fetch_nodes_managed(netuid, force_fresh)
    
    def setup_neuron(self) -> bool:
        """Delegate to neuron manager (for compatibility)."""
        return self.neuron_manager.setup_neuron()
    
    # ====== CRITICAL: Properties must also sync with neuron manager ======
    
    @property
    def wallet_name(self):
        """Delegate wallet name access to neuron manager."""
        return getattr(self.neuron_manager, 'wallet_name', None)
    
    @property 
    def hotkey_name(self):
        """Delegate hotkey name access to neuron manager."""
        return getattr(self.neuron_manager, 'hotkey_name', None)
    
    @property
    def subtensor_network(self):
        """Delegate subtensor network access to neuron manager."""
        return getattr(self.neuron_manager, 'subtensor_network', None)
    
    @property
    def subtensor_chain_endpoint(self):
        """Delegate subtensor chain endpoint access to neuron manager."""
        return getattr(self.neuron_manager, 'subtensor_chain_endpoint', None)


async def main():
    """Main entry point for the new modular validator."""
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
    
    # Comprehensive Database Setup
    logger.info("[NEW_VALIDATOR] 🚀 Starting comprehensive database setup...")
    try:
        from gaia.validator.database.comprehensive_db_setup import setup_comprehensive_database, DatabaseConfig
        
        db_config = DatabaseConfig(
            database_name=os.getenv("DB_NAME", "gaia_validator"),
            postgres_version=os.getenv("POSTGRES_VERSION", "14"),
            postgres_password=os.getenv("DB_PASSWORD", "postgres"),
            postgres_user=os.getenv("DB_USER", "postgres"),
            port=int(os.getenv("DB_PORT", "5432")),
            data_directory=os.getenv("POSTGRES_DATA_DIR", "/var/lib/postgresql/14/main"),
            config_directory=os.getenv("POSTGRES_CONFIG_DIR", "/etc/postgresql/14/main")
        )
        
        setup_success = await setup_comprehensive_database(
            test_mode=args.test,
            config=db_config
        )
        
        if not setup_success:
            logger.error("[NEW_VALIDATOR] ❌ Database setup failed - validator cannot start")
            sys.exit(1)
        
        logger.info("[NEW_VALIDATOR] ✅ Database setup completed successfully")
        
    except Exception as e:
        logger.error(f"[NEW_VALIDATOR] ❌ Critical database setup error: {e}", exc_info=True)
        sys.exit(1)
    
    # Create and run the new modular validator
    logger.info("[NEW_VALIDATOR] Creating new modular validator...")
    validator = NewGaiaValidator(args)
    
    try:
        logger.info("[NEW_VALIDATOR] Starting validator main loop...")
        await validator.run_main_loop()
    except KeyboardInterrupt:
        logger.info("[NEW_VALIDATOR] Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.critical(f"[NEW_VALIDATOR] Unhandled exception: {e}", exc_info=True)
    finally:
        logger.info("[NEW_VALIDATOR] Validator execution completed")


if __name__ == "__main__":
    asyncio.run(main()) 