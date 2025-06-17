import asyncio
import os
import sys
import traceback
import tracemalloc
from typing import Optional

from fiber.logging_utils import get_logger
from gaia.database.db_wipe import handle_db_wipe
from gaia.validator.sync.auto_sync_manager import get_auto_sync_manager

try:
    import memray
    MEMRAY_AVAILABLE = True
except ImportError:
    MEMRAY_AVAILABLE = False
    memray = None

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None

logger = get_logger(__name__)


class TaskOrchestrator:
    """
    Handles the main execution loop and task orchestration for the validator.
    Extracted from the monolithic validator to improve maintainability.
    """
    
    def __init__(self, validator):
        self.validator = validator
        
    async def run_main_loop(self):
        """Main execution loop for the validator."""
        memray_active = False
        memray_output_file_path = "validator_memray_output.bin" 

        if os.getenv("ENABLE_MEMRAY_TRACKING", "false").lower() == "true":
            if MEMRAY_AVAILABLE:
                try:
                    logger.info(f"Programmatic Memray tracking enabled. Output will be saved to: {memray_output_file_path}")
                    self.validator.memray_tracker = memray.Tracker(
                        destination=memray.FileDestination(path=memray_output_file_path, overwrite=True),
                        native_traces=True 
                    )
                    memray_active = True
                except Exception as e:
                    logger.error(f"Failed to initialize Memray tracker: {e}")
                    self.validator.memray_tracker = None
            else:
                logger.warning("Memray library not available. Programmatic Memray tracking is disabled.")
        
        await self._run_validator_logic(memray_active)

    async def _run_validator_logic(self, memray_active: bool):
        """Core validator logic execution."""
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

        try:
            # Setup and initialization
            await self._perform_initialization()
            
            # Setup HTTP clients
            await self._setup_http_clients()
            
            # Start watchdog and monitoring
            await self._start_monitoring_services()
            
            # Initialize memory tracking
            if not memray_active:
                logger.info("Starting tracemalloc for memory analysis...")
                tracemalloc.start(25)
            
            # Initialize baseline models
            logger.info("Initializing baseline models...")
            await self.validator.basemodel_evaluator.initialize_models()
            logger.info("Baseline models initialization complete")
            
            # Start auto-updater as independent task
            logger.info("Starting independent auto-updater task...")
            auto_updater_task = asyncio.create_task(self.validator.check_for_updates())
            logger.info("Auto-updater task started independently")
            
            # Create and run main service tasks
            await self._run_main_service_tasks(memray_active)
            
        except Exception as e:
            logger.error(f"Error in validator logic: {e}")
            logger.error(traceback.format_exc())
            raise

    async def _perform_initialization(self):
        """Perform initial setup and database initialization."""
        logger.info("Setting up neuron...")
        if not self.validator.setup_neuron():
            logger.error("Failed to setup neuron, exiting...")
            return

        logger.info("Neuron setup complete.")

        logger.info("Checking metagraph initialization...")
        if self.validator.metagraph is None:
            logger.error("Metagraph not initialized, exiting...")
            return

        logger.info("Metagraph initialized.")

        logger.info("Initializing database connection...") 
        await self.validator.database_manager.initialize_database()
        logger.info("Database tables initialized.")
        
        # Initialize DB Sync Components - AFTER DB init
        await self.initialize_db_sync_components()

        await handle_db_wipe(self.validator.database_manager)
        
        # Perform startup history cleanup AFTER db init and wipe check
        await self.validator.cleanup_stale_history_on_startup()

        # Lock storage to prevent any writes
        self.validator.database_manager._storage_locked = False
        if self.validator.database_manager._storage_locked:
            logger.warning("Database storage is locked - no data will be stored until manually unlocked")

    async def _setup_http_clients(self):
        """Setup HTTP clients for miner and API communication."""
        if not HTTPX_AVAILABLE:
            logger.error("httpx library not available - cannot setup HTTP clients")
            return
            
        logger.info("Checking HTTP clients...")
        # Only create clients if they don't exist or are closed
        if not hasattr(self.validator, 'miner_client') or self.validator.miner_client.is_closed:
            self.validator.miner_client = httpx.AsyncClient(
                timeout=30.0, follow_redirects=True, verify=False
            )
            logger.info("Created new miner client")
        if not hasattr(self.validator, 'api_client') or self.validator.api_client.is_closed:
            self.validator.api_client = httpx.AsyncClient(
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

    async def _start_monitoring_services(self):
        """Start watchdog and other monitoring services."""
        logger.info("Starting watchdog...")
        await self.validator.start_watchdog()
        logger.info("Watchdog started.")

    async def _run_main_service_tasks(self, memray_active: bool):
        """Create and run the main service tasks."""
        # Create task list
        tasks_lambdas = self._create_task_list(memray_active)
        
        # Setup AutoSyncManager if available
        await self._setup_auto_sync_manager()
        
        # Conditionally add miner_score_sender task
        self._add_conditional_tasks(tasks_lambdas)

        # Run the main task monitoring loop
        await self._monitor_service_tasks(tasks_lambdas)

    def _create_task_list(self, memray_active: bool):
        """Create the list of main service tasks."""
        tasks_lambdas = [
            lambda: self.validator.geomagnetic_task.validator_execute(self.validator),
            lambda: self.validator.soil_task.validator_execute(self.validator),
            lambda: self.validator.weather_task.validator_execute(self.validator),
            lambda: self.validator.status_logger(),
            lambda: self.validator.scoring_engine.run_main_scoring(),
            lambda: self.validator.miner_manager.handle_miner_deregistration_loop(),
            lambda: self.validator.manage_earthdata_token(),
            lambda: self.validator.monitor_client_health(),
            lambda: self.validator.periodic_substrate_cleanup(),
            lambda: self.validator.aggressive_memory_cleanup(),
        ]
        
        if not memray_active:
            tasks_lambdas.append(lambda: self.validator.memory_snapshot_taker())
            
        return tasks_lambdas

    async def _setup_auto_sync_manager(self):
        """Setup AutoSyncManager if available."""
        if self.validator.auto_sync_manager:
            logger.info(f"AutoSyncManager is active - Starting setup and scheduling...")
            logger.info(f"DB Sync Configuration: Primary={self.validator.is_source_validator_for_db_sync}")
            
            # Setup AutoSyncManager (includes system configuration AND scheduling)
            try:
                logger.info("🚀 Setting up AutoSyncManager (includes system config and scheduling)...")
                setup_success = await self.validator.auto_sync_manager.setup()
                if setup_success:
                    logger.info("✅ AutoSyncManager setup and scheduling completed successfully!")
                else:
                    logger.warning("⚠️ AutoSyncManager setup failed - attempting fallback scheduling for basic monitoring...")
                    # If setup failed, try just starting scheduling for monitoring
                    try:
                        await self.validator.auto_sync_manager.start_scheduling()
                        logger.info("✅ AutoSyncManager fallback scheduling started successfully!")
                    except Exception as fallback_e:
                        logger.error(f"❌ AutoSyncManager fallback scheduling also failed: {fallback_e}")
                        self.validator.auto_sync_manager = None
            except Exception as e:
                logger.error(f"❌ AutoSyncManager setup failed with exception: {e}")
                logger.info("🔄 Attempting fallback scheduling for basic monitoring...")
                # If setup completely failed, try just starting scheduling
                try:
                    await self.validator.auto_sync_manager.start_scheduling()
                    logger.info("✅ AutoSyncManager fallback scheduling started successfully!")
                except Exception as fallback_e:
                    logger.error(f"❌ AutoSyncManager fallback scheduling also failed: {fallback_e}")
                    logger.error("🚫 AutoSyncManager will be completely disabled")
                    self.validator.auto_sync_manager = None
        else:
            logger.info("AutoSyncManager is not active for this node (initialization failed or not configured).")

    def _add_conditional_tasks(self, tasks_lambdas):
        """Add conditional tasks based on environment variables."""
        # Conditionally add miner_score_sender task
        score_sender_on_str = os.getenv("SCORE_SENDER_ON", "False")
        if score_sender_on_str.lower() == "true":
            logger.info("SCORE_SENDER_ON is True, enabling MinerScoreSender task.")
            tasks_lambdas.insert(5, lambda: self.validator.miner_score_sender.run_async())

    async def _monitor_service_tasks(self, tasks_lambdas):
        """Monitor and manage the main service tasks."""
        active_service_tasks = []
        shutdown_waiter = None
        
        try:
            logger.info(f"Creating {len(tasks_lambdas)} main service tasks...")
            active_service_tasks = [asyncio.create_task(t()) for t in tasks_lambdas]
            logger.info(f"All {len(active_service_tasks)} main service tasks created.")

            shutdown_waiter = asyncio.create_task(self.validator._shutdown_event.wait())
            
            # Tasks to monitor are all service tasks plus the shutdown_waiter
            all_tasks_being_monitored = active_service_tasks + [shutdown_waiter]

            while not self.validator._shutdown_event.is_set():
                # Filter out already completed tasks from the list we pass to asyncio.wait
                current_wait_list = [t for t in all_tasks_being_monitored if not t.done()]
                
                if not current_wait_list: 
                    # This means all tasks (services + shutdown_waiter) are done.
                    logger.info("All monitored tasks have completed.")
                    if not self.validator._shutdown_event.is_set():
                         logger.warning("All tasks completed but shutdown event was not explicitly set. Setting it now to ensure proper cleanup.")
                         self.validator._shutdown_event.set()
                    break

                done, pending = await asyncio.wait(
                    current_wait_list,
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # If shutdown_event is set or shutdown_waiter completed, break the loop.
                if self.validator._shutdown_event.is_set() or shutdown_waiter.done():
                    logger.info("Shutdown signaled or shutdown_waiter completed. Breaking main monitoring loop.")
                    break

                # Check for any failed tasks
                for task in done:
                    if task != shutdown_waiter and task.exception():
                        logger.error(f"Service task failed with exception: {task.exception()}")
                        # Optionally restart the failed task or handle the failure

            logger.info("Main task monitoring loop completed.")

        except asyncio.CancelledError:
            logger.info("Task orchestrator cancelled - cleaning up service tasks")
            
        except Exception as e:
            logger.error(f"Error in service task monitoring: {e}")
            logger.error(traceback.format_exc())
            
        finally:
            # Cleanup tasks
            await self._cleanup_service_tasks(active_service_tasks, shutdown_waiter)

    async def _cleanup_service_tasks(self, active_service_tasks, shutdown_waiter):
        """Clean up service tasks during shutdown."""
        logger.info("Cleaning up service tasks...")
        
        # Cancel all active service tasks
        for task in active_service_tasks:
            if not task.done():
                task.cancel()
        
        if shutdown_waiter and not shutdown_waiter.done():
            shutdown_waiter.cancel()
        
        # Wait for tasks to complete cancellation
        if active_service_tasks:
            try:
                await asyncio.wait(active_service_tasks, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Some service tasks did not complete cleanup within timeout")
        
        logger.info("Service task cleanup completed.")

    async def cleanup_resources(self):
        """Clean up any resources used by the validator during recovery."""
        try:
            # First clean up database resources
            if hasattr(self.validator, 'database_manager'):
                await self.validator.database_manager.execute(
                    """
                    UPDATE geomagnetic_predictions 
                    SET status = 'pending'
                    WHERE status = 'processing'
                    """
                )
                logger.info("Reset in-progress prediction statuses")
                
                await self.validator.database_manager.execute(
                    """
                    DELETE FROM score_table 
                    WHERE task_name = 'geomagnetic' 
                    AND status = 'processing'
                    """
                )
                logger.info("Cleaned up incomplete scoring operations")
                
                # Close database connections
                await self.validator.database_manager.close_all_connections()
                logger.info("Closed database connections")

            # Clean up HTTP clients
            if hasattr(self.validator, 'miner_client') and self.validator.miner_client and not self.validator.miner_client.is_closed:
                # Clean up connections before closing client
                try:
                    await self.validator._cleanup_idle_connections()
                except Exception as e:
                    logger.debug(f"Error cleaning up connections before client close: {e}")
                
                await self.validator.miner_client.aclose()
                logger.info("Closed miner HTTP client")
            
            if hasattr(self.validator, 'api_client') and self.validator.api_client and not self.validator.api_client.is_closed:
                await self.validator.api_client.aclose()
                logger.info("Closed API HTTP client")

            # Clean up task-specific resources
            if hasattr(self.validator, 'miner_score_sender'):
                if hasattr(self.validator.miner_score_sender, 'cleanup'):
                    await self.validator.miner_score_sender.cleanup()
                logger.info("Cleaned up miner score sender resources")

            # Clean up WeatherTask resources that might be using gcsfs
            try:
                if hasattr(self.validator, 'weather_task'):
                    await self.validator.weather_task.cleanup_resources()
                    logger.info("Cleaned up WeatherTask resources")
            except Exception as e:
                logger.debug(f"Error cleaning up WeatherTask: {e}")

            # Clean up substrate connection manager
            try:
                if hasattr(self.validator, 'substrate_manager') and self.validator.substrate_manager:
                    self.validator.substrate_manager.cleanup()
                    logger.info("Cleaned up substrate connection manager")
            except Exception as e:
                logger.debug(f"Error cleaning up substrate manager: {e}")

            # Clean up AutoSyncManager
            try:
                if hasattr(self.validator, 'auto_sync_manager') and self.validator.auto_sync_manager:
                    await self.validator.auto_sync_manager.shutdown()
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
        """Recover a specific task - delegates to validator."""
        await self.validator.recover_task(task_name)

    async def initialize_db_sync_components(self):
        """Initialize database synchronization components."""
        logger.info("Attempting to initialize DB Sync components...")
        
        db_sync_enabled_str = os.getenv("DB_SYNC_ENABLED", "True")  # Default to True if not set
        if db_sync_enabled_str.lower() != "true":
            logger.info("DB_SYNC_ENABLED is not 'true'. Database synchronization feature will be disabled.")
            self.validator.auto_sync_manager = None
            return

        # Initialize AutoSyncManager (streamlined sync system using pgBackRest + R2)
        try:
            logger.info("Initializing AutoSyncManager (streamlined sync system)...")
            self.validator.auto_sync_manager = await get_auto_sync_manager(test_mode=self.validator.args.test)
            if self.validator.auto_sync_manager:
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