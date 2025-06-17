"""
Gaia Validator - Refactored Main Module

This is the main validator module refactored to use core modules for clean separation
of concerns. The core functionality has been moved to:
- neuron: Neuron setup, substrate connections, metagraph synchronization
- miner_comms: Miner communication, handshakes, HTTP client management  
- scoring_engine: Weight calculations, scoring algorithms, normalization
- miner_manager: Miner state management, registration/deregistration
- task_orchestrator: Task scheduling, health monitoring, watchdog operations
"""

import gc
import os
import sys
import time
import signal
import asyncio
import traceback
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import Optional

# Core module imports
from gaia.validator.core import (
    ValidatorNeuron,
    MinerCommunicator, 
    ScoringEngine,
    MinerManager,
    TaskOrchestrator
)

# Task imports
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask  
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask

# Database and utilities
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.APIcalls.miner_score_sender import MinerScoreSender
from gaia.validator.basemodel_evaluator import BaseModelEvaluator
from gaia.validator.utils.db_wipe import handle_db_wipe
from gaia.validator.utils.earthdata_tokens import ensure_valid_earthdata_token
from gaia.validator.sync.auto_sync_manager import get_auto_sync_manager

# Logging
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None 
    PSUTIL_AVAILABLE = False


class GaiaValidator:
    """
    Refactored Gaia Validator that delegates functionality to core modules.
    """
    
    def __init__(self, args):
        """Initialize the Gaia Validator with core modules."""
        logger.info("Initializing Gaia Validator with modular core architecture")
        self.args = args
        
        # Initialize core modules
        self.neuron = ValidatorNeuron(args)
        self.miner_communicator = MinerCommunicator(args)
        self.task_orchestrator = TaskOrchestrator(args)
        
        # Initialize database manager
        self.database_manager = ValidatorDatabaseManager()
        
        # Initialize scoring engine and miner manager (need database_manager)
        self.scoring_engine = ScoringEngine(args, self.database_manager, None)  # substrate_manager set in setup
        self.miner_manager = MinerManager(self.database_manager)
        
        # Initialize task modules
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
        
        # Initialize other components
        self.basemodel_evaluator = BaseModelEvaluator(
            db_manager=self.database_manager,
            test_mode=self.args.test if hasattr(self.args, 'test') else False
        )
        
        # Initialize miner score sender
        self.miner_score_sender = MinerScoreSender(
            database_manager=self.database_manager,
            api_client=self.miner_communicator.api_client
        )
        
        # DB Sync components
        self.auto_sync_manager = None
        self.is_source_validator_for_db_sync = os.getenv("IS_SOURCE_VALIDATOR_FOR_DB_SYNC", "False").lower() == "true"
        
        if self.args.test:
            self.db_sync_interval_hours = 0.25  # 15 minutes for testing
        else:
            self.db_sync_interval_hours = int(os.getenv("DB_SYNC_INTERVAL_HOURS", "1"))
            
        # Shutdown handling
        self._cleanup_done = False
        self._shutdown_event = asyncio.Event()
        
        # Setup signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signame = signal.Signals(signum).name
        logger.info(f"Received shutdown signal {signame}")
        if not self._cleanup_done:
            if asyncio.get_event_loop().is_running():
                logger.info("Setting shutdown event in running loop")
                self._shutdown_event.set()
            else:
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
            logger.info("Setting shutdown event...")
            self._shutdown_event.set()
            
            logger.info("Stopping watchdog...")
            if hasattr(self.task_orchestrator, 'watchdog_running') and self.task_orchestrator.watchdog_running:
                await self.task_orchestrator.stop_watchdog()
            
            logger.info("Updating task statuses to 'stopping'...")
            for task_name in ['soil', 'geomagnetic', 'weather', 'scoring', 'deregistration']:
                try:
                    await self.task_orchestrator.update_task_status(task_name, 'stopping')
                except Exception as e:
                    logger.error(f"Error updating {task_name} task status: {e}")

            logger.info("Cleaning up core module resources...")
            await self.cleanup_resources()
            
            logger.info("Performing final garbage collection...")
            gc.collect()
            
            self._cleanup_done = True
            logger.info("Graceful shutdown sequence completed.")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}", exc_info=True)
            self._cleanup_done = True

    async def setup_validator(self) -> bool:
        """Set up the validator with all core components."""
        try:
            logger.info("Setting up validator neuron...")
            if not self.neuron.setup_neuron():
                logger.error("Failed to setup neuron")
                return False

            # Configure scoring engine with neuron details
            self.scoring_engine.setup(
                netuid=self.neuron.netuid,
                wallet_name=self.neuron.wallet_name,
                hotkey_name=self.neuron.hotkey_name,
                subtensor_network=self.neuron.subtensor_network
            )
            
            # Set substrate manager reference in scoring engine
            self.scoring_engine.substrate_manager = self.neuron.substrate_manager

            logger.info("Initializing database...")
            await self.database_manager.initialize_database()
            
            # Initialize DB Sync components
            await self._initialize_db_sync_components()

            logger.info("Performing database wipe check...")
            await handle_db_wipe(self.database_manager)
            
            logger.info("Performing stale history cleanup...")
            await self.miner_manager.cleanup_stale_history_on_startup(
                self.neuron.metagraph,
                self.neuron._fetch_nodes_managed
            )

            logger.info("Initializing baseline models...")
            await self.basemodel_evaluator.initialize_models()
            
            logger.info("Starting watchdog...")
            await self.task_orchestrator.start_watchdog()
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting up validator: {e}")
            logger.error(traceback.format_exc())
            return False

    async def _initialize_db_sync_components(self):
        """Initialize database synchronization components."""
        logger.info("Attempting to initialize DB Sync components...")
        
        db_sync_enabled_str = os.getenv("DB_SYNC_ENABLED", "True")
        if db_sync_enabled_str.lower() != "true":
            logger.info("DB_SYNC_ENABLED is not 'true'. Database synchronization disabled.")
            return

        try:
            logger.info("Initializing AutoSyncManager...")
            self.auto_sync_manager = await get_auto_sync_manager(test_mode=self.args.test)
            if self.auto_sync_manager:
                logger.info("✅ AutoSyncManager initialized successfully")
                
                # Setup AutoSyncManager
                setup_success = await self.auto_sync_manager.setup()
                if setup_success:
                    logger.info("✅ AutoSyncManager setup completed successfully!")
                else:
                    logger.warning("⚠️ AutoSyncManager setup failed")
            else:
                logger.warning("AutoSyncManager failed to initialize")
        except Exception as e:
            logger.warning(f"AutoSyncManager initialization failed: {e}")
            self.auto_sync_manager = None
        
        logger.info("DB Sync initialization completed.")

    async def main_scoring_loop(self):
        """Main scoring loop using the scoring engine."""
        while not self._shutdown_event.is_set():
            try:
                await self.task_orchestrator.update_task_status('scoring', 'active')
                
                success = await self.scoring_engine.main_scoring_cycle(
                    self.neuron,
                    self.task_orchestrator.task_health
                )
                
                if success:
                    await self.task_orchestrator.update_task_status('scoring', 'idle')
                else:
                    await self.task_orchestrator.update_task_status('scoring', 'error')
                    
                # Force substrate connection refresh if scoring had issues
                if not success:
                    try:
                        self.neuron.substrate = self.neuron.substrate_manager.force_reconnect()
                    except Exception as e:
                        logger.error(f"Failed to reconnect substrate: {e}")
                
            except asyncio.CancelledError:
                logger.info("Main scoring loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in main scoring loop: {e}")
                await self.task_orchestrator.update_task_status('scoring', 'error')
                
            await asyncio.sleep(60)

    async def miner_deregistration_loop(self):
        """Miner deregistration loop using the miner manager."""
        try:
            await self.miner_manager.handle_miner_deregistration_loop(
                sync_metagraph_func=self.neuron.sync_metagraph,
                fetch_nodes_func=lambda: self.neuron._fetch_nodes_managed(self.neuron.netuid)
            )
        except asyncio.CancelledError:
            logger.info("Miner deregistration loop cancelled")

    async def task_execution_wrapper(self, task, task_name: str):
        """Wrapper for task execution with proper status tracking."""
        while not self._shutdown_event.is_set():
            try:
                await self.task_orchestrator.update_task_status(task_name, 'active')
                
                # Execute the task with query_miners function
                if hasattr(task, 'validator_execute'):
                    # Pass self for compatibility, but the task should use miner_communicator
                    await task.validator_execute(self)
                else:
                    # Fallback for tasks that don't have validator_execute
                    await task.run()
                    
                await self.task_orchestrator.update_task_status(task_name, 'idle')
                
            except asyncio.CancelledError:
                logger.info(f"{task_name} task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in {task_name} task: {e}")
                await self.task_orchestrator.update_task_status(task_name, 'error')
                await asyncio.sleep(30)  # Brief delay before retry

    async def query_miners(self, payload, endpoint, hotkeys=None):
        """
        Delegate miner querying to the miner communicator.
        This method is kept for compatibility with existing task modules.
        """
        return await self.miner_communicator.query_miners(
            payload=payload,
            endpoint=endpoint,
            hotkeys=hotkeys,
            metagraph=self.neuron.metagraph,
            keypair=self.neuron.keypair
        )

    async def manage_earthdata_token(self):
        """Manage Earthdata token refresh."""
        logger.info("Starting Earthdata token management...")
        
        while not self._shutdown_event.is_set():
            try:
                logger.info("Running Earthdata token check...")
                token = await ensure_valid_earthdata_token()
                if token:
                    logger.info("✅ Earthdata token check successful")
                else:
                    logger.warning("⚠️ Earthdata token check failed")

                await asyncio.sleep(86400)  # Check daily

            except asyncio.CancelledError:
                logger.info("Earthdata token management cancelled")
                break
            except Exception as e:
                logger.error(f"Error in Earthdata token management: {e}")
                await asyncio.sleep(3600)  # Retry in an hour

    async def cleanup_resources(self):
        """Clean up all validator resources using core modules."""
        try:
            # Clean up core modules
            if hasattr(self, 'miner_communicator'):
                await self.miner_communicator.cleanup()
            
            if hasattr(self, 'scoring_engine'):
                await self.scoring_engine.cleanup()
            
            if hasattr(self, 'miner_manager'):
                await self.miner_manager.cleanup()
                
            if hasattr(self, 'task_orchestrator'):
                await self.task_orchestrator.cleanup()
                
            if hasattr(self, 'neuron'):
                self.neuron.cleanup()

            # Clean up database
            if hasattr(self, 'database_manager'):
                await self.database_manager.close_all_connections()
                
            # Clean up tasks
            task_list = ['weather_task', 'soil_task', 'geomagnetic_task']
            for task_name in task_list:
                if hasattr(self, task_name):
                    task = getattr(self, task_name)
                    if hasattr(task, 'cleanup_resources'):
                        await task.cleanup_resources()

            # Clean up AutoSyncManager
            if hasattr(self, 'auto_sync_manager') and self.auto_sync_manager:
                await self.auto_sync_manager.shutdown()

            logger.info("Resource cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during resource cleanup: {e}")

    async def main(self):
        """Main execution loop for the validator."""
        try:
            logger.info("Starting Gaia Validator main execution...")
            
            # Setup validator
            if not await self.setup_validator():
                logger.error("Failed to setup validator, exiting...")
                return

            # Create main service tasks
            tasks_lambdas = [
                lambda: self.task_execution_wrapper(self.geomagnetic_task, 'geomagnetic'),
                lambda: self.task_execution_wrapper(self.soil_task, 'soil'), 
                lambda: self.task_execution_wrapper(self.weather_task, 'weather'),
                lambda: self.task_orchestrator.status_logger(),
                lambda: self.main_scoring_loop(),
                lambda: self.miner_deregistration_loop(),
                lambda: self.manage_earthdata_token(),
                lambda: self.miner_communicator.monitor_client_health(),
                lambda: self.task_orchestrator.periodic_substrate_cleanup(),
                lambda: self.task_orchestrator.aggressive_memory_cleanup(),
            ]

            # Add MinerScoreSender task if enabled
            score_sender_on = os.getenv("SCORE_SENDER_ON", "False").lower() == "true"
            if score_sender_on:
                logger.info("SCORE_SENDER_ON is True, enabling MinerScoreSender task")
                tasks_lambdas.insert(5, lambda: self.miner_score_sender.run_async())

            active_service_tasks = []
            shutdown_waiter = None
            
            try:
                logger.info(f"Creating {len(tasks_lambdas)} main service tasks...")
                active_service_tasks = [asyncio.create_task(t()) for t in tasks_lambdas]
                shutdown_waiter = asyncio.create_task(self._shutdown_event.wait())
                
                all_tasks_being_monitored = active_service_tasks + [shutdown_waiter]

                while not self._shutdown_event.is_set():
                    # Filter out completed tasks
                    current_wait_list = [t for t in all_tasks_being_monitored if not t.done()]
                    
                    if not current_wait_list:
                        logger.info("All monitored tasks have completed.")
                        if not self._shutdown_event.is_set():
                            logger.warning("All tasks completed but shutdown not set. Setting shutdown event.")
                            self._shutdown_event.set()
                        break

                    done, pending = await asyncio.wait(
                        current_wait_list,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # Check for shutdown
                    if self._shutdown_event.is_set() or shutdown_waiter.done():
                        logger.info("Shutdown signaled. Breaking main monitoring loop.")
                        break 

                    # Log completed tasks
                    for task in done:
                        if task in active_service_tasks:
                            try:
                                result = task.result()
                                logger.warning(f"Service task {task.get_name()} completed unexpectedly: {result}")
                            except asyncio.CancelledError:
                                logger.info(f"Service task {task.get_name()} was cancelled")
                            except Exception as e:
                                logger.error(f"Service task {task.get_name()} failed: {e}", exc_info=True)
                
                # Cancel remaining tasks
                logger.info("Cancelling remaining active tasks...")
                for task_to_cancel in active_service_tasks:
                    if not task_to_cancel.done():
                        task_to_cancel.cancel()
                
                if shutdown_waiter and not shutdown_waiter.done():
                    shutdown_waiter.cancel()
                
                # Wait for all tasks to complete
                await asyncio.gather(
                    *(active_service_tasks + ([shutdown_waiter] if shutdown_waiter else [])), 
                    return_exceptions=True
                )
                logger.info("All service tasks have been processed.")
                
            except asyncio.CancelledError:
                logger.info("Main task execution was cancelled")
                # Cancel all tasks
                for task_to_cancel in (active_service_tasks + ([shutdown_waiter] if shutdown_waiter else [])):
                    if task_to_cancel and not task_to_cancel.done():
                        task_to_cancel.cancel()
                await asyncio.gather(*[t for t in (active_service_tasks + ([shutdown_waiter] if shutdown_waiter else [])) if t], return_exceptions=True)

        except Exception as e:
            logger.error(f"Error in main: {e}")
            logger.error(traceback.format_exc())
        finally:
            if not self._cleanup_done:
                await self._initiate_shutdown()


def main():
    """Entry point for the validator."""
    # Argument parsing
    parser = ArgumentParser()
    parser.add_argument("--wallet", type=str, help="Name of the wallet to use")
    parser.add_argument("--hotkey", type=str, help="Name of the hotkey to use") 
    parser.add_argument("--netuid", type=int, help="Netuid to use")
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    
    subtensor_group = parser.add_argument_group("subtensor")
    subtensor_group.add_argument("--subtensor.chain_endpoint", type=str, help="Subtensor chain endpoint")
    subtensor_group.add_argument("--subtensor.network", type=str, help="Subtensor network")
    
    args = parser.parse_args()

    # Set node type environment variable
    os.environ["NODE_TYPE"] = "validator"

    # Create and run validator
    validator = GaiaValidator(args)
    try:
        asyncio.run(validator.main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {e}", exc_info=True)
    finally:
        logger.info("Validator shutdown complete")


if __name__ == "__main__":
    main()