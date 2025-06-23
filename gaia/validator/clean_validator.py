#!/usr/bin/env python3
"""
Clean Gaia Validator
===================

Beautiful, functional, and clean validator implementation using modular architecture.
This validator is organized using pure functions instead of heavy OOP patterns.

Architecture Principles:
- Functional over Object-Oriented
- Direct imports instead of manager delegation  
- Clean separation of concerns
- Configuration-driven design
- Easy to read and maintain
"""

import asyncio
import tracemalloc
import signal
import time
from argparse import ArgumentParser
from typing import Dict, Any, List, NamedTuple, Optional
from datetime import datetime, timezone
from fiber.logging_utils import get_logger

# === Existing Managers (Bridge to Old System) ===
from gaia.validator.core.neuron import NeuronManager
from gaia.validator.core.http_client import HTTPClientManager
from gaia.validator.core.background_workers import BackgroundWorkersManager
from gaia.validator.core.database_operations import DatabaseOperationsManager
from gaia.validator.core.memory_management import MemoryManagementManager
from gaia.validator.core.main_execution import MainExecutionManager
from gaia.validator.core.substrate_manager import SubstrateManager
from gaia.validator.core.weight_calculations import WeightCalculationsManager

# === Scoring Operations ===
from gaia.validator.core.scoring import WeightManager

# === Task Management ===
from gaia.tasks.task_registry import TaskRegistry
from gaia.tasks.task_instance import TaskInstance

# === Database and Sync ===
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.validator.utils.db_sync_utils import get_auto_sync_manager

logger = get_logger(__name__)


class ValidatorComponents(NamedTuple):
    """Complete validator components container."""
    neuron_manager: NeuronManager
    http_client_manager: HTTPClientManager
    database_manager: ValidatorDatabaseManager
    memory_manager: MemoryManagementManager
    main_execution_manager: MainExecutionManager
    weight_manager: WeightManager
    auto_sync_manager: Optional[Any]
    substrate_manager: SubstrateManager
    background_workers_manager: BackgroundWorkersManager
    database_operations_manager: DatabaseOperationsManager


class CleanGaiaValidator:
    """
    Beautiful, clean validator implementation.
    
    This validator bridges the functional programming approach with the existing
    manager-based system to preserve 100% business logic while being cleaner.
    """
    
    def __init__(self, args):
        """Initialize the clean validator with minimal setup."""
        logger.info("🚀 Starting Clean Gaia Validator")
        
        self.args = args
        self.components: Optional[ValidatorComponents] = None
        self.background_tasks: List[asyncio.Task] = []
        self.shutdown_event = asyncio.Event()
        
        # Essential validator attributes from original
        self.weights = [0.0] * 256
        self.last_set_weights_block = 0
        self.task_health = {
            'scoring': {'last_success': time.time(), 'status': 'idle'},
            'geomagnetic': {'last_success': time.time(), 'status': 'idle'},  
            'soil': {'last_success': time.time(), 'status': 'idle'},
            'weather': {'last_success': time.time(), 'status': 'idle'},
        }
        
        # Memory monitoring configuration
        self.memory_monitor_enabled = True
        self.memory_warning_threshold_mb = 8000
        self.memory_emergency_threshold_mb = 10000
        self.memory_critical_threshold_mb = 12000
        self.database_monitor_history = []
        
        # Setup signal handlers for graceful shutdown
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signal_name = signal.Signals(signum).name
        logger.info(f"🛑 Received {signal_name} - initiating graceful shutdown")
        self.shutdown_event.set()
    
    async def run(self) -> None:
        """
        Main validator execution - clean and simple.
        
        This is the only method you need to call to run the validator.
        Everything else is handled by clean, functional modules.
        """
        try:
            # Step 1: Initialize all systems
            logger.info("🔧 Initializing validator systems...")
            components = await self._initialize_systems()
            self.components = components
            
            # Step 2: Start main execution loop
            logger.info("🎯 Starting main execution loop...")
            await self._run_main_loop()
            
        except Exception as e:
            logger.error(f"❌ Critical error in validator: {e}", exc_info=True)
            raise
        finally:
            await self._cleanup()
    
    async def _initialize_systems(self) -> ValidatorComponents:
        """Initialize all validator systems using the existing managers."""
        
        # Step 1: Initialize neuron manager
        logger.info("🧠 Initializing neuron manager...")
        neuron_manager = NeuronManager()
        await neuron_manager.setup_neuron(
            wallet_name=getattr(self.args, 'wallet', None),
            hotkey_name=getattr(self.args, 'hotkey', None),
            netuid=getattr(self.args, 'netuid', 19),
            subtensor_chain_endpoint=getattr(self.args, 'subtensor_chain_endpoint', None),
        )
        
        # Step 2: Initialize database system
        logger.info("💾 Initializing database system...")
        database_manager = ValidatorDatabaseManager()
        database_manager.initialize()
        
        # Step 3: Initialize database sync components
        logger.info("🔄 Initializing database sync...")
        auto_sync_manager = None
        try:
            auto_sync_manager = await get_auto_sync_manager(test_mode=self.args.test if hasattr(self.args, 'test') else False)
            if auto_sync_manager:
                logger.info("✅ AutoSyncManager initialized successfully")
                setup_success = await auto_sync_manager.setup()
                if not setup_success:
                    logger.warning("AutoSyncManager setup failed")
            else:
                logger.warning("AutoSyncManager failed to initialize")
        except Exception as e:
            logger.warning(f"AutoSyncManager initialization failed: {e}")
        
        # Step 4: Initialize HTTP client manager
        logger.info("🌐 Setting up HTTP clients...")
        http_client_manager = HTTPClientManager()
        http_client_manager.setup_clients()
        
        # Step 5: Initialize memory management
        logger.info("🧠 Initializing memory management...")
        memory_manager = MemoryManagementManager()
        memory_manager.initialize(self)
        
        # Step 6: Initialize main execution manager
        logger.info("⚙️ Initializing main execution...")
        main_execution_manager = MainExecutionManager()
        main_execution_manager.initialize(self)
        
        # Step 7: Initialize substrate manager
        logger.info("⛓️ Initializing substrate manager...")
        substrate_manager = SubstrateManager()
        substrate_manager.initialize(neuron_manager.substrate)
        
        # Step 8: Initialize background workers
        logger.info("👷 Initializing background workers...")  
        background_workers_manager = BackgroundWorkersManager()
        background_workers_manager.initialize(self)
        
        # Step 9: Initialize database operations
        logger.info("💽 Initializing database operations...")
        database_operations_manager = DatabaseOperationsManager()
        database_operations_manager.initialize(database_manager)
        
        # Step 10: Initialize weight manager (scoring)
        logger.info("⚖️ Initializing weight manager...")
        weight_manager = WeightManager()
        await weight_manager.initialize(
            neuron_manager, database_manager, memory_manager, main_execution_manager
        )
        
        # Sync critical attributes after neuron setup
        if hasattr(neuron_manager, 'metagraph') and neuron_manager.metagraph:
            logger.info("🔄 Syncing critical attributes after neuron setup...")
            self.metagraph = neuron_manager.metagraph
            self.substrate = neuron_manager.substrate
            self.keypair = neuron_manager.keypair
            self.validator_uid = neuron_manager.validator_uid
            self.wallet_name = neuron_manager.wallet_name
            self.hotkey_name = neuron_manager.hotkey_name
            self.subtensor_network = neuron_manager.subtensor_network
        
        components = ValidatorComponents(
            neuron_manager=neuron_manager,
            http_client_manager=http_client_manager,
            database_manager=database_manager,
            memory_manager=memory_manager,
            main_execution_manager=main_execution_manager,
            weight_manager=weight_manager,
            auto_sync_manager=auto_sync_manager,
            substrate_manager=substrate_manager,
            background_workers_manager=background_workers_manager,
            database_operations_manager=database_operations_manager
        )
        
        logger.info("✅ All systems initialized successfully")
        return components
    
    async def _run_main_loop(self) -> None:
        """Run the main validator loop using clean task orchestration."""
        
        # Start tracemalloc for memory analysis
        tracemalloc.start(25)
        logger.info("🔍 Memory analysis started")
        
        # Create all background tasks
        logger.info("📋 Creating background tasks...")
        self.background_tasks = await self._create_background_tasks()
        logger.info(f"✅ Created {len(self.background_tasks)} background tasks")
        
        # Create shutdown waiter
        shutdown_waiter = asyncio.create_task(self.shutdown_event.wait())
        all_tasks = self.background_tasks + [shutdown_waiter]
        
        # Main monitoring loop - clean and simple
        logger.info("🎯 Entering main monitoring loop...")
        try:
            while not self.shutdown_event.is_set():
                # Get currently running tasks
                current_tasks = [t for t in all_tasks if not t.done()]
                
                if not current_tasks:
                    logger.info("✅ All tasks completed")
                    break
                
                # Wait for first completion
                done, pending = await asyncio.wait(
                    current_tasks,
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Check if shutdown was signaled
                if self.shutdown_event.is_set() or shutdown_waiter.done():
                    logger.info("🛑 Shutdown signaled - exiting main loop")
                    break
                
                # Handle completed tasks
                for task in done:
                    if task in self.background_tasks:
                        await self._handle_completed_task(task)
        
        finally:
            # Clean shutdown of all tasks
            logger.info("🧹 Cleaning up background tasks...")
            await self._cleanup_tasks()
    
    async def _create_background_tasks(self) -> List[asyncio.Task]:
        """Create all background tasks using the existing managers."""
        if not self.components:
            return []
        
        tasks = []
        
        # Create scoring task
        scoring_task = asyncio.create_task(self._execute_scoring_task())
        scoring_task.set_name("scoring")
        tasks.append(scoring_task)
        
        # Create memory monitoring task
        memory_task = asyncio.create_task(self._execute_memory_monitoring())
        memory_task.set_name("memory_monitoring")
        tasks.append(memory_task)
        
        # Create database monitoring task
        db_monitor_task = asyncio.create_task(self._execute_database_monitoring())
        db_monitor_task.set_name("database_monitoring")
        tasks.append(db_monitor_task)
        
        # Create task execution tasks using the actual task system
        task_registry = TaskRegistry.get_all_tasks()
        
        for task_name, task_class in task_registry.items():
            if task_name in ['weather', 'geomagnetic', 'soil']:
                try:
                    # Create task instance
                    task_instance = TaskInstance(task_class)
                    
                    # Create execution task
                    exec_task = asyncio.create_task(
                        self._execute_task_instance(task_name, task_instance)
                    )
                    exec_task.set_name(f"task_{task_name}")
                    tasks.append(exec_task)
                    
                    logger.info(f"✅ Created task: {task_name}")
                except Exception as e:
                    logger.error(f"❌ Failed to create task {task_name}: {e}")
        
        return tasks
    
    async def _execute_scoring_task(self) -> None:
        """Execute scoring task using the weight manager."""
        while not self.shutdown_event.is_set():
            try:
                await self.update_task_status('scoring', 'active')
                
                # Use the weight manager for scoring
                success = await self.components.weight_manager.run_scoring_cycle()
                
                if success:
                    await self.update_task_status('scoring', 'idle')
                else:
                    await self.update_task_status('scoring', 'error')
                
                await asyncio.sleep(60)  # Wait before next scoring cycle
                
            except asyncio.CancelledError:
                logger.info("🛑 Scoring task cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in scoring task: {e}", exc_info=True)
                await self.update_task_status('scoring', 'error')
                await asyncio.sleep(60)
    
    async def _execute_memory_monitoring(self) -> None:
        """Execute memory monitoring task."""
        while not self.shutdown_event.is_set():
            try:
                if self.components.memory_manager:
                    await self.components.memory_manager._log_memory_usage()
                await asyncio.sleep(30)  # Monitor every 30 seconds
            except asyncio.CancelledError:
                logger.info("🛑 Memory monitoring task cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in memory monitoring: {e}")
                await asyncio.sleep(30)
    
    async def _execute_database_monitoring(self) -> None:
        """Execute database monitoring task."""
        while not self.shutdown_event.is_set():
            try:
                if self.components.database_operations_manager:
                    await self.components.database_operations_manager.monitor_database_performance()
                await asyncio.sleep(60)  # Monitor every minute
            except asyncio.CancelledError:
                logger.info("🛑 Database monitoring task cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in database monitoring: {e}")
                await asyncio.sleep(60)
    
    async def _execute_task_instance(self, task_name: str, task_instance: TaskInstance) -> None:
        """Execute a specific task instance."""
        try:
            # Create a validator interface for the task
            validator_interface = self._create_task_interface()
            
            # Execute the task
            await task_instance.execute(validator_interface)
            
        except asyncio.CancelledError:
            logger.info(f"🛑 Task {task_name} cancelled")
        except Exception as e:
            logger.error(f"❌ Error in task {task_name}: {e}", exc_info=True)
            await self.update_task_status(task_name, 'error')
    
    def _create_task_interface(self):
        """Create a clean interface for tasks that need validator-like object."""
        
        class TaskInterface:
            """Clean interface for task execution."""
            
            def __init__(self, validator):
                self.validator = validator
                
                # Expose necessary attributes for tasks
                self.database_manager = validator.components.database_manager
                self.substrate = validator.components.neuron_manager.substrate
                self.metagraph = validator.components.neuron_manager.metagraph
                self.keypair = validator.components.neuron_manager.keypair
                self.miner_client = validator.components.http_client_manager.miner_client
                self.api_client = validator.components.http_client_manager.api_client
                self.validator_uid = validator.components.neuron_manager.validator_uid
                self.test = getattr(validator.args, 'test', False)
                self.task_health = validator.task_health
            
            async def query_miners(self, payload, endpoint, hotkeys=None):
                """Delegate to HTTP client manager."""
                return await self.validator.components.http_client_manager.query_miners(
                    payload, endpoint, self.validator.components.neuron_manager, hotkeys
                )
            
            async def update_task_status(self, task_name, status, operation=None):
                """Update task status."""
                await self.validator.update_task_status(task_name, status, operation)
            
            def get_current_task_weights(self):
                """Get current task weights."""
                return self.validator.get_current_task_weights()
        
        return TaskInterface(self)
    
    async def _handle_completed_task(self, task: asyncio.Task) -> None:
        """Handle a completed background task."""
        task_name = task.get_name() or "unnamed"
        
        try:
            result = task.result()
            logger.warning(f"⚠️ Background task '{task_name}' completed unexpectedly")
        except asyncio.CancelledError:
            logger.info(f"✅ Background task '{task_name}' was cancelled")
        except Exception as e:
            logger.error(f"❌ Background task '{task_name}' failed: {e}", exc_info=True)
            await self.update_task_status(task_name, 'error')
    
    async def _cleanup_tasks(self) -> None:
        """Clean up all background tasks."""
        
        # Cancel all background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
        
        # Wait for all tasks to complete
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        logger.info("✅ All tasks cleaned up")
    
    async def _cleanup(self) -> None:
        """Clean up all validator resources."""
        if not self.components:
            return
        
        logger.info("🧹 Starting validator cleanup...")
        
        try:
            # Cleanup HTTP clients
            if self.components.http_client_manager:
                await self.components.http_client_manager.cleanup()
            
            # Cleanup memory
            if self.components.memory_manager:
                self.components.memory_manager._comprehensive_memory_cleanup("shutdown")
            
            logger.info("✅ Validator cleanup completed")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}", exc_info=True)
    
    # =============================================================================
    # BUSINESS LOGIC COMPATIBILITY METHODS
    # =============================================================================
    # These methods preserve 100% business logic compatibility with the original validator
    
    async def update_task_status(self, task_name: str, status: str, operation: str = None):
        """Update task status (business logic preservation)."""
        current_time = time.time()
        
        if task_name in self.task_health:
            self.task_health[task_name]['status'] = status
            if status in ['idle', 'completed']:
                self.task_health[task_name]['last_success'] = current_time
        
        if operation:
            logger.info(f"Task {task_name}: {status} ({operation})")
        else:
            logger.info(f"Task {task_name}: {status}")
    
    def get_current_task_weights(self):
        """Get current task weights (business logic preservation)."""
        # Task weights schedule from original validator
        now_utc = datetime.now(timezone.utc)
        
        # Default weights
        active_weights = {
            'weather': 0.70,
            'geomagnetic': 0.15,
            'soil': 0.15
        }
        
        return active_weights.copy()
    
    def _track_background_task(self, task_name: str, status: str):
        """Track background task status (business logic preservation)."""
        if task_name in self.task_health:
            self.task_health[task_name]['status'] = status
            if status == 'success':
                self.task_health[task_name]['last_success'] = time.time()
    
    def custom_serializer(self, obj):
        """Custom JSON serializer (business logic preservation)."""
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        else:
            return str(obj)
    
    async def _log_memory_usage(self):
        """Log memory usage (business logic preservation)."""
        if self.components and self.components.memory_manager:
            await self.components.memory_manager._log_memory_usage()
    
    async def _aggressive_substrate_cleanup(self):
        """Aggressive substrate cleanup (business logic preservation).""" 
        if self.components and self.components.memory_manager:
            self.components.memory_manager._aggressive_substrate_cleanup()
    
    # Property delegations for compatibility
    @property
    def wallet_name(self):
        return getattr(self.components.neuron_manager, 'wallet_name', None) if self.components else None
    
    @wallet_name.setter
    def wallet_name(self, value):
        if self.components:
            self.components.neuron_manager.wallet_name = value
    
    @property
    def hotkey_name(self):
        return getattr(self.components.neuron_manager, 'hotkey_name', None) if self.components else None
    
    @hotkey_name.setter
    def hotkey_name(self, value):
        if self.components:
            self.components.neuron_manager.hotkey_name = value
    
    @property
    def subtensor_network(self):
        return getattr(self.components.neuron_manager, 'subtensor_network', None) if self.components else None
    
    @property
    def substrate(self):
        return getattr(self.components.neuron_manager, 'substrate', None) if self.components else None
    
    @property
    def metagraph(self):
        return getattr(self.components.neuron_manager, 'metagraph', None) if self.components else None


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

async def main():
    """Clean main entry point."""
    parser = ArgumentParser()
    
    # Add arguments
    parser.add_argument("--wallet", type=str, help="Wallet name")
    parser.add_argument("--hotkey", type=str, help="Hotkey name")
    parser.add_argument("--netuid", type=int, default=19, help="Network UID")
    parser.add_argument("--subtensor.chain_endpoint", type=str, help="Subtensor endpoint")
    parser.add_argument("--test", action="store_true", help="Test mode")
    
    args = parser.parse_args()
    
    # Create and run the clean validator
    validator = CleanGaiaValidator(args)
    await validator.run()


if __name__ == "__main__":
    """Beautiful, simple entry point."""
    print("🚀 Starting Clean Gaia Validator")
    print("=" * 50)
    asyncio.run(main()) 