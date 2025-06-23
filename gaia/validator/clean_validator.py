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
from typing import Dict, Any, List, NamedTuple
from datetime import datetime, timezone
from fiber.logging_utils import get_logger

# === Configuration Management ===
from gaia.validator.core.config import (
    load_validator_config, 
    get_network_config,
    get_database_config,
    get_memory_config,
    get_task_config,
    TASK_WEIGHTS_SCHEDULE
)

# === Initialization Functions ===
from gaia.validator.core.initialization import (
    initialize_database_system,
    initialize_network_components,
    initialize_task_instances,
    initialize_additional_components
)

# === Network Operations ===
from gaia.validator.core.network import (
    query_miners,
    sync_metagraph,
    cleanup_idle_connections,
    monitor_client_health
)

# === Scoring Operations ===
from gaia.validator.core.scoring import (
    calculate_and_set_weights,
    fetch_scoring_data,
    compute_final_weights
)

# === System Management ===
from gaia.validator.core.system import (
    monitor_memory_usage,
    cleanup_resources,
    check_system_health,
    handle_graceful_shutdown
)

# === Task Orchestration ===
from gaia.validator.core.tasks import (
    create_background_tasks,
    monitor_task_health,
    update_task_status
)

logger = get_logger(__name__)


class ValidatorState(NamedTuple):
    """Complete validator state container."""
    config: Any
    network: Any
    database: Any
    tasks: Dict[str, Any]
    components: Dict[str, Any]
    shutdown_event: asyncio.Event


class CleanGaiaValidator:
    """
    Beautiful, clean validator implementation.
    
    This validator uses functional programming principles instead of heavy OOP.
    All operations are delegated to clean, pure functions organized by domain.
    """
    
    def __init__(self, args):
        """Initialize the clean validator with minimal setup."""
        logger.info("🚀 Starting Clean Gaia Validator")
        
        self.args = args
        self.state: ValidatorState = None
        
        # Setup signal handlers for graceful shutdown
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signal_name = signal.Signals(signum).name
        logger.info(f"🛑 Received {signal_name} - initiating graceful shutdown")
        
        if self.state and self.state.shutdown_event:
            self.state.shutdown_event.set()
    
    async def run(self) -> None:
        """
        Main validator execution - clean and simple.
        
        This is the only method you need to call to run the validator.
        Everything else is handled by clean, functional modules.
        """
        try:
            # Step 1: Load configuration
            logger.info("📋 Loading validator configuration...")
            config = load_validator_config(self.args)
            logger.info(f"✅ Configuration loaded (test_mode: {config.test_mode})")
            
            # Step 2: Initialize all systems
            logger.info("🔧 Initializing validator systems...")
            state = await self._initialize_systems(config)
            self.state = state
            
            # Step 3: Start main execution loop
            logger.info("🎯 Starting main execution loop...")
            await self._run_main_loop(state)
            
        except Exception as e:
            logger.error(f"❌ Critical error in validator: {e}", exc_info=True)
            raise
        finally:
            await self._cleanup()
    
    async def _initialize_systems(self, config) -> ValidatorState:
        """Initialize all validator systems using clean functional modules."""
        
        # Extract configuration for each domain
        network_config = get_network_config(config)
        database_config = get_database_config(config)
        memory_config = get_memory_config(config)
        task_config = get_task_config(config)
        
        # Initialize database system
        logger.info("💾 Initializing database system...")
        database = await initialize_database_system(database_config, config.test_mode)
        
        # Initialize network components
        logger.info("🌐 Initializing network components...")
        network = await initialize_network_components(network_config)
        
        # Initialize task instances
        logger.info("⚙️ Initializing task instances...")
        tasks = await initialize_task_instances(database, network, task_config)
        
        # Initialize additional components
        logger.info("🔧 Initializing additional components...")
        components = await initialize_additional_components(
            database, network, config
        )
        
        # Create shutdown event
        shutdown_event = asyncio.Event()
        
        # Create validator state
        state = ValidatorState(
            config=config,
            network=network,
            database=database,
            tasks=tasks,
            components=components,
            shutdown_event=shutdown_event
        )
        
        logger.info("✅ All systems initialized successfully")
        return state
    
    async def _run_main_loop(self, state: ValidatorState) -> None:
        """
        Run the main validator loop using clean task orchestration.
        
        This is where the magic happens - all the complex coordination
        is handled by clean, functional modules.
        """
        
        # Start tracemalloc for memory analysis
        tracemalloc.start(25)
        logger.info("🔍 Memory analysis started")
        
        # Create all background tasks using functional approach
        logger.info("📋 Creating background tasks...")
        background_tasks = await create_background_tasks(state)
        logger.info(f"✅ Created {len(background_tasks)} background tasks")
        
        # Create shutdown waiter
        shutdown_waiter = asyncio.create_task(state.shutdown_event.wait())
        all_tasks = background_tasks + [shutdown_waiter]
        
        # Main monitoring loop - clean and simple
        logger.info("🎯 Entering main monitoring loop...")
        try:
            while not state.shutdown_event.is_set():
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
                if state.shutdown_event.is_set() or shutdown_waiter.done():
                    logger.info("🛑 Shutdown signaled - exiting main loop")
                    break
                
                # Handle completed tasks
                for task in done:
                    if task in background_tasks:
                        await self._handle_completed_task(task, state)
        
        finally:
            # Clean shutdown of all tasks
            logger.info("🧹 Cleaning up background tasks...")
            await self._cleanup_tasks(background_tasks, shutdown_waiter)
    
    async def _handle_completed_task(self, task: asyncio.Task, state: ValidatorState) -> None:
        """Handle a completed background task."""
        task_name = task.get_name() or "unnamed"
        
        try:
            result = task.result()
            logger.warning(f"⚠️ Background task '{task_name}' completed unexpectedly")
        except asyncio.CancelledError:
            logger.info(f"✅ Background task '{task_name}' was cancelled")
        except Exception as e:
            logger.error(f"❌ Background task '{task_name}' failed: {e}", exc_info=True)
            
            # Update task health status
            await update_task_status(task_name, 'error', state)
    
    async def _cleanup_tasks(self, background_tasks: List[asyncio.Task], shutdown_waiter: asyncio.Task) -> None:
        """Clean up all background tasks."""
        
        # Cancel all background tasks
        for task in background_tasks:
            if not task.done():
                task.cancel()
        
        # Cancel shutdown waiter
        if shutdown_waiter and not shutdown_waiter.done():
            shutdown_waiter.cancel()
        
        # Wait for all tasks to complete
        all_tasks = background_tasks + ([shutdown_waiter] if shutdown_waiter else [])
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        
        logger.info("✅ All tasks cleaned up")
    
    async def _cleanup(self) -> None:
        """Clean up all validator resources."""
        if not self.state:
            return
        
        logger.info("🧹 Starting validator cleanup...")
        
        try:
            # Use functional cleanup module
            await cleanup_resources(self.state)
            logger.info("✅ Validator cleanup completed")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}", exc_info=True)


# =============================================================================
# CLEAN TASK DELEGATION FUNCTIONS
# =============================================================================
# These functions provide a clean interface for task execution
# They delegate to the original task classes while maintaining clean architecture

async def _execute_scoring_task(state: ValidatorState) -> None:
    """Execute scoring task using clean functional approach."""
    while not state.shutdown_event.is_set():
        try:
            await update_task_status('scoring', 'active', state)
            
            # Use functional scoring operations
            success = await calculate_and_set_weights(
                state.network, state.database, state.config
            )
            
            if success:
                await update_task_status('scoring', 'idle', state)
            else:
                await update_task_status('scoring', 'error', state)
            
            await asyncio.sleep(60)  # Wait before next scoring cycle
            
        except asyncio.CancelledError:
            logger.info("🛑 Scoring task cancelled")
            break
        except Exception as e:
            logger.error(f"❌ Error in scoring task: {e}", exc_info=True)
            await update_task_status('scoring', 'error', state)
            await asyncio.sleep(60)


async def _execute_geomagnetic_task(state: ValidatorState) -> None:
    """Execute geomagnetic task with clean delegation."""
    # Delegate to the actual geomagnetic task but with clean state management
    geomagnetic_task = state.tasks['geomagnetic']
    
    # Create a clean validator interface for the task
    validator_interface = _create_task_interface(state)
    
    try:
        await geomagnetic_task.validator_execute(validator_interface)
    except asyncio.CancelledError:
        logger.info("🛑 Geomagnetic task cancelled")
    except Exception as e:
        logger.error(f"❌ Error in geomagnetic task: {e}", exc_info=True)


async def _execute_soil_task(state: ValidatorState) -> None:
    """Execute soil task with clean delegation."""
    # Delegate to the actual soil task but with clean state management
    soil_task = state.tasks['soil']
    
    # Create a clean validator interface for the task
    validator_interface = _create_task_interface(state)
    
    try:
        await soil_task.validator_execute(validator_interface)
    except asyncio.CancelledError:
        logger.info("🛑 Soil task cancelled")
    except Exception as e:
        logger.error(f"❌ Error in soil task: {e}", exc_info=True)


async def _execute_weather_task(state: ValidatorState) -> None:
    """Execute weather task with clean delegation."""
    # Delegate to the actual weather task but with clean state management
    weather_task = state.tasks['weather']
    
    # Create a clean validator interface for the task
    validator_interface = _create_task_interface(state)
    
    try:
        await weather_task.validator_execute(validator_interface)
    except asyncio.CancelledError:
        logger.info("🛑 Weather task cancelled")
    except Exception as e:
        logger.error(f"❌ Error in weather task: {e}", exc_info=True)


def _create_task_interface(state: ValidatorState):
    """
    Create a clean interface for tasks that need validator-like object.
    
    This bridges the functional architecture with the existing task classes
    that expect a validator object.
    """
    
    class TaskInterface:
        """Clean interface for task execution."""
        
        def __init__(self, state: ValidatorState):
            self.state = state
            
            # Expose necessary attributes for tasks
            self.database_manager = state.database
            self.substrate = state.network.substrate
            self.metagraph = state.network.metagraph
            self.keypair = state.network.keypair
            self.miner_client = state.network.miner_client
            self.api_client = state.network.api_client
            self.validator_uid = state.network.validator_uid
            self.test = state.config.test_mode
            
            # Task health tracking (simplified)
            self.task_health = {
                'scoring': {'last_success': time.time(), 'status': 'idle'},
                'geomagnetic': {'last_success': time.time(), 'status': 'idle'},
                'soil': {'last_success': time.time(), 'status': 'idle'},
                'weather': {'last_success': time.time(), 'status': 'idle'},
            }
        
        async def query_miners(self, payload, endpoint, hotkeys=None):
            """Delegate to functional miner query."""
            return await query_miners(
                payload, endpoint, self.miner_client,
                self.metagraph, self.keypair, hotkeys, self.test
            )
        
        async def update_task_status(self, task_name, status, operation=None):
            """Update task status using functional approach."""
            await update_task_status(task_name, status, self.state, operation)
        
        def get_current_task_weights(self):
            """Get current task weights from schedule."""
            now_utc = datetime.now(timezone.utc)
            active_weights = TASK_WEIGHTS_SCHEDULE[0][1]
            
            for dt_threshold, weights_at_threshold in TASK_WEIGHTS_SCHEDULE:
                if now_utc >= dt_threshold:
                    active_weights = weights_at_threshold
                else:
                    break
            
            return active_weights.copy()
    
    return TaskInterface(state)


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

async def main():
    """Clean main entry point."""
    parser = ArgumentParser()
    
    # Add arguments
    parser.add_argument("--wallet", type=str, help="Wallet name")
    parser.add_argument("--hotkey", type=str, help="Hotkey name")
    parser.add_argument("--netuid", type=int, help="Network UID")
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