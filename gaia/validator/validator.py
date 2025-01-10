from datetime import datetime, timezone, timedelta
import os
import time
import asyncio
import traceback
from typing import Any, Optional, List, Dict, Set
from argparse import ArgumentParser

from fiber.logging_utils import get_logger
from fiber.chain.metagraph import Metagraph
from substrateinterface import SubstrateInterface, Keypair

from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask
from gaia.APIcalls.miner_score_sender import MinerScoreSender

from gaia.validator.core.config_manager import ConfigManager
from gaia.validator.core.validator_state import ValidatorState
from gaia.validator.core.constants import (
    WATCHDOG_TIMEOUT,
    MAIN_LOOP_TIMEOUT,
    SCORING_CYCLE_TIMEOUT,
    NETWORK_OP_TIMEOUT,
    DB_OP_TIMEOUT,
    STATUS_UPDATE_TIMEOUT,
    MAIN_LOOP_SLEEP,
    WATCHDOG_SLEEP,
    SCORING_SLEEP,
    STATUS_SLEEP,
    DEFAULT_CHAIN_ENDPOINT,
    DEFAULT_NETWORK,
    DEFAULT_NETUID,
    DEFAULT_WALLET,
    DEFAULT_HOTKEY
)

from gaia.validator.components.miner_manager import MinerManager
from gaia.validator.components.weight_manager import WeightManager
from gaia.validator.network.network_manager import NetworkManager
from gaia.validator.network.metagraph_manager import MetagraphManager
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.validator.database.schema_manager import SchemaManager
from gaia.validator.monitoring.watchdog import Watchdog
from gaia.validator.monitoring.status_monitor import StatusMonitor
from gaia.validator.monitoring.metrics_collector import MetricsCollector
from gaia.validator.utils.task_decorator import task_step

logger = get_logger(__name__)

class GaiaValidatorV2:
    """
    Enhanced Gaia Validator implementation with improved architecture.
    
    This class coordinates all validator components and manages the main validation loop.
    It maintains backwards compatibility while providing enhanced features:
    
    Key Improvements:
    - Modular component architecture
    - Enhanced error handling and recovery
    - Improved monitoring and metrics
    - Better state management
    - More robust database operations
    - Comprehensive logging
    
    Components:
    - Network: Chain and metagraph interactions
    - Database: Data persistence and schema management
    - Monitoring: Health checks and metrics
    - Tasks: Geomagnetic and soil moisture validation
    - State: Validator registration and configuration
    """
    
    def __init__(self, args):
        """
        Initialize the enhanced validator with provided arguments.
        
        Args:
            args: Command line arguments including:
                - wallet: Wallet name
                - hotkey: Hotkey name
                - netuid: Network UID
                - test_soil: Flag for soil task testing
                - subtensor: Chain endpoint configuration
        """
        self.args = args
        
        # Initialize configuration
        self.config = ConfigManager()
        self._load_config_from_args()
        
        # Initialize core components
        self.network_manager = NetworkManager(
            chain_endpoint=self.chain_endpoint,
            network=self.network
        )
        
        self.database_manager = ValidatorDatabaseManager(
            host=self.config.get("database.host"),
            port=self.config.get("database.port"),
            database=self.config.get("database.name"),
            user=self.config.get("database.user"),
            password=self.config.get("database.password")
        )
        
        self.schema_manager = SchemaManager(self.database_manager)
        
        # Initialize task components
        self.soil_task = SoilMoistureTask(
            db_manager=self.database_manager,
            node_type="validator",
            test_mode=args.test_soil
        )
        self.geomagnetic_task = GeomagneticTask(
            db_manager=self.database_manager
        )
        
        # Initialize state tracking
        self.metagraph_manager: Optional[MetagraphManager] = None
        self.validator_state: Optional[ValidatorState] = None
        self.miner_manager: Optional[MinerManager] = None
        self.weight_manager: Optional[WeightManager] = None
        
        # Initialize monitoring
        self.watchdog: Optional[Watchdog] = None
        self.status_monitor: Optional[StatusMonitor] = None
        self.metrics_collector: Optional[MetricsCollector] = None
        
        # Initialize auxiliary components
        self.miner_score_sender = MinerScoreSender(
            database_manager=self.database_manager,
            loop=asyncio.get_event_loop()
        )
        
        # Task management
        self._worker_tasks: Set[asyncio.Task] = set()
        self._running = False
        self._initialized = False
        
    def _load_config_from_args(self):
        """Load configuration from command line arguments."""
        # Network configuration
        self.chain_endpoint = (
            self.args.subtensor.chain_endpoint
            if hasattr(self.args, "subtensor") and self.args.subtensor.chain_endpoint
            else self.config.get("chain.endpoint", DEFAULT_CHAIN_ENDPOINT)
        )
        
        self.network = (
            self.args.subtensor.network
            if hasattr(self.args, "subtensor") and self.args.subtensor.network
            else self.config.get("chain.network", DEFAULT_NETWORK)
        )
        
        self.netuid = (
            self.args.netuid
            if self.args.netuid is not None
            else self.config.get("chain.netuid", DEFAULT_NETUID)
        )
        
        # Wallet configuration
        self.wallet_name = (
            self.args.wallet
            if self.args.wallet
            else self.config.get("wallet.name", DEFAULT_WALLET)
        )
        
        self.hotkey_name = (
            self.args.hotkey
            if self.args.hotkey
            else self.config.get("wallet.hotkey", DEFAULT_HOTKEY)
        ) 

    @task_step(
        name="initialize_components",
        description="Initialize all validator components",
        timeout_seconds=NETWORK_OP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=10
    )
    async def initialize_components(self) -> bool:
        """Initialize all validator components."""
        try:
            # Initialize network components first
            if not await self.network_manager.connect():
                logger.error("Failed to connect to network")
                return False
                
            # Load keypair
            try:
                keypair = Keypair(
                    ss58_address=self.wallet_name,
                    crypto_type=2
                )
                setattr(self.network_manager, "keypair", keypair)
            except Exception as e:
                logger.error(f"Failed to load keypair: {e}")
                return False
                
            self.metagraph_manager = MetagraphManager(
                network_manager=self.network_manager,
                netuid=self.netuid
            )
            
            if not await self.metagraph_manager.sync_metagraph():
                logger.error("Failed to sync metagraph")
                return False
                
            # Initialize state tracking
            self.validator_state = ValidatorState(
                network_manager=self.network_manager,
                metagraph_manager=self.metagraph_manager,
                database_manager=self.database_manager,
                wallet_name=self.wallet_name,
                hotkey_name=self.hotkey_name,
                netuid=self.netuid
            )
            
            if not await self.validator_state.load_state():
                logger.warning("Failed to load validator state")
            
            # Initialize managers
            if not self.network_manager.substrate:
                logger.error("Substrate not initialized")
                return False

            self.miner_manager = MinerManager(
                database_manager=self.database_manager,
                metagraph=self.metagraph_manager.metagraph,
                substrate=self.network_manager.substrate,
                soil_task=self.soil_task,
                geomagnetic_task=self.geomagnetic_task
            )
            
            self.weight_manager = WeightManager(
                database_manager=self.database_manager,
                metagraph=self.metagraph_manager.metagraph,
                substrate=self.network_manager.substrate,
                netuid=self.netuid,
                wallet_name=self.wallet_name,
                hotkey_name=self.hotkey_name,
                network=self.network
            )
            
            # Setup monitoring
            self.status_monitor = StatusMonitor(
                metagraph=self.metagraph_manager.metagraph,
                substrate=self.network_manager.substrate,
                chain_endpoint=self.chain_endpoint
            )
            
            self.watchdog = Watchdog(
                database_manager=self.database_manager,
                network_manager=self.network_manager,
                status_monitor=self.status_monitor
            )
            
            self.metrics_collector = MetricsCollector(
                database_manager=self.database_manager,
                watchdog=self.watchdog,
                status_monitor=self.status_monitor
            )
            
            self._initialized = True
            logger.info("Successfully initialized all components")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing components: {e}")
            logger.error(traceback.format_exc())
            return False
            
    @task_step(
        name="setup_neuron",
        description="Setup validator neuron and verify registration",
        timeout_seconds=NETWORK_OP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=10
    )
    async def setup_neuron(self) -> bool:
        """
        Setup the validator neuron and verify registration.
        
        This method:
        1. Loads keypair
        2. Checks registration
        3. Registers if needed
        4. Verifies validator permit
        
        Returns:
            bool: True if setup successful
        """
        try:
            if not self._initialized:
                logger.error("Components not initialized")
                return False
                
            if not self.network_manager.keypair:
                logger.error("Keypair not initialized")
                return False
                
            if not self.validator_state:
                logger.error("Validator state not initialized")
                return False
                
            # Check registration
            registered = await self.validator_state.check_registration(
                self.network_manager.keypair
            )
            
            if not registered:
                logger.warning("Validator not registered, attempting registration...")
                registered = await self.validator_state.register_validator(
                    self.network_manager.keypair
                )
                
                if not registered:
                    logger.error("Failed to register validator")
                    return False
                    
            # Verify validator permit
            if not self.validator_state.validator_permit:
                logger.error("Validator does not have validator permit")
                return False
                
            # Save state after successful setup
            await self.validator_state.save_state()
                
            logger.info(
                f"Validator setup complete - "
                f"UID: {self.validator_state.uid}, "
                f"Stake: {self.validator_state.stake}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error setting up neuron: {e}")
            logger.error(traceback.format_exc())
            return False 

    @task_step(
        name="create_worker_tasks",
        description="Create all worker tasks",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _create_worker_tasks(self) -> bool:
        """
        Create all worker tasks for the validator.
        
        Returns:
            bool: True if tasks created successfully
        """
        try:
            if not self._initialized:
                logger.error("Components not initialized")
                return False
                
            if not all([
                self.watchdog,
                self.status_monitor,
                self.metrics_collector,
                self.miner_manager
            ]):
                logger.error("Required components not initialized")
                return False
                
            tasks = [
                # Core tasks
                asyncio.create_task(self.geomagnetic_task.validator_execute(self)),  # type: ignore
                asyncio.create_task(self.soil_task.validator_execute(self)),  # type: ignore
                asyncio.create_task(self.miner_score_sender.run_async()),  # type: ignore
                
                # Monitoring tasks
                asyncio.create_task(self.watchdog.start_monitoring()),  # type: ignore
                asyncio.create_task(self.status_monitor.start_monitoring()),  # type: ignore
                asyncio.create_task(self.metrics_collector.start_collection()),  # type: ignore
                
                # State management tasks
                asyncio.create_task(self.miner_manager.check_deregistrations()),  # type: ignore
                asyncio.create_task(self._run_scoring_cycle())
            ]
            
            for task in tasks:
                self._worker_tasks.add(task)
                task.add_done_callback(self._worker_tasks.discard)
                
            return True
            
        except Exception as e:
            logger.error(f"Error creating worker tasks: {e}")
            logger.error(traceback.format_exc())
            return False
        
    @task_step(
        name="run_scoring_cycle",
        description="Run continuous scoring and weight setting cycle",
        timeout_seconds=SCORING_CYCLE_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=15
    )
    async def _run_scoring_cycle(self):
        """
        Run the scoring cycle with proper timeout and retry handling.
        This method coordinates scoring tasks and weight setting.
        """
        try:
            while self._running:
                if not all([
                    self.validator_state,
                    self.weight_manager,
                    self.network_manager.keypair
                ]):
                    logger.error("Required components not initialized")
                    await asyncio.sleep(SCORING_SLEEP)
                    continue
                    
                # Get validator UID
                validator_uid = await self.validator_state.check_registration(  # type: ignore
                    self.network_manager.keypair
                )
                if not validator_uid:
                    logger.error("Validator not registered")
                    await asyncio.sleep(SCORING_SLEEP)
                    continue
                
                # Check if we can set weights
                blocks_since_update, min_interval = await self.weight_manager.check_weight_setting_interval(  # type: ignore
                    validator_uid
                )
                if blocks_since_update < min_interval:
                    logger.info(f"Too soon to set weights. Blocks since update: {blocks_since_update}, Min interval: {min_interval}")
                    await asyncio.sleep(SCORING_SLEEP)
                    continue
                    
                # Verify permission
                can_set = await self.weight_manager.verify_weight_setting_permission(  # type: ignore
                    validator_uid
                )
                if not can_set:
                    logger.warning("No permission to set weights")
                    await asyncio.sleep(SCORING_SLEEP)
                    continue
                    
                # Calculate and set weights
                weights = await self._calculate_weights()
                if weights:
                    success = await self._set_weights(weights)
                    if success:
                        await self.weight_manager.update_last_weights_block()  # type: ignore
                        logger.info("Successfully set weights")
                        
                await asyncio.sleep(SCORING_SLEEP)
                
        except Exception as e:
            logger.error(f"Error in scoring cycle: {e}")
            logger.error(traceback.format_exc())
            
        finally:
            logger.info("Scoring cycle stopped")

    @task_step(
        name="calculate_weights",
        description="Calculate weights based on task scores",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _calculate_weights(self) -> Optional[Dict[str, float]]:
        """
        Calculate weights based on task performance scores.
        
        Returns:
            Optional[Dict[str, float]]: Calculated weights if successful
        """
        try:
            if not self.weight_manager:
                logger.error("Weight manager not initialized")
                return None
                
            # Get task scores
            soil_scores, geo_scores = await self.weight_manager.fetch_task_scores()  # type: ignore
            
            if not soil_scores and not geo_scores:
                logger.error("No valid scores available")
                return None
                
            # Calculate combined weights
            raw_weights = await self.weight_manager.calculate_weights(  # type: ignore
                soil_scores=soil_scores,
                geo_scores=geo_scores
            )
            
            if not raw_weights:
                logger.error("Failed to calculate weights")
                return None
                
            # Convert list to dictionary
            if not self.metagraph_manager or not self.metagraph_manager.metagraph:
                logger.error("Metagraph not initialized")
                return None
                
            # Map weights to hotkeys
            weights = {}
            nodes = getattr(self.metagraph_manager.metagraph, "nodes", {})
            for idx, weight in enumerate(raw_weights):
                if idx >= len(nodes):
                    break
                hotkey = list(nodes.keys())[idx]
                weights[hotkey] = weight
                
            return weights
            
        except Exception as e:
            logger.error(f"Error calculating weights: {e}")
            logger.error(traceback.format_exc())
            return None

    @task_step(
        name="set_weights",
        description="Set calculated weights on the network",
        timeout_seconds=NETWORK_OP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=10
    )
    async def _set_weights(self, weights: Dict[str, float]) -> bool:
        """
        Set the calculated weights on the network.
        
        Args:
            weights: Dictionary mapping hotkeys to weights
            
        Returns:
            bool: True if weights set successfully
        """
        try:
            if not weights:
                logger.error("No weights provided")
                return False
                
            if not self.metagraph_manager or not self.network_manager.keypair:
                logger.error("Required components not initialized")
                return False
                
            success = await self.metagraph_manager.set_weights(
                weights,
                self.network_manager.keypair
            )
            
            if not success:
                logger.error("Failed to set weights on network")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error setting weights: {e}")
            logger.error(traceback.format_exc())
            return False

    @task_step(
        name="update_miner_scores",
        description="Update miner scores in database",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _update_miner_scores(self) -> bool:
        """
        Update miner scores in the database.
        
        Returns:
            bool: True if scores updated successfully
        """
        try:
            if not self.miner_manager:
                logger.error("Miner manager not initialized")
                return False
                
            # Get latest scores
            soil_scores = await self.soil_task.validator_score()  # type: ignore
            geo_scores = await self.geomagnetic_task.validator_score()  # type: ignore
            
            if not soil_scores and not geo_scores:
                logger.error("No valid scores to update")
                return False
                
            # Update database
            await self.miner_manager.update_scores(  # type: ignore
                soil_scores=soil_scores,
                geo_scores=geo_scores
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating miner scores: {e}")
            logger.error(traceback.format_exc())
            return False

    @task_step(
        name="run_main_loop",
        description="Run the main validator loop with proper timeout handling",
        timeout_seconds=MAIN_LOOP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=15
    )
    async def _run_main_loop(self):
        """
        Run the main validator loop with proper timeout and retry handling.
        This is the core loop that coordinates all validator activities.
        """
        try:
            self._running = True
            
            # Create worker tasks
            if not await self._create_worker_tasks():
                logger.error("Failed to create worker tasks")
                return
                
            while self._running:
                # Run health checks
                await self._check_system_health()
                
                if not all([
                    self.metagraph_manager,
                    self.validator_state,
                    self.metrics_collector,
                    self.status_monitor,
                    self.network_manager.keypair
                ]):
                    logger.error("Required components not initialized")
                    await asyncio.sleep(MAIN_LOOP_SLEEP)
                    continue
                
                # Update metagraph and sync with network
                if not await self.metagraph_manager.sync_metagraph():  # type: ignore
                    logger.error("Failed to sync metagraph")
                    await asyncio.sleep(MAIN_LOOP_SLEEP)
                    continue
                
                # Check registration and update state
                validator_uid = await self.validator_state.check_registration(  # type: ignore
                    self.network_manager.keypair
                )
                if not validator_uid:
                    logger.error("Validator not registered")
                    await asyncio.sleep(MAIN_LOOP_SLEEP)
                    continue
                
                # Update metrics and status
                await self.metrics_collector.collect_metrics()  # type: ignore
                await self.status_monitor.log_status_update()  # type: ignore
                
                await asyncio.sleep(MAIN_LOOP_SLEEP)
                
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            logger.error(traceback.format_exc())
            
        finally:
            self._running = False
            await self._cancel_worker_tasks()

    async def _cancel_worker_tasks(self):
        """Cancel all worker tasks."""
        try:
            for task in self._worker_tasks:
                if not task.done():
                    task.cancel()
                    
            # Wait for tasks to complete
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
            self._worker_tasks.clear()
            
        except Exception as e:
            logger.error(f"Error cancelling worker tasks: {e}")
            logger.error(traceback.format_exc())

    @task_step(
        name="check_system_health",
        description="Check overall system health including components",
        timeout_seconds=STATUS_UPDATE_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _check_system_health(self) -> bool:
        """
        Check the health of all system components and perform recovery if needed.
        
        Returns:
            bool: True if all components are healthy
        """
        try:
            # Check database health
            if not await self.database_manager.check_connection():  # type: ignore
                logger.warning("Database connection lost - attempting reset")
                await self.database_manager.reset_pool()  # type: ignore
            
            # Check network connectivity
            if not self.network_manager.is_connected():
                logger.warning("Network connection lost - attempting reconnect")
                await self.network_manager.connect()
            
            if not all([
                self.watchdog,
                self.status_monitor
            ]):
                logger.error("Required monitoring components not initialized")
                return False
            
            # Check component health
            await self.watchdog.check_components()  # type: ignore
            if not await self.status_monitor.check_health():  # type: ignore
                logger.warning("Status monitor health check failed")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            logger.error(traceback.format_exc())
            return False

    @task_step(
        name="cleanup",
        description="Clean up resources and connections",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _cleanup(self) -> bool:
        """
        Clean up resources and connections with proper timeout handling.
        
        Returns:
            bool: True if cleanup successful
        """
        try:
            # Cancel worker tasks
            await self._cancel_worker_tasks()
            
            # Close database connections
            await self.database_manager.close()
            
            # Close network connections
            await self.network_manager.close()
            
            # Stop monitoring tasks
            if self.watchdog:
                await self.watchdog.stop()
            if self.status_monitor:
                await self.status_monitor.stop()
            if self.metrics_collector:
                await self.metrics_collector.stop()
            
            # Close any remaining connections
            try:
                await self.miner_score_sender.close()  # type: ignore
            except Exception as e:
                logger.warning(f"Error closing miner score sender: {e}")
            
            logger.info("Successfully cleaned up all resources")
            return True
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            logger.error(traceback.format_exc())
            return False

    async def main(self):
        """
        Main entry point for the validator.
        Coordinates initialization, main loop, and cleanup.
        """
        try:
            # Initialize components
            if not await self.initialize_components():
                logger.error("Failed to initialize components")
                return
            
            # Setup neuron
            if not await self.setup_neuron():
                logger.error("Failed to setup neuron")
                return
            
            # Run main loop
            await self._run_main_loop()
            
        except Exception as e:
            logger.error(f"Error in main: {e}")
            logger.error(traceback.format_exc())
            
        finally:
            # Ensure cleanup happens
            await self._cleanup()

if __name__ == "__main__":
    parser = ArgumentParser()
    
    # Add arguments
    parser.add_argument(
        "--wallet",
        type=str,
        help="Name of the wallet to use"
    )
    parser.add_argument(
        "--hotkey",
        type=str,
        help="Name of the hotkey to use"
    )
    parser.add_argument(
        "--netuid",
        type=int,
        help="Network UID to use"
    )
    parser.add_argument(
        "--test-soil",
        action="store_true",
        help="Run soil moisture task in test mode"
    )
    
    # Add subtensor group
    subtensor_group = parser.add_argument_group("subtensor")
    subtensor_group.add_argument(
        "--subtensor.chain_endpoint",
        type=str,
        help="Chain endpoint to use"
    )
    subtensor_group.add_argument(
        "--subtensor.network",
        type=str,
        help="Network to use (finney/test)"
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Run validator
    validator = GaiaValidatorV2(args)
    asyncio.run(validator.main()) 