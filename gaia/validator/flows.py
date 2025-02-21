import sys
from argparse import ArgumentParser


def create_validator_args():
    parser = ArgumentParser()
    parser.add_argument('--test', action='store_true', help='Run in test mode')
    parser.add_argument('--netuid', type=int, default=1, help='Network UID')
    parser.add_argument('--subtensor.network', type=str, default='finney', help='Subtensor network')
    parser.add_argument('--logging.debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--neuron.name', type=str, default='validator', help='Neuron name')
    parser.add_argument('--wallet.name', type=str, default='validator', help='Wallet name')
    parser.add_argument('--wallet.hotkey', type=str, default='default', help='Wallet hotkey')
    args = parser.parse_args([])
    return args


from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner, ConcurrentTaskRunner
from datetime import datetime, timezone, timedelta
import asyncio
import anyio
from typing import Dict, List, Optional
from fiber.logging_utils import get_logger
import traceback
from prefect.deployments import run_deployment

logger = get_logger(__name__)

class ValidatorFlows:
    """Container for all validator flows."""
    _validator_instance = None
    _lock = None

    @staticmethod
    async def initialize_validator():
        """Initialize and return the singleton validator instance."""
        if ValidatorFlows._lock is None:
            from asyncio import Lock
            ValidatorFlows._lock = Lock()
        if ValidatorFlows._validator_instance is None:
            async with ValidatorFlows._lock:
                if ValidatorFlows._validator_instance is None:
                    from gaia.validator.validator import GaiaValidator
                    #TODO Use create_validator_args defined locally in this module to get default args
                    args = create_validator_args()
                    validator = GaiaValidator(args=args)
                    #TODO Call an initialization method
                    if hasattr(validator, 'ensure_initialized'):
                        await validator.ensure_initialized()
                    ValidatorFlows._validator_instance = validator
        return ValidatorFlows._validator_instance

    @staticmethod
    @flow(
        name="validator_core_flow",
        description="Core validator operations",
        retries=3,
        retry_delay_seconds=30
    )
    async def core_flow() -> None:
        """Single execution of core validator operations."""
        try:
            logger.info("Starting validator core flow execution")
            validator = await ValidatorFlows.initialize_validator()
            await validator.main()
            logger.info("Completed validator core flow execution")
        except Exception as e:
            logger.error(f"Error in core flow: {e}")
            logger.error(traceback.format_exc())
            raise

    @staticmethod
    @flow(
        name="scoring_flow",
        description="Weight calculation and setting",
        task_runner=ThreadPoolTaskRunner(),
        retries=3,
        retry_delay_seconds=30
    )
    async def scoring_flow() -> None:
        """Single execution of scoring logic."""
        try:
            logger.info("Starting scoring flow execution")
            validator = await ValidatorFlows.initialize_validator()
            await validator.main_scoring()
            logger.info("Completed scoring flow execution")
        except Exception as e:
            logger.error(f"Error in scoring flow: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @staticmethod
    @flow(
        name="task_processing_flow",
        description="Task execution management",
        task_runner=ThreadPoolTaskRunner(),
        retries=3,
        retry_delay_seconds=30
    )
    async def task_processing_flow() -> None:
        """Triggers the child flows using their deployments via run_deployment."""
        try:
            logger.info("Starting task processing flow by triggering child flows via run_deployment")
            validator = await ValidatorFlows.initialize_validator()
            
            geo_deployment = "geomagnetic-validator"
            soil_deployment = "soil-validator"
            
            results = await asyncio.gather(
                run_deployment(geo_deployment, parameters={}),
                run_deployment(soil_deployment, parameters={})
            )
            
            logger.info(f"Triggered child flows successfully: {results}")
            logger.info("Completed task processing execution")
        except Exception as e:
            logger.error(f"Error in task processing flow: {str(e)}")
            logger.error(traceback.format_exc())
            validator = await ValidatorFlows.initialize_validator()
            await validator.update_task_status('geomagnetic', 'error')
            await validator.update_task_status('soil', 'error')
            raise

    @staticmethod
    @flow(
        name="monitoring_flow",
        description="System monitoring and health checks",
        retries=3,
        retry_delay_seconds=30
    )
    async def monitoring_flow() -> None:
        """Single execution of monitoring operations."""
        try:
            logger.info("Starting monitoring execution")
            validator = await ValidatorFlows.initialize_validator()
            await validator.status_logger()
            await validator.check_for_updates()
            logger.info("Completed monitoring execution")
        except Exception as e:
            logger.error(f"Error in monitoring flow: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @staticmethod
    @flow(
        name="miner_query_flow",
        description="Parallel miner querying",
        task_runner=ConcurrentTaskRunner(),
        retries=3,
        retry_delay_seconds=30
    )
    async def miner_query_flow(payload: Dict, endpoint: str) -> Dict:
        """Single execution of miner querying."""
        try:
            logger.info(f"Starting miner query to endpoint: {endpoint}")
            validator = await ValidatorFlows.initialize_validator()
            result = await validator.query_miners(payload, endpoint)
            logger.info("Completed miner query")
            return result
        except Exception as e:
            logger.error(f"Error in miner query flow: {str(e)}")
            logger.error(traceback.format_exc())
            raise

# Export flow function
core_flow = ValidatorFlows.core_flow
scoring_flow = ValidatorFlows.scoring_flow
task_processing_flow = ValidatorFlows.task_processing_flow
monitoring_flow = ValidatorFlows.monitoring_flow
miner_query_flow = ValidatorFlows.miner_query_flow
