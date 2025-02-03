from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner, ConcurrentTaskRunner
from datetime import datetime, timezone, timedelta
import asyncio
import anyio
from typing import Dict, List, Optional
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class ValidatorFlows:
    """
    Flow orchestration layer for the Gaia Validator.
    
    This class is responsible for:
    1. Defining the flow structure and dependencies
    2. Providing Prefect observability for operations
    3. Managing task runners and concurrency
    4. Error handling and retries at the flow level
    
    It does NOT:
    1. Implement core business logic (that stays in validator.py)
    2. Handle data storage (remains in database_manager)
    3. Manage state (validator.py maintains state)
    """

    @flow(
        name="validator_core_flow",
        description="Core validator operations"
    )
    async def core_flow(self, validator) -> None:
        """
        Main flow orchestrating core validator operations.
        Provides observability while preserving original timing logic.
        """
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self.task_processing_flow, validator)
                tg.start_soon(self.scoring_flow, validator)
                tg.start_soon(self.monitoring_flow, validator)
                
        except Exception as e:
            logger.error(f"Error in core flow: {e}")
            raise

    @flow(
        name="scoring_flow",
        description="Weight calculation and setting",
        task_runner=ThreadPoolTaskRunner()
    )
    async def scoring_flow(self, validator) -> None:
        """Wraps the scoring logic for observability while preserving block timing."""
        while not validator._shutdown_event.is_set():
            try:
                await validator.main_scoring()
                await asyncio.sleep(12)
            except Exception as e:
                logger.error(f"Error in scoring flow: {e}")
                await asyncio.sleep(12)

    @flow(
        name="task_processing_flow",
        description="Task execution management"
    )
    async def task_processing_flow(self, validator) -> None:
        """Manages task execution while providing observability."""
        try:
            tasks = [
                validator.geomagnetic_task.execute_flow(validator),
                validator.soil_task.validator_execute(validator)
            ]
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Error in task processing: {e}")
            raise

    @flow(
        name="monitoring_flow",
        description="System monitoring and health checks"
    )
    async def monitoring_flow(self, validator) -> None:
        """Wraps monitoring operations for observability."""
        while not validator._shutdown_event.is_set():
            try:
                await asyncio.gather(
                    validator.status_logger(),
                    validator.check_for_updates()
                )
            except Exception as e:
                logger.error(f"Error in monitoring flow: {e}")
                await asyncio.sleep(60)

    @flow(
        name="miner_query_flow",
        description="Parallel miner querying",
        task_runner=ConcurrentTaskRunner()
    )
    async def miner_query_flow(self, validator, payload: Dict, endpoint: str) -> Dict:
        """Wraps miner querying for observability."""
        try:
            return await validator.query_miners(payload, endpoint)
        except Exception as e:
            logger.error(f"Error in miner query flow: {e}")
            raise
