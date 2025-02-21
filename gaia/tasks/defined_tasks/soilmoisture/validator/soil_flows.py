from datetime import datetime, timezone, timedelta
import asyncio
import traceback
from prefect import flow
from fiber.logging_utils import get_logger
from gaia.parallel.core.executor import get_task_runner
from gaia.parallel.config.settings import TaskType

logger = get_logger(__name__)

@flow(name="soil_moisture_prep_flow", 
      retries=3, 
      retry_delay_seconds=300, 
      timeout_seconds=1800, 
      description="Prep flow: fetch and preprocess soil and SMAP data")
async def soil_moisture_prep_flow(self):
    try:
        current_time = datetime.now(timezone.utc)
        next_prep_time = await self.get_next_preparation_time(current_time)
        if next_prep_time is None:
            logger.info("No upcoming prep window found, skipping preparation.")
            return
        regions = await self.get_todays_regions(next_prep_time)
        if not regions:
            logger.info("No regions to process in prep flow.")
            return

        task_runner = get_task_runner(TaskType.IO_BOUND)
        futures = []
        for region in regions:
            future = task_runner.submit(
                self._process_region,
                region,
                next_prep_time,
                current_time,
                key=f"prep_region_{region['id']}_{next_prep_time.isoformat()}"
            )
            futures.append(future)
        results = await asyncio.gather(*futures)
        logger.info(f"Prep flow processed {len(results)} regions.")
    except Exception as e:
        logger.error(f"Error in soil_moisture_prep_flow: {str(e)}")
        logger.error(traceback.format_exc())
        raise

@flow(name="soil_moisture_execute_flow", 
      retries=3, 
      retry_delay_seconds=300, 
      timeout_seconds=1800, 
      description="Execution flow: query miners and process soil predictions")
async def soil_moisture_execute_flow(self):
    try:
        current_time = datetime.now(timezone.utc)
        task_runner = get_task_runner(TaskType.CPU_BOUND)
        pending_tasks = await self.get_pending_tasks()
        if not pending_tasks:
            logger.info("No pending tasks for execution flow.")
            return

        futures = []
        for task in pending_tasks:
            future = task_runner.submit(
                self._process_prediction,
                task["prediction"],
                task,
                task["smap_data"],
                key=f"execute_prediction_{task['id']}_{current_time.isoformat()}"
            )
            futures.append(future)
        results = await asyncio.gather(*futures)
        logger.info(f"Execution flow processed {len(results)} predictions.")
    except Exception as e:
        logger.error(f"Error in soil_moisture_execute_flow: {str(e)}")
        logger.error(traceback.format_exc())
        raise

@flow(name="soil_moisture_scoring_flow", 
      retries=3, 
      retry_delay_seconds=300, 
      timeout_seconds=7200, 
      description="Scoring flow: score soil moisture predictions")
async def soil_moisture_scoring_flow(self):
    try:
        task_runner = get_task_runner(TaskType.CPU_BOUND)
        current_time = datetime.now(timezone.utc)
        scoring_time = current_time - self.scoring_delay

        scoring_tasks = await self.get_tasks_for_scoring(scoring_time)
        if not scoring_tasks:
            logger.info("No tasks ready for scoring in scoring flow.")
            return

        futures = []
        for task in scoring_tasks:
            future = task_runner.submit(
                self.score_task,
                task,
                key=f"score_task_{task['id']}_{current_time.isoformat()}"
            )
            futures.append(future)
        results = await asyncio.gather(*futures)
        logger.info(f"Scoring flow processed scores for {len(results)} tasks.")
        await self.build_score_row(scoring_time, results)
    except Exception as e:
        logger.error(f"Error in soil_moisture_scoring_flow: {str(e)}")
        logger.error(traceback.format_exc())
        raise

@flow(name="soil_validator_workflow", 
      description="Unified entry point for soil moisture validator task", 
      retries=3)
async def soil_validator_workflow(self, validator):
    """Single entry point that runs the prep, execute, and scoring flows."""
    try:
        await soil_moisture_prep_flow(self)
        await soil_moisture_execute_flow(self)
        await soil_moisture_scoring_flow(self)
    except Exception as e:
        logger.error(f"Error in soil_validator_workflow: {str(e)}")
        logger.error(traceback.format_exc())
        raise