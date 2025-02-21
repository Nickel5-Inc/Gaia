import asyncio
import datetime
import traceback
from datetime import timedelta

from prefect import flow
from gaia.parallel.core.executor import get_task_runner
from gaia.parallel.config.settings import TaskType
from fiber.logging_utils import get_logger

from gaia.tasks.defined_tasks.geomagnetic.validator.geomagnetic_prefect_tasks import fetch_geomag_data
from gaia.tasks.defined_tasks.geomagnetic.validator.geomagnetic_prefect_tasks import query_miners, process_scores

logger = get_logger(__name__)

@flow(name="validator_prepare", retries=2, task_runner=get_task_runner(TaskType.IO_BOUND))
async def prepare_flow(validator) -> dict:
    """Prepare the validator task by checking if we're at the top of the hour and fetching geomagnetic data."""
    try:
        current_time = datetime.datetime.now(datetime.timezone.utc)
        
        if not validator.test_mode:
            if current_time.minute >= 2:
                logger.info("Not within the first 2 minutes of the hour, skipping execution")
                return None
            
        next_hour = current_time.replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(hours=1)

        logger.info(f"Preparing execution for current hour: {current_time.isoformat()}")
        logger.info(f"Next hour will be: {next_hour.isoformat()}")

        # Fetch and preprocess geomagnetic data
        data_future = fetch_geomag_data.submit()
        timestamp, dst_value, historical_data = await data_future

        prepared_data = {
            "timestamp": timestamp,
            "dst_value": dst_value,
            "historical_data": historical_data
        }

        return {
            "current_time": current_time,
            "next_hour": next_hour,
            "geomag_data": prepared_data
        }
    except Exception as e:
        logger.error(f"Error in prepare_flow: {e}")
        logger.error(traceback.format_exc())
        raise

@flow(name="geomagnetic_execute", task_runner=get_task_runner(TaskType.MIXED))
async def execute_flow(validator, prepared_data):
    """Execute the main geomagnetic validator workflow using prepared data to send miner queries and process scores."""
    try:
        if prepared_data is None:
            logger.info("No prepared data available, skipping execution")
            return

        geomag_data = prepared_data.get("geomag_data", {})
        timestamp = geomag_data.get("timestamp", "N/A")
        dst_value = geomag_data.get("dst_value", "N/A")
        historical_data = geomag_data.get("historical_data")

        await validator.update_task_status('geomagnetic', 'active')
        await validator.update_task_status('geomagnetic', 'processing', 'miner_query')

        current_time = datetime.datetime.now(datetime.timezone.utc)
        next_hour = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

        if timestamp != "N/A" and dst_value != "N/A":
            miner_query_future = query_miners.submit(validator, timestamp, dst_value, historical_data, next_hour)
            responses_count = await miner_query_future
            logger.info(f"Collected {responses_count} predictions at hour {next_hour}")

        previous_hour = next_hour - timedelta(hours=1)
        await validator.update_task_status('geomagnetic', 'processing', 'scoring')

        score_future = process_scores.submit(validator, previous_hour)
        total_processed, success_count = await score_future
        logger.info(f"Processed {total_processed} predictions, {success_count} successful")

        await validator.update_task_status('geomagnetic', 'idle')
    except Exception as e:
        logger.error(f"Error in execute_flow: {str(e)}")
        logger.error(traceback.format_exc())
        await validator.update_task_status('geomagnetic', 'error')

@flow(name="validator_score", retries=2, task_runner=get_task_runner(TaskType.CPU_BOUND))
async def score_flow(validator, query_hour: datetime.datetime) -> None:
    """Score predictions for a given hour using Dask for parallel processing. This flow scores predictions collected in the execute_flow."""
    try:
        ground_truth_future = validator.fetch_ground_truth.submit()
        ground_truth_value = await ground_truth_future
        
        if ground_truth_value is None:
            logger.warning("Ground truth data not available. Skipping scoring.")
            return

        hour_start = query_hour
        hour_end = query_hour + timedelta(hours=1)

        logger.info(
            f"Scoring predictions collected between {hour_start} and {hour_end}"
        )
        
        tasks_future = validator.get_tasks_for_hour.submit(hour_start, hour_end, validator)
        tasks = await tasks_future
        
        if not tasks:
            logger.info(f"No predictions found for collection hour {query_hour}")
            return

        current_time = datetime.datetime.now(datetime.timezone.utc)
        score_future = validator.score_tasks.submit(tasks, ground_truth_value, current_time)
        success_count = await score_future
        
        logger.info(f"Completed scoring {len(tasks)} predictions from hour {query_hour}")

    except Exception as e:
        logger.error(f"Error in score_flow: {str(e)}")
        logger.error(traceback.format_exc())
        raise

@flow(name="geomagnetic_validator_workflow", retries=3, description="Unified entry point for geomagnetic validator task")
async def geomagnetic_validator_workflow(validator):
    """Singular entry point for the geomagnetic validator workflow, including data preparation, execution, and scoring."""
    try:
        prep_data = await prepare_flow(validator)
        if prep_data is None:
            logger.info("Prepare flow returned no data, skipping execution.")
            return

        await execute_flow(validator)

        query_hour = datetime.datetime.now(datetime.timezone.utc).replace(minute=0, second=0, microsecond=0)
        await score_flow(validator, query_hour)
    except Exception as e:
        logger.error(f"Error in geomagnetic_validator_workflow: {str(e)}")
        logger.error(traceback.format_exc())
        raise
