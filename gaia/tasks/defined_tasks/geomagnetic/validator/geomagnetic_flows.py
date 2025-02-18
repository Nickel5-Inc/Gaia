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
    """Prepare the validator task by checking if we're at the top of the hour."""
    try:
        current_time = datetime.datetime.now(datetime.timezone.utc)
        
        if not validator.test_mode:
            minutes = current_time.minute
            seconds = current_time.second
            if minutes != 0 or seconds > 30:
                logger.info("Not at the top of the hour, skipping execution")
                return None
            
        next_hour = current_time.replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(hours=1)

        logger.info(f"Preparing execution for current hour: {current_time.isoformat()}")
        logger.info(f"Next hour will be: {next_hour.isoformat()}")

        return {
            "current_time": current_time,
            "next_hour": next_hour
        }
    except Exception as e:
        logger.error(f"Error in prepare_flow: {e}")
        logger.error(traceback.format_exc())
        raise

@flow(name="geomagnetic_execute", task_runner=get_task_runner(TaskType.MIXED))
async def execute_flow(validator):
    """Execute the main geomagnetic validator workflow."""
    try:
        while not validator._shutdown_event.is_set():
            try:
                data_future = fetch_geomag_data.submit()
                timestamp, dst_value, historical_data = await data_future

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

                if validator.test_mode:
                    await asyncio.sleep(3)
                else:
                    await asyncio.sleep(30)

            except Exception as e:
                logger.error(f"Error in execution cycle: {str(e)}")
                logger.error(traceback.format_exc())
                await validator.update_task_status('geomagnetic', 'error')
                if validator.test_mode:
                    await asyncio.sleep(3)
                else:
                    await asyncio.sleep(30)
    finally:
        pass

@flow(name="validator_score", retries=2, task_runner=get_task_runner(TaskType.CPU_BOUND))
async def score_flow(validator, query_hour: datetime.datetime) -> None:
    """Score predictions for a given hour using Dask for parallel processing."""
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
