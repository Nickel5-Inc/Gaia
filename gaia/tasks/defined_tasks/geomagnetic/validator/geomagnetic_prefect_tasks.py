from datetime import timedelta, datetime
import traceback
from uuid import uuid4

from prefect import task
from prefect.tasks import task_input_hash
from gaia.tasks.defined_tasks.geomagnetic.utils.process_geomag_data import get_latest_geomag_data
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

@task(
    name="fetch_geomag_data",
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=5),
    description="Fetch latest geomagnetic data including historical records"
)
async def fetch_geomag_data():
    try:
        logger.info("Fetching latest geomagnetic data...")
        timestamp, dst_value, historical_data = await get_latest_geomag_data(include_historical=True)
        if historical_data is None:
            logger.warning("No historical data available for the current month.")
            historical_data = []
        else:
            logger.info(f"Fetched {len(historical_data)} historical records")
        logger.info(f"Latest measurement - timestamp: {timestamp}, DST value: {dst_value}")
        return timestamp, dst_value, historical_data
    except Exception as e:
        logger.error(f"Error fetching geomagnetic data: {str(e)}")
        logger.error(traceback.format_exc())
        raise


@task(
    name="query_miners",
    retry_delay_seconds=60,
    description="Query miners for geomagnetic predictions and process responses"
)
async def query_miners(validator, timestamp, dst_value, historical_data, current_hour_start):
    try:
        if timestamp == "N/A" or dst_value == "N/A":
            logger.warning("Invalid geomagnetic data. Skipping miner queries.")
            return 0

        # Construct payload for miners
        nonce = str(uuid4())
        historical_records = []
        if historical_data is not None:
            # Assuming historical_data is a DataFrame-like object with iterrows()
            for _, row in historical_data.iterrows():
                historical_records.append({
                    "timestamp": row["timestamp"].isoformat(),
                    "Dst": row["Dst"]
                })

        payload_template = {
            "nonce": nonce,
            "data": {
                "name": "Geomagnetic Data",
                "timestamp": timestamp.isoformat(),
                "value": dst_value,
                "historical_values": historical_records,
            }
        }
        endpoint = "/geomagnetic-request"

        logger.info("Querying miners for geomagnetic predictions")
        responses = await validator.query_miners(payload_template, endpoint)
        logger.info(f"Collected responses from miners: {len(responses)}")

        await process_miner_responses(responses, current_hour_start, validator)
        logger.info(f"Added {len(responses)} predictions to the database")
        return len(responses)
    except Exception as e:
        logger.error(f"Error querying miners: {str(e)}")
        logger.error(traceback.format_exc())
        raise


@task(
    name="process_scores",
    retry_delay_seconds=30,
    description="Process and archive scores for predictions from a specific hour"
)
async def process_scores(validator, query_hour):
    try:
        ground_truth_value = await validator.fetch_ground_truth()
        if ground_truth_value is None:
            logger.warning("Ground truth data not available. Skipping scoring.")
            return (0, 0)

        hour_start = query_hour
        hour_end = query_hour + timedelta(hours=1)
        logger.info(f"Scoring predictions collected between {hour_start} and {hour_end}")

        tasks = await validator.get_tasks_for_hour(hour_start, hour_end, validator)
        if not tasks:
            logger.info(f"No predictions found for collection hour {query_hour}")
            return (0, 0)

        current_time = datetime.now(datetime.timezone.utc)
        success_count = await validator.score_tasks(tasks, ground_truth_value, current_time)
        logger.info(f"Completed scoring {len(tasks)} predictions from hour {query_hour}")
        return (len(tasks), success_count)
    except Exception as e:
        logger.error(f"Error processing scores: {str(e)}")
        logger.error(traceback.format_exc())
        raise


@task(
    name="process_miner_responses",
    retry_delay_seconds=30,
    description="Process responses from miners and add predictions to the queue"
)
async def process_miner_responses(responses, current_hour_start, validator):
    try:
        if not responses:
            logger.warning("No responses received from miners")
            return
        
        for hotkey, response in responses.items():
            try:
                logger.info(f"Raw response from miner {hotkey}: {response}")
                
                predicted_value = extract_prediction(response)
                if predicted_value is None:
                    logger.error(f"No valid prediction found in response from {hotkey}")
                    continue

                try:
                    predicted_value = float(predicted_value)
                except (TypeError, ValueError) as e:
                    logger.error(f"Invalid prediction from {hotkey}: {predicted_value}")
                    continue

                logger.info("=" * 50)
                logger.info(f"Received prediction from miner:")
                logger.info(f"Miner Hotkey: {hotkey}")
                logger.info(f"Predicted Value: {predicted_value}")
                logger.info(f"Timestamp: {current_hour_start}")
                logger.info("=" * 50)

                result = await validator.db_manager.fetch_one(
                    """
                    SELECT uid FROM node_table 
                    WHERE hotkey = :miner_hotkey
                    """,
                    {"miner_hotkey": hotkey}
                )
                if not result:
                    logger.warning(f"No UID found for hotkey {hotkey}")
                    continue
                miner_uid = str(result["uid"])
                logger.info(f"Found miner UID {miner_uid} for hotkey {hotkey}")
                logger.info(f"Adding prediction to queue for {hotkey} with value {predicted_value}")
                await validator.add_prediction_to_queue(
                    miner_uid=miner_uid,
                    miner_hotkey=hotkey,
                    predicted_value=predicted_value,
                    query_time=current_hour_start,
                    status="pending",
                )

            except Exception as e:
                logger.error(f"Error processing response from {hotkey}: {e}")
                logger.error(traceback.format_exc())
                continue
    except Exception as e:
        logger.error(f"Error processing miner responses: {e}")
        logger.error(traceback.format_exc())
        raise


def extract_prediction(response):
    """
    Recursively extract prediction from response, handling various formats.
    """
    if isinstance(response, dict):
        if "predicted_values" in response:
            return response["predicted_values"]
        if "predicted_value" in response:
            return response["predicted_value"]
        if "text" in response:
            try:
                import json
                parsed = json.loads(response["text"])
                return extract_prediction(parsed)
            except Exception:
                return None
    return None 