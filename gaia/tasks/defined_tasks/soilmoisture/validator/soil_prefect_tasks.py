from prefect import task
from uuid import uuid4
from datetime import datetime, timedelta, timezone
import json
import traceback
import asyncio
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

@task(
    name="process_region",
    retries=4,
    retry_delay_seconds=60,
    timeout_seconds=1800,
    description="Process a single region",
    cache_policy=None
)
async def process_region(self, region: Dict, target_time: datetime, current_time: datetime):
    """Process a single region.
    
    Args:
        region: Region data dictionary
        target_time: Target SMAP time
        current_time: Current timestamp
        
    Raises:
        ValueError: If region data is invalid
    """
    region_id = region["id"]
    logger.info(f"Processing region {region_id}")
    
    await validate_region_data(self, region)
    task_data = await prepare_task_data(self, region, target_time)
    
    payload = {"nonce": str(uuid4()), "data": task_data}
    responses = await self.validator.query_miners(
        payload=payload,
        endpoint="/soilmoisture-request"
    )
    
    if responses:
        metadata = {
            "region_id": region_id,
            "target_time": target_time,
            "data_collection_time": current_time,
            "ifs_forecast_time": self.get_ifs_time_for_smap(target_time),
            "sentinel_bounds": region["sentinel_bounds"],
            "sentinel_crs": region["sentinel_crs"]
        }
        await self.add_task_to_queue(responses, metadata)
        await update_region_status(self, region_id, "sent_to_miners")
        
@task(
    name="validate_region_data",
    retries=2,
    retry_delay_seconds=15,
    timeout_seconds=180,
    description="Validate region data format and contents",
    cache_policy=None
)
async def validate_region_data(self, region: Dict):
    """Validate region data format and contents.
    
    Args:
        region: Region data dictionary
        
    Raises:
        ValueError: If region data is invalid
    """
    if "combined_data" not in region:
        raise ValueError("Region missing combined_data field")
        
    combined_data = region["combined_data"]
    if not isinstance(combined_data, bytes):
        raise ValueError(f"Combined data must be bytes, got {type(combined_data)}")
        
    # Validate TIFF header
    if combined_data[:4] != b'II*\x00':
        raise ValueError("Invalid TIFF header in combined data")

@task(
    name="prepare_task_data",
    retries=3,
    retry_delay_seconds=30,
    timeout_seconds=300,
    description="Prepare task data for miners by encoding region data",
    cache_policy=None
)
async def prepare_task_data(self, region: Dict, target_time: datetime) -> Dict:
    """Prepare task data for miners.
    
    Args:
        region: Region data dictionary
        target_time: Target SMAP time
        
    Returns:
        dict: Prepared task data
    """
    encoded_data = base64.b64encode(region["combined_data"])
    logger.debug(f"Base64 first 16 chars: {encoded_data[:16]}")
    
    return {
        "region_id": region["id"],
        "combined_data": encoded_data.decode("ascii"),
        "sentinel_bounds": region["sentinel_bounds"],
        "sentinel_crs": region["sentinel_crs"],
        "target_time": target_time.isoformat()
    }
    
@task(
    name="update_region_status",
    retries=3,
    retry_delay_seconds=20,
    timeout_seconds=120,
    description="Update the status of a region in the database",
    cache_policy=None
)
async def update_region_status(self, region_id: int, status: str):
    """Update region status in database.
    
    Args:
        region_id: ID of the region to update
        status: New status value
    """
    update_query = """
        UPDATE soil_moisture_regions 
        SET status = :status
        WHERE id = :region_id
    """
    await self.db_manager.execute(
        update_query, 
        {"region_id": region_id, "status": status}
    )
    
@task(
    name="clear_old_regions",
    retries=3,                    # 3 retries for database cleanup
    retry_delay_seconds=60,       # 1 minute delay between retries
    timeout_seconds=600,          # 10 minute timeout for cleanup
    description="Clear old or scored regions from the database",
    cache_policy=None
)
async def _clear_old_regions(self):
    """Clear old or scored regions from the database."""
    try:
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
        
        clear_query = """
            DELETE FROM soil_moisture_regions r
            USING soil_moisture_predictions p
            WHERE r.id = p.region_id
            AND (
                p.status = 'scored'
                OR r.target_time < :cutoff_time
            )
        """
        await self.db_manager.execute(clear_query, {"cutoff_time": cutoff_time})
        logger.info("Cleared old/scored regions")
        
    except Exception as e:
        logger.error(f"Error clearing old regions: {str(e)}")
        logger.error(traceback.format_exc())
        raise

@task(
    name="process_prediction",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=300,
    cache_policy=None
)
async def _process_prediction(self, prediction: Dict, task: Dict, smap_data: str):
    """Process a single prediction using the scoring mechanism.
    
    Args:
        prediction: Prediction data
        task: Task data
        smap_data: Path to SMAP data file
        
    Returns:
        Score dictionary or None if scoring fails
    """
    try:
        pred_data = {
            "bounds": task["sentinel_bounds"],
            "crs": task["sentinel_crs"],
            "predictions": prediction,
            "target_time": task["target_time"],
            "region": {"id": task["id"]},
            "miner_id": prediction["miner_id"],
            "miner_hotkey": prediction["miner_hotkey"],
            "smap_file": smap_data
        }
        return await self.scoring_mechanism.score(pred_data)
    except Exception as e:
        logger.error(f"Error processing prediction: {str(e)}")
        return None


    
@task(
    name="get_next_preparation_time",
    retries=2,
    retry_delay_seconds=10,
    timeout_seconds=60,
    description="Calculate the next preparation window start time",
    cache_policy=None  # Disable caching
)
async def get_next_preparation_time(self, current_time: datetime) -> datetime:
    """Get the next preparation window start time."""
    windows = [
        (1, 30, 2, 0),  # Prep window for 1:30 SMAP time
        (2, 0, 2, 30),  # Execution window for 1:30 SMAP time
        (9, 30, 10, 0),  # Prep window for 7:30 SMAP time
        (10, 0, 10, 30),  # Execution window for 7:30 SMAP time
        (13, 30, 14, 0),  # Prep window for 13:30 SMAP time
        (14, 0, 14, 30),  # Execution window for 13:30 SMAP time
        (19, 30, 20, 0),  # Prep window for 19:30 SMAP time
        (20, 0, 20, 30),  # Execution window for 19:30 SMAP time
    ]
    current_mins = current_time.hour * 60 + current_time.minute

    for start_hr, start_min, _, _ in windows:
        window_start_mins = start_hr * 60 + start_min
        if window_start_mins > current_mins:
            return current_time.replace(
                hour=start_hr, minute=start_min, second=0, microsecond=0
            )

    tomorrow = current_time + timedelta(days=1)
    first_window = windows[0]
    return tomorrow.replace(
        hour=first_window[0], minute=first_window[1], second=0, microsecond=0
    )