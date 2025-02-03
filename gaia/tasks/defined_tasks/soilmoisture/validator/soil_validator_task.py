"""Soil Moisture Validator Task.

This module implements the validator task for soil moisture prediction. It handles:
1. Region preparation and data collection
2. Miner response validation and scoring
3. Database management for predictions and scores
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any, ClassVar, Union
import os
import base64
import json
import asyncio
import traceback
from uuid import uuid4

from prefect import flow, task
from pydantic import Field
from prefect.tasks import task_input_hash

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.tasks.base.task import BaseTask
from gaia.tasks.defined_tasks.soilmoisture.soil_metadata import SoilMoistureMetadata
from gaia.tasks.defined_tasks.soilmoisture.protocol import (
    SoilMoistureInputs, SoilMoistureOutputs, SoilMoisturePrediction,
    SoilMoisturePayload, ValidationResult
)
from gaia.tasks.defined_tasks.soilmoisture.validator.soil_scoring_mechanism import SoilScoringMechanism
from gaia.tasks.defined_tasks.soilmoisture.validator.soil_validator_preprocessing import SoilValidatorPreprocessing
from gaia.tasks.defined_tasks.soilmoisture.utils.soil_apis import (
    get_soil_data_parallel_flow
)
from gaia.parallel.core.executor import get_dask_runner, parallel_context
from gaia.parallel.config.settings import TaskType
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

os.environ["PYTHONASYNCIODEBUG"] = "1"


class SoilValidatorTask(BaseTask):
    """
    A task class for processing and analyzing soil moisture data, with
    execution methods for both miner and validator workflows.
    """

    # Declare Pydantic fields
    db_manager: Union[ValidatorDatabaseManager, MinerDatabaseManager] = Field(
        default_factory=ValidatorDatabaseManager,
        description="Database manager for the task",
    )
    model: Optional[Any] = Field(
        default=None, description="The soil moisture prediction model"
    )
    node_type: str = Field(
        default="validator",
        description="Type of node running the task (validator or miner)"
    )
    test_mode: bool = Field(
        default=False,
        description="Whether to run in test mode (immediate execution, limited scope)"
    )

    prepare_flow: ClassVar[Any]
    execute_flow: ClassVar[Any]
    score_flow: ClassVar[Any]
    validator_execute: ClassVar[Any]
    validator_score: ClassVar[Any]
    
    get_next_preparation_time: ClassVar[Any]
    get_tasks_for_hour: ClassVar[Any]
    fetch_ground_truth: ClassVar[Any]
    score_tasks: ClassVar[Any]
    _fetch_soil_data: ClassVar[Any]
    _query_miners: ClassVar[Any]
    _process_scores: ClassVar[Any]
    move_task_to_history: ClassVar[Any]
    build_score_row: ClassVar[Any]
    _handle_prep_window: ClassVar[Any]
    _handle_execution_window: ClassVar[Any]
    _process_region: ClassVar[Any]
    _validate_region_data: ClassVar[Any]
    _prepare_task_data: ClassVar[Any]
    _update_region_status: ClassVar[Any]
    _clear_old_regions: ClassVar[Any]

    prediction_horizon: timedelta = Field(
        default_factory=lambda: timedelta(hours=6),
        description="Prediction horizon for the task",
    )
    scoring_delay: timedelta = Field(
        default_factory=lambda: timedelta(days=3),
        description="Delay before scoring due to SMAP data latency",
    )
    validator_preprocessing: Optional[Any] = None

    def __init__(self, db_manager=None, test_mode=False, **data):
        """Initialize the soil moisture validator task.
        
        Args:
            db_manager: Database manager instance
            test_mode: Whether to run in test mode
            **data: Additional task configuration
        """
        scoring_mechanism = SoilScoringMechanism(
            db_manager=db_manager,
            baseline_rmse=50,
            alpha=10,
            beta=0.1,
            task=None
        )
        
        super().__init__(
            name="SoilMoistureValidatorTask",
            description="Soil moisture prediction validation task",
            metadata=SoilMoistureMetadata().model_dump(),
            inputs=SoilMoistureInputs(),
            outputs=SoilMoistureOutputs(),
            scoring_mechanism=scoring_mechanism,
            db_manager=db_manager,
            test_mode=test_mode,
            **data
        )
        
        self.validator_preprocessing = SoilValidatorPreprocessing()
        self._prepared_regions = {}
        scoring_mechanism.task = self

    @task(
        name="get_next_preparation_time",
        retries=2,                    
        retry_delay_seconds=10,       
        timeout_seconds=60,           
        description="Calculate the next preparation window start time",
        cache_policy=None  # I did this to avoid serialization issues with database manager
    )
    def get_next_preparation_time(self, current_time: datetime) -> datetime:
        """Get the next preparation window start time."""
        windows = self.get_validator_windows()
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

    @flow(
        name="validator_execute",
        retries=3,
        retry_delay_seconds=300,
        timeout_seconds=14400,
        description="Execute the validator task",
        task_runner=get_dask_runner(
            task_type=TaskType.MIXED
        )
    )
    async def validator_execute(self, validator):
        """Execute validator workflow."""
        if not hasattr(self, "db_manager") or self.db_manager is None:
            self.db_manager = validator.db_manager

        await self.ensure_retry_columns_exist()
        
        while True:
            try:
                await validator.update_task_status('soil', 'active')
                current_time = datetime.now(timezone.utc)

                # Check for scoring every 5 minutes
                if current_time.minute % 5 == 0:
                    await validator.update_task_status('soil', 'processing', 'scoring')
                    await self.validator_score()
                    await asyncio.sleep(60)
                    continue

                if self.test_mode:
                    logger.info("Running in test mode - bypassing window checks")
                    target_smap_time = self.get_smap_time_for_validator(current_time)
                    ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)

                    # Clear old regions in test mode
                    await self._clear_old_regions()
                    logger.info("Cleared old/scored regions in test mode")

                    await validator.update_task_status('soil', 'processing', 'data_download')
                    # Get regions first
                    query = """
                        SELECT * FROM soil_moisture_regions 
                        WHERE status = 'pending'
                        AND target_time = :target_time
                    """
                    regions = await self.db_manager.fetch_all(query, {"target_time": target_smap_time})
                    
                    if regions:
                        # Process regions in parallel
                        region_bboxes = [json.loads(region['bbox']) for region in regions]
                        logger.info(f"Processing {len(region_bboxes)} regions in parallel")
                        
                        # Use our new parallel processing function
                        results = await get_soil_data_parallel_flow(region_bboxes, ifs_forecast_time)
                        
                        # Process results and update database
                        for region, (output_file, sentinel_bounds, sentinel_crs) in zip(regions, results):
                            if output_file:
                                try:
                                    with open(output_file, 'rb') as f:
                                        combined_data = f.read()
                                    
                                    update_query = """
                                        UPDATE soil_moisture_regions 
                                        SET combined_data = :combined_data,
                                            sentinel_bounds = :sentinel_bounds,
                                            sentinel_crs = :sentinel_crs,
                                            status = 'processed'
                                        WHERE id = :region_id
                                    """
                                    await self.db_manager.execute(
                                        update_query,
                                        {
                                            "combined_data": combined_data,
                                            "sentinel_bounds": sentinel_bounds,
                                            "sentinel_crs": sentinel_crs,
                                            "region_id": region['id']
                                        }
                                    )
                                    logger.info(f"Successfully processed region {region['id']}")
                                except Exception as e:
                                    logger.error(f"Error processing region {region['id']}: {str(e)}")
                                    continue

                    # In test mode, attempt to score immediately
                    logger.info("Test mode: Attempting immediate scoring")
                    await self.validator_score()
                    
                    logger.info("Test mode execution complete. Re-running in 10 mins")
                    await validator.update_task_status('soil', 'idle')
                    await asyncio.sleep(600)
                    continue

                current_window = self.get_validator_windows(current_time)
                
                if not current_window:
                    next_time = self.get_next_preparation_time(current_time)
                    sleep_seconds = min(
                        300,  # Cap at 5 minutes
                        (next_time - current_time).total_seconds()
                    )
                    logger.info(f"Not in any preparation or execution window: {current_time}")
                    logger.info(f"Next soil task time: {next_time}")
                    logger.info(f"Sleeping for {sleep_seconds} seconds")
                    await validator.update_task_status('soil', 'idle')
                    await asyncio.sleep(sleep_seconds)
                    continue
                
                is_prep = current_window[1] == 30  # If minutes = 30, it's a prep window
                target_smap_time = self.get_smap_time_for_validator(current_time)
                ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)

                if is_prep:
                    await validator.update_task_status('soil', 'processing', 'data_download')
                    # Get regions first
                    query = """
                        SELECT * FROM soil_moisture_regions 
                        WHERE status = 'pending'
                        AND target_time = :target_time
                    """
                    regions = await self.db_manager.fetch_all(query, {"target_time": target_smap_time})
                    
                    if regions:
                        # Process regions in parallel
                        region_bboxes = [json.loads(region['bbox']) for region in regions]
                        logger.info(f"Processing {len(region_bboxes)} regions in parallel")
                        
                        # Use our new parallel processing function
                        results = await get_soil_data_parallel_flow(region_bboxes, ifs_forecast_time)
                        
                        # Process results and update database
                        for region, (output_file, sentinel_bounds, sentinel_crs) in zip(regions, results):
                            if output_file:
                                try:
                                    with open(output_file, 'rb') as f:
                                        combined_data = f.read()
                                    
                                    update_query = """
                                        UPDATE soil_moisture_regions 
                                        SET combined_data = :combined_data,
                                            sentinel_bounds = :sentinel_bounds,
                                            sentinel_crs = :sentinel_crs,
                                            status = 'processed'
                                        WHERE id = :region_id
                                    """
                                    await self.db_manager.execute(
                                        update_query,
                                        {
                                            "combined_data": combined_data,
                                            "sentinel_bounds": sentinel_bounds,
                                            "sentinel_crs": sentinel_crs,
                                            "region_id": region['id']
                                        }
                                    )
                                    logger.info(f"Successfully processed region {region['id']}")
                                except Exception as e:
                                    logger.error(f"Error processing region {region['id']}: {str(e)}")
                                    continue
                else:
                    # Handle execution window - query miners for processed regions
                    query = """
                        SELECT * FROM soil_moisture_regions 
                        WHERE status = 'processed'
                        AND target_time = :target_time
                    """
                    regions = await self.db_manager.fetch_all(query, {"target_time": target_smap_time})
                    
                    if regions:
                        for region in regions:
                            try:
                                await validator.update_task_status('soil', 'processing', 'miner_query')
                                combined_data = region["combined_data"]
                                
                                if not isinstance(combined_data, bytes):
                                    logger.error(f"Region {region['id']} has invalid data type")
                                    continue
                                    
                                encoded_data = base64.b64encode(combined_data)
                                task_data = {
                                    "region_id": region["id"],
                                    "combined_data": encoded_data.decode("ascii"),
                                    "sentinel_bounds": region["sentinel_bounds"],
                                    "sentinel_crs": region["sentinel_crs"],
                                    "target_time": target_smap_time.isoformat()
                                }
                                
                                payload = {"nonce": str(uuid4()), "data": task_data}
                                responses = await validator.query_miners(
                                    payload=payload,
                                    endpoint="/soilmoisture-request"
                                )
                                
                                if responses:
                                    metadata = {
                                        "region_id": region["id"],
                                        "target_time": target_smap_time,
                                        "data_collection_time": current_time,
                                        "ifs_forecast_time": ifs_forecast_time,
                                        "sentinel_bounds": region["sentinel_bounds"],
                                        "sentinel_crs": region["sentinel_crs"]
                                    }
                                    await self.add_task_to_queue(responses, metadata)
                                    
                                    # Update region status
                                    update_query = """
                                        UPDATE soil_moisture_regions 
                                        SET status = 'sent_to_miners'
                                        WHERE id = :region_id
                                    """
                                    await self.db_manager.execute(update_query, {"region_id": region["id"]})
                                    
                            except Exception as e:
                                logger.error(f"Error processing region {region['id']}: {str(e)}")
                                continue

                # Clean up old regions
                await self._clear_old_regions()
                
                if not self.test_mode:
                    await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in validator_execute: {str(e)}")
                logger.error(traceback.format_exc())
                await validator.update_task_status('soil', 'error')
                await asyncio.sleep(60)

    @flow(
        name="soil_moisture_prep_flow",
        retries=3,
        retry_delay_seconds=300,
        timeout_seconds=1800
    )
    async def _handle_prep_window(self, current_time: datetime):
        """Handle preparation window tasks."""
        try:
            target_smap_time = self.get_smap_time_for_validator(current_time)
            ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)
            
            await self.validator_preprocessing.get_daily_regions(
                target_time=target_smap_time,
                ifs_forecast_time=ifs_forecast_time
            )
            
        except Exception as e:
            error_msg = f"Error in preparation window: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise RuntimeError(error_msg) from e
            
    @flow(
        name="soil_moisture_execute_flow",
        retries=3,
        retry_delay_seconds=300,
        timeout_seconds=1800
    )
    async def _handle_execution_window(self, current_time: datetime):
        """Handle execution window tasks."""
        try:
            target_smap_time = self.get_smap_time_for_validator(current_time)
            
            query = """
                SELECT * FROM soil_moisture_regions 
                WHERE status = 'pending'
                AND target_time = :target_time
            """
            regions = await self.db_manager.fetch_all(query, {"target_time": target_smap_time})
            
            if not regions:
                logger.info("No pending regions found")
                return
                
            for region in regions:
                try:
                    await self._process_region(region, target_smap_time, current_time)
                except Exception as e:
                    logger.error(f"Error processing region {region['id']}: {str(e)}")
                    logger.error(traceback.format_exc())
                    continue
                    
        except Exception as e:
            error_msg = f"Error in execution window: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise RuntimeError(error_msg) from e
            
    @task(
        name="process_region",
        retries=4,                    # 4 retries for individual region processing
        retry_delay_seconds=60,       # 1 minute delay between retries
        timeout_seconds=1800,         # 30 minute timeout per region
        description="Process a single region"
    )
    async def _process_region(self, region: Dict, target_time: datetime, current_time: datetime):
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
        
        # Validate region data
        await self._validate_region_data(region)
        
        # Prepare task data
        task_data = await self._prepare_task_data(region, target_time)
        
        # Query miners
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
            await self._update_region_status(region_id, "sent_to_miners")
            
    @task(
        name="validate_region_data",
        retries=2,                    # 2 retries for validation
        retry_delay_seconds=15,       # 15 second delay between retries
        timeout_seconds=180,          # 3 minute timeout for validation
        description="Validate region data format and contents"
    )
    async def _validate_region_data(self, region: Dict):
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
        retries=3,                    # 3 retries since encoding rarely fails
        retry_delay_seconds=30,       # 30 second delay is sufficient
        timeout_seconds=300,          # 5 minute timeout should be plenty
        description="Prepare task data for miners by encoding region data"
    )
    async def _prepare_task_data(self, region: Dict, target_time: datetime) -> Dict:
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
        retries=3,                    # 3 retries for database operations
        retry_delay_seconds=20,       # 20 second delay between retries
        timeout_seconds=120,          # 2 minute timeout for status update
        description="Update the status of a region in the database"
    )
    async def _update_region_status(self, region_id: int, status: str):
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
        description="Clear old or scored regions from the database"
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

    async def get_todays_regions(self, target_time: datetime) -> List[Dict]:
        """Get regions already selected for today."""
        try:
            query = """
                SELECT * FROM soil_moisture_regions
                WHERE region_date = :target_date
                AND status = 'pending'
            """
            result = await self.db_manager.fetch_all(query, {"target_date": target_time.date()})
            return result
        except Exception as e:
            logger.error(f"Error getting today's regions: {str(e)}")
            return []

    async def add_task_to_queue(
        self, responses: Dict[str, Any], metadata: Dict[str, Any]
    ):
        """Add predictions to the database queue."""
        try:
            logger.info(f"Starting add_task_to_queue with metadata: {metadata}")
            logger.info(f"Adding predictions to queue for {len(responses)} miners")
            if not self.db_manager:
                raise RuntimeError("Database manager not initialized")

            # Update region status
            update_query = """
                UPDATE soil_moisture_regions 
                SET status = 'sent_to_miners' 
                WHERE id = :region_id
            """
            await self.db_manager.execute(update_query, {"region_id": metadata["region_id"]})

            for miner_hotkey, response_data in responses.items():
                try:
                    logger.info(f"Raw response data for miner {miner_hotkey}: {response_data.keys() if isinstance(response_data, dict) else 'not dict'}")
                    logger.info(f"Processing prediction from miner {miner_hotkey}")
                    
                    # Get miner UID
                    query = "SELECT uid FROM node_table WHERE hotkey = :miner_hotkey"
                    result = await self.db_manager.fetch_one(query, {"miner_hotkey": miner_hotkey})
                    if not result:
                        logger.warning(f"No UID found for hotkey {miner_hotkey}")
                        continue
                    miner_uid = str(result["uid"])

                    if isinstance(response_data, dict) and "text" in response_data:
                        try:
                            response_data = json.loads(response_data["text"])
                            logger.info(f"Parsed response data for miner {miner_hotkey}: {response_data.keys()}")
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse response text for {miner_hotkey}: {e}")
                            continue

                    prediction_data = {
                        "surface_sm": response_data.get("surface_sm", []),
                        "rootzone_sm": response_data.get("rootzone_sm", []),
                        "uncertainty_surface": response_data.get("uncertainty_surface"),
                        "uncertainty_rootzone": response_data.get("uncertainty_rootzone"),
                        "sentinel_bounds": response_data.get("sentinel_bounds", metadata.get("sentinel_bounds")),
                        "sentinel_crs": response_data.get("sentinel_crs", metadata.get("sentinel_crs")),
                        "target_time": metadata["target_time"]
                    }

                    # Validate that returned bounds and CRS match the original request
                    original_bounds = metadata.get("sentinel_bounds")
                    original_crs = metadata.get("sentinel_crs")
                    returned_bounds = response_data.get("sentinel_bounds")
                    returned_crs = response_data.get("sentinel_crs")
                    if returned_bounds != original_bounds:
                        logger.warning(f"Miner {miner_hotkey} returned different bounds than requested. Rejecting prediction.")
                        logger.warning(f"Original: {original_bounds}")
                        logger.warning(f"Returned: {returned_bounds}")
                        continue
                    if returned_crs != original_crs:
                        logger.warning(f"Miner {miner_hotkey} returned different CRS than requested. Rejecting prediction.")
                        logger.warning(f"Original: {original_crs}")
                        logger.warning(f"Returned: {returned_crs}")
                        continue

                    if not SoilMoisturePrediction.validate_prediction(prediction_data):
                        logger.warning(f"Skipping invalid prediction from miner {miner_hotkey}")
                        continue

                    db_prediction_data = {
                        "region_id": metadata["region_id"],
                        "miner_uid": miner_uid,
                        "miner_hotkey": miner_hotkey,
                        "target_time": metadata["target_time"],
                        "surface_sm": prediction_data["surface_sm"],
                        "rootzone_sm": prediction_data["rootzone_sm"],
                        "uncertainty_surface": prediction_data["uncertainty_surface"],
                        "uncertainty_rootzone": prediction_data["uncertainty_rootzone"],
                        "sentinel_bounds": prediction_data["sentinel_bounds"],
                        "sentinel_crs": prediction_data["sentinel_crs"],
                        "status": "sent_to_miner",
                    }

                    logger.info(f"About to insert prediction_data for miner {miner_hotkey}: {db_prediction_data}")

                    insert_query = """
                        INSERT INTO soil_moisture_predictions 
                        (region_id, miner_uid, miner_hotkey, target_time, surface_sm, rootzone_sm, 
                        uncertainty_surface, uncertainty_rootzone, sentinel_bounds, 
                        sentinel_crs, status)
                        VALUES 
                        (:region_id, :miner_uid, :miner_hotkey, :target_time, 
                        :surface_sm, :rootzone_sm,
                        :uncertainty_surface, :uncertainty_rootzone, :sentinel_bounds,
                        :sentinel_crs, :status)
                    """
                    await self.db_manager.execute(insert_query, db_prediction_data)

                    # Verify insertion
                    verify_query = """
                        SELECT COUNT(*) as count 
                        FROM soil_moisture_predictions 
                        WHERE miner_hotkey = :hotkey 
                        AND target_time = :target_time
                    """
                    verify_params = {
                        "hotkey": db_prediction_data["miner_hotkey"], 
                        "target_time": db_prediction_data["target_time"]
                    }
                    result = await self.db_manager.fetch_one(verify_query, verify_params)
                    logger.info(f"Verification found {result['count']} matching records")

                    logger.info(f"Successfully stored prediction for miner {miner_hotkey} (UID: {miner_uid}) for region {metadata['region_id']}")

                except Exception as e:
                    logger.error(f"Error processing response from miner {miner_hotkey}: {str(e)}")
                    logger.error(traceback.format_exc())
                    continue

        except Exception as e:
            logger.error(f"Error storing predictions: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def get_pending_tasks(self):
        """Get tasks that are ready for scoring and haven't been scored yet."""

        if self.test_mode: # Force scoring to use old data in test mode
            scoring_time = datetime.now(timezone.utc)
            scoring_time = scoring_time.replace(hour=19, minute=30, second=0, microsecond=0)
        else:
            scoring_time = datetime.now(timezone.utc) - self.scoring_delay
        
        try:
            # Get task status counts for debugging
            debug_query = """
                SELECT p.status, COUNT(*) as count, MIN(r.target_time) as earliest, MAX(r.target_time) as latest
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                GROUP BY p.status
            """
            debug_result = await self.db_manager.fetch_all(debug_query)
            for row in debug_result:
                logger.info(f"Status: {row['status']}, Count: {row['count']}, Time Range: {row['earliest']} to {row['latest']}")

            # Get pending tasks
            pending_query = """
                SELECT 
                    r.*,
                    json_agg(json_build_object(
                        'miner_id', p.miner_uid,
                        'miner_hotkey', p.miner_hotkey,
                        'surface_sm', p.surface_sm,
                        'rootzone_sm', p.rootzone_sm,
                        'uncertainty_surface', p.uncertainty_surface,
                        'uncertainty_rootzone', p.uncertainty_rootzone
                    )) as predictions
                FROM soil_moisture_regions r
                JOIN soil_moisture_predictions p ON p.region_id = r.id
                WHERE p.status = 'sent_to_miner'
                AND (
                    -- Normal case: Past scoring delay and no retry
                    (
                        r.target_time <= :scoring_time 
                        AND p.next_retry_time IS NULL
                    )
                    OR 
                    -- Retry case: Has retry time and it's in the past
                    (
                        p.next_retry_time IS NOT NULL 
                        AND p.next_retry_time <= :current_time
                        AND p.retry_count < 5
                    )
                )
                GROUP BY r.id, r.target_time, r.sentinel_bounds, r.sentinel_crs, r.status
                ORDER BY r.target_time ASC
            """
            params = {
                "scoring_time": scoring_time,
                "current_time": datetime.now(timezone.utc)
            }
            result = await self.db_manager.fetch_all(pending_query, params)
            if not result:
                await asyncio.sleep(60)  # Sleep for 60 seconds when no tasks are found
            return result

        except Exception as e:
            logger.error(f"Error fetching pending tasks: {str(e)}")
            return []

    async def move_task_to_history(
        self, region: Dict, predictions: Dict, ground_truth: Dict, scores: Dict
    ):
        """Move completed task data to history tables."""
        try:
            logger.info(f"Final scores for region {region['id']}:")
            logger.info(f"Surface RMSE: {scores['metrics'].get('surface_rmse'):.4f}")
            logger.info(f"Surface SSIM: {scores['metrics'].get('surface_ssim', 0):.4f}")
            logger.info(f"Rootzone RMSE: {scores['metrics'].get('rootzone_rmse'):.4f}")
            logger.info(f"Rootzone SSIM: {scores['metrics'].get('rootzone_ssim', 0):.4f}")
            logger.info(f"Total Score: {scores.get('total_score', 0):.4f}")

            for prediction in predictions:
                try:
                    miner_id = prediction["miner_id"]
                    params = {
                        "region_id": region["id"],
                        "miner_uid": miner_id,
                        "miner_hotkey": prediction.get("miner_hotkey", ""),
                        "target_time": region["target_time"],
                        "surface_sm_pred": prediction["surface_sm"],
                        "rootzone_sm_pred": prediction["rootzone_sm"],
                        "surface_sm_truth": ground_truth["surface_sm"] if ground_truth else None,
                        "rootzone_sm_truth": ground_truth["rootzone_sm"] if ground_truth else None,
                        "surface_rmse": scores["metrics"].get("surface_rmse"),
                        "rootzone_rmse": scores["metrics"].get("rootzone_rmse"),
                        "surface_structure_score": scores["metrics"].get("surface_ssim", 0),
                        "rootzone_structure_score": scores["metrics"].get("rootzone_ssim", 0),
                    }

                    insert_query = """
                        INSERT INTO soil_moisture_history 
                        (region_id, miner_uid, miner_hotkey, target_time,
                            surface_sm_pred, rootzone_sm_pred,
                            surface_sm_truth, rootzone_sm_truth,
                            surface_rmse, rootzone_rmse,
                            surface_structure_score, rootzone_structure_score)
                        VALUES 
                        (:region_id, :miner_uid, :miner_hotkey, :target_time,
                            :surface_sm_pred, :rootzone_sm_pred,
                            :surface_sm_truth, :rootzone_sm_truth,
                            :surface_rmse, :rootzone_rmse,
                            :surface_structure_score, :rootzone_structure_score)
                    """
                    await self.db_manager.execute(insert_query, params)

                    update_query = """
                        UPDATE soil_moisture_predictions 
                        SET status = 'scored'
                        WHERE region_id = :region_id 
                        AND miner_uid = :miner_uid
                        AND status = 'sent_to_miner'
                    """
                    await self.db_manager.execute(update_query, {
                        "region_id": region["id"],
                        "miner_uid": miner_id
                    })

                except Exception as e:
                    logger.error(f"Error processing prediction for miner {miner_id}: {str(e)}")
                    continue

            logger.info(f"Moved {len(predictions)} tasks to history for region {region['id']}")

            await self.cleanup_predictions(
                bounds=region["sentinel_bounds"],
                target_time=region["target_time"],
                miner_uid=miner_id
            )

            return True

        except Exception as e:
            logger.error(f"Failed to move task to history: {str(e)}")
            logger.error(traceback.format_exc())
            return False
            
    @flow(
        name="soil_moisture_scoring_flow",
        retries=3,
        retry_delay_seconds=300,
        timeout_seconds=7200
    )
    async def validator_score(self):
        """Score predictions after delay."""
        try:
            pending_tasks = await self.get_pending_tasks()
            if not pending_tasks:
                return {"status": "no_pending_tasks"}

            scoring_results = {}
            tasks_by_time = {}
            for task in pending_tasks:
                target_time = task["target_time"]
                if target_time not in tasks_by_time:
                    tasks_by_time[target_time] = []
                tasks_by_time[target_time].append(task)

            for target_time, tasks in tasks_by_time.items():
                logger.info(f"Processing {len(tasks)} predictions for timestamp {target_time}")
                
                smap_url = construct_smap_url(target_time, test_mode=self.test_mode)
                temp_file = None
                temp_path = None
                try:
                    temp_file = tempfile.NamedTemporaryFile(suffix=".h5", delete=False)
                    temp_path = temp_file.name
                    temp_file.close()

                    if not download_smap_data(smap_url, temp_file.name):
                        logger.error(f"Failed to download SMAP data for {target_time}")
                        # Update retry information for failed tasks
                        for task in tasks:
                            for prediction in task["predictions"]:
                                update_query = """
                                    UPDATE soil_moisture_predictions
                                    SET retry_count = COALESCE(retry_count, 0) + 1,
                                        next_retry_time = :next_retry_time,
                                        last_error = :error_message
                                    WHERE region_id = :region_id
                                    AND miner_uid = :miner_uid
                                """
                                params = {
                                    "region_id": task["id"],
                                    "miner_uid": prediction["miner_id"],
                                    "next_retry_time": datetime.now(timezone.utc) + timedelta(hours=1),
                                    "error_message": "Failed to download SMAP data"
                                }
                                await self.db_manager.execute(update_query, params)
                        continue

                    # Create tasks for parallel execution
                    prediction_tasks = []
                    for task in tasks:
                        for prediction in task["predictions"]:
                            pred_data = {
                                "bounds": task["sentinel_bounds"],
                                "crs": task["sentinel_crs"],
                                "predictions": prediction,
                                "target_time": target_time,
                                "region": {"id": task["id"]},
                                "miner_id": prediction["miner_id"],
                                "miner_hotkey": prediction["miner_hotkey"],
                                "smap_file": temp_path
                            }
                            # Submit prediction for parallel processing
                            prediction_tasks.append(
                                self._process_prediction.submit(pred_data)
                            )
                    
                    # Gather results from parallel execution
                    scores = await asyncio.gather(*[task.result() for task in prediction_tasks])
                    
                    # Process valid scores and move to history as before
                    valid_scores = [s for s in scores if s and not isinstance(s, Exception)]
                    if valid_scores:
                        for score in valid_scores:
                            task["score"] = score
                            await self.move_task_to_history(
                                region=task,
                                predictions=task["predictions"],
                                ground_truth=score.get("ground_truth"),
                                scores=score
                            )

                finally:
                    if temp_path and os.path.exists(temp_path):
                        try:
                            os.unlink(temp_path)
                            logger.debug(f"Removed temporary file: {temp_path}")
                        except Exception as e:
                            logger.error(f"Failed to remove temporary file {temp_path}: {e}")
                    
                    try:
                        for f in glob.glob("/tmp/*.h5"):
                            try:
                                os.unlink(f)
                                logger.debug(f"Cleaned up additional temp file: {f}")
                            except Exception as e:
                                logger.error(f"Failed to remove temp file {f}: {e}")
                    except Exception as e:
                        logger.error(f"Error during temp file cleanup: {e}")

            return {"status": "success", "results": scoring_results}

        except Exception as e:
            logger.error(f"Error in validator_score: {str(e)}")
            return {"status": "error", "message": str(e)}

    @task(
        name="process_prediction",
        retries=2,
        retry_delay_seconds=30,
        timeout_seconds=300
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

    async def validator_prepare_subtasks(self):
        """Prepare the subtasks for execution.

        Returns:
            List[Dict]: List of subtasks
        """
        pass

    def get_ifs_time_for_smap(self, smap_time: datetime) -> datetime:
        """Get corresponding IFS forecast time for SMAP target time."""
        smap_to_ifs = {
            1: 0,  # 01:30 uses 00:00 forecast
            7: 6,  # 07:30 uses 06:00 forecast
            13: 12,  # 13:30 uses 12:00 forecast
            19: 18,  # 19:30 uses 18:00 forecast
        }

        ifs_hour = smap_to_ifs.get(smap_time.hour)
        if ifs_hour is None:
            raise ValueError(f"Invalid SMAP time: {smap_time.hour}:30")

        return smap_time.replace(hour=ifs_hour, minute=0, second=0, microsecond=0)

    def get_smap_time_for_validator(self, current_time: datetime) -> datetime:
        """Get SMAP time based on validator execution time."""
        if self.test_mode:
            smap_hours = [1, 7, 13, 19]
            current_hour = currenttime.hour
            closest_hour = min(smap_hours, key=lambda x: abs(x - current_hour))
            return currenttime.replace(
                hour=1, minute=30, second=0, microsecond=0
            )

        validator_to_smap = {
            1: 1,    # 1:30 prep → 1:30 SMAP
            2: 1,    # 2:00 execution → 1:30 SMAP
            9: 7,    # 9:30 prep → 7:30 SMAP
            10: 7,   # 10:00 execution → 7:30 SMAP
            13: 13,  # 13:30 prep → 13:30 SMAP
            14: 13,  # 14:00 execution → 13:30 SMAP
            19: 19,  # 19:30 prep → 19:30 SMAP
            20: 19,  # 20:00 execution → 19:30 SMAP
        }
        smap_hour = validator_to_smap.get(currenttime.hour)
        if smap_hour is None:
            raise ValueError(f"No SMAP time mapping for validator hour {currenttime.hour}")

        return currenttime.replace(hour=smap_hour, minute=30, second=0, microsecond=0)

    def get_validator_windows(self, current_time: datetime) -> Tuple[int, int, int, int]:
        """Get all validator windows (hour_start, min_start, hour_end, min_end)."""
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
        return next(
            (w for w in windows if self.is_in_window(current_time, w)), None
        )

    def is_in_window(
        self, current_time: datetime, window: Tuple[int, int, int, int]
    ) -> bool:
        """Check if current time is within a specific window."""
        start_hr, start_min, end_hr, end_min = window
        current_mins = current_time.hour * 60 + current_time.minute
        window_start_mins = start_hr * 60 + start_min
        window_end_mins = end_hr * 60 + end_min
        return window_start_mins <= current_mins < window_end_mins

    async def build_score_row(self, target_time, recent_tasks=None):
        """Build score row for global scoring mechanism."""
        try:
            current_time = datetime.now(timezone.utc)
            scores = [float("nan")] * 256

            # Get miner mappings
            miner_query = """
                SELECT uid, hotkey FROM node_table 
                WHERE hotkey IS NOT NULL
            """
            miner_mappings = await self.db_manager.fetch_all(miner_query)
            hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}
            logger.info(f"Found {len(hotkey_to_uid)} miner mappings: {hotkey_to_uid}")

            scores = [float("nan")] * 256
            current_datetime = datetime.fromisoformat(str(target_time))

            if recent_tasks:
                logger.info(f"Processing {len(recent_tasks)} recent tasks")
                miner_scores = {}
                
                for task in recent_tasks:
                    task_score = task.get("score", {})
                    for prediction in task.get("predictions", []):
                        miner_id = prediction.get("miner_id")
                        if miner_id not in miner_scores:
                            miner_scores[miner_id] = []
                        if isinstance(task_score.get("total_score"), (int, float)):
                            miner_scores[miner_id].append(float(task_score["total_score"]))
                            logger.info(f"Added score {task_score['total_score']} for miner_id {miner_id} in region {task['id']}")

                for miner_id, scores_list in miner_scores.items():
                    if scores_list:
                        scores[int(miner_id)] = sum(scores_list) / len(scores_list)
                        logger.info(f"Final average score for miner {miner_id}: {scores[int(miner_id)]} across {len(scores_list)} regions")

            score_row = {
                "task_name": "soil_moisture",
                "task_id": str(current_datetime.timestamp()),
                "score": scores,
                "status": "completed"
            }
            
            logger.info(f"Raw score row being inserted: {json.dumps({**score_row, 'score': [f'{s:.4f}' if not math.isnan(s) else 'nan' for s in score_row['score']]})}")

            return [score_row]

        except Exception as e:
            logger.error(f"Error building score row: {e}")
            logger.error(traceback.format_exc())
            return []


    async def cleanup_predictions(self, bounds, target_time=None, miner_uid=None):
        """Clean up predictions after they've been processed and moved to history."""
        try:
            delete_query = """
                DELETE FROM soil_moisture_predictions p
                USING soil_moisture_regions r
                WHERE p.region_id = r.id 
                AND r.sentinel_bounds = :bounds
                AND r.target_time = :target_time
                AND p.miner_uid = :miner_uid
                AND p.status = 'scored'
            """
            params = {
                "bounds": bounds,
                "target_time": target_time,
                "miner_uid": miner_uid
            }
            await self.db_manager.execute(delete_query, params)
            
            logger.info(
                f"Cleaned up predictions for bounds {bounds}"
                f"{f', time {target_time}' if target_time else ''}"
                f"{f', miner {miner_uid}' if miner_uid else ''}"
            )

        except Exception as e:
            logger.error(f"Failed to cleanup predictions: {str(e)}")
            logger.error(traceback.format_exc())

    async def ensure_retry_columns_exist(self):
        """Ensure retry-related columns exist in soil_moisture_predictions table."""
        try:
            # Check if columns exist
            check_query = """
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name = 'soil_moisture_predictions' 
                    AND column_name = 'retry_count'
                )
            """
            result = await self.db_manager.fetch_one(check_query)
            columns_exist = result["exists"] if result else False
            
            if not columns_exist:
                logger.info("Adding retry columns to soil_moisture_predictions table")
                alter_query = """
                    ALTER TABLE soil_moisture_predictions 
                    ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS next_retry_time TIMESTAMP WITH TIME ZONE,
                    ADD COLUMN IF NOT EXISTS last_retry_at TIMESTAMP WITH TIME ZONE
                """
                await self.db_manager.execute(alter_query)
                logger.info("Successfully added retry columns")
            
        except Exception as e:
            logger.error(f"Error ensuring retry columns exist: {e}")
            logger.error(traceback.format_exc())

    async def cleanup_resources(self):
        """Clean up any resources used by the task during recovery."""
        try:
            # First clean up temporary files
            temp_dir = "/tmp"
            patterns = ["*.h5", "*.tif", "*.tiff"]
            for pattern in patterns:
                try:
                    for f in glob.glob(os.path.join(temp_dir, pattern)):
                        try:
                            os.unlink(f)
                            logger.debug(f"Cleaned up temp file: {f}")
                        except Exception as e:
                            logger.error(f"Failed to remove temp file {f}: {e}")
                except Exception as e:
                    logger.error(f"Error cleaning up {pattern} files: {e}")

            # Reset processing states in database
            try:
                update_query = """
                    UPDATE soil_moisture_regions 
                    SET status = 'pending'
                    WHERE status = 'processing'
                """
                await self.db_manager.execute(update_query)
                logger.info("Reset in-progress region statuses")
            except Exception as e:
                logger.error(f"Failed to reset region statuses: {e}")

            self._daily_regions = {}
            
            logger.info("Completed soil task cleanup")
            
        except Exception as e:
            logger.error(f"Error during soil task cleanup: {e}")
            logger.error(traceback.format_exc())
            raise
