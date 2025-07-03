from gaia.tasks.base.task import Task
from gaia.tasks.base.deterministic_job_id import DeterministicJobID
from datetime import datetime, timedelta, timezone
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import importlib.util
import os
from gaia.tasks.base.components.metadata import Metadata
from gaia.tasks.defined_tasks.soilmoisture.soil_miner_preprocessing import (
    SoilMinerPreprocessing,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_scoring_mechanism import (
    SoilScoringMechanism,
)
from gaia.tasks.defined_tasks.soilmoisture.utils.smap_api import (
    construct_smap_url,
    download_smap_data,
    get_smap_data_for_sentinel_bounds,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_inputs import (
    SoilMoistureInputs,
    SoilMoisturePayload,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_outputs import (
    SoilMoistureOutputs,
    SoilMoisturePrediction,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_metadata import SoilMoistureMetadata
from pydantic import Field
from fiber.logging_utils import get_logger
from uuid import uuid4
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from sqlalchemy import text
from gaia.models.soil_moisture_basemodel import SoilModel
import traceback
import base64
import json
import asyncio
import tempfile
import math
import glob
from collections import defaultdict
import torch
from gaia.tasks.defined_tasks.soilmoisture.utils.inference_class import SoilMoistureInferencePreprocessor
from gaia.tasks.defined_tasks.soilmoisture.utils.smap_api import get_smap_data, get_smap_data_multi_region

logger = get_logger(__name__)

os.environ["PYTHONASYNCIODEBUG"] = "1"

if os.environ.get("NODE_TYPE") == "validator":
    from gaia.tasks.defined_tasks.soilmoisture.soil_validator_preprocessing import (
        SoilValidatorPreprocessing,
    )
else:
    SoilValidatorPreprocessing = None


class SoilMoistureTask(Task):
    """Task for soil moisture prediction using satellite and weather data."""

    prediction_horizon: timedelta = Field(
        default_factory=lambda: timedelta(hours=6),
        description="Prediction horizon for the task",
    )
    scoring_delay: timedelta = Field(
        default_factory=lambda: timedelta(days=3),
        description="Delay before scoring due to SMAP data latency. Enhanced error handling distinguishes between 404 (data not available yet) vs other error types for optimized retry timing.",
    )

    validator_preprocessing: Optional["SoilValidatorPreprocessing"] = None # type: ignore # steven this is for the linter lol, just leave it here unless it's causing issues
    miner_preprocessing: Optional["SoilMinerPreprocessing"] = None
    model: Optional[SoilModel] = None
    db_manager: Any = Field(default=None)
    node_type: str = Field(default="miner")
    test_mode: bool = Field(default=False)
    use_raw_preprocessing: bool = Field(default=False)
    validator: Any = Field(default=None, description="Reference to the validator instance")
    use_threaded_scoring: bool = Field(default=False, description="Enable threaded scoring for performance improvement")

    def __init__(self, db_manager=None, node_type=None, test_mode=False, **data):
        super().__init__(
            name="SoilMoistureTask",
            description="Soil moisture prediction task",
            task_type="atomic",
            metadata=SoilMoistureMetadata(),
            inputs=SoilMoistureInputs(),
            outputs=SoilMoistureOutputs(),
            scoring_mechanism=SoilScoringMechanism(
                db_manager=db_manager,
                baseline_rmse=50,
                alpha=10,
                beta=0.1,
                task=None
            ),
        )

        self.db_manager = db_manager
        self.node_type = node_type
        self.test_mode = test_mode
        self.scoring_mechanism.task = self
        
        # Configure threading for soil scoring performance improvement
        import os
        self.use_threaded_scoring = os.getenv("SOIL_THREADED_SCORING", "false").lower() == "true"
        if self.use_threaded_scoring:
            logger.info("🚀 Soil threaded scoring enabled - improved performance expected")

        if node_type == "validator":
            self.validator_preprocessing = SoilValidatorPreprocessing()
        else:
            self.miner_preprocessing = SoilMinerPreprocessing(task=self)

            custom_model_path = "gaia/models/custom_models/custom_soil_model.py"
            if os.path.exists(custom_model_path):
                import importlib.util
                spec = importlib.util.spec_from_file_location("custom_soil_model", custom_model_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self.model = module.CustomSoilModel()
                self.use_raw_preprocessing = True
                logger.info("Initialized custom soil model")
            else:
                self.model = self.miner_preprocessing.model
                self.use_raw_preprocessing = False
                logger.info("Initialized base soil model")
                
            logger.info("Initialized miner components for SoilMoistureTask")

        self._prepared_regions = {}

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

    async def validator_execute(self, validator):
        """Execute validator workflow."""
        if not hasattr(self, "db_manager") or self.db_manager is None:
            self.db_manager = validator.db_manager
        self.validator = validator

        # Run startup retry check for any pending tasks from previous sessions
        logger.info("🚀 Checking for pending tasks on startup...")
        try:
            await self._startup_retry_check()
        except Exception as e:
            logger.error(f"Error during startup retry check: {e}")
        
        while True:
            try:
                await validator.update_task_status('soil', 'active')
                current_time = datetime.now(timezone.utc)

                # Check for scoring every 5 minutes AND ensure retries run frequently
                should_score = (
                    current_time.minute % 5 == 0 or  # Regular 5-minute intervals
                    # Also check for retries every 60 seconds regardless of minute
                    getattr(self, '_last_retry_check', datetime.min.replace(tzinfo=timezone.utc)) < current_time - timedelta(seconds=60)
                )
                
                if should_score:
                    await validator.update_task_status('soil', 'processing', 'scoring')
                    logger.info(f"🔄 Running retry/scoring check at {current_time}")
                    await self.validator_score()
                    self._last_retry_check = current_time
                    
                    # Only sleep and continue for regular 5-minute intervals, not retry checks
                    if current_time.minute % 5 == 0:
                        await asyncio.sleep(60)
                        continue

                if self.test_mode:
                    logger.info("Running in test mode - bypassing window checks")
                    target_smap_time = self.get_smap_time_for_validator(current_time)
                    ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)

                    clear_query = """
                        DELETE FROM soil_moisture_regions r
                        USING soil_moisture_predictions p
                        WHERE r.id = p.region_id
                        AND (
                            p.status = 'scored'
                            OR r.target_time < :cutoff_time
                        )
                    """
                    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
                    await self.db_manager.execute(clear_query, {"cutoff_time": cutoff_time})
                    logger.info("Cleared old/scored regions in test mode")

                    await validator.update_task_status('soil', 'processing', 'data_download')
                    await self.validator_preprocessing.get_daily_regions(
                        target_time=target_smap_time,
                        ifs_forecast_time=ifs_forecast_time,
                    )

                    # Get pending regions for this target time
                    query = """
                        SELECT * FROM soil_moisture_regions 
                        WHERE status = 'pending'
                        AND target_time = :target_time
                    """
                    regions = await self.db_manager.fetch_all(query, {"target_time": target_smap_time})

                    if regions:
                        for region in regions:
                            try:
                                await validator.update_task_status('soil', 'processing', 'region_processing')
                                logger.info(f"Processing region {region['id']}")

                                if "combined_data" not in region:
                                    logger.error(f"Region {region['id']} missing combined_data field")
                                    continue

                                if not region["combined_data"]:
                                    logger.error(f"Region {region['id']} has null combined_data")
                                    continue

                                combined_data = region["combined_data"]
                                if not isinstance(combined_data, bytes):
                                    logger.error(f"Region {region['id']} has invalid data type: {type(combined_data)}")
                                    continue

                                if not (combined_data.startswith(b"II\x2A\x00") or combined_data.startswith(b"MM\x00\x2A")):
                                    logger.error(f"Region {region['id']} has invalid TIFF header")
                                    logger.error(f"First 16 bytes: {combined_data[:16].hex()}")
                                    continue

                                logger.info(f"Region {region['id']} TIFF size: {len(combined_data) / (1024 * 1024):.2f} MB")
                                logger.info(f"Region {region['id']} TIFF header: {combined_data[:4]}")
                                logger.info(f"Region {region['id']} TIFF header hex: {combined_data[:16].hex()}")
                                
                                # Track memory usage for large TIFF processing
                                tiff_size_mb = len(combined_data) / (1024 * 1024)
                                if tiff_size_mb > 50:
                                    logger.warning(f"Processing large TIFF for region {region['id']}: {tiff_size_mb:.1f}MB")
                                
                                # Log memory before processing
                                if hasattr(validator, '_log_memory_usage'):
                                    validator._log_memory_usage(f"soil_before_region_{region['id']}")
                                
                                loop = asyncio.get_event_loop()
                                encoded_data_bytes = await loop.run_in_executor(None, base64.b64encode, combined_data)
                                encoded_data_ascii = encoded_data_bytes.decode("ascii")
                                
                                # Immediately clean up large variables to prevent memory accumulation
                                del encoded_data_bytes  # Free the intermediate bytes object
                                original_data_copy = combined_data  # Keep reference for baseline model if needed
                                del combined_data  # Free the original large TIFF data
                                
                                # Log memory after base64 encoding and cleanup
                                if hasattr(validator, '_log_memory_usage'):
                                    validator._log_memory_usage(f"soil_after_encoding_{region['id']}")
                                
                                logger.info(f"Base64 first 16 chars: {encoded_data_ascii[:16]}")

                                task_data = {
                                    "region_id": region["id"],
                                    "combined_data": encoded_data_ascii,
                                    "sentinel_bounds": region["sentinel_bounds"],
                                    "sentinel_crs": region["sentinel_crs"],
                                    "target_time": target_smap_time.isoformat(),
                                }

                                if validator.basemodel_evaluator:
                                    try:
                                        logger.info(f"Running soil moisture baseline model for region {region['id']}")
                                        model_inputs = None
                                        temp_file_path = None # Initialize temp_file_path
                                        try:
                                            # Offload file writing and preprocessing to an executor
                                            def _write_and_preprocess_sync(data_bytes):
                                                t_file_path = None
                                                try:
                                                    with tempfile.NamedTemporaryFile(suffix='.tiff', delete=False) as temp_f:
                                                        temp_f.write(data_bytes)
                                                        t_file_path = temp_f.name
                                                    
                                                    s_preprocessor = SoilMoistureInferencePreprocessor()
                                                    m_inputs = s_preprocessor.preprocess(t_file_path)
                                                    return m_inputs, t_file_path
                                                finally:
                                                    # Ensure temp file is cleaned up if preprocess fails before returning path
                                                    if t_file_path and (not 'm_inputs' in locals() or m_inputs is None) and os.path.exists(t_file_path):
                                                        try:
                                                            os.unlink(t_file_path)
                                                        except Exception as e_unlink_inner:
                                                            logger.error(f"Error cleaning temp file in sync helper: {e_unlink_inner}")

                                            loop = asyncio.get_event_loop()
                                            model_inputs, temp_file_path = await loop.run_in_executor(None, _write_and_preprocess_sync, original_data_copy)
                                            
                                            # Free the original data copy after baseline model processing
                                            del original_data_copy
                                            
                                            if model_inputs:
                                                for key, value in model_inputs.items():
                                                    if isinstance(value, np.ndarray):
                                                        model_inputs[key] = torch.from_numpy(value).float()
                                            
                                            model_inputs["sentinel_bounds"] = region["sentinel_bounds"]
                                            model_inputs["sentinel_crs"] = region["sentinel_crs"]
                                            model_inputs["target_time"] = target_smap_time
                                            
                                            logger.info(f"Preprocessed data for soil moisture baseline model. Keys: {list(model_inputs.keys() if model_inputs else [])}")
                                        except Exception as e:
                                            logger.error(f"Error preprocessing data for soil moisture baseline model: {str(e)}")
                                            logger.error(traceback.format_exc())
                                            # Clean up on error
                                            if 'original_data_copy' in locals():
                                                del original_data_copy
                                        
                                        if model_inputs:
                                            if isinstance(target_smap_time, datetime):
                                                if target_smap_time.tzinfo is not None:
                                                    target_smap_time_utc = target_smap_time.astimezone(timezone.utc)
                                                else:
                                                    target_smap_time_utc = target_smap_time.replace(tzinfo=timezone.utc)
                                            else:
                                                target_smap_time_utc = target_smap_time
                                            # Using timestamp-based deterministic task ID (already deterministic)
                                            # Could optionally use: DeterministicJobID.generate_task_id("soil_moisture", target_smap_time_utc, region_id)
                                            task_id = str(target_smap_time_utc.timestamp())
                                            baseline_prediction = await validator.basemodel_evaluator.predict_soil_and_store(
                                                data=model_inputs,
                                                task_id=task_id,
                                                region_id=str(region["id"])
                                            )
                                            if baseline_prediction:
                                                logger.info(f"Soil moisture baseline prediction stored for region {region['id']}")
                                            else:
                                                logger.error(f"Failed to generate soil moisture baseline prediction for region {region['id']}")
                                        else:
                                            logger.error(f"Preprocessing failed for soil moisture baseline model, region {region['id']}")
                                        
                                        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
                                            try:
                                                os.unlink(temp_file_path)
                                            except Exception as e:
                                                logger.error(f"Error cleaning up temporary file: {str(e)}")
                                    except Exception as e:
                                        logger.error(f"Error running soil moisture baseline model: {str(e)}")
                                        logger.error(traceback.format_exc())
                                        # Ensure cleanup on any error
                                        if 'original_data_copy' in locals():
                                            del original_data_copy
                                else:
                                    # If no baseline model, clean up the original data copy immediately
                                    if 'original_data_copy' in locals():
                                        del original_data_copy

                                payload = {"nonce": str(uuid4()), "data": task_data}

                                logger.info(f"Sending region {region['id']} to miners...")
                                await validator.update_task_status('soil', 'processing', 'miner_query')
                                responses = await validator.query_miners(
                                    payload=payload, endpoint="/soilmoisture-request"
                                )
                                
                                # IMMEDIATE cleanup after query to free encoded data
                                try:
                                    del task_data
                                    del payload  
                                    del encoded_data_ascii
                                    
                                    # Clean up the original data copy if still in scope
                                    if 'original_data_copy' in locals():
                                        del original_data_copy
                                        
                                    # Force garbage collection after processing large TIFF
                                    if tiff_size_mb > 50:
                                        import gc
                                        collected = gc.collect()
                                        logger.info(f"Cleaned up large TIFF data for region {region['id']} ({tiff_size_mb:.1f}MB), GC collected {collected} objects")
                                except Exception as cleanup_err:
                                    logger.warning(f"Error during soil task cleanup for region {region['id']}: {cleanup_err}")

                                # Log memory after cleanup
                                if hasattr(validator, '_log_memory_usage'):
                                    validator._log_memory_usage(f"soil_after_cleanup_{region['id']}")

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

                                    # In test mode, attempt to score immediately
                                    logger.info("Test mode: Attempting immediate scoring")
                                    await self.validator_score()

                            except Exception as e:
                                logger.error(f"Error preparing region: {str(e)}")
                                continue

                    logger.info("Test mode execution complete. Re-running in 10 mins")
                    await validator.update_task_status('soil', 'idle')
                    await asyncio.sleep(600)
                    continue

                windows = self.get_validator_windows()
                current_window = next(
                    (w for w in windows if self.is_in_window(current_time, w)), None
                )

                if not current_window:
                    next_time = self.get_next_preparation_time(current_time)
                    sleep_seconds = min(
                        300,  # Cap at 5 minutes
                        (next_time - current_time).total_seconds()
                    )
                    logger.info(
                        f"Not in any preparation or execution window: {current_time}"
                    )
                    logger.info(
                        f"Next soil task time: {next_time}"
                    )
                    logger.info(f"Sleeping for {sleep_seconds} seconds")
                    await validator.update_task_status('soil', 'idle')
                    await asyncio.sleep(sleep_seconds)
                    continue

                is_prep = current_window[1] == 30  # If minutes = 30, it's a prep window

                target_smap_time = self.get_smap_time_for_validator(current_time)
                ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)

                if is_prep:
                    await validator.update_task_status('soil', 'processing', 'data_download')
                    await self.validator_preprocessing.get_daily_regions(
                        target_time=target_smap_time,
                        ifs_forecast_time=ifs_forecast_time,
                    )
                    regions = None # take this out if it's a problem, but then we need another solution for L285
                else:
                    # Get pending regions for this target time
                    query = """
                        SELECT * FROM soil_moisture_regions 
                        WHERE status = 'pending'
                        AND target_time = :target_time
                    """
                    regions = await self.db_manager.fetch_all(query, {"target_time": target_smap_time})

                if regions:
                    for region in regions:
                        try:
                            await validator.update_task_status('soil', 'processing', 'region_processing')
                            logger.info(f"Processing region {region['id']}")

                            if "combined_data" not in region:
                                logger.error(f"Region {region['id']} missing combined_data field")
                                continue

                            if not region["combined_data"]:
                                logger.error(f"Region {region['id']} has null combined_data")
                                continue

                            combined_data = region["combined_data"]
                            if not isinstance(combined_data, bytes):
                                logger.error(f"Region {region['id']} has invalid data type: {type(combined_data)}")
                                continue

                            if not (combined_data.startswith(b"II\x2A\x00") or combined_data.startswith(b"MM\x00\x2A")):
                                logger.error(f"Region {region['id']} has invalid TIFF header")
                                logger.error(f"First 16 bytes: {combined_data[:16].hex()}")
                                continue

                            logger.info(f"Region {region['id']} TIFF size: {len(combined_data) / (1024 * 1024):.2f} MB")
                            logger.info(f"Region {region['id']} TIFF header: {combined_data[:4]}")
                            logger.info(f"Region {region['id']} TIFF header hex: {combined_data[:16].hex()}")
                                
                            # Track memory usage for large TIFF processing
                            tiff_size_mb = len(combined_data) / (1024 * 1024)
                            if tiff_size_mb > 50:
                                logger.warning(f"Processing large TIFF for region {region['id']}: {tiff_size_mb:.1f}MB")
                                
                            # Log memory before processing
                            if hasattr(validator, '_log_memory_usage'):
                                validator._log_memory_usage(f"soil_before_region_{region['id']}")
                                
                            loop = asyncio.get_event_loop()
                            encoded_data_bytes = await loop.run_in_executor(None, base64.b64encode, combined_data)
                            encoded_data_ascii = encoded_data_bytes.decode("ascii")
                            
                            # Immediately clean up large variables to prevent memory accumulation
                            del encoded_data_bytes  # Free the intermediate bytes object
                            original_data_copy = combined_data  # Keep reference for baseline model if needed
                            del combined_data  # Free the original large TIFF data
                            
                            # Log memory after base64 encoding and cleanup
                            if hasattr(validator, '_log_memory_usage'):
                                validator._log_memory_usage(f"soil_after_encoding_{region['id']}")
                                
                            logger.info(f"Base64 first 16 chars: {encoded_data_ascii[:16]}")

                            task_data = {
                                "region_id": region["id"],
                                "combined_data": encoded_data_ascii,
                                "sentinel_bounds": region["sentinel_bounds"],
                                "sentinel_crs": region["sentinel_crs"],
                                "target_time": target_smap_time.isoformat(),
                            }

                            payload = {"nonce": str(uuid4()), "data": task_data}

                            logger.info(f"Sending payload to miners with region_id: {task_data['region_id']}")
                            await validator.update_task_status('soil', 'processing', 'miner_query')
                            responses = await validator.query_miners(
                                payload=payload,
                                endpoint="/soilmoisture-request",
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
                            logger.error(f"Error preparing region: {str(e)}")
                            logger.error(traceback.format_exc())
                            continue

                # Get all prep windows
                prep_windows = [w for w in self.get_validator_windows() if w[1] == 0]
                in_any_prep = any(
                    self.is_in_window(current_time, w) for w in prep_windows
                )

                if not in_any_prep:
                    next_prep_time = self.get_next_preparation_time(current_time)
                    sleep_seconds = (
                        next_prep_time - datetime.now(timezone.utc)
                    ).total_seconds()
                    if sleep_seconds > 0:
                        logger.info(
                            f"Sleeping until next soil task window: {next_prep_time}"
                        )
                        await validator.update_task_status('soil', 'idle')
                        await asyncio.sleep(sleep_seconds)

                if not self.test_mode:
                    await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Error in validator_execute: {e}")
                logger.error(traceback.format_exc())
                await validator.update_task_status('soil', 'error')
                await asyncio.sleep(60)

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

    async def miner_execute(self, data: Dict[str, Any], miner) -> Dict[str, Any]:
        """Execute miner workflow."""
        try:
            processed_data = await self.miner_preprocessing.process_miner_data(
                data["data"]
            )
            
            if hasattr(self.model, "run_inference"):
                predictions = self.model.run_inference(processed_data) # Custom model inference
            else:
                predictions = self.run_model_inference(processed_data) # Base model inference

            try:
                # Visualization disabled for now
                # import matplotlib.pyplot as plt
                # import numpy as np

                # fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
                # surface_plot = ax1.imshow(predictions["surface"], cmap='viridis')
                # ax1.set_title('Surface Soil Moisture')
                # plt.colorbar(surface_plot, ax=ax1, label='Moisture Content')
                # rootzone_plot = ax2.imshow(predictions["rootzone"], cmap='viridis')
                # ax2.set_title('Root Zone Soil Moisture')
                # plt.colorbar(rootzone_plot, ax=ax2, label='Moisture Content')

                # plt.tight_layout()
                # plt.savefig('soil_moisture_predictions.png')
                # plt.close()

                # logger.info("Saved prediction visualization to soil_moisture_predictions.png")
                logger.info(
                    f"Surface moisture stats - Min: {predictions['surface'].min():.3f}, "
                    f"Max: {predictions['surface'].max():.3f}, "
                    f"Mean: {predictions['surface'].mean():.3f}"
                )
                logger.info(
                    f"Root zone moisture stats - Min: {predictions['rootzone'].min():.3f}, "
                    f"Max: {predictions['rootzone'].max():.3f}, "
                    f"Mean: {predictions['rootzone'].mean():.3f}"
                )

            except Exception as e:
                logger.error(f"Error creating visualization: {str(e)}")
            target_time = data["data"]["target_time"]
            if isinstance(target_time, datetime):
                pass
            elif isinstance(target_time, str):
                target_time = datetime.fromisoformat(target_time)
            else:
                logger.error(f"Unexpected target_time type: {type(target_time)}")
                raise ValueError(f"Unexpected target_time format: {target_time}")

            prediction_time = self.get_next_preparation_time(target_time)

            return {
                "surface_sm": predictions["surface"].tolist(),
                "rootzone_sm": predictions["rootzone"].tolist(),
                "uncertainty_surface": None,
                "uncertainty_rootzone": None,
                "miner_hotkey": miner.keypair.ss58_address,
                "sentinel_bounds": data["data"]["sentinel_bounds"],
                "sentinel_crs": data["data"]["sentinel_crs"],
                "target_time": prediction_time.isoformat(),
            }

        except Exception as e:
            logger.error(f"Error in miner execution: {str(e)}")
            logger.error(traceback.format_exc())
            raise

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
            logger.info(f"📊 Task status summary at {datetime.now(timezone.utc)}")
            for row in debug_result:
                logger.info(f"  {row['status']}: {row['count']} tasks, time range: {row['earliest']} to {row['latest']}")

            # Check specifically how many are ready for retry
            retry_ready_query = """
                SELECT 
                    COUNT(CASE WHEN p.status = 'retry_scheduled' AND p.next_retry_time <= :current_time AND p.retry_count < 5 THEN 1 END) as retry_ready,
                    COUNT(CASE WHEN p.status = 'sent_to_miner' AND r.target_time <= :scoring_time THEN 1 END) as scoring_ready,
                    COUNT(CASE WHEN p.status = 'retry_scheduled' AND p.retry_count >= 5 THEN 1 END) as stuck_retries
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                WHERE p.status IN ('sent_to_miner', 'retry_scheduled')
            """
            ready_counts = await self.db_manager.fetch_one(retry_ready_query, {
                "current_time": datetime.now(timezone.utc),
                "scoring_time": scoring_time
            })
            logger.info(f"  Ready for retry: {ready_counts.get('retry_ready', 0)}")
            logger.info(f"  Ready for scoring: {ready_counts.get('scoring_ready', 0)}")
            logger.info(f"  Stuck retries (>=5): {ready_counts.get('stuck_retries', 0)}")

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
                WHERE p.status IN ('sent_to_miner', 'retry_scheduled')
                AND (
                    -- Normal case: Past scoring delay and no retry scheduled
                    (
                        p.status = 'sent_to_miner'
                        AND r.target_time <= :scoring_time 
                        AND p.next_retry_time IS NULL
                    )
                    OR 
                    -- Retry case: Has retry time and it's in the past
                    (
                        p.status IN ('sent_to_miner', 'retry_scheduled')
                        AND p.next_retry_time IS NOT NULL 
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
                    
                    # CRITICAL FIX: Use individual miner scores instead of aggregate scores
                    # Each prediction should have its own score with individual metrics
                    prediction_score = prediction.get("score", {})
                    prediction_metrics = prediction_score.get("metrics", {})
                    
                    # Fall back to aggregate scores only if individual scores are missing
                    if not prediction_metrics:
                        logger.warning(f"No individual metrics found for miner {miner_id}, using aggregate scores")
                        prediction_metrics = scores.get("metrics", {})
                    
                    params = {
                        "region_id": region["id"],
                        "miner_uid": miner_id,
                        "miner_hotkey": prediction.get("miner_hotkey", ""),
                        "target_time": region["target_time"],
                        "surface_sm_pred": prediction["surface_sm"],
                        "rootzone_sm_pred": prediction["rootzone_sm"],
                        "surface_sm_truth": ground_truth["surface_sm"] if ground_truth else None,
                        "rootzone_sm_truth": ground_truth["rootzone_sm"] if ground_truth else None,
                        "surface_rmse": prediction.get("score", {}).get("metrics", {}).get("surface_rmse"),
                        "rootzone_rmse": prediction.get("score", {}).get("metrics", {}).get("rootzone_rmse"),
                        "surface_structure_score": prediction.get("score", {}).get("metrics", {}).get("surface_ssim", 0),
                        "rootzone_structure_score": prediction.get("score", {}).get("metrics", {}).get("rootzone_ssim", 0),
                        "sentinel_bounds": region.get("sentinel_bounds"),
                        "sentinel_crs": region.get("sentinel_crs"),
                    }
                    
                    # Log individual metrics for debugging
                    logger.debug(f"Storing individual metrics for miner {miner_id}: "
                               f"surface_rmse={prediction_metrics.get('surface_rmse'):.4f}, "
                               f"rootzone_rmse={prediction_metrics.get('rootzone_rmse'):.4f}")

                    # Use UPSERT to prevent duplicates in history table
                    upsert_query = """
                        INSERT INTO soil_moisture_history 
                        (region_id, miner_uid, miner_hotkey, target_time,
                            surface_sm_pred, rootzone_sm_pred,
                            surface_sm_truth, rootzone_sm_truth,
                            surface_rmse, rootzone_rmse,
                            surface_structure_score, rootzone_structure_score,
                            sentinel_bounds, sentinel_crs)
                        VALUES 
                        (:region_id, :miner_uid, :miner_hotkey, :target_time,
                            :surface_sm_pred, :rootzone_sm_pred,
                            :surface_sm_truth, :rootzone_sm_truth,
                            :surface_rmse, :rootzone_rmse,
                            :surface_structure_score, :rootzone_structure_score,
                            :sentinel_bounds, :sentinel_crs)
                        ON CONFLICT (region_id, miner_uid, target_time) 
                        DO UPDATE SET 
                            surface_sm_pred = EXCLUDED.surface_sm_pred,
                            rootzone_sm_pred = EXCLUDED.rootzone_sm_pred,
                            surface_sm_truth = EXCLUDED.surface_sm_truth,
                            rootzone_sm_truth = EXCLUDED.rootzone_sm_truth,
                            surface_rmse = EXCLUDED.surface_rmse,
                            rootzone_rmse = EXCLUDED.rootzone_rmse,
                            surface_structure_score = EXCLUDED.surface_structure_score,
                            rootzone_structure_score = EXCLUDED.rootzone_structure_score,
                            sentinel_bounds = EXCLUDED.sentinel_bounds,
                            sentinel_crs = EXCLUDED.sentinel_crs
                    """
                    await self.db_manager.execute(upsert_query, params)

                    update_query = """
                        UPDATE soil_moisture_predictions 
                        SET status = 'scored'
                        WHERE region_id = :region_id 
                        AND miner_uid = :miner_uid
                        AND status IN ('sent_to_miner', 'retry_scheduled')
                    """
                    await self.db_manager.execute(update_query, {
                        "region_id": region["id"],
                        "miner_uid": miner_id
                    })

                except Exception as e:
                    logger.error(f"Error processing prediction for miner {miner_id}: {str(e)}")
                    continue

            logger.info(f"Moved {len(predictions)} tasks to history for region {region['id']}")

            # Clean up ALL predictions for this region/target_time (not just one miner)
            await self.cleanup_predictions(
                bounds=region["sentinel_bounds"],
                target_time=region["target_time"],
                miner_uid=None  # Clean up all miners for this region
            )

            return True

        except Exception as e:
            logger.error(f"Failed to move task to history: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    async def validator_score(self, result=None):
        """Score task results by timestamp to minimize SMAP data downloads."""
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

            for target_time, tasks_in_time_window in tasks_by_time.items():
                temp_path = None
                try:
                    logger.info(f"🌍 Processing tasks for target_time: {target_time} ({len(tasks_in_time_window)} regions)")
                    # Transform tasks into the format expected by get_smap_data
                    regions_for_smap = []
                    for task in tasks_in_time_window:
                        regions_for_smap.append({
                            "bounds": task["sentinel_bounds"],
                            "crs": task["sentinel_crs"]
                        })
                    smap_data_result = await get_smap_data_multi_region(target_time, regions_for_smap)
                    
                    if smap_data_result is None or not isinstance(smap_data_result, dict):
                        logger.error(f"Failed to download or process SMAP data for {target_time}")
                        # Update retry information for failed tasks
                        for task in tasks_in_time_window:
                            for prediction in task["predictions"]:
                                update_query = """
                                    UPDATE soil_moisture_predictions
                                    SET retry_count = COALESCE(retry_count, 0) + 1,
                                        next_retry_time = :next_retry_time,
                                        retry_error_message = :error_message,
                                        status = 'retry_scheduled'
                                    WHERE region_id = :region_id
                                    AND miner_uid = :miner_uid
                                """
                                params = {
                                    "region_id": task["id"],
                                    "miner_uid": prediction["miner_id"],
                                    "next_retry_time": datetime.now(timezone.utc) + timedelta(hours=2),  # SMAP data availability issue
                                    "error_message": "Failed to download SMAP data"
                                }
                                await self.db_manager.execute(update_query, params)
                        continue
                    
                    # Check if SMAP result contains error information (enhanced error handling)
                    if "success" in smap_data_result and not smap_data_result["success"]:
                        error_type = smap_data_result.get("error_type", "unknown")
                        status_code = smap_data_result.get("status_code")
                        error_message = smap_data_result.get("message", "Unknown error")
                        
                        # Determine retry timing based on error type
                        if error_type == "http_error" and status_code == 404:
                            # Data not available yet - retry sooner since data might become available
                            retry_hours = 1  # Retry in 1 hour for 404 errors
                            detailed_message = f"SMAP data not available yet (HTTP 404) for {target_time}"
                            logger.warning(f"⏳ {detailed_message} - will retry in {retry_hours} hour(s)")
                        elif error_type == "http_error" and status_code in [401, 403]:
                            # Authentication issue - retry later but log as concerning
                            retry_hours = 4  # Longer retry for auth issues
                            detailed_message = f"SMAP authentication failed (HTTP {status_code}) for {target_time}"
                            logger.error(f"🔐 {detailed_message} - will retry in {retry_hours} hour(s)")
                        elif error_type == "http_error" and status_code >= 500:
                            # Server error - retry moderately soon
                            retry_hours = 2  # Standard retry for server errors
                            detailed_message = f"SMAP server error (HTTP {status_code}) for {target_time}"
                            logger.error(f"🔧 {detailed_message} - will retry in {retry_hours} hour(s)")
                        elif error_type == "network_error":
                            # Network issue - retry moderately soon
                            retry_hours = 1.5  # Quick retry for network issues
                            detailed_message = f"Network error accessing SMAP data for {target_time}: {error_message}"
                            logger.warning(f"🌐 {detailed_message} - will retry in {retry_hours} hour(s)")
                        else:
                            # Other errors - use default timing
                            retry_hours = 2
                            detailed_message = f"SMAP error ({error_type}) for {target_time}: {error_message}"
                            logger.error(f"❌ {detailed_message} - will retry in {retry_hours} hour(s)")
                        
                        # Update retry information with detailed error
                        for task in tasks_in_time_window:
                            for prediction in task["predictions"]:
                                update_query = """
                                    UPDATE soil_moisture_predictions
                                    SET retry_count = COALESCE(retry_count, 0) + 1,
                                        next_retry_time = :next_retry_time,
                                        retry_error_message = :error_message,
                                        status = 'retry_scheduled'
                                    WHERE region_id = :region_id
                                    AND miner_uid = :miner_uid
                                """
                                params = {
                                    "region_id": task["id"],
                                    "miner_uid": prediction["miner_id"],
                                    "next_retry_time": datetime.now(timezone.utc) + timedelta(hours=retry_hours),
                                    "error_message": detailed_message
                                }
                                await self.db_manager.execute(update_query, params)
                        continue
                    
                    # Extract file path and processed data
                    temp_path = smap_data_result.get("file_path")
                    smap_processed_data = smap_data_result.get("data", {})
                    
                    if not temp_path or not os.path.exists(temp_path):
                        logger.error(f"SMAP data file missing after download for {target_time}")
                        # Update retry information for failed tasks - this indicates file processing issues
                        for task in tasks_in_time_window:
                            for prediction in task["predictions"]:
                                update_query = """
                                    UPDATE soil_moisture_predictions
                                    SET retry_count = COALESCE(retry_count, 0) + 1,
                                        next_retry_time = :next_retry_time,
                                        retry_error_message = :error_message,
                                        status = 'retry_scheduled'
                                    WHERE region_id = :region_id
                                    AND miner_uid = :miner_uid
                                """
                                params = {
                                    "region_id": task["id"],
                                    "miner_uid": prediction["miner_id"],
                                    "next_retry_time": datetime.now(timezone.utc) + timedelta(hours=1),  # Quick retry for file processing issues
                                    "error_message": f"SMAP data file missing after download for {target_time}"
                                }
                                await self.db_manager.execute(update_query, params)
                        continue

                    for task in tasks_in_time_window:
                        try:
                            # Detect if this is a retry scenario
                            is_retry_scenario = any(
                                pred.get("retry_count", 0) > 0 or pred.get("next_retry_time") is not None 
                                for pred in task.get("predictions", [])
                            )
                            
                            # Use threaded scoring if enabled for performance improvement
                            if self.use_threaded_scoring:
                                if is_retry_scenario:
                                    logger.info(f"🔄 Using threaded scoring for RETRY scenario - Region {task['id']} with {len(task.get('predictions', []))} miners")
                                else:
                                    logger.info(f"🚀 Using threaded scoring for regular scenario - Region {task['id']} with {len(task.get('predictions', []))} miners")
                                scored_predictions, task_ground_truth = await self._score_predictions_threaded(task, temp_path)
                            else:
                                if is_retry_scenario:
                                    logger.info(f"🔄 Using sequential scoring for RETRY scenario - Region {task['id']} with {len(task.get('predictions', []))} miners")
                                else:
                                    logger.info(f"⏳ Using sequential scoring for regular scenario - Region {task['id']} with {len(task.get('predictions', []))} miners")
                                # Original sequential scoring
                                scored_predictions = []
                                task_ground_truth = None
                                
                                for prediction in task["predictions"]:
                                    pred_data = {
                                        "bounds": task["sentinel_bounds"],
                                        "crs": task["sentinel_crs"],
                                        "predictions": prediction,
                                        "target_time": target_time,
                                        "region": {"id": task["id"]},
                                        "miner_id": prediction["miner_id"],
                                        "miner_hotkey": prediction["miner_hotkey"],
                                        "smap_file": temp_path,
                                        "smap_file_path": temp_path
                                    }
                                    
                                    score = await self.scoring_mechanism.score(pred_data)
                                    if score:
                                        baseline_score = None
                                        if self.validator and hasattr(self.validator, 'basemodel_evaluator'):
                                            try:
                                                if isinstance(target_time, datetime):
                                                    if target_time.tzinfo is not None:
                                                        target_time_utc = target_time.astimezone(timezone.utc)
                                                    else:
                                                        target_time_utc = target_time.replace(tzinfo=timezone.utc)
                                                else:
                                                    target_time_utc = target_time
                                                task_id = str(target_time_utc.timestamp())
                                                smap_file_to_use = temp_path if temp_path and os.path.exists(temp_path) else None
                                                if smap_file_to_use:
                                                    logger.info(f"Using existing SMAP file for baseline scoring: {smap_file_to_use}")
                                                
                                                self.validator.basemodel_evaluator.test_mode = self.test_mode
                                                
                                                baseline_score = await self.validator.basemodel_evaluator.score_soil_baseline(
                                                    task_id=task_id,
                                                    region_id=str(task["id"]),
                                                    ground_truth=score.get("ground_truth", {}),
                                                    smap_file_path=smap_file_to_use
                                                )
                                                
                                                if baseline_score is not None:
                                                    miner_score = score.get("total_score", 0)
                                                    
                                                    miner_metrics = score.get("metrics", {})
                                                    logger.info(f"Soil Task - Miner: {prediction['miner_id']}, Region: {task['id']} - Miner: {miner_score:.4f}, Baseline: {baseline_score:.4f}, Diff: {miner_score - baseline_score:.4f}")
                                                    
                                                    if "surface_rmse" in miner_metrics:
                                                        surface_rmse = miner_metrics["surface_rmse"]
                                                        
                                                        baseline_metrics = getattr(self.validator.basemodel_evaluator.soil_scoring, '_last_baseline_metrics', {})
                                                        baseline_surface_rmse = baseline_metrics.get("validation_metrics", {}).get("surface_rmse")
                                                        
                                                        if baseline_surface_rmse is not None:
                                                            logger.debug(f"Surface RMSE - Miner: {surface_rmse:.4f}, Baseline: {baseline_surface_rmse:.4f}, Diff: {baseline_surface_rmse - surface_rmse:.4f}")
                                                    
                                                    if "rootzone_rmse" in miner_metrics:
                                                        rootzone_rmse = miner_metrics["rootzone_rmse"]
                                                        
                                                        baseline_metrics = getattr(self.validator.basemodel_evaluator.soil_scoring, '_last_baseline_metrics', {})
                                                        baseline_rootzone_rmse = baseline_metrics.get("validation_metrics", {}).get("rootzone_rmse")
                                                        
                                                        if baseline_rootzone_rmse is not None:
                                                            logger.debug(f"Rootzone RMSE - Miner: {rootzone_rmse:.4f}, Baseline: {baseline_rootzone_rmse:.4f}, Diff: {baseline_rootzone_rmse - rootzone_rmse:.4f}")
                                                    
                                                    standard_epsilon = 0.005
                                                    excellent_rmse_threshold = 0.04
                                                    
                                                    baseline_metrics = getattr(self.validator.basemodel_evaluator.soil_scoring, '_last_baseline_metrics', {})
                                                    baseline_surface_rmse = baseline_metrics.get("validation_metrics", {}).get("surface_rmse")
                                                    baseline_rootzone_rmse = baseline_metrics.get("validation_metrics", {}).get("rootzone_rmse")
                                                    
                                                    has_excellent_performance = False
                                                    avg_baseline_rmse = None
                                                    
                                                    if baseline_surface_rmse is not None and baseline_rootzone_rmse is not None:
                                                        avg_baseline_rmse = (baseline_surface_rmse + baseline_rootzone_rmse) / 2
                                                        has_excellent_performance = avg_baseline_rmse <= excellent_rmse_threshold
                                                        logger.debug(f"Average baseline RMSE: {avg_baseline_rmse:.4f}")
                                                        
                                                        if has_excellent_performance:
                                                            logger.debug(f"Baseline has excellent performance (RMSE <= {excellent_rmse_threshold})")
                                                    
                                                    passes_comparison = False
                                                    
                                                    if has_excellent_performance and avg_baseline_rmse is not None:
                                                        allowed_score_range = baseline_score * 0.95
                                                        passes_comparison = miner_score >= allowed_score_range
                                                        
                                                        if passes_comparison:
                                                            if miner_score >= baseline_score:
                                                                logger.info(f"Score valid - Exceeds excellent baseline: {miner_score:.4f} > {baseline_score:.4f}")
                                                            else:
                                                                logger.info(f"Score valid - Within 5% of excellent baseline: {miner_score:.4f} vs {baseline_score:.4f} (min: {allowed_score_range:.4f})")
                                                        else:
                                                            logger.info(f"Score zeroed - Too far below excellent baseline: {miner_score:.4f} < {allowed_score_range:.4f}")
                                                            score["total_score"] = 0
                                                    else:
                                                        passes_comparison = miner_score > baseline_score + standard_epsilon
                                                        
                                                        if not passes_comparison:
                                                            if miner_score < baseline_score:
                                                                logger.info(f"Score zeroed - Below baseline: {miner_score:.4f} < {baseline_score:.4f}")
                                                            elif miner_score == baseline_score:
                                                                logger.info(f"Score zeroed - Equal to baseline: {miner_score:.4f}")
                                                            else:
                                                                logger.info(f"Score zeroed - Insufficient improvement: {miner_score:.4f} vs baseline {baseline_score:.4f} (needed > {baseline_score + standard_epsilon:.4f})")
                                                            
                                                            score["total_score"] = 0
                                                        else:
                                                            logger.info(f"Score valid - Exceeds baseline by {miner_score - baseline_score:.4f} (threshold: {standard_epsilon:.4f})")
                                            except Exception as e:
                                                logger.error(f"Error retrieving baseline score: {e}")
                                        
                                        # Store the scored prediction and ground truth
                                        prediction["score"] = score
                                        scored_predictions.append(prediction)
                                        if task_ground_truth is None:
                                            task_ground_truth = score.get("ground_truth")
                                            
                                    else:
                                        # Update retry information for failed scoring
                                        update_query = """
                                            UPDATE soil_moisture_predictions
                                            SET retry_count = COALESCE(retry_count, 0) + 1,
                                                next_retry_time = :next_retry_time,
                                                retry_error_message = :error_message,
                                                status = 'retry_scheduled'
                                            WHERE region_id = :region_id
                                            AND miner_uid = :miner_uid
                                        """
                                        params = {
                                            "region_id": task["id"],
                                            "miner_uid": prediction["miner_id"],
                                            "next_retry_time": datetime.now(timezone.utc) + timedelta(minutes=5),  # Scoring error - quick retry
                                            "error_message": "Failed to calculate score"
                                        }
                                        await self.db_manager.execute(update_query, params)
                            
                            # Move to history ONCE per task, only if we have scored predictions
                            if scored_predictions:
                                # Use the first scored prediction's metrics for the task score
                                task_score = scored_predictions[0]["score"]
                                await self.move_task_to_history(
                                    region=task,
                                    predictions=scored_predictions,  # Only scored predictions
                                    ground_truth=task_ground_truth,
                                    scores=task_score
                                )

                        except Exception as e:
                            logger.error(f"Error scoring task {task['id']}: {str(e)}")
                            continue

                    score_rows = await self.build_score_row(target_time, tasks_in_time_window)
                    if score_rows:
                        # Check if scores already exist for this timestamp (indicating a retry scenario)
                        check_existing_query = """
                            SELECT COUNT(*) as count FROM score_table 
                            WHERE task_name = 'soil_moisture_region_global' 
                            AND task_id = :task_id
                        """
                        task_id = str(datetime.fromisoformat(str(target_time)).timestamp())
                        existing_result = await self.db_manager.fetch_one(check_existing_query, {"task_id": task_id})
                        has_existing_scores = existing_result and existing_result.get("count", 0) > 0
                        
                        if has_existing_scores:
                            # Use UPSERT for existing scores (retry scenario)
                            upsert_query = """
                                INSERT INTO score_table 
                                (task_name, task_id, score, status)
                                VALUES 
                                (:task_name, :task_id, :score, :status)
                                ON CONFLICT (task_name, task_id) 
                                DO UPDATE SET 
                                    score = EXCLUDED.score,
                                    status = EXCLUDED.status
                            """
                            for score_row in score_rows:
                                await self.db_manager.execute(upsert_query, score_row)
                            logger.info(f"Updated global scores for timestamp {target_time} (existing scores found)")
                        else:
                            # Regular insert for new scores
                            insert_query = """
                                INSERT INTO score_table 
                                (task_name, task_id, score, status)
                                VALUES 
                                (:task_name, :task_id, :score, :status)
                            """
                            for score_row in score_rows:
                                await self.db_manager.execute(insert_query, score_row)
                            logger.info(f"Stored global scores for timestamp {target_time}")

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

    async def validator_prepare_subtasks(self):
        """Prepare the subtasks for execution.

        Returns:
            List[Dict]: List of subtasks
        """
        pass

    def run_model_inference(self, processed_data):
        """Run model inference on processed data."""
        if not self.model:
            raise RuntimeError(
                "Model not initialized. Are you running on a miner node?"
            )

        return self.miner_preprocessing.predict_smap(processed_data, self.model)

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
            current_hour = current_time.hour
            closest_hour = min(smap_hours, key=lambda x: abs(x - current_hour))
            return current_time.replace(
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
        smap_hour = validator_to_smap.get(current_time.hour)
        if smap_hour is None:
            raise ValueError(f"No SMAP time mapping for validator hour {current_time.hour}")

        return current_time.replace(hour=smap_hour, minute=30, second=0, microsecond=0)

    def get_validator_windows(self) -> List[Tuple[int, int, int, int]]:
        """Get all validator windows (hour_start, min_start, hour_end, min_end)."""
        return [
            (1, 30, 2, 0),  # Prep window for 1:30 SMAP time
            (2, 0, 2, 30),  # Execution window for 1:30 SMAP time
            (9, 30, 10, 0),  # Prep window for 7:30 SMAP time
            (10, 0, 10, 30),  # Execution window for 7:30 SMAP time
            (13, 30, 14, 0),  # Prep window for 13:30 SMAP time
            (14, 0, 14, 30),  # Execution window for 13:30 SMAP time
            (19, 30, 20, 0),  # Prep window for 19:30 SMAP time
            (20, 0, 20, 30),  # Execution window for 19:30 SMAP time
        ]

    def is_in_window(
        self, current_time: datetime, window: Tuple[int, int, int, int]
    ) -> bool:
        """Check if current time is within a specific window."""
        start_hr, start_min, end_hr, end_min = window
        current_mins = current_time.hour * 60 + current_time.minute
        window_start_mins = start_hr * 60 + start_min
        window_end_mins = end_hr * 60 + end_min
        return window_start_mins <= current_mins < window_end_mins

    def miner_preprocess(self, preprocessing=None, inputs=None):
        """Preprocess data for model input."""
        pass

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
            logger.info(f"Found {len(hotkey_to_uid)} miner mappings")

            scores = [float("nan")] * 256
            current_datetime = datetime.fromisoformat(str(target_time))

            # Check if we're processing a retry batch - if so, allow overwriting existing scores
            has_retry_tasks = False
            if recent_tasks:
                for task in recent_tasks:
                    for prediction in task.get("predictions", []):
                        # Check if any of the tasks have retry information in the database
                        retry_check_query = """
                            SELECT retry_count FROM soil_moisture_predictions 
                            WHERE miner_uid = :miner_id AND target_time = :target_time
                        """
                        retry_result = await self.db_manager.fetch_one(retry_check_query, {
                            "miner_id": prediction.get("miner_id"),
                            "target_time": target_time
                        })
                        if retry_result and retry_result.get("retry_count", 0) > 0:
                            has_retry_tasks = True
                            break
                    if has_retry_tasks:
                        break
            
            # Only skip if scores exist AND this is not a retry batch
            if not has_retry_tasks:
                check_query = """
                    SELECT COUNT(*) as count FROM score_table 
                    WHERE task_name = 'soil_moisture_region_global' 
                    AND task_id = :task_id
                """
                result = await self.db_manager.fetch_one(check_query, {"task_id": str(current_datetime.timestamp())})
                if result and result["count"] > 0:
                    logger.warning(f"Score row already exists for target_time {target_time}. Skipping.")
                    return []
            else:
                logger.info(f"Processing retry batch for target_time {target_time}. Will update existing scores.")

            if recent_tasks:
                logger.info(f"Processing {len(recent_tasks)} recent tasks across all regions")
                
                processed_region_ids = set()
                miner_scores = defaultdict(list)
                region_counts = defaultdict(set)
                
                for task in recent_tasks:
                    region_id = task.get("id", "unknown")
                    
                    if region_id in processed_region_ids:
                        logger.warning(f"Skipping duplicate region {region_id}")
                        continue
                    
                    processed_region_ids.add(region_id)
                    task_score = task.get("score", {})
                    
                    logger.info(f"Processing scores for region {region_id}")
                    
                    for prediction in task.get("predictions", []):
                        miner_uid_from_prediction = prediction.get("miner_id")
                        miner_hotkey_from_prediction = prediction.get("miner_hotkey")

                        if miner_uid_from_prediction is None or miner_hotkey_from_prediction is None:
                            logger.warning(f"Skipping prediction due to missing UID or Hotkey: UID {miner_uid_from_prediction}, Hotkey {miner_hotkey_from_prediction}")
                            continue
                        
                        is_valid_in_metagraph = False
                        if self.validator and hasattr(self.validator, 'metagraph') and self.validator.metagraph is not None:
                            # Check if the hotkey exists in the metagraph's nodes dictionary
                            if miner_hotkey_from_prediction in self.validator.metagraph.nodes:
                                # Retrieve the Node object from the metagraph
                                node_in_metagraph = self.validator.metagraph.nodes[miner_hotkey_from_prediction]
                                # Compare the UID from the prediction with the UID from the metagraph node
                                if hasattr(node_in_metagraph, 'node_id') and str(node_in_metagraph.node_id) == str(miner_uid_from_prediction):
                                    is_valid_in_metagraph = True
                                else:
                                    metagraph_uid_str = getattr(node_in_metagraph, 'node_id', '[UID not found]')
                                    logger.warning(f"Metagraph UID mismatch for {miner_hotkey_from_prediction}: Prediction UID {miner_uid_from_prediction}, Metagraph Node UID {metagraph_uid_str}. Skipping score.")
                            else:
                                logger.warning(f"Miner hotkey {miner_hotkey_from_prediction} not found in current metagraph. Prediction UID {miner_uid_from_prediction}. Skipping score.")
                        else:
                            logger.warning("Validator or metagraph not available for validation. Cannot confirm miner registration. Skipping score.")
                            
                        if not is_valid_in_metagraph:
                            continue
                            
                        if isinstance(task_score.get("total_score"), (int, float)):
                            score_value = float(task_score["total_score"])
                            miner_scores[miner_uid_from_prediction].append(score_value)
                            region_counts[miner_uid_from_prediction].add(region_id)
                            logger.info(f"Added score {score_value:.4f} for miner_id {miner_uid_from_prediction} in region {region_id}")

                for miner_id, scores_list in miner_scores.items():
                    if scores_list:
                        avg_score = sum(scores_list) / len(scores_list)
                        region_count = len(region_counts[miner_id])
                        scores[int(miner_id)] = avg_score
                        logger.info(f"Final average score for miner {miner_id}: {avg_score:.4f} across {region_count} unique regions")
                        
                        if region_count > 5:
                            logger.warning(f"Unexpected number of regions ({region_count}) for miner {miner_id}. Expected maximum 5.")
                
                # MEMORY LEAK FIX: Clear intermediate data structures
                try:
                    del processed_region_ids, miner_scores, region_counts
                    import gc
                    collected = gc.collect()
                    logger.debug(f"Soil score building cleanup: collected {collected} objects")
                except Exception as cleanup_err:
                    logger.debug(f"Error during soil score cleanup: {cleanup_err}")

            score_row = {
                "task_name": "soil_moisture_region_global",
                "task_id": str(current_datetime.timestamp()),
                "score": scores,
                "status": "completed"
            }
            
            non_nan_scores = [(i, s) for i, s in enumerate(scores) if not math.isnan(s)]
            logger.info(f"Built score row with {len(non_nan_scores)} non-NaN scores for target time {target_time}")
            if non_nan_scores:
                logger.info(f"Score summary - min: {min(s for _, s in non_nan_scores):.4f}, max: {max(s for _, s in non_nan_scores):.4f}")
                logger.info(f"Regions per miner: {', '.join([f'miner_{mid}={len(regions)}' for mid, regions in region_counts.items() if len(regions) > 0])}")
            return [score_row]

        except Exception as e:
            logger.error(f"Error building score row: {e}")
            logger.error(traceback.format_exc())
            return []


    async def cleanup_predictions(self, bounds, target_time=None, miner_uid=None):
        """Clean up predictions after they've been processed and moved to history."""
        try:
            if miner_uid is not None:
                # Clean up specific miner's predictions
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
            else:
                # Clean up ALL scored predictions for this region/target_time
                delete_query = """
                    DELETE FROM soil_moisture_predictions p
                    USING soil_moisture_regions r
                    WHERE p.region_id = r.id 
                    AND r.sentinel_bounds = :bounds
                    AND r.target_time = :target_time
                    AND p.status = 'scored'
                """
                params = {
                    "bounds": bounds,
                    "target_time": target_time
                }
            
            result = await self.db_manager.execute(delete_query, params)
            
            # Try to get the number of deleted rows
            rows_deleted = getattr(result, 'rowcount', 0) if result else 0
            
            logger.info(
                f"Cleaned up {rows_deleted} predictions for bounds {bounds}"
                f"{f', time {target_time}' if target_time else ''}"
                f"{f', miner {miner_uid}' if miner_uid else ' (all miners)'}"
            )

        except Exception as e:
            logger.error(f"Failed to cleanup predictions: {str(e)}")
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

    async def _startup_retry_check(self):
        """Comprehensive startup retry check - aggressively find and retry stuck tasks."""
        try:
            current_time = datetime.now(timezone.utc)
            
            # First, clean up stuck retry tasks
            logger.info("🧹 Cleaning up stuck retry tasks...")
            try:
                deleted_count = await self.cleanup_stuck_retries(max_retry_limit=5, days_back=7)
                if deleted_count > 0:
                    logger.info(f"Cleaned up {deleted_count} stuck retry tasks during startup")
            except Exception as e:
                logger.error(f"Error during stuck retry cleanup: {e}")
            
            # Then check for and build any missing retroactive score rows
            logger.info("🔍 Checking for missing soil moisture score rows...")
            try:
                await self.build_retroactive_score_rows(days_back=7, force_rebuild=False)
            except Exception as e:
                logger.error(f"Error during retroactive score building: {e}")
                # Continue with regular startup retry check even if retroactive scoring fails
            
            # First, let's check what we have in the database
            status_query = """
                SELECT 
                    p.status,
                    COUNT(*) as count,
                    MIN(r.target_time) as earliest_time,
                    MAX(r.target_time) as latest_time,
                    COUNT(DISTINCT p.miner_uid) as unique_miners
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                WHERE p.status IN ('sent_to_miner', 'retry_scheduled')
                GROUP BY p.status
            """
            status_summary = await self.db_manager.fetch_all(status_query)
            
            total_stuck_tasks = 0
            for row in status_summary:
                total_stuck_tasks += row['count']
                logger.info(f"Found {row['count']} tasks with status '{row['status']}' - "
                          f"miners: {row['unique_miners']}, time range: {row['earliest_time']} to {row['latest_time']}")
            
            if total_stuck_tasks == 0:
                logger.info("✅ No stuck tasks found in database")
                return
            
            logger.warning(f"🚨 Found {total_stuck_tasks} potentially stuck soil moisture tasks")
            
            # Aggressive eligibility criteria for startup
            eligible_query = """
                SELECT 
                    r.*,
                    json_agg(json_build_object(
                        'miner_id', p.miner_uid,
                        'miner_hotkey', p.miner_hotkey,
                        'retry_count', p.retry_count,
                        'next_retry_time', p.next_retry_time,
                        'retry_error_message', p.retry_error_message,
                        'surface_sm', p.surface_sm,
                        'rootzone_sm', p.rootzone_sm,
                        'uncertainty_surface', p.uncertainty_surface,
                        'uncertainty_rootzone', p.uncertainty_rootzone
                    )) as predictions
                FROM soil_moisture_regions r
                JOIN soil_moisture_predictions p ON p.region_id = r.id
                WHERE p.status IN ('sent_to_miner', 'retry_scheduled')
                AND (
                    -- Any scheduled retry that's ready or overdue
                    (
                        p.status = 'retry_scheduled'
                        AND p.next_retry_time IS NOT NULL 
                        AND p.next_retry_time <= :current_time
                        AND COALESCE(p.retry_count, 0) < 10
                    )
                    OR
                    -- Any task that's been sitting for more than 30 minutes
                    (
                        p.status = 'sent_to_miner'
                        AND r.target_time <= :recent_cutoff_time
                        AND COALESCE(p.retry_count, 0) < 10
                    )
                    OR
                    -- Processing/scoring errors - immediate retry on startup
                    (
                        p.status IN ('sent_to_miner', 'retry_scheduled')
                        AND p.retry_error_message IS NOT NULL
                        AND (
                            p.retry_error_message LIKE '%processing%' OR
                            p.retry_error_message LIKE '%scoring%' OR
                            p.retry_error_message LIKE '%calculate%' OR
                            p.retry_error_message LIKE '%_FillValue%'
                        )
                        AND COALESCE(p.retry_count, 0) < 10
                    )
                )
                GROUP BY r.id, r.target_time, r.sentinel_bounds, r.sentinel_crs, r.status
                ORDER BY r.target_time ASC
                LIMIT 50
            """
            
            # Look for tasks older than 30 minutes for immediate startup retry
            recent_cutoff_time = current_time - timedelta(minutes=30)
            
            params = {
                "current_time": current_time,
                "recent_cutoff_time": recent_cutoff_time
            }
            
            eligible_tasks = await self.db_manager.fetch_all(eligible_query, params)
            
            if not eligible_tasks:
                logger.info("✅ No tasks eligible for immediate startup retry")
                return
            
            logger.info(f"🔄 Found {len(eligible_tasks)} tasks eligible for immediate startup retry")
            
            # Count different types and reset retry times for immediate processing
            scheduled_retries = 0
            old_pending = 0
            processing_errors = 0
            immediate_retry_count = 0
            
            for task in eligible_tasks:
                for pred in task["predictions"]:
                    retry_error_message = pred.get("retry_error_message", "") or ""
                    
                    if pred.get("next_retry_time"):
                        scheduled_retries += 1
                    else:
                        old_pending += 1
                    
                    if retry_error_message and any(keyword in retry_error_message.lower() for keyword in ['processing', 'scoring', 'calculate', '_fillvalue']):
                        processing_errors += 1
                        # Force immediate retry for processing/scoring errors
                        immediate_retry_query = """
                            UPDATE soil_moisture_predictions
                            SET next_retry_time = :immediate_time,
                                status = 'retry_scheduled'
                            WHERE miner_uid = :miner_id 
                            AND target_time = :target_time
                        """
                        await self.db_manager.execute(immediate_retry_query, {
                            "miner_id": pred["miner_id"],
                            "target_time": task["target_time"],
                            "immediate_time": current_time - timedelta(seconds=1)  # Make it ready now
                        })
                        immediate_retry_count += 1
                        
            logger.info(f"   - {scheduled_retries} scheduled retries ready")
            logger.info(f"   - {old_pending} old pending tasks from previous session")
            logger.info(f"   - {processing_errors} processing/scoring errors")
            logger.info(f"   - {immediate_retry_count} tasks set for immediate retry")
            
            # Force processing of old sent_to_miner tasks
            logger.info("🚀 Checking for old sent_to_miner tasks...")
            try:
                await self.force_process_old_predictions()
            except Exception as e:
                logger.error(f"Error during forced processing: {e}")
            
            # Force an immediate scoring attempt for eligible retry tasks
            if eligible_tasks:
                logger.info("🚀 Starting aggressive startup retry scoring...")
                try:
                    # Run multiple scoring attempts to clear backlog
                    for attempt in range(3):
                        logger.info(f"Startup retry attempt {attempt + 1}/3")
                        result = await self.validator_score()
                        if result.get("status") == "no_pending_tasks":
                            logger.info("✅ All startup retries completed successfully")
                            break
                        await asyncio.sleep(10)  # Short delay between attempts
                except Exception as e:
                    logger.error(f"Error during startup retry scoring: {e}")
                
        except Exception as e:
            logger.error(f"Error in startup retry check: {e}")
            logger.error(traceback.format_exc())

    async def force_immediate_retries(self, error_types=None):
        """Force immediate retry of stuck tasks, optionally filtered by error type."""
        try:
            current_time = datetime.now(timezone.utc)
            
            if error_types is None:
                # Default to processing/scoring errors that should retry immediately
                error_types = ['processing', 'scoring', 'calculate', '_fillvalue', 'failed to calculate']
            
            logger.info(f"🔧 Forcing immediate retries for error types: {error_types}")
            
            # Find all tasks with these error types
            filter_conditions = " OR ".join([f"p.retry_error_message ILIKE '%{error_type}%'" for error_type in error_types])
            
            force_retry_query = f"""
                UPDATE soil_moisture_predictions
                SET next_retry_time = :immediate_time,
                    status = 'retry_scheduled'
                WHERE status IN ('sent_to_miner', 'retry_scheduled')
                AND retry_error_message IS NOT NULL
                AND ({filter_conditions})
                AND COALESCE(retry_count, 0) < 10
                RETURNING miner_uid, target_time, retry_error_message
            """
            
            updated_tasks = await self.db_manager.fetch_all(force_retry_query, {
                "immediate_time": current_time - timedelta(seconds=1)  # Make it ready now
            })
            
            if updated_tasks:
                logger.info(f"✅ Forced immediate retry for {len(updated_tasks)} stuck tasks")
                for task in updated_tasks:
                    logger.info(f"   - Miner {task['miner_uid']} at {task['target_time']}: {task['retry_error_message']}")
                
                # Trigger scoring immediately
                logger.info("🚀 Starting forced retry scoring...")
                await self.validator_score()
            else:
                logger.info("ℹ️  No tasks found matching the specified error types")
                
        except Exception as e:
            logger.error(f"Error in force_immediate_retries: {e}")
            logger.error(traceback.format_exc())

    async def build_retroactive_score_rows(self, days_back=7, force_rebuild=False):
        """
        Retroactively build score rows by scoring individual predictions against ground truth.
        
        This method:
        1. Finds unscored predictions in soil_moisture_predictions table
        2. Scores each prediction individually against SMAP ground truth
        3. Aggregates scores by target_time to build score rows
        4. Moves scored predictions to history table and cleans up
        
        Args:
            days_back (int): How many days back to look for unscored predictions
            force_rebuild (bool): Whether to rebuild existing score rows
        """
        try:
            logger.info(f"🔄 Starting retroactive prediction scoring for last {days_back} days...")
            
            # Get cutoff time
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
            
            # Get miner mappings for hotkey validation
            miner_query = """
                SELECT uid, hotkey FROM node_table 
                WHERE hotkey IS NOT NULL
            """
            miner_mappings = await self.db_manager.fetch_all(miner_query)
            hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}
            logger.info(f"Found {len(hotkey_to_uid)} miner mappings for retroactive scoring")
            
            # Find predictions that need scoring - completed but not yet moved to history
            unscored_predictions_query = """
                SELECT p.*, r.sentinel_bounds, r.sentinel_crs, r.target_time
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                WHERE r.target_time >= :cutoff_time 
                AND p.status = 'completed'
                AND NOT EXISTS (
                    SELECT 1 FROM soil_moisture_history h 
                    WHERE h.region_id = p.region_id 
                    AND h.miner_uid = p.miner_uid 
                    AND h.target_time = r.target_time
                )
                ORDER BY r.target_time DESC, p.region_id, p.miner_uid
            """
            
            unscored_predictions = await self.db_manager.fetch_all(unscored_predictions_query, {"cutoff_time": cutoff_time})
            
            if not unscored_predictions:
                logger.info("✅ No unscored predictions found for retroactive scoring")
                return
            
            logger.info(f"🔍 Found {len(unscored_predictions)} unscored predictions for retroactive scoring")
            
            # Group predictions by target_time for batch processing
            predictions_by_time = defaultdict(list)
            for pred in unscored_predictions:
                predictions_by_time[pred['target_time']].append(pred)
            
            logger.info(f"📊 Processing {len(predictions_by_time)} unique target times:")
            for target_time, preds in predictions_by_time.items():
                logger.info(f"   - {target_time}: {len(preds)} predictions")
            
            successful_builds = 0
            failed_builds = 0
            total_scored_predictions = 0
            
            # Process each target time
            for target_time, predictions in predictions_by_time.items():
                try:
                    logger.info(f"📊 Processing retroactive scoring for {target_time} ({len(predictions)} predictions)...")
                    
                    # Check if score row already exists (unless force rebuild)
                    if not force_rebuild:
                        existing_check_query = """
                            SELECT COUNT(*) as count FROM score_table 
                            WHERE task_name = 'soil_moisture_region_global' 
                            AND task_id = CAST(EXTRACT(EPOCH FROM CAST(:target_time AS TIMESTAMPTZ)) AS TEXT)
                        """
                        existing_result = await self.db_manager.fetch_one(existing_check_query, {"target_time": target_time})
                        if existing_result and existing_result.get("count", 0) > 0:
                            logger.info(f"⏭️  Score row already exists for {target_time}, skipping")
                            continue
                    
                    # Group predictions by region for scoring
                    predictions_by_region = defaultdict(list)
                    for pred in predictions:
                        predictions_by_region[pred['region_id']].append(pred)
                    
                    # Score each region's predictions
                    miner_scores = defaultdict(list)
                    region_counts = defaultdict(set)
                    scored_predictions_for_history = []
                    
                    for region_id, region_predictions in predictions_by_region.items():
                        logger.debug(f"Scoring {len(region_predictions)} predictions for region {region_id}")
                        
                        # Get SMAP ground truth file for this region/time
                        smap_path = None
                        try:
                            # Use existing SMAP API to get file path
                            from gaia.tasks.defined_tasks.soilmoisture.utils.smap_api import construct_smap_url
                            from pathlib import Path
                            
                            # Get the SMAP URL and check if file exists in cache
                            smap_url = construct_smap_url(target_time, test_mode=self.test_mode)
                            cache_dir = Path("smap_cache")
                            cache_file = cache_dir / Path(smap_url).name
                            
                            if cache_file.exists():
                                smap_path = str(cache_file)
                                logger.debug(f"Found cached SMAP file: {smap_path}")
                            else:
                                # Try to download SMAP file
                                from gaia.tasks.defined_tasks.soilmoisture.utils.smap_api import download_smap_data
                                cache_dir.mkdir(exist_ok=True)
                                
                                if await download_smap_data(smap_url, str(cache_file)):
                                    smap_path = str(cache_file)
                                    logger.debug(f"Downloaded SMAP file: {smap_path}")
                                else:
                                    logger.warning(f"Failed to download SMAP file for {target_time}")
                            
                            if not smap_path or not os.path.exists(smap_path):
                                logger.warning(f"SMAP file not available for {target_time}, skipping region {region_id}")
                                continue
                                
                        except Exception as smap_error:
                            logger.error(f"Error getting SMAP file for {target_time}: {smap_error}")
                            continue
                        
                        # Score each prediction in this region
                        for pred in region_predictions:
                            try:
                                # Validate miner is still in current metagraph
                                miner_uid = pred['miner_uid']
                                miner_hotkey = pred['miner_hotkey']
                                
                                is_valid_in_metagraph = False
                                if self.validator and hasattr(self.validator, 'metagraph') and self.validator.metagraph is not None:
                                    if miner_hotkey in self.validator.metagraph.nodes:
                                        node_in_metagraph = self.validator.metagraph.nodes[miner_hotkey]
                                        if hasattr(node_in_metagraph, 'node_id') and str(node_in_metagraph.node_id) == str(miner_uid):
                                            is_valid_in_metagraph = True
                                            logger.debug(f"Retroactive: Valid hotkey match for miner {miner_uid} ({miner_hotkey})")
                                        else:
                                            metagraph_uid = getattr(node_in_metagraph, 'node_id', 'unknown')
                                            logger.warning(f"Retroactive: UID mismatch for hotkey {miner_hotkey} - Prediction UID: {miner_uid}, Current UID: {metagraph_uid}")
                                    else:
                                        logger.warning(f"Retroactive: Hotkey {miner_hotkey} not in current metagraph")
                                else:
                                    logger.warning("Retroactive: No metagraph available for validation")
                                
                                if not is_valid_in_metagraph:
                                    # Delete invalid prediction instead of scoring it
                                    delete_query = """
                                        DELETE FROM soil_moisture_predictions 
                                        WHERE id = :prediction_id
                                    """
                                    await self.db_manager.execute(delete_query, {"prediction_id": pred['id']})
                                    logger.info(f"Deleted mismatched prediction for UID {miner_uid}, hotkey {miner_hotkey}")
                                    continue
                                
                                # Build prediction data for scoring
                                pred_data = {
                                    "bounds": pred["sentinel_bounds"],
                                    "crs": pred["sentinel_crs"],
                                    "predictions": {
                                        "miner_id": pred["miner_uid"],
                                        "miner_hotkey": pred["miner_hotkey"],
                                        "surface_sm": pred["surface_sm"],
                                        "rootzone_sm": pred["rootzone_sm"]
                                    },
                                    "target_time": target_time,
                                    "region": {"id": region_id},
                                    "miner_id": pred["miner_uid"],
                                    "miner_hotkey": pred["miner_hotkey"],
                                    "smap_file": smap_path,
                                    "smap_file_path": smap_path
                                }
                                
                                # Score the individual prediction
                                logger.debug(f"Scoring prediction for miner {miner_uid} in region {region_id}")
                                score = await self.scoring_mechanism.score(pred_data)
                                
                                if not score:
                                    logger.warning(f"Failed to score prediction for miner {miner_uid}")
                                    continue
                                
                                # Validate score structure
                                total_score = score.get("total_score", 0)
                                if not isinstance(total_score, (int, float)) or total_score < 0 or total_score > 1:
                                    logger.warning(f"Invalid score {total_score} for miner {miner_uid}")
                                    continue
                                
                                # Add to miner scores for aggregation
                                miner_scores[miner_uid].append(total_score)
                                region_counts[miner_uid].add(region_id)
                                
                                logger.debug(f"Retroactive: Scored miner {miner_uid} in region {region_id}: {total_score:.4f}")
                                
                                # Prepare for history table
                                score_metrics = score.get("metrics", {})
                                ground_truth = score.get("ground_truth", {})
                                
                                history_record = {
                                    "region_id": region_id,
                                    "miner_uid": miner_uid,
                                    "miner_hotkey": miner_hotkey,
                                    "target_time": target_time,
                                    "surface_sm_pred": pred["surface_sm"],
                                    "rootzone_sm_pred": pred["rootzone_sm"],
                                    "surface_sm_truth": ground_truth.get("surface_sm"),
                                    "rootzone_sm_truth": ground_truth.get("rootzone_sm"),
                                    "surface_rmse": score_metrics.get("surface_rmse"),
                                    "rootzone_rmse": score_metrics.get("rootzone_rmse"),
                                    "surface_structure_score": score_metrics.get("surface_ssim", 0),
                                    "rootzone_structure_score": score_metrics.get("rootzone_ssim", 0),
                                    "sentinel_bounds": pred["sentinel_bounds"],
                                    "sentinel_crs": pred["sentinel_crs"],
                                    "prediction_id": pred["id"]
                                }
                                scored_predictions_for_history.append(history_record)
                                total_scored_predictions += 1
                                
                            except Exception as pred_error:
                                logger.error(f"Error scoring prediction {pred['id']} for miner {pred['miner_uid']}: {pred_error}")
                                continue
                    
                    # Build score array from aggregated miner scores
                    scores = [float("nan")] * 256
                    for miner_uid, scores_list in miner_scores.items():
                        if scores_list:
                            avg_score = sum(scores_list) / len(scores_list)
                            region_count = len(region_counts[miner_uid])
                            try:
                                scores[int(miner_uid)] = avg_score
                                logger.debug(f"Retroactive: Final score for miner {miner_uid}: {avg_score:.4f} across {region_count} regions")
                            except (ValueError, IndexError):
                                logger.warning(f"Invalid miner UID for scoring: {miner_uid}")
                                continue
                    
                    # Create and insert score row
                    current_datetime = target_time
                    score_row = {
                        "task_name": "soil_moisture_region_global",
                        "task_id": str(current_datetime.timestamp()),
                        "score": scores,
                        "status": "completed"
                    }
                    
                    upsert_query = """
                        INSERT INTO score_table 
                        (task_name, task_id, score, status)
                        VALUES 
                        (:task_name, :task_id, :score, :status)
                        ON CONFLICT (task_name, task_id) 
                        DO UPDATE SET 
                            score = EXCLUDED.score,
                            status = EXCLUDED.status
                    """
                    await self.db_manager.execute(upsert_query, score_row)
                    
                    # Move scored predictions to history table
                    if scored_predictions_for_history:
                        history_insert_query = """
                            INSERT INTO soil_moisture_history 
                            (region_id, miner_uid, miner_hotkey, target_time, 
                             surface_sm_pred, rootzone_sm_pred, surface_sm_truth, rootzone_sm_truth,
                             surface_rmse, rootzone_rmse, surface_structure_score, rootzone_structure_score,
                             sentinel_bounds, sentinel_crs)
                            VALUES 
                            (:region_id, :miner_uid, :miner_hotkey, :target_time,
                             :surface_sm_pred, :rootzone_sm_pred, :surface_sm_truth, :rootzone_sm_truth,
                             :surface_rmse, :rootzone_rmse, :surface_structure_score, :rootzone_structure_score,
                             :sentinel_bounds, :sentinel_crs)
                        """
                        
                        # Insert all history records
                        for record in scored_predictions_for_history:
                            await self.db_manager.execute(history_insert_query, record)
                        
                        # Delete corresponding predictions from predictions table
                        prediction_ids = [record["prediction_id"] for record in scored_predictions_for_history]
                        if prediction_ids:
                            # Delete predictions one by one to ensure compatibility
                            for prediction_id in prediction_ids:
                                delete_prediction_query = """
                                    DELETE FROM soil_moisture_predictions 
                                    WHERE id = :prediction_id
                                """
                                await self.db_manager.execute(delete_prediction_query, {"prediction_id": prediction_id})
                            
                            logger.info(f"✅ Deleted {len(prediction_ids)} predictions from predictions table")
                        
                        logger.info(f"✅ Moved {len(scored_predictions_for_history)} scored predictions to history")
                    
                    # Log summary
                    non_nan_scores = [(i, s) for i, s in enumerate(scores) if not math.isnan(s)]
                    if non_nan_scores:
                        logger.info(f"✅ Created retroactive score row for {target_time}: {len(non_nan_scores)} miners, "
                                  f"scores: {min(s for _, s in non_nan_scores):.4f} - {max(s for _, s in non_nan_scores):.4f}")
                    else:
                        logger.warning(f"⚠️  No valid scores generated for {target_time}")
                    
                    successful_builds += 1
                    
                except Exception as time_error:
                    logger.error(f"❌ Failed to process target_time {target_time}: {time_error}")
                    logger.error(traceback.format_exc())
                    failed_builds += 1
                    continue
            
            logger.info(f"🎯 Retroactive scoring complete: {successful_builds} successful, {failed_builds} failed")
            logger.info(f"📊 Total predictions scored: {total_scored_predictions}")
            
            if successful_builds > 0:
                logger.info("💡 Retroactive scores have been added and predictions moved to history")
            
        except Exception as e:
            logger.error(f"Error in build_retroactive_score_rows: {e}")
            logger.error(traceback.format_exc())

    async def trigger_retroactive_scoring(self, days_back=7, force_rebuild=False):
        """
        Public method to trigger retroactive score building.
        Can be called manually or during startup.
        
        Args:
            days_back (int): How many days back to look for missing score rows
            force_rebuild (bool): Whether to rebuild existing score rows
        """
        try:
            logger.info("🚀 Triggering retroactive soil moisture score building...")
            await self.build_retroactive_score_rows(days_back=days_back, force_rebuild=force_rebuild)
            logger.info("✅ Retroactive soil moisture score building completed")
            
        except Exception as e:
            logger.error(f"Error triggering retroactive scoring: {e}")
            logger.error(traceback.format_exc())

    async def cleanup_incorrect_history_rmse_values(self, days_back=3):
        """
        Clean up history entries that have incorrect aggregate RMSE values instead of individual ones.
        This fixes the bug where all miners in a region got the same RMSE values.
        
        Args:
            days_back (int): How many days back to clean up
        """
        try:
            logger.info(f"🧹 Starting cleanup of incorrect RMSE values in history table for last {days_back} days...")
            
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
            
            # Find target times where multiple miners have identical RMSE values (indicating the bug)
            suspicious_query = """
                SELECT 
                    target_time,
                    surface_rmse,
                    rootzone_rmse,
                    COUNT(*) as miner_count
                FROM soil_moisture_history h
                WHERE h.target_time >= :cutoff_time
                GROUP BY target_time, surface_rmse, rootzone_rmse
                HAVING COUNT(*) > 5  -- More than 5 miners with identical RMSE values is suspicious
                ORDER BY target_time DESC, miner_count DESC
            """
            
            suspicious_entries = await self.db_manager.fetch_all(suspicious_query, {"cutoff_time": cutoff_time})
            
            if not suspicious_entries:
                logger.info("✅ No suspicious RMSE entries found - no cleanup needed")
                return
            
            logger.info(f"🔍 Found {len(suspicious_entries)} suspicious target times with identical RMSE values:")
            for entry in suspicious_entries:
                logger.info(f"   - {entry['target_time']}: {entry['miner_count']} miners with RMSE "
                          f"surface={entry['surface_rmse']:.4f}, rootzone={entry['rootzone_rmse']:.4f}")
            
            total_deleted = 0
            
            # For each suspicious target time, check if we can recalculate individual RMSE values
            for entry in suspicious_entries:
                target_time = entry['target_time']
                surface_rmse = entry['surface_rmse']
                rootzone_rmse = entry['rootzone_rmse']
                
                try:
                    logger.info(f"🔧 Processing cleanup for {target_time}...")
                    
                    # Get all history entries for this target time with these suspicious RMSE values
                    affected_entries_query = """
                        SELECT h.*, r.sentinel_bounds, r.sentinel_crs
                        FROM soil_moisture_history h
                        JOIN soil_moisture_regions r ON h.region_id = r.id
                        WHERE h.target_time = :target_time
                        AND h.surface_rmse = :surface_rmse
                        AND h.rootzone_rmse = :rootzone_rmse
                    """
                    
                    affected_entries = await self.db_manager.fetch_all(affected_entries_query, {
                        "target_time": target_time,
                        "surface_rmse": surface_rmse,
                        "rootzone_rmse": rootzone_rmse
                    })
                    
                    if not affected_entries:
                        continue
                    
                    logger.info(f"   Found {len(affected_entries)} affected entries")
                    
                    # Check if we have the prediction and ground truth data to recalculate
                    entries_with_data = [e for e in affected_entries 
                                       if e.get('surface_sm_pred') is not None and 
                                          e.get('rootzone_sm_pred') is not None and
                                          e.get('surface_sm_truth') is not None and
                                          e.get('rootzone_sm_truth') is not None]
                    
                    if len(entries_with_data) == 0:
                        # No data to recalculate - just delete the problematic entries
                        logger.info(f"   No prediction/truth data available for recalculation. Deleting {len(affected_entries)} entries...")
                        
                        delete_query = """
                            DELETE FROM soil_moisture_history
                            WHERE target_time = :target_time
                            AND surface_rmse = :surface_rmse  
                            AND rootzone_rmse = :rootzone_rmse
                        """
                        
                        result = await self.db_manager.execute(delete_query, {
                            "target_time": target_time,
                            "surface_rmse": surface_rmse,
                            "rootzone_rmse": rootzone_rmse
                        })
                        
                        deleted_count = getattr(result, 'rowcount', len(affected_entries))
                        total_deleted += deleted_count
                        logger.info(f"   ✅ Deleted {deleted_count} problematic entries for {target_time}")
                        
                    else:
                        logger.info(f"   Found {len(entries_with_data)} entries with prediction/truth data")
                        logger.info(f"   TODO: Implement individual RMSE recalculation (complex - requires numpy processing)")
                        # For now, just delete these too since the fix is already in place for new data
                        # In the future, we could implement individual RMSE recalculation here
                        
                        delete_query = """
                            DELETE FROM soil_moisture_history
                            WHERE target_time = :target_time
                            AND surface_rmse = :surface_rmse  
                            AND rootzone_rmse = :rootzone_rmse
                        """
                        
                        result = await self.db_manager.execute(delete_query, {
                            "target_time": target_time,
                            "surface_rmse": surface_rmse,
                            "rootzone_rmse": rootzone_rmse
                        })
                        
                        deleted_count = getattr(result, 'rowcount', len(affected_entries))
                        total_deleted += deleted_count
                        logger.info(f"   ✅ Deleted {deleted_count} entries with aggregate RMSE values for {target_time}")
                
                except Exception as e:
                    logger.error(f"❌ Error processing cleanup for {target_time}: {e}")
                    continue
            
            logger.info(f"🎯 Cleanup complete: Deleted {total_deleted} history entries with incorrect aggregate RMSE values")
            
            if total_deleted > 0:
                logger.info("💡 These entries will be properly rescored with individual RMSE values when new predictions are made")
                logger.info("🔄 Consider running retroactive scoring to rebuild score rows for these target times")
            
        except Exception as e:
            logger.error(f"Error in cleanup_incorrect_history_rmse_values: {e}")
            logger.error(traceback.format_exc())

    async def _score_predictions_threaded(self, task, temp_path):
        """
        Score miner predictions using threading for improved performance.
        
        THREADING PERFORMANCE IMPROVEMENT:
        
        This method parallelizes the scoring of individual miner predictions within each soil task.
        Instead of scoring miners sequentially (which can take minutes for 256 miners), 
        it scores them concurrently using asyncio semaphores and batching.
        
        PERFORMANCE BENEFITS:
        - Sequential: ~5-10 minutes for 256 miners
        - Threaded: ~1-3 minutes for 256 miners  
        - Maintains same accuracy and error handling
        - Yields control between batches to allow other tasks to run
        
        CONFIGURATION:
        Set environment variable: SOIL_THREADED_SCORING=true
        Or modify self.use_threaded_scoring = True in constructor
        
        PARAMETERS:
        - max_concurrent_threads: 8 (limits simultaneous scoring operations)
        - batch_size: 12 (processes miners in batches with yielding)
        - yield_time: 0.1s (brief pause between batches)
        
        Args:
            task: Task containing predictions to score
            temp_path: Path to SMAP ground truth file
            
        Returns:
            tuple: (scored_predictions, task_ground_truth)
        """
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        import threading
        
        predictions = task.get("predictions", [])
        if not predictions:
            return [], None
            
        # Count retry vs regular miners for better visibility
        retry_miners = sum(1 for pred in predictions if pred.get("retry_count", 0) > 0)
        regular_miners = len(predictions) - retry_miners
        
        if retry_miners > 0:
            logger.info(f"Threading soil scoring for {len(predictions)} miners in region {task['id']} "
                       f"({retry_miners} retries, {regular_miners} regular)")
        else:
            logger.info(f"Threading soil scoring for {len(predictions)} miners in region {task['id']}")
        
        # Configure threading parameters
        max_concurrent_threads = min(len(predictions), 8)  # Limit concurrent threads
        batch_size = 12  # Process in batches to allow yielding
        
        scored_predictions = []
        task_ground_truth = None
        
        async def score_single_miner(prediction):
            """Score a single miner's prediction - thread-safe wrapper"""
            try:
                pred_data = {
                    "bounds": task["sentinel_bounds"],
                    "crs": task["sentinel_crs"],
                    "predictions": prediction,
                    "target_time": task["target_time"],
                    "region": {"id": task["id"]},
                    "miner_id": prediction["miner_id"],
                    "miner_hotkey": prediction["miner_hotkey"],
                    "smap_file": temp_path,
                    "smap_file_path": temp_path
                }
                
                # Score the prediction
                score = await self.scoring_mechanism.score(pred_data)
                if not score:
                    return None, None
                    
                # Baseline comparison (thread-safe - read-only operations)
                baseline_score = None
                if self.validator and hasattr(self.validator, 'basemodel_evaluator'):
                    try:
                        target_time = task["target_time"]
                        if isinstance(target_time, datetime):
                            if target_time.tzinfo is not None:
                                target_time_utc = target_time.astimezone(timezone.utc)
                            else:
                                target_time_utc = target_time.replace(tzinfo=timezone.utc)
                        else:
                            target_time_utc = target_time
                            
                        task_id = str(target_time_utc.timestamp())
                        smap_file_to_use = temp_path if temp_path and os.path.exists(temp_path) else None
                        
                        self.validator.basemodel_evaluator.test_mode = self.test_mode
                        
                        baseline_score = await self.validator.basemodel_evaluator.score_soil_baseline(
                            task_id=task_id,
                            region_id=str(task["id"]),
                            ground_truth=score.get("ground_truth", {}),
                            smap_file_path=smap_file_to_use
                        )
                        
                        if baseline_score is not None:
                            miner_score = score.get("total_score", 0)
                            miner_metrics = score.get("metrics", {})
                            
                            # Apply baseline comparison logic
                            standard_epsilon = 0.005
                            excellent_rmse_threshold = 0.04
                            
                            baseline_metrics = getattr(self.validator.basemodel_evaluator.soil_scoring, '_last_baseline_metrics', {})
                            baseline_surface_rmse = baseline_metrics.get("validation_metrics", {}).get("surface_rmse")
                            baseline_rootzone_rmse = baseline_metrics.get("validation_metrics", {}).get("rootzone_rmse")
                            
                            has_excellent_performance = False
                            avg_baseline_rmse = None
                            
                            if baseline_surface_rmse is not None and baseline_rootzone_rmse is not None:
                                avg_baseline_rmse = (baseline_surface_rmse + baseline_rootzone_rmse) / 2
                                has_excellent_performance = avg_baseline_rmse <= excellent_rmse_threshold
                            
                            if has_excellent_performance and avg_baseline_rmse is not None:
                                allowed_score_range = baseline_score * 0.95
                                passes_comparison = miner_score >= allowed_score_range
                                if not passes_comparison:
                                    score["total_score"] = 0
                            else:
                                passes_comparison = miner_score > baseline_score + standard_epsilon
                                if not passes_comparison:
                                    score["total_score"] = 0
                                    
                    except Exception as e:
                        logger.error(f"Error retrieving baseline score for miner {prediction['miner_id']}: {e}")
                
                # Store the scored prediction
                prediction_copy = prediction.copy()
                prediction_copy["score"] = score
                
                return prediction_copy, score.get("ground_truth")
                
            except Exception as e:
                logger.error(f"Error scoring miner {prediction.get('miner_id', 'unknown')}: {e}")
                return None, None
        
        # Process predictions in batches to allow yielding
        for batch_start in range(0, len(predictions), batch_size):
            batch_end = min(batch_start + batch_size, len(predictions))
            batch_predictions = predictions[batch_start:batch_end]
            
            logger.debug(f"Processing batch {batch_start//batch_size + 1} ({len(batch_predictions)} miners)")
            
            # Create semaphore to limit concurrent threads
            semaphore = asyncio.Semaphore(max_concurrent_threads)
            
            async def score_with_semaphore(prediction):
                async with semaphore:
                    return await score_single_miner(prediction)
            
            # Score batch concurrently
            batch_tasks = [score_with_semaphore(pred) for pred in batch_predictions]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Process results
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Batch scoring exception: {result}")
                    continue
                    
                scored_prediction, ground_truth = result
                if scored_prediction:
                    scored_predictions.append(scored_prediction)
                    if task_ground_truth is None and ground_truth:
                        task_ground_truth = ground_truth
            
            # Yield control between batches to allow other tasks to run
            if batch_end < len(predictions):
                await asyncio.sleep(0.1)  # Brief yield - adjust as needed
                
        logger.info(f"Threaded scoring completed: {len(scored_predictions)}/{len(predictions)} miners scored successfully")
        return scored_predictions, task_ground_truth

    async def cleanup_stuck_retries(self, max_retry_limit=5, days_back=7):
        """Clean up tasks that have exceeded the retry limit and are stuck forever."""
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
            
            logger.info(f"🧹 Cleaning up stuck retry tasks (retry_count >= {max_retry_limit}) from last {days_back} days...")
            
            # Find tasks that have exceeded retry limit
            stuck_query = """
                SELECT 
                    COUNT(*) as total_stuck,
                    MIN(r.target_time) as earliest_target,
                    MAX(r.target_time) as latest_target
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                WHERE p.status = 'retry_scheduled'
                AND p.retry_count >= :max_retry_limit
                AND r.target_time >= :cutoff_time
            """
            
            stuck_summary = await self.db_manager.fetch_one(stuck_query, {
                "max_retry_limit": max_retry_limit,
                "cutoff_time": cutoff_time
            })
            
            if not stuck_summary or stuck_summary.get("total_stuck", 0) == 0:
                logger.info("✅ No stuck retry tasks found to clean up")
                return 0
            
            logger.info(f"Found {stuck_summary['total_stuck']} stuck tasks from {stuck_summary['earliest_target']} to {stuck_summary['latest_target']}")
            
            # Option 1: Delete stuck predictions and let them be regenerated
            # This is safer than trying to reset retry counts
            delete_query = """
                DELETE FROM soil_moisture_predictions p
                USING soil_moisture_regions r
                WHERE p.region_id = r.id
                AND p.status = 'retry_scheduled'
                AND p.retry_count >= :max_retry_limit
                AND r.target_time >= :cutoff_time
            """
            
            result = await self.db_manager.execute(delete_query, {
                "max_retry_limit": max_retry_limit,
                "cutoff_time": cutoff_time
            })
            
            deleted_count = getattr(result, 'rowcount', 0) if result else 0
            logger.info(f"✅ Deleted {deleted_count} stuck retry tasks - they can be regenerated in future runs")
            
            # Also reset any regions that might be stuck in 'sent_to_miners' status
            # but have no active predictions
            reset_regions_query = """
                UPDATE soil_moisture_regions r
                SET status = 'pending'
                WHERE r.status = 'sent_to_miners'
                AND r.target_time >= :cutoff_time
                AND NOT EXISTS (
                    SELECT 1 FROM soil_moisture_predictions p 
                    WHERE p.region_id = r.id 
                    AND p.status IN ('sent_to_miner', 'retry_scheduled')
                )
            """
            
            reset_result = await self.db_manager.execute(reset_regions_query, {"cutoff_time": cutoff_time})
            reset_count = getattr(reset_result, 'rowcount', 0) if reset_result else 0
            
            if reset_count > 0:
                logger.info(f"✅ Reset {reset_count} regions back to 'pending' status")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up stuck retries: {e}")
            logger.error(traceback.format_exc())
            return 0

    async def force_process_old_predictions(self, days_back=3):
        """Force processing of old sent_to_miner tasks that should have been scored by now."""
        try:
            # For soil moisture, the scoring delay is 3 days, so anything older than that should be processed
            scoring_cutoff = datetime.now(timezone.utc) - self.scoring_delay
            
            logger.info(f"🚀 Checking for old sent_to_miner tasks ready for scoring (older than {scoring_cutoff})...")
            
            # Count old tasks
            old_tasks_query = """
                SELECT 
                    COUNT(*) as total_old,
                    MIN(r.target_time) as earliest_target,
                    MAX(r.target_time) as latest_target,
                    COUNT(DISTINCT p.miner_uid) as unique_miners
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                WHERE p.status = 'sent_to_miner'
                AND r.target_time <= :scoring_cutoff
            """
            
            old_summary = await self.db_manager.fetch_one(old_tasks_query, {"scoring_cutoff": scoring_cutoff})
            
            if not old_summary or old_summary.get("total_old", 0) == 0:
                logger.info("✅ No old sent_to_miner tasks found ready for scoring")
                return 0
            
            logger.info(f"Found {old_summary['total_old']} old sent_to_miner tasks from {old_summary['unique_miners']} miners")
            logger.info(f"Target time range: {old_summary['earliest_target']} to {old_summary['latest_target']}")
            
            # Force immediate scoring attempt
            logger.info("🚀 Forcing immediate scoring of old tasks...")
            try:
                result = await self.validator_score()
                if result.get("status") == "success":
                    logger.info("✅ Successfully processed old sent_to_miner tasks")
                    return True
                else:
                    logger.warning(f"Scoring returned status: {result.get('status')}")
                    return False
            except Exception as e:
                logger.error(f"Error during forced scoring: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error forcing processing of old predictions: {e}")
            logger.error(traceback.format_exc())
            return False

    async def fix_stuck_retries_now(self):
        """
        Manual method to immediately fix stuck retries and process old tasks.
        Call this to test the retry fixes.
        """
        try:
            logger.info("🔧 MANUAL FIX: Starting comprehensive retry cleanup and processing...")
            
            # Step 1: Clean up stuck retries
            logger.info("Step 1: Cleaning up stuck retries...")
            deleted_count = await self.cleanup_stuck_retries(max_retry_limit=5, days_back=7)
            
            # Step 2: Force processing of old predictions
            logger.info("Step 2: Processing old sent_to_miner tasks...")
            await self.force_process_old_predictions()
            
            # Step 3: Force immediate retries for processing errors
            logger.info("Step 3: Forcing immediate retries for stuck tasks...")
            await self.force_immediate_retries()
            
            # Step 4: Run scoring to process everything
            logger.info("Step 4: Running comprehensive scoring...")
            for attempt in range(3):
                logger.info(f"Scoring attempt {attempt + 1}/3")
                result = await self.validator_score()
                if result.get("status") == "no_pending_tasks":
                    logger.info("✅ All retries completed successfully!")
                    break
                await asyncio.sleep(5)
            
            logger.info("🎯 Manual retry fix completed!")
            return True
            
        except Exception as e:
            logger.error(f"Error in manual retry fix: {e}")
            logger.error(traceback.format_exc())
            return False

