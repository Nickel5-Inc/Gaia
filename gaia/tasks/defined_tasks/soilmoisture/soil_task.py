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

        # Run startup consolidated retry check and missing score row check
        logger.info("🚀 Running startup consolidated retry check...")
        try:
            await self._consolidated_retry_check()
        except Exception as e:
            logger.error(f"Error during startup retry check: {e}")
        
        # Check for missing score rows on startup
        logger.info("🔍 Checking for missing score rows on startup...")
        try:
            await self.check_and_build_missing_score_rows(days_back=7)
        except Exception as e:
            logger.error(f"Error during startup missing score row check: {e}")
        
        # Diagnose stuck predictions on startup
        logger.info("🔍 Diagnosing stuck predictions on startup...")
        try:
            diagnosis = await self.diagnose_stuck_predictions(days_back=7)
            if diagnosis.get("past_scoring_delay", 0) > 0:
                logger.warning(f"🚨 Found {diagnosis['past_scoring_delay']} predictions past scoring delay!")
                logger.warning("   Consider running manual scoring or cleanup to resolve this")
        except Exception as e:
            logger.error(f"Error during startup stuck predictions diagnosis: {e}")
        
        while True:
            try:
                await validator.update_task_status('soil', 'active')
                current_time = datetime.now(timezone.utc)

                # SMART RETRY CHECK SCHEDULING: Adaptive timing based on actual needs
                last_retry_check = getattr(self, '_last_retry_check', datetime.min.replace(tzinfo=timezone.utc))
                last_retry_result = getattr(self, '_last_retry_result', {})
                
                # Determine appropriate retry check interval based on last result
                if last_retry_result.get("status") == "no_retry_tasks":
                    # No eligible tasks - check less frequently
                    next_earliest_retry = last_retry_result.get("next_earliest_retry")
                    if next_earliest_retry:
                        try:
                            next_retry_time = self.parse_datetime_with_microseconds(next_earliest_retry)
                            
                            if next_retry_time:
                                time_until_next = (next_retry_time - current_time).total_seconds() / 60
                                
                                if time_until_next > 60:  # Next retry > 1 hour away
                                    retry_check_interval = 15 * 60  # Check every 15 minutes
                                elif time_until_next > 30:  # Next retry 30-60 minutes away  
                                    retry_check_interval = 10 * 60  # Check every 10 minutes
                                elif time_until_next > 10:  # Next retry 10-30 minutes away
                                    retry_check_interval = 5 * 60   # Check every 5 minutes
                                else:  # Next retry < 10 minutes away
                                    retry_check_interval = 60       # Check every minute
                            else:
                                retry_check_interval = 10 * 60  # Default: 10 minutes
                        except Exception as e:
                            logger.debug(f"Failed to parse next_earliest_retry '{next_earliest_retry}': {e}")
                            retry_check_interval = 10 * 60  # Default: 10 minutes
                    else:
                        retry_check_interval = 15 * 60  # No next retry known: 15 minutes
                else:
                    # Had eligible tasks or errors - check more frequently
                    retry_check_interval = 5 * 60  # 5 minutes
                
                # Also run on regular 5-minute intervals regardless
                should_check_retries = (
                    current_time.minute % 5 == 0 or  # Regular 5-minute intervals
                    last_retry_check < current_time - timedelta(seconds=retry_check_interval)
                )
                
                if should_check_retries:
                    await validator.update_task_status('soil', 'processing', 'retry_check')
                    logger.info(f"🔄 Running consolidated retry check at {current_time} (interval: {retry_check_interval/60:.1f}min)")
                    result = await self._consolidated_retry_check()
                    self._last_retry_check = current_time
                    self._last_retry_result = result or {}
                    
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
        """
        Move completed task data to history tables with enhanced error handling and validation.
        
        CRITICAL: This method MUST complete successfully before any cleanup to prevent data loss.
        """
        try:
            region_id = region["id"]
            target_time = region["target_time"]
            
            logger.info(f"📦 Moving {len(predictions)} predictions to history for region {region_id}")
            logger.info(f"Final scores for region {region_id}:")
            logger.info(f"Surface RMSE: {scores['metrics'].get('surface_rmse'):.4f}")
            logger.info(f"Surface SSIM: {scores['metrics'].get('surface_ssim', 0):.4f}")
            logger.info(f"Rootzone RMSE: {scores['metrics'].get('rootzone_rmse'):.4f}")
            logger.info(f"Rootzone SSIM: {scores['metrics'].get('rootzone_ssim', 0):.4f}")
            logger.info(f"Total Score: {scores.get('total_score', 0):.4f}")

            successfully_moved = []
            failed_moves = []

            for prediction in predictions:
                try:
                    miner_id = prediction["miner_id"]
                    miner_hotkey = prediction.get("miner_hotkey", "")
                    
                    # CRITICAL FIX: Use individual miner scores instead of aggregate scores
                    prediction_score = prediction.get("score", {})
                    prediction_metrics = prediction_score.get("metrics", {})
                    
                    # Validate that we have individual metrics
                    if not prediction_metrics:
                        logger.warning(f"No individual metrics found for miner {miner_id}, using aggregate scores")
                        prediction_metrics = scores.get("metrics", {})
                    
                    # Ensure we have required data
                    if not prediction.get("surface_sm") or not prediction.get("rootzone_sm"):
                        logger.error(f"Missing prediction data for miner {miner_id}, skipping history move")
                        failed_moves.append(miner_id)
                        continue
                    
                    params = {
                        "region_id": region_id,
                        "miner_uid": miner_id,
                        "miner_hotkey": miner_hotkey,
                        "target_time": target_time,
                        "surface_sm_pred": prediction["surface_sm"],
                        "rootzone_sm_pred": prediction["rootzone_sm"],
                        "surface_sm_truth": ground_truth.get("surface_sm") if ground_truth else None,
                        "rootzone_sm_truth": ground_truth.get("rootzone_sm") if ground_truth else None,
                        "surface_rmse": prediction_score.get("metrics", {}).get("surface_rmse"),
                        "rootzone_rmse": prediction_score.get("metrics", {}).get("rootzone_rmse"),
                        "surface_structure_score": prediction_score.get("metrics", {}).get("surface_ssim", 0),
                        "rootzone_structure_score": prediction_score.get("metrics", {}).get("rootzone_ssim", 0),
                        "sentinel_bounds": region.get("sentinel_bounds"),
                        "sentinel_crs": region.get("sentinel_crs"),
                    }
                    
                    # Log individual metrics for debugging
                    logger.debug(f"Storing metrics for miner {miner_id}: "
                               f"surface_rmse={prediction_metrics.get('surface_rmse', 'None')}, "
                               f"rootzone_rmse={prediction_metrics.get('rootzone_rmse', 'None')}")

                    # ENHANCED: Use UPSERT with better error handling
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
                    
                    # Verify the history insert worked
                    verify_history_query = """
                        SELECT COUNT(*) as count FROM soil_moisture_history 
                        WHERE region_id = :region_id AND miner_uid = :miner_uid AND target_time = :target_time
                    """
                    verify_result = await self.db_manager.fetch_one(verify_history_query, {
                        "region_id": region_id,
                        "miner_uid": miner_id,
                        "target_time": target_time
                    })
                    
                    if not verify_result or verify_result.get("count", 0) == 0:
                        logger.error(f"CRITICAL: History insert failed for miner {miner_id}!")
                        failed_moves.append(miner_id)
                        continue

                    # ONLY update prediction status AFTER successful history insert
                    update_query = """
                        UPDATE soil_moisture_predictions 
                        SET status = 'scored'
                        WHERE region_id = :region_id 
                        AND miner_uid = :miner_uid
                        AND status IN ('sent_to_miner', 'retry_scheduled')
                    """
                    await self.db_manager.execute(update_query, {
                        "region_id": region_id,
                        "miner_uid": miner_id
                    })
                    
                    successfully_moved.append(miner_id)
                    logger.debug(f"✅ Successfully moved miner {miner_id} to history")

                except Exception as e:
                    logger.error(f"❌ Error processing prediction for miner {prediction.get('miner_id', 'unknown')}: {str(e)}")
                    logger.error(traceback.format_exc())
                    failed_moves.append(prediction.get("miner_id", "unknown"))
                    continue

            # Report results
            logger.info(f"📊 History move results for region {region_id}:")
            logger.info(f"   ✅ Successfully moved: {len(successfully_moved)} predictions")
            if failed_moves:
                logger.error(f"   ❌ Failed to move: {len(failed_moves)} predictions (miners: {failed_moves})")
                logger.error(f"   CRITICAL: Some predictions not moved to history - cleanup will be skipped!")
                return False
            
            # ONLY cleanup if ALL predictions were successfully moved to history
            logger.info(f"🧹 All predictions successfully moved to history, proceeding with cleanup...")
            await self.cleanup_predictions(
                bounds=region["sentinel_bounds"],
                target_time=target_time,
                miner_uid=None  # Clean up all miners for this region
            )

            logger.info(f"✅ Completed move to history for region {region_id}: {len(successfully_moved)} predictions")
            return True

        except Exception as e:
            logger.error(f"❌ CRITICAL: Failed to move task to history for region {region.get('id', 'unknown')}: {str(e)}")
            logger.error(traceback.format_exc())
            logger.error(f"   Cleanup will be SKIPPED to prevent data loss!")
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
                    
                    # PRE-CHECK: Verify SMAP data is expected to be available before attempting download
                    availability_status = await self.is_smap_data_expected_available(target_time)
                    
                    if not availability_status["available"]:
                        logger.info(f"⏳ SMAP data not expected to be available yet for {target_time}")
                        logger.info(f"   Reason: {availability_status['reason']}")
                        logger.info(f"   Expected available: {availability_status['expected_available_time']}")
                        logger.info(f"   Will check again: {availability_status['next_check_time']}")
                        
                        # Schedule tasks to check again later WITHOUT incrementing retry count
                        for task in tasks_in_time_window:
                            for prediction in task["predictions"]:
                                update_query = """
                                    UPDATE soil_moisture_predictions
                                    SET next_retry_time = :next_retry_time,
                                        retry_error_message = :error_message,
                                        status = 'retry_scheduled'
                                    WHERE region_id = :region_id
                                    AND miner_uid = :miner_uid
                                """
                                params = {
                                    "region_id": task["id"],
                                    "miner_uid": prediction["miner_id"],
                                    "next_retry_time": availability_status["next_check_time"],
                                    "error_message": f"SMAP data not expected until {availability_status['expected_available_time']} - pre-check avoided retry count increment"
                                }
                                await self.db_manager.execute(update_query, params)
                        
                        logger.info(f"⏳ Scheduled {len(tasks_in_time_window)} regions for {target_time} to retry at {availability_status['next_check_time']} (NO retry count increment)")
                        continue
                    
                    logger.info(f"✅ SMAP data expected to be available for {target_time}: {availability_status['reason']}")
                    
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
                        # Update retry information for failed tasks - NOW increment retry count for actual failures
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
                                    "next_retry_time": datetime.now(timezone.utc) + timedelta(hours=2),  # Actual failure - standard retry
                                    "error_message": "Failed to download SMAP data (actual failure after pre-check passed)"
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
                            # Data not available yet - but this should be rare now with pre-check
                            retry_hours = 1  # Retry in 1 hour for 404 errors
                            detailed_message = f"SMAP data not available yet (HTTP 404) for {target_time} - unexpected after pre-check"
                            logger.warning(f"⚠️  {detailed_message} - will retry in {retry_hours} hour(s)")
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
                        else:
                            # Other errors - general retry
                            retry_hours = 2
                            detailed_message = f"SMAP error ({error_type}: {error_message}) for {target_time}"
                            logger.error(f"❌ {detailed_message} - will retry in {retry_hours} hour(s)")
                        
                        # Update retry information for tasks with actual errors - increment retry count
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

            # IMPROVED RETRY DETECTION: Check database directly for retry tasks
            has_retry_tasks = False
            if recent_tasks:
                # First check the recent_tasks for retry indicators
                for task in recent_tasks:
                    for prediction in task.get("predictions", []):
                        if prediction.get("retry_count", 0) > 0 or prediction.get("next_retry_time") is not None:
                            has_retry_tasks = True
                            break
                    if has_retry_tasks:
                        break
                
                # Also check database for any retry tasks at this target_time
                if not has_retry_tasks:
                    retry_check_query = """
                        SELECT COUNT(*) as retry_count 
                        FROM soil_moisture_predictions p
                        JOIN soil_moisture_regions r ON p.region_id = r.id
                        WHERE r.target_time = :target_time
                        AND (p.retry_count > 0 OR p.next_retry_time IS NOT NULL)
                    """
                    retry_result = await self.db_manager.fetch_one(retry_check_query, {"target_time": target_time})
                    has_retry_tasks = retry_result and retry_result.get("retry_count", 0) > 0
            
            # Handle existing scores for retry scenarios
            check_existing_query = """
                SELECT COUNT(*) as count FROM score_table 
                WHERE task_name = 'soil_moisture_region_global' 
                AND task_id = :task_id
            """
            task_id = str(current_datetime.timestamp())
            existing_result = await self.db_manager.fetch_one(check_existing_query, {"task_id": task_id})
            has_existing_scores = existing_result and existing_result.get("count", 0) > 0
            
            if has_existing_scores and not has_retry_tasks:
                logger.warning(f"Score row already exists for target_time {target_time} and no retry tasks detected. Skipping.")
                return []
            elif has_existing_scores and has_retry_tasks:
                logger.info(f"Score row exists for target_time {target_time} but retry tasks detected. Will update existing scores.")

            if recent_tasks:
                logger.info(f"Processing {len(recent_tasks)} recent tasks across all regions{' (RETRY SCENARIO)' if has_retry_tasks else ''}")
                
                processed_region_ids = set()
                miner_scores = defaultdict(list)
                region_counts = defaultdict(set)
                
                for task in recent_tasks:
                    region_id = task.get("id", "unknown")
                    
                    if region_id in processed_region_ids:
                        logger.warning(f"Skipping duplicate region {region_id}")
                        continue
                    
                    processed_region_ids.add(region_id)
                    
                    logger.info(f"Processing scores for region {region_id}")
                    
                    for prediction in task.get("predictions", []):
                        miner_uid_from_prediction = prediction.get("miner_id")
                        miner_hotkey_from_prediction = prediction.get("miner_hotkey")

                        if miner_uid_from_prediction is None or miner_hotkey_from_prediction is None:
                            logger.warning(f"Skipping prediction due to missing UID or Hotkey: UID {miner_uid_from_prediction}, Hotkey {miner_hotkey_from_prediction}")
                            continue
                        
                        # Validate miner is in current metagraph
                        is_valid_in_metagraph = False
                        if self.validator and hasattr(self.validator, 'metagraph') and self.validator.metagraph is not None:
                            if miner_hotkey_from_prediction in self.validator.metagraph.nodes:
                                node_in_metagraph = self.validator.metagraph.nodes[miner_hotkey_from_prediction]
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
                        
                        # Extract individual miner's score (not aggregate task score)
                        prediction_score = prediction.get("score", {})
                        if isinstance(prediction_score.get("total_score"), (int, float)):
                            score_value = float(prediction_score["total_score"])
                            miner_scores[miner_uid_from_prediction].append(score_value)
                            region_counts[miner_uid_from_prediction].add(region_id)
                            logger.debug(f"Added score {score_value:.4f} for miner_id {miner_uid_from_prediction} in region {region_id}")

                # Build final score array
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
            logger.info(f"Built score row with {len(non_nan_scores)} non-NaN scores for target time {target_time}{' (RETRY)' if has_retry_tasks else ''}")
            if non_nan_scores:
                logger.info(f"Score summary - min: {min(s for _, s in non_nan_scores):.4f}, max: {max(s for _, s in non_nan_scores):.4f}")
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

    def parse_datetime_with_microseconds(self, time_str):
        """Helper function to parse datetime strings with variable microsecond precision."""
        if not time_str:
            return None
        try:
            if isinstance(time_str, str):
                # Fix microseconds formatting for Python datetime parsing
                fixed_time_str = time_str.replace('Z', '+00:00')
                # Handle microseconds with less than 6 digits
                if '.' in fixed_time_str and '+' in fixed_time_str:
                    dt_part, tz_part = fixed_time_str.rsplit('+', 1)
                    if '.' in dt_part:
                        base_part, micro_part = dt_part.rsplit('.', 1)
                        # Pad microseconds to 6 digits
                        micro_part = micro_part.ljust(6, '0')[:6]
                        fixed_time_str = f"{base_part}.{micro_part}+{tz_part}"
                return datetime.fromisoformat(fixed_time_str)
            else:
                return time_str
        except Exception as e:
            logger.debug(f"Failed to parse datetime '{time_str}': {e}")
            return None

    async def _consolidated_retry_check(self):
        """
        CONSOLIDATED RETRY MECHANISM - Single method for all retry scenarios.
        
        This replaces multiple retry mechanisms (_startup_retry_check, force_immediate_retries, etc.)
        with one unified approach that:
        1. Finds all tasks eligible for retry (startup, scheduled, stuck, etc.)
        2. Processes them using the EXACT same scoring logic as regular tasks
        3. Ensures score rows are built properly for retry scenarios
        """
        
        try:
            current_time = datetime.now(timezone.utc)
            logger.info("🔄 Running consolidated retry check...")
            
            # STEP 0: Log current database state for debugging
            debug_status_query = """
                SELECT 
                    p.status,
                    COUNT(*) as count,
                    MIN(r.target_time) as earliest_time,
                    MAX(r.target_time) as latest_time,
                    AVG(COALESCE(p.retry_count, 0)) as avg_retry_count,
                    MAX(COALESCE(p.retry_count, 0)) as max_retry_count
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                WHERE p.status IN ('sent_to_miner', 'retry_scheduled')
                GROUP BY p.status
            """
            debug_status = await self.db_manager.fetch_all(debug_status_query)
            
            logger.info(f"📊 Current database state before retry check:")
            for row in debug_status:
                logger.info(f"   - {row['status']}: {row['count']} tasks, retry_count avg={row['avg_retry_count']:.1f}/max={row['max_retry_count']}, time range: {row['earliest_time']} to {row['latest_time']}")
            
            if not debug_status:
                logger.info("   - No tasks in sent_to_miner or retry_scheduled status")
            
            # STEP 1: Clean up tasks that are permanently stuck (>10 retries or very old)
            try:
                cutoff_time = current_time - timedelta(days=7)
                cleanup_query = """
                    DELETE FROM soil_moisture_predictions p
                    USING soil_moisture_regions r
                    WHERE p.region_id = r.id
                    AND (
                        p.retry_count >= 10
                        OR (p.status = 'retry_scheduled' AND r.target_time < :cutoff_time)
                    )
                """
                cleanup_result = await self.db_manager.execute(cleanup_query, {"cutoff_time": cutoff_time})
                cleanup_count = getattr(cleanup_result, 'rowcount', 0) if cleanup_result else 0
                if cleanup_count > 0:
                    logger.info(f"🧹 Cleaned up {cleanup_count} permanently stuck tasks")
                else:
                    logger.info("🧹 No permanently stuck tasks to clean up")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
            
            # STEP 1.5: Periodic missing score row check (every hour)
            last_score_check = getattr(self, '_last_score_row_check', datetime.min.replace(tzinfo=timezone.utc))
            score_check_interval = 60 * 60  # 1 hour
            
            if last_score_check < current_time - timedelta(seconds=score_check_interval):
                try:
                    logger.info("🔍 Running periodic missing score row check...")
                    score_result = await self.check_and_build_missing_score_rows(days_back=3)
                    self._last_score_row_check = current_time
                    
                    if score_result.get("missing_score_rows", 0) > 0:
                        logger.info(f"✅ Periodic check built {score_result.get('successful_builds', 0)} missing score rows")
                    else:
                        logger.debug("✅ Periodic check found no missing score rows")
                except Exception as e:
                    logger.error(f"Error during periodic score row check: {e}")
                    self._last_score_row_check = current_time  # Don't retry immediately
            
            # STEP 2: Find ALL tasks eligible for retry processing
            # This includes:
            # - Scheduled retries that are ready
            # - Old sent_to_miner tasks past scoring delay
            # - Tasks with processing/scoring errors
            # - Startup recovery tasks
            
            scoring_cutoff = current_time - self.scoring_delay  # 3 days for soil moisture
            immediate_cutoff = current_time - timedelta(minutes=30)  # Old pending tasks
            
            logger.info(f"🔍 Searching for eligible retry tasks with cutoffs:")
            logger.info(f"   - scoring_cutoff: {scoring_cutoff} (3 days ago)")
            logger.info(f"   - immediate_cutoff: {immediate_cutoff} (30 minutes ago)")
            logger.info(f"   - current_time: {current_time}")
            
            # First, let's get a detailed breakdown of what tasks exist
            detailed_breakdown_query = """
                SELECT 
                    CASE 
                        WHEN p.status = 'retry_scheduled' AND p.next_retry_time IS NOT NULL AND p.next_retry_time <= :current_time THEN 'scheduled_retries_ready'
                        WHEN p.status = 'sent_to_miner' AND r.target_time <= :scoring_cutoff AND p.next_retry_time IS NULL THEN 'regular_tasks_past_scoring_delay'
                        WHEN p.status = 'sent_to_miner' AND r.target_time <= :immediate_cutoff AND COALESCE(p.retry_count, 0) = 0 THEN 'startup_recovery_tasks'
                        WHEN COALESCE(p.retry_count, 0) >= 10 THEN 'max_retries_reached'
                        WHEN p.status = 'sent_to_miner' THEN 'sent_to_miner_not_ready'
                        WHEN p.status = 'retry_scheduled' AND p.next_retry_time > :current_time THEN 'retry_scheduled_future'
                        ELSE 'other'
                    END as category,
                    COUNT(*) as count,
                    MIN(r.target_time) as earliest_target,
                    MAX(r.target_time) as latest_target,
                    MIN(COALESCE(p.retry_count, 0)) as min_retry_count,
                    MAX(COALESCE(p.retry_count, 0)) as max_retry_count
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                WHERE p.status IN ('sent_to_miner', 'retry_scheduled')
                GROUP BY 1
                ORDER BY count DESC
            """
            
            breakdown_result = await self.db_manager.fetch_all(detailed_breakdown_query, {
                "current_time": current_time,
                "scoring_cutoff": scoring_cutoff,
                "immediate_cutoff": immediate_cutoff
            })
            
            logger.info(f"📋 Detailed task breakdown:")
            for row in breakdown_result:
                logger.info(f"   - {row['category']}: {row['count']} tasks, "
                          f"retry_count {row['min_retry_count']}-{row['max_retry_count']}, "
                          f"target_time {row['earliest_target']} to {row['latest_target']}")
            
            eligible_retry_query = """
                SELECT 
                    r.*,
                    json_agg(json_build_object(
                        'miner_id', p.miner_uid,
                        'miner_hotkey', p.miner_hotkey,
                        'retry_count', COALESCE(p.retry_count, 0),
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
                AND COALESCE(p.retry_count, 0) < 10
                AND (
                    -- Scheduled retries that are ready
                    (
                        p.status = 'retry_scheduled'
                        AND p.next_retry_time IS NOT NULL 
                        AND p.next_retry_time <= :current_time
                    )
                    OR 
                    -- Regular tasks past scoring delay
                    (
                        p.status = 'sent_to_miner'
                        AND r.target_time <= :scoring_cutoff
                        AND p.next_retry_time IS NULL
                    )
                    OR
                    -- Old pending tasks from previous sessions (startup recovery)
                    (
                        p.status = 'sent_to_miner'
                        AND r.target_time <= :immediate_cutoff
                        AND COALESCE(p.retry_count, 0) = 0
                    )
                )
                GROUP BY r.id, r.target_time, r.sentinel_bounds, r.sentinel_crs, r.status
                ORDER BY r.target_time ASC
            """
            
            eligible_tasks = await self.db_manager.fetch_all(eligible_retry_query, {
                "current_time": current_time,
                "scoring_cutoff": scoring_cutoff,
                "immediate_cutoff": immediate_cutoff
            })
            
            if not eligible_tasks:
                logger.info("✅ No tasks eligible for retry processing")
                
                # Log why tasks might not be eligible
                logger.info("🔍 Checking why no tasks are eligible...")
                
                # Check for tasks that are close but not quite eligible
                near_miss_query = """
                    SELECT 
                        'retry_scheduled_but_future' as reason,
                        COUNT(*) as count,
                        MIN(p.next_retry_time)::text as earliest_time,
                        MAX(p.next_retry_time)::text as latest_time
                    FROM soil_moisture_predictions p
                    JOIN soil_moisture_regions r ON p.region_id = r.id
                    WHERE p.status = 'retry_scheduled'
                    AND p.next_retry_time IS NOT NULL 
                    AND p.next_retry_time > :current_time
                    AND COALESCE(p.retry_count, 0) < 10
                    
                    UNION ALL
                    
                    SELECT 
                        'sent_to_miner_but_recent' as reason,
                        COUNT(*) as count,
                        MIN(r.target_time)::text as earliest_time,
                        MAX(r.target_time)::text as latest_time
                    FROM soil_moisture_predictions p
                    JOIN soil_moisture_regions r ON p.region_id = r.id
                    WHERE p.status = 'sent_to_miner'
                    AND r.target_time > :scoring_cutoff
                    AND COALESCE(p.retry_count, 0) < 10
                    
                    UNION ALL
                    
                    SELECT 
                        'max_retries_reached' as reason,
                        COUNT(*) as count,
                        MIN(COALESCE(p.retry_count, 0))::text as earliest_time,
                        MAX(COALESCE(p.retry_count, 0))::text as latest_time
                    FROM soil_moisture_predictions p
                    JOIN soil_moisture_regions r ON p.region_id = r.id
                    WHERE p.status IN ('sent_to_miner', 'retry_scheduled')
                    AND COALESCE(p.retry_count, 0) >= 10
                """
                
                near_miss_result = await self.db_manager.fetch_all(near_miss_query, {
                    "current_time": current_time,
                    "scoring_cutoff": scoring_cutoff
                })
                
                next_earliest_retry = None
                
                for row in near_miss_result:
                    if row['count'] > 0:
                        if row['reason'] == 'retry_scheduled_but_future':
                            earliest_time_str = row['earliest_time']
                            if earliest_time_str:
                                next_earliest_retry = earliest_time_str  # Store for smart scheduling
                                try:
                                    next_retry = self.parse_datetime_with_microseconds(earliest_time_str)
                                    if next_retry:
                                        time_until_retry = (next_retry - current_time).total_seconds() / 60
                                        logger.info(f"   - {row['count']} tasks scheduled for future retry (next in {time_until_retry:.1f} minutes)")
                                    else:
                                        logger.info(f"   - {row['count']} tasks scheduled for future retry (earliest: {earliest_time_str})")
                                except Exception:
                                    logger.info(f"   - {row['count']} tasks scheduled for future retry (earliest: {earliest_time_str})")
                            else:
                                logger.info(f"   - {row['count']} tasks scheduled for future retry")
                        elif row['reason'] == 'sent_to_miner_but_recent':
                            logger.info(f"   - {row['count']} sent_to_miner tasks that are too recent (not past scoring delay)")
                            logger.info(f"     Time range: {row['earliest_time']} to {row['latest_time']}")
                        elif row['reason'] == 'max_retries_reached':
                            logger.info(f"   - {row['count']} tasks that have reached max retry limit")
                            logger.info(f"     Retry count range: {row['earliest_time']} to {row['latest_time']}")
                
                return {"status": "no_retry_tasks", "next_earliest_retry": next_earliest_retry}
            
            # STEP 3: Categorize and log retry types
            scheduled_retries = 0
            overdue_scoring = 0
            startup_recovery = 0
            processing_errors = 0
            
            task_details = []
            
            for task in eligible_tasks:
                for pred in task["predictions"]:
                    retry_count = pred.get("retry_count", 0)
                    next_retry_time = pred.get("next_retry_time")
                    error_message = pred.get("retry_error_message", "") or ""
                    
                    task_detail = {
                        "task_id": task["id"],
                        "target_time": task["target_time"],
                        "miner_id": pred["miner_id"],
                        "retry_count": retry_count,
                        "next_retry_time": next_retry_time,
                        "error_message": error_message[:50] + "..." if len(error_message) > 50 else error_message
                    }
                    
                    # Parse next_retry_time if it's a string
                    parsed_retry_time = self.parse_datetime_with_microseconds(next_retry_time)
                    
                    # Categorize task and ensure every task gets a category
                    if parsed_retry_time and parsed_retry_time <= current_time:
                        scheduled_retries += 1
                        task_detail["category"] = "scheduled_retry"
                    elif retry_count == 0 and task["target_time"] <= immediate_cutoff:
                        startup_recovery += 1
                        task_detail["category"] = "startup_recovery"
                    elif task["target_time"] <= scoring_cutoff:
                        overdue_scoring += 1
                        task_detail["category"] = "overdue_scoring"
                    else:
                        # Default category for tasks that don't match other criteria
                        task_detail["category"] = "other"
                    
                    if any(keyword in error_message.lower() for keyword in ['processing', 'scoring', 'calculate', '_fillvalue']):
                        processing_errors += 1
                        task_detail["has_processing_error"] = True
                    
                    task_details.append(task_detail)
            
            logger.info(f"📊 Found {len(eligible_tasks)} regions with retry-eligible tasks:")
            logger.info(f"   - {scheduled_retries} scheduled retries ready")
            logger.info(f"   - {overdue_scoring} overdue for scoring")
            logger.info(f"   - {startup_recovery} startup recovery tasks")
            logger.info(f"   - {processing_errors} with processing/scoring errors")
            
            # Log first few task details for debugging
            logger.info(f"🔍 Sample eligible tasks (showing first 5):")
            for i, detail in enumerate(task_details[:5]):
                logger.info(f"   {i+1}. Region {detail['task_id']}, Target: {detail['target_time']}, "
                          f"Miner: {detail['miner_id']}, Category: {detail['category']}, "
                          f"Retry: {detail['retry_count']}, Error: {detail.get('error_message', 'None')}")
            
            if len(task_details) > 5:
                logger.info(f"   ... and {len(task_details) - 5} more tasks")
            
            # STEP 4: Mark retry tasks for immediate processing
            # Set next_retry_time to past so they get picked up by regular validator_score()
            immediate_retry_time = current_time - timedelta(seconds=1)
            
            logger.info(f"🔄 Updating {len(task_details)} tasks for immediate retry processing...")
            
            update_retry_query = """
                UPDATE soil_moisture_predictions p
                SET next_retry_time = :immediate_time,
                    status = 'retry_scheduled',
                    retry_count = COALESCE(retry_count, 0) + CASE 
                        WHEN retry_count IS NULL OR retry_count = 0 THEN 1 
                        ELSE 0 
                    END
                FROM soil_moisture_regions r
                WHERE p.region_id = r.id
                AND p.status IN ('sent_to_miner', 'retry_scheduled')
                AND COALESCE(p.retry_count, 0) < 10
                AND (
                    (p.status = 'retry_scheduled' AND p.next_retry_time IS NOT NULL AND p.next_retry_time <= :current_time)
                    OR (p.status = 'sent_to_miner' AND r.target_time <= :scoring_cutoff AND p.next_retry_time IS NULL)
                    OR (p.status = 'sent_to_miner' AND r.target_time <= :immediate_cutoff AND COALESCE(p.retry_count, 0) = 0)
                )
            """
            
            update_result = await self.db_manager.execute(update_retry_query, {
                "immediate_time": immediate_retry_time,
                "current_time": current_time,
                "scoring_cutoff": scoring_cutoff,
                "immediate_cutoff": immediate_cutoff
            })
            
            updated_count = getattr(update_result, 'rowcount', 0) if update_result else 0
            logger.info(f"✅ Updated {updated_count} predictions for immediate retry")
            
            if updated_count != len(task_details):
                logger.warning(f"⚠️ Expected to update {len(task_details)} tasks but actually updated {updated_count}")
            
            # STEP 5: Use REGULAR validator_score() method for retry processing
            # This ensures 100% identical scoring logic - no deviations!
            logger.info("🚀 Processing retries using REGULAR scoring logic...")
            
            # Check how many tasks are now pending before scoring
            pending_check_query = """
                SELECT COUNT(*) as count 
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                WHERE p.status IN ('sent_to_miner', 'retry_scheduled')
                AND (
                    (p.status = 'retry_scheduled' AND p.next_retry_time IS NOT NULL AND p.next_retry_time <= :current_time)
                    OR (p.status = 'sent_to_miner' AND r.target_time <= :scoring_cutoff)
                )
            """
            
            pending_result = await self.db_manager.fetch_one(pending_check_query, {
                "current_time": current_time,
                "scoring_cutoff": scoring_cutoff
            })
            pending_count = pending_result.get("count", 0) if pending_result else 0
            
            logger.info(f"📊 {pending_count} tasks should now be pending for validator_score()")
            
            # The validator_score() method will:
            # 1. Find these retry tasks via get_pending_tasks()
            # 2. Score them using identical logic as regular tasks
            # 3. Move them to history via move_task_to_history()
            # 4. Build score rows via build_score_row() with retry detection
            
            result = await self.validator_score()
            
            logger.info(f"🔍 validator_score() returned: {result}")
            
            if result.get("status") == "success":
                logger.info("✅ Consolidated retry processing completed successfully")
                return {"status": "success", "retry_tasks_processed": len(eligible_tasks)}
            elif result.get("status") == "no_pending_tasks":
                logger.info("✅ All retry tasks were processed and completed")
                return {"status": "success", "retry_tasks_processed": len(eligible_tasks)}
            else:
                logger.warning(f"⚠️ Retry processing returned status: {result.get('status')}")
                return result
                
        except Exception as e:
            logger.error(f"Error in consolidated retry check: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": str(e)}

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
                            smap_url_info = construct_smap_url(target_time, test_mode=self.test_mode)
                            smap_url = smap_url_info[0] if isinstance(smap_url_info, tuple) else smap_url_info
                            cache_dir = Path("smap_cache")
                            cache_file = cache_dir / Path(smap_url).name
                            
                            if cache_file.exists():
                                smap_path = str(cache_file)
                                logger.debug(f"Found cached SMAP file: {smap_path}")
                            else:
                                # Try to download SMAP file
                                from gaia.tasks.defined_tasks.soilmoisture.utils.smap_api import download_smap_data
                                cache_dir.mkdir(exist_ok=True)
                                
                                download_result = await download_smap_data(smap_url_info, str(cache_file))
                                if download_result.get("success", False):
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

    # DEPRECATED METHODS - Replaced by _consolidated_retry_check()
    # These methods are kept for backward compatibility but should not be used
    
    async def _startup_retry_check(self):
        """DEPRECATED: Use _consolidated_retry_check() instead."""
        logger.warning("⚠️ _startup_retry_check() is deprecated. Using _consolidated_retry_check() instead.")
        return await self._consolidated_retry_check()
    
    async def force_immediate_retries(self, error_types=None):
        """DEPRECATED: Use _consolidated_retry_check() instead."""
        logger.warning("⚠️ force_immediate_retries() is deprecated. Using _consolidated_retry_check() instead.")
        return await self._consolidated_retry_check()
    
    async def fix_stuck_retries_now(self):
        """DEPRECATED: Use _consolidated_retry_check() instead."""
        logger.warning("⚠️ fix_stuck_retries_now() is deprecated. Using _consolidated_retry_check() instead.")
        return await self._consolidated_retry_check()
    
    async def force_process_old_predictions(self, days_back=3):
        """DEPRECATED: Use _consolidated_retry_check() instead."""
        logger.warning("⚠️ force_process_old_predictions() is deprecated. Using _consolidated_retry_check() instead.")
        return await self._consolidated_retry_check()
    
    async def cleanup_stuck_retries(self, max_retry_limit=5, days_back=7):
        """DEPRECATED: Cleanup is now handled within _consolidated_retry_check()."""
        logger.warning("⚠️ cleanup_stuck_retries() is deprecated. Cleanup is handled within _consolidated_retry_check().")
        return 0

    async def check_and_build_missing_score_rows(self, days_back=7):
        """
        Check for missing score rows by comparing soil_moisture_history with score_table.
        
        This method:
        1. Finds target_times in soil_moisture_history that have completed predictions
        2. Checks which of those target_times are missing from score_table
        3. Retroactively builds score rows for missing target_times
        4. Ensures complete scoring data for weight calculations
        
        Args:
            days_back (int): How many days back to check for missing score rows
        """
        try:
            logger.info(f"🔍 Checking for missing soil moisture score rows in last {days_back} days...")
            
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
            
            # Find all target_times in history that should have score rows
            history_targets_query = """
                SELECT 
                    target_time,
                    COUNT(DISTINCT miner_uid) as miner_count,
                    COUNT(*) as total_predictions,
                    MIN(surface_rmse) as min_surface_rmse,
                    MAX(surface_rmse) as max_surface_rmse,
                    AVG(surface_rmse) as avg_surface_rmse
                FROM soil_moisture_history h
                WHERE h.target_time >= :cutoff_time
                AND h.surface_rmse IS NOT NULL
                AND h.rootzone_rmse IS NOT NULL
                GROUP BY target_time
                HAVING COUNT(DISTINCT miner_uid) >= 5  -- Only process if we have enough miners
                ORDER BY target_time DESC
            """
            
            history_targets = await self.db_manager.fetch_all(history_targets_query, {"cutoff_time": cutoff_time})
            
            if not history_targets:
                logger.info("✅ No target times found in soil moisture history for score row checking")
                return {"status": "no_history_data", "missing_score_rows": 0}
            
            logger.info(f"📊 Found {len(history_targets)} target times in soil moisture history")
            
            # Check which target_times are missing from score_table
            missing_score_rows = []
            existing_score_rows = 0
            
            for target in history_targets:
                target_time = target['target_time']
                task_id = str(target_time.timestamp())
                
                # Check if score row exists
                score_check_query = """
                    SELECT COUNT(*) as count FROM score_table 
                    WHERE task_name = 'soil_moisture_region_global' 
                    AND task_id = :task_id
                """
                
                score_result = await self.db_manager.fetch_one(score_check_query, {"task_id": task_id})
                has_score_row = score_result and score_result.get("count", 0) > 0
                
                if not has_score_row:
                    missing_score_rows.append({
                        "target_time": target_time,
                        "miner_count": target["miner_count"],
                        "total_predictions": target["total_predictions"],
                        "avg_surface_rmse": target["avg_surface_rmse"]
                    })
                    logger.debug(f"Missing score row for {target_time}: {target['miner_count']} miners, avg RMSE: {target['avg_surface_rmse']:.4f}")
                else:
                    existing_score_rows += 1
            
            logger.info(f"📋 Score row analysis:")
            logger.info(f"   - {existing_score_rows} target times already have score rows")
            logger.info(f"   - {len(missing_score_rows)} target times missing score rows")
            
            if not missing_score_rows:
                logger.info("✅ All target times already have score rows")
                return {"status": "complete", "missing_score_rows": 0, "existing_score_rows": existing_score_rows}
            
            # Build missing score rows
            logger.info(f"🔨 Building {len(missing_score_rows)} missing score rows...")
            successful_builds = 0
            failed_builds = 0
            
            for missing in missing_score_rows:
                try:
                    target_time = missing["target_time"]
                    logger.info(f"🔨 Building score row for {target_time} ({missing['miner_count']} miners)")
                    
                    # Get all history records for this target_time
                    history_records_query = """
                        SELECT h.*, r.sentinel_bounds, r.sentinel_crs
                        FROM soil_moisture_history h
                        LEFT JOIN soil_moisture_regions r ON h.region_id = r.id
                        WHERE h.target_time = :target_time
                        AND h.surface_rmse IS NOT NULL
                        AND h.rootzone_rmse IS NOT NULL
                        ORDER BY h.miner_uid, h.region_id
                    """
                    
                    history_records = await self.db_manager.fetch_all(history_records_query, {"target_time": target_time})
                    
                    if not history_records:
                        logger.warning(f"No history records found for {target_time}")
                        failed_builds += 1
                        continue
                    
                    # Get miner mappings for validation
                    miner_query = """
                        SELECT uid, hotkey FROM node_table 
                        WHERE hotkey IS NOT NULL
                    """
                    miner_mappings = await self.db_manager.fetch_all(miner_query)
                    hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}
                    
                    # Calculate scores from history data
                    scores = [float("nan")] * 256
                    miner_scores = defaultdict(list)
                    region_counts = defaultdict(set)
                    
                    for record in history_records:
                        miner_uid = record["miner_uid"]
                        miner_hotkey = record["miner_hotkey"]
                        
                        # Validate miner is still in current metagraph (if available)
                        is_valid_in_metagraph = True  # Default to valid if no metagraph
                        if self.validator and hasattr(self.validator, 'metagraph') and self.validator.metagraph is not None:
                            if miner_hotkey in self.validator.metagraph.nodes:
                                node_in_metagraph = self.validator.metagraph.nodes[miner_hotkey]
                                if hasattr(node_in_metagraph, 'node_id') and str(node_in_metagraph.node_id) == str(miner_uid):
                                    is_valid_in_metagraph = True
                                else:
                                    is_valid_in_metagraph = False
                                    logger.debug(f"Skipping miner {miner_uid} - UID mismatch in current metagraph")
                            else:
                                is_valid_in_metagraph = False
                                logger.debug(f"Skipping miner {miner_uid} - not found in current metagraph")
                        
                        if not is_valid_in_metagraph:
                            continue
                        
                        # Calculate score from RMSE values (using inverse relationship)
                        surface_rmse = record.get("surface_rmse")
                        rootzone_rmse = record.get("rootzone_rmse")
                        
                        if surface_rmse is not None and rootzone_rmse is not None:
                            # Simple scoring: convert RMSE to score (lower RMSE = higher score)
                            # Using similar logic to the scoring mechanism
                            avg_rmse = (surface_rmse + rootzone_rmse) / 2
                            
                            # Cap RMSE for reasonable scoring (adjust these thresholds as needed)
                            max_rmse = 100.0  # Very poor performance
                            min_rmse = 0.01   # Excellent performance
                            
                            # Normalize and invert (lower RMSE = higher score)
                            normalized_rmse = min(max(avg_rmse, min_rmse), max_rmse)
                            score = max(0, 1.0 - (normalized_rmse / max_rmse))
                            
                            miner_scores[miner_uid].append(score)
                            region_counts[miner_uid].add(record.get("region_id"))
                            
                            logger.debug(f"Miner {miner_uid}: RMSE {avg_rmse:.4f} -> Score {score:.4f}")
                    
                    # Build final score array
                    valid_scores = 0
                    for miner_uid, scores_list in miner_scores.items():
                        if scores_list:
                            avg_score = sum(scores_list) / len(scores_list)
                            region_count = len(region_counts[miner_uid])
                            try:
                                scores[int(miner_uid)] = avg_score
                                valid_scores += 1
                                logger.debug(f"Final score for miner {miner_uid}: {avg_score:.4f} across {region_count} regions")
                            except (ValueError, IndexError):
                                logger.warning(f"Invalid miner UID for scoring: {miner_uid}")
                                continue
                    
                    if valid_scores == 0:
                        logger.warning(f"No valid scores generated for {target_time}")
                        failed_builds += 1
                        continue
                    
                    # Create and insert score row
                    current_datetime = target_time
                    score_row = {
                        "task_name": "soil_moisture_region_global",
                        "task_id": str(current_datetime.timestamp()),
                        "score": scores,
                        "status": "completed"
                    }
                    
                    insert_query = """
                        INSERT INTO score_table 
                        (task_name, task_id, score, status)
                        VALUES 
                        (:task_name, :task_id, :score, :status)
                        ON CONFLICT (task_name, task_id) 
                        DO UPDATE SET 
                            score = EXCLUDED.score,
                            status = EXCLUDED.status
                    """
                    
                    await self.db_manager.execute(insert_query, score_row)
                    
                    logger.info(f"✅ Built score row for {target_time}: {valid_scores} miners scored")
                    successful_builds += 1
                    
                except Exception as e:
                    logger.error(f"❌ Failed to build score row for {missing.get('target_time')}: {e}")
                    failed_builds += 1
                    continue
            
            logger.info(f"🎯 Missing score row building complete:")
            logger.info(f"   - {successful_builds} score rows built successfully")
            logger.info(f"   - {failed_builds} score rows failed to build")
            logger.info(f"   - {existing_score_rows} score rows already existed")
            
            return {
                "status": "completed",
                "missing_score_rows": len(missing_score_rows),
                "successful_builds": successful_builds,
                "failed_builds": failed_builds,
                "existing_score_rows": existing_score_rows
            }
            
        except Exception as e:
            logger.error(f"Error in check_and_build_missing_score_rows: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": str(e)}

    async def trigger_missing_score_row_check(self, days_back=7):
        """
        Manual method to trigger missing score row check and building.
        Useful for debugging or manual maintenance.
        
        Args:
            days_back (int): How many days back to check for missing score rows
        """
        try:
            logger.info("🔧 MANUAL TRIGGER: Starting missing score row check...")
            result = await self.check_and_build_missing_score_rows(days_back=days_back)
            logger.info("✅ Manual missing score row check completed")
            return result
            
        except Exception as e:
            logger.error(f"Error in manual missing score row check: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": str(e)}

    async def diagnose_stuck_predictions(self, days_back=7):
        """
        Diagnose predictions that should have been scored and moved to history but are still in predictions table.
        
        This helps identify:
        1. Predictions that were scored but not moved to history
        2. Predictions stuck in various statuses
        3. Potential data flow issues
        
        Args:
            days_back (int): How many days back to analyze
        """
        try:
            logger.info(f"🔍 Diagnosing stuck soil moisture predictions in last {days_back} days...")
            
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
            scoring_cutoff = datetime.now(timezone.utc) - self.scoring_delay  # 3 days ago
            
            # Get comprehensive breakdown of predictions table
            predictions_breakdown_query = """
                SELECT 
                    p.status,
                    CASE 
                        WHEN r.target_time <= :scoring_cutoff THEN 'past_scoring_delay'
                        WHEN r.target_time > :scoring_cutoff THEN 'within_scoring_delay'
                        ELSE 'unknown'
                    END as scoring_eligibility,
                    COUNT(*) as count,
                    COUNT(DISTINCT r.target_time) as unique_target_times,
                    COUNT(DISTINCT p.miner_uid) as unique_miners,
                    MIN(r.target_time) as earliest_target,
                    MAX(r.target_time) as latest_target,
                    AVG(COALESCE(p.retry_count, 0)) as avg_retry_count
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                WHERE r.target_time >= :cutoff_time
                GROUP BY p.status, scoring_eligibility
                ORDER BY count DESC
            """
            
            predictions_breakdown = await self.db_manager.fetch_all(predictions_breakdown_query, {
                "cutoff_time": cutoff_time,
                "scoring_cutoff": scoring_cutoff
            })
            
            logger.info(f"📊 Predictions table breakdown (last {days_back} days):")
            total_predictions = 0
            past_scoring_delay_count = 0
            
            for row in predictions_breakdown:
                total_predictions += row['count']
                if row['scoring_eligibility'] == 'past_scoring_delay':
                    past_scoring_delay_count += row['count']
                
                logger.info(f"   - {row['status']} ({row['scoring_eligibility']}): {row['count']} predictions")
                logger.info(f"     Target times: {row['unique_target_times']}, Miners: {row['unique_miners']}")
                logger.info(f"     Time range: {row['earliest_target']} to {row['latest_target']}")
                logger.info(f"     Avg retry count: {row['avg_retry_count']:.1f}")
            
            # Compare with history table
            history_summary_query = """
                SELECT 
                    COUNT(*) as total_history_records,
                    COUNT(DISTINCT target_time) as unique_target_times,
                    COUNT(DISTINCT miner_uid) as unique_miners,
                    MIN(target_time) as earliest_target,
                    MAX(target_time) as latest_target
                FROM soil_moisture_history h
                WHERE h.target_time >= :cutoff_time
            """
            
            history_summary = await self.db_manager.fetch_one(history_summary_query, {"cutoff_time": cutoff_time})
            
            logger.info(f"📋 History table summary (last {days_back} days):")
            logger.info(f"   - Total records: {history_summary.get('total_history_records', 0)}")
            logger.info(f"   - Unique target times: {history_summary.get('unique_target_times', 0)}")
            logger.info(f"   - Unique miners: {history_summary.get('unique_miners', 0)}")
            logger.info(f"   - Time range: {history_summary.get('earliest_target')} to {history_summary.get('latest_target')}")
            
            # Find predictions that should have been moved to history
            stuck_predictions_query = """
                SELECT 
                    r.target_time,
                    COUNT(*) as prediction_count,
                    COUNT(DISTINCT p.miner_uid) as miner_count,
                    BOOL_OR(EXISTS(
                        SELECT 1 FROM soil_moisture_history h 
                        WHERE h.target_time = r.target_time 
                        AND h.miner_uid = p.miner_uid
                    )) as has_any_history,
                    array_agg(DISTINCT p.status) as statuses
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                WHERE r.target_time <= :scoring_cutoff
                AND r.target_time >= :cutoff_time
                GROUP BY r.target_time
                HAVING COUNT(*) > 0
                ORDER BY r.target_time DESC
                LIMIT 10
            """
            
            stuck_predictions = await self.db_manager.fetch_all(stuck_predictions_query, {
                "scoring_cutoff": scoring_cutoff,
                "cutoff_time": cutoff_time
            })
            
            if stuck_predictions:
                logger.warning(f"⚠️  Found {len(stuck_predictions)} target times with predictions past scoring delay:")
                for row in stuck_predictions:
                    logger.warning(f"   - {row['target_time']}: {row['prediction_count']} predictions from {row['miner_count']} miners")
                    logger.warning(f"     Statuses: {row['statuses']}, Has history: {row['has_any_history']}")
            else:
                logger.info("✅ No predictions found past scoring delay")
            
            # Summary
            logger.info(f"🎯 Diagnosis summary:")
            logger.info(f"   - Total predictions in table: {total_predictions}")
            logger.info(f"   - Predictions past scoring delay: {past_scoring_delay_count}")
            logger.info(f"   - Total records in history: {history_summary.get('total_history_records', 0)}")
            
            if past_scoring_delay_count > 0:
                logger.warning(f"🚨 {past_scoring_delay_count} predictions are past scoring delay but still in predictions table!")
                logger.warning(f"   This suggests scoring or move-to-history issues")
            
            return {
                "status": "completed",
                "total_predictions": total_predictions,
                "past_scoring_delay": past_scoring_delay_count,
                "total_history_records": history_summary.get('total_history_records', 0),
                "stuck_target_times": len(stuck_predictions) if stuck_predictions else 0
            }
            
        except Exception as e:
            logger.error(f"Error in diagnose_stuck_predictions: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": str(e)}

    async def force_score_stuck_predictions(self, days_back=7, max_regions=10):
        """
        Force scoring of predictions that are stuck past the scoring delay.
        
        This method:
        1. Finds predictions past scoring delay that should have been scored
        2. Forces them through the regular scoring pipeline
        3. Moves successfully scored predictions to history
        4. Helps resolve stuck prediction backlogs
        
        Args:
            days_back (int): How many days back to look for stuck predictions
            max_regions (int): Maximum number of regions to process at once (to prevent overload)
        """
        try:
            logger.info(f"🚀 Force scoring stuck predictions from last {days_back} days...")
            
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
            scoring_cutoff = datetime.now(timezone.utc) - self.scoring_delay  # 3 days ago
            
            # Find stuck predictions past scoring delay
            stuck_regions_query = """
                SELECT 
                    r.*,
                    json_agg(json_build_object(
                        'miner_id', p.miner_uid,
                        'miner_hotkey', p.miner_hotkey,
                        'retry_count', COALESCE(p.retry_count, 0),
                        'status', p.status,
                        'surface_sm', p.surface_sm,
                        'rootzone_sm', p.rootzone_sm,
                        'uncertainty_surface', p.uncertainty_surface,
                        'uncertainty_rootzone', p.uncertainty_rootzone
                    )) as predictions
                FROM soil_moisture_regions r
                JOIN soil_moisture_predictions p ON p.region_id = r.id
                WHERE r.target_time <= :scoring_cutoff
                AND r.target_time >= :cutoff_time
                AND p.status IN ('sent_to_miner', 'retry_scheduled', 'completed')
                AND NOT EXISTS (
                    SELECT 1 FROM soil_moisture_history h 
                    WHERE h.region_id = r.id 
                    AND h.target_time = r.target_time 
                    AND h.miner_uid = p.miner_uid
                )
                GROUP BY r.id, r.target_time, r.sentinel_bounds, r.sentinel_crs, r.status
                ORDER BY r.target_time ASC
                LIMIT :max_regions
            """
            
            stuck_regions = await self.db_manager.fetch_all(stuck_regions_query, {
                "scoring_cutoff": scoring_cutoff,
                "cutoff_time": cutoff_time,
                "max_regions": max_regions
            })
            
            if not stuck_regions:
                logger.info("✅ No stuck predictions found past scoring delay")
                return {"status": "no_stuck_predictions", "processed": 0}
            
            logger.info(f"🔍 Found {len(stuck_regions)} regions with stuck predictions past scoring delay")
            for region in stuck_regions:
                pred_count = len(region['predictions']) if region['predictions'] else 0
                logger.info(f"   - Region {region['id']} ({region['target_time']}): {pred_count} stuck predictions")
            
            # Process each stuck region using regular scoring logic
            processed_regions = 0
            successful_scores = 0
            failed_scores = 0
            
            # Group by target_time for efficient SMAP data usage
            regions_by_time = defaultdict(list)
            for region in stuck_regions:
                regions_by_time[region['target_time']].append(region)
            
            for target_time, time_regions in regions_by_time.items():
                logger.info(f"🌍 Force scoring {len(time_regions)} stuck regions for {target_time}")
                
                # Get SMAP data for this target time
                temp_path = None
                try:
                    # Use the same SMAP logic as regular scoring
                    regions_for_smap = []
                    for region in time_regions:
                        regions_for_smap.append({
                            "bounds": region["sentinel_bounds"],
                            "crs": region["sentinel_crs"]
                        })
                    
                    from gaia.tasks.defined_tasks.soilmoisture.utils.smap_api import get_smap_data_multi_region
                    smap_data_result = await get_smap_data_multi_region(target_time, regions_for_smap)
                    
                    if not smap_data_result or not smap_data_result.get("success", True):
                        logger.error(f"Failed to get SMAP data for {target_time}, skipping time period")
                        continue
                    
                    temp_path = smap_data_result.get("file_path")
                    if not temp_path or not os.path.exists(temp_path):
                        logger.error(f"SMAP file missing for {target_time}, skipping time period")
                        continue
                    
                    # Score each region
                    for region in time_regions:
                        try:
                            logger.info(f"🎯 Force scoring region {region['id']} with {len(region['predictions'])} predictions")
                            
                            # Use threaded scoring if enabled
                            if self.use_threaded_scoring:
                                scored_predictions, task_ground_truth = await self._score_predictions_threaded(region, temp_path)
                            else:
                                # Sequential scoring
                                scored_predictions = []
                                task_ground_truth = None
                                
                                for prediction in region["predictions"]:
                                    pred_data = {
                                        "bounds": region["sentinel_bounds"],
                                        "crs": region["sentinel_crs"],
                                        "predictions": prediction,
                                        "target_time": target_time,
                                        "region": {"id": region["id"]},
                                        "miner_id": prediction["miner_id"],
                                        "miner_hotkey": prediction["miner_hotkey"],
                                        "smap_file": temp_path,
                                        "smap_file_path": temp_path
                                    }
                                    
                                    score = await self.scoring_mechanism.score(pred_data)
                                    if score:
                                        # Apply baseline comparison if available
                                        if self.validator and hasattr(self.validator, 'basemodel_evaluator'):
                                            try:
                                                target_time_utc = target_time if isinstance(target_time, datetime) else datetime.fromisoformat(str(target_time))
                                                task_id = str(target_time_utc.timestamp())
                                                
                                                baseline_score = await self.validator.basemodel_evaluator.score_soil_baseline(
                                                    task_id=task_id,
                                                    region_id=str(region["id"]),
                                                    ground_truth=score.get("ground_truth", {}),
                                                    smap_file_path=temp_path
                                                )
                                                
                                                if baseline_score is not None:
                                                    miner_score = score.get("total_score", 0)
                                                    standard_epsilon = 0.005
                                                    
                                                    if not (miner_score > baseline_score + standard_epsilon):
                                                        score["total_score"] = 0
                                            except Exception as e:
                                                logger.error(f"Error applying baseline for miner {prediction['miner_id']}: {e}")
                                        
                                        prediction["score"] = score
                                        scored_predictions.append(prediction)
                                        if task_ground_truth is None:
                                            task_ground_truth = score.get("ground_truth")
                            
                            # Move to history if we have scored predictions
                            if scored_predictions:
                                # Use first scored prediction's score for task score
                                task_score = scored_predictions[0]["score"]
                                
                                success = await self.move_task_to_history(
                                    region=region,
                                    predictions=scored_predictions,
                                    ground_truth=task_ground_truth,
                                    scores=task_score
                                )
                                
                                if success:
                                    successful_scores += len(scored_predictions)
                                    logger.info(f"✅ Successfully force scored and moved {len(scored_predictions)} predictions for region {region['id']}")
                                else:
                                    failed_scores += len(region['predictions'])
                                    logger.error(f"❌ Failed to move predictions to history for region {region['id']}")
                            else:
                                failed_scores += len(region['predictions'])
                                logger.warning(f"⚠️ No successful scores for region {region['id']}")
                            
                            processed_regions += 1
                            
                        except Exception as region_error:
                            logger.error(f"❌ Error force scoring region {region['id']}: {region_error}")
                            logger.error(traceback.format_exc())
                            failed_scores += len(region['predictions'])
                            continue
                
                finally:
                    # Clean up SMAP temp file
                    if temp_path and os.path.exists(temp_path):
                        try:
                            os.unlink(temp_path)
                            logger.debug(f"Removed temp SMAP file: {temp_path}")
                        except Exception as e:
                            logger.error(f"Failed to remove temp file {temp_path}: {e}")
            
            logger.info(f"🎯 Force scoring complete:")
            logger.info(f"   - Processed regions: {processed_regions}")
            logger.info(f"   - Successful scores: {successful_scores}")
            logger.info(f"   - Failed scores: {failed_scores}")
            
            if successful_scores > 0:
                # Try to build score rows for newly scored data
                logger.info("🔨 Building score rows for newly scored predictions...")
                try:
                    await self.check_and_build_missing_score_rows(days_back=1)
                except Exception as e:
                    logger.error(f"Error building score rows after force scoring: {e}")
            
            return {
                "status": "completed",
                "processed_regions": processed_regions,
                "successful_scores": successful_scores,
                "failed_scores": failed_scores
            }
            
        except Exception as e:
            logger.error(f"Error in force_score_stuck_predictions: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": str(e)}

    async def is_smap_data_expected_available(self, target_time: datetime) -> Dict[str, Any]:
        """
        Check if SMAP data is expected to be available for a given target time.
        
        Args:
            target_time: The target time for which SMAP data is needed
            
        Returns:
            Dict with keys:
            - available: bool - True if data is expected to be available
            - reason: str - Explanation of the availability status
            - next_check_time: datetime - When to check again if not available
            - expected_available_time: datetime - When data is expected to become available
        """
        current_time = datetime.now(timezone.utc)
        
        # SMAP data typically becomes available 3-5 days after the observation time
        # Use conservative estimate of 5 days plus 6 hours buffer for processing/upload time
        smap_availability_delay = timedelta(days=5, hours=6)
        expected_available_time = target_time + smap_availability_delay
        
        # In test mode, use much shorter delay for faster testing
        if self.test_mode:
            smap_availability_delay = timedelta(hours=1)  # 1 hour delay in test mode
            expected_available_time = target_time + smap_availability_delay
        
        # Check if enough time has passed for data to be available
        time_since_target = current_time - target_time
        is_available = time_since_target >= smap_availability_delay
        
        if is_available:
            return {
                "available": True,
                "reason": f"Sufficient time ({time_since_target.days} days, {time_since_target.seconds//3600} hours) has passed since target time",
                "next_check_time": None,
                "expected_available_time": expected_available_time
            }
        else:
            # Data not expected yet - schedule next check for when it should be available
            time_until_available = expected_available_time - current_time
            
            # Check more frequently as we approach the expected availability time
            if time_until_available <= timedelta(hours=6):
                next_check_delay = timedelta(hours=1)  # Check hourly in final 6 hours
            elif time_until_available <= timedelta(days=1):
                next_check_delay = timedelta(hours=4)  # Check every 4 hours in final day
            elif time_until_available <= timedelta(days=2):
                next_check_delay = timedelta(hours=12)  # Check twice daily in final 2 days
            else:
                next_check_delay = timedelta(days=1)   # Check daily when far from availability
            
            next_check_time = current_time + next_check_delay
            
            return {
                "available": False,
                "reason": f"SMAP data not expected for {time_until_available.days} days, {time_until_available.seconds//3600} hours (needs {smap_availability_delay.days} days delay)",
                "next_check_time": next_check_time,
                "expected_available_time": expected_available_time
            }

