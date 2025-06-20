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
        description="Delay before scoring due to SMAP data latency",
    )

    validator_preprocessing: Optional["SoilValidatorPreprocessing"] = None # type: ignore # steven this is for the linter lol, just leave it here unless it's causing issues
    miner_preprocessing: Optional["SoilMinerPreprocessing"] = None
    model: Optional[SoilModel] = None
    db_manager: Any = Field(default=None)
    node_type: str = Field(default="miner")
    test_mode: bool = Field(default=False)
    use_raw_preprocessing: bool = Field(default=False)
    validator: Any = Field(default=None, description="Reference to the validator instance")

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

                    # Use UPSERT to prevent duplicates in history table
                    upsert_query = """
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
                        ON CONFLICT (region_id, miner_uid, target_time) 
                        DO UPDATE SET 
                            surface_sm_pred = EXCLUDED.surface_sm_pred,
                            rootzone_sm_pred = EXCLUDED.rootzone_sm_pred,
                            surface_sm_truth = EXCLUDED.surface_sm_truth,
                            rootzone_sm_truth = EXCLUDED.rootzone_sm_truth,
                            surface_rmse = EXCLUDED.surface_rmse,
                            rootzone_rmse = EXCLUDED.rootzone_rmse,
                            surface_structure_score = EXCLUDED.surface_structure_score,
                            rootzone_structure_score = EXCLUDED.rootzone_structure_score
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
                    logger.info(f"Processing tasks for target_time: {target_time}")
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
                                        last_error = :error_message,
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
                    
                    # Extract file path and processed data
                    temp_path = smap_data_result.get("file_path")
                    smap_processed_data = smap_data_result.get("data", {})
                    
                    if not temp_path or not os.path.exists(temp_path):
                        logger.error(f"Failed to download or process SMAP data for {target_time}")
                        # Update retry information for failed tasks
                        for task in tasks_in_time_window:
                            for prediction in task["predictions"]:
                                update_query = """
                                    UPDATE soil_moisture_predictions
                                    SET retry_count = COALESCE(retry_count, 0) + 1,
                                        next_retry_time = :next_retry_time,
                                        last_error = :error_message,
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

                    for task in tasks_in_time_window:
                        try:
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
                                            last_error = :error_message,
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
            
            # First, check for and build any missing retroactive score rows
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
                        'last_error', p.last_error,
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
                        AND p.last_error IS NOT NULL
                        AND (
                            p.last_error LIKE '%processing%' OR
                            p.last_error LIKE '%scoring%' OR
                            p.last_error LIKE '%calculate%' OR
                            p.last_error LIKE '%_FillValue%'
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
                    last_error = pred.get("last_error", "") or ""
                    
                    if pred.get("next_retry_time"):
                        scheduled_retries += 1
                    else:
                        old_pending += 1
                    
                    if last_error and any(keyword in last_error.lower() for keyword in ['processing', 'scoring', 'calculate', '_fillvalue']):
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
            
            # Force an immediate scoring attempt
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
            filter_conditions = " OR ".join([f"p.last_error ILIKE '%{error_type}%'" for error_type in error_types])
            
            force_retry_query = f"""
                UPDATE soil_moisture_predictions
                SET next_retry_time = :immediate_time,
                    status = 'retry_scheduled'
                WHERE status IN ('sent_to_miner', 'retry_scheduled')
                AND last_error IS NOT NULL
                AND ({filter_conditions})
                AND COALESCE(retry_count, 0) < 10
                RETURNING miner_uid, target_time, last_error
            """
            
            updated_tasks = await self.db_manager.fetch_all(force_retry_query, {
                "immediate_time": current_time - timedelta(seconds=1)  # Make it ready now
            })
            
            if updated_tasks:
                logger.info(f"✅ Forced immediate retry for {len(updated_tasks)} stuck tasks")
                for task in updated_tasks:
                    logger.info(f"   - Miner {task['miner_uid']} at {task['target_time']}: {task['last_error']}")
                
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
        Retroactively build score rows from soil_moisture_history table for tasks 
        that were completed but never had their scores properly aggregated.
        
        Args:
            days_back (int): How many days back to look for missing score rows
            force_rebuild (bool): Whether to rebuild existing score rows
        """
        try:
            logger.info(f"🔄 Starting retroactive score row building for last {days_back} days...")
            
            # Get cutoff time
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
            
            # Get miner mappings
            miner_query = """
                SELECT uid, hotkey FROM node_table 
                WHERE hotkey IS NOT NULL
            """
            miner_mappings = await self.db_manager.fetch_all(miner_query)
            hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}
            logger.info(f"Found {len(hotkey_to_uid)} miner mappings for retroactive scoring")
            
            # Find target times that have history entries but no score rows
            missing_scores_query = """
                SELECT DISTINCT h.target_time, COUNT(*) as history_count
                FROM soil_moisture_history h
                WHERE h.target_time >= :cutoff_time
                AND NOT EXISTS (
                    SELECT 1 FROM score_table s 
                    WHERE s.task_name = 'soil_moisture_region_global' 
                    AND s.task_id = CAST(EXTRACT(EPOCH FROM h.target_time) AS TEXT)
                )
                GROUP BY h.target_time
                ORDER BY h.target_time DESC
            """
            
            if force_rebuild:
                # If force rebuild, get all target times regardless of existing score rows
                missing_scores_query = """
                    SELECT DISTINCT h.target_time, COUNT(*) as history_count
                    FROM soil_moisture_history h
                    WHERE h.target_time >= :cutoff_time
                    GROUP BY h.target_time
                    ORDER BY h.target_time DESC
                """
            
            missing_times = await self.db_manager.fetch_all(missing_scores_query, {"cutoff_time": cutoff_time})
            
            if not missing_times:
                logger.info("✅ No missing score rows found in soil moisture history")
                return
            
            logger.info(f"🔍 Found {len(missing_times)} target times with missing score rows:")
            for time_row in missing_times:
                logger.info(f"   - {time_row['target_time']}: {time_row['history_count']} history entries")
            
            successful_builds = 0
            failed_builds = 0
            
            # Process each missing target time
            for time_row in missing_times:
                target_time = time_row['target_time']
                try:
                    logger.info(f"📊 Building retroactive score row for {target_time}...")
                    
                    # Get all history entries for this target time
                    history_query = """
                        SELECT h.*, r.sentinel_bounds, r.sentinel_crs
                        FROM soil_moisture_history h
                        JOIN soil_moisture_regions r ON h.region_id = r.id
                        WHERE h.target_time = :target_time
                        ORDER BY h.region_id, h.miner_uid
                    """
                    
                    history_entries = await self.db_manager.fetch_all(history_query, {"target_time": target_time})
                    
                    if not history_entries:
                        logger.warning(f"No history entries found for {target_time}")
                        failed_builds += 1
                        continue
                    
                    # Group by region and calculate aggregate scores per miner
                    regions_data = defaultdict(lambda: defaultdict(list))
                    
                    for entry in history_entries:
                        region_id = entry['region_id']
                        miner_uid = entry['miner_uid']
                        miner_hotkey = entry['miner_hotkey']
                        
                        # Extract RMSE values from history (SSIM values are stored but not used in final score)
                        surface_rmse = entry.get('surface_rmse', 0) or 0
                        rootzone_rmse = entry.get('rootzone_rmse', 0) or 0
                        surface_ssim = entry.get('surface_structure_score', 0) or 0
                        rootzone_ssim = entry.get('rootzone_structure_score', 0) or 0
                        
                        # CRITICAL: Apply EXACT same validation as regular scoring flow
                        # Mirror SoilScoringMechanism.validate_metrics() exactly
                        
                        # Validate metrics format and basic requirements
                        has_surface = surface_rmse is not None and surface_rmse != 0
                        has_rootzone = rootzone_rmse is not None and rootzone_rmse != 0
                        
                        if not (has_surface or has_rootzone):
                            logger.warning(f"Retroactive: No valid metrics found for miner {miner_uid}, region {region_id}")
                            continue
                        
                        # Validate surface metrics if present
                        if has_surface:
                            if not isinstance(surface_rmse, (int, float)) or math.isnan(surface_rmse) or math.isinf(surface_rmse):
                                logger.warning(f"Retroactive: Invalid surface_rmse format: {surface_rmse}, skipping entry")
                                continue
                            if surface_rmse < 0:
                                logger.warning(f"Retroactive: Surface RMSE must be positive, got {surface_rmse}, skipping entry")
                                continue
                            if surface_ssim is not None and surface_ssim != 0:
                                if not isinstance(surface_ssim, (int, float)) or math.isnan(surface_ssim) or math.isinf(surface_ssim):
                                    logger.warning(f"Retroactive: Invalid surface_ssim format: {surface_ssim}, setting to 0")
                                    surface_ssim = 0
                                elif not -1 <= surface_ssim <= 1:
                                    logger.warning(f"Retroactive: Surface SSIM {surface_ssim} outside valid range [-1,1], setting to 0")
                                    surface_ssim = 0
                        
                        # Validate rootzone metrics if present
                        if has_rootzone:
                            if not isinstance(rootzone_rmse, (int, float)) or math.isnan(rootzone_rmse) or math.isinf(rootzone_rmse):
                                logger.warning(f"Retroactive: Invalid rootzone_rmse format: {rootzone_rmse}, skipping entry")
                                continue
                            if rootzone_rmse < 0:
                                logger.warning(f"Retroactive: Rootzone RMSE must be positive, got {rootzone_rmse}, skipping entry")
                                continue
                            if rootzone_ssim is not None and rootzone_ssim != 0:
                                if not isinstance(rootzone_ssim, (int, float)) or math.isnan(rootzone_ssim) or math.isinf(rootzone_ssim):
                                    logger.warning(f"Retroactive: Invalid rootzone_ssim format: {rootzone_ssim}, setting to 0")
                                    rootzone_ssim = 0
                                elif not -1 <= rootzone_ssim <= 1:
                                    logger.warning(f"Retroactive: Rootzone SSIM {rootzone_ssim} outside valid range [-1,1], setting to 0")
                                    rootzone_ssim = 0
                        
                        # Apply sigmoid transformation exactly as in SoilScoringMechanism
                        # sigmoid_rmse(rmse) = 1 / (1 + exp(alpha * (rmse - beta)))
                        # Where alpha=3, beta=0.15 (SoilScoringMechanism defaults)
                        alpha = 3.0
                        beta = 0.15
                        
                        surface_score = 1.0 / (1.0 + math.exp(alpha * (surface_rmse - beta)))
                        rootzone_score = 1.0 / (1.0 + math.exp(alpha * (rootzone_rmse - beta)))
                        
                        # Validate transformed scores
                        if math.isnan(surface_score) or math.isinf(surface_score):
                            logger.warning(f"Retroactive: Invalid surface score after sigmoid: {surface_score}, skipping entry")
                            continue
                            
                        if math.isnan(rootzone_score) or math.isinf(rootzone_score):
                            logger.warning(f"Retroactive: Invalid rootzone score after sigmoid: {rootzone_score}, skipping entry")
                            continue
                        
                        # Final score calculation exactly as in SoilScoringMechanism.compute_final_score()
                        composite_score = 0.6 * surface_score + 0.4 * rootzone_score
                        
                        # Validate final score exactly as regular flow does
                        if math.isnan(composite_score) or math.isinf(composite_score):
                            logger.warning(f"Retroactive: Invalid composite score: {composite_score}, skipping entry")
                            continue
                        
                        if not 0 <= composite_score <= 1:
                            logger.warning(f"Retroactive: Final score {composite_score} outside valid range [0,1], skipping entry")
                            continue
                        
                        logger.debug(f"Retroactive score calculation for miner {miner_uid}: "
                                   f"surface_rmse={surface_rmse:.4f}→{surface_score:.4f}, "
                                   f"rootzone_rmse={rootzone_rmse:.4f}→{rootzone_score:.4f}, "
                                   f"final={composite_score:.4f}")
                        
                        regions_data[region_id][miner_uid] = {
                            'score': composite_score,
                            'hotkey': miner_hotkey,
                            'surface_rmse': surface_rmse,
                            'rootzone_rmse': rootzone_rmse,
                            'surface_ssim': surface_ssim,
                            'rootzone_ssim': rootzone_ssim
                        }
                    
                    # Build the score array
                    scores = [float("nan")] * 256
                    miner_scores = defaultdict(list)
                    region_counts = defaultdict(set)
                    
                    for region_id, miners in regions_data.items():
                        for miner_uid, data in miners.items():
                            # CRITICAL: Validate miner hotkey matches current metagraph
                            # If hotkey doesn't match, we should DELETE the old prediction, not score it for the new miner
                            is_valid_in_metagraph = False
                            should_delete_old_prediction = False
                            
                            if self.validator and hasattr(self.validator, 'metagraph') and self.validator.metagraph is not None:
                                miner_hotkey = data['hotkey']
                                if miner_hotkey in self.validator.metagraph.nodes:
                                    node_in_metagraph = self.validator.metagraph.nodes[miner_hotkey]
                                    if hasattr(node_in_metagraph, 'node_id') and str(node_in_metagraph.node_id) == str(miner_uid):
                                        is_valid_in_metagraph = True
                                        logger.debug(f"Retroactive: Valid hotkey match for miner {miner_uid} ({miner_hotkey})")
                                    else:
                                        metagraph_uid = getattr(node_in_metagraph, 'node_id', 'unknown')
                                        logger.warning(f"Retroactive: UID mismatch for hotkey {miner_hotkey} - History UID: {miner_uid}, Current UID: {metagraph_uid}. DELETING old prediction.")
                                        should_delete_old_prediction = True
                                else:
                                    logger.warning(f"Retroactive: Hotkey {miner_hotkey} not in current metagraph. DELETING old prediction for UID {miner_uid}.")
                                    should_delete_old_prediction = True
                            else:
                                # If no metagraph available, be conservative and skip retroactive scoring
                                logger.warning(f"Retroactive: No metagraph available for validation. Skipping retroactive scoring for miner {miner_uid}.")
                                continue
                            
                            # If hotkey doesn't match current metagraph, delete the old prediction from history
                            if should_delete_old_prediction:
                                try:
                                    delete_query = """
                                        DELETE FROM soil_moisture_history 
                                        WHERE target_time = :target_time 
                                        AND miner_uid = :miner_uid 
                                        AND miner_hotkey = :miner_hotkey
                                        AND region_id = :region_id
                                    """
                                    await self.db_manager.execute(delete_query, {
                                        "target_time": target_time,
                                        "miner_uid": miner_uid,
                                        "miner_hotkey": miner_hotkey,
                                        "region_id": region_id
                                    })
                                    logger.info(f"Retroactive: Deleted mismatched prediction for UID {miner_uid}, hotkey {miner_hotkey}, region {region_id}")
                                except Exception as delete_error:
                                    logger.error(f"Error deleting mismatched prediction: {delete_error}")
                                continue  # Skip scoring for this miner
                            
                            # Only score if hotkey validation passed
                            if is_valid_in_metagraph:
                                score_value = data['score']
                                miner_scores[miner_uid].append(score_value)
                                region_counts[miner_uid].add(region_id)
                                logger.debug(f"Retroactive: Added score {score_value:.4f} for miner {miner_uid} in region {region_id}")
                    
                    # Calculate final scores for each miner
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
                    
                    # Create score row
                    current_datetime = target_time
                    score_row = {
                        "task_name": "soil_moisture_region_global",
                        "task_id": str(current_datetime.timestamp()),
                        "score": scores,
                        "status": "completed"
                    }
                    
                    # Insert or update the score row (always use UPSERT to avoid conflicts)
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
                    
                    if force_rebuild:
                        logger.info(f"✅ Updated retroactive score row for {target_time}")
                    else:
                        logger.info(f"✅ Created/updated retroactive score row for {target_time}")
                    
                    # Log summary
                    non_nan_scores = [(i, s) for i, s in enumerate(scores) if not math.isnan(s)]
                    if non_nan_scores:
                        logger.info(f"   📈 Built score row with {len(non_nan_scores)} miners, "
                                  f"scores: {min(s for _, s in non_nan_scores):.4f} - {max(s for _, s in non_nan_scores):.4f}")
                    
                    successful_builds += 1
                    
                except Exception as e:
                    logger.error(f"❌ Failed to build retroactive score row for {target_time}: {e}")
                    logger.error(traceback.format_exc())
                    failed_builds += 1
                    continue
            
            logger.info(f"🎯 Retroactive score building complete: {successful_builds} successful, {failed_builds} failed")
            
            if successful_builds > 0:
                logger.info("💡 Retroactive scores have been added to the score_table and should be reflected in the next weight calculation")
            
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

