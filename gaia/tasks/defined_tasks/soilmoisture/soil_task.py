from gaia.tasks.base.task import Task
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
import gc
import traceback
import base64
import json
import asyncio
import tempfile
import math
import glob
from collections import defaultdict
import psutil
import shutil
from gaia.tasks.defined_tasks.soilmoisture.utils.soil_apis import get_data_dir

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

    validator_preprocessing: Optional["SoilValidatorPreprocessing"] = None # type: ignore
    miner_preprocessing: Optional["SoilMinerPreprocessing"] = None # type: ignore
    model: Optional[SoilModel] = None
    db_manager: Any = Field(default=None)
    node_type: str = Field(default="miner")
    test_mode: bool = Field(default=False)
    use_raw_preprocessing: bool = Field(default=False)

    def __init__(self, db_manager=None, node_type=None, test_mode=False, **data):
        super().__init__(
            name="SoilMoistureTask",
            description="Soil moisture prediction task",
            task_type="atomic",
            metadata=SoilMoistureMetadata(),
            inputs=SoilMoistureInputs(),
            outputs=SoilMoistureOutputs(),
            scoring_mechanism=SoilScoringMechanism(db_manager=db_manager),
        )

        self.db_manager = db_manager
        self.node_type = node_type
        self.test_mode = test_mode

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

        await self.ensure_retry_columns_exist()
        
        while True:
            try:
                await validator.update_task_status('soil', 'active')
                current_time = datetime.now(timezone.utc)

                if current_time.minute % 1 == 0:
                    await validator.update_task_status('soil', 'processing', 'scoring')
                    await self.validator_score()

                if self.test_mode:
                    logger.info("Running in test mode - bypassing window checks")
                    target_smap_time = self.get_smap_time_for_validator(current_time)
                    ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)

                    await validator.update_task_status('soil', 'processing', 'data_download')
                    await self.validator_preprocessing.get_daily_regions(
                        target_time=target_smap_time,
                        ifs_forecast_time=ifs_forecast_time,
                    )

                    # Process regions in chunks with memory management
                    chunk_size = 5
                    offset = 0
                    
                    while True:
                        # Use a memory-efficient query that only fetches necessary columns
                        regions = await self.db_manager.fetch_many(
                            """
                            SELECT id, combined_data, sentinel_bounds, sentinel_crs, target_time 
                            FROM soil_moisture_regions 
                            WHERE status = 'pending'
                            AND target_time = :target_time
                            LIMIT :chunk_size OFFSET :offset
                            """,
                            {
                                "target_time": target_smap_time,
                                "chunk_size": chunk_size,
                                "offset": offset
                            }
                        )
                        
                        if not regions:
                            break
                            
                        for region in regions:
                            try:
                                await validator.update_task_status('soil', 'processing', 'region_processing')
                                logger.info(f"Processing region {region['id']} (chunk offset: {offset})")

                                if not self._validate_region_data(region):
                                    continue

                                # Process region data in a separate function to allow better garbage collection
                                await self._process_single_region(region, validator)
                                
                                # Force garbage collection after each region
                                import gc
                                gc.collect()
                                
                            except Exception as e:
                                logger.error(f"Error preparing region: {str(e)}")
                                logger.error(traceback.format_exc())
                                continue
                        
                        offset += chunk_size
                        # Small delay between chunks and cleanup
                        await asyncio.sleep(1)
                        gc.collect()

                    logger.info("Test mode execution complete. Disabling test mode.")
                    self.test_mode = False
                    await validator.update_task_status('soil', 'idle')
                    continue

                windows = self.get_validator_windows()
                current_window = next(
                    (w for w in windows if self.is_in_window(current_time, w)), None
                )

                if not current_window:
                    logger.info(
                        f"Not in any preparation or execution window: {current_time}"
                    )
                    logger.info(
                        f"Next soil task time: {self.get_next_preparation_time(current_time)}"
                    )
                    logger.info(f"Sleeping for 60 seconds")
                    await validator.update_task_status('soil', 'idle')
                    await asyncio.sleep(60)
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
                else:
                    # Execution window logic
                    query = """
                    SELECT * FROM soil_moisture_regions 
                    WHERE status = 'pending'
                    AND target_time = :target_time
                    """
                    
                    # Get total count of pending regions before processing
                    total_regions = await self.db_manager.fetch_one(
                        """
                        SELECT COUNT(*) as count 
                        FROM soil_moisture_regions 
                        WHERE status = 'pending'
                        AND target_time = :target_time
                        """,
                        {"target_time": target_smap_time}
                    )
                    total_count = total_regions["count"] if total_regions else 0
                    processed_count = 0
                    logger.info(f"Starting processing of {total_count} pending regions for target time {target_smap_time}")
                    
                    # Process regions in chunks of 5 to prevent memory bloat
                    chunk_size = 5
                    offset = 0
                    
                    while True:
                        chunked_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
                        regions = await self.db_manager.fetch_many(
                            chunked_query,
                            {"target_time": target_smap_time},
                        )
                        
                        if not regions:
                            break
                            
                        for region in regions:
                            try:
                                await validator.update_task_status('soil', 'processing', 'region_processing')
                                processed_count += 1
                                logger.info(f"Processing region {region['id']} (progress: {processed_count}/{total_count}, chunk offset: {offset})")

                                if "combined_data" not in region:
                                    logger.error(f"Region {region['id']} missing combined_data field")
                                    continue
                                    
                                if not region["combined_data"]:
                                    logger.error(f"Region {region['id']} has null combined_data")
                                    continue
                                    
                                combined_data = region["combined_data"]
                                if not isinstance(combined_data, bytes):
                                    logger.error(
                                        f"Region {region['id']} has invalid data type: {type(combined_data)}"
                                    )
                                    continue
                                    
                                # Process the region data
                                if not (
                                    combined_data.startswith(b"II\x2A\x00")
                                    or combined_data.startswith(b"MM\x00\x2A")
                                ):
                                    logger.error(
                                        f"Region {region['id']} has invalid TIFF header"
                                    )
                                    logger.error(
                                        f"First 16 bytes: {combined_data[:16].hex()}"
                                    )
                                    continue

                                logger.info(
                                    f"Region {region['id']} TIFF size: {len(combined_data) / (1024 * 1024):.2f} MB"
                                )
                                logger.info(
                                    f"Region {region['id']} TIFF header: {combined_data[:4]}"
                                )
                                logger.info(
                                    f"Region {region['id']} TIFF header hex: {combined_data[:16].hex()}"
                                )
                                encoded_data = base64.b64encode(combined_data)
                                logger.info(
                                    f"Base64 first 16 chars: {encoded_data[:16]}"
                                )

                                task_data = {
                                    "region_id": region["id"],
                                    "combined_data": encoded_data.decode("ascii"),
                                    "sentinel_bounds": region["sentinel_bounds"],
                                    "sentinel_crs": region["sentinel_crs"],
                                    "target_time": target_smap_time.isoformat(),
                                }

                                payload = {
                                    "nonce": str(uuid4()),
                                    "data": task_data,
                                }

                                logger.info(
                                    f"Sending payload to miners with region_id: {task_data['region_id']}"
                                )
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
                                    }
                                    await self.add_task_to_queue(responses, metadata)

                                    await self.db_manager.execute(
                                        """
                                        UPDATE soil_moisture_regions 
                                        SET status = 'sent_to_miners'
                                        WHERE id = :region_id
                                        """,
                                        {"region_id": region["id"]},
                                    )

                            except Exception as e:
                                logger.error(f"Error preparing region: {str(e)}")
                                logger.error(traceback.format_exc())
                                continue

                        offset += chunk_size
                        # Small delay between chunks to prevent overwhelming the system
                        await asyncio.sleep(1)
                        
                        # Force garbage collection after each chunk
                        import gc
                        gc.collect()
                        
                        # Log progress after each chunk
                        logger.info(f"Processed {processed_count}/{total_count} regions ({(processed_count/total_count*100):.1f}% complete)")

                    # Log final statistics
                    success_count = await self.db_manager.fetch_one(
                        """
                        SELECT COUNT(*) as count 
                        FROM soil_moisture_regions 
                        WHERE status = 'sent_to_miners'
                        AND target_time = :target_time
                        """,
                        {"target_time": target_smap_time}
                    )
                    success_count = success_count["count"] if success_count else 0
                    
                    error_count = total_count - success_count
                    logger.info(f"Region processing complete for {target_smap_time}:")
                    logger.info(f"Total regions: {total_count}")
                    logger.info(f"Successfully processed: {success_count}")
                    logger.info(f"Failed to process: {error_count}")
                    logger.info(f"Success rate: {(success_count/total_count*100):.1f}%" + (" if total_count > 0 else 'N/A'"))

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

            except Exception as e:
                logger.error(f"Error in validator_execute: {e}")
                logger.error(traceback.format_exc())
                await validator.update_task_status('soil', 'error')
                await asyncio.sleep(60)

    async def get_todays_regions(self, target_time: datetime) -> List[Dict]:
        """Get regions already selected for today."""
        try:
            async with self.db_manager.get_connection() as conn:
                regions = await conn.fetch(
                    """
                    SELECT * FROM soil_moisture_regions
                    WHERE region_date = $1::date
                    AND status = 'pending'
                """,
                    target_time.date(),
                )
                return regions
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
        """Add task to queue with memory-efficient processing."""
        try:
            if not self.db_manager:
                raise RuntimeError("Database manager not initialized")

            # Update region status first
            await self.db_manager.execute(
                """
                UPDATE soil_moisture_regions 
                SET status = 'sent_to_miners' 
                WHERE id = :region_id
                """,
                {"region_id": metadata["region_id"]}
            )

            # Process responses in chunks to manage memory
            chunk_size = 10
            response_items = list(responses.items())
            
            for i in range(0, len(response_items), chunk_size):
                chunk = response_items[i:i + chunk_size]
                
                for miner_hotkey, response_data in chunk:
                    try:
                        # Get miner UID
                        result = await self.db_manager.fetch_one(
                            "SELECT uid FROM node_table WHERE hotkey = :miner_hotkey",
                            {"miner_hotkey": miner_hotkey}
                        )

                        if not result:
                            logger.warning(f"No UID found for hotkey {miner_hotkey}")
                            continue

                        miner_uid = str(result["uid"])

                        # Process response data
                        if isinstance(response_data, dict) and "text" in response_data:
                            try:
                                response_data = json.loads(response_data["text"])
                            except json.JSONDecodeError as e:
                                logger.error(f"Failed to parse response text for {miner_hotkey}: {e}")
                                continue

                        # Create prediction data with minimal memory footprint
                        prediction_data = {
                            "region_id": metadata["region_id"],
                            "miner_uid": miner_uid,
                            "miner_hotkey": miner_hotkey,
                            "target_time": metadata["target_time"],
                            "surface_sm": response_data.get("surface_sm", []),
                            "rootzone_sm": response_data.get("rootzone_sm", []),
                            "uncertainty_surface": response_data.get("uncertainty_surface"),
                            "uncertainty_rootzone": response_data.get("uncertainty_rootzone"),
                            "sentinel_bounds": response_data.get("sentinel_bounds", metadata.get("sentinel_bounds")),
                            "sentinel_crs": response_data.get("sentinel_crs", metadata.get("sentinel_crs")),
                            "status": "sent_to_miner",
                        }

                        # Insert prediction
                        await self.db_manager.execute(
                            """
                            INSERT INTO soil_moisture_predictions 
                            (region_id, miner_uid, miner_hotkey, target_time, surface_sm, rootzone_sm, 
                            uncertainty_surface, uncertainty_rootzone, sentinel_bounds, 
                            sentinel_crs, status)
                            VALUES 
                            (:region_id, :miner_uid, :miner_hotkey, :target_time, 
                            :surface_sm, :rootzone_sm,
                            :uncertainty_surface, :uncertainty_rootzone, :sentinel_bounds,
                            :sentinel_crs, :status)
                            """,
                            prediction_data,
                        )

                        logger.debug(f"Stored predictions from miner {miner_hotkey} (UID: {miner_uid})")
                        
                        # Clear data to free memory
                        del prediction_data
                        del response_data
                        
                    except Exception as e:
                        logger.error(f"Error processing response from miner {miner_hotkey}: {str(e)}")
                        logger.error(traceback.format_exc())
                        continue
                
                # Cleanup after each chunk
                gc.collect()
                await asyncio.sleep(0.1)

        except Exception as e:
            logger.error(f"Error storing predictions: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def get_pending_tasks(self):
        """Get tasks that are ready for scoring and haven't been scored yet."""
        scoring_time = datetime.now(timezone.utc) - self.scoring_delay
        #scoring_time = scoring_time.replace(hour=19, minute=30, second=0, microsecond=0) # this is for testing
        
        try:
            debug_result = await self.db_manager.fetch_many(
                """
                SELECT p.status, COUNT(*) as count, MIN(r.target_time) as earliest, MAX(r.target_time) as latest
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                GROUP BY p.status
                """
            )
            for row in debug_result:
                logger.info(f"Status: {row['status']}, Count: {row['count']}, Time Range: {row['earliest']} to {row['latest']}")

            result = await self.db_manager.fetch_many(
                """
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
                """,
                {
                    "scoring_time": scoring_time,
                    "current_time": datetime.now(timezone.utc)
                }
            )
            logger.debug(f"Found {len(result) if result else 0} pending tasks")
            return result

        except Exception as e:
            logger.error(f"Error fetching pending tasks: {str(e)}")
            return []

    async def move_task_to_history(
        self, region: Dict, predictions: Dict, ground_truth: Dict, scores: Dict
    ):
        conn = None
        try:
            logger.info(f"Final scores for region {region['id']}:")
            logger.info(f"Surface RMSE: {scores['metrics'].get('surface_rmse'):.4f}")
            logger.info(f"Surface SSIM: {scores['metrics'].get('surface_ssim', 0):.4f}")
            logger.info(f"Rootzone RMSE: {scores['metrics'].get('rootzone_rmse'):.4f}")
            logger.info(
                f"Rootzone SSIM: {scores['metrics'].get('rootzone_ssim', 0):.4f}"
            )
            logger.info(f"Total Score: {scores.get('total_score', 0):.4f}")

            conn = await self.db_manager.get_connection()
            async with conn as session:
                async with session.begin():
                    for prediction in predictions:
                        miner_id = prediction["miner_id"]
                        params = {
                            "region_id": region["id"],
                            "miner_uid": miner_id,
                            "miner_hotkey": prediction.get("miner_hotkey", ""),
                            "target_time": region["target_time"],
                            "surface_sm_pred": prediction["surface_sm"],
                            "rootzone_sm_pred": prediction["rootzone_sm"],
                            "surface_sm_truth": (
                                ground_truth["surface_sm"] if ground_truth else None
                            ),
                            "rootzone_sm_truth": (
                                ground_truth["rootzone_sm"] if ground_truth else None
                            ),
                            "surface_rmse": scores["metrics"].get("surface_rmse"),
                            "rootzone_rmse": scores["metrics"].get("rootzone_rmse"),
                            "surface_structure_score": scores["metrics"].get(
                                "surface_ssim", 0
                            ),
                            "rootzone_structure_score": scores["metrics"].get(
                                "rootzone_ssim", 0
                            ),
                        }

                        query = text(
                            """
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
                        )

                        await session.execute(query, params)
                        logger.info(
                            f"Stored scores for miner {miner_id} in region {region['id']}"
                        )

                        update_query = text("""
                            UPDATE soil_moisture_predictions 
                            SET status = 'scored'
                            WHERE region_id = :region_id AND miner_uid = :miner_uid
                        """)
                        await session.execute(update_query, params)

            await self.cleanup_predictions(
                bounds=region["sentinel_bounds"],
                target_time=region["target_time"],
                miner_uid=miner_id
            )
            
        except Exception as e:
            logger.error(f"Failed to move task to history: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            if conn:
                await conn.close()

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

            for target_time, tasks in tasks_by_time.items():
                logger.info(f"Processing {len(tasks)} predictions for timestamp {target_time}")
                
                smap_url = construct_smap_url(target_time)
                temp_file = None
                temp_path = None
                try:
                    temp_file = tempfile.NamedTemporaryFile(suffix=".h5", delete=False)
                    temp_path = temp_file.name
                    temp_file.close()

                    if not download_smap_data(smap_url, temp_file.name):
                        logger.error(f"Failed to download SMAP data for {target_time}")
                        continue

                    for task in tasks:
                        try:
                            scores = {}
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
                                
                                score = await self.scoring_mechanism.score(pred_data)
                                if score:
                                    scores = score
                                    task["score"] = score
                                    await self.move_task_to_history(
                                        region=task,
                                        predictions=task["predictions"],
                                        ground_truth=score.get("ground_truth"),
                                        scores=score
                                    )

                        except Exception as e:
                            logger.error(f"Error scoring task {task['id']}: {str(e)}")
                            continue

                    score_rows = await self.build_score_row(target_time, tasks)
                    if score_rows:
                        query = """
                        INSERT INTO score_table (task_name, task_id, score, status)
                        VALUES (:task_name, :task_id, :score, :status)
                        """
                        for row in score_rows:
                            await self.db_manager.execute(query, row)
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
            
            miner_query = """
            SELECT uid, hotkey FROM node_table 
            WHERE hotkey IS NOT NULL
            """
            miner_mappings = await self.db_manager.fetch_many(miner_query)
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

    async def recalculate_recent_scores(self, uids: list):
        """
        Recalculate recent scores for the given UIDs across the 3-day scoring window
        and update all affected score_table rows.

        Args:
            uids (list): List of UIDs to recalculate scores for.
        """
        try:
            # Validate UIDs
            if not all(isinstance(uid, int) and 0 <= uid < 256 for uid in uids):
                logger.error("Invalid UIDs provided for recalculation")
                return

            # First fetch historical data to ensure it's available
            current_time = datetime.now(timezone.utc)
            history_window = current_time - self.scoring_delay  # 3 days

            history_query = """
            SELECT 
                miner_uid,
                miner_hotkey,
                surface_rmse,
                rootzone_rmse,
                surface_structure_score,
                rootzone_structure_score,
                target_time
            FROM soil_moisture_history
            WHERE target_time >= :history_window
            AND miner_uid = ANY(:uids)
            ORDER BY target_time ASC
            """

            history_results = await self.db_manager.fetch_many(
                history_query, 
                {
                    "history_window": history_window,
                    "uids": [str(uid) for uid in uids]
                }
            )

            if not history_results:
                logger.warning(f"No historical data found for UIDs {uids} in window {history_window} to {current_time}")
                return

            # Start transaction for deletion operations
            async with self.db_manager.get_connection() as conn:
                async with conn.begin():
                    # Delete predictions
                    delete_query = """
                    DELETE FROM soil_moisture_predictions
                    WHERE miner_uid = ANY(:uids)
                    """
                    str_uids = [str(uid) for uid in uids]
                    await conn.execute(text(delete_query), {"uids": str_uids})
                    
                    # Delete scores
                    delete_scores_query = """
                    DELETE FROM score_table 
                    WHERE task_name = 'soil_moisture'
                    AND task_id::float >= :start_timestamp
                    AND task_id::float <= :end_timestamp
                    """
                    await conn.execute(
                        text(delete_scores_query),
                        {
                            "start_timestamp": history_window.timestamp(),
                            "end_timestamp": current_time.timestamp(),
                        },
                    )
                    logger.info(f"Successfully deleted predictions and scores for UIDs: {uids}")

            # Group records by day
            daily_records = {}
            for record in history_results:
                day_key = record["target_time"].replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                if day_key not in daily_records:
                    daily_records[day_key] = []
                daily_records[day_key].append(record)

            # Process each day's records
            for day, records in daily_records.items():
                miner_scores = {}
                for record in records:
                    try:
                        miner_uid = int(record["miner_uid"])
                        if miner_uid not in miner_scores:
                            miner_scores[miner_uid] = {
                                "surface_rmse": [],
                                "rootzone_rmse": [],
                                "surface_ssim": [],
                                "rootzone_ssim": [],
                            }

                        # Safely append non-None values
                        if record["surface_rmse"] is not None:
                            miner_scores[miner_uid]["surface_rmse"].append(record["surface_rmse"])
                        if record["rootzone_rmse"] is not None:
                            miner_scores[miner_uid]["rootzone_rmse"].append(record["rootzone_rmse"])
                        if record["surface_structure_score"] is not None:
                            miner_scores[miner_uid]["surface_ssim"].append(record["surface_structure_score"])
                        if record["rootzone_structure_score"] is not None:
                            miner_scores[miner_uid]["rootzone_ssim"].append(record["rootzone_structure_score"])
                    except (ValueError, TypeError) as e:
                        logger.error(f"Error processing record for miner {record.get('miner_uid')}: {e}")
                        continue

                scores = [float("nan")] * 256
                for miner_uid, score_lists in miner_scores.items():
                    try:
                        # Calculate averages only if we have valid data
                        if score_lists["surface_rmse"] and score_lists["rootzone_rmse"]:
                            avg_surface_rmse = sum(score_lists["surface_rmse"]) / len(score_lists["surface_rmse"])
                            avg_rootzone_rmse = sum(score_lists["rootzone_rmse"]) / len(score_lists["rootzone_rmse"])
                            
                            # Handle optional SSIM scores
                            avg_surface_ssim = (
                                sum(score_lists["surface_ssim"]) / len(score_lists["surface_ssim"])
                                if score_lists["surface_ssim"] else 0.0
                            )
                            avg_rootzone_ssim = (
                                sum(score_lists["rootzone_ssim"]) / len(score_lists["rootzone_ssim"])
                                if score_lists["rootzone_ssim"] else 0.0
                            )

                            surface_score = math.exp(-abs(avg_surface_rmse) / 10)
                            rootzone_score = math.exp(-abs(avg_rootzone_rmse) / 10)

                            total_score = (
                                0.25 * surface_score
                                + 0.25 * rootzone_score
                                + 0.25 * avg_surface_ssim
                                + 0.25 * avg_rootzone_ssim
                            )

                            scores[miner_uid] = total_score
                            logger.debug(
                                f"Calculated score for miner {miner_uid}: {total_score:.4f} "
                                f"(surface: {surface_score:.4f}, rootzone: {rootzone_score:.4f}, "
                                f"surface_ssim: {avg_surface_ssim:.4f}, rootzone_ssim: {avg_rootzone_ssim:.4f})"
                            )
                    except Exception as e:
                        logger.error(f"Error calculating scores for miner {miner_uid}: {e}")
                        continue

                # Insert new score row within a transaction
                async with self.db_manager.get_connection() as conn:
                    async with conn.begin():
                        score_row = {
                            "task_name": "soil_moisture",
                            "task_id": str(day.timestamp()),
                            "score": scores,
                            "status": "completed",
                        }

                        insert_query = """
                        INSERT INTO score_table (task_name, task_id, score, status)
                        VALUES (:task_name, :task_id, :score, :status)
                        """
                        await conn.execute(text(insert_query), score_row)
                        logger.info(f"Recalculated and inserted score row for day {day}")

            logger.info(f"Completed recalculation of scores for UIDs: {uids} over 3-day window")

        except Exception as e:
            logger.error(f"Error recalculating recent scores: {e}")
            logger.error(traceback.format_exc())
            raise  # Re-raise to trigger error handling in deregistration loop

    async def cleanup_predictions(self, bounds, target_time=None, miner_uid=None):
        """Clean up predictions after they've been processed and moved to history."""
        try:
            conn = await self.db_manager.get_connection()
            try:
                async with conn.begin():
                    result = await conn.execute(
                        text("""
                        DELETE FROM soil_moisture_predictions p
                        USING soil_moisture_regions r
                        WHERE p.region_id = r.id 
                        AND r.sentinel_bounds = :bounds
                        AND r.target_time = :target_time
                        AND p.miner_uid = :miner_uid
                        AND p.status = 'scored'
                        RETURNING p.id
                        """),
                        {
                            "bounds": bounds,
                            "target_time": target_time,
                            "miner_uid": miner_uid
                        }
                    )
                    
                    deleted_count = result.rowcount
                    logger.info(
                        f"Cleaned up {deleted_count} predictions for bounds {bounds}"
                        f"{f', time {target_time}' if target_time else ''}"
                        f"{f', miner {miner_uid}' if miner_uid else ''}"
                    )
                    
            finally:
                await conn.close()

        except Exception as e:
            logger.error(f"Failed to cleanup predictions: {str(e)}")
            logger.error(traceback.format_exc())

    async def ensure_retry_columns_exist(self):
        """Ensure retry-related columns exist in soil_moisture_predictions table."""
        try:
            columns_check = await self.db_manager.fetch_one("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name = 'soil_moisture_predictions' 
                    AND column_name = 'retry_count'
                );
            """)
            
            if not columns_check or not columns_check['exists']:
                logger.info("Adding retry columns to soil_moisture_predictions table")
                await self.db_manager.execute("""
                    ALTER TABLE soil_moisture_predictions 
                    ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS next_retry_time TIMESTAMP WITH TIME ZONE,
                    ADD COLUMN IF NOT EXISTS last_retry_at TIMESTAMP WITH TIME ZONE;
                """)
                logger.info("Successfully added retry columns")
            
        except Exception as e:
            logger.error(f"Error ensuring retry columns exist: {e}")
            logger.error(traceback.format_exc())

    async def _cleanup_processing_rows(self):
        """Clean up any rows that might still have 'processing' status."""
        try:
            async with self.db_manager.get_connection() as conn:
                async with conn.begin():
                    result = await conn.execute(
                        text("""
                        UPDATE soil_moisture_regions 
                        SET status = 'pending',
                            updated_at = NOW()
                        WHERE status = 'processing'
                        RETURNING id
                        """)
                    )
                    cleaned_rows = await result.fetchall()
                    if cleaned_rows:
                        logger.warning(f"Reset {len(cleaned_rows)} regions from 'processing' to 'pending' status")
                        logger.warning(f"Region IDs: {[row[0] for row in cleaned_rows]}")
        except Exception as e:
            logger.error(f"Error cleaning up processing rows: {e}")
            logger.error(traceback.format_exc())

    async def cleanup_resources(self):
        """Clean up any resources used by the task during recovery or shutdown."""
        try:
            # Clean up any rows with 'processing' status
            await self._cleanup_processing_rows()

            # 1. Clean up temporary files with better error handling
            temp_dir = "/tmp"
            patterns = ["*.h5", "*.tif", "*.tiff"]
            for pattern in patterns:
                try:
                    for f in glob.glob(os.path.join(temp_dir, pattern)):
                        try:
                            if os.path.exists(f):
                                os.unlink(f)
                                logger.debug(f"Cleaned up temp file: {f}")
                        except Exception as e:
                            logger.error(f"Failed to remove temp file {f}: {e}")
                except Exception as e:
                    logger.error(f"Error cleaning up {pattern} files: {e}")

            # 2. Clean up any orphaned 'sent_to_miners' regions that haven't been scored
            try:
                async with self.db_manager.get_connection() as conn:
                    async with conn.begin():
                        await conn.execute(
                            text("""
                            UPDATE soil_moisture_regions 
                            SET status = 'pending',
                                updated_at = NOW()
                            WHERE status = 'sent_to_miners'
                            AND updated_at < NOW() - INTERVAL '1 hour'
                            AND NOT EXISTS (
                                SELECT 1 FROM soil_moisture_predictions 
                                WHERE soil_moisture_predictions.region_id = soil_moisture_regions.id
                            )
                            """)
                        )

                logger.info("Cleaned up orphaned regions")

            except Exception as e:
                logger.error(f"Failed to cleanup orphaned regions: {e}")
                logger.error(traceback.format_exc())

            # 3. Clear in-memory caches
            self._daily_regions = {}
            
            # 4. Clean up any temporary data directories
            data_dir = get_data_dir()
            if os.path.exists(data_dir):
                try:
                    for item in os.listdir(data_dir):
                        item_path = os.path.join(data_dir, item)
                        try:
                            if os.path.isdir(item_path) and item.startswith('tmp'):
                                shutil.rmtree(item_path)
                                logger.debug(f"Removed temp directory: {item_path}")
                            elif item.endswith(('.tif', '.h5')):
                                os.unlink(item_path)
                                logger.debug(f"Removed temp file: {item_path}")
                        except Exception as e:
                            logger.error(f"Failed to remove {item_path}: {e}")
                except Exception as e:
                    logger.error(f"Error cleaning up data directory: {e}")
            
            logger.info("Completed soil task cleanup")
            
        except Exception as e:
            logger.error(f"Error during soil task cleanup: {e}")
            logger.error(traceback.format_exc())
            raise

    def _validate_region_data(self, region: Dict) -> bool:
        """Validate region data before processing."""
        if "combined_data" not in region:
            logger.error(f"Region {region['id']} missing combined_data field")
            return False

        if not region["combined_data"]:
            logger.error(f"Region {region['id']} has null combined_data")
            return False

        combined_data = region["combined_data"]
        if not isinstance(combined_data, bytes):
            logger.error(f"Region {region['id']} has invalid data type: {type(combined_data)}")
            return False

        if not (combined_data.startswith(b"II\x2A\x00") or combined_data.startswith(b"MM\x00\x2A")):
            logger.error(f"Region {region['id']} has invalid TIFF header")
            logger.error(f"First 16 bytes: {combined_data[:16].hex()}")
            return False

        return True

    async def _process_single_region(self, region: Dict, validator) -> None:
        """Process a single region with memory management."""
        try:
            # Check if region is still pending using row-level locking
            region_status = await self.db_manager.fetch_one(
                """
                SELECT status 
                FROM soil_moisture_regions 
                WHERE id = :region_id
                AND status = 'pending'
                FOR UPDATE SKIP LOCKED
                """,
                {"region_id": region["id"]}
            )
            
            if not region_status:
                logger.info(f"Skipping region {region['id']} as it's no longer pending")
                return

            # Log initial memory state
            memory_info = psutil.Process().memory_info()
            initial_memory = memory_info.rss / (1024 * 1024)
            logger.debug(f"Initial memory before processing region {region['id']}: {initial_memory:.2f}MB")

            # Process TIFF data in chunks if possible
            combined_data = region["combined_data"]
            logger.info(f"Region {region['id']} TIFF size: {len(combined_data) / (1024 * 1024):.2f} MB")
            
            # Encode data in chunks to reduce memory usage
            chunk_size = 1024 * 1024  # 1MB chunks
            encoded_chunks = []
            for i in range(0, len(combined_data), chunk_size):
                chunk = combined_data[i:i + chunk_size]
                encoded_chunks.append(base64.b64encode(chunk).decode("ascii"))
            encoded_data = "".join(encoded_chunks)
            
            # Clear the original data to free memory
            del combined_data
            gc.collect()

            task_data = {
                "region_id": region["id"],
                "combined_data": encoded_data,
                "sentinel_bounds": region["sentinel_bounds"],
                "sentinel_crs": region["sentinel_crs"],
                "target_time": region["target_time"].isoformat(),
            }

            # Clear encoded chunks to free memory
            del encoded_chunks
            gc.collect()

            payload = {"nonce": str(uuid4()), "data": task_data}
            
            # Clear task data to free memory
            del task_data
            gc.collect()

            logger.info(f"Sending payload to miners with region_id: {region['id']}")
            await validator.update_task_status('soil', 'processing', 'miner_query')
            
            responses = await validator.query_miners(
                payload=payload,
                endpoint="/soilmoisture-request",
            )

            if responses:
                metadata = {
                    "region_id": region["id"],
                    "target_time": region["target_time"],
                    "data_collection_time": datetime.now(timezone.utc),
                }
                await self.add_task_to_queue(responses, metadata)

                # Update status to sent_to_miners only if we got responses
                await self.db_manager.execute(
                    """
                    UPDATE soil_moisture_regions 
                    SET status = 'sent_to_miners'
                    WHERE id = :region_id
                    AND status = 'pending'
                    """,
                    {"region_id": region["id"]},
                )
                logger.info(f"Successfully processed region {region['id']}")

        except Exception as e:
            logger.error(f"Error processing region {region['id']}: {str(e)}")
            logger.error(traceback.format_exc())
