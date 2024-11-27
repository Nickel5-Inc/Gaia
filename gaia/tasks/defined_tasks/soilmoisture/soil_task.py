from gaia.tasks.base.task import Task
from datetime import datetime, timedelta, timezone
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from gaia.tasks.base.components.metadata import Metadata
from gaia.tasks.defined_tasks.soilmoisture.soil_miner_preprocessing import SoilMinerPreprocessing
from gaia.tasks.defined_tasks.soilmoisture.soil_scoring_mechanism import SoilScoringMechanism
from gaia.tasks.defined_tasks.soilmoisture.soil_inputs import (
    SoilMoistureInputs,
    SoilMoisturePayload,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_outputs import SoilMoistureOutputs, SoilMoisturePrediction
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
import os

logger = get_logger(__name__)

# Conditional imports based on node type
if os.environ.get("NODE_TYPE") == "validator":
    from gaia.tasks.defined_tasks.soilmoisture.soil_validator_preprocessing import SoilValidatorPreprocessing
else:
    SoilValidatorPreprocessing = None  # type: ignore

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

    validator_preprocessing: Optional["SoilValidatorPreprocessing"] = None  # type: ignore
    miner_preprocessing: Optional["SoilMinerPreprocessing"] = None  # type: ignore
    model: Optional[SoilModel] = None
    db_manager: Any = Field(default=None)
    node_type: str = Field(default="miner")

    def __init__(self, db_manager, node_type: str = "miner"):
        super().__init__(
            name="SoilMoistureTask",
            description="Soil moisture prediction task",
            task_type="atomic",
            metadata=SoilMoistureMetadata(),
            inputs=SoilMoistureInputs(),
            outputs=SoilMoistureOutputs(),
            scoring_mechanism=SoilScoringMechanism(),
        )

        self.db_manager = db_manager
        self.node_type = node_type
        
        # Initialize appropriate preprocessing based on node type
        if node_type == "validator":
            self.validator_preprocessing = SoilValidatorPreprocessing(db_manager=db_manager)
        else:
            self.miner_preprocessing = SoilMinerPreprocessing(db_manager=db_manager)
            self.model = self.miner_preprocessing.model
            logger.info("Initialized miner components for SoilMoistureTask")

        self._prepared_regions = {}  # Format: {target_time: regions_data}

    def get_next_valid_time(self, current_time: datetime) -> datetime:
        """Get next valid SMAP measurement time."""
        valid_times = [
            (1, 30),
            (7, 30),
            (13, 30),
            (19, 30),
        ]
        target_time = current_time + self.prediction_horizon
        current_hour = target_time.hour
        next_time = min(
            valid_times,
            key=lambda x: (
                (x[0] - current_hour) % 24
                if (x[0] - current_hour) % 24 > 0
                else float("inf")
            ),
        )

        return target_time.replace(
            hour=next_time[0], minute=next_time[1], second=0, microsecond=0
        )

    async def validator_execute(self, validator):
        """Execute validator workflow."""
        while True:
            try:
                current_time = datetime.now(timezone.utc)
                
                if not self.should_run_validator(current_time):
                    # Sleep for 5 minutes before next check
                    await asyncio.sleep(100)
                    continue

                target_smap_time = self.get_smap_time_for_validator(current_time)
                ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)
                
                # If in preparation window, only collect data
                if current_time.minute < 30:
                    logger.info(f"In preparation window, collecting data for SMAP time: {target_smap_time}")
                    await self.validator_preprocessing.get_daily_regions(
                        target_time=target_smap_time,
                        ifs_forecast_time=ifs_forecast_time
                    )
                    # Sleep until execution window (30 minutes)
                    await asyncio.sleep(1800)
                    continue

                # If in execution window, query miners with prepared data
                logger.info(f"In execution window for SMAP time: {target_smap_time}")
                regions = await self.db_manager.fetch_many(
                    """
                    SELECT * FROM soil_moisture_regions 
                    WHERE status = 'pending'
                    AND datetime = :target_time
                    """,
                    {"target_time": target_smap_time}
                )

                if regions:
                    for region in regions:
                        try:
                            logger.info(f"Processing region {region['id']}")
                            
                            if 'combined_data' not in region:
                                logger.error(f"Region {region['id']} missing combined_data field")
                                continue
                            
                            if not region['combined_data']:
                                logger.error(f"Region {region['id']} has null combined_data")
                                continue

                            # Validate TIFF data retrieved from database
                            combined_data = region['combined_data']
                            if not isinstance(combined_data, bytes):
                                logger.error(f"Region {region['id']} has invalid data type: {type(combined_data)}")
                                continue

                            if not (combined_data.startswith(b'II\x2A\x00') or combined_data.startswith(b'MM\x00\x2A')):
                                logger.error(f"Region {region['id']} has invalid TIFF header")
                                logger.error(f"First 16 bytes: {combined_data[:16].hex()}")
                                continue

                            logger.info(f"Region {region['id']} TIFF size: {len(combined_data) / (1024 * 1024):.2f} MB")
                            logger.info(f"Region {region['id']} TIFF header: {combined_data[:4]}")
                            logger.info(f"Region {region['id']} TIFF header hex: {combined_data[:16].hex()}")

                            # Explicitly encode as base64
                            encoded_data = base64.b64encode(combined_data)  # This returns bytes
                            logger.info(f"Base64 first 16 chars: {encoded_data[:16]}")

                            task_data = {
                                'region_id': region['id'],
                                'combined_data': encoded_data.decode('ascii'),
                                'sentinel_bounds': region['sentinel_bounds'],
                                'sentinel_crs': region['sentinel_crs'],
                                'target_time': target_smap_time.isoformat()
                            }

                            payload = {
                                "nonce": str(uuid4()),
                                "data": task_data
                            }

                            logger.info(f"Sending region {region['id']} to miners...")
                            responses = await validator.query_miners(
                                payload=payload,
                                endpoint="/soilmoisture-request"
                            )

                            if responses:
                                metadata = {
                                    "region_id": region['id'],
                                    "target_time": target_smap_time,
                                    "data_collection_time": current_time,
                                    "ifs_forecast_time": ifs_forecast_time
                                }
                                await self.add_task_to_queue(responses, metadata)

                                await self.db_manager.execute(
                                    """
                                    UPDATE soil_moisture_regions 
                                    SET status = 'sent_to_miners'
                                    WHERE id = :region_id
                                    """,
                                    {"region_id": region['id']}
                                )

                        except Exception as e:
                            logger.error(f"Error preparing region: {str(e)}")
                            continue

                # Sleep until next preparation window
                next_prep_time = self.get_next_preparation_time(current_time)
                sleep_seconds = (next_prep_time - datetime.now(timezone.utc)).total_seconds()
                if sleep_seconds > 0:
                    logger.info(f"Sleeping until next preparation window: {next_prep_time}")
                    await asyncio.sleep(sleep_seconds)

            except Exception as e:
                logger.error(f"Error in validator_execute: {e}")
                logger.error(traceback.format_exc())
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
            processed_data = await self.miner_preprocessing.process_miner_data(data['data'])
            predictions = self.run_model_inference(processed_data)
            try:
                import matplotlib.pyplot as plt
                import numpy as np
                
                fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
                surface_plot = ax1.imshow(predictions["surface"], cmap='viridis')
                ax1.set_title('Surface Soil Moisture')
                plt.colorbar(surface_plot, ax=ax1, label='Moisture Content')
                rootzone_plot = ax2.imshow(predictions["rootzone"], cmap='viridis')
                ax2.set_title('Root Zone Soil Moisture')
                plt.colorbar(rootzone_plot, ax=ax2, label='Moisture Content')
                
                plt.tight_layout()
                plt.savefig('soil_moisture_predictions.png')
                plt.close()
                
                logger.info("Saved prediction visualization to soil_moisture_predictions.png")
                logger.info(f"Surface moisture stats - Min: {predictions['surface'].min():.3f}, "
                          f"Max: {predictions['surface'].max():.3f}, "
                          f"Mean: {predictions['surface'].mean():.3f}")
                logger.info(f"Root zone moisture stats - Min: {predictions['rootzone'].min():.3f}, "
                          f"Max: {predictions['rootzone'].max():.3f}, "
                          f"Mean: {predictions['rootzone'].mean():.3f}")
                
            except Exception as e:
                logger.error(f"Error creating visualization: {str(e)}")
            target_time = data['data']["target_time"]
            if isinstance(target_time, datetime):
                pass
            elif isinstance(target_time, str):
                target_time = datetime.fromisoformat(target_time)
            else:
                logger.error(f"Unexpected target_time type: {type(target_time)}")
                raise ValueError(f"Unexpected target_time format: {target_time}")
            
            prediction_time = self.get_next_valid_time(target_time)
            
            return {
                "surface_sm": predictions["surface"].tolist(),
                "rootzone_sm": predictions["rootzone"].tolist(),
                "uncertainty_surface": None,
                "uncertainty_rootzone": None,
                "miner_hotkey": miner.keypair.ss58_address,
                "sentinel_bounds": data['data']["sentinel_bounds"],
                "sentinel_crs": data['data']["sentinel_crs"],
                "target_time": prediction_time.isoformat()
            }

        except Exception as e:
            logger.error(f"Error in miner execution: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def score_predictions(self, predictions: Dict, ground_truth: Dict) -> Dict:
        """
        Score predictions RMSE + something else:
        - RMSE
        - Spatial structure score somehow
        MIGHT CHANGE THIS
        """
        return self.scoring_mechanism.compute_scores(predictions, ground_truth)


    def miner_preprocess(self, preprocessing=None, inputs=None):
        """Preprocess data for model input."""
        pass

    async def add_task_to_queue(self, responses: Dict, metadata: Dict) -> None:
        """Add task responses to the database queue."""
        try:
            query = """
            SELECT uid, hotkey FROM node_table 
            WHERE hotkey IS NOT NULL
            """
            miner_mappings = await self.db_manager.fetch_many(query)
            hotkey_to_uid = {row['hotkey']: str(row['uid']) for row in miner_mappings}
            
            for miner_hotkey, response in responses.items():
                try:
                    # Validate miner hotkey exists in node_table
                    if miner_hotkey not in hotkey_to_uid:
                        logger.warning(f"Miner hotkey {miner_hotkey} not found in node_table, skipping")
                        continue
                        
                    miner_uid = hotkey_to_uid[miner_hotkey]
                    response_data = json.loads(response['text'])
                    
                    surface_sm = np.array(response_data["surface_sm"]).astype(np.float32).tolist()
                    rootzone_sm = np.array(response_data["rootzone_sm"]).astype(np.float32).tolist()
                    
                    prediction_data = {
                        "region_id": metadata["region_id"],
                        "miner_uid": miner_uid,
                        "miner_hotkey": miner_hotkey,
                        "target_time": metadata["target_time"],
                        "surface_sm": surface_sm,
                        "rootzone_sm": rootzone_sm,
                        "uncertainty_surface": response_data.get("uncertainty_surface"),
                        "uncertainty_rootzone": response_data.get("uncertainty_rootzone"),
                        "sentinel_bounds": response_data["sentinel_bounds"],
                        "sentinel_crs": response_data["sentinel_crs"],
                        "status": "pending"
                    }

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
                        prediction_data
                    )
                    
                    logger.info(f"Stored predictions from miner {miner_hotkey} (UID: {miner_uid}) for region {metadata['region_id']}")

                except Exception as e:
                    logger.error(f"Error processing response from miner {miner_hotkey}: {str(e)}")
                    logger.error(traceback.format_exc())
                    continue

        except Exception as e:
            logger.error(f"Error storing predictions: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def get_pending_tasks(self):
        """Get tasks that are ready for scoring."""
        scoring_time = datetime.now(timezone.utc) - self.scoring_delay

        try:
            result = await self.db_manager.fetch_many(
                """
                SELECT 
                    r.*,
                    json_agg(json_build_object(
                        'miner_id', p.miner_id,
                        'surface_sm', p.surface_sm,
                        'rootzone_sm', p.rootzone_sm,
                        'uncertainty_surface', p.uncertainty_surface,
                        'uncertainty_rootzone', p.uncertainty_rootzone
                    )) as predictions
                FROM soil_moisture_regions r
                JOIN soil_moisture_predictions p ON p.region_id = r.id
                WHERE r.status = 'sent_to_miners'
                AND r.target_time <= :scoring_time
                GROUP BY r.id
                ORDER BY r.target_time ASC
                """,
                {"scoring_time": scoring_time}
            )
            return result

        except Exception as e:
            logger.error(f"Error fetching pending tasks: {str(e)}")
            return []

    async def move_task_to_history(
        self, region: Dict, predictions: Dict, ground_truth: Dict, scores: Dict
    ):
        """Move scored predictions to history table.

        Args:
            region (Dict): Region information
            predictions (Dict): Dictionary of miner predictions
            ground_truth (Dict): SMAP ground truth data
            scores (Dict): Computed scores for each miner
        """
        try:
            async with self.db_manager.get_connection() as conn:
                for miner_id, score in scores.items():
                    await conn.execute(
                        """
                        INSERT INTO soil_moisture_history 
                        (region_id, miner_id, target_time, 
                         surface_sm_pred, rootzone_sm_pred,
                         surface_sm_truth, rootzone_sm_truth,
                         surface_rmse, rootzone_rmse,
                         surface_structure_score, rootzone_structure_score)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """,
                        region["id"],
                        miner_id,
                        region["target_time"],
                        predictions[miner_id]["surface_sm"],
                        predictions[miner_id]["rootzone_sm"],
                        ground_truth["surface_sm"],
                        ground_truth["rootzone_sm"],
                        score["surface_rmse"],
                        score["rootzone_rmse"],
                        score["surface_structure"],
                        score["rootzone_structure"],
                    )

                await conn.execute(
                    """
                    UPDATE soil_moisture_regions 
                    SET status = 'scored'
                    WHERE id = $1
                """,
                    region["id"],
                )

        except Exception as e:
            logger.error(f"Error moving task to history: {str(e)}")
            raise

    async def validator_score(self, result=None):
        """Score the task results.

        Args:
            result: Result from miner predictions
        """
        pass

    async def validator_prepare_subtasks(self):
        """Prepare the subtasks for execution.

        Returns:
            List[Dict]: List of subtasks
        """
        pass

    def run_model_inference(self, processed_data):
        """Run model inference on processed data."""
        if not self.model:
            raise RuntimeError("Model not initialized. Are you running on a miner node?")
            
        return self.miner_preprocessing.predict_smap(processed_data, self.model)

    def should_run_validator(self, current_time: datetime) -> bool:
        """Check if validator should run at current time."""
        # Debug logging
        logger.info(f"Checking validator time: {current_time.hour}:{current_time.minute}")
        
        validator_windows = [
            (1, 30, 2, 0),     # Prep 1:30-2:00
            (7, 0, 7, 30),     # Prep 7:00-7:30
            (13, 30, 14, 0),   # Prep 13:30-14:00
            (19, 0, 19, 30),   # Prep 19:00-19:30
            (20, 0, 20, 5)     # Test window 20:00-20:05
        ]
        
        for window in validator_windows:
            start_hr, start_min, end_hr, end_min = window
            current_time_mins = current_time.hour * 60 + current_time.minute
            window_start_mins = start_hr * 60 + start_min
            window_end_mins = end_hr * 60 + end_min
            
            if window_start_mins <= current_time_mins < window_end_mins:
                logger.info(f"Found valid time window: {start_hr}:{start_min:02d}-{end_hr}:{end_min:02d}")
                return True
            
        # Find next window
        current_mins = current_time.hour * 60 + current_time.minute
        next_window = None
        for window in validator_windows:
            window_start_mins = window[0] * 60 + window[1]
            if window_start_mins > current_mins:
                next_window = window
                break
        
        if next_window is None:  # If no future window today, use first window tomorrow
            next_window = validator_windows[0]
        
        logger.info(f"Next execution window: {next_window[0]}:{next_window[1]:02d}")
        return False

    def get_ifs_time_for_smap(self, smap_time: datetime) -> datetime:
        """Get corresponding IFS forecast time for SMAP target time."""
        smap_to_ifs = {
            1: 0,    # 01:30 uses 00:00 forecast
            7: 6,    # 07:30 uses 06:00 forecast
            13: 12,  # 13:30 uses 12:00 forecast
            19: 18,  # 19:30 uses 18:00 forecast
        }
        ifs_hour = smap_to_ifs.get(smap_time.hour)
        if ifs_hour is None:
            raise ValueError(f"Invalid SMAP time: {smap_time.hour}:30")
        return smap_time.replace(hour=ifs_hour, minute=0, second=0, microsecond=0)

    def get_smap_time_for_validator(self, current_time: datetime) -> datetime:
        """Get SMAP time based on validator execution time."""
        validator_to_smap = {
            2: 1,   # 02:00 validator → 01:30 SMAP
            8: 7,   # 08:00 validator → 07:30 SMAP
            14: 13, # 14:00 validator → 13:30 SMAP
            20: 19  # 20:00 validator → 19:30 SMAP
        }
        smap_hour = validator_to_smap.get(current_time.hour)
        if smap_hour is None:
            raise ValueError(f"Invalid validator time: {current_time.hour}:00")
        
        return current_time.replace(hour=smap_hour, minute=30, second=0, microsecond=0)
