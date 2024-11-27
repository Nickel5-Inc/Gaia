from gaia.tasks.base.task import Task
from datetime import datetime, timedelta, timezone
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from gaia.tasks.base.components.metadata import Metadata
from gaia.tasks.defined_tasks.soilmoisture.soil_miner_preprocessing import SoilMinerPreprocessing
from gaia.tasks.defined_tasks.soilmoisture.soil_validator_preprocessing import SoilValidatorPreprocessing
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

logger = get_logger(__name__)

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

    validator_preprocessing: Optional[SoilValidatorPreprocessing] = None
    miner_preprocessing: Optional[SoilMinerPreprocessing] = None
    model: Optional[SoilModel] = None

    def __init__(self, db_manager=None, node_type=None, **data):
        super().__init__(
            name="SoilMoistureTask",
            description="Soil moisture prediction task",
            task_type="atomic",
            metadata=SoilMoistureMetadata(),
            inputs=SoilMoistureInputs(),
            outputs=SoilMoistureOutputs(),
            scoring_mechanism=SoilScoringMechanism(),
            **data
        )

        if node_type == "validator":
            self.validator_preprocessing = SoilValidatorPreprocessing()
            self.db = ValidatorDatabaseManager() if not db_manager else db_manager
            logger.info("Initialized validator components for SoilMoistureTask")
            
        elif node_type == "miner":
            self.miner_preprocessing = SoilMinerPreprocessing()
            self.model = self.miner_preprocessing.model
            logger.info("Initialized miner components for SoilMoistureTask")

    def get_next_valid_time(self, current_time: datetime) -> datetime:
        """Get next valid SMAP measurement time."""
        valid_times = [
            (1, 30),
            (4, 30),
            (7, 30),
            (10, 30),
            (13, 30),
            (16, 30),
            (19, 30),
            (22, 30),
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
        try:
            current_time = datetime.now(timezone.utc)
            ifs_forecast_time = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
            next_smap_time = ifs_forecast_time + timedelta(days=3)

            logger.info("=== Starting Validator Execute ===")
            logger.info(f"IFS forecast time: {ifs_forecast_time}")
            logger.info(f"Target SMAP time: {next_smap_time}")

            # First get new regions and store them
            logger.info("Calling get_daily_regions to collect and store new data...")
            await self.validator_preprocessing.get_daily_regions(
                target_time=next_smap_time,
                ifs_forecast_time=ifs_forecast_time
            )
            logger.info("Finished get_daily_regions call")

            # Then pull pending regions from DB
            logger.info("Fetching pending regions from database...")
            regions = await self.db.fetch_many(
                """
                SELECT * FROM soil_moisture_regions 
                WHERE status = 'pending'
                """
            )
            logger.info(f"Found {len(regions) if regions else 0} pending regions in database")

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
                            'target_time': next_smap_time.isoformat()
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
                                "target_time": next_smap_time,
                                "data_collection_time": current_time,
                                "ifs_forecast_time": ifs_forecast_time
                            }
                            await self.add_task_to_queue(responses, metadata)

                            await self.db.execute(
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

        except Exception as e:
            logger.error(f"Error in validator execute: {str(e)}")
            raise

    async def get_todays_regions(self, target_time: datetime) -> List[Dict]:
        """Get regions already selected for today."""
        try:
            async with self.db.get_connection() as conn:
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
            processed_data = self.miner_preprocessing.process_miner_data(data['data'])
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
            # Get mapping of hotkeys to UIDs from node_table
            query = """
            SELECT uid, hotkey FROM node_table 
            WHERE hotkey IS NOT NULL
            """
            miner_mappings = await self.db.fetch_many(query)
            hotkey_to_uid = {row['hotkey']: str(row['uid']) for row in miner_mappings}  # Convert UID to string
            
            for miner_hotkey, response in responses.items():
                try:
                    # Validate miner hotkey exists in node_table
                    if miner_hotkey not in hotkey_to_uid:
                        logger.warning(f"Miner hotkey {miner_hotkey} not found in node_table, skipping")
                        continue
                        
                    miner_uid = hotkey_to_uid[miner_hotkey]  # Already a string from the mapping
                    response_data = json.loads(response['text'])
                    
                    # Convert numpy arrays to database-compatible format
                    surface_sm = np.array(response_data["surface_sm"]).astype(np.float32).tolist()
                    rootzone_sm = np.array(response_data["rootzone_sm"]).astype(np.float32).tolist()
                    
                    # Create prediction record
                    prediction_data = {
                        "region_id": metadata["region_id"],
                        "miner_uid": miner_uid,  # Now guaranteed to be a string
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

                    await self.db.execute(
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
            result = await self.db.fetch_many(
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
            async with self.db.get_connection() as conn:
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
