from gaia.tasks.base.task import Task
from datetime import datetime, timedelta, timezone
import numpy as np
from typing import Dict, List, Optional, Tuple
from gaia.tasks.base.components.metadata import Metadata
from gaia.tasks.defined_tasks.soilmoisture.soil_miner_preprocessing import SoilMinerPreprocessing
from gaia.tasks.defined_tasks.soilmoisture.soil_validator_preprocessing import SoilValidatorPreprocessing
from gaia.tasks.defined_tasks.soilmoisture.soil_scoring_mechanism import SoilScoringMechanism
from gaia.tasks.defined_tasks.soilmoisture.soil_inputs import (
    SoilMoistureInputs,
    SoilMoisturePayload,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_outputs import SoilMoistureOutputs
from gaia.tasks.defined_tasks.soilmoisture.soil_metadata import SoilMoistureMetadata
from pydantic import Field
from fiber.logging_utils import get_logger
from uuid import uuid4
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from sqlalchemy import text

logger = get_logger(__name__)

class SoilMoistureTask(Task):
    """Task for soil moisture prediction using satellite and weather data."""

    validator_preprocessing: SoilValidatorPreprocessing = Field(
        default_factory=SoilValidatorPreprocessing,
        description="Preprocessing component for validator",
    )
    miner_preprocessing: SoilMinerPreprocessing = Field(
        default_factory=SoilMinerPreprocessing,
        description="Preprocessing component for miner",
    )
    prediction_horizon: timedelta = Field(
        default_factory=lambda: timedelta(hours=6),
        description="Prediction horizon for the task",
    )
    scoring_delay: timedelta = Field(
        default_factory=lambda: timedelta(days=3),
        description="Delay before scoring due to SMAP data latency",
    )

    def __init__(self):
        super().__init__(
            name="SoilMoistureTask",
            description="Soil moisture prediction from satellite/weather data",
            task_type="atomic",
            metadata=SoilMoistureMetadata(),
            inputs=SoilMoistureInputs(),
            outputs=SoilMoistureOutputs(),
            scoring_mechanism=SoilScoringMechanism(),
        )

        self.miner_preprocessing = SoilMinerPreprocessing()
        self.validator_preprocessing = SoilValidatorPreprocessing()
        self.db = ValidatorDatabaseManager()

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

    async def validator_execute(self):
        """Execute validator workflow."""
        current_time = datetime.now(timezone.utc)
        logger.info(f"Current time (UTC): {current_time}")

        # check for tasks ready for scoring (3 days)
        pending_tasks = await self.get_pending_tasks()
        if pending_tasks:
            logger.info(f"Found {len(pending_tasks)} tasks ready for scoring")
            for task in pending_tasks:
                try:
                    logger.info(f"Processing task {task['id']} from {task['target_time']}")
                    ground_truth = await self.collect_ground_truth(task['target_time'])
                    if ground_truth:
                        await self.process_scoring(task, ground_truth)
                except Exception as e:
                    logger.error(f"Error processing task {task['id']}: {str(e)}")

        next_smap_time = self.get_next_valid_time(current_time)
        preparation_time = next_smap_time - self.prediction_horizon  # 6 hours before
        
        time_until_prep = (preparation_time - current_time).total_seconds() / 60
        logger.info(f"Next SMAP measurement time: {next_smap_time}")
        logger.info(f"Task preparation time: {preparation_time}")
        logger.info(f"Minutes until preparation: {time_until_prep}")

        if True:  # TODO: Restore time check in production: -180 <= time_until_prep <= 180
            try:
                ifs_forecast_time = preparation_time.replace(
                    hour=(preparation_time.hour // 6) * 6,
                    minute=0, second=0, microsecond=0
                )
                logger.info(f"Using IFS forecast from: {ifs_forecast_time}")
                logger.info(f"For prediction target: {next_smap_time}")

                regions = await self.validator_preprocessing.get_daily_regions(
                    target_time=next_smap_time,
                    ifs_forecast_time=ifs_forecast_time
                )

                if regions:
                    logger.info(f"Selected {len(regions)} new regions")
                    for region in regions:
                        try:
                            task_data = {
                                'region_id': region['id'],
                                'data_collection_time': current_time,
                                'ifs_forecast_time': ifs_forecast_time,
                                'target_prediction_time': next_smap_time,
                                'combined_data': region['combined_data'],
                                'sentinel_bounds': region['sentinel_bounds'],
                                'sentinel_crs': region['sentinel_crs']
                            }
                            await self.query_miners(task_data)
                            
                            async with self.db.get_connection() as conn:
                                await conn.execute("""
                                    UPDATE soil_moisture_regions 
                                    SET status = 'sent_to_miners'
                                    WHERE id = $1
                                """, region['id'])
                                
                        except Exception as e:
                            logger.error(f"Error preparing region: {str(e)}")
                            continue
            except Exception as e:
                logger.error(f"Error in task preparation: {str(e)}")
        else:
            logger.debug(f"Not in preparation window. Next preparation in {time_until_prep:.2f} minutes")

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
            print(f"Error getting today's regions: {str(e)}")
            return []

    def miner_execute(self, data):
        """Execute miner workflow."""
        try:
            processed_data = self.miner_preprocessing.process_miner_data(data)
            predictions = self.run_model_inference(processed_data)

            return {
                "surface_sm": predictions["surface"],
                "rootzone_sm": predictions["rootzone"],
                "uncertainty_surface": predictions.get(
                    "uncertainty", None
                ),  # TODO: seperate surface and rootzone uncertainty
            }

        except Exception as e:
            print(f"Error in miner execution: {str(e)}")
            return None

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

    async def query_miners(self, task_data: Dict):
        """Send task to miners and collect responses."""
        try:
            async with self.db.get_connection() as conn:
                region = await conn.fetchrow("""
                    SELECT combined_data, sentinel_bounds, sentinel_crs, target_time
                    FROM soil_moisture_regions 
                    WHERE id = $1
                """, task_data['region_id'])
            soil_inputs = SoilMoistureInputs()
            payload = soil_inputs.format_miner_payload(
                region_id=task_data['region_id'],
                combined_data=region['combined_data'],
                sentinel_bounds=region['sentinel_bounds'],
                sentinel_crs=region['sentinel_crs'],
                target_time=region['target_time']
            )
            soil_inputs.validate_inputs(payload)
            responses = await self.broadcast_task(payload)
            if responses:
                await self.add_task_to_queue(responses, task_data)
            return responses
        except Exception as e:
            print(f"Error querying miners: {str(e)}")
            return None

    async def add_task_to_queue(self, predictions: Dict, metadata: Dict):
        """Add miner predictions to database for later scoring."""
        try:
            async with self.db.get_connection() as conn:
                for miner_id, pred in predictions.items():
                    await conn.execute(
                        """
                        INSERT INTO soil_moisture_predictions 
                        (region_id, miner_id, target_time, surface_sm, 
                         rootzone_sm, uncertainty_surface, uncertainty_rootzone,
                         sentinel_bounds, sentinel_crs)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                        metadata["region_id"],
                        miner_id,
                        metadata["target_time"],
                        pred["surface_sm"],
                        pred["rootzone_sm"],
                        pred.get("uncertainty_surface"),
                        pred.get("uncertainty_rootzone"),
                    )
        except Exception as e:
            print(f"Error storing predictions: {str(e)}")
            raise

    async def get_pending_tasks(self) -> List[Dict]:
        """Get tasks ready for scoring (past 3 day SMAP delay and predictions present).

        Returns:
            List[Dict]: List of tasks containing region info and predictions
        """
        scoring_time = datetime.now(timezone.utc) - self.scoring_delay

        try:
            conn = await self.db.get_connection()
            async with conn.transaction():
                return await conn.fetch("""
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
                    AND r.target_time <= $1
                    GROUP BY r.id
                    ORDER BY r.target_time ASC
                """, scoring_time)

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
            print(f"Error moving task to history: {str(e)}")
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
