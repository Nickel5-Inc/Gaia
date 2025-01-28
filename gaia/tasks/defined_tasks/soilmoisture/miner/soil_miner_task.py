"""Soil Moisture Miner Task Flow.

This module implements the miner-side task flow for soil moisture predictions.
"""

from prefect import flow, task
from prefect.tasks import task_input_hash
from typing import Dict, Any, Optional, List
import numpy as np
from datetime import datetime, timedelta
import os
import logging
import traceback
from pydantic import Field

from gaia.tasks.base.task import MinerTask
from gaia.tasks.defined_tasks.soilmoisture.protocol import (
    SoilMoisturePayload,
    SoilMoisturePrediction,
    SoilResponseData,
    SoilRequestData,
    SoilMeasurement
)
from gaia.tasks.defined_tasks.soilmoisture.preprocessing import SoilMinerPreprocessing
from gaia.tasks.defined_tasks.soilmoisture.models import SoilModel
from gaia.parallel.core.executor import get_task_runner
from gaia.parallel.config.settings import NodeType, TaskType

logger = logging.getLogger(__name__)

class SoilMinerTaskFlow(MinerTask):
    """Miner task flow for soil moisture prediction using satellite and weather data."""

    prediction_horizon: timedelta = Field(
        default_factory=lambda: timedelta(hours=6),
        description="Prediction horizon for the task",
    )
    scoring_delay: timedelta = Field(
        default_factory=lambda: timedelta(days=3),
        description="Delay before scoring due to SMAP data latency",
    )

    miner_preprocessing: Optional[SoilMinerPreprocessing] = None
    model: Optional[SoilModel] = None
    use_raw_preprocessing: bool = Field(default=False)
    test_mode: bool = Field(default=False)

    def __init__(self, **kwargs):
        """Initialize the soil miner task flow."""
        super().__init__(
            name="SoilMoistureTask",
            description="Soil moisture prediction task",
            schema_path="gaia/tasks/defined_tasks/soilmoisture/schema.json",
            **kwargs
        )
        
        self.test_mode = kwargs.get('test_mode', False)
        self._initialize_miner_components()

    def _initialize_miner_components(self):
        """Initialize miner-specific components."""
        self.miner_preprocessing = SoilMinerPreprocessing(task=self)

        # Try loading custom model if available
        custom_model_path = "gaia/models/custom_models/custom_soil_model.py"
        if os.path.exists(custom_model_path):
            try:
                import importlib.util
                spec = importlib.util.spec_from_file_location("custom_soil_model", custom_model_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self.model = module.CustomSoilModel()
                self.use_raw_preprocessing = True
                logger.info("Initialized custom soil model")
            except Exception as e:
                logger.error(f"Failed to load custom model: {str(e)}")
                self.model = self.miner_preprocessing.model
                self.use_raw_preprocessing = False
        else:
            self.model = self.miner_preprocessing.model
            self.use_raw_preprocessing = False
            logger.info("Initialized base soil model")

        logger.info("Initialized miner components for SoilMoistureTask")

    @task(retries=2)
    async def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process input data for model inference."""
        try:
            return await self.miner_preprocessing.process_miner_data(data)
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            raise

    @task(retries=2)
    async def generate_predictions(self, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate predictions using the model."""
        try:
            if hasattr(self.model, "run_inference"):
                predictions = self.model.run_inference(processed_data)  # Custom model
            else:
                predictions = self.run_model_inference(processed_data)  # Base model

            # Log prediction statistics
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

            return predictions
        except Exception as e:
            logger.error(f"Error generating predictions: {str(e)}")
            raise

    @flow(
        name="miner_prepare",
        retries=2,
        task_runner=get_task_runner(
            node_type=NodeType.MINER,
            task_type=TaskType.SOIL_MOISTURE
        )
    )
    async def prepare_flow(self, **kwargs) -> Dict[str, Any]:
        """
        Preprocess input data.
        Implements the abstract method from MinerTask.
        """
        try:
            data = kwargs.get("data", {})
            if not data:
                raise ValueError("No data provided for preprocessing")

            # Validate input using protocol
            SoilMoisturePayload(**data)

            # Process the data
            processed_data = await self.process_data.submit(data)
            
            return {
                "processed_data": processed_data,
                "region_id": data.get("region_id"),
                "target_time": data.get("target_time"),
                "sentinel_bounds": data.get("sentinel_bounds"),
                "sentinel_crs": data.get("sentinel_crs")
            }
        except Exception as e:
            logger.error(f"Error in prepare flow: {str(e)}")
            logger.error(traceback.format_exc())
            await self._handle_error(e)
            raise

    @flow(
        name="miner_execute",
        retries=2,
        task_runner=get_task_runner(
            node_type=NodeType.MINER,
            task_type=TaskType.SOIL_MOISTURE
        )
    )
    async def execute_flow(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute computation logic.
        Implements the abstract method from MinerTask.
        """
        try:
            # Generate predictions
            predictions = await self.generate_predictions.submit(inputs["processed_data"])

            # Format for output
            target_time = inputs["target_time"]
            if isinstance(target_time, str):
                target_time = datetime.fromisoformat(target_time)
            elif not isinstance(target_time, datetime):
                raise ValueError(f"Invalid target_time format: {target_time}")

            prediction_time = self.get_next_preparation_time(target_time)

            # Create response using protocol models
            prediction = SoilMoisturePrediction(
                surface_sm=predictions["surface"],
                rootzone_sm=predictions["rootzone"],
                uncertainty_surface=None,
                uncertainty_rootzone=None,
                sentinel_bounds=inputs["sentinel_bounds"],
                sentinel_crs=inputs["sentinel_crs"],
                target_time=prediction_time
            )

            # Validate prediction using protocol
            if not SoilMoisturePrediction.validate_prediction(prediction.dict()):
                raise ValueError("Invalid prediction format")

            return prediction.dict()

        except Exception as e:
            logger.error(f"Error in execute flow: {str(e)}")
            logger.error(traceback.format_exc())
            await self._handle_error(e)
            raise

    def run_model_inference(self, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run model inference on processed data."""
        if not self.model:
            raise RuntimeError("Model not initialized. Are you running on a miner node?")
        return self.model.predict(processed_data)

    def get_next_preparation_time(self, target_time: datetime) -> datetime:
        """Calculate the next preparation time based on target time."""
        return target_time + self.prediction_horizon
