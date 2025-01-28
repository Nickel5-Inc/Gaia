import traceback
from typing import Any, Dict, Optional
import os
import importlib.util
import datetime
import numpy as np
import pandas as pd
from prefect import flow, task
from pydantic import Field

from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.tasks.base.task import Task
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_metadata import GeomagneticMetadata
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_inputs import GeomagneticInputs
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import GeomagneticPreprocessing
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_outputs import GeomagneticOutputs
from gaia.models.geomag_basemodel import GeoMagBaseModel
from gaia.parallel.core.executor import get_task_runner
from gaia.parallel.config.settings import NodeType, TaskType
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class GeomagneticMinerTask(Task):
    """
    Miner-specific task for geomagnetic predictions.
    Handles model loading, data preprocessing, and inference.

    This task involves:
        - Loading and managing the prediction model
        - Preprocessing incoming data
        - Running model inference
        - Formatting predictions for response

    Attributes:
        name (str): The name of the task
        description (str): A description of the task's purpose
        task_type (str): Specifies the type of task
        preprocessing (GeomagneticPreprocessing): Processes raw data
        model (GeoMagBaseModel): The prediction model
    """

    db_manager: MinerDatabaseManager = Field(
        default_factory=MinerDatabaseManager,
        description="Database manager for the miner task",
    )
    preprocessing: GeomagneticPreprocessing = Field(
        default_factory=GeomagneticPreprocessing,
        description="Preprocessing component for miner",
    )
    model: Optional[GeoMagBaseModel] = Field(
        default=None,
        description="The geomagnetic prediction model"
    )

    def __init__(self, db_manager, **data):
        """Initialize the miner task."""
        super().__init__(
            name="GeomagneticMinerTask",
            description="Geomagnetic prediction task for miners",
            task_type="atomic",
            metadata=GeomagneticMetadata(),
            inputs=GeomagneticInputs(),
            outputs=GeomagneticOutputs(),
            db_manager=db_manager,
            **data,
        )

    @task(
        name="load_model",
        retries=2,
        retry_delay_seconds=30,
        description="Load the geomagnetic prediction model"
    )
    def load_model(self) -> None:
        """Load either custom or base model for predictions."""
        try:
            logger.info("Loading geomagnetic model...")
            # Try to load custom model first
            custom_model_path = "gaia/models/custom_models/custom_geomagnetic_model.py"
            if os.path.exists(custom_model_path):
                spec = importlib.util.spec_from_file_location(
                    "custom_geomagnetic_model", custom_model_path
                )
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self.model = module.CustomGeomagneticModel()
                logger.info("Successfully loaded custom geomagnetic model")
            else:
                # Fall back to base model
                self.model = GeoMagBaseModel()
                logger.info("No custom model found, using base model")
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            logger.error(traceback.format_exc())
            raise

    @task(
        name="preprocess_data",
        retries=2,
        retry_delay_seconds=15,
        description="Preprocess raw geomagnetic data"
    )
    def preprocess_data(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Preprocess raw geomagnetic data for model input.

        Args:
            raw_data (Dict[str, Any]): Raw input data

        Returns:
            pd.DataFrame: Preprocessed data ready for prediction
            
        Raises:
            ValueError: If data format is invalid
        """
        try:
            if not raw_data or not raw_data.get("data"):
                raise ValueError("Invalid data format")

            # Process current data
            input_data = pd.DataFrame({
                "timestamp": [pd.to_datetime(raw_data["data"]["timestamp"])],
                "value": [float(raw_data["data"]["value"])],
            })

            # Process historical data if available
            if raw_data["data"].get("historical_values"):
                historical_df = pd.DataFrame(raw_data["data"]["historical_values"])
                historical_df = historical_df.rename(columns={"Dst": "value"})
                historical_df["timestamp"] = pd.to_datetime(historical_df["timestamp"])
                historical_df = historical_df[["timestamp", "value"]]
                combined_df = pd.concat([historical_df, input_data], ignore_index=True)
            else:
                combined_df = input_data

            return self.preprocessing.process_miner_data(combined_df)
        except Exception as e:
            logger.error(f"Error preprocessing data: {e}")
            raise

    @task(
        name="run_inference",
        retries=2,
        retry_delay_seconds=15,
        description="Run model inference on preprocessed data"
    )
    def run_inference(self, processed_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Run model inference on preprocessed data.

        Args:
            processed_data (pd.DataFrame): Preprocessed input data

        Returns:
            Dict[str, Any]: Prediction results
            
        Raises:
            Exception: If there's an error during inference
        """
        try:
            if hasattr(self.model, "run_inference"):
                logger.info("Using custom model inference")
                predictions = self.model.run_inference(processed_data)
            else:
                logger.info("Using base model inference")
                prediction = self.model.predict(processed_data)
                
                # Handle NaN or infinite values
                if np.isnan(prediction) or np.isinf(prediction):
                    logger.warning("Model returned NaN/Inf, using fallback value")
                    prediction = float(processed_data["value"].iloc[-1])

                predictions = {
                    "predicted_value": float(prediction),
                    "prediction_time": processed_data.index[-1]
                }

            return predictions
        except Exception as e:
            logger.error(f"Error during model inference: {e}")
            raise

    @flow(name="miner_prepare", retries=2)
    async def prepare_flow(self) -> Dict[str, Any]:
        """
        Prepare the miner task by loading model and setting up task runner.
        
        Returns:
            Dict[str, Any]: Preparation status and task runner
        """
        try:
            if self.model is None:
                await self.load_model()
            
            task_runner = get_task_runner(NodeType.MINER, TaskType.GEOMAGNETIC)
            logger.info(f"Initialized task runner with type: {task_runner.__class__.__name__}")
            
            return {"status": "ready", "task_runner": task_runner}
        except Exception as e:
            logger.error(f"Error in prepare_flow: {e}")
            raise

    @flow(name="miner_execute", retries=2)
    async def execute_flow(self, data: Dict[str, Any], miner: Any) -> Dict[str, Any]:
        """
        Execute the miner prediction workflow.

        Args:
            data (Dict[str, Any]): Input data from validator
            miner (Any): Miner instance

        Returns:
            Dict[str, Any]: Prediction results with miner hotkey
        """
        try:
            # Get task runner
            prep_result = await self.prepare_flow()
            task_runner = prep_result["task_runner"]

            # Preprocess data with task runner
            processed_data_future = task_runner.submit(
                self.preprocess_data,
                data,
                pure=True,
                retries=2
            )
            processed_data = await processed_data_future

            # Run inference with task runner
            predictions_future = task_runner.submit(
                self.run_inference,
                processed_data,
                pure=True,
                retries=2
            )
            predictions = await predictions_future

            # Format response
            response = {
                "predicted_values": float(predictions.get("predicted_value", 0.0)),
                "timestamp": predictions.get("prediction_time", data["data"]["timestamp"]),
                "miner_hotkey": miner.keypair.ss58_address,
            }

            logger.info(f"Generated prediction: {response}")
            return response

        except Exception as e:
            logger.error(f"Error in execute_flow: {e}")
            raise

