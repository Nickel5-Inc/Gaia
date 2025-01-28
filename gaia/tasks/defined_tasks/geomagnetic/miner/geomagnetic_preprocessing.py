from gaia.tasks.base.components.preprocessing import Preprocessing
import pandas as pd
import numpy as np
from gaia.models.geomag_basemodel import GeoMagBaseModel
from typing import Dict, Any, Optional
from prefect import task
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class GeomagneticPreprocessing(Preprocessing):
    """
    Preprocessing component for geomagnetic data.
    Handles data cleaning, normalization, and preparation for model input.
    """

    @task(
        name="preprocess_base",
        retries=2,
        retry_delay_seconds=15,
        description="Basic preprocessing of geomagnetic data"
    )
    def preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares the cleaned DST DataFrame for prediction.

        Args:
            data (pd.DataFrame): The recent DST data provided by the validator

        Returns:
            pd.DataFrame: Processed data, if further processing is needed
            
        Raises:
            ValueError: If data format is invalid
        """
        try:
            if not isinstance(data, pd.DataFrame):
                raise ValueError("Input must be a pandas DataFrame")
                
            # Perform any necessary preprocessing (if required)
            return data
        except Exception as e:
            logger.error(f"Error in preprocess: {e}")
            raise

    @task(
        name="predict_next_hour",
        retries=2,
        retry_delay_seconds=15,
        description="Predict DST value for next hour"
    )
    def predict_next_hour(self, processed_data: pd.DataFrame, model: Optional[Any] = None) -> Dict[str, Any]:
        """
        Predicts the DST index for the next hour using the specified model.
        If no model is provided, uses the base model from geomag_basemodel.

        Args:
            processed_data (pd.DataFrame): The DataFrame with recent DST values
            model (object, optional): A custom model object with a `predict` method

        Returns:
            Dict[str, Any]: Predicted DST value and timestamp in UTC
            
        Raises:
            Exception: If prediction fails
        """
        try:
            # Use the provided model, or fall back to the base model if none is provided
            if model is None:
                model = GeoMagBaseModel()  # Initialize the base model

            # Ensure the timestamp is in UTC format
            last_timestamp = processed_data["timestamp"].iloc[-1]
            last_timestamp_utc = (
                last_timestamp.tz_convert("UTC")
                if last_timestamp.tzinfo
                else last_timestamp
            )

            # Assume the model has a `predict` method that takes the processed data
            prediction = model.predict(processed_data)
            
            if np.isnan(prediction) or np.isinf(prediction):
                logger.warning("Model returned invalid prediction, using fallback")
                prediction = float(processed_data["value"].iloc[-1])

            return {
                "predicted_value": float(prediction), 
                "timestamp": last_timestamp_utc
            }
            
        except Exception as e:
            logger.error(f"Error in predict_next_hour: {e}")
            raise

    @task(
        name="process_miner_data",
        retries=2,
        retry_delay_seconds=15,
        description="Process raw geomagnetic data for model input"
    )
    def process_miner_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Process raw geomagnetic data for model input.

        Args:
            data (pd.DataFrame): DataFrame containing:
                - timestamp: UTC timestamp
                - value: DST value
                
        Returns:
            pd.DataFrame: Processed data ready for model input with historical context
            
        Raises:
            ValueError: If data format is invalid
            Exception: For other processing errors
        """
        try:
            # Ensure data is a DataFrame
            if not isinstance(data, pd.DataFrame):
                raise ValueError("Input must be a pandas DataFrame")

            # Create a copy and convert timestamp to naive UTC datetime
            processed_df = data.copy()

            # Convert timestamps to pandas datetime and remove timezone
            processed_df["timestamp"] = pd.to_datetime(processed_df["timestamp"])
            if processed_df["timestamp"].dt.tz is not None:
                processed_df["timestamp"] = (
                    processed_df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
                )

            # Get current values (now in naive UTC)
            current_timestamp = processed_df["timestamp"].iloc[-1]
            current_value = processed_df["value"].iloc[-1]

            # Create historical points using proper pandas datetime handling
            historical_data = pd.DataFrame(
                {
                    "timestamp": [
                        current_timestamp - pd.Timedelta(hours=i) for i in range(1, 4)
                    ],
                    "value": [current_value] * 3,
                }
            )

            # Combine historical and current data
            processed_df = pd.concat([historical_data, processed_df], ignore_index=True)

            # Normalize values
            processed_df["value"] = processed_df["value"] / 100.0

            # Rename columns to match Prophet requirements
            processed_df = processed_df.rename(
                columns={"timestamp": "ds", "value": "y"}
            )

            # Sort by timestamp
            processed_df = processed_df.sort_values("ds").reset_index(drop=True)

            logger.info(f"Processed {len(processed_df)} records for model input")
            return processed_df

        except Exception as e:
            logger.error(f"Error in process_miner_data: {e}")
            raise
