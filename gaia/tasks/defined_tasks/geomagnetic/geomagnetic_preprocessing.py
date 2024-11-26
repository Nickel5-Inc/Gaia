from gaia.tasks.base.components.preprocessing import Preprocessing
import pandas as pd
from gaia.models.geomag_basemodel import GeoMagBaseModel
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class GeomagneticPreprocessing(Preprocessing):
    def preprocess(self, data: pd.DataFrame):
        """
        Prepares the cleaned DST DataFrame for prediction.

        Args:
            data (pd.DataFrame): The recent DST data provided by the validator.

        Returns:
            pd.DataFrame: Processed data, if further processing is needed.
        """
        # Perform any necessary preprocessing (if required)
        return data

    def predict_next_hour(self, processed_data: pd.DataFrame, model=None):
        """
        Predicts the DST index for the next hour using the specified model.

        If no model is provided, uses the base model from geomag_basemodel.

        Args:
            processed_data (pd.DataFrame): The DataFrame with recent DST values.
            model (object, optional): A custom model object with a `predict` method.

        Returns:
            dict: Predicted DST value and timestamp in UTC.
        """
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

        try:
            # Assume the model has a `predict` method that takes the processed data
            prediction = model.predict(processed_data)
        except Exception as e:
            print(f"Error in model prediction: {e}")
            return None

        return {"predicted_value": int(prediction), "timestamp": last_timestamp_utc}

    def process_miner_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Process raw geomagnetic data for model input.
        
        Args:
            data (pd.DataFrame): DataFrame containing:
                - timestamp: UTC timestamp
                - value: DST value
        Returns:
            pd.DataFrame: Processed data ready for model input with historical context
        """
        try:
            # Ensure data is a DataFrame
            if not isinstance(data, pd.DataFrame):
                raise ValueError("Input must be a pandas DataFrame")
            
            # Create a copy to avoid modifying the original
            processed_df = data.copy()
            
            # Add historical context - create synthetic data points for the past few hours
            current_timestamp = pd.to_datetime(processed_df['timestamp'].iloc[-1])
            current_value = processed_df['value'].iloc[-1]
            
            # Create historical points (last 3 hours)
            historical_data = []
            for i in range(1, 4):
                historical_data.append({
                    'timestamp': current_timestamp - pd.Timedelta(hours=i),
                    'value': current_value  # Use current value as a simple baseline
                })
            
            # Combine historical and current data
            historical_df = pd.DataFrame(historical_data)
            processed_df = pd.concat([historical_df, processed_df], ignore_index=True)
            
            # Normalize values
            processed_df['value'] = processed_df['value'] / 100.0
            
            # Rename columns to match Prophet requirements
            processed_df = processed_df.rename(columns={
                'timestamp': 'ds',
                'value': 'y'
            })
            
            # Ensure timestamp is datetime
            processed_df['ds'] = pd.to_datetime(processed_df['ds'])
            
            # Sort by timestamp
            processed_df = processed_df.sort_values('ds')
            
            return processed_df
            
        except Exception as e:
            logger.error(f"Error in process_miner_data: {e}")
            raise
