from gaia.tasks.base.components.preprocessing import Preprocessing
import pandas as pd
from gaia.models.geomag_basemodel import GeoMagBaseModel


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
