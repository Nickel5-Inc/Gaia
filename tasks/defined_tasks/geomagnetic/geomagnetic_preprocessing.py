from tasks.base.components.preprocessing import Preprocessing
import pandas as pd
from models.geomag_basemodel import GeoMagBaseModel

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
            int or None: Predicted DST value for the next hour, or None if an error occurs.
        """
        # Use the provided model, or fall back to the base model if none is provided
        if model is None:
            model = GeoMagBaseModel()  # Initialize the base model

        try:
            # Assume the model has a `predict` method that takes the processed data
            prediction = model.predict(processed_data)
            return int(prediction)
        except Exception as e:
            print(f"Error in model prediction: {e}")
            return None
