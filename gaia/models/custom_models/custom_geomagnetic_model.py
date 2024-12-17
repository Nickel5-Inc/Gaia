from typing import Dict, Any
import pandas as pd
import numpy as np

"""
TEMPLATE FOR CUSTOM GEOMAGNETIC MODEL

Implement your custom geomagnetic model in this file.
Add any methods as needed.

IMPORTANT: This class must:
1. Be named exactly 'CustomGeomagneticModel'
2. Reside in a file named exactly 'custom_geomagnetic_model.py'
3. Be placed in the 'Gaia/gaia/models/custom_models/' directory
4. Implement a method named exactly 'run_inference()' as specified below
"""

class CustomGeomagneticModel:
    def __init__(self):
        """Initialize your custom geomagnetic model."""
        self._load_model()

    def _load_model(self):
        """
        Load your model implementation.

        Use this method to load any resources needed for the model, such as
        machine learning weights, configuration files, or constants.
        """
        pass

    def run_inference(self, inputs: pd.DataFrame) -> float:
        """
        Required method that must be implemented to run model predictions.
        This method should call all necessary methods to preprocess data,
        and run the model end-to-end.

        Args:
            inputs (pd.DataFrame): Preprocessed input data for the model, containing:
                - `timestamp` (datetime): Datetime information (column can also be named `ds`).
                - `value` (float): Geomagnetic Dst index (column can also be named `y`).

        Returns:
            float: Predicted Dst value for the next hour.

        Example:
            model = CustomGeomagneticModel()
            predictions = model.run_inference(
                pd.DataFrame({
                    "timestamp": [datetime(2024, 12, 10, 0, 0), datetime(2024, 12, 10, 1, 0)],
                    "value": [-20, -25]
                })
            )
            print(predictions)  # Output: -24.5
        """
        pass


"""
INPUT DATA:
The input to run_inference() must be a pandas DataFrame with the following structure:
- `timestamp` (or `ds`): Datetime information (datetime or string).
- `value` (or `y`): Geomagnetic Dst index values (float).

Example:
pd.DataFrame({
    "timestamp": [datetime(2024, 12, 10, 0, 0), datetime(2024, 12, 10, 1, 0)],
    "value": [-20, -25]
})

OUTPUT DATA:
The output of run_inference() must be a single float representing the predicted Dst index.

Example:
-25.0
"""
