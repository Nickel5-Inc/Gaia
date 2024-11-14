import pandas as pd
from tasks.base.components.inputs import Inputs


class GeomagneticInputs(Inputs):
    def load_data(self, data: pd.DataFrame):
        """
        Accepts a preprocessed DataFrame containing geomagnetic data.

        Args:
            data (pd.DataFrame): The preprocessed data.

        Returns:
            pd.DataFrame: Validated and loaded data.
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Data must be provided as a pandas DataFrame.")

        print("Data loaded successfully.")
        return data

    def validate_data(self, data: pd.DataFrame):
        """
        Validates the geomagnetic data DataFrame to ensure it meets requirements.

        Checks that the DataFrame contains necessary columns and is not empty.

        Args:
            data (pd.DataFrame): The DataFrame to validate.

        Returns:
            bool: True if validation is successful, False otherwise.
        """
        if data.empty:
            print("Validation failed: DataFrame is empty.")
            return False

        required_columns = ["timestamp", "value"]  # Example required columns
        if not all(column in data.columns for column in required_columns):
            print(f"Validation failed: Missing required columns {required_columns}.")
            return False

        print("Data validation successful.")
        return True

