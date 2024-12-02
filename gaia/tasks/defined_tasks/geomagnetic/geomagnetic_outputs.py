import numpy as np
from typing import Any, Dict
from pydantic import Field
from gaia.tasks.base.components.outputs import Outputs


class GeomagneticOutputs(Outputs):
    outputs: Dict[str, Any] = Field(
        default_factory=dict,
        description="A dictionary to store the outputs of the geomagnetic task.",
    )

    def save_results(self, results):
        """
        Simply returns the formatted results without saving to a file or database.

        Args:
            results (list or np.ndarray): The results data to format.

        Returns:
            np.ndarray: Formatted results as a numpy array of integers.
        """
        return self.format_results(results)

    def format_results(self, results):
        """
        Formats the results as a numpy array of integers for consistency.

        Args:
            results (list or np.ndarray): The raw results data.

        Returns:
            np.ndarray: The formatted results as a numpy array of integers.
        """
        # Ensure results are formatted as an integer numpy array
        if isinstance(results, list):
            results = np.array(results, dtype=int)
        elif isinstance(results, np.ndarray):
            results = results.astype(int)
        else:
            raise ValueError("Results must be a list or numpy array.")

        return results

    def validate_outputs(self, outputs):
        """
        Validates the outputs generated by the geomagnetic task.

        Args:
            outputs (dict): The outputs to validate. Example:
                {
                    "predictions": list or np.ndarray,
                    "metadata": dict
                }

        Returns:
            bool: True if validation is successful, False otherwise.
        """
        if not isinstance(outputs, dict):
            raise ValueError("Outputs must be a dictionary.")

        # Ensure required keys exist
        required_keys = ["predictions", "metadata"]
        if not all(key in outputs for key in required_keys):
            raise ValueError(
                f"Outputs are missing required keys. Expected keys: {required_keys}"
            )

        # Validate predictions
        predictions = outputs.get("predictions")
        if not isinstance(predictions, (list, np.ndarray)):
            raise ValueError("Predictions must be a list or numpy array.")
        if not all(isinstance(p, (int, float)) for p in predictions):
            raise ValueError("All predictions must be numeric.")

        # Validate metadata
        metadata = outputs.get("metadata")
        if not isinstance(metadata, dict):
            raise ValueError("Metadata must be a dictionary.")

        print("Outputs validation successful.")
        return True
