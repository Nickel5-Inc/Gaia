import numpy as np
from tasks.base.components.outputs import Outputs


class GeomagneticOutputs(Outputs):
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
