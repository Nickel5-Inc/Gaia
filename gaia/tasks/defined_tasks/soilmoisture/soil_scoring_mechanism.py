import numpy as np
from typing import Dict, Tuple, Optional
import rasterio
import matplotlib.pyplot as plt

class SoilScoringMechanism:
    """
    A class to evaluate soil moisture predictions from miners against ground truth.
    """

    def __init__(self, normalize_score: bool = True, max_score: float = 60, baseline_rmse: float = 50):
        """
        Initialize the scoring mechanism.
        Args:
            normalize_score (bool): Whether to normalize the RMSE to a score.
            max_score (float): Maximum possible score (lower RMSE gets higher score).
            baseline_rmse (float): RMSE corresponding to the baseline performance.
        """
        self.normalize_score = normalize_score
        self.max_score = max_score
        self.baseline_rmse = baseline_rmse

    def calculate_rmse(self, predictions: np.ndarray, ground_truth: np.ndarray) -> float:
        """
        Calculate Root Mean Square Error (RMSE).
        Args:
            predictions (np.ndarray): Miner predictions.
            ground_truth (np.ndarray): Ground truth data.
        Returns:
            float: RMSE value.
        """
        valid_mask = ~np.isnan(predictions) & ~np.isnan(ground_truth)
        predictions = predictions[valid_mask]
        ground_truth = ground_truth[valid_mask]
        return np.sqrt(np.mean((predictions - ground_truth) ** 2))

    def calculate_score(self, rmse: float) -> float:
        """
        Normalize RMSE to a score.
        Args:
            rmse (float): Calculated RMSE.
        Returns:
            float: Normalized score.
        """
        if not self.normalize_score:
            return rmse
        return max(self.max_score - (rmse / self.baseline_rmse) * self.max_score, 0)

    def score(self, predictions: np.ndarray, ground_truth: np.ndarray) -> Dict[str, float]:
        """
        Compute RMSE and normalized score.
        Args:
            predictions (np.ndarray): Miner predictions.
            ground_truth (np.ndarray): Ground truth data.
        Returns:
            Dict[str, float]: Dictionary with RMSE and score.
        """
        rmse = self.calculate_rmse(predictions, ground_truth)
        score = self.calculate_score(rmse)
        return {"rmse": rmse, "score": score}

    def visualize_error(self, predictions: np.ndarray, ground_truth: np.ndarray, save_path: Optional[str] = None):
        """
        Generate a heatmap of spatial errors.
        Args:
            predictions (np.ndarray): Miner predictions.
            ground_truth (np.ndarray): Ground truth data.
            save_path (str): Optional path to save the visualization.
        """
        error = predictions - ground_truth
        plt.imshow(error, cmap='coolwarm', interpolation='nearest')
        plt.colorbar(label='Error (Predicted - Ground Truth)')
        plt.title("Spatial Error Heatmap")
        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()

    def process_tif_files(self, pred_file: str, truth_file: str) -> Tuple[np.ndarray, np.ndarray]:
        """
        Load prediction and ground truth data from .tif files.
        Args:
            pred_file (str): Path to prediction .tif file.
            truth_file (str): Path to ground truth .tif file.
        Returns:
            Tuple[np.ndarray, np.ndarray]: Aligned arrays of predictions and ground truth.
        """
        with rasterio.open(pred_file) as pred_src, rasterio.open(truth_file) as truth_src:
            pred_data = pred_src.read(1)
            truth_data = truth_src.read(1)
            # Optionally, reproject or crop to ensure alignment
        return pred_data, truth_data
