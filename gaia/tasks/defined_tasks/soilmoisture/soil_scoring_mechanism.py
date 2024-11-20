from gaia.tasks.base.components.scoring_mechanism import ScoringMechanism
from gaia.tasks.base.decorators import task_timer
import numpy as np
from typing import Dict, Optional
import matplotlib.pyplot as plt

class SoilScoringMechanism(ScoringMechanism):
    """Scoring mechanism for soil moisture predictions."""
    
    def __init__(self, baseline_rmse: float = 50):
        super().__init__(
            name="SoilMoistureScoringMechanism",
            description="Evaluates soil moisture predictions using RMSE",
            normalize_score=True,
            max_score=60.0
        )
        self._baseline_rmse = baseline_rmse

    def calculate_rmse(self, predictions: np.ndarray, ground_truth: np.ndarray) -> float:
        """Calculate Root Mean Square Error (RMSE)."""
        valid_mask = ~np.isnan(predictions) & ~np.isnan(ground_truth)
        predictions = predictions[valid_mask]
        ground_truth = ground_truth[valid_mask]
        return np.sqrt(np.mean((predictions - ground_truth) ** 2))

    def calculate_normalized_score(self, rmse: float) -> float:
        """Normalize RMSE to a score."""
        if not self.normalize_score:
            return rmse
        return max(self.max_score - (rmse / self._baseline_rmse) * self.max_score, 0)

    @task_timer
    def score(self, predictions: np.ndarray, ground_truth: np.ndarray) -> Dict[str, float]:
        """Score predictions against ground truth."""
        rmse = self.calculate_rmse(predictions, ground_truth)
        score = self.calculate_normalized_score(rmse)
        return {"rmse": rmse, "score": score}

    def visualize_error(self, predictions: np.ndarray, ground_truth: np.ndarray, save_path: Optional[str] = None):
        """Generate spatial error heatmap."""
        error = predictions - ground_truth
        plt.imshow(error, cmap='coolwarm', interpolation='nearest')
        plt.colorbar(label='Error (Predicted - Ground Truth)')
        plt.title("Spatial Error Heatmap")
        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()
