import datetime
from distutils import core
import tempfile
from gaia.tasks.base.components.scoring_mechanism import ScoringMechanism
from gaia.tasks.base.decorators import task_timer
import numpy as np
from typing import Dict, Optional
from rasterio.coords import BoundingBox
from rasterio.crs import CRS
import torch
import torch.nn.functional as F
from torchmetrics.functional import structural_similarity_index_measure as ssim

from gaia.tasks.defined_tasks.soilmoisture.utils.smap_api import construct_smap_url, download_smap_data, get_smap_data_for_sentinel_bounds

class SoilScoringMechanism(ScoringMechanism):
    """Scoring mechanism for soil moisture predictions."""

    def __init__(self, baseline_rmse: float = 50):
        super().__init__(
            name="SoilMoistureScoringMechanism",
            description="Evaluates soil moisture predictions using RMSE and SSIM",
            normalize_score=True,
            max_score=60.0,
        )

    def calculate_normalized_score(self, rmse: float) -> float:
        """Normalize RMSE and SSIM to a score. 60% RMSE, 40% SSIM."""
        if not self.normalize_score:
            return rmse
        return max(self.max_score - (rmse / self._baseline_rmse) * self.max_score, 0)

    @task_timer
    def score(
        self, predictions: np.ndarray, ground_truth: np.ndarray
    ) -> Dict[str, float]:
        """Score predictions against ground truth."""

        pass

    def compute_smap_score_metrics(
        bounds: tuple[float, float, float, float],
        crs: float,
        model_predictions: torch.Tensor,
        target_date: datetime
    ) -> dict:
        """
        Compute RMSE and SSIM between model predictions and SMAP data for valid pixels only.
        """       
        device = model_predictions.device
        sentinel_bounds = BoundingBox(left=bounds[0], bottom=bounds[1], right=bounds[2], top=bounds[3])
        sentinel_crs = CRS.from_epsg(int(crs))

        smap_url = construct_smap_url(target_date)
        with tempfile.NamedTemporaryFile(suffix='.h5') as temp_file:
            if not download_smap_data(smap_url, temp_file.name):
                return None
            smap_data = get_smap_data_for_sentinel_bounds(
                temp_file.name,
                (sentinel_bounds.left, sentinel_bounds.bottom, 
                sentinel_bounds.right, sentinel_bounds.top),
                sentinel_crs.to_string()
            )
            if not smap_data:
                return None
                
            surface_sm = torch.from_numpy(smap_data['surface_sm']).float().unsqueeze(0).unsqueeze(0).to(device)
            rootzone_sm = torch.from_numpy(smap_data['rootzone_sm']).float().unsqueeze(0).unsqueeze(0).to(device)
            surface_sm_11x11 = F.interpolate(surface_sm, size=(11, 11), mode='bilinear', align_corners=False)
            rootzone_sm_11x11 = F.interpolate(rootzone_sm, size=(11, 11), mode='bilinear', align_corners=False)
            surface_mask_11x11 = ~torch.isnan(surface_sm_11x11[0,0])
            rootzone_mask_11x11 = ~torch.isnan(rootzone_sm_11x11[0,0])
            
            results = {'validation_metrics': {}}
            if surface_mask_11x11.any():
                valid_surface_pred = model_predictions[0,0][surface_mask_11x11]
                valid_surface_truth = surface_sm_11x11[0,0][surface_mask_11x11]
                surface_rmse = torch.sqrt(F.mse_loss(valid_surface_pred, valid_surface_truth))
                results['validation_metrics']['surface_rmse'] = surface_rmse.item()
                
                surface_pred_masked = torch.zeros_like(model_predictions[0:1,0:1])
                surface_truth_masked = torch.zeros_like(surface_sm_11x11)
                surface_pred_masked[0,0][surface_mask_11x11] = model_predictions[0,0][surface_mask_11x11]
                surface_truth_masked[0,0][surface_mask_11x11] = surface_sm_11x11[0,0][surface_mask_11x11]
                
                valid_min = torch.min(valid_surface_pred.min(), valid_surface_truth.min())
                valid_max = torch.max(valid_surface_pred.max(), valid_surface_truth.max())
                data_range = valid_max - valid_min
                
                if data_range > 0:
                    surface_ssim = ssim(
                        surface_pred_masked, 
                        surface_truth_masked,
                        data_range=data_range,
                        kernel_size=3
                    )
                    results['validation_metrics']['surface_ssim'] = surface_ssim.item()
            
            if rootzone_mask_11x11.any():
                valid_rootzone_pred = model_predictions[0,1][rootzone_mask_11x11]
                valid_rootzone_truth = rootzone_sm_11x11[0,0][rootzone_mask_11x11]
                rootzone_rmse = torch.sqrt(F.mse_loss(valid_rootzone_pred, valid_rootzone_truth))
                results['validation_metrics']['rootzone_rmse'] = rootzone_rmse.item()
                
                rootzone_pred_masked = torch.zeros_like(model_predictions[0:1,1:2])
                rootzone_truth_masked = torch.zeros_like(rootzone_sm_11x11)
                rootzone_pred_masked[0,0][rootzone_mask_11x11] = model_predictions[0,1][rootzone_mask_11x11]
                rootzone_truth_masked[0,0][rootzone_mask_11x11] = rootzone_sm_11x11[0,0][rootzone_mask_11x11]
                
                valid_min = torch.min(valid_rootzone_pred.min(), valid_rootzone_truth.min())
                valid_max = torch.max(valid_rootzone_pred.max(), valid_rootzone_truth.max())
                data_range = valid_max - valid_min
                
                if data_range > 0:
                    rootzone_ssim = ssim(
                        rootzone_pred_masked, 
                        rootzone_truth_masked,
                        data_range=data_range,
                        kernel_size=3
                    )
                    results['validation_metrics']['rootzone_ssim'] = rootzone_ssim.item()

            return results

#example use 
#validation_results = compute_smap_validation(
#    bounds=(499980.0, 5590200.0, 609780.0, 5700000.0),
#    crs=32647.0,
#    model_predictions=model_predictions,
#    target_date=target_date