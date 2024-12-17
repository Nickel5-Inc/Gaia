from datetime import datetime, timezone
from prefect import flow, task, get_run_logger
from typing import Dict, List, Optional, Tuple, Any, Union
import numpy as np
import torch
from ..soil_scoring_mechanism import SoilScoringMechanism
from .smap_data_flow import smap_data_flow

logger = get_run_logger()

@task(
    name="prepare_predictions_task",
    retries=2,
    tags=["preprocessing", "scoring"],
    description="Prepare predictions for scoring"
)
async def prepare_predictions_task(predictions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Prepare and validate predictions for scoring.
    """
    try:
        logger.info("Preparing predictions for scoring")
        
        # Extract and validate prediction data
        pred_data = predictions.get("predictions")
        if pred_data is None:
            logger.error("No predictions found in input")
            return None
            
        if isinstance(pred_data, dict):
            try:
                surface_sm = torch.tensor(pred_data["surface_sm"])
                rootzone_sm = torch.tensor(pred_data["rootzone_sm"])
                model_predictions = torch.stack([surface_sm, rootzone_sm], dim=0).unsqueeze(0)
            except Exception as e:
                logger.error(f"Error converting predictions to tensor: {str(e)}")
                return None
        else:
            model_predictions = pred_data
            
        predictions["predictions"] = model_predictions
        logger.info(f"Successfully prepared predictions with shape: {model_predictions.shape}")
        return predictions
        
    except Exception as e:
        logger.error(f"Error preparing predictions: {str(e)}")
        raise

@task(
    name="compute_metrics_task",
    retries=2,
    tags=["scoring"],
    description="Compute scoring metrics"
)
async def compute_metrics_task(
    predictions: Dict[str, Any],
    ground_truth: Dict[str, Any]
) -> Optional[Dict[str, float]]:
    """
    Compute RMSE and SSIM metrics for predictions.
    """
    try:
        logger.info("Computing scoring metrics")
        
        # Initialize scoring mechanism
        scoring_mechanism = SoilScoringMechanism()
        
        # Prepare data for scoring
        model_predictions = predictions["predictions"]
        surface_truth = torch.tensor(ground_truth["surface_sm"]).unsqueeze(0).unsqueeze(0)
        rootzone_truth = torch.tensor(ground_truth["rootzone_sm"]).unsqueeze(0).unsqueeze(0)
        
        # Compute metrics
        metrics = {
            "validation_metrics": {}
        }
        
        # Surface metrics
        surface_mask = ~torch.isnan(surface_truth[0, 0])
        if surface_mask.any():
            valid_surface_pred = model_predictions[0, 0][surface_mask]
            valid_surface_truth = surface_truth[0, 0][surface_mask]
            surface_rmse = torch.sqrt(torch.nn.functional.mse_loss(valid_surface_pred, valid_surface_truth))
            metrics["validation_metrics"]["surface_rmse"] = surface_rmse.item()
            
            # Compute SSIM if possible
            if valid_surface_pred.numel() > 9:  # Minimum size for SSIM
                surface_ssim = torch.nn.functional.mse_loss(valid_surface_pred, valid_surface_truth)
                metrics["validation_metrics"]["surface_ssim"] = surface_ssim.item()
                
        # Rootzone metrics
        rootzone_mask = ~torch.isnan(rootzone_truth[0, 0])
        if rootzone_mask.any():
            valid_rootzone_pred = model_predictions[0, 1][rootzone_mask]
            valid_rootzone_truth = rootzone_truth[0, 0][rootzone_mask]
            rootzone_rmse = torch.sqrt(torch.nn.functional.mse_loss(valid_rootzone_pred, valid_rootzone_truth))
            metrics["validation_metrics"]["rootzone_rmse"] = rootzone_rmse.item()
            
            # Compute SSIM if possible
            if valid_rootzone_pred.numel() > 9:  # Minimum size for SSIM
                rootzone_ssim = torch.nn.functional.mse_loss(valid_rootzone_pred, valid_rootzone_truth)
                metrics["validation_metrics"]["rootzone_ssim"] = rootzone_ssim.item()
                
        logger.info(f"Computed metrics: {metrics['validation_metrics']}")
        return metrics
        
    except Exception as e:
        logger.error(f"Error computing metrics: {str(e)}")
        raise

@task(
    name="compute_final_score_task",
    retries=2,
    tags=["scoring"],
    description="Compute final score from metrics"
)
async def compute_final_score_task(metrics: Dict[str, Any]) -> Optional[float]:
    """
    Compute final score from metrics using sigmoid function.
    """
    try:
        logger.info("Computing final score")
        
        scoring_mechanism = SoilScoringMechanism()
        final_score = scoring_mechanism.compute_final_score(metrics)
        
        if not 0 <= final_score <= 1:
            logger.error(f"Final score {final_score} outside valid range [0,1]")
            return None
            
        logger.info(f"Computed final score: {final_score:.4f}")
        return final_score
        
    except Exception as e:
        logger.error(f"Error computing final score: {str(e)}")
        raise

@flow(
    name="soil_scoring_flow",
    retries=3,
    description="Orchestrates the scoring of soil moisture predictions"
)
async def soil_scoring_flow(
    predictions: Dict[str, Any],
    bounds: Tuple[float, float, float, float],
    crs: str,
    target_time: datetime
) -> Optional[Dict[str, Any]]:
    """
    Main flow for scoring soil moisture predictions.
    """
    try:
        # Prepare predictions
        prepared_predictions = await prepare_predictions_task(predictions)
        if not prepared_predictions:
            raise ValueError("Failed to prepare predictions")
            
        # Get ground truth data
        ground_truth = await smap_data_flow(target_time, bounds, crs)
        if not ground_truth:
            raise ValueError("Failed to get ground truth data")
            
        # Compute metrics
        metrics = await compute_metrics_task(prepared_predictions, ground_truth)
        if not metrics:
            raise ValueError("Failed to compute metrics")
            
        # Compute final score
        final_score = await compute_final_score_task(metrics)
        if final_score is None:
            raise ValueError("Failed to compute final score")
            
        # Prepare result
        result = {
            "miner_id": predictions.get("miner_id"),
            "miner_hotkey": predictions.get("miner_hotkey"),
            "metrics": metrics["validation_metrics"],
            "total_score": final_score,
            "timestamp": target_time.isoformat(),
            "ground_truth": ground_truth
        }
        
        logger.info(f"Successfully completed scoring flow: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error in scoring flow: {str(e)}")
        raise 