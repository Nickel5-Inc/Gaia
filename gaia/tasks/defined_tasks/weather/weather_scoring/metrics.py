import numpy as np
import xarray as xr
import asyncio
from typing import Dict, Any, Union, Optional, Tuple, List
from fiber.logging_utils import get_logger
logger = get_logger(__name__)

"""
Core metric calculations for weather forecast evaluation.
This module provides implementations of common metrics for
evaluating weather forecasts, including RMSE, MAE, bias, correlation,
and spread statistics for ensemble forecasts.
"""

async def calculate_rmse(prediction: Dict, ground_truth: Dict) -> float:
    """
    Calculate overall RMSE across all variables.
    
    Args:
        prediction: Dictionary of predicted variables
        ground_truth: Dictionary of ground truth variables
    
    Returns:
        Root Mean Square Error value
    """
    try:
        total_squared_error = 0.0
        total_count = 0
        
        for var in prediction:
            if var in ground_truth:
                pred_data = prediction[var]
                truth_data = ground_truth[var]
                
                if pred_data.shape != truth_data.shape:
                    continue
                    
                squared_error = (pred_data - truth_data) ** 2
                total_squared_error += np.sum(squared_error)
                total_count += np.size(squared_error)
        
        if total_count > 0:
            return np.sqrt(total_squared_error / total_count)
        else:
            return float('inf')
    except Exception as e:
        logger.error(f"Error in calculate_rmse: {e}")
        raise

async def normalize_rmse(prediction: np.ndarray, ground_truth: np.ndarray, var_name: str) -> float:
    """
    Calculate normalized RMSE for a specific variable.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
        var_name: Variable name for appropriate normalization
    
    Returns:
        Normalized RMSE
    """
    try:
        scale_factors = {
            "2t": 5.0,      # ~5K is typical temperature error range
            "msl": 500.0,   # ~500Pa is typical pressure error range
            "10u": 3.0,     # ~3m/s is typical wind error range
            "10v": 3.0,
            "t": 5.0,
            "q": 0.001,     # ~0.001 kg/kg is typical humidity error range
            "z": 50.0,      # ~50m^2/s^2 is typical geopotential error range
            "u": 3.0,
            "v": 3.0
        }
        
        squared_error = (prediction - ground_truth) ** 2
        rmse = np.sqrt(np.mean(squared_error))
        
        scale = scale_factors.get(var_name, 1.0)
        return rmse / max(scale, 1e-6)
    except Exception as e:
        logger.error(f"Error in normalize_rmse for variable {var_name}: {e}")
        raise

async def calculate_mean_abs_diff(array1: np.ndarray, array2: np.ndarray) -> float:
    """
    Calculate mean absolute difference between two arrays.
    
    Args:
        array1: First array
        array2: Second array
    
    Returns:
        Mean absolute difference
    """
    try:
        if array1.shape != array2.shape:
            logger.warning(f"Shape mismatch in calculate_mean_abs_diff: {array1.shape} vs {array2.shape}")
            return float('inf')
            
        return np.mean(np.abs(array1 - array2))
    except Exception as e:
        logger.error(f"Error in calculate_mean_abs_diff: {e}")
        raise

async def calculate_mae(prediction: np.ndarray, ground_truth: np.ndarray) -> float:
    """
    Calculate Mean Absolute Error.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
    
    Returns:
        Mean Absolute Error
    """
    try:
        return np.mean(np.abs(prediction - ground_truth))
    except Exception as e:
        logger.error(f"Error in calculate_mae: {e}")
        raise

async def calculate_bias(prediction: np.ndarray, ground_truth: np.ndarray) -> float:
    """
    Calculate bias (mean error).
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
    
    Returns:
        Bias (positive means overprediction)
    """
    try:
        return np.mean(prediction - ground_truth)
    except Exception as e:
        logger.error(f"Error in calculate_bias: {e}")
        raise

async def calculate_correlation(prediction: np.ndarray, ground_truth: np.ndarray) -> float:
    """
    Calculate Pearson correlation coefficient.
    Handles NaN values by excluding them pairwise.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
    
    Returns:
        Correlation coefficient (-1 to 1)
    """
    try:
        pred_flat = prediction.flatten()
        truth_flat = ground_truth.flatten()
        
        if len(pred_flat) != len(truth_flat):
            logger.warning(f"Shape mismatch in calculate_correlation: {prediction.shape} vs {ground_truth.shape}")
            return 0.0

        valid_indices = ~(np.isnan(pred_flat) | np.isnan(truth_flat))
        pred_valid = pred_flat[valid_indices]
        truth_valid = truth_flat[valid_indices]
        
        if len(pred_valid) < 2:
            return 0.0
        
        if np.std(pred_valid) == 0 or np.std(truth_valid) == 0:
            return 0.0

        return np.corrcoef(pred_valid, truth_valid)[0, 1]
    except Exception as e:
        logger.error(f"Error in calculate_correlation: {e}")
        return 0.0

async def calculate_mse_decomposition(
    prediction: np.ndarray, 
    ground_truth: np.ndarray
) -> Dict[str, float]:
    """
    Decompose Mean Squared Error (MSE) into bias-squared and variance error components.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
    
    Returns:
        Dictionary containing MSE components (bias_squared, variance_error, phase_error, total_mse)
    """
    try:
        pred_flat = prediction.flatten()
        truth_flat = ground_truth.flatten()
        
        if len(pred_flat) != len(truth_flat):
            logger.warning(f"Shape mismatch in calculate_mse_decomposition: {prediction.shape} vs {ground_truth.shape}")
            return {
                "bias_squared": float('nan'),
                "variance_error": float('nan'),
                "phase_error": float('nan'),
                "total_mse": float('nan')
            }

        valid_indices = ~(np.isnan(pred_flat) | np.isnan(truth_flat))
        pred_valid = pred_flat[valid_indices]
        truth_valid = truth_flat[valid_indices]
        
        n_valid = len(pred_valid)
        if n_valid < 2:
            return {
                "bias_squared": 0.0,
                "variance_error": 0.0,
                "phase_error": 0.0,
                "total_mse": 0.0
            }
        
        bias = np.mean(pred_valid) - np.mean(truth_valid)
        bias_squared = bias ** 2
        
        pred_std = np.std(pred_valid)
        truth_std = np.std(truth_valid)
        variance_error = (pred_std - truth_std) ** 2
        
        corr = await calculate_correlation(prediction, ground_truth)
        if np.isnan(corr):
            phase_error = float('nan')
        else:
            phase_error = 2 * pred_std * truth_std * (1 - corr)
        
        total_mse = np.mean((pred_valid - truth_valid) ** 2)
        
        return {
            "bias_squared": bias_squared,
            "variance_error": variance_error,
            "phase_error": phase_error,
            "total_mse": total_mse
        }
    except Exception as e:
        logger.error(f"Error in calculate_mse_decomposition: {e}")
        raise

async def calculate_forecast_spread(
    predictions: List[np.ndarray]
) -> Dict[str, np.ndarray]:
    """
    Calculate spread metrics for an ensemble of forecasts.
    
    Args:
        predictions: List of prediction arrays
    
    Returns:
        Dictionary containing ensemble mean, standard deviation, min, max values
    """
    try:
        if not predictions or len(predictions) < 2:
            logger.warning("Insufficient predictions for spread calculation.")
            return {
                "ensemble_mean": np.array([]),
                "ensemble_std": np.array([]),
                "ensemble_min": np.array([]),
                "ensemble_max": np.array([])
            }
        
        stacked = np.stack(predictions, dtype=np.float32)
        
        ens_mean = np.nanmean(stacked, axis=0)
        ens_std = np.nanstd(stacked, axis=0)
        ens_min = np.nanmin(stacked, axis=0)
        ens_max = np.nanmax(stacked, axis=0)
        
        return {
            "ensemble_mean": ens_mean,
            "ensemble_std": ens_std,
            "ensemble_min": ens_min,
            "ensemble_max": ens_max
        }
    except Exception as e:
        logger.error(f"Error in calculate_forecast_spread: {e}")
        raise

async def calculate_crps(
    prediction_quantiles: Dict[float, np.ndarray], 
    ground_truth: np.ndarray
) -> float:
    """
    Calculate Continuous Ranked Probability Score (CRPS).
    
    Args:
        prediction_quantiles: Dictionary mapping quantile values to predicted arrays
        ground_truth: Ground truth data array
    
    Returns:
        CRPS value (lower is better)
    """
    try:
        if not prediction_quantiles:
            logger.warning("Empty prediction quantiles provided.")
            return float('inf')
        
        quantiles = sorted(prediction_quantiles.keys())
        if len(quantiles) < 2:
            logger.warning("At least two quantiles are required for CRPS calculation.")
            return float('inf')
        
        total_crps = 0.0
        valid_count = 0
        
        for idx in np.ndindex(ground_truth.shape):
            truth_val = ground_truth[idx]
            if np.isnan(truth_val):
                continue
                
            point_crps = 0.0
            for i in range(len(quantiles) - 1):
                q_low = quantiles[i]
                q_high = quantiles[i+1]
                
                val_low = prediction_quantiles[q_low][idx]
                val_high = prediction_quantiles[q_high][idx]
                
                if np.isnan(val_low) or np.isnan(val_high):
                    continue
                
                prob_mass = q_high - q_low
                
                if truth_val < val_low:
                    point_crps += prob_mass * (val_low - truth_val)**2
                elif truth_val > val_high:
                    point_crps += prob_mass * (truth_val - val_high)**2
                else:
                    # This is an approximation assuming linear CDF within the interval
                    norm_pos = (truth_val - val_low) / max(val_high - val_low, 1e-8)
                    point_crps += prob_mass * norm_pos * (1.0 - norm_pos)
            
            total_crps += point_crps
            valid_count += 1
        
        if valid_count > 0:
            return total_crps / valid_count
        else:
            return float('inf')
    except Exception as e:
        logger.error(f"Error in calculate_crps: {e}")
        raise

async def calculate_all_metrics(
    prediction: np.ndarray,
    ground_truth: np.ndarray,
    variable_name: str = "unknown",
    prediction_quantiles: Optional[Dict[float, np.ndarray]] = None
) -> Dict[str, float]:
    """
    Calculate a comprehensive set of core quantifiable metrics for a prediction.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
        variable_name: Name of the variable being evaluated
        prediction_quantiles: Optional dictionary mapping quantiles to forecast arrays for CRPS
    
    Returns:
        Dictionary of calculated metrics
    """
    try:
        pred_np = np.asarray(prediction)
        truth_np = np.asarray(ground_truth)
        
        if pred_np.shape != truth_np.shape:
            logger.error(f"Shape mismatch between prediction {pred_np.shape} and ground truth {truth_np.shape}")
            return {}
        
        metrics = {}
        
        metrics["rmse"] = np.sqrt(np.mean((pred_np - truth_np)**2))
        metrics["normalized_rmse"] = await normalize_rmse(pred_np, truth_np, variable_name)
        metrics["mae"] = await calculate_mae(pred_np, truth_np)
        metrics["bias"] = await calculate_bias(pred_np, truth_np)
        metrics["correlation"] = await calculate_correlation(pred_np, truth_np)
        
        mse_components = await calculate_mse_decomposition(pred_np, truth_np)
        metrics.update(mse_components)
        
        if prediction_quantiles is not None:
            quantiles_np = {q: np.asarray(p) for q, p in prediction_quantiles.items()}
            metrics["crps"] = await calculate_crps(quantiles_np, truth_np)
        
        return metrics
    except Exception as e:
        logger.error(f"Error in calculate_all_metrics for variable {variable_name}: {e}")
        return {}

async def calculate_rmse_vectorized(forecast: Dict, ground_truth: Dict, subsample_factor: int = 1) -> float:
    """
    Vectorized RMSE calculation across multiple variables with optional subsampling.
    
    Args:
        forecast: Dictionary of predicted variables
        ground_truth: Dictionary of ground truth variables
        subsample_factor: Factor to subsample grid points
    
    Returns:
        Overall Root Mean Square Error
    """
    try:
        total_se = 0.0
        total_count = 0
        
        common_vars = set(forecast.keys()) & set(ground_truth.keys())
        if not common_vars:
            logger.warning("No common variables found for RMSE calculation.")
            return float('inf')
        
        for var in common_vars:
            pred = forecast[var]
            truth = ground_truth[var]
            
            if pred.shape != truth.shape:
                logger.warning(f"Skipping variable '{var}': shape mismatch {pred.shape} vs {truth.shape}.")
                continue
            
            pred = pred.astype(np.float32)
            truth = truth.astype(np.float32)
            
            valid_mask = ~(np.isnan(pred) | np.isnan(truth))
            if np.sum(valid_mask) == 0:
                continue
            
            if subsample_factor > 1:
                pred = pred[::subsample_factor, ::subsample_factor]
                truth = truth[::subsample_factor, ::subsample_factor]
            
            se = np.sum((pred - truth) ** 2)
            count = pred.size
            
            total_se += se
            total_count += count
        
        if total_count > 0:
            return np.sqrt(total_se / total_count)
        else:
            logger.warning("No valid data points for RMSE calculation.")
            return float('inf')
    except Exception as e:
        logger.error(f"Error in calculate_rmse_vectorized: {e}")
        raise