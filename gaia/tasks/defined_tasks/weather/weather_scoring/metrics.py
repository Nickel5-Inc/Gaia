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
    Calculate overall RMSE across all variables in the provided dictionaries.
    Assumes variable values are numpy arrays.
    
    Args:
        prediction: Dictionary of predicted variables {var_name: array}
        ground_truth: Dictionary of ground truth variables {var_name: array}
    
    Returns:
        Root Mean Square Error value across all common variables and valid points.
    """
    try:
        total_squared_error = 0.0
        total_count = 0
        
        common_vars = set(prediction.keys()) & set(ground_truth.keys())
        if not common_vars:
            logger.warning("calculate_rmse: No common variables found.")
            return float('inf')
            
        for var in common_vars:
            pred_data = prediction[var]
            truth_data = ground_truth[var]
            
            if not isinstance(pred_data, np.ndarray) or not isinstance(truth_data, np.ndarray):
                logger.warning(f"calculate_rmse: Skipping var '{var}', data is not a numpy array.")
                continue
                
            if pred_data.shape != truth_data.shape:
                logger.warning(f"calculate_rmse: Skipping var '{var}', shape mismatch {pred_data.shape} vs {truth_data.shape}.")
                continue
                    
            squared_error = np.square(pred_data - truth_data)
            valid_mask = ~np.isnan(squared_error)
            
            total_squared_error += np.sum(squared_error[valid_mask])
            total_count += np.sum(valid_mask)
        
        if total_count > 0:
            return np.sqrt(total_squared_error / total_count)
        else:
            logger.warning("calculate_rmse: No valid (non-NaN) data points found for comparison.")
            return float('inf')
    except Exception as e:
        logger.error(f"Error in calculate_rmse: {e}")
        raise

async def normalize_rmse(prediction: np.ndarray, ground_truth: np.ndarray, var_name: str) -> float:
    """
    Calculate normalized RMSE for a specific variable array.
    Normalization uses predefined scale factors based on typical error ranges.
    Calculates RMSE over all valid (non-NaN) points in the arrays.
    
    Args:
        prediction: Predicted data array (can be multi-dimensional).
        ground_truth: Ground truth data array (must match prediction shape).
        var_name: Variable name for selecting the normalization scale factor.
    
    Returns:
        Normalized RMSE (dimensionless).
    """
    try:
        if prediction.shape != ground_truth.shape:
             logger.warning(f"normalize_rmse: Shape mismatch for var '{var_name}': {prediction.shape} vs {ground_truth.shape}. Returning inf.")
             return float('inf')
             
        # Typical error ranges
        scale_factors = {
            "2t": 5.0,      # ~5K temperature error
            "msl": 500.0,   # ~500Pa pressure error
            "10u": 3.0,     # ~3m/s wind error
            "10v": 3.0,
            "t": 5.0,       # Temperature at pressure levels
            "q": 0.001,     # ~0.001 kg/kg humidity error
            "z": 50.0,      # ~50m^2/s^2 geopotential error
            "u": 3.0,       # Wind at pressure levels
            "v": 3.0
        }
        
        squared_error = np.square(prediction - ground_truth)
        mean_squared_error = np.nanmean(squared_error)
        
        if np.isnan(mean_squared_error):
             logger.warning(f"normalize_rmse: All data points were NaN for var '{var_name}'. Returning inf.")
             return float('inf')
             
        rmse = np.sqrt(mean_squared_error)
        
        scale = scale_factors.get(var_name, 1.0)
        safe_scale = max(abs(scale), 1e-9)
        
        return rmse / safe_scale
    except Exception as e:
        logger.error(f"Error in normalize_rmse for variable '{var_name}': {e}")
        raise

async def calculate_mean_abs_diff(array1: np.ndarray, array2: np.ndarray) -> float:
    """
    Calculate mean absolute difference between two arrays.
    Averages over all valid (non-NaN) elements.
    
    Args:
        array1: First array (can be multi-dimensional).
        array2: Second array (must match array1 shape).
    
    Returns:
        Mean absolute difference.
    """
    try:
        if array1.shape != array2.shape:
            logger.warning(f"Shape mismatch in calculate_mean_abs_diff: {array1.shape} vs {array2.shape}. Returning inf.")
            return float('inf')
            
        mean_diff = np.nanmean(np.abs(array1 - array2))
        
        if np.isnan(mean_diff):
            logger.warning("calculate_mean_abs_diff: All data points were NaN. Returning inf.")
            return float('inf')
            
        return mean_diff
    except Exception as e:
        logger.error(f"Error in calculate_mean_abs_diff: {e}")
        raise

async def calculate_mae(prediction: np.ndarray, ground_truth: np.ndarray) -> float:
    """
    Calculate Mean Absolute Error (MAE).
    Averages over all valid (non-NaN) elements.

    Args:
        prediction: Predicted data array (can be multi-dimensional).
        ground_truth: Ground truth data array (must match prediction shape).
    
    Returns:
        Mean Absolute Error.
    """
    try:
        if prediction.shape != ground_truth.shape:
             logger.warning(f"calculate_mae: Shape mismatch: {prediction.shape} vs {ground_truth.shape}. Returning inf.")
             return float('inf')
             
        mae = np.nanmean(np.abs(prediction - ground_truth))
        
        if np.isnan(mae):
            logger.warning("calculate_mae: All data points were NaN. Returning inf.")
            return float('inf')
            
        return mae
    except Exception as e:
        logger.error(f"Error in calculate_mae: {e}")
        raise

async def calculate_mae_dict(prediction: Dict, ground_truth: Dict) -> float:
    """
    Calculate overall MAE across all variables in the provided dictionaries.
    Assumes variable values are numpy arrays.
    
    Args:
        prediction: Dictionary of predicted variables {var_name: array}
        ground_truth: Dictionary of ground truth variables {var_name: array}
    
    Returns:
        Mean Absolute Error value across all common variables and valid points.
    """
    try:
        total_absolute_error = 0.0
        total_count = 0
        
        common_vars = set(prediction.keys()) & set(ground_truth.keys())
        if not common_vars:
            logger.warning("calculate_mae_dict: No common variables found.")
            return float('inf')
            
        for var in common_vars:
            pred_data = prediction[var]
            truth_data = ground_truth[var]
            
            if not isinstance(pred_data, np.ndarray) or not isinstance(truth_data, np.ndarray):
                logger.warning(f"calculate_mae_dict: Skipping var '{var}', data is not a numpy array.")
                continue
                
            if pred_data.shape != truth_data.shape:
                logger.warning(f"calculate_mae_dict: Skipping var '{var}', shape mismatch {pred_data.shape} vs {truth_data.shape}.")
                continue
                    
            absolute_error = np.abs(pred_data - truth_data)
            valid_mask = ~np.isnan(absolute_error)
            
            total_absolute_error += np.sum(absolute_error[valid_mask])
            total_count += np.sum(valid_mask)
        
        if total_count > 0:
            return total_absolute_error / total_count
        else:
            logger.warning("calculate_mae_dict: No valid (non-NaN) data points found for comparison.")
            return float('inf')
    except Exception as e:
        logger.error(f"Error in calculate_mae_dict: {e}")
        raise

async def calculate_bias(prediction: np.ndarray, ground_truth: np.ndarray) -> float:
    """
    Calculate bias (mean error).
    Averages over all valid (non-NaN) elements.

    Args:
        prediction: Predicted data array (can be multi-dimensional).
        ground_truth: Ground truth data array (must match prediction shape).
    
    Returns:
        Bias (positive means overprediction).
    """
    try:
        if prediction.shape != ground_truth.shape:
             logger.warning(f"calculate_bias: Shape mismatch: {prediction.shape} vs {ground_truth.shape}. Returning nan.")
             return float('nan')
             
        bias = np.nanmean(prediction - ground_truth)
        
        return bias
    except Exception as e:
        logger.error(f"Error in calculate_bias: {e}")
        raise

async def calculate_correlation(prediction: np.ndarray, ground_truth: np.ndarray) -> float:
    """
    Calculate Pearson correlation coefficient.
    Flattens arrays and handles NaN values by excluding them pairwise.
    
    Args:
        prediction: Predicted data array (can be multi-dimensional).
        ground_truth: Ground truth data array (must match prediction shape).
    
    Returns:
        Correlation coefficient (-1 to 1), or 0.0 if calculation is not possible.
    """
    try:
        if prediction.shape != ground_truth.shape:
            logger.warning(f"Shape mismatch in calculate_correlation: {prediction.shape} vs {ground_truth.shape}. Returning 0.0.")
            return 0.0
            
        pred_flat = prediction.flatten()
        truth_flat = ground_truth.flatten()
        
        valid_mask = ~(np.isnan(pred_flat) | np.isnan(truth_flat))
        pred_valid = pred_flat[valid_mask]
        truth_valid = truth_flat[valid_mask]
        
        if len(pred_valid) < 2:
            logger.debug("calculate_correlation: Fewer than 2 valid data points after NaN removal. Returning 0.0.")
            return 0.0
        
        pred_std = np.std(pred_valid)
        truth_std = np.std(truth_valid)
        if pred_std < 1e-9 or truth_std < 1e-9:
            logger.debug(f"calculate_correlation: Zero standard deviation detected (pred_std={pred_std:.2e}, truth_std={truth_std:.2e}). Returning 0.0.")
            return 0.0

        correlation = np.corrcoef(pred_valid, truth_valid)[0, 1]
        if np.isnan(correlation):
             logger.warning("calculate_correlation: np.corrcoef returned NaN. Returning 0.0.")
             return 0.0
             
        return correlation
    except Exception as e:
        logger.error(f"Error in calculate_correlation: {e}")
        return 0.0

async def calculate_mse_decomposition(
    prediction: np.ndarray, 
    ground_truth: np.ndarray
) -> Dict[str, float]:
    """
    Decompose Mean Squared Error (MSE) into bias-squared, variance error, 
    and phase error components. Operates on flattened arrays after removing NaNs.
    
    Args:
        prediction: Predicted data array (can be multi-dimensional).
        ground_truth: Ground truth data array (must match prediction shape).
    
    Returns:
        Dictionary containing MSE components:
         - bias_squared: Squared difference of means.
         - variance_error: Squared difference of standard deviations.
         - phase_error: 2 * std_pred * std_truth * (1 - correlation).
         - total_mse: Overall Mean Squared Error (calculated directly for verification).
    """
    results = {
        "bias_squared": float('nan'),
        "variance_error": float('nan'),
        "phase_error": float('nan'),
        "total_mse": float('nan')
    }
    try:
        if prediction.shape != ground_truth.shape:
            logger.warning(f"Shape mismatch in calculate_mse_decomposition: {prediction.shape} vs {ground_truth.shape}.")
            return results

        pred_flat = prediction.flatten()
        truth_flat = ground_truth.flatten()
        
        valid_mask = ~(np.isnan(pred_flat) | np.isnan(truth_flat))
        pred_valid = pred_flat[valid_mask]
        truth_valid = truth_flat[valid_mask]
        
        n_valid = len(pred_valid)
        if n_valid < 2:
            logger.debug("calculate_mse_decomposition: Fewer than 2 valid points. Returning zeros.")
            results = {k: 0.0 for k in results}
            return results
        
        pred_mean = np.mean(pred_valid)
        truth_mean = np.mean(truth_valid)
        bias = pred_mean - truth_mean
        results["bias_squared"] = bias ** 2
        
        pred_std = np.std(pred_valid)
        truth_std = np.std(truth_valid)
        results["variance_error"] = (pred_std - truth_std) ** 2
        

        if pred_std < 1e-9 or truth_std < 1e-9:
             corr = 0.0
             logger.debug("calculate_mse_decomposition: Zero std dev, setting corr=0 for phase error.")
        else:
             corr = np.corrcoef(pred_valid, truth_valid)[0, 1]
             if np.isnan(corr):
                  logger.warning("calculate_mse_decomposition: Correlation calculation resulted in NaN. Phase error will be NaN.")
                  corr = float('nan')
                  
        if not np.isnan(corr):
            results["phase_error"] = 2 * pred_std * truth_std * (1 - corr)

        results["total_mse"] = np.mean(np.square(pred_valid - truth_valid))
        
        return results
    except Exception as e:
        logger.error(f"Error in calculate_mse_decomposition: {e}")
        return results

async def calculate_forecast_spread(
    predictions: List[np.ndarray]
) -> Dict[str, np.ndarray]:
    """
    Calculate spread metrics for an ensemble of forecasts.
    Uses NaN-aware functions for calculations.
    
    Args:
        predictions: List of prediction arrays (numpy arrays, assumed to have same shape).
    
    Returns:
        Dictionary containing ensemble statistics (arrays of the same shape as inputs):
         - ensemble_mean: Mean across the ensemble dimension.
         - ensemble_std: Standard deviation across the ensemble dimension (the 'spread').
         - ensemble_min: Minimum value across the ensemble dimension.
         - ensemble_max: Maximum value across the ensemble dimension.
    """
    default_result = {
        "ensemble_mean": np.array([]),
        "ensemble_std": np.array([]),
        "ensemble_min": np.array([]),
        "ensemble_max": np.array([])
    }
    try:
        if not predictions or len(predictions) < 2:
            logger.warning("Insufficient predictions (< 2) for spread calculation.")
            return default_result
        
        if predictions[0].shape != predictions[1].shape:
             logger.warning("Input arrays have different shapes. Cannot calculate spread.")
             return default_result
             
        try:
             stacked = np.stack(predictions, axis=0, dtype=np.float32)
        except ValueError as e:
             logger.error(f"Error stacking predictions, likely due to inconsistent shapes: {e}")
             shapes = [p.shape for p in predictions]
             logger.error(f"Prediction shapes: {shapes}")
             return default_result
             
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
        return default_resul

async def calculate_crps(
    prediction_quantiles: Dict[float, np.ndarray], 
    ground_truth: np.ndarray
) -> float:
    """
    Calculate Continuous Ranked Probability Score (CRPS).
    
    NOTE: This implementation iterates pixel by pixel and is very very slow.
    
    Args:
        prediction_quantiles: Dictionary mapping quantile values (0-1) to predicted arrays.
                              Arrays must have the same shape as ground_truth.
        ground_truth: Ground truth data array.
    
    Returns:
        CRPS value (lower is better), averaged over all valid grid points.
    """
    try:
        if not prediction_quantiles:
            logger.warning("calculate_crps: Empty prediction quantiles provided.")
            return float('inf')
        
        quantiles = sorted(prediction_quantiles.keys())
        if len(quantiles) < 2:
            logger.warning("calculate_crps: At least two quantiles are required. Returning inf.")
            return float('inf')
            
        first_q_val = prediction_quantiles[quantiles[0]]
        if first_q_val.shape != ground_truth.shape:
            logger.warning(f"calculate_crps: Shape mismatch - Quantile shape {first_q_val.shape}, GT shape {ground_truth.shape}. Returning inf.")
            return float('inf')
            
        total_crps = 0.0
        valid_count = 0
        
        for idx in np.ndindex(ground_truth.shape):
            truth_val = ground_truth[idx]
            if np.isnan(truth_val):
                continue
                
            point_crps = 0.0
            last_val = -np.inf
            # Integrate over quantile intervals
            for i in range(len(quantiles) - 1):
                q_low = quantiles[i]
                q_high = quantiles[i+1]
                
                try:
                    val_low = prediction_quantiles[q_low][idx]
                    val_high = prediction_quantiles[q_high][idx]
                except IndexError:
                    logger.warning(f"calculate_crps: IndexError accessing quantiles at index {idx}. Skipping point.")
                    point_crps = np.nan
                    break
                    
                if np.isnan(val_low) or np.isnan(val_high) or val_low > val_high:
                    continue
                
                prob_mass = q_high - q_low
                if prob_mass <= 0: continue
                
                if truth_val < val_low:

                    point_crps += prob_mass * (val_low - truth_val)**2
                    
                elif truth_val > val_high:
                    point_crps += prob_mass * (truth_val - val_high)**2
                else:
                    norm_pos = (truth_val - val_low) / max(val_high - val_low, 1e-9)
                    point_crps += prob_mass * norm_pos * (1.0 - norm_pos) # Placeholder approximation

                last_val = val_high
                
            min_q, max_q = quantiles[0], quantiles[-1]
            min_val, max_val = prediction_quantiles[min_q][idx], prediction_quantiles[max_q][idx]
            
            if not np.isnan(min_val) and truth_val < min_val:
                 pass 

            if not np.isnan(max_val) and truth_val > max_val:
                 pass
                 
            if not np.isnan(point_crps):
                total_crps += point_crps
                valid_count += 1
        
        if valid_count > 0:
            average_crps = total_crps / valid_count
            logger.info(f"Calculated average CRPS: {average_crps:.4f} over {valid_count} valid points.")
            return average_crps
        else:
            logger.warning("calculate_crps: No valid points found for CRPS calculation.")
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
    Metrics are computed over all valid (non-NaN) data points in the input arrays.
    
    Args:
        prediction: Predicted data array (can be multi-dimensional, e.g., [time, lat, lon]).
        ground_truth: Ground truth data array (must match prediction shape).
        variable_name: Name of the variable being evaluated (used for normalization).
        prediction_quantiles: Optional dictionary mapping quantiles to forecast arrays for CRPS.
    
    Returns:
        Dictionary of calculated metrics. Values will be float or NaN if calculation fails.
    """
    metrics = {}
    try:
        pred_np = np.asarray(prediction)
        truth_np = np.asarray(ground_truth)
        
        if pred_np.shape != truth_np.shape:
            logger.error(f"Shape mismatch in calculate_all_metrics for '{variable_name}': {pred_np.shape} vs {truth_np.shape}")
            return {}
        
        try:
             squared_error = np.square(pred_np - truth_np)
             mean_squared_error = np.nanmean(squared_error)
             metrics["rmse"] = np.sqrt(mean_squared_error) if not np.isnan(mean_squared_error) else float('nan')
        except Exception as rmse_err:
             logger.error(f"Error calculating RMSE for '{variable_name}': {rmse_err}")
             metrics["rmse"] = float('nan')
             
        try:
             metrics["normalized_rmse"] = await normalize_rmse(pred_np, truth_np, variable_name)
        except Exception as nrmse_err:
             logger.error(f"Error calculating normalized_rmse for '{variable_name}': {nrmse_err}")
             metrics["normalized_rmse"] = float('nan')
             
        try:
             metrics["mae"] = await calculate_mae(pred_np, truth_np)
        except Exception as mae_err:
             logger.error(f"Error calculating MAE for '{variable_name}': {mae_err}")
             metrics["mae"] = float('nan')
             
        try:
             metrics["bias"] = await calculate_bias(pred_np, truth_np)
        except Exception as bias_err:
             logger.error(f"Error calculating Bias for '{variable_name}': {bias_err}")
             metrics["bias"] = float('nan')
             
        try:
             metrics["correlation"] = await calculate_correlation(pred_np, truth_np)
        except Exception as corr_err:
             logger.error(f"Error calculating Correlation for '{variable_name}': {corr_err}")
             metrics["correlation"] = float('nan')
        
        try:
             mse_components = await calculate_mse_decomposition(pred_np, truth_np)
             metrics.update(mse_components)
        except Exception as mse_err:
             logger.error(f"Error calculating MSE Decomposition for '{variable_name}': {mse_err}")
             if "bias_squared" not in metrics: metrics["bias_squared"] = float('nan')
             if "variance_error" not in metrics: metrics["variance_error"] = float('nan')
             if "phase_error" not in metrics: metrics["phase_error"] = float('nan')
             if "total_mse" not in metrics: metrics["total_mse"] = float('nan')
             
        if prediction_quantiles is not None:
            try:
                quantiles_np = {}
                valid_quantiles = True
                for q, p_arr in prediction_quantiles.items():
                    p_np = np.asarray(p_arr)
                    if p_np.shape != truth_np.shape:
                         logger.warning(f"CRPS: Shape mismatch for quantile {q} ({p_np.shape}) vs GT ({truth_np.shape}). Skipping CRPS.")
                         valid_quantiles = False
                         break
                    quantiles_np[q] = p_np
                    
                if valid_quantiles:
                    metrics["crps"] = await calculate_crps(quantiles_np, truth_np)
                else:
                    metrics["crps"] = float('nan')
            except Exception as crps_err:
                 logger.error(f"Error calculating CRPS for '{variable_name}': {crps_err}")
                 metrics["crps"] = float('nan')
        
        logger.info(f"Metrics for '{variable_name}': { {k: f'{v:.4f}' if isinstance(v, float) and not np.isnan(v) else v for k, v in metrics.items()} }")
        return metrics
        
    except Exception as e:
        logger.error(f"Unhandled error in calculate_all_metrics for variable '{variable_name}': {e}")
        return metrics if metrics else {}

async def calculate_rmse_vectorized(forecast: Dict, ground_truth: Dict, subsample_factor: int = 1) -> float:
    """
    Vectorized RMSE calculation across multiple variables with optional subsampling.
    Calculates a single RMSE value averaged over all variables and points.
    Ignores NaNs in the calculation.
    
    Args:
        forecast: Dictionary of predicted variables {var_name: array}.
        ground_truth: Dictionary of ground truth variables {var_name: array}.
        subsample_factor: Factor to subsample grid points (e.g., 2 means use every 2nd point).
    
    Returns:
        Overall Root Mean Square Error.
    """
    try:
        total_se = 0.0
        total_count = 0
        
        common_vars = set(forecast.keys()) & set(ground_truth.keys())
        if not common_vars:
            logger.warning("calculate_rmse_vectorized: No common variables found.")
            return float('inf')
        
        for var in common_vars:
            pred = forecast[var]
            truth = ground_truth[var]
            
            if not isinstance(pred, np.ndarray) or not isinstance(truth, np.ndarray):
                logger.warning(f"calculate_rmse_vectorized: Skipping var '{var}', data is not a numpy array.")
                continue
                
            if pred.shape != truth.shape:
                logger.warning(f"calculate_rmse_vectorized: Skipping var '{var}': shape mismatch {pred.shape} vs {truth.shape}.")
                continue
            
            pred = pred.astype(np.float32)
            truth = truth.astype(np.float32)
            
            if subsample_factor > 1:
                slicer = (slice(None),) + (slice(None, None, subsample_factor),) * (pred.ndim - 1)
                try:
                    pred = pred[slicer]
                    truth = truth[slicer]
                    logger.debug(f"Subsampled var '{var}' by {subsample_factor}. New shape: {pred.shape}")
                except IndexError:
                    logger.warning(f"Could not apply subsampling factor {subsample_factor} to var '{var}' with shape {forecast[var].shape}. Skipping subsampling for this var.")
                    pred = forecast[var].astype(np.float32)
                    truth = ground_truth[var].astype(np.float32)
            
            squared_error = np.square(pred - truth)
            valid_mask = ~np.isnan(squared_error)
            
            total_se += np.sum(squared_error[valid_mask])
            total_count += np.sum(valid_mask)
        
        if total_count > 0:
            overall_rmse = np.sqrt(total_se / total_count)
            logger.info(f"Calculated overall vectorized RMSE: {overall_rmse:.4f} over {len(common_vars)} vars and {total_count} points.")
            return overall_rmse
        else:
            logger.warning("calculate_rmse_vectorized: No valid (non-NaN) data points found.")
            return float('inf')
    except Exception as e:
        logger.error(f"Error in calculate_rmse_vectorized: {e}")
        raise