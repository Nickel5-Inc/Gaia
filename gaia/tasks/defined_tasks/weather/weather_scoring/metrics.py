import numpy as np
import xarray as xr
from typing import Dict, Any, Union, Optional, Tuple, List
from fiber.logging_utils import get_logger
logger = get_logger(__name__)

"""
Core metric calculations for weather forecast evaluation.
This module provides implementations of common metrics for
evaluating weather forecasts, including RMSE, correlation, bias, and
specialized meteorological skill scores.
"""

def calculate_rmse(prediction: Dict, ground_truth: Dict) -> float:
    """
    Calculate overall RMSE across all variables.
    
    Args:
        prediction: Dictionary of predicted variables
        ground_truth: Dictionary of ground truth variables
    
    Returns:
        Root Mean Square Error value
    """
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

def normalize_rmse(prediction: np.ndarray, ground_truth: np.ndarray, var_name: str) -> float:
    """
    Calculate normalized RMSE for a specific variable.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
        var_name: Variable name for appropriate normalization
    
    Returns:
        Normalized RMSE
    """
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
    return rmse / scale

def calculate_mean_abs_diff(array1: np.ndarray, array2: np.ndarray) -> float:
    """
    Calculate mean absolute difference between two arrays.
    
    Args:
        array1: First array
        array2: Second array
    
    Returns:
        Mean absolute difference
    """
    if array1.shape != array2.shape:
        return float('inf')
        
    return np.mean(np.abs(array1 - array2))

def calculate_mae(prediction: np.ndarray, ground_truth: np.ndarray) -> float:
    """
    Calculate Mean Absolute Error.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
    
    Returns:
        Mean Absolute Error
    """
    return np.mean(np.abs(prediction - ground_truth))

def calculate_bias(prediction: np.ndarray, ground_truth: np.ndarray) -> float:
    """
    Calculate bias (mean error).
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
    
    Returns:
        Bias (positive means overprediction)
    """
    return np.mean(prediction - ground_truth)

def calculate_correlation(prediction: np.ndarray, ground_truth: np.ndarray) -> float:
    """
    Calculate Pearson correlation coefficient.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
    
    Returns:
        Correlation coefficient (-1 to 1)
    """
    pred_flat = prediction.flatten()
    truth_flat = ground_truth.flatten()
    valid_indices = ~(np.isnan(pred_flat) | np.isnan(truth_flat))
    pred_valid = pred_flat[valid_indices]
    truth_valid = truth_flat[valid_indices]
    
    if len(pred_valid) < 2:
        return 0.0
    
    try:
        return np.corrcoef(pred_valid, truth_valid)[0, 1]
    except:
        return 0.0

def calculate_mse_decomposition(
    prediction: np.ndarray, 
    ground_truth: np.ndarray
) -> Dict[str, float]:
    """
    Decompose MSE into bias, variance, and phase error components.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
    
    Returns:
        Dictionary containing MSE components (bias^2, variance error, phase error)
    """
    pred_flat = prediction.flatten()
    truth_flat = ground_truth.flatten()
    
    valid_indices = ~(np.isnan(pred_flat) | np.isnan(truth_flat))
    pred_valid = pred_flat[valid_indices]
    truth_valid = truth_flat[valid_indices]
    
    if len(pred_valid) < 2:
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
    
    corr = calculate_correlation(prediction, ground_truth)
    phase_error = 2 * pred_std * truth_std * (1 - corr)
    
    total_mse = np.mean((pred_valid - truth_valid) ** 2)
    
    return {
        "bias_squared": bias_squared,
        "variance_error": variance_error,
        "phase_error": phase_error,
        "total_mse": total_mse
    }

def calculate_anomaly_correlation(
    prediction: np.ndarray, 
    ground_truth: np.ndarray, 
    climatology: np.ndarray
) -> float:
    """
    Calculate Anomaly Correlation Coefficient (ACC).
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
        climatology: Climatological mean data array (same shape as others)
    
    Returns:
        ACC value (-1 to 1)
    """
    pred_anomaly = prediction - climatology
    truth_anomaly = ground_truth - climatology
    
    pred_anom_flat = pred_anomaly.flatten()
    truth_anom_flat = truth_anomaly.flatten()
    
    valid_indices = ~(np.isnan(pred_anom_flat) | np.isnan(truth_anom_flat))
    pred_anom_valid = pred_anom_flat[valid_indices]
    truth_anom_valid = truth_anom_flat[valid_indices]
    
    if len(pred_anom_valid) < 2:
        return 0.0
    
    numerator = np.sum(pred_anom_valid * truth_anom_valid)
    denominator = np.sqrt(np.sum(pred_anom_valid**2) * np.sum(truth_anom_valid**2))
    
    if denominator == 0:
        return 0.0
    
    return numerator / denominator

def calculate_csi(
    prediction: np.ndarray, 
    ground_truth: np.ndarray, 
    threshold: float
) -> float:
    """
    Calculate Critical Success Index (CSI) for a threshold.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
        threshold: Threshold for event detection
    
    Returns:
        CSI value (0 to 1)
    """
    pred_mask = prediction >= threshold
    truth_mask = ground_truth >= threshold
    
    hits = np.sum(pred_mask & truth_mask)
    misses = np.sum(~pred_mask & truth_mask)
    false_alarms = np.sum(pred_mask & ~truth_mask)
    
    denominator = hits + misses + false_alarms
    if denominator == 0:
        return 0.0
    
    return hits / denominator

def calculate_ets(
    prediction: np.ndarray, 
    ground_truth: np.ndarray, 
    threshold: float
) -> float:
    """
    Calculate Equitable Threat Score (ETS) for a threshold.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
        threshold: Threshold for event detection
    
    Returns:
        ETS value (< 1, with 1 being perfect)
    """
    pred_mask = prediction >= threshold
    truth_mask = ground_truth >= threshold
    
    hits = np.sum(pred_mask & truth_mask)
    misses = np.sum(~pred_mask & truth_mask)
    false_alarms = np.sum(pred_mask & ~truth_mask)
    correct_negatives = np.sum(~pred_mask & ~truth_mask)
    
    total = hits + misses + false_alarms + correct_negatives
    hits_random = (hits + misses) * (hits + false_alarms) / total
    
    denominator = hits + misses + false_alarms - hits_random
    if denominator == 0:
        return 0.0
    
    return (hits - hits_random) / denominator

def calculate_extreme_value_scores(
    prediction: np.ndarray, 
    ground_truth: np.ndarray, 
    percentile: float = 95.0
) -> Dict[str, float]:
    """
    Calculate skill scores for extreme values.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
        percentile: Percentile threshold for extreme value detection
    
    Returns:
        Dictionary with various extreme value metrics
    """
    threshold = np.nanpercentile(ground_truth, percentile)
    
    csi = calculate_csi(prediction, ground_truth, threshold)
    ets = calculate_ets(prediction, ground_truth, threshold)
    
    pred_mask = prediction >= threshold
    truth_mask = ground_truth >= threshold
    
    hits = np.sum(pred_mask & truth_mask)
    misses = np.sum(~pred_mask & truth_mask)
    false_alarms = np.sum(pred_mask & ~truth_mask)
    
    # Probability of detection (hit rate)
    pod = hits / (hits + misses) if (hits + misses) > 0 else 0.0
    # False alarm ratio
    far = false_alarms / (hits + false_alarms) if (hits + false_alarms) > 0 else 0.0
    # Frequency bias
    bias = (hits + false_alarms) / (hits + misses) if (hits + misses) > 0 else 0.0
    
    return {
        "threshold": threshold,
        "csi": csi,
        "ets": ets,
        "pod": pod,
        "far": far,
        "bias": bias
    }

def calculate_spatial_pattern_metrics(
    prediction: np.ndarray, 
    ground_truth: np.ndarray
) -> Dict[str, float]:
    """
    Calculate metrics related to spatial pattern similarity.
    
    Args:
        prediction: Predicted data array (2D)
        ground_truth: Ground truth data array (2D)
    
    Returns:
        Dictionary of spatial pattern metrics
    """
    if prediction.ndim != 2 or ground_truth.ndim != 2:
        return {
            "spatial_corr": 0.0,
            "gradient_corr": 0.0, 
            "fourier_corr": 0.0
        }
    
    spatial_corr = calculate_correlation(prediction, ground_truth)
    
    # Gradient correlation (pattern similarity)
    try:
        grad_x_pred, grad_y_pred = np.gradient(prediction)
        grad_x_truth, grad_y_truth = np.gradient(ground_truth)
        
        # Combine gradients
        grad_pred = np.sqrt(grad_x_pred**2 + grad_y_pred**2)
        grad_truth = np.sqrt(grad_x_truth**2 + grad_y_truth**2)
        gradient_corr = calculate_correlation(grad_pred, grad_truth)
    except:
        gradient_corr = 0.0
    
    # Fourier spectrum correlation (frequency domain similarity)
    try:
        from scipy import fftpack
        
        # Apply FFT
        fft_pred = np.abs(fftpack.fft2(prediction))
        fft_truth = np.abs(fftpack.fft2(ground_truth))
        fourier_corr = calculate_correlation(fft_pred, fft_truth)
    except:
        fourier_corr = 0.0
    
    return {
        "spatial_corr": spatial_corr,
        "gradient_corr": gradient_corr, 
        "fourier_corr": fourier_corr
    }

def calculate_forecast_spread(
    predictions: List[np.ndarray]
) -> Dict[str, np.ndarray]:
    """
    Calculate spread statistics for an ensemble of forecasts.
    
    Args:
        predictions: List of forecast arrays (same shape)
    
    Returns:
        Dictionary with ensemble mean, standard deviation, etc.
    """
    if not predictions or len(predictions) < 2:
        return {}
    
    stacked = np.stack(predictions)
    mean = np.mean(stacked, axis=0)
    std = np.std(stacked, axis=0)
    spread = std
    min_vals = np.min(stacked, axis=0)
    max_vals = np.max(stacked, axis=0)
    
    p10 = np.percentile(stacked, 10, axis=0)
    p90 = np.percentile(stacked, 90, axis=0)
    
    return {
        "mean": mean,
        "std": std,
        "spread": spread,
        "min": min_vals,
        "max": max_vals,
        "p10": p10,
        "p90": p90
    }

def calculate_crps(
    prediction_quantiles: Dict[float, np.ndarray], 
    ground_truth: np.ndarray
) -> float:
    """
    Calculate Continuous Ranked Probability Score for probabilistic forecasts.
    
    Args:
        prediction_quantiles: Dictionary mapping quantile (0-1) to forecast value
        ground_truth: Ground truth data array
    
    Returns:
        CRPS value (lower is better)
    """
    quantiles = sorted(prediction_quantiles.keys())
    
    if len(quantiles) < 2:
        return float('inf')
    
    crps = 0.0
    
    for i in range(len(quantiles) - 1):
        q_low = quantiles[i]
        q_high = quantiles[i+1]
        
        val_low = prediction_quantiles[q_low]
        val_high = prediction_quantiles[q_high]
        prob_mass = q_high - q_low

        mask_below = ground_truth < val_low
        mask_above = ground_truth > val_high
        mask_between = ~(mask_below | mask_above)
        
        # Contribution for observations below segment
        crps += prob_mass * np.mean((val_low - ground_truth[mask_below])**2)
        # Contribution for observations above segment
        crps += prob_mass * np.mean((ground_truth[mask_above] - val_high)**2)
        # Contribution for observations within segment (linear interpolation)
        if np.any(mask_between):
            norm_pos = (ground_truth[mask_between] - val_low) / (val_high - val_low + 1e-8)
            # Weight for integration
            crps += prob_mass * np.mean((1.0 - norm_pos)**2)
    
    return crps

def calculate_all_metrics(
    prediction: np.ndarray,
    ground_truth: np.ndarray,
    climatology: Optional[np.ndarray] = None,
    variable_name: str = "unknown"
) -> Dict[str, float]:
    """
    Calculate a comprehensive set of metrics for a single variable.
    
    Args:
        prediction: Predicted data array
        ground_truth: Ground truth data array
        climatology: Optional climatology for anomaly-based metrics
        variable_name: Name of variable for normalization
    
    Returns:
        Dictionary with all calculated metrics
    """
    metrics = {}
    metrics["rmse"] = np.sqrt(np.mean((prediction - ground_truth)**2))
    metrics["normalized_rmse"] = normalize_rmse(prediction, ground_truth, variable_name)
    metrics["mae"] = calculate_mae(prediction, ground_truth)
    metrics["bias"] = calculate_bias(prediction, ground_truth)
    metrics["correlation"] = calculate_correlation(prediction, ground_truth)

    # MSE decomposition
    mse_components = calculate_mse_decomposition(prediction, ground_truth)
    metrics.update({f"mse_{key}": value for key, value in mse_components.items()})
    
    # Anomaly correlation if climatology available
    if climatology is not None:
        metrics["acc"] = calculate_anomaly_correlation(prediction, ground_truth, climatology)
    
    # Threshold-based metrics
    threshold_map = {
        "2t": 5.0,       # 5K deviation
        "msl": 1000.0,   # 1000Pa pressure difference
        "10u": 10.0,     # 10m/s wind
        "10v": 10.0,     # 10m/s wind
        "q": 0.01,       # 0.01 kg/kg specific humidity
    }
    threshold = threshold_map.get(variable_name, np.nanpercentile(ground_truth, 90))
    metrics["csi"] = calculate_csi(prediction, ground_truth, threshold)
    metrics["ets"] = calculate_ets(prediction, ground_truth, threshold)
    extreme_metrics = calculate_extreme_value_scores(prediction, ground_truth, 95.0)
    metrics.update({f"extreme_{key}": value for key, value in extreme_metrics.items()})
    
    if prediction.ndim == 2 and ground_truth.ndim == 2:
        spatial_metrics = calculate_spatial_pattern_metrics(prediction, ground_truth)
        metrics.update(spatial_metrics)
    
    return metrics