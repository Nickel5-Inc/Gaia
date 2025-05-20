import numpy as np
import xarray as xr
import xskillscore as xs
import asyncio
from typing import Dict, Any, Union, Optional, Tuple, List
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

"""
Core metric calculations for weather forecast evaluation.
This module provides implementations of common metrics for
evaluating weather forecasts, including RMSE, MAE, bias, correlation,
"""

def _compute_metric_to_scalar_array(metric_fn, *args, **kwargs):
    res = metric_fn(*args, **kwargs)
    if hasattr(res, 'compute'):
        res = res.compute()
    return res

def _get_metric_as_float(metric_fn, *args, **kwargs):
    computed_array = _compute_metric_to_scalar_array(metric_fn, *args, **kwargs)
    if computed_array.ndim == 0:
        return float(computed_array.item())
    elif computed_array.ndim == 1:
        logger.debug(f"Metric returned 1D array (shape: {computed_array.shape}), taking mean for scalar result.")
        mean_val = computed_array.mean()
        return float(mean_val.item())
    else:
        raise ValueError(f"Metric calculation resulted in an array with unexpected dimensions: {computed_array.shape}. Cannot convert to single float.")

def _calculate_latitude_weights(lat_da: xr.DataArray) -> xr.DataArray:
    return np.cos(np.deg2rad(lat_da)).where(np.isfinite(lat_da), 0)

async def calculate_bias_corrected_forecast(
    forecast_da: xr.DataArray, 
    truth_da: xr.DataArray
) -> xr.DataArray:
    """Calculates a bias-corrected forecast by subtracting the spatial mean error."""
    try:
        if not isinstance(forecast_da, xr.DataArray) or not isinstance(truth_da, xr.DataArray):
            logger.error("Inputs to calculate_bias_corrected_forecast must be xr.DataArray")
            raise TypeError("Inputs must be xr.DataArray")

        spatial_dims = [d for d in forecast_da.dims if d.lower() in ('latitude', 'longitude', 'lat', 'lon')]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for bias correction.")
            return forecast_da

        error = forecast_da - truth_da
        bias_uncomputed = error.mean(dim=spatial_dims) 
        if hasattr(bias_uncomputed, 'compute'):
            bias = await asyncio.to_thread(bias_uncomputed.compute)
        else:
            bias = bias_uncomputed
            
        forecast_bc_da = forecast_da - bias
        logger.info(f"Bias correction calculated. Mean bias: {bias.values}")
        return forecast_bc_da
    except Exception as e:
        logger.error(f"Error in calculate_bias_corrected_forecast: {e}", exc_info=True)
        return forecast_da

async def calculate_mse_skill_score(
    forecast_bc_da: xr.DataArray, 
    truth_da: xr.DataArray, 
    reference_da: xr.DataArray,
    lat_weights: xr.DataArray
) -> float:
    """Calculates the MSE-based skill score: 1 - (MSE_forecast / MSE_reference)."""
    try:
        spatial_dims = [d for d in forecast_bc_da.dims if d.lower() in ('latitude', 'longitude', 'lat', 'lon')]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for MSE skill score.")
            return -np.inf

        mse_forecast_scalar = await asyncio.to_thread(
            _compute_metric_to_scalar_array, xs.mse, forecast_bc_da, truth_da, dim=spatial_dims, weights=lat_weights, skipna=True
        )
        mse_reference_scalar = await asyncio.to_thread(
            _compute_metric_to_scalar_array, xs.mse, reference_da, truth_da, dim=spatial_dims, weights=lat_weights, skipna=True
        )

        if mse_reference_scalar == 0:
            if mse_forecast_scalar == 0:
                return 1.0
            return -np.inf
        
        skill_score = 1.0 - (mse_forecast_scalar / mse_reference_scalar)
        logger.info(f"MSE Skill Score: 1.0 - ({mse_forecast_scalar.item():.4f} / {mse_reference_scalar.item():.4f}) = {skill_score.item():.4f}")
        return float(skill_score.item())
    except Exception as e:
        logger.error(f"Error in calculate_mse_skill_score: {e}", exc_info=True)
        return -np.inf

async def calculate_acc(
    forecast_da: xr.DataArray, 
    truth_da: xr.DataArray,
    climatology_da: xr.DataArray,
    lat_weights: xr.DataArray
) -> float:
    """Calculates the Anomaly Correlation Coefficient (ACC)."""
    try:
        spatial_dims = [d for d in forecast_da.dims if d.lower() in ('latitude', 'longitude', 'lat', 'lon')]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for ACC.")
            return -np.inf

        forecast_anom = forecast_da - climatology_da
        truth_anom = truth_da - climatology_da

        logger.info(f"[calculate_acc] forecast_anom shape: {forecast_anom.shape}, dims: {forecast_anom.dims}, type: {type(forecast_anom.data)}")
        logger.info(f"[calculate_acc] truth_anom shape: {truth_anom.shape}, dims: {truth_anom.dims}, type: {type(truth_anom.data)}")
        if lat_weights is not None:
            logger.info(f"[calculate_acc] lat_weights shape: {lat_weights.shape}, dims: {lat_weights.dims}, type: {type(lat_weights.data)}")
        else:
            logger.info("[calculate_acc] lat_weights is None")
        logger.info(f"[calculate_acc] spatial_dims for pearson_r: {spatial_dims}")

        acc_float = await asyncio.to_thread(
            _get_metric_as_float, xs.pearson_r, forecast_anom, truth_anom, dim=spatial_dims, weights=lat_weights, skipna=True
        )
        logger.info(f"ACC calculated: {acc_float:.4f}")
        return acc_float
    except Exception as e:
        logger.error(f"Error in calculate_acc: {e}", exc_info=True)
        return -np.inf

async def perform_sanity_checks(
    forecast_da: xr.DataArray,
    reference_da_for_corr: xr.DataArray,
    variable_name: str,
    climatology_bounds_config: Dict[str, Tuple[float, float]],
    pattern_corr_threshold: float,
    lat_weights: xr.DataArray
) -> Dict[str, any]:
    """
    Performs climatological bounds check and pattern correlation against a reference.
    Returns a dictionary with check statuses and values.
    """
    results = {
        "climatology_passed": None,
        "climatology_min_actual": None,
        "climatology_max_actual": None,
        "pattern_correlation_passed": None,
        "pattern_correlation_value": None
    }
    spatial_dims = [d for d in forecast_da.dims if d.lower() in ('latitude', 'longitude', 'lat', 'lon')]

    try:
        min_val_uncomputed = forecast_da.min()
        if hasattr(min_val_uncomputed, 'compute'):
            actual_min_scalar = await asyncio.to_thread(min_val_uncomputed.compute)
        else:
            actual_min_scalar = min_val_uncomputed
        actual_min = float(actual_min_scalar.item())

        max_val_uncomputed = forecast_da.max()
        if hasattr(max_val_uncomputed, 'compute'):
            actual_max_scalar = await asyncio.to_thread(max_val_uncomputed.compute)
        else:
            actual_max_scalar = max_val_uncomputed
        actual_max = float(actual_max_scalar.item())
        
        results["climatology_min_actual"] = actual_min
        results["climatology_max_actual"] = actual_max

        passed_general_bounds = True
        bounds = climatology_bounds_config.get(variable_name)
        if bounds:
            min_bound, max_bound = bounds
            if not (actual_min >= min_bound and actual_max <= max_bound):
                passed_general_bounds = False
                logger.warning(f"Climatology check FAILED for {variable_name}. Expected: {bounds}, Got: ({actual_min:.2f}, {actual_max:.2f})")
        else:
            logger.info(f"No general climatology bounds configured for {variable_name}. Skipping general bounds part of check.")

        passed_specific_physical_checks = True
        if variable_name.startswith('q'):
            if actual_min < 0.0:
                passed_specific_physical_checks = False
                logger.warning(f"Physicality check FAILED for humidity {variable_name}: min value {actual_min:.4f} is negative.")
        
        results["climatology_passed"] = passed_general_bounds and passed_specific_physical_checks
        if not results["climatology_passed"] and passed_general_bounds and not passed_specific_physical_checks:
             logger.warning(f"Climatology check FAILED for {variable_name} due to specific physical constraint violation.")

        if not spatial_dims:
            logger.warning(f"No spatial dimensions for pattern correlation on {variable_name}. Skipping.")
            results["pattern_correlation_passed"] = True
        else:
            correlation_float = await asyncio.to_thread(
                _get_metric_as_float, xs.pearson_r, forecast_da, reference_da_for_corr, dim=spatial_dims, weights=lat_weights, skipna=True
            )
            results["pattern_correlation_value"] = correlation_float
            if correlation_float >= pattern_corr_threshold:
                results["pattern_correlation_passed"] = True
            else:
                results["pattern_correlation_passed"] = False
                logger.warning(f"Pattern correlation FAILED for {variable_name}. Expected >= {pattern_corr_threshold}, Got: {results['pattern_correlation_value']:.2f}")
                
    except Exception as e:
        logger.error(f"Error during sanity checks for {variable_name}: {e}", exc_info=True)
        if results["climatology_passed"] is None: results["climatology_passed"] = False
        if results["pattern_correlation_passed"] is None: results["pattern_correlation_passed"] = False
        
    logger.info(f"Sanity check results for {variable_name}: {results}")
    return results


async def calculate_rmse(prediction: Dict[str, np.ndarray], ground_truth: Dict[str, np.ndarray]) -> float:
    """
    Calculate overall RMSE across all variables in the provided dictionaries.
    Assumes variable values are numpy arrays. This version is kept for potential
    use in final ERA5 scoring if inputs are dicts of numpy arrays.
    
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
            logger.warning("calculate_rmse (dict input): No common variables found.")
            return float('inf')
            
        for var in common_vars:
            pred_data = prediction[var]
            truth_data = ground_truth[var]
            
            if not isinstance(pred_data, np.ndarray) or not isinstance(truth_data, np.ndarray):
                logger.warning(f"calculate_rmse (dict input): Skipping var '{var}', data is not a numpy array.")
                continue
                
            if pred_data.shape != truth_data.shape:
                logger.warning(f"calculate_rmse (dict input): Skipping var '{var}', shape mismatch {pred_data.shape} vs {truth_data.shape}.")
                continue
                    
            squared_error = np.square(pred_data - truth_data)
            valid_mask = ~np.isnan(squared_error)
            
            total_squared_error += np.sum(squared_error[valid_mask])
            total_count += np.sum(valid_mask)
        
        if total_count > 0:
            return np.sqrt(total_squared_error / total_count)
        else:
            logger.warning("calculate_rmse (dict input): No valid (non-NaN) data points found for comparison.")
            return float('inf')
    except Exception as e:
        logger.error(f"Error in calculate_rmse (dict input): {e}", exc_info=True)
        return float('inf')

