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
        bias = await asyncio.to_thread(error.mean, dim=spatial_dims)
        forecast_bc_da = forecast_da - bias
        logger.debug(f"Bias correction calculated. Mean bias: {bias.values}")
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

        mse_forecast = await asyncio.to_thread(
            xs.mse, forecast_bc_da, truth_da, dim=spatial_dims, weights=lat_weights, skipna=True
        )
        mse_reference = await asyncio.to_thread(
            xs.mse, reference_da, truth_da, dim=spatial_dims, weights=lat_weights, skipna=True
        )

        if mse_reference == 0:
            if mse_forecast == 0:
                return 1.0
            return -np.inf
        
        skill_score = 1.0 - (mse_forecast / mse_reference)
        logger.debug(f"MSE Skill Score: 1.0 - ({mse_forecast.item():.4f} / {mse_reference.item():.4f}) = {skill_score.item():.4f}")
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

        acc_value = await asyncio.to_thread(
            xs.pearson_r, forecast_anom, truth_anom, dim=spatial_dims, weights=lat_weights, skipna=True
        )
        logger.debug(f"ACC calculated: {acc_value.item():.4f}")
        return float(acc_value.item())
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
        actual_min = float(forecast_da.min().item())
        actual_max = float(forecast_da.max().item())
        results["climatology_min_actual"] = actual_min
        results["climatology_max_actual"] = actual_max

        passed_general_bounds = True
        bounds = climatology_bounds_config.get(variable_name)
        if bounds:
            min_val, max_val = bounds
            if not (actual_min >= min_val and actual_max <= max_val):
                passed_general_bounds = False
                logger.warning(f"Climatology check FAILED for {variable_name}. Expected: {bounds}, Got: ({actual_min:.2f}, {actual_max:.2f})")
        else:
            logger.debug(f"No general climatology bounds configured for {variable_name}. Skipping general bounds part of check.")

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
            correlation = await asyncio.to_thread(
                xs.pearson_r, forecast_da, reference_da_for_corr, dim=spatial_dims, weights=lat_weights, skipna=True
            )
            results["pattern_correlation_value"] = float(correlation.item())
            if results["pattern_correlation_value"] >= pattern_corr_threshold:
                results["pattern_correlation_passed"] = True
            else:
                results["pattern_correlation_passed"] = False
                logger.warning(f"Pattern correlation FAILED for {variable_name}. Expected >= {pattern_corr_threshold}, Got: {results['pattern_correlation_value']:.2f}")
                
    except Exception as e:
        logger.error(f"Error during sanity checks for {variable_name}: {e}", exc_info=True)
        if results["climatology_passed"] is None: results["climatology_passed"] = False
        if results["pattern_correlation_passed"] is None: results["pattern_correlation_passed"] = False
        
    logger.debug(f"Sanity check results for {variable_name}: {results}")
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

