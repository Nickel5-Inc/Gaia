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

async def _compute_metric_to_scalar_array(metric_func, *args, **kwargs):
    """
    Helper function to compute metrics and convert to scalar using async threading.
    Handles both numpy arrays and dask arrays properly.
    """
    result = await asyncio.to_thread(metric_func, *args, **kwargs)
    
    # Force computation if it's a dask array
    if hasattr(result, 'compute'):
        result = await asyncio.to_thread(result.compute)
    
    return result

def _get_metric_as_float(metric_fn, *args, **kwargs):
    """
    Synchronous helper that directly calls the metric function.
    This function is called via asyncio.to_thread() so it should be synchronous.
    Handles both numpy arrays and dask arrays properly.
    """
    computed_array = metric_fn(*args, **kwargs)
    
    # Force computation if it's a dask array
    if hasattr(computed_array, 'compute'):
        computed_array = computed_array.compute()
    
    if computed_array.ndim == 0:
        return float(computed_array.item())
    elif computed_array.ndim == 1:
        logger.debug(f"Metric returned 1D array (shape: {computed_array.shape}), taking mean for scalar result.")
        mean_val = computed_array.mean()
        # Force computation again if mean result is also dask
        if hasattr(mean_val, 'compute'):
            mean_val = mean_val.compute()
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
    """
    Calculates the MSE-based skill score: 1 - (MSE_forecast / MSE_reference).
    Enhanced with async processing for large datasets.
    """
    try:
        spatial_dims = [d for d in forecast_bc_da.dims if d.lower() in ('latitude', 'longitude', 'lat', 'lon')]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for MSE skill score.")
            return -np.inf

        # Check if we should use async processing for large datasets
        use_async_processing = forecast_bc_da.size > 50000  # Use async for large arrays
        
        if use_async_processing:
            logger.debug("Using async processing for MSE skill score calculation")
            
            try:
                from ..utils.async_processing import compute_statistical_metrics_async
                
                # Convert to numpy arrays for async processing
                forecast_data = forecast_bc_da.values
                truth_data = truth_da.values
                reference_data = reference_da.values
                weights_data = lat_weights.values if lat_weights is not None else None
                
                # Use async processing for metrics calculation
                metrics_result = await compute_statistical_metrics_async(
                    forecast_data,
                    truth_data,
                    reference_data,
                    None,  # No climatology needed for skill score
                    weights_data,
                    ['skill']  # Only compute skill score
                )
                
                if metrics_result and 'skill' in metrics_result:
                    skill_score = metrics_result['skill']
                    logger.info(f"MSE Skill Score (Async): {skill_score:.4f}")
                    return skill_score
                else:
                    logger.warning("Async skill score calculation failed, falling back to xskillscore")
                    # Fall through to xskillscore calculation
                    
            except Exception as async_error:
                logger.warning(f"Async MSE calculation failed: {async_error}, falling back to xskillscore")
                # Fall through to xskillscore calculation

        # Use existing xskillscore calculation (fallback or small datasets)
        mse_forecast_scalar = await asyncio.to_thread(
            _compute_metric_to_scalar_array, xs.mse, forecast_bc_da, truth_da, dim=spatial_dims, weights=lat_weights, skipna=True
        )
        mse_reference_scalar = await asyncio.to_thread(
            _compute_metric_to_scalar_array, xs.mse, reference_da, truth_da, dim=spatial_dims, weights=lat_weights, skipna=True
        )

        # Ensure results are computed if they're dask arrays
        if hasattr(mse_forecast_scalar, 'compute'):
            mse_forecast_scalar = await asyncio.to_thread(mse_forecast_scalar.compute)
        if hasattr(mse_reference_scalar, 'compute'):
            mse_reference_scalar = await asyncio.to_thread(mse_reference_scalar.compute)

        if mse_reference_scalar.item() == 0:
            skill_score = 1.0 if mse_forecast_scalar.item() == 0 else -np.inf
        else:
            skill_score = 1 - (mse_forecast_scalar.item() / mse_reference_scalar.item())

        logger.info(f"MSE Skill Score (xskillscore): {skill_score:.4f}")
        return skill_score

    except Exception as e:
        logger.error(f"Error calculating MSE skill score: {e}")
        return -np.inf

async def calculate_acc(
    forecast_da: xr.DataArray, 
    truth_da: xr.DataArray,
    climatology_da: xr.DataArray,
    lat_weights: xr.DataArray
) -> float:
    """
    Calculates the Anomaly Correlation Coefficient (ACC).
    Enhanced with async processing for large datasets.
    """
    try:
        spatial_dims = [d for d in forecast_da.dims if d.lower() in ('latitude', 'longitude', 'lat', 'lon')]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for ACC.")
            return -np.inf

        # Check if we should use async processing for large datasets
        use_async_processing = forecast_da.size > 50000  # Use async for large arrays
        
        if use_async_processing:
            logger.debug("Using async processing for ACC calculation")
            
            try:
                from ..utils.async_processing import compute_statistical_metrics_async
                
                # Convert to numpy arrays for async processing
                forecast_data = forecast_da.values
                truth_data = truth_da.values
                climatology_data = climatology_da.values
                weights_data = lat_weights.values if lat_weights is not None else None
                
                # Use async processing for ACC calculation
                metrics_result = await compute_statistical_metrics_async(
                    forecast_data,
                    truth_data,
                    None,  # No reference needed for ACC
                    climatology_data,
                    weights_data,
                    ['acc']  # Only compute ACC
                )
                
                if metrics_result and 'acc' in metrics_result:
                    acc_float = metrics_result['acc']
                    logger.info(f"ACC calculated (Async): {acc_float:.4f}")
                    return acc_float
                else:
                    logger.warning("Async ACC calculation failed, falling back to xskillscore")
                    # Fall through to xskillscore calculation
                    
            except Exception as async_error:
                logger.warning(f"Async ACC calculation failed: {async_error}, falling back to xskillscore")
                # Fall through to xskillscore calculation

        # Use existing xskillscore calculation (fallback or small datasets)
        forecast_anom = forecast_da - climatology_da
        truth_anom = truth_da - climatology_da

        logger.info(f"[calculate_acc] forecast_anom shape: {forecast_anom.shape}, dims: {forecast_anom.dims}, type: {type(forecast_anom.data)}")
        logger.info(f"[calculate_acc] truth_anom shape: {truth_anom.shape}, dims: {truth_anom.dims}, type: {type(truth_anom.data)}")

        acc_scalar = await asyncio.to_thread(
            _compute_metric_to_scalar_array, xs.pearson_r, forecast_anom, truth_anom, dim=spatial_dims, weights=lat_weights, skipna=True
        )

        # Ensure result is computed if it's a dask array
        if hasattr(acc_scalar, 'compute'):
            acc_scalar = await asyncio.to_thread(acc_scalar.compute)

        acc_float = acc_scalar.item()
        logger.info(f"ACC calculated (xskillscore): {acc_float:.4f}")
        return acc_float

    except Exception as e:
        logger.error(f"Error calculating ACC: {e}")
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


async def calculate_rmse(
    forecast_da: xr.DataArray, 
    truth_da: xr.DataArray, 
    lat_weights: xr.DataArray
) -> float:
    """
    Calculates the Root Mean Square Error (RMSE) using async processing.
    """
    try:
        spatial_dims = [d for d in forecast_da.dims if d.lower() in ('latitude', 'longitude', 'lat', 'lon')]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for RMSE.")
            return np.inf

        # Use async processing for large datasets
        if forecast_da.size > 50000:
            logger.debug("Using async processing for RMSE calculation")
            try:
                from ..utils.async_processing import compute_statistical_metrics_async
                
                forecast_data = forecast_da.values
                truth_data = truth_da.values
                weights_data = lat_weights.values if lat_weights is not None else None
                
                metrics_result = await compute_statistical_metrics_async(
                    forecast_data,
                    truth_data,
                    None, None,  # No reference or climatology needed
                    weights_data,
                    ['rmse']
                )
                
                if metrics_result and 'rmse' in metrics_result:
                    rmse_value = metrics_result['rmse']
                    logger.info(f"RMSE calculated (Async): {rmse_value:.4f}")
                    return rmse_value
                    
            except Exception as async_error:
                logger.warning(f"Async RMSE calculation failed: {async_error}, falling back to xskillscore")

        # Fallback to xskillscore
        rmse_scalar = await asyncio.to_thread(
            _compute_metric_to_scalar_array, xs.rmse, forecast_da, truth_da, dim=spatial_dims, weights=lat_weights, skipna=True
        )
        
        # Ensure result is computed if it's a dask array
        if hasattr(rmse_scalar, 'compute'):
            rmse_scalar = await asyncio.to_thread(rmse_scalar.compute)
            
        rmse_value = rmse_scalar.item()
        logger.info(f"RMSE calculated (xskillscore): {rmse_value:.4f}")
        return rmse_value

    except Exception as e:
        logger.error(f"Error calculating RMSE: {e}")
        return np.inf


async def calculate_bias(
    forecast_da: xr.DataArray, 
    truth_da: xr.DataArray, 
    lat_weights: xr.DataArray
) -> float:
    """
    Calculates the bias (mean error) using async processing.
    """
    try:
        spatial_dims = [d for d in forecast_da.dims if d.lower() in ('latitude', 'longitude', 'lat', 'lon')]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for bias.")
            return np.nan

        # Use async processing for large datasets
        if forecast_da.size > 50000:
            logger.debug("Using async processing for bias calculation")
            try:
                from ..utils.async_processing import compute_statistical_metrics_async
                
                forecast_data = forecast_da.values
                truth_data = truth_da.values
                weights_data = lat_weights.values if lat_weights is not None else None
                
                metrics_result = await compute_statistical_metrics_async(
                    forecast_data,
                    truth_data,
                    None, None,  # No reference or climatology needed
                    weights_data,
                    ['bias']
                )
                
                if metrics_result and 'bias' in metrics_result:
                    bias_value = metrics_result['bias']
                    logger.info(f"Bias calculated (Async): {bias_value:.4f}")
                    return bias_value
                    
            except Exception as async_error:
                logger.warning(f"Async bias calculation failed: {async_error}, falling back to manual calculation")

        # Fallback to manual calculation
        diff = forecast_da - truth_da
        if lat_weights is not None:
            weighted_diff = diff * lat_weights
            weighted_sum = weighted_diff.sum()
            weights_sum = lat_weights.sum()
            
            # Compute dask arrays if needed
            if hasattr(weighted_sum, 'compute'):
                weighted_sum = await asyncio.to_thread(weighted_sum.compute)
            if hasattr(weights_sum, 'compute'):
                weights_sum = await asyncio.to_thread(weights_sum.compute)
                
            bias_value = float(weighted_sum / weights_sum)
        else:
            mean_diff = diff.mean()
            
            # Compute dask array if needed
            if hasattr(mean_diff, 'compute'):
                mean_diff = await asyncio.to_thread(mean_diff.compute)
                
            bias_value = float(mean_diff)
            
        logger.info(f"Bias calculated (manual): {bias_value:.4f}")
        return bias_value

    except Exception as e:
        logger.error(f"Error calculating bias: {e}")
        return np.nan

