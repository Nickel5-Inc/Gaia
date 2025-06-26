"""
Async processing utilities for CPU-intensive weather operations.

This module replaces the multiprocessing approach with async/dask-based processing
for better resource management and simpler debugging.
"""

import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import traceback

import numpy as np
import xarray as xr
from pathlib import Path

try:
    import dask.array as da
    import dask
    DASK_AVAILABLE = True
except ImportError:
    da = None
    dask = None
    DASK_AVAILABLE = False

from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class AsyncProcessingConfig:
    """Configuration for async processing operations."""
    
    def __init__(self):
        # Set optimal dask configuration for weather operations
        if DASK_AVAILABLE:
            # Use threads for CPU-bound operations (better for numerical work)
            dask.config.set(scheduler='threads')
            # Optimize chunk sizes for weather data
            dask.config.set({'array.chunk-size': '128MB'})
            # Optimize for memory usage
            dask.config.set({'array.slicing.split_large_chunks': True})
            logger.info("🚀 Dask configured for async weather processing")
        else:
            logger.warning("⚠️ Dask not available - falling back to asyncio.to_thread")
    
    @staticmethod
    def should_use_async_processing(array_size: int, min_threshold: int = 10000) -> bool:
        """Determine if async processing should be used based on array size."""
        return array_size >= min_threshold


# Global configuration instance
_async_config = AsyncProcessingConfig()


async def process_era5_variable_async(
    era5_data: xr.Dataset,
    var_name: str,
    target_grid: xr.DataArray,
    interpolation_method: str = "linear"
) -> xr.DataArray:
    """
    Process a single ERA5 variable using async/dask approach.
    Handles coordinate transformations and interpolation.
    """
    def _process_variable():
        try:
            logger.debug(f"Async: Processing ERA5 variable {var_name}")
            
            if var_name not in era5_data:
                logger.warning(f"Async: Variable {var_name} not found in ERA5 data")
                return None
            
            var_data = era5_data[var_name]
            
            # Standardize coordinate names
            rename_dict = {}
            for dim_name in var_data.dims:
                if dim_name.lower() in ('latitude', 'lat_0'):
                    rename_dict[dim_name] = 'lat'
                elif dim_name.lower() in ('longitude', 'lon_0'):
                    rename_dict[dim_name] = 'lon'
            
            if rename_dict:
                var_data = var_data.rename(rename_dict)
            
            # Ensure latitude is descending
            if 'lat' in var_data.dims:
                lat_vals = var_data.lat.values
                if len(lat_vals) > 1 and lat_vals[0] < lat_vals[-1]:
                    var_data = var_data.isel(lat=slice(None, None, -1))
            
            # Use dask-backed interpolation if available
            if DASK_AVAILABLE and hasattr(var_data, 'chunk'):
                # Ensure data is chunked for parallel processing
                var_data = var_data.chunk({'lat': 50, 'lon': 50})
            
            # Interpolate to target grid
            result = var_data.interp_like(target_grid, method=interpolation_method)
            
            # Force computation
            result = result.compute()
            
            logger.debug(f"Async: Successfully processed ERA5 variable {var_name}")
            return result
            
        except Exception as e:
            logger.error(f"Async: Error processing ERA5 variable {var_name}: {e}")
            return None
    
    return await asyncio.to_thread(_process_variable)


async def compute_climatology_interpolation_async(
    climatology_data: np.ndarray,
    source_coords: Tuple[np.ndarray, np.ndarray],  # (lat, lon)
    target_coords: Tuple[np.ndarray, np.ndarray],  # (lat, lon)
    var_name: str,
    interpolation_method: str = "linear"
) -> np.ndarray:
    """
    Perform climatology interpolation using async/dask approach.
    Uses xarray's built-in interpolation with dask backend for efficiency.
    """
    def _interpolate_climatology():
        try:
            logger.debug(f"Async: Computing climatology interpolation for {var_name}")
            
            source_lat, source_lon = source_coords
            target_lat, target_lon = target_coords
            
            # Create xarray DataArray from numpy data for efficient interpolation
            if climatology_data.ndim == 3:  # (time, lat, lon)
                dims = ['time', 'lat', 'lon']
                coords = {
                    'time': range(climatology_data.shape[0]),
                    'lat': source_lat,
                    'lon': source_lon
                }
            else:  # (lat, lon)
                dims = ['lat', 'lon']
                coords = {
                    'lat': source_lat,
                    'lon': source_lon
                }
            
            # Create source DataArray
            source_da = xr.DataArray(
                climatology_data,
                dims=dims,
                coords=coords
            )
            
            # Create target grid
            target_da = xr.DataArray(
                np.zeros((len(target_lat), len(target_lon))),
                dims=['lat', 'lon'],
                coords={'lat': target_lat, 'lon': target_lon}
            )
            
            # Use dask chunking for large datasets
            if DASK_AVAILABLE and source_da.size > 50000:
                if climatology_data.ndim == 3:
                    source_da = source_da.chunk({'time': 1, 'lat': 50, 'lon': 50})
                else:
                    source_da = source_da.chunk({'lat': 50, 'lon': 50})
            
            # Perform interpolation using xarray's optimized methods
            result = source_da.interp_like(target_da, method=interpolation_method)
            
            # Compute and return numpy array
            result_computed = result.compute()
            result_array = result_computed.values.astype(np.float32)
            
            logger.debug(f"Async: Successfully computed climatology interpolation for {var_name}")
            return result_array
            
        except Exception as e:
            logger.error(f"Async: Error in climatology interpolation for {var_name}: {e}")
            logger.error(f"Async: Traceback: {traceback.format_exc()}")
            return None
    
    return await asyncio.to_thread(_interpolate_climatology)


async def compute_statistical_metrics_async(
    forecast_data: np.ndarray,
    truth_data: np.ndarray,
    reference_data: Optional[np.ndarray] = None,
    climatology_data: Optional[np.ndarray] = None,
    lat_weights: Optional[np.ndarray] = None,
    metrics: List[str] = None
) -> Dict[str, float]:
    """
    Compute statistical metrics using async/dask approach.
    Uses vectorized operations with dask arrays for large datasets.
    """
    if metrics is None:
        metrics = ['mse', 'rmse', 'corr']
    
    def _compute_metrics():
        try:
            logger.debug(f"Async: Computing statistical metrics: {metrics}")
            
            results = {}
            
            # Convert to float32 for memory efficiency
            forecast = np.asarray(forecast_data, dtype=np.float32)
            truth = np.asarray(truth_data, dtype=np.float32)
            
            if forecast.shape != truth.shape:
                logger.error(f"Async: Shape mismatch - forecast: {forecast.shape}, truth: {truth.shape}")
                return {metric: np.nan for metric in metrics}
            
            # Use dask arrays for large datasets
            use_dask = DASK_AVAILABLE and forecast.size > 50000
            
            if use_dask:
                logger.debug("Async: Using dask arrays for large dataset metrics computation")
                # Create dask arrays with optimal chunking
                chunk_size = min(10000, forecast.size // 4)
                forecast_da = da.from_array(forecast, chunks=chunk_size)
                truth_da = da.from_array(truth, chunks=chunk_size)
                
                # Create valid data mask
                valid_mask_da = ~(da.isnan(forecast_da) | da.isnan(truth_da))
                
                if da.sum(valid_mask_da).compute() == 0:
                    logger.warning("Async: No valid data points for metrics computation")
                    return {metric: np.nan for metric in metrics}
                
                # Extract valid data
                forecast_valid_da = forecast_da[valid_mask_da]
                truth_valid_da = truth_da[valid_mask_da]
                
                # Apply weights if provided
                if lat_weights is not None:
                    weights_da = da.from_array(lat_weights, chunks=chunk_size)[valid_mask_da]
                    weights_da = weights_da / da.sum(weights_da)
                else:
                    weights_da = None
                
                # Compute metrics using dask operations
                if 'bias' in metrics:
                    diff = forecast_valid_da - truth_valid_da
                    if weights_da is not None:
                        weighted_sum = da.sum(diff * weights_da)
                        weights_sum = da.sum(weights_da)
                        results['bias'] = float((weighted_sum / weights_sum).compute())
                    else:
                        results['bias'] = float(da.mean(diff).compute())
                
                if 'mae' in metrics:
                    abs_error = da.abs(forecast_valid_da - truth_valid_da)
                    if weights_da is not None:
                        weighted_sum = da.sum(abs_error * weights_da)
                        weights_sum = da.sum(weights_da)
                        results['mae'] = float((weighted_sum / weights_sum).compute())
                    else:
                        results['mae'] = float(da.mean(abs_error).compute())
                
                if 'mse' in metrics or 'rmse' in metrics or 'skill' in metrics:
                    squared_error = (forecast_valid_da - truth_valid_da) ** 2
                    if weights_da is not None:
                        # For dask arrays, we need to ensure shapes match or use axis parameter
                        weighted_sum = da.sum(squared_error * weights_da)
                        weights_sum = da.sum(weights_da)
                        mse = float((weighted_sum / weights_sum).compute())
                    else:
                        mse = float(da.mean(squared_error).compute())
                    
                    if 'mse' in metrics:
                        results['mse'] = mse
                    if 'rmse' in metrics:
                        results['rmse'] = float(np.sqrt(mse))
                
                if 'corr' in metrics:
                    # Simple correlation with dask
                    forecast_valid_for_corr = forecast_valid_da.compute()
                    truth_valid_for_corr = truth_valid_da.compute()
                    if len(forecast_valid_for_corr) > 1:
                        corr_matrix = np.corrcoef(forecast_valid_for_corr, truth_valid_for_corr)
                        results['corr'] = float(corr_matrix[0, 1])
                    else:
                        results['corr'] = np.nan
                
            else:
                # Standard numpy approach for smaller datasets
                logger.debug("Async: Using numpy arrays for metrics computation")
                
                # Create valid data mask
                valid_mask = ~(np.isnan(forecast) | np.isnan(truth))
                if np.sum(valid_mask) == 0:
                    logger.warning("Async: No valid data points for metrics computation")
                    return {metric: np.nan for metric in metrics}
                
                forecast_valid = forecast[valid_mask]
                truth_valid = truth[valid_mask]
                
                # Apply weights if provided
                if lat_weights is not None:
                    weights = np.asarray(lat_weights, dtype=np.float32)[valid_mask]
                    weights = weights / np.sum(weights)
                else:
                    weights = None
                
                # Compute metrics using numpy
                if 'bias' in metrics:
                    if weights is not None:
                        results['bias'] = float(np.average(forecast_valid - truth_valid, weights=weights))
                    else:
                        results['bias'] = float(np.mean(forecast_valid - truth_valid))
                
                if 'mae' in metrics:
                    abs_error = np.abs(forecast_valid - truth_valid)
                    if weights is not None:
                        results['mae'] = float(np.average(abs_error, weights=weights))
                    else:
                        results['mae'] = float(np.mean(abs_error))
                
                if 'mse' in metrics or 'rmse' in metrics or 'skill' in metrics:
                    squared_error = (forecast_valid - truth_valid) ** 2
                    if weights is not None:
                        mse = float(np.average(squared_error, weights=weights))
                    else:
                        mse = float(np.mean(squared_error))
                    
                    if 'mse' in metrics:
                        results['mse'] = mse
                    if 'rmse' in metrics:
                        results['rmse'] = float(np.sqrt(mse))
                
                if 'corr' in metrics:
                    # Correlation computation
                    if weights is not None:
                        # Weighted correlation
                        forecast_mean = np.average(forecast_valid, weights=weights)
                        truth_mean = np.average(truth_valid, weights=weights)
                        
                        forecast_centered = forecast_valid - forecast_mean
                        truth_centered = truth_valid - truth_mean
                        
                        cov = np.average(forecast_centered * truth_centered, weights=weights)
                        var_forecast = np.average(forecast_centered ** 2, weights=weights)
                        var_truth = np.average(truth_centered ** 2, weights=weights)
                        
                        if var_forecast > 0 and var_truth > 0:
                            results['corr'] = float(cov / np.sqrt(var_forecast * var_truth))
                        else:
                            results['corr'] = np.nan
                    else:
                        if len(forecast_valid) > 1:
                            corr_matrix = np.corrcoef(forecast_valid, truth_valid)
                            results['corr'] = float(corr_matrix[0, 1])
                        else:
                            results['corr'] = np.nan
            
            # Handle skill score and ACC - ensure variables are properly computed for both dask and numpy paths
            
            # Get computed arrays for skill score and ACC calculations
            if use_dask:
                # Compute numpy arrays from dask arrays for final calculations
                forecast_valid_np = forecast_valid_da.compute()
                truth_valid_np = truth_valid_da.compute()
                valid_mask_np = valid_mask_da.compute()
                if lat_weights is not None:
                    weights_np = weights_da.compute()
                else:
                    weights_np = None
            else:
                # Use existing numpy arrays - ensure they exist
                if 'forecast_valid' in locals() and 'truth_valid' in locals():
                    forecast_valid_np = forecast_valid
                    truth_valid_np = truth_valid
                    valid_mask_np = valid_mask
                    weights_np = weights if 'weights' in locals() else None
                else:
                    # Fallback: recompute from original arrays
                    valid_mask_np = ~(np.isnan(forecast) | np.isnan(truth))
                    forecast_valid_np = forecast[valid_mask_np]
                    truth_valid_np = truth[valid_mask_np]
                    if lat_weights is not None:
                        weights_np = np.asarray(lat_weights, dtype=np.float32)[valid_mask_np]
                        weights_np = weights_np / np.sum(weights_np)
                    else:
                        weights_np = None
            
            if 'skill' in metrics and reference_data is not None:
                reference = np.asarray(reference_data, dtype=np.float32)
                if reference.shape == truth.shape:
                    ref_valid = reference[valid_mask_np]
                    ref_squared_error = (ref_valid - truth_valid_np) ** 2
                    
                    if weights_np is not None:
                        mse_ref = float(np.average(ref_squared_error, weights=weights_np))
                    else:
                        mse_ref = float(np.mean(ref_squared_error))
                    
                    if 'mse' in locals() and mse_ref > 0:
                        results['skill'] = float(1.0 - (mse / mse_ref))
                    else:
                        results['skill'] = 1.0 if 'mse' in locals() and mse == 0 else -np.inf
                else:
                    results['skill'] = np.nan
            
            if 'acc' in metrics and climatology_data is not None:
                climatology = np.asarray(climatology_data, dtype=np.float32)
                if climatology.shape == truth.shape:
                    clim_valid = climatology[valid_mask_np]
                    
                    # Compute anomalies
                    forecast_anom = forecast_valid_np - clim_valid
                    truth_anom = truth_valid_np - clim_valid
                    
                    # ACC is correlation of anomalies
                    if weights_np is not None:
                        # Weighted correlation of anomalies
                        forecast_anom_mean = np.average(forecast_anom, weights=weights_np)
                        truth_anom_mean = np.average(truth_anom, weights=weights_np)
                        
                        forecast_anom_centered = forecast_anom - forecast_anom_mean
                        truth_anom_centered = truth_anom - truth_anom_mean
                        
                        cov = np.average(forecast_anom_centered * truth_anom_centered, weights=weights_np)
                        var_forecast = np.average(forecast_anom_centered ** 2, weights=weights_np)
                        var_truth = np.average(truth_anom_centered ** 2, weights=weights_np)
                        
                        if var_forecast > 0 and var_truth > 0:
                            results['acc'] = float(cov / np.sqrt(var_forecast * var_truth))
                        else:
                            results['acc'] = np.nan
                    else:
                        if len(forecast_anom) > 1:
                            corr_matrix = np.corrcoef(forecast_anom, truth_anom)
                            results['acc'] = float(corr_matrix[0, 1])
                        else:
                            results['acc'] = np.nan
                else:
                    results['acc'] = np.nan
            
            logger.debug(f"Async: Successfully computed metrics: {list(results.keys())}")
            return results
            
        except Exception as e:
            logger.error(f"Async: Error computing statistical metrics: {e}")
            logger.error(f"Async: Traceback: {traceback.format_exc()}")
            return {metric: np.nan for metric in metrics}
    
    return await asyncio.to_thread(_compute_metrics)


async def process_coordinate_transformation_async(
    data_array: np.ndarray,
    source_coords: Dict[str, np.ndarray],
    target_coords: Dict[str, np.ndarray],
    transformation_type: str = "interpolation"
) -> np.ndarray:
    """
    Process coordinate transformation using async/dask approach.
    Uses xarray's optimized coordinate transformation methods.
    """
    def _transform_coordinates():
        try:
            logger.debug(f"Async: Computing coordinate transformation: {transformation_type}")
            
            # Create source DataArray
            source_da = xr.DataArray(
                data_array,
                dims=list(source_coords.keys()),
                coords=source_coords
            )
            
            # Create target DataArray for the grid
            target_dims = list(target_coords.keys())
            target_shape = [len(target_coords[dim]) for dim in target_dims]
            target_data = np.zeros(target_shape)
            
            target_da = xr.DataArray(
                target_data,
                dims=target_dims,
                coords=target_coords
            )
            
            # Use dask chunking for large arrays
            if DASK_AVAILABLE and source_da.size > 50000:
                # Determine optimal chunk sizes based on dimensions
                chunk_dict = {}
                for dim in source_da.dims:
                    dim_size = source_da.sizes[dim]
                    chunk_dict[dim] = min(50, dim_size)
                source_da = source_da.chunk(chunk_dict)
            
            # Perform transformation based on type
            if transformation_type == "interpolation":
                result = source_da.interp_like(target_da, method="linear")
            else:
                # Default to interpolation
                result = source_da.interp_like(target_da, method="linear")
            
            # Compute and return result
            result_computed = result.compute()
            return result_computed.values.astype(np.float32)
            
        except Exception as e:
            logger.error(f"Async: Error in coordinate transformation: {e}")
            logger.error(f"Async: Traceback: {traceback.format_exc()}")
            return None
    
    return await asyncio.to_thread(_transform_coordinates)


def log_async_processing_summary():
    """Log a summary of async processing capabilities."""
    logger.info("🎯 Weather Task Async Processing Summary:")
    logger.info(f"   Dask Available: {'✅' if DASK_AVAILABLE else '❌'}")
    if DASK_AVAILABLE:
        logger.info(f"   Scheduler: {dask.config.get('scheduler', 'default')}")
        logger.info(f"   Chunk Size: {dask.config.get('array.chunk-size', 'default')}")
    logger.info("   Processing Methods:")
    logger.info("     - ERA5 Variable Processing: async/dask interpolation")
    logger.info("     - Climatology Interpolation: xarray-based with dask chunking")
    logger.info("     - Statistical Metrics: vectorized dask arrays")
    logger.info("     - Coordinate Transformation: xarray optimized methods")
    logger.info("   Benefits over multiprocessing:")
    logger.info("     - Simpler debugging and error handling")
    logger.info("     - Better memory management with dask")
    logger.info("     - No process overhead or serialization costs")
    logger.info("     - Seamless integration with xarray/dask ecosystem") 