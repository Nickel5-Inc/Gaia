"""
Memory-aware multiprocessing pool for CPU-intensive weather operations.

This module provides a multiprocessing pool specifically designed for the weather task
that intelligently manages memory usage and process creation to optimize performance
while preventing OOM conditions.
"""

import asyncio
import gc
import multiprocessing as mp
import os
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import traceback

import numpy as np
import xarray as xr
from pathlib import Path

from fiber.logging_utils import get_logger

logger = get_logger(__name__)

# Configuration constants
DEFAULT_MAX_WORKERS = min(mp.cpu_count(), 4)  # Conservative default
MEMORY_MONITORING_ENABLED = True

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not available - memory monitoring will be limited")


class MemoryAwareProcessPool:
    """
    A process pool that monitors memory usage and dynamically adjusts worker count
    to prevent OOM conditions while maximizing CPU utilization.
    """
    
    def __init__(
        self, 
        max_workers: Optional[int] = None,
        memory_threshold_mb: int = 8000,  # 8GB threshold
        min_workers: int = 1,
        enable_monitoring: bool = True
    ):
        self.max_workers = max_workers or DEFAULT_MAX_WORKERS
        self.min_workers = min_workers
        self.memory_threshold_mb = memory_threshold_mb
        self.enable_monitoring = enable_monitoring
        
        # Start with conservative worker count
        self.current_workers = min(self.max_workers, 2)
        self.executor: Optional[ProcessPoolExecutor] = None
        self._is_initialized = False
        
        # Register this pool for global memory cleanup coordination
        try:
            from gaia.utils.global_memory_manager import register_thread_cleanup
            
            def cleanup_process_pool_caches():
                # Clear any caches that accumulate in the process pool management
                import gc
                collected = gc.collect()
                logger.debug(f"[MemoryAwareProcessPool] Performed cleanup, collected {collected} objects")
            
            register_thread_cleanup("memory_aware_process_pool", cleanup_process_pool_caches)
            logger.debug("[MemoryAwareProcessPool] Registered for global memory cleanup")
        except Exception as e:
            logger.debug(f"[MemoryAwareProcessPool] Failed to register cleanup: {e}")
        
        logger.info(f"MemoryAwareProcessPool initialized: max_workers={self.max_workers}, "
                   f"memory_threshold={memory_threshold_mb}MB, current_workers={self.current_workers}")
    
    def _get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB."""
        if not PSUTIL_AVAILABLE:
            return 0.0
        try:
            process = psutil.Process()
            return process.memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0
    
    def _adjust_workers(self) -> bool:
        """
        Adjust worker count based on memory pressure.
        Returns True if workers were adjusted.
        """
        if not self.enable_monitoring:
            return False
            
        current_memory = self._get_memory_usage_mb()
        
        if current_memory > self.memory_threshold_mb:
            # High memory pressure - reduce workers
            if self.current_workers > self.min_workers:
                old_workers = self.current_workers
                self.current_workers = max(self.min_workers, self.current_workers - 1)
                logger.warning(f"High memory pressure ({current_memory:.1f}MB) - "
                              f"reducing workers from {old_workers} to {self.current_workers}")
                return True
        elif current_memory < self.memory_threshold_mb * 0.7:
            # Low memory pressure - can increase workers
            if self.current_workers < self.max_workers:
                old_workers = self.current_workers
                self.current_workers = min(self.max_workers, self.current_workers + 1)
                logger.info(f"Low memory pressure ({current_memory:.1f}MB) - "
                           f"increasing workers from {old_workers} to {self.current_workers}")
                return True
        
        return False
    
    def _create_executor(self) -> ProcessPoolExecutor:
        """Create a new process executor with current worker count."""
        if self.executor:
            try:
                self.executor.shutdown(wait=False)
            except Exception as e:
                logger.debug(f"Error shutting down old executor: {e}")
        
        logger.debug(f"Creating ProcessPoolExecutor with {self.current_workers} workers")
        self.executor = ProcessPoolExecutor(
            max_workers=self.current_workers,
            mp_context=mp.get_context('spawn')  # Use spawn for better isolation
        )
        return self.executor
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function in the process pool with memory monitoring.
        
        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Result of the function execution
        """
        # Check memory before execution
        if self.enable_monitoring:
            current_memory = self._get_memory_usage_mb()
            if current_memory > self.memory_threshold_mb * 1.2:  # 20% over threshold
                logger.error(f"Memory too high ({current_memory:.1f}MB) - falling back to async execution")
                # Fallback to single-threaded execution
                return await asyncio.to_thread(func, *args, **kwargs)
        
        # Adjust workers if needed
        workers_adjusted = self._adjust_workers()
        if workers_adjusted or not self._is_initialized:
            self._create_executor()
            self._is_initialized = True
        
        # Execute in process pool
        loop = asyncio.get_event_loop()
        try:
            start_time = time.time()
            result = await loop.run_in_executor(self.executor, func, *args, **kwargs)
            duration = time.time() - start_time
            
            logger.debug(f"Process pool execution completed in {duration:.2f}s using {self.current_workers} workers")
            return result
            
        except Exception as e:
            logger.error(f"Error in process pool execution: {e}")
            # Fallback to single-threaded execution
            logger.info("Falling back to single-threaded execution")
            return await asyncio.to_thread(func, *args, **kwargs)
    
    def shutdown(self):
        """Shutdown the process pool."""
        if self.executor:
            logger.info("Shutting down MemoryAwareProcessPool")
            self.executor.shutdown(wait=True)
            self.executor = None
        self._is_initialized = False


# Global process pool instance
_global_process_pool: Optional[MemoryAwareProcessPool] = None


def get_process_pool() -> MemoryAwareProcessPool:
    """Get or create the global process pool instance."""
    global _global_process_pool
    
    if _global_process_pool is None:
        # Get configuration from environment
        max_workers = int(os.getenv('WEATHER_MP_MAX_WORKERS', DEFAULT_MAX_WORKERS))
        memory_threshold = int(os.getenv('WEATHER_MP_MEMORY_THRESHOLD_MB', '8000'))
        
        _global_process_pool = MemoryAwareProcessPool(
            max_workers=max_workers,
            memory_threshold_mb=memory_threshold
        )
        logger.info(f"Created global process pool with {max_workers} max workers")
    
    return _global_process_pool


def shutdown_process_pool():
    """Shutdown the global process pool."""
    global _global_process_pool
    if _global_process_pool:
        _global_process_pool.shutdown()
        _global_process_pool = None


# CPU-intensive functions for multiprocessing

def process_era5_variable_mp(
    era5_data: xr.Dataset,
    var_name: str,
    target_grid: xr.DataArray,
    interpolation_method: str = "linear"
) -> xr.DataArray:
    """
    Process a single ERA5 variable in a separate process.
    Handles coordinate transformations and interpolation.
    """
    try:
        logger.debug(f"MP: Processing ERA5 variable {var_name}")
        
        if var_name not in era5_data:
            logger.warning(f"MP: Variable {var_name} not found in ERA5 data")
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
        
        # Interpolate to target grid
        result = var_data.interp_like(target_grid, method=interpolation_method)
        
        # Force computation to avoid lazy loading issues
        result = result.compute()
        
        logger.debug(f"MP: Successfully processed ERA5 variable {var_name}")
        return result
        
    except Exception as e:
        logger.error(f"MP: Error processing ERA5 variable {var_name}: {e}")
        return None


def compute_climatology_interpolation_mp(
    climatology_data: np.ndarray,
    source_coords: Tuple[np.ndarray, np.ndarray],  # (lat, lon)
    target_coords: Tuple[np.ndarray, np.ndarray],  # (lat, lon)
    var_name: str,
    interpolation_method: str = "linear"
) -> np.ndarray:
    """
    Perform climatology interpolation in a separate process.
    This is one of the most CPU-intensive operations.
    """
    try:
        from scipy.interpolate import griddata
        
        logger.debug(f"MP: Computing climatology interpolation for {var_name}")
        
        # Flatten source coordinates and data
        source_lat, source_lon = source_coords
        target_lat, target_lon = target_coords
        
        # Create meshgrids
        source_lat_mesh, source_lon_mesh = np.meshgrid(source_lat, source_lon, indexing='ij')
        target_lat_mesh, target_lon_mesh = np.meshgrid(target_lat, target_lon, indexing='ij')
        
        # Flatten for interpolation
        source_points = np.column_stack((
            source_lat_mesh.ravel(),
            source_lon_mesh.ravel()
        ))
        target_points = np.column_stack((
            target_lat_mesh.ravel(),
            target_lon_mesh.ravel()
        ))
        
        # Handle time dimension if present
        if climatology_data.ndim == 3:  # (time, lat, lon)
            result_shape = (climatology_data.shape[0], target_lat.size, target_lon.size)
            result = np.zeros(result_shape, dtype=np.float32)
            
            for t in range(climatology_data.shape[0]):
                values = climatology_data[t].ravel()
                # Remove NaN values
                valid_mask = ~np.isnan(values)
                if np.sum(valid_mask) < 4:  # Need at least 4 points for interpolation
                    logger.warning(f"MP: Not enough valid points for {var_name} at time {t}")
                    result[t] = np.nan
                    continue
                
                interpolated = griddata(
                    source_points[valid_mask],
                    values[valid_mask],
                    target_points,
                    method=interpolation_method,
                    fill_value=np.nan
                )
                result[t] = interpolated.reshape(target_lat.size, target_lon.size)
        else:  # (lat, lon)
            values = climatology_data.ravel()
            valid_mask = ~np.isnan(values)
            
            if np.sum(valid_mask) < 4:
                logger.warning(f"MP: Not enough valid points for {var_name}")
                return np.full((target_lat.size, target_lon.size), np.nan, dtype=np.float32)
            
            interpolated = griddata(
                source_points[valid_mask],
                values[valid_mask],
                target_points,
                method=interpolation_method,
                fill_value=np.nan
            )
            result = interpolated.reshape(target_lat.size, target_lon.size)
        
        logger.debug(f"MP: Successfully computed climatology interpolation for {var_name}")
        return result.astype(np.float32)  # Reduce memory usage
        
    except Exception as e:
        logger.error(f"MP: Error in climatology interpolation for {var_name}: {e}")
        logger.error(f"MP: Traceback: {traceback.format_exc()}")
        return None


def compute_statistical_metrics_mp(
    forecast_data: np.ndarray,
    truth_data: np.ndarray,
    reference_data: Optional[np.ndarray] = None,
    climatology_data: Optional[np.ndarray] = None,
    lat_weights: Optional[np.ndarray] = None,
    metrics: List[str] = None
) -> Dict[str, float]:
    """
    Compute statistical metrics in a separate process.
    
    Args:
        forecast_data: Forecast array
        truth_data: Truth/analysis array  
        reference_data: Reference forecast array (for skill scores)
        climatology_data: Climatology array (for ACC)
        lat_weights: Latitude weights for area weighting
        metrics: List of metrics to compute ['mse', 'rmse', 'mae', 'bias', 'corr', 'skill', 'acc']
    
    Returns:
        Dictionary of computed metrics
    """
    if metrics is None:
        metrics = ['mse', 'rmse', 'corr']
    
    try:
        logger.debug(f"MP: Computing statistical metrics: {metrics}")
        
        results = {}
        
        # Ensure arrays are numpy and have same shape
        forecast = np.asarray(forecast_data, dtype=np.float32)
        truth = np.asarray(truth_data, dtype=np.float32)
        
        if forecast.shape != truth.shape:
            logger.error(f"MP: Shape mismatch - forecast: {forecast.shape}, truth: {truth.shape}")
            return {metric: np.nan for metric in metrics}
        
        # Create valid data mask
        valid_mask = ~(np.isnan(forecast) | np.isnan(truth))
        if np.sum(valid_mask) == 0:
            logger.warning("MP: No valid data points for metrics computation")
            return {metric: np.nan for metric in metrics}
        
        forecast_valid = forecast[valid_mask]
        truth_valid = truth[valid_mask]
        
        # Apply weights if provided
        if lat_weights is not None:
            weights = np.asarray(lat_weights, dtype=np.float32)[valid_mask]
            weights = weights / np.sum(weights)  # Normalize
        else:
            weights = None
        
        # Compute basic metrics
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
            # Weighted correlation
            if weights is not None:
                # Weighted means
                forecast_mean = np.average(forecast_valid, weights=weights)
                truth_mean = np.average(truth_valid, weights=weights)
                
                # Weighted covariance and variances
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
                # Simple correlation
                if len(forecast_valid) > 1:
                    corr_matrix = np.corrcoef(forecast_valid, truth_valid)
                    results['corr'] = float(corr_matrix[0, 1])
                else:
                    results['corr'] = np.nan
        
        # Skill score (requires reference)
        if 'skill' in metrics and reference_data is not None:
            reference = np.asarray(reference_data, dtype=np.float32)
            if reference.shape == truth.shape:
                ref_valid = reference[valid_mask]
                ref_squared_error = (ref_valid - truth_valid) ** 2
                
                if weights is not None:
                    mse_ref = float(np.average(ref_squared_error, weights=weights))
                else:
                    mse_ref = float(np.mean(ref_squared_error))
                
                if mse_ref > 0:
                    results['skill'] = float(1.0 - (mse / mse_ref))
                else:
                    results['skill'] = 1.0 if mse == 0 else -np.inf
            else:
                results['skill'] = np.nan
        
        # ACC (requires climatology)
        if 'acc' in metrics and climatology_data is not None:
            climatology = np.asarray(climatology_data, dtype=np.float32)
            if climatology.shape == truth.shape:
                clim_valid = climatology[valid_mask]
                
                # Compute anomalies
                forecast_anom = forecast_valid - clim_valid
                truth_anom = truth_valid - clim_valid
                
                # ACC is correlation of anomalies
                if weights is not None:
                    # Weighted correlation of anomalies
                    forecast_anom_mean = np.average(forecast_anom, weights=weights)
                    truth_anom_mean = np.average(truth_anom, weights=weights)
                    
                    forecast_anom_centered = forecast_anom - forecast_anom_mean
                    truth_anom_centered = truth_anom - truth_anom_mean
                    
                    cov = np.average(forecast_anom_centered * truth_anom_centered, weights=weights)
                    var_forecast = np.average(forecast_anom_centered ** 2, weights=weights)
                    var_truth = np.average(truth_anom_centered ** 2, weights=weights)
                    
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
        
        logger.debug(f"MP: Successfully computed metrics: {list(results.keys())}")
        return results
        
    except Exception as e:
        logger.error(f"MP: Error computing statistical metrics: {e}")
        logger.error(f"MP: Traceback: {traceback.format_exc()}")
        return {metric: np.nan for metric in metrics}


def process_coordinate_transformation_mp(
    data_array: np.ndarray,
    source_coords: Dict[str, np.ndarray],
    target_coords: Dict[str, np.ndarray],
    transformation_type: str = "interpolation"
) -> np.ndarray:
    """
    Perform coordinate transformations in a separate process.
    """
    try:
        logger.debug(f"MP: Processing coordinate transformation: {transformation_type}")
        
        # This is a placeholder for complex coordinate transformations
        # that might be needed for different grid projections, etc.
        
        if transformation_type == "interpolation":
            # Use scipy griddata for interpolation
            from scipy.interpolate import griddata
            
            # Extract coordinates
            source_lat = source_coords.get('lat')
            source_lon = source_coords.get('lon')
            target_lat = target_coords.get('lat')
            target_lon = target_coords.get('lon')
            
            if any(coord is None for coord in [source_lat, source_lon, target_lat, target_lon]):
                logger.error("MP: Missing coordinates for interpolation")
                return None
            
            # Create coordinate meshes
            source_lat_mesh, source_lon_mesh = np.meshgrid(source_lat, source_lon, indexing='ij')
            target_lat_mesh, target_lon_mesh = np.meshgrid(target_lat, target_lon, indexing='ij')
            
            # Prepare points
            source_points = np.column_stack((
                source_lat_mesh.ravel(),
                source_lon_mesh.ravel()
            ))
            target_points = np.column_stack((
                target_lat_mesh.ravel(),
                target_lon_mesh.ravel()
            ))
            
            # Handle multi-dimensional data
            if data_array.ndim == 2:
                values = data_array.ravel()
                valid_mask = ~np.isnan(values)
                
                if np.sum(valid_mask) < 4:
                    logger.warning("MP: Not enough valid points for interpolation")
                    return np.full((target_lat.size, target_lon.size), np.nan, dtype=np.float32)
                
                interpolated = griddata(
                    source_points[valid_mask],
                    values[valid_mask],
                    target_points,
                    method='linear',
                    fill_value=np.nan
                )
                result = interpolated.reshape(target_lat.size, target_lon.size)
                
            else:
                # Handle 3D+ data (e.g., time series)
                extra_dims = data_array.shape[:-2]  # All dims except lat, lon
                result_shape = extra_dims + (target_lat.size, target_lon.size)
                result = np.zeros(result_shape, dtype=np.float32)
                
                # Flatten extra dimensions for iteration
                for idx in np.ndindex(extra_dims):
                    slice_data = data_array[idx]
                    values = slice_data.ravel()
                    valid_mask = ~np.isnan(values)
                    
                    if np.sum(valid_mask) < 4:
                        result[idx] = np.nan
                        continue
                    
                    interpolated = griddata(
                        source_points[valid_mask],
                        values[valid_mask],
                        target_points,
                        method='linear',
                        fill_value=np.nan
                    )
                    result[idx] = interpolated.reshape(target_lat.size, target_lon.size)
        
        else:
            logger.error(f"MP: Unknown transformation type: {transformation_type}")
            return None
        
        logger.debug(f"MP: Successfully completed coordinate transformation")
        return result.astype(np.float32)
        
    except Exception as e:
        logger.error(f"MP: Error in coordinate transformation: {e}")
        logger.error(f"MP: Traceback: {traceback.format_exc()}")
        return None


# Cleanup function
def cleanup_multiprocessing():
    """Clean up multiprocessing resources."""
    try:
        shutdown_process_pool()
        gc.collect()
        logger.info("Multiprocessing cleanup completed")
    except Exception as e:
        logger.error(f"Error during multiprocessing cleanup: {e}") 