import numpy as np
import xarray as xr
from typing import Dict, List, Tuple, Union, Optional, Any
import gc
import os
import asyncio
from fiber.logging_utils import get_logger
from gaia.tasks.defined_tasks.weather.weather_scoring.metrics import calculate_rmse, normalize_rmse
logger = get_logger(__name__)

"""
Ensemble generation and Bayesian Model Averaging (BMA) for weather forecasts.
This module implements weighted ensemble creation and BMA weight updating for
generating superior consensus forecasts from multiple weather models.
"""

async def create_weighted_ensemble(
    predictions: Dict[str, Dict],
    weights: Dict[str, float]
) -> Dict:
    """
    Create a weighted ensemble forecast from multiple predictions.
    
    Args:
        predictions: Dictionary mapping miner_id -> prediction variables
        weights: Dictionary mapping miner_id -> weight
        
    Returns:
        Dictionary of ensemble variables
    """
    try:
        if not predictions:
            return {}
        
        total_weight = sum(weights.values())
        if total_weight <= 0:
            normalized_weights = {m: 1.0/len(predictions) for m in predictions}
        else:
            normalized_weights = {m: w/total_weight for m, w in weights.items()}
        
        all_vars = set()
        for pred in predictions.values():
            all_vars.update(pred.keys())
        
        ensemble = {}
        
        for var in all_vars:
            miners_with_var = [m for m in predictions if var in predictions[m]]
            if not miners_with_var:
                continue
                
            shapes = [predictions[m][var].shape for m in miners_with_var]
            if len(set(shapes)) > 1:
                logger.warning(f"Skipping variable {var} due to inconsistent shapes: {shapes}")
                continue
            
            var_weights = np.array([normalized_weights[m] for m in miners_with_var])
            var_weights = var_weights / np.sum(var_weights)
            
            stacked_forecasts = np.stack([predictions[m][var] for m in miners_with_var])
            ensemble[var] = np.sum(stacked_forecasts * var_weights[:, np.newaxis, np.newaxis], axis=0)
            
        return ensemble
    except Exception as e:
        logger.error(f"Error in create_weighted_ensemble: {e}")
        raise

async def update_bma_weights(
    current_weights: Dict[str, float],
    recent_scores: Dict[str, float],
    learning_rate: float = 0.2,
    min_weight: float = 0.01
) -> Dict[str, float]:
    """
    Update BMA weights based on recent performance with vectorized operations.
    
    Args:
        current_weights: Dictionary of current weights
        recent_scores: Dictionary of recent scores
        learning_rate: Rate of adaptation (0.0-1.0)
        min_weight: Minimum weight to maintain for diversity
        
    Returns:
        Dictionary of updated weights
    """
    try:
        # Convert scores to positive values with exponentiation
        # This emphasizes differences and ensures positive weights
        skill_factors = {
            m: np.exp(max(-5, min(5, score * 3.0)))
            for m, score in recent_scores.items()
        }
        
        updated_weights = {}
        
        for miner_id, current_weight in current_weights.items():
            if miner_id in skill_factors:
                updated_weights[miner_id] = (
                    (1 - learning_rate) * current_weight + 
                    learning_rate * skill_factors[miner_id]
                )
            else:
                updated_weights[miner_id] = current_weight * 0.8
        
        for miner_id in skill_factors:
            if miner_id not in current_weights:
                updated_weights[miner_id] = skill_factors[miner_id]
        
        for miner_id in updated_weights:
            updated_weights[miner_id] = max(updated_weights[miner_id], min_weight)
        
        total = sum(updated_weights.values())
        if total > 0:
            normalized_weights = {m: w/total for m, w in updated_weights.items()}
        else:
            normalized_weights = {m: 1.0/len(updated_weights) for m in updated_weights}
        
        return normalized_weights
    except Exception as e:
        logger.error(f"Error in update_bma_weights: {e}")
        raise

async def memory_efficient_bma(
    predictions_file_paths: Dict[str, str],
    historical_weights: Dict[str, float],
    output_path: str,
    chunk_size: int = 4
) -> Dict[str, float]:
    """
    Create BMA ensemble by processing predictions sequentially to minimize memory usage.
    
    Args:
        predictions_file_paths: Dictionary mapping miner_id -> file path
        historical_weights: Dictionary of historical weights
        output_path: Path to save ensemble
        chunk_size: Number of timesteps to process at once
        
    Returns:
        Dictionary of miner weights used
    """
    try:
        total_weight = sum(historical_weights.values())
        if total_weight <= 0:
            normalized_weights = {m: 1.0/len(predictions_file_paths) for m in predictions_file_paths}
        else:
            normalized_weights = {m: w/total_weight for m, w in historical_weights.items()}
        
        for time_idx in range(0, 40, chunk_size):
            time_chunk = slice(time_idx, min(time_idx + chunk_size, 40))
            
            logger.info(f"Processing time chunk {time_idx} to {min(time_idx + chunk_size, 40)}")
            
            chunk_data = {}
            for miner_id, file_path in predictions_file_paths.items():
                try:
                    with xr.open_dataset(file_path) as ds:
                        chunk_forecast = ds.isel(time=time_chunk)
                        chunk_data[miner_id] = {}
                        
                        for var in chunk_forecast.data_vars:
                            chunk_data[miner_id][var] = chunk_forecast[var].values
                except Exception as e:
                    logger.error(f"Error loading prediction for {miner_id}: {e}")
            
            chunk_ensemble = await create_weighted_ensemble(chunk_data, normalized_weights)
            await save_ensemble_chunk(chunk_ensemble, time_idx, output_path)
            
            del chunk_data
            gc.collect()
        
        return normalized_weights
    except Exception as e:
        logger.error(f"Error in memory_efficient_bma: {e}")
        raise

async def save_ensemble_chunk(ensemble_chunk: Dict, time_idx: int, output_path: str) -> None:
    """
    Save a chunk of the ensemble forecast.
    
    Args:
        ensemble_chunk: Dictionary of ensemble variables
        time_idx: Starting time index of this chunk
        output_path: Base path for output files
    """
    try:
        day_idx = time_idx // 4
        chunk_path = f"{output_path}_day{day_idx}.nc"
        
        data_vars = {}
        for var, data in ensemble_chunk.items():
            if data.ndim == 2:  # surface variable (lat, lon)
                data = data[np.newaxis, ...]
            elif data.ndim == 3 and time_idx % 4 > 0:  # (time, lat, lon) or (level, lat, lon) 
                if data.shape[0] <= 13:  # Pressure levels
                    data = data[np.newaxis, ...]
        
            data_vars[var] = (["time", "y", "x"], data)
        
        ds = xr.Dataset(data_vars)
        encoding = {var: {"zlib": True, "complevel": 6} for var in ds.data_vars}
        
        if os.path.exists(chunk_path) and time_idx % 4 > 0:
            with xr.open_dataset(chunk_path) as existing:
                combined = xr.concat([existing, ds], dim="time")
                combined.to_netcdf(chunk_path + ".tmp", encoding=encoding)
            
            os.rename(chunk_path + ".tmp", chunk_path)
        else:
            ds.to_netcdf(chunk_path, encoding=encoding)
    except Exception as e:
        logger.error(f"Error in save_ensemble_chunk: {e}")
        raise

async def create_variable_level_ensemble(
    processed_forecasts: Dict[str, Dict], 
    variable_weights: Dict[str, Dict[str, float]]
) -> Dict:
    """
    Create ensemble with separate weights for each variable and level.
    
    Args:
        processed_forecasts: Dictionary of forecasts by miner
        variable_weights: Dictionary of weights by variable and miner
    
    Returns:
        Dictionary containing the ensemble forecast
    """
    try:
        ensemble = {}
        
        surface_vars = ['2t', 'msl', '10u', '10v']
        for var in surface_vars:
            if var not in variable_weights:
                continue
                
            miners_with_var = [
                m for m in processed_forecasts 
                if var in processed_forecasts[m]
            ]
            
            if not miners_with_var:
                continue
                
            var_weights = variable_weights[var]
            total_weight = sum(var_weights.get(m, 0.0) for m in miners_with_var)
            if total_weight <= 0:
                normalized_weights = {m: 1.0/len(miners_with_var) for m in miners_with_var}
            else:
                normalized_weights = {
                    m: var_weights.get(m, 0.0)/total_weight for m in miners_with_var
                }
            
            first_miner = miners_with_var[0]
            forecast_shape = processed_forecasts[first_miner][var].shape
            weighted_sum = np.zeros(forecast_shape, dtype=np.float32)
            
            for miner in miners_with_var:
                weight = normalized_weights.get(miner, 0.0)
                weighted_sum += processed_forecasts[miner][var] * weight
            
            ensemble[var] = weighted_sum
        
        atmo_vars = ['t', 'q', 'z', 'u', 'v']
        for var in atmo_vars:
            if var not in variable_weights:
                continue
                
            all_levels = set()
            for miner_forecast in processed_forecasts.values():
                if var in miner_forecast:
                    all_levels.update(miner_forecast[var].keys())
            
            for level in sorted(all_levels):
                level_key = f"{var}_{level}"
                
                miners_with_level = [
                    m for m in processed_forecasts 
                    if var in processed_forecasts[m] and level in processed_forecasts[m][var]
                ]
                
                if not miners_with_level:
                    continue
                    
                level_weights = variable_weights.get(level_key, variable_weights[var])
                total_weight = sum(level_weights.get(m, 0.0) for m in miners_with_level)

                if total_weight <= 0:
                    normalized_weights = {m: 1.0/len(miners_with_level) for m in miners_with_level}
                else:
                    normalized_weights = {
                        m: level_weights.get(m, 0.0)/total_weight for m in miners_with_level
                    }
                
                first_miner = miners_with_level[0]
                forecast_shape = processed_forecasts[first_miner][var][level].shape
            
                weighted_sum = np.zeros(forecast_shape, dtype=np.float32)
                for miner in miners_with_level:
                    weight = normalized_weights.get(miner, 0.0)
                    weighted_sum += processed_forecasts[miner][var][level] * weight
                
                if var not in ensemble:
                    ensemble[var] = {}
                
                ensemble[var][level] = weighted_sum
        
        return ensemble
    except Exception as e:
        logger.error(f"Error in create_variable_level_ensemble: {e}")
        raise

async def optimize_ensemble_weights(
    forecasts: Dict[str, Dict],
    truth: Dict,
    initial_weights: Optional[Dict[str, float]] = None,
    max_iter: int = 10,
    learning_rate: float = 0.1,
    variables: Optional[List[str]] = None
) -> Dict[str, float]:
    """
    Optimize ensemble weights to minimize RMSE against truth data.
    
    Args:
        forecasts: Dictionary of forecasts by miner
        truth: Dictionary of truth data
        initial_weights: Initial weights to start with
        max_iter: Maximum number of optimization iterations
        learning_rate: Learning rate for gradient-based updates
        variables: List of variables to include (defaults to all)
    
    Returns:
        Dictionary of optimized weights by miner
    """
    try:
        if not forecasts:
            return {}
        
        if not variables:
            variables = list(truth.keys())
        
        all_miners = list(forecasts.keys())
        if not all_miners:
            return {}
        
        if initial_weights is None:
            weights = {m: 1.0 / len(all_miners) for m in all_miners}
        else:
            weights = {m: initial_weights.get(m, 0.0) for m in all_miners}
            total = sum(weights.values())
            if total > 0:
                weights = {m: w / total for m, w in weights.items()}
            else:
                weights = {m: 1.0 / len(all_miners) for m in all_miners}
        
        best_weights = weights.copy()
        best_error = float('inf')
        
        for iteration in range(max_iter):
            ensemble = await create_weighted_ensemble(forecasts, weights)
            
            current_error = 0.0
            for var in variables:
                if var in ensemble and var in truth:
                    rmse = await normalize_rmse(ensemble[var], truth[var], var)
                    current_error += rmse
            
            if current_error < best_error:
                best_error = current_error
                best_weights = weights.copy()
            
            new_weights = {}
            for miner in all_miners:
                miner_error = 0.0
                for var in variables:
                    if var in forecasts[miner] and var in truth:
                        rmse = await normalize_rmse(forecasts[miner][var], truth[var], var)
                        miner_error += rmse
                
                # relative performance (lower error = higher weight)
                if current_error > 0:
                    # Skill relative to ensemble
                    relative_skill = max(0.1, current_error / (miner_error + 1e-8))
                    
                    new_weights[miner] = weights[miner] * (1.0 + learning_rate * (relative_skill - 1.0))
                else:
                    new_weights[miner] = weights[miner]
            
            total = sum(new_weights.values())
            if total > 0:
                weights = {m: w / total for m, w in new_weights.items()}
            
            weight_change = sum(abs(weights[m] - best_weights[m]) for m in all_miners)
            if weight_change < 0.01:
                break
        
        return best_weights
    except Exception as e:
        logger.error(f"Error in optimize_ensemble_weights: {e}")
        raise

async def ensemble_forecast_pipeline(
    miner_data: Dict[str, Dict],
    historical_weights: Dict[str, float],
    lead_time: int,
    variables: List[str],
    ground_truth: Optional[Dict] = None
) -> Tuple[Dict, Dict[str, float]]:
    """
    End-to-end pipeline for creating ensemble forecast.
    
    Args:
        miner_data: Raw miner forecasts
        historical_weights: Historical weights by miner
        lead_time: Forecast lead time in hours
        variables: List of variables to include
        ground_truth: Optional ground truth for weight optimization
        
    Returns:
        Tuple of (ensemble_forecast, updated_weights)
    """
    try:
        from scoring import preprocess_forecasts
        
        processed_data = await preprocess_forecasts(miner_data, variables, lead_time)
        if ground_truth is not None:
            optimized_weights = await optimize_ensemble_weights(
                processed_data,
                ground_truth,
                initial_weights=historical_weights,
                variables=variables
            )
        else:
            optimized_weights = historical_weights
        
        ensemble_forecast = await create_weighted_ensemble(processed_data, optimized_weights)
        return ensemble_forecast, optimized_weights
    except Exception as e:
        logger.error(f"Error in ensemble_forecast_pipeline: {e}")
        raise