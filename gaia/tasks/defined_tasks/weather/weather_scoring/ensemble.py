import numpy as np
import xarray as xr
from typing import Dict, List, Tuple, Union, Optional
import gc
from utils.metrics import calculate_rmse

"""
Ensemble generation and Bayesian Model Averaging (BMA) for weather forecasts.
This module implements weighted ensemble creation and BMA weight updating for
generating superior consensus forecasts from multiple weather models.
"""

def create_weighted_ensemble(
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
            continue
        
        weighted_sum = np.zeros_like(predictions[miners_with_var[0]][var])
        total_var_weight = 0.0
        
        for m in miners_with_var:
            weighted_sum += predictions[m][var] * normalized_weights[m]
            total_var_weight += normalized_weights[m]
        
        if total_var_weight > 0:
            ensemble[var] = weighted_sum / total_var_weight
        
    return ensemble

def update_bma_weights(
    current_weights: Dict[str, float],
    recent_scores: Dict[str, float],
    learning_rate: float = 0.2
) -> Dict[str, float]:
    """
    Update BMA weights based on recent performance.
    
    Args:
        current_weights: Dictionary of current weights
        recent_scores: Dictionary of recent scores
        learning_rate: Rate of adaptation (0.0-1.0)
        
    Returns:
        Dictionary of updated weights
    """
    updated_weights = {}
    
    for miner_id, current_weight in current_weights.items():
        if miner_id in recent_scores:
            skill_factor = np.exp(recent_scores[miner_id] * 3.0)
            
            # Blend with current weight
            updated_weights[miner_id] = (
                (1 - learning_rate) * current_weight + 
                learning_rate * skill_factor
            )
        else:
            # Decay for inactive uids
            updated_weights[miner_id] = current_weight * 0.8
    
    # Add new miners not in current weights
    for miner_id in recent_scores:
        if miner_id not in current_weights:
            skill_factor = np.exp(recent_scores[miner_id] * 3.0)
            updated_weights[miner_id] = skill_factor
    
    # Norm weights
    total = sum(updated_weights.values())
    if total > 0:
        normalized_weights = {m: w/total for m, w in updated_weights.items()}
    else:
        normalized_weights = {m: 1.0/len(updated_weights) for m in updated_weights}
    
    return normalized_weights

def memory_efficient_bma(
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
    # Norm weights
    total_weight = sum(historical_weights.values())
    if total_weight <= 0:
        normalized_weights = {m: 1.0/len(predictions_file_paths) for m in predictions_file_paths}
    else:
        normalized_weights = {m: w/total_weight for m, w in historical_weights.items()}
    
    # Chunk process 
    for time_idx in range(0, 40, chunk_size):
        time_chunk = slice(time_idx, min(time_idx + chunk_size, 40))
        
        chunk_data = {}
        for miner_id, file_path in predictions_file_paths.items():
            try:
                with xr.open_dataset(file_path) as ds:
                    chunk_forecast = ds.isel(time=time_chunk)
                    chunk_data[miner_id] = {}
                    
                    for var in chunk_forecast.data_vars:
                        chunk_data[miner_id][var] = chunk_forecast[var].values
            except Exception as e:
                print(f"Error loading prediction for {miner_id}: {e}")
        
        # Create weighted chunk
        chunk_ensemble = create_weighted_ensemble(chunk_data, normalized_weights)
        save_ensemble_chunk(chunk_ensemble, time_idx, output_path)
        
        # Garbage collect
        del chunk_data
        gc.collect()
    
    return normalized_weights

def save_ensemble_chunk(ensemble_chunk: Dict, time_idx: int, output_path: str) -> None:
    """
    Save a chunk of the ensemble forecast.
    
    Args:
        ensemble_chunk: Dictionary of ensemble variables
        time_idx: Starting time index of this chunk
        output_path: Base path for output files
    """
    import os
    
    day_idx = time_idx // 4
    chunk_path = f"{output_path}_day{day_idx}.nc"
    
    data_vars = {}
    for var, data in ensemble_chunk.items():
        if data.ndim == 2:  # surface variable (lat, lon)
            data = data[np.newaxis, ...]
        elif data.ndim == 3 and time_idx % 4 > 0:  # (time, lat, lon) or (level, lat, lon)
            if data.shape[0] <= 13:
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