"""
Core metric calculations for weather forecast evaluation.
"""

import numpy as np
from typing import Dict, Any, Union

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
            # Get data arrays
            pred_data = prediction[var]
            truth_data = ground_truth[var]
            
            # Ensure same shape
            if pred_data.shape != truth_data.shape:
                continue
                
            # Calculate squared errors
            squared_error = (pred_data - truth_data) ** 2
            
            # Sum and count
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
    # Typical scale factors for different variables
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
    
    # Calculate RMSE
    squared_error = (prediction - ground_truth) ** 2
    rmse = np.sqrt(np.mean(squared_error))
    
    # Apply normalization if factor exists
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
    # Ensure same shape
    if array1.shape != array2.shape:
        return float('inf')
        
    # Calculate mean absolute difference
    return np.mean(np.abs(array1 - array2))