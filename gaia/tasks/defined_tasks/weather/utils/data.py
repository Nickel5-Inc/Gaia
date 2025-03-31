import numpy as np
import xarray as xr
from typing import Dict, Any, Tuple, Union
"""
Data handling utilities for weather task.
"""

def extract_variable(data: Dict, var_name: str) -> np.ndarray:
    """
    Extract variable data, handling differences in structure.
    
    Args:
        data: Dictionary of weather variables
        var_name: Name of variable to extract
        
    Returns:
        NumPy array of variable data
    """
    if var_name not in data:
        raise KeyError(f"Variable {var_name} not found in data")
        
    var_data = data[var_name]
    
    if isinstance(var_data, np.ndarray):
        return var_data
    elif isinstance(var_data, xr.DataArray):
        return var_data.values
    elif hasattr(var_data, 'numpy'):
        return var_data.numpy()
    else:
        return np.array(var_data)

def batch_to_dict(batch) -> Dict:
    """
    Convert Aurora Batch object to dictionary of variables.
    
    Args:
        batch: Aurora Batch object
        
    Returns:
        Dictionary mapping variable names to NumPy arrays
    """
    result = {}
    
    for var_name, var_data in batch.surf_vars.items():
        # Keep (lat, lon)
        result[var_name] = extract_tensor(var_data)[0, 0]
    
    for var_name, var_data in batch.atmos_vars.items():
        # Keep (level, lat, lon)
        result[var_name] = extract_tensor(var_data)[0, 0]
    
    return result

def extract_tensor(tensor) -> np.ndarray:
    """
    Extract numpy array from tensor of various types.
    
    Args:
        tensor: Tensor (numpy, torch, etc.)
        
    Returns:
        NumPy array
    """
    if isinstance(tensor, np.ndarray):
        return tensor
    elif hasattr(tensor, 'numpy'):
        return tensor.numpy()
    else:
        return np.array(tensor)

def load_chunked_prediction(file_path: str, time_idx: int = None) -> Dict:
    """
    Load prediction from file with memory-efficient chunking.
    
    Args:
        file_path: Path to NetCDF file
        time_idx: Optional specific time index to load
        
    Returns:
        Dictionary of variables
    """
    result = {}
    
    with xr.open_dataset(file_path) as ds:
        if time_idx is not None:
            ds = ds.isel(time=time_idx)
            
        for var in ds.data_vars:
            result[var] = ds[var].values
            
    return result