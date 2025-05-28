import os
import glob
import numpy as np
import xarray as xr
import pandas as pd
from typing import Dict, List, Tuple, Union, Optional
import time
"""
Module to optimize forecast file size.

Reduces float precision per variable, seperates static fields and compresses data.
"""

def optimize_forecast_precision(ds: xr.Dataset) -> Tuple[xr.Dataset, Dict]:
    """
    Optimize a forecast dataset by applying appropriate precision and encoding
    for each variable.
    
    Args:
        ds: Original xarray Dataset
        
    Returns:
        Tuple of (optimized dataset, encoding dictionary)
    """
    encoding = {}
    
    # Surface variables (K)
    temp_scale = 0.01  # 0.01K precision
    temp_vars = ['surf_2t', 'atmos_t']
    for var in temp_vars:
        if var in ds:
            encoding[var] = {
                'dtype': 'int16',
                'scale_factor': temp_scale,
                'add_offset': 200.0,  # Shifted to allow temps from 200K to 335K
                '_FillValue': -32767,
                'zlib': True,
                'complevel': 5,
                'shuffle': True,
                'fletcher32': True
            }
    
    # Wind components (m/s)
    wind_scale = 0.01  # 0.01 m/s precision
    wind_vars = ['surf_10u', 'surf_10v', 'atmos_u', 'atmos_v']
    for var in wind_vars:
        if var in ds:
            encoding[var] = {
                'dtype': 'int16',
                'scale_factor': wind_scale,
                'add_offset': 0.0,
                '_FillValue': -32767,
                'zlib': True,
                'complevel': 5,
                'shuffle': True,
                'fletcher32': True
            }
    
    # Pressure (Pa)
    if 'surf_msl' in ds:
        encoding['surf_msl'] = {
            'dtype': 'int16',
            'scale_factor': 1.0,  # 1 Pa precision
            'add_offset': 100000.0,  # Centered around 100,000 Pa
            '_FillValue': -32767,
            'zlib': True,
            'complevel': 5,
            'shuffle': True,
            'fletcher32': True
        }
    
    # Humidity (kg/kg)
    if 'atmos_q' in ds:
        encoding['atmos_q'] = {
            'dtype': 'int16',
            'scale_factor': 1e-6,  # 1e-6 kg/kg precision
            'add_offset': 0.0,
            '_FillValue': -32767,
            'zlib': True,
            'complevel': 5,
            'shuffle': True,
            'fletcher32': True
        }
    
    # Geopotential height (m)
    if 'atmos_z' in ds:
        encoding['atmos_z'] = {
            'dtype': 'int16',
            'scale_factor': 1.0,  # 1m precision
            'add_offset': 50000.0,  # Centered around 50,000m
            '_FillValue': -32767,
            'zlib': True,
            'complevel': 5,
            'shuffle': True,
            'fletcher32': True
        }
        
        if 'level' in ds.dims:
            level_values = ds['level'].values if 'level' in ds else []
            if 500 in level_values:
                level_idx = np.where(level_values == 500)[0][0]
    
    for var in encoding:
        shape = ds[var].shape
        if len(shape) >= 4:
            if 'level' in ds[var].dims and ds[var].dims.index('level') < len(shape):
                level_dim = ds[var].dims.index('level')
                n_levels = shape[level_dim]
                
                # Dynamic chunking - 1 level/chunk, 1/4 globe spatial chunks
                chunks = list(shape)
                
                if 'batch' in ds[var].dims:
                    chunks[ds[var].dims.index('batch')] = 1
                if 'history' in ds[var].dims:
                    chunks[ds[var].dims.index('history')] = 1
                    
                if 'time' in ds[var].dims:
                    chunks[ds[var].dims.index('time')] = 1
                
                if 'level' in ds[var].dims:
                    chunks[ds[var].dims.index('level')] = 1
                
                if 'latitude' in ds[var].dims:
                    lat_idx = ds[var].dims.index('latitude')
                    chunks[lat_idx] = min(180, shape[lat_idx])
                if 'longitude' in ds[var].dims:
                    lon_idx = ds[var].dims.index('longitude')
                    chunks[lon_idx] = min(360, shape[lon_idx])
                
                encoding[var]['chunksizes'] = chunks
    
    if 'time' in ds.dims:
        encoding['time'] = {
            'dtype': 'int32',
            'units': 'hours since 2025-03-28T00:00:00',
            'calendar': 'standard'
        }
    
    return ds, encoding

def remove_singleton_dims(ds: xr.Dataset) -> xr.Dataset:
    """
    Remove singleton dimensions (size 1) that don't add value.
    
    Args:
        ds: Original dataset
        
    Returns:
        Dataset with singleton dimensions removed
    """
    singleton_dims = [dim for dim, size in ds.dims.items() if size == 1]
    
    if singleton_dims:
        ds = ds.squeeze(singleton_dims)
        
    return ds

def extract_and_reference_static_fields(ds: xr.Dataset, 
                                      static_file_path: str = 'static_fields.nc') -> xr.Dataset:
    """
    Extract static fields to a separate file and return dataset without them.
    
    Args:
        ds: Input dataset
        static_file_path: Path to store static fields
        
    Returns:
        Dataset with static fields removed
    """
    static_vars = [var for var in ds.data_vars if 'static_' in var]
    
    if static_vars and not os.path.exists(static_file_path):
        static_ds = ds[static_vars]
        
        encoding = {var: {
            'zlib': True,
            'complevel': 6,
            'shuffle': True,
        } for var in static_vars}
        
        static_ds.to_netcdf(static_file_path, encoding=encoding)
        print(f"Static fields saved to {static_file_path}")
    
    dynamic_ds = ds.drop_vars(static_vars)
    dynamic_ds.attrs['static_fields_path'] = os.path.abspath(static_file_path)
    
    return dynamic_ds

def reconstruct_full_dataset(dynamic_ds: xr.Dataset, 
                           static_file_path: str = None) -> xr.Dataset:
    """
    Reconstruct the full dataset by merging with static fields.
    
    Args:
        dynamic_ds: Dataset without static fields
        static_file_path: Path to static fields (if None, read from attrs)
        
    Returns:
        Complete dataset with static fields reattached
    """
    if static_file_path is None:
        static_file_path = dynamic_ds.attrs.get('static_fields_path')
    
    if not static_file_path or not os.path.exists(static_file_path):
        return dynamic_ds
    
    static_ds = xr.open_dataset(static_file_path)
    merged_ds = xr.merge([dynamic_ds, static_ds])
    
    return merged_ds

def optimize_forecast_full(ds: xr.Dataset, 
                         static_file_path: str = 'static_fields.nc') -> Tuple[xr.Dataset, Dict]:
    """
    Apply all optimization strategies to a forecast dataset.
    
    Args:
        ds: Original dataset
        static_file_path: Path to save static fields
        
    Returns:
        Tuple of (optimized dataset, encoding dictionary)
    """
    ds = remove_singleton_dims(ds)
    ds = extract_and_reference_static_fields(ds, static_file_path)
    ds, encoding = optimize_forecast_precision(ds)
    
    return ds, encoding

def combine_forecast_steps(file_paths: List[str], 
                         output_path: str, 
                         static_file_path: str = 'static_fields.nc') -> str:
    """
    Combine multiple forecast step files into a single optimized file.
    
    Args:
        file_paths: List of forecast step file paths
        output_path: Path to save the combined file
        static_file_path: Path to save static fields
        
    Returns:
        Path to the saved file
    """
    datasets = []
    for path in sorted(file_paths):
        ds = xr.open_dataset(path)
        step = int(os.path.basename(path).split('_step_')[1].split('.')[0])
        ds = ds.assign_coords(step=step)
        
        if len(datasets) == 0:
            extract_and_reference_static_fields(ds, static_file_path)
            
        ds = ds.drop_vars([var for var in ds.data_vars if 'static_' in var])
        ds = remove_singleton_dims(ds)
        
        datasets.append(ds)
    
    combined = xr.concat(datasets, dim='step')
    _, encoding = optimize_forecast_precision(combined)
    combined.to_netcdf(output_path, encoding=encoding)
    
    return output_path

def create_synthetic_variant(ds: xr.Dataset, 
                           perturbation_scale: float = 0.05, 
                           seed: int = None) -> xr.Dataset:
    """
    Create a synthetic variant of a forecast by adding controlled noise.
    Useful for testing ensemble methods.
    
    Args:
        ds: Original forecast dataset
        perturbation_scale: Scale of the perturbation (0.05 = 5%)
        seed: Random seed for reproducibility
        
    Returns:
        Perturbed dataset
    """
    if seed is not None:
        np.random.seed(seed)
    
    perturbed = ds.copy(deep=True)
    perturbation_config = {
        'surf_2t': {'scale': 1.0, 'type': 'additive'},  # Add/subtract up to 1K
        'surf_10u': {'scale': 0.5, 'type': 'additive'},  # Add/subtract up to 0.5m/s
        'surf_10v': {'scale': 0.5, 'type': 'additive'},  # Add/subtract up to 0.5m/s
        'surf_msl': {'scale': 100.0, 'type': 'additive'},  # Add/subtract up to 100Pa
        'atmos_t': {'scale': 0.5, 'type': 'additive'},  # Add/subtract up to 0.5K
        'atmos_u': {'scale': 0.5, 'type': 'additive'},  # Add/subtract up to 0.5m/s
        'atmos_v': {'scale': 0.5, 'type': 'additive'},  # Add/subtract up to 0.5m/s
        'atmos_q': {'scale': 0.00001, 'type': 'additive'},  # Small perturbation
        'atmos_z': {'scale': 10.0, 'type': 'additive'},  # Add/subtract up to 10m
    }
    
    for var in ds.data_vars:
        if 'static' in var:
            continue
            
        config = perturbation_config.get(var, {'scale': 0.02, 'type': 'multiplicative'})
        noise_shape = ds[var].shape
        
        if config['type'] == 'additive':
            noise = np.random.normal(0, 1, noise_shape) * config['scale'] * perturbation_scale
            perturbed[var] = ds[var] + noise
        else:
            noise = 1.0 + np.random.normal(0, 1, noise_shape) * config['scale'] * perturbation_scale
            perturbed[var] = ds[var] * noise
    
    return perturbed

def generate_ensemble_members(base_forecast_path: str, 
                             num_members: int = 10, 
                             output_dir: str = 'ensemble',
                             static_file_path: str = 'static_fields.nc') -> List[str]:
    """
    Generate an ensemble of synthetic forecast variants.
    
    Args:
        base_forecast_path: Path to the base forecast
        num_members: Number of ensemble members to generate
        output_dir: Directory to save the ensemble members
        static_file_path: Path to save/load static fields
        
    Returns:
        List of paths to the generated ensemble members
    """
    os.makedirs(output_dir, exist_ok=True)
    base_ds = xr.open_dataset(base_forecast_path)
    extract_and_reference_static_fields(base_ds, static_file_path)
    base_ds = base_ds.drop_vars([var for var in base_ds.data_vars if 'static_' in var])
    
    member_paths = []
    for i in range(num_members):
        perturbed = create_synthetic_variant(
            base_ds, 
            perturbation_scale=0.05 + (i * 0.01),
            seed=i
        )
        
        member_path = os.path.join(output_dir, f'member_{i:03d}.nc')
        _, encoding = optimize_forecast_precision(perturbed)
        
        perturbed.attrs['static_fields_path'] = os.path.abspath(static_file_path)
        perturbed.to_netcdf(member_path, encoding=encoding)
        member_paths.append(member_path)
        
    return member_paths

def calculate_ensemble_stats(member_paths: List[str], 
                           output_path: str,
                           static_file_path: str = 'static_fields.nc') -> Dict:
    """
    Calculate ensemble statistics (mean, spread) from ensemble members.
    
    Args:
        member_paths: List of paths to ensemble members
        output_path: Path to save ensemble statistics
        static_file_path: Path to static fields
        
    Returns:
        Dictionary with ensemble statistics
    """
    members = []
    for path in member_paths:
        ds = xr.open_dataset(path)
        members.append(ds)
    
    coords = members[0].coords
    mean_ds = xr.Dataset(coords=coords)
    spread_ds = xr.Dataset(coords=coords)
    
    for var in members[0].data_vars:
        var_data = [member[var] for member in members]
        # Mean
        mean_ds[var] = sum(var_data) / len(var_data)
        # STD
        squared_diffs = [(x - mean_ds[var])**2 for x in var_data]
        variance = sum(squared_diffs) / len(var_data)
        spread_ds[var] = variance ** 0.5
    
    ensemble_stats = xr.Dataset()
    
    for var in mean_ds.data_vars:
        ensemble_stats[f"{var}_mean"] = mean_ds[var]
    
    for var in spread_ds.data_vars:
        ensemble_stats[f"{var}_spread"] = spread_ds[var]
    
    ensemble_stats.attrs['static_fields_path'] = os.path.abspath(static_file_path)
    
    _, encoding = optimize_forecast_precision(ensemble_stats)
    ensemble_stats.to_netcdf(output_path, encoding=encoding)
    
    return {
        'mean_path': output_path,
        'num_members': len(members)
    }

if __name__ == "__main__":
    print("Forecast Optimization and Ensemble Utilities")
    print("--------------------------------------------")
    print("Use these functions to:")
    print("1. Optimize forecast file size")
    print("2. Create synthetic ensemble members for testing")
    print("3. Calculate ensemble statistics") 