import os
import logging
import pickle
import numpy as np
import torch
import xarray as xr
import requests
from datetime import datetime, timedelta
from typing import Dict, Tuple, List, Optional, Union
import warnings
from aurora import Batch, Metadata
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('aurora_data_prep')
warnings.filterwarnings('ignore', message='numpy.core.numeric is deprecated', category=DeprecationWarning)


def download_static_pickle(download_dir: str = '.', resolution: str = '0.25') -> str:
    """
    Download the static variables pickle file from HuggingFace if not already present.
    
    Args:
        download_dir: Directory to download the file (default: current directory)
        resolution: Resolution of the Aurora model ('0.25' or '0.1')
        
    Returns:
        str: Path to the static pickle file
    """
    os.makedirs(download_dir, exist_ok=True)
    
    static_urls = {
        '0.25': 'https://huggingface.co/microsoft/aurora/resolve/main/aurora-0.25-static.pickle',
        '0.1': 'https://huggingface.co/microsoft/aurora/resolve/main/aurora-0.1-static.pickle'
    }
    
    if resolution not in static_urls:
        raise ValueError(f"Unsupported resolution: {resolution}. Choose from {list(static_urls.keys())}")
    
    pickle_file = os.path.join(download_dir, f'aurora-{resolution}-static.pickle')
    
    if not os.path.exists(pickle_file):
        logger.info(f"Downloading Aurora static variables pickle for {resolution}° resolution")
        url = static_urls[resolution]
        
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(pickle_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Downloaded static variables to {pickle_file}")
        else:
            raise RuntimeError(f"Failed to download static variables: HTTP {response.status_code}")
    else:
        logger.info(f"Static variables pickle already exists at {pickle_file}")
    
    return pickle_file


def load_static_variables(pickle_path: str) -> Dict[str, np.ndarray]:
    """
    Load the static variables from the Aurora pickle file.
    
    Args:
        pickle_path: Path to the Aurora static variables pickle file
        
    Returns:
        Dict[str, np.ndarray]: Dictionary containing the static variables
    """
    logger.info(f"Loading static variables from {pickle_path}")
    
    try:
        with open(pickle_path, 'rb') as f:
            static_data = pickle.load(f)
        
        required_vars = ['lsm', 'slt', 'z']
        for var in required_vars:
            if var not in static_data:
                raise KeyError(f"Required static variable '{var}' not found in pickle file")
        
        logger.info(f"Successfully loaded static variables: {list(static_data.keys())}")
        return static_data
    
    except Exception as e:
        logger.error(f"Error loading static variables: {e}")
        raise


def prepare_aurora_batch(
    gfs_data: xr.Dataset, 
    static_data: Dict[str, np.ndarray],
    history_steps: int = 2,
    lead_time: Optional[int] = None
) -> Batch:
    """
    Prepare an Aurora Batch from GFS data and static variables.
    
    Args:
        gfs_data: xarray Dataset with GFS data in Aurora variable naming
        static_data: Dictionary of static variables from the Aurora pickle
        history_steps: Number of time steps to include (default: 2)
        lead_time: Specific lead time to use (default: None, uses all available times)
        
    Returns:
        Batch: An Aurora Batch object with the formatted data
    """
    logger.info("Preparing Aurora Batch from GFS data")
    
    aurora_pressure_levels = [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000]
    
    for var_name in ['t', 'u', 'v', 'q', 'z']:
        if var_name in gfs_data and 'lev' in gfs_data[var_name].dims:
            logger.info(f"Filtering {var_name} to Aurora supported pressure levels")
            gfs_data[var_name] = gfs_data[var_name].sel(lev=aurora_pressure_levels, method="nearest")
    
    # Check if we have enough time steps for history
    if 'time' in gfs_data.dims and len(gfs_data.time) < history_steps:
        raise ValueError(f"Need at least {history_steps} time steps, but only have {len(gfs_data.time)}")
    
    # Ensure latitudes are in decreasing order (90 to -90)
    if gfs_data.lat[0] < gfs_data.lat[-1]:
        logger.info("Reordering latitudes to be decreasing (90 to -90)")
        gfs_data = gfs_data.reindex(lat=gfs_data.lat[::-1])
    
    # Ensure longitudes are in range [0, 360)
    if gfs_data.lon.min() < 0:
        logger.info("Converting longitudes to range [0, 360)")
        gfs_data = gfs_data.assign_coords(lon=(gfs_data.lon % 360))
        gfs_data = gfs_data.sortby('lon')
    
    # Select specific lead time if requested
    if lead_time is not None and 'time' in gfs_data.dims:
        gfs_data = gfs_data.isel(time=slice(lead_time, lead_time + history_steps))
    
    # Initialize variables for Aurora Batch
    surface_vars = {}
    atmos_vars = {}
    
    # Define variable lists
    surf_var_names = ['2t', '10u', '10v', 'msl']
    atmos_var_names = ['t', 'u', 'v', 'q', 'z']
    
    # Convert to numpy arrays and create tensors
    # For surface variables: (batch_size, time_steps, lat, lon)
    for var_name in surf_var_names:
        if var_name in gfs_data:
            # Get data and ensure it has shape (time, lat, lon)
            var_data = gfs_data[var_name].values
            
            # Add batch dimension if not processing multiple forecasts
            var_data = var_data[-history_steps:].copy()  # Get the last 'history_steps' time steps
            var_data = var_data[None, ...]  # Add batch dimension
            
            # Convert to torch tensor
            surface_vars[var_name] = torch.from_numpy(var_data)
        else:
            logger.warning(f"Surface variable {var_name} not found in GFS data")
    
    # For atmospheric variables: (batch_size, time_steps, pressure_levels, lat, lon)
    for var_name in atmos_var_names:
        if var_name in gfs_data and 'lev' in gfs_data[var_name].dims:
            # Get data with shape (time, level, lat, lon)
            var_data = gfs_data[var_name].values
            
            var_data = var_data[-history_steps:].copy()  # Get the last 'history_steps' time steps
            var_data = var_data[None, ...]  # Add batch dimension
            
            # Convert to torch tensor
            atmos_vars[var_name] = torch.from_numpy(var_data)
        elif var_name in gfs_data:
            logger.warning(f"Atmospheric variable {var_name} has no level dimension")
        else:
            logger.warning(f"Atmospheric variable {var_name} not found in GFS data")
    
    static_vars = {}
    for var_name in ['lsm', 'slt', 'z']:
        if var_name in static_data:
            static_vars[var_name] = torch.from_numpy(static_data[var_name])
        else:
            logger.warning(f"Static variable {var_name} not found in static data")
    
    if 'time' in gfs_data.dims:
        times = gfs_data.time.values
        time_tuples = tuple(pd.to_datetime(t).to_pydatetime() for t in times[-1:])
    else:
        logger.warning("No time dimension found in GFS data, using current time")
        time_tuples = (datetime.now(),)
    
    # Always use Aurora's supported pressure levels for metadata
    # This ensures compatibility with the normalization function
    pressure_levels = tuple(aurora_pressure_levels)
    logger.info(f"Using pressure levels for Aurora: {pressure_levels}")
    
    metadata = Metadata(
        lat=torch.from_numpy(gfs_data.lat.values),
        lon=torch.from_numpy(gfs_data.lon.values),
        time=time_tuples,
        atmos_levels=pressure_levels
    )
    
    batch = Batch(
        surf_vars=surface_vars,
        static_vars=static_vars,
        atmos_vars=atmos_vars,
        metadata=metadata
    )
    
    logger.info("Successfully prepared Aurora Batch")
    return batch


def create_aurora_batch_from_gfs(
    gfs_data: xr.Dataset,
    static_pickle_path: Optional[str] = None,
    resolution: str = '0.25',
    download_dir: str = '.',
    history_steps: int = 2
) -> Batch:
    """
    Create an Aurora Batch object from GFS data and static variables.
    
    Args:
        gfs_data: xarray Dataset with GFS data (already processed to have Aurora variable names)
        static_pickle_path: Path to the static variables pickle file (if None, will download)
        resolution: Resolution of the Aurora model ('0.25' or '0.1')
        download_dir: Directory to download static variables if needed
        history_steps: Number of time steps to include for history dimension
        
    Returns:
        Batch: An Aurora Batch object ready for input to the model
    """
    # Make sure we have required packages
    try:
        import pandas as pd
    except ImportError:
        logger.error("pandas is required but not installed. Please install it with 'pip install pandas'")
        raise
    
    if static_pickle_path is None:
        static_pickle_path = download_static_pickle(download_dir, resolution)
    
    static_data = load_static_variables(static_pickle_path)
    
    batch = prepare_aurora_batch(gfs_data, static_data, history_steps)
    
    return batch 