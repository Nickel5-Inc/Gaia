import os
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Union, Tuple
import numpy as np
import xarray as xr
import warnings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('gfs_api')

warnings.filterwarnings('ignore', 
                       message='Ambiguous reference date string', 
                       category=xr.SerializationWarning,
                       module='xarray.coding.times')

warnings.filterwarnings('ignore',
                       message='numpy.core.numeric is deprecated',
                       category=DeprecationWarning)

def fetch_gfs_data(run_time: datetime, lead_hours: List[int], output_dir: Optional[str] = None) -> xr.Dataset:
    """
    Fetch GFS data for the given run_time and lead_hours using OPeNDAP.
    
    Args:
        run_time: The model run datetime (e.g., 2023-08-01 00:00)
        lead_hours: List of forecast lead times in hours to retrieve
        output_dir: Optional directory to save NetCDF output (if None, don't save files)
        
    Returns:
        xr.Dataset: Dataset containing all required variables
    """
    logger.info(f"Fetching GFS data for run time: {run_time}, lead hours: {lead_hours}")
    date_str = run_time.strftime('%Y%m%d')
    cycle_str = f"{run_time.hour:02d}"

    dods_base = "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs"
    base_url = f"{dods_base}{date_str}/gfs_0p25_{cycle_str}z"
    logger.info(f"Using OPeNDAP URL: {base_url}")
    
    aurora_pressure_levels = [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000]
    
    aurora_level_indices = [22, 20, 19, 18, 17, 16, 14, 12, 10, 8, 5, 3, 0]
    
    level_index_to_value = {
        0: 1000,   # 1000 hPa
        3: 925,    # 925 hPa
        5: 850,    # 850 hPa
        8: 700,    # 700 hPa
        10: 600,   # 600 hPa
        12: 500,   # 500 hPa
        14: 400,   # 400 hPa
        16: 300,   # 300 hPa
        17: 250,   # 250 hPa
        18: 200,   # 200 hPa
        19: 150,   # 150 hPa
        20: 100,   # 100 hPa
        22: 50     # 50 hPa
    }
    
    surface_vars = ["tmp2m", "ugrd10m", "vgrd10m", "prmslmsl"]
    atmos_vars = ["tmpprs", "ugrdprs", "vgrdprs", "spfhprs", "hgtprs"]
    
    valid_times = [run_time + timedelta(hours=h) for h in lead_hours]
    
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=xr.SerializationWarning)
            
            logger.info("Loading dataset metadata to find time indices and available dimensions")
            full_ds = xr.open_dataset(base_url, decode_times=True)
            
            time_indices = []
            for vt in valid_times:
                time_diffs = abs(full_ds.time.values - np.datetime64(vt))
                closest_idx = np.argmin(time_diffs)
                time_indices.append(closest_idx)
                actual_time = full_ds.time.values[closest_idx]
                logger.info(f"Requested time {vt} matches dataset time {actual_time} at index {closest_idx}")
            
            logger.info(f"Loading surface variables: {surface_vars}")
            surface_ds = full_ds[surface_vars].isel(time=time_indices)
            
            atmos_ds = None
            
            for var in atmos_vars:
                logger.info(f"Loading {var} with only the required 13 pressure levels using exact indices")
                
                if var in full_ds:
                    var_ds = full_ds[[var]].isel(time=time_indices, lev=aurora_level_indices)
                    logger.info(f"Selected pressure levels for {var}: {var_ds.lev.values}")
                    
                    if atmos_ds is None:
                        atmos_ds = var_ds
                    else:
                        atmos_ds = xr.merge([atmos_ds, var_ds])
                else:
                    logger.warning(f"Variable {var} not found in dataset")
            
            ds = xr.merge([surface_ds, atmos_ds])
            
            if 'lev' in ds.dims:
                actual_levels = [float(lev) for lev in ds.lev.values]
                logger.info(f"Final pressure levels in dataset: {actual_levels}")
                
                if len(actual_levels) != len(aurora_pressure_levels):
                    logger.warning(f"Expected {len(aurora_pressure_levels)} pressure levels but got {len(actual_levels)}")
                
                for i, level in enumerate(aurora_pressure_levels):
                    if i < len(actual_levels):
                        if abs(actual_levels[i] - level) > 1:  # Allow 1 hPa difference for floating point
                            logger.warning(f"Level mismatch: Expected {level} hPa, got {actual_levels[i]} hPa")
            
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                out_file = os.path.join(output_dir, f"gfs_{date_str}_{cycle_str}z.nc")
                logger.info(f"Saving data to: {out_file}")
                ds.to_netcdf(out_file)
            
            logger.info("Processing dataset to match Aurora requirements")
            processed_ds = process_opendap_dataset(ds)
            
            return processed_ds
        
    except Exception as e:
        logger.error(f"Error accessing OPeNDAP: {e}")
        raise

def process_opendap_dataset(ds: xr.Dataset) -> xr.Dataset:
    """Process the OPeNDAP dataset to match Aurora's expected format."""
    var_attrs = {}
    for var_name in ds.data_vars:
        var_attrs[var_name] = ds[var_name].attrs.copy()
    
    var_mapping = {
        'tmp2m': '2t',             # '2m_temperature'
        'ugrd10m': '10u',          # '10m_u_component_of_wind'
        'vgrd10m': '10v',          # '10m_v_component_of_wind'
        'prmslmsl': 'msl',         # 'mean_sea_level_pressure'
        'tmpprs': 't',             # 'temperature'
        'ugrdprs': 'u',            # 'u_component_of_wind'
        'vgrdprs': 'v',            # 'v_component_of_wind'
        'spfhprs': 'q',            # 'specific_humidity'
        'hgtprs': 'z',             # 'geopotential'
    }
    
    new_ds = xr.Dataset(coords=ds.coords)
    for old_name, new_name in var_mapping.items():
        if old_name in ds:
            new_ds[new_name] = ds[old_name].copy()
            if old_name in var_attrs:
                new_ds[new_name].attrs = var_attrs[old_name]
    
    ds = new_ds
    default_units = {
        '2t': 'K',                    # Temperature in Kelvin
        '10u': 'm s-1',               # Wind in meters per second
        '10v': 'm s-1',               # Wind in meters per second
        'msl': 'Pa',                  # Pressure in Pascal
        't': 'K',                     # Temperature in Kelvin
        'u': 'm s-1',                 # Wind in meters per second
        'v': 'm s-1',                 # Wind in meters per second
        'q': 'kg kg-1',               # Specific humidity in kg/kg
        'z': 'm2 s-2'                 # Geopotential in m²/s²
    }
    
    for var_name, units in default_units.items():
        if var_name in ds and ('units' not in ds[var_name].attrs or not ds[var_name].attrs['units']):
            ds[var_name].attrs['units'] = units
    
    if 'msl' in ds:
        sample_value = float(ds['msl'].isel(time=0, lat=0, lon=0).values)
        if 900 < sample_value < 1100:
            logger.info("Converting mean sea level pressure from hPa to Pa")
            ds['msl'] = ds['msl'] * 100.0
            ds['msl'].attrs['units'] = 'Pa'
    
    if 'lat' in ds.coords:
        lat_orientation = "decreasing" if ds.lat[0] > ds.lat[-1] else "increasing"
        logger.info(f"Latitude orientation: {lat_orientation} (first: {ds.lat[0].item():.2f}, last: {ds.lat[-1].item():.2f})")
        
        if lat_orientation != "decreasing":
            logger.info("Flipping latitudes to match Aurora requirements (90 to -90)")
            ds = ds.reindex(lat=ds.lat[::-1])
    
    if 'lon' in ds.coords:
        lon_range = f"[{ds.lon.min().item():.2f}, {ds.lon.max().item():.2f}]"
        logger.info(f"Longitude range: {lon_range}")
        
        if ds.lon.min() < 0:
            logger.info("Converting longitude range from [-180, 180] to [0, 360)")
            ds = ds.assign_coords(lon=(ds.lon % 360))
            ds = ds.sortby('lon')
    
    logger.info(f"Grid dimensions: {len(ds.lat)}x{len(ds.lon)}")
    
    if len(ds.lat) != 721 or len(ds.lon) != 1440:
        logger.warning(f"Grid dimensions {len(ds.lat)}x{len(ds.lon)} don't match Aurora requirements (721x1440)")
    
    return ds

def get_consecutive_lead_hours(first_lead: int, last_lead: int, interval: int = 6) -> List[int]:
    """
    Generate a list of consecutive lead hours at specified intervals.
    
    Args:
        first_lead: The first lead hour to include
        last_lead: The last lead hour to include
        interval: Hour interval between lead times
        
    Returns:
        List[int]: List of lead hours
    """
    return list(range(first_lead, last_lead + 1, interval))
