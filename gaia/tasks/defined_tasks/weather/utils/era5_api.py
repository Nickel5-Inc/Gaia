import asyncio
import cdsapi
import xarray as xr
import os
from pathlib import Path
from datetime import datetime
from typing import List, Optional
import hashlib
import tempfile
import traceback
import numpy as np
import pandas as pd

from fiber.logging_utils import get_logger

logger = get_logger(__name__)

AURORA_PRESSURE_LEVELS = [
    "50", "100", "150", "200", "250", "300", "400", "500",
    "600", "700", "850", "925", "1000"
]

ERA5_SINGLE_LEVEL_VARS = {
    '2m_temperature': '2t',
    '10m_u_component_of_wind': '10u',
    '10m_v_component_of_wind': '10v',
    'mean_sea_level_pressure': 'msl'
}

ERA5_PRESSURE_LEVEL_VARS = {
    'temperature': 't',
    'u_component_of_wind': 'u',
    'v_component_of_wind': 'v',
    'specific_humidity': 'q',
    'geopotential': 'z' # Geopotential height (z) = geopotential / g
}


async def fetch_era5_data(
    target_times: List[datetime],
    cache_dir: Path = Path("./era5_cache")
) -> Optional[xr.Dataset]:
    """
    Fetches ERA5 data (single and pressure levels) for the specified times.

    Requires a configured .cdsapirc file in the user's home directory.
    Uses file caching to avoid re-downloading data.

    Args:
        target_times: A list of datetime objects for which to fetch data.
        cache_dir: The directory to use for caching downloaded/processed files.

    Returns:
        An xarray.Dataset containing the combined and processed ERA5 data for
        the target times, or None if fetching or processing fails.
    """
    if not target_times:
        logger.warning("fetch_era5_data called with no target_times.")
        return None

    cache_dir.mkdir(parents=True, exist_ok=True)

    target_times = sorted(list(set(target_times)))
    time_strings = [t.strftime("%Y%m%d%H%M") for t in target_times]
    cache_key = hashlib.md5("_".join(time_strings).encode()).hexdigest()
    cache_filename = cache_dir / f"era5_data_{cache_key}.nc"

    # Check for cache files in multiple formats
    cache_files_to_check = [
        cache_filename,  # .nc format
        cache_filename.with_suffix('.pkl')  # pickle fallback format
    ]
    
    for potential_cache_file in cache_files_to_check:
        if potential_cache_file.exists():
            try:
                logger.info(f"Loading cached ERA5 data from: {potential_cache_file}")
                
                if potential_cache_file.suffix == '.pkl':
                    # Load pickle format
                    import pickle
                    with open(potential_cache_file, 'rb') as f:
                        ds_combined = pickle.load(f)
                    logger.info("Loaded cached data from pickle format")
                else:
                    # Load netCDF format with corrected engine handling
                    load_successful = False
                    
                    # 1. Try default engine first (most reliable)
                    try:
                        logger.debug("Attempting to load cache with default engine...")
                        ds_combined = xr.open_dataset(potential_cache_file)
                        logger.info("Successfully loaded cached data with default engine")
                        load_successful = True
                    except Exception as default_load_err:
                        logger.debug(f"Default load failed: {default_load_err}")
                        
                        # 2. Try explicit netcdf4 engine
                        try:
                            logger.debug("Attempting to load cache with netcdf4 engine...")
                            ds_combined = xr.open_dataset(potential_cache_file, engine='netcdf4')
                            logger.info("Successfully loaded cached data with netcdf4 engine")
                            load_successful = True
                        except Exception as netcdf4_load_err:
                            logger.debug(f"netcdf4 load failed: {netcdf4_load_err}")
                            
                            # 3. Try with decode_cf=False to handle problematic metadata
                            try:
                                logger.debug("Attempting to load cache with decode_cf=False...")
                                ds_combined = xr.open_dataset(potential_cache_file, decode_cf=False)
                                logger.info("Successfully loaded cached data with decode_cf=False")
                                load_successful = True
                            except Exception as decode_load_err:
                                logger.debug(f"decode_cf=False load failed: {decode_load_err}")
                    
                    if not load_successful:
                        logger.warning(f"Failed to load cache file with all methods: {potential_cache_file}")
                        continue
                
                # Validate cache content
                target_times_np_ns = [np.datetime64(t.replace(tzinfo=None), 'ns') for t in target_times]
                if all(t_np_ns in ds_combined.time.values for t_np_ns in target_times_np_ns):
                    logger.info("Cache hit is valid.")
                    
                    # CRITICAL FIX: Force data loading for cached datasets too
                    # This prevents lazy-loading issues when cache files might be moved/deleted
                    try:
                        logger.debug("Loading cached data into memory to prevent file access issues...")
                        for var_name in ds_combined.data_vars:
                            _ = ds_combined[var_name].values  # Force loading
                        logger.debug("Successfully loaded cached data into memory")
                    except Exception as load_err:
                        logger.warning(f"Error during cached data loading: {load_err}")
                        # Continue - the data might still be accessible
                    
                    return ds_combined
                else:
                    logger.warning("Cached file exists but missing requested times. Re-fetching.")
                    if hasattr(ds_combined, 'close'):
                        ds_combined.close()
                    potential_cache_file.unlink()
                    
            except Exception as e:
                logger.warning(f"Failed to load or validate cache file {potential_cache_file}: {e}. Re-fetching.")
                if potential_cache_file.exists():
                    try: 
                        potential_cache_file.unlink()
                    except OSError: 
                        pass

    logger.info(f"Cache miss or invalid cache for times: {time_strings[0]} to {time_strings[-1]}. Fetching from CDS API.")

    dates = sorted(list(set(t.strftime("%Y-%m-%d") for t in target_times)))
    times = sorted(list(set(t.strftime("%H:%M") for t in target_times)))

    common_request = {
        'product_type': 'reanalysis',
        'format': 'netcdf',
        'date': dates,
        'time': times,
        'grid': '0.25/0.25', # 0.25 degree resolution
    }

    single_level_request = common_request.copy()
    single_level_request['variable'] = list(ERA5_SINGLE_LEVEL_VARS.keys())

    pressure_level_request = common_request.copy()
    pressure_level_request['variable'] = list(ERA5_PRESSURE_LEVEL_VARS.keys())
    pressure_level_request['pressure_level'] = AURORA_PRESSURE_LEVELS

    def _sync_fetch_and_process():
        # THREADING FIX: Re-initialize netcdf4 backend in thread context
        try:
            import netCDF4
            import xarray as xr
            
            # Force re-registration of netcdf4 backend in this thread
            if hasattr(xr.backends, 'NetCDF4BackendEntrypoint'):
                try:
                    # Manually register the netcdf4 backend in this thread context
                    backend = xr.backends.NetCDF4BackendEntrypoint()
                    xr.backends.backends.BACKENDS['netcdf4'] = backend
                    logger.debug("Successfully re-registered netcdf4 backend in thread context")
                except Exception as backend_reg_err:
                    logger.warning(f"Could not manually register netcdf4 backend: {backend_reg_err}")
            
            logger.debug(f"Thread context - Available engines: {list(xr.backends.list_engines())}")
        except ImportError as netcdf_err:
            logger.warning(f"netCDF4 not available in thread context: {netcdf_err}")
        
        temp_sl_file = None
        temp_pl_file = None
        try:
            c = cdsapi.Client(quiet=True)
            
            fd_sl, temp_sl_path = tempfile.mkstemp(suffix='_era5_sl.nc', dir=cache_dir)
            fd_pl, temp_pl_path = tempfile.mkstemp(suffix='_era5_pl.nc', dir=cache_dir)
            os.close(fd_sl)
            os.close(fd_pl)
            temp_sl_file = Path(temp_sl_path)
            temp_pl_file = Path(temp_pl_path)

            api_sl_vars = ['2m_temperature', '10m_u_component_of_wind', '10m_v_component_of_wind', 'mean_sea_level_pressure']
            api_pl_vars = ['temperature', 'u_component_of_wind', 'v_component_of_wind', 'specific_humidity', 'geopotential']

            current_single_level_request = single_level_request.copy()
            current_single_level_request['variable'] = api_sl_vars

            current_pressure_level_request = pressure_level_request.copy()
            current_pressure_level_request['variable'] = api_pl_vars

            logger.info(f"Requesting ERA5 single level data with vars: {api_sl_vars}...")
            c.retrieve(
                'reanalysis-era5-single-levels',
                current_single_level_request,
                str(temp_sl_file)
            )
            logger.info(f"Single level data downloaded to {temp_sl_file}")

            logger.info(f"Requesting ERA5 pressure level data with vars: {api_pl_vars}...")
            c.retrieve(
                'reanalysis-era5-pressure-levels',
                current_pressure_level_request,
                str(temp_pl_file)
            )
            logger.info(f"Pressure level data downloaded to {temp_pl_file}")

            logger.info("Loading and processing downloaded files...")
            
            # ROBUST BACKEND APPROACH: Use multiple fallback methods for maximum compatibility
            logger.info("Loading downloaded files using robust backend detection with fallbacks...")
            
            # Load single level file with corrected engine handling
            ds_sl = None
            try:
                logger.debug("Loading single level file with default engine...")
                ds_sl = xr.open_dataset(temp_sl_file)
                logger.debug("✅ Successfully loaded single level file with default engine")
            except Exception as default_err:
                logger.debug(f"Default load failed for single level: {default_err}")
                
                # Try explicit netcdf4 engine
                try:
                    logger.debug("Loading single level file with netcdf4 engine...")
                    ds_sl = xr.open_dataset(temp_sl_file, engine='netcdf4')
                    logger.debug("✅ Successfully loaded single level file with netcdf4 engine")
                except Exception as netcdf4_err:
                    logger.debug(f"netcdf4 load failed for single level: {netcdf4_err}")
                    
                    # Try with decode_cf=False
                    try:
                        logger.debug("Loading single level file with decode_cf=False...")
                        ds_sl = xr.open_dataset(temp_sl_file, decode_cf=False)
                        logger.debug("✅ Successfully loaded single level file with decode_cf=False")
                    except Exception as decode_err:
                        logger.debug(f"decode_cf=False load failed for single level: {decode_err}")
            
            if ds_sl is None:
                raise ValueError("Failed to load single level file with any method")
            
            # Load pressure level file with corrected engine handling
            ds_pl = None
            try:
                logger.debug("Loading pressure level file with default engine...")
                ds_pl = xr.open_dataset(temp_pl_file)
                logger.debug("✅ Successfully loaded pressure level file with default engine")
            except Exception as default_err:
                logger.debug(f"Default load failed for pressure level: {default_err}")
                
                # Try explicit netcdf4 engine
                try:
                    logger.debug("Loading pressure level file with netcdf4 engine...")
                    ds_pl = xr.open_dataset(temp_pl_file, engine='netcdf4')
                    logger.debug("✅ Successfully loaded pressure level file with netcdf4 engine")
                except Exception as netcdf4_err:
                    logger.debug(f"netcdf4 load failed for pressure level: {netcdf4_err}")
                    
                    # Try with decode_cf=False
                    try:
                        logger.debug("Loading pressure level file with decode_cf=False...")
                        ds_pl = xr.open_dataset(temp_pl_file, decode_cf=False)
                        logger.debug("✅ Successfully loaded pressure level file with decode_cf=False")
                    except Exception as decode_err:
                        logger.debug(f"decode_cf=False load failed for pressure level: {decode_err}")
            
            if ds_pl is None:
                raise ValueError("Failed to load pressure level file with any method")

            logger.info(f"Variables in downloaded single-level ERA5: {list(ds_sl.data_vars)}")
            logger.info(f"Variables in downloaded pressure-level ERA5: {list(ds_pl.data_vars)}")

            rename_map_sl = {
                't2m': '2t',
                'u10': '10u',
                'v10': '10v',
                'msl': 'msl' 
            }
            rename_map_pl = {
                't': 't',
                'u': 'u',
                'v': 'v',
                'q': 'q',
                'z': 'z'
            }

            ds_sl_renamed = ds_sl.rename({k: v for k, v in rename_map_sl.items() if k in ds_sl})
            ds_pl_renamed = ds_pl.rename({k: v for k, v in rename_map_pl.items() if k in ds_pl})
            
            if '2t' not in ds_sl_renamed.data_vars and ('t2m' in ds_sl.data_vars or '2m_temperature' in ds_sl.data_vars) :
                logger.warning(f"Variable '2t' not found after renaming. Original SL vars: {list(ds_sl.data_vars)}. Renamed SL vars: {list(ds_sl_renamed.data_vars)}")

            logger.info(f"Renamed ds_sl_renamed data_vars: {list(ds_sl_renamed.data_vars)}")
            logger.info(f"Renamed ds_pl_renamed data_vars: {list(ds_pl_renamed.data_vars)}")
            
            ds_combined = xr.merge([ds_sl_renamed, ds_pl_renamed])

            rename_coords = {}
            if 'latitude' in ds_combined.coords: rename_coords['latitude'] = 'lat'
            if 'longitude' in ds_combined.coords: rename_coords['longitude'] = 'lon'
            if 'valid_time' in ds_combined.coords: rename_coords['valid_time'] = 'time'
            if rename_coords:
                ds_combined = ds_combined.rename(rename_coords)
                logger.info(f"Renamed coordinates: {list(rename_coords.keys())} -> {list(rename_coords.values())}")

            if 'z' in ds_combined:
                logger.info("Ensuring Geopotential (z) units and attributes...")
                if 'units' not in ds_combined['z'].attrs or ds_combined['z'].attrs['units'].lower() != 'm**2 s**-2':
                     ds_combined['z'].attrs['units'] = 'm**2 s**-2'
                ds_combined['z'].attrs['standard_name'] = 'geopotential'
                ds_combined['z'].attrs['long_name'] = 'Geopotential'

            if 'lat' in ds_combined.coords and len(ds_combined.lat) > 1 and ds_combined.lat.values[0] < ds_combined.lat.values[-1]:
                logger.info("Reversing latitude coordinate to be decreasing (90 to -90).")
                ds_combined = ds_combined.reindex(lat=ds_combined.lat[::-1])

            if 'lon' in ds_combined.coords and ds_combined.lon.values.min() < 0:
                logger.info("Adjusting longitude coordinate from [-180, 180] to [0, 360).")
                ds_combined.coords['lon'] = (ds_combined.coords['lon'] + 360) % 360
                ds_combined = ds_combined.sortby(ds_combined.lon)

            if 'time' in ds_combined.coords:
                target_times_np_ns = [np.datetime64(t.replace(tzinfo=None), 'ns') for t in target_times]
                ds_combined = ds_combined.sel(time=target_times_np_ns, method='nearest')
                logger.info("Selected target time steps.")
            else:
                logger.error("Processed dataset missing 'time' coordinate after potential rename. Cannot select times.")
                raise ValueError("Missing 'time' coordinate in processed dataset")

            logger.info("Data processing and coordinate adjustments complete.")
            
            # Force data loading before temp file cleanup
            logger.info("Loading data into memory to prevent lazy-loading issues with temporary files...")
            try:
                for var_name in ds_combined.data_vars:
                    _ = ds_combined[var_name].values  # Force loading
                logger.debug("Successfully loaded all variables into memory")
            except Exception as load_err:
                logger.warning(f"Error during forced data loading: {load_err}")
            
            logger.info(f"Saving processed data to cache: {cache_filename}")
            
            # IMPROVED SAVE APPROACH: Use auto-detection first, then available engines
            save_successful = False
            
            # FIXED: Robust save approach with correct xarray engine usage
            save_successful = False
            
            # 1. Try default netcdf4 (most common and reliable)
            try:
                logger.debug("Attempting to save with default netcdf4 engine...")
                ds_combined.to_netcdf(cache_filename)  # Default engine
                logger.info(f"✅ Successfully saved processed data with default engine: {cache_filename}")
                save_successful = True
            except Exception as default_save_err:
                logger.debug(f"Default save failed: {default_save_err}")
                
                # 2. Try explicit netcdf4 engine
                try:
                    logger.debug("Attempting to save with explicit netcdf4 engine...")
                    ds_combined.to_netcdf(cache_filename, engine='netcdf4')
                    logger.info(f"✅ Successfully saved with netcdf4 engine: {cache_filename}")
                    save_successful = True
                except Exception as netcdf4_save_err:
                    logger.debug(f"netcdf4 save failed: {netcdf4_save_err}")
                    
                    # 3. Try with computed dataset (resolve lazy operations)
                    try:
                        logger.debug("Computing dataset and saving with default engine...")
                        ds_computed = ds_combined.compute()
                        ds_computed.to_netcdf(cache_filename)
                        logger.info(f"✅ Successfully saved computed dataset: {cache_filename}")
                        save_successful = True
                    except Exception as computed_save_err:
                        logger.debug(f"Computed dataset save failed: {computed_save_err}")
                        
                        # 4. Try NETCDF4 format explicitly
                        try:
                            logger.debug("Attempting to save with NETCDF4 format...")
                            if 'ds_computed' not in locals():
                                ds_computed = ds_combined.compute()
                            ds_computed.to_netcdf(cache_filename, format='NETCDF4')
                            logger.info(f"✅ Successfully saved with NETCDF4 format: {cache_filename}")
                            save_successful = True
                        except Exception as netcdf4_format_save_err:
                            logger.debug(f"NETCDF4 format save failed: {netcdf4_format_save_err}")
                            
                            # 5. Try NETCDF3_64BIT format (most compatible)
                            try:
                                logger.debug("Attempting to save with NETCDF3_64BIT format...")
                                if 'ds_computed' not in locals():
                                    ds_computed = ds_combined.compute()
                                ds_computed.to_netcdf(cache_filename, format='NETCDF3_64BIT')
                                logger.info(f"✅ Successfully saved with NETCDF3_64BIT format: {cache_filename}")
                                save_successful = True
                            except Exception as netcdf3_save_err:
                                logger.debug(f"NETCDF3_64BIT save failed: {netcdf3_save_err}")
            
            if not save_successful:
                logger.error("Failed to save with all netCDF engines, using pickle fallback...")
                try:
                    pickle_filename = cache_filename.with_suffix('.pkl')
                    import pickle
                    with open(pickle_filename, 'wb') as f:
                        pickle.dump(ds_combined, f)
                    logger.warning(f"Saved data in pickle format as fallback: {pickle_filename}")
                except Exception as pickle_err:
                    logger.error(f"Pickle fallback also failed: {pickle_err}")

            return ds_combined

        except Exception as e:
            logger.error(f"Error during ERA5 sync fetch/process: {e}")
            logger.info(traceback.format_exc())
            return None
        finally:
            if temp_sl_file and temp_sl_file.exists():
                try: temp_sl_file.unlink()
                except OSError: pass
            if temp_pl_file and temp_pl_file.exists():
                try: temp_pl_file.unlink()
                except OSError: pass

    try:
        result_dataset = await asyncio.to_thread(_sync_fetch_and_process)
        return result_dataset
    except Exception as e:
        logger.error(f"Failed to run ERA5 fetch in thread: {e}")
        return None

async def _test_fetch():
    print("Testing ERA5 fetch...")
    test_times = [1,
        datetime(2025, 4, 9, 6, 0),
        datetime(2025, 4, 9, 12, 0)
    ]
    cache = Path("./era5_test_cache")
    era5_data = await fetch_era5_data(test_times, cache_dir=cache)

    if era5_data:
        print("\nSuccessfully fetched ERA5 data:")
        print(era5_data)
        print(f"\nData saved/cached in: {cache}")
    else:
        print("\nFailed to fetch ERA5 data. Check logs and ~/.cdsapirc configuration.")

if __name__ == "__main__":
    # Uncomment to test
    asyncio.run(_test_fetch())
    pass 