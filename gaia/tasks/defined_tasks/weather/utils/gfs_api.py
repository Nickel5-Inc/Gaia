import os
from fiber.logging_utils import get_logger
from datetime import datetime, timedelta
from typing import List, Optional, Union, Tuple
import numpy as np
import xarray as xr
import warnings
import asyncio
import hashlib
import tempfile
import traceback
from pathlib import Path

logger = get_logger('gfs_api')
G = 9.80665

warnings.filterwarnings('ignore',
                       message='Ambiguous reference date string',
                       category=xr.SerializationWarning,
                       module='xarray.coding.times')

warnings.filterwarnings('ignore',
                       message='numpy.core.numeric is deprecated',
                       category=DeprecationWarning)

# --- Constants --- 
DODS_BASE_URL = "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs"
AURORA_PRESSURE_LEVELS_HPA = [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000]
GFS_SURFACE_VARS = ["tmp2m", "ugrd10m", "vgrd10m", "prmslmsl"]
GFS_ATMOS_VARS = ["tmpprs", "ugrdprs", "vgrdprs", "spfhprs", "hgtprs"]

async def fetch_gfs_data(run_time: datetime, lead_hours: List[int], output_dir: Optional[str] = None) -> xr.Dataset:
    """
    Fetch GFS data asynchronously for the given run_time and lead_hours using OPeNDAP.

    Args:
        run_time: The model run datetime (e.g., 2023-08-01 00:00)
        lead_hours: List of forecast lead times in hours to retrieve
        output_dir: Optional directory to save NetCDF output (if None, don't save files)

    Returns:
        xr.Dataset: Dataset containing all required variables, processed for Aurora.
    """
    logger.info(f"Asynchronously fetching GFS data for run time: {run_time}, lead hours: {lead_hours}")

    def _sync_fetch_and_process():
        """Synchronous function containing the blocking xarray/OPeNDAP logic."""
        logger.debug(f"Executing synchronous fetch for {run_time} in thread.")
        date_str = run_time.strftime('%Y%m%d')
        cycle_str = f"{run_time.hour:02d}"

        dods_base = "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs"
        base_url = f"{dods_base}{date_str}/gfs_0p25_{cycle_str}z"
        logger.info(f"Using OPeNDAP URL: {base_url}")

        aurora_pressure_levels = [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000]
        aurora_level_indices = [22, 20, 19, 18, 17, 16, 14, 12, 10, 8, 5, 3, 0]

        surface_vars = ["tmp2m", "ugrd10m", "vgrd10m", "prmslmsl"]
        atmos_vars = ["tmpprs", "ugrdprs", "vgrdprs", "spfhprs", "hgtprs"]

        valid_times = [run_time + timedelta(hours=h) for h in lead_hours]

        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=xr.SerializationWarning)

                logger.info("Opening dataset via OPeNDAP (this might take time)...")
                full_ds = xr.open_dataset(base_url, decode_times=True)
                logger.info("Dataset metadata loaded. Selecting time indices.")

                time_indices = []
                dataset_times_np = full_ds.time.values
                for vt in valid_times:
                    vt_np = np.datetime64(vt)
                    time_diffs = np.abs(dataset_times_np - vt_np)
                    closest_idx = np.argmin(time_diffs)
                    time_indices.append(closest_idx)
                    actual_time = dataset_times_np[closest_idx]
                    if abs(vt_np - actual_time) > np.timedelta64(3, 'h'):
                        logger.warning(f"Requested time {vt} has large difference from closest dataset time {actual_time} at index {closest_idx}")
                    else:
                        logger.debug(f"Requested time {vt} matches dataset time {actual_time} at index {closest_idx}")

                time_indices = sorted(list(set(time_indices)))
                logger.info(f"Selected time indices: {time_indices}")
                logger.info(f"Loading surface variables: {surface_vars} at selected times.")
                surface_ds = full_ds[surface_vars].isel(time=time_indices).load()
                logger.info("Surface variables loaded.")

                atmos_ds_list = []
                for var in atmos_vars:
                    logger.info(f"Loading atmospheric variable: {var} at selected times and levels.")
                    if var in full_ds:
                        var_ds = full_ds[[var]].isel(time=time_indices, lev=aurora_level_indices).load()
                        logger.debug(f"Loaded {var}, shape: {var_ds[var].shape}")
                        atmos_ds_list.append(var_ds)
                    else:
                        logger.warning(f"Atmospheric variable {var} not found in dataset.")

                if atmos_ds_list:
                    atmos_ds = xr.merge(atmos_ds_list)
                    logger.info("Atmospheric variables loaded and merged.")
                else:
                    atmos_ds = xr.Dataset()

                full_ds.close()
                logger.info("Closed remote dataset connection.")
                ds = xr.merge([surface_ds, atmos_ds])

                if output_dir:
                    try:
                        os.makedirs(output_dir, exist_ok=True)
                        out_file = os.path.join(output_dir, f"gfs_raw_{date_str}_{cycle_str}z.nc")
                        logger.info(f"Saving raw fetched data to: {out_file}")
                        ds.to_netcdf(out_file)
                    except Exception as save_err:
                         logger.error(f"Failed to save raw NetCDF file: {save_err}")

                logger.info("Processing fetched data for Aurora requirements...")
                processed_ds = process_opendap_dataset(ds)
                logger.info("Data processing complete.")

                return processed_ds

        except Exception as e:
            logger.error(f"Error during synchronous OPeNDAP fetch/process: {e}", exc_info=True)
            if 'full_ds' in locals() and hasattr(full_ds, 'close'):
                try: full_ds.close()
                except: pass
            raise

    try:
        result_dataset = await asyncio.to_thread(_sync_fetch_and_process)
        return result_dataset
    except Exception as e:
        logger.error(f"Async fetch GFS data failed: {e}")
        return None


def process_opendap_dataset(ds: xr.Dataset) -> xr.Dataset:
    """Process the OPeNDAP dataset to match Aurora's expected format, including Geopotential conversion."""
    logger.debug("Starting dataset processing...")
    var_attrs = {var_name: ds[var_name].attrs.copy() for var_name in ds.data_vars}

    var_mapping = {
        'tmp2m': '2t',
        'ugrd10m': '10u',
        'vgrd10m': '10v',
        'prmslmsl': 'msl',
        'tmpprs': 't',
        'ugrdprs': 'u',
        'vgrdprs': 'v',
        'spfhprs': 'q',
        'hgtprs': 'z_height',
    }

    new_ds = xr.Dataset(coords=ds.coords)
    found_vars = []
    for old_name, new_name in var_mapping.items():
        if old_name in ds:
            new_ds[new_name] = ds[old_name].copy(deep=True)
            if old_name in var_attrs:
                 new_ds[new_name].attrs = var_attrs[old_name]
            found_vars.append(new_name)
        else:
            logger.debug(f"Variable {old_name} not found in input dataset.")

    logger.debug(f"Renamed variables present: {found_vars}")

    if 'z_height' in new_ds:
        logger.info("Converting Geopotential Height (z_height) to Geopotential (z)...")
        z_height_var = new_ds['z_height']
        geopotential = G * z_height_var
        new_ds['z'] = geopotential
        new_ds['z'].attrs['units'] = 'm2 s-2'
        new_ds['z'].attrs['long_name'] = 'Geopotential'
        new_ds['z'].attrs['standard_name'] = 'geopotential'
        new_ds['z'].attrs['comment'] = f'Calculated as g * z_height, with g={G} m/s^2'
        del new_ds['z_height']
        logger.info("Geopotential (z) calculated and added.")
    elif 'z' not in new_ds:
        logger.warning("Geopotential Height (hgtprs/z_height) not found, cannot calculate Geopotential (z).")


    # Units 
    default_units = {
        '2t': 'K', '10u': 'm s-1', '10v': 'm s-1', 'msl': 'Pa',
        't': 'K', 'u': 'm s-1', 'v': 'm s-1', 'q': 'kg kg-1', 'z': 'm2 s-2'
    }

    for var_name, expected_units in default_units.items():
        if var_name in new_ds:
            current_units = new_ds[var_name].attrs.get('units', '').lower()
            if not current_units:
                logger.debug(f"Assigning default units '{expected_units}' to {var_name}.")
                new_ds[var_name].attrs['units'] = expected_units
            elif var_name == 'msl' and current_units in ['hpa', 'millibars', 'mb']:
                logger.info(f"Converting MSL pressure from {current_units} to Pa.")
                new_ds['msl'] = new_ds['msl'] * 100.0
                new_ds['msl'].attrs['units'] = 'Pa'
            elif current_units != expected_units.lower():
                 logger.warning(f"Unexpected units for {var_name}. Expected '{expected_units}', found '{current_units}'. No conversion applied.")

    if 'lat' in new_ds.coords:
        if new_ds.lat.values[0] < new_ds.lat.values[-1]:
            logger.info("Reversing latitude coordinate to be decreasing (90 to -90).")
            new_ds = new_ds.reindex(lat=new_ds.lat[::-1])

    if 'lon' in new_ds.coords:
        if new_ds.lon.values.min() < -1.0
            logger.info("Adjusting longitude coordinate from [-180, 180] to [0, 360).")
            new_ds = new_ds.assign_coords(lon=(((new_ds.lon + 180) % 360) - 180 + 360) % 360) # Careful conversion
            new_ds = new_ds.sortby('lon')
        elif new_ds.lon.values.max() >= 360.0:
             logger.info("Adjusting longitude to be strictly < 360.")
             new_ds = new_ds.sel(lon=new_ds.lon < 360.0)

    logger.debug("Dataset processing finished.")
    return new_ds


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

def _get_gfs_cycle_url(target_time: datetime) -> Optional[str]:
    """Constructs the OPeNDAP URL for the GFS cycle initialized at target_time."""
    if target_time.hour % 6 != 0 or target_time.minute != 0 or target_time.second != 0:
        logger.error(f"Target time {target_time} is not a valid GFS cycle init time (00, 06, 12, 18 UTC). Cannot fetch analysis.")
        return None
        
    date_str = target_time.strftime('%Y%m%d')
    cycle_str = f"{target_time.hour:02d}"
    url = f"{DODS_BASE_URL}{date_str}/gfs_0p25_{cycle_str}z"
    return url

async def fetch_gfs_analysis_data(
    target_times: List[datetime],
    cache_dir: Path = Path("./gfs_analysis_cache")
) -> Optional[xr.Dataset]:
    """
    Fetches GFS ANALYSIS data (T+0h) asynchronously for multiple specific times.
    Uses OPeNDAP and file caching.

    Args:
        target_times: List of exact datetime objects for which to fetch analysis.
                      Each time MUST correspond to a GFS cycle time (00, 06, 12, 18 UTC).
        cache_dir: Directory for caching processed analysis files.

    Returns:
        xr.Dataset: Combined dataset of processed analysis variables for target times, or None.
    """
    if not target_times:
        logger.warning("fetch_gfs_analysis_data called with no target_times.")
        return None

    valid_target_times = []
    for t in target_times:
        if t.hour % 6 == 0 and t.minute == 0 and t.second == 0:
            valid_target_times.append(t)
        else:
            logger.warning(f"Skipping invalid target time {t}: Not a GFS cycle hour (00, 06, 12, 18 UTC).")
    
    if not valid_target_times:
        logger.error("No valid GFS cycle times provided for analysis fetching.")
        return None
    
    target_times = sorted(list(set(valid_target_times)))

    cache_dir.mkdir(parents=True, exist_ok=True)
    time_strings = [t.strftime("%Y%m%d%H") for t in target_times]
    cache_key = hashlib.md5("_anal_".join(time_strings).encode()).hexdigest()
    cache_filename = cache_dir / f"gfs_analysis_{cache_key}.nc"

    if cache_filename.exists():
        try:
            logger.info(f"Loading cached GFS analysis data from: {cache_filename}")
            ds_cached = xr.open_dataset(cache_filename)
            # Check if all times are present
            if all(np.datetime64(t) in ds_cached.time for t in target_times):
                logger.info("GFS Analysis cache hit is valid.")
                return ds_cached
            else:
                logger.warning("Cached GFS analysis file missing requested times. Re-fetching.")
                ds_cached.close()
                cache_filename.unlink()
        except Exception as e:
            logger.warning(f"Failed load/validate GFS analysis cache {cache_filename}: {e}. Re-fetching.")
            if cache_filename.exists():
                try: cache_filename.unlink()
                except OSError: pass

    logger.info(f"GFS Analysis cache miss for times: {time_strings[0]}...{time_strings[-1]}. Fetching from NOMADS.")

    def _sync_fetch_and_process_analysis():
        analysis_slices = []
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=xr.SerializationWarning)
            
            for target_time in target_times:
                full_ds = None
                try:
                    base_url = _get_gfs_cycle_url(target_time)
                    if not base_url:
                        logger.error(f"Could not get URL for GFS cycle {target_time}. Skipping.")
                        continue
                        
                    logger.info(f"Opening GFS cycle {target_time} from {base_url} for analysis (T+0h)...")
                    full_ds = xr.open_dataset(base_url, decode_times=True, chunks={})

                    analysis_slice = full_ds.sel(time=target_time, method='nearest')
                    if abs(analysis_slice.time.values - np.datetime64(target_time)) > np.timedelta64(1, 'h'):
                        logger.error(f"Nearest time found ({analysis_slice.time.values}) is too far from requested analysis time {target_time} for cycle {target_time}. Skipping.")
                        continue

                    vars_to_load = [v for v in GFS_SURFACE_VARS + GFS_ATMOS_VARS if v in full_ds]
                    if not vars_to_load:
                         logger.warning(f"No expected GFS variables found in dataset for cycle {target_time}. Skipping analysis for this time.")
                         continue
                         
                    loaded_slice = analysis_slice[vars_to_load].load()
                    loaded_slice = loaded_slice.expand_dims(dim='time', axis=0)
                    loaded_slice['time'] = [np.datetime64(target_time)]
                    
                    logger.info(f"Loaded analysis slice for {target_time}")
                    analysis_slices.append(loaded_slice)

                except Exception as e:
                    logger.error(f"Failed to fetch/process analysis for {target_time}: {e}", exc_info=True)
                finally:
                    if full_ds:
                        try: full_ds.close()
                        except: pass

        if not analysis_slices:
            logger.error("Failed to fetch any valid GFS analysis slices.")
            return None

        try:
            logger.info(f"Combining {len(analysis_slices)} analysis slices...")
            combined_ds = xr.concat(analysis_slices, dim='time')
        except Exception as e_concat:
             logger.error(f"Failed to combine analysis slices: {e_concat}")
             return None
             
        logger.debug("Starting analysis dataset processing...")
        
        var_mapping = {
            'tmp2m': '2t',
            'ugrd10m': '10u',
            'vgrd10m': '10v',
            'prmslmsl': 'msl',
            'tmpprs': 't',
            'ugrdprs': 'u',
            'vgrdprs': 'v',
            'spfhprs': 'q',
            'hgtprs': 'z_height', 
        }
        vars_to_rename = {k: v for k, v in var_mapping.items() if k in combined_ds.data_vars}
        processed_ds = combined_ds.rename(vars_to_rename)
        logger.debug(f"Renamed variables: {vars_to_rename}")

        coord_rename = {}
        if 'lev' in processed_ds.coords: coord_rename['lev'] = 'pressure_level'
        if 'latitude' in processed_ds.coords: coord_rename['latitude'] = 'lat'
        if 'longitude' in processed_ds.coords: coord_rename['longitude'] = 'lon'
        if coord_rename:
            processed_ds = processed_ds.rename(coord_rename)
            logger.debug(f"Renamed coords: {coord_rename}")

        if 'z_height' in processed_ds:
            logger.info("Converting Geopotential Height (z_height) to Geopotential (z)...")
            try:
                z_height_var = processed_ds['z_height']
                geopotential = G * z_height_var
                processed_ds['z'] = geopotential
                processed_ds['z'].attrs['units'] = 'm2 s-2'
                processed_ds['z'].attrs['long_name'] = 'Geopotential'
                processed_ds['z'].attrs['standard_name'] = 'geopotential'
                processed_ds['z'].attrs['comment'] = f'Calculated as g * z_height, with g={G} m/s^2'
                processed_ds = processed_ds.drop_vars(['z_height'])
            except Exception as e_z:
                 logger.error(f"Failed to calculate geopotential 'z': {e_z}")
        elif 'z' not in processed_ds:
            logger.warning("Geopotential Height (hgtprs/z_height) not found.")

        if 'pressure_level' in processed_ds.coords:
             if processed_ds['pressure_level'].attrs.get('units', '').lower() == 'pa':
                  logger.info("Converting pressure_level coordinate from Pa to hPa.")
                  processed_ds['pressure_level'] = processed_ds['pressure_level'] / 100.0
                  processed_ds['pressure_level'].attrs['units'] = 'hPa'
                  try:
                       processed_ds = processed_ds.sel(pressure_level=AURORA_PRESSURE_LEVELS_HPA)
                       logger.info(f"Selected standard pressure levels: {AURORA_PRESSURE_LEVELS_HPA}")
                  except Exception as e_sel_p:
                       logger.warning(f"Could not select standard pressure levels after conversion: {e_sel_p}")
             elif processed_ds['pressure_level'].attrs.get('units', '').lower() != 'hpa':
                  logger.warning(f"Pressure level units are not Pa or hPa.")

        default_units = {
            '2t': 'K', '10u': 'm s-1', '10v': 'm s-1', 'msl': 'Pa',
            't': 'K', 'u': 'm s-1', 'v': 'm s-1', 'q': 'kg kg-1', 'z': 'm2 s-2'
        }
        for var_name, expected_units in default_units.items():
            if var_name in processed_ds and 'units' not in processed_ds[var_name].attrs:
                processed_ds[var_name].attrs['units'] = expected_units

        if 'lat' in processed_ds.coords and len(processed_ds.lat) > 1 and processed_ds.lat.values[0] < processed_ds.lat.values[-1]:
            logger.info("Reversing latitude coordinate.")
            processed_ds = processed_ds.reindex(lat=processed_ds.lat[::-1])

        if 'lon' in processed_ds.coords and processed_ds.lon.values.min() < 0:
            logger.info("Adjusting longitude coordinate to [0, 360).")
            processed_ds.coords['lon'] = (processed_ds.coords['lon'] + 360) % 360
            processed_ds = processed_ds.sortby(processed_ds.lon)
        elif 'lon' in processed_ds.coords and processed_ds.lon.values.max() >= 360.0:
             processed_ds = processed_ds.sel(lon=processed_ds.lon < 360.0)
             
        logger.debug("Analysis dataset processing finished.")

        try:
            logger.info(f"Saving processed GFS analysis data to cache: {cache_filename}")
            processed_ds.to_netcdf(cache_filename)
        except Exception as e_cache:
            logger.error(f"Failed to save GFS analysis to cache {cache_filename}: {e_cache}")
            
        return processed_ds

    try:
        result_dataset = await asyncio.to_thread(_sync_fetch_and_process_analysis)
        return result_dataset
    except Exception as e:
        logger.error(f"Async fetch GFS analysis data failed: {e}")
        return None

async def _test_fetch_analysis():
     print("Testing GFS Analysis fetch...")
     test_times = [
         datetime(2024, 6, 15, 0, 0),
         datetime(2024, 6, 15, 12, 0)
     ]
     cache = Path("./gfs_analysis_test_cache")
     gfs_analysis_data = await fetch_gfs_analysis_data(test_times, cache_dir=cache)
     
     if gfs_analysis_data:
         print("\nSuccessfully fetched GFS analysis data:")
         print(gfs_analysis_data)
         print(f"\nData saved/cached in: {cache}")
     else:
         print("\nFailed to fetch GFS analysis data. Check logs.")

if __name__ == "__main__":
    # asyncio.run(_test_fetch_analysis())
    pass
