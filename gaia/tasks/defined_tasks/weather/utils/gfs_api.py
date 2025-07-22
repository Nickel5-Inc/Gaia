import os
from fiber.logging_utils import get_logger
from datetime import datetime, timedelta, timezone
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

DODS_BASE_URL = "http://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs"
AURORA_PRESSURE_LEVELS_HPA = [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000]
GFS_SURFACE_VARS = ["tmp2m", "ugrd10m", "vgrd10m", "prmslmsl"]
GFS_ATMOS_VARS = ["tmpprs", "ugrdprs", "vgrdprs", "spfhprs", "hgtprs"]

async def fetch_gfs_data(run_time: datetime, lead_hours: List[int], output_dir: Optional[str] = None,
                         target_surface_vars: Optional[List[str]] = None,
                         target_atmos_vars: Optional[List[str]] = None,
                         target_pressure_levels_hpa: Optional[List[int]] = None) -> xr.Dataset:
    """
    Fetch GFS data asynchronously for the given run_time and lead_hours using OPeNDAP.

    Args:
        run_time: The model run datetime (e.g., 2023-08-01 00:00)
        lead_hours: List of forecast lead times in hours to retrieve
        output_dir: Optional directory to save NetCDF output (if None, don't save files)
        target_surface_vars: Optional list of GFS surface variable names to fetch.
        target_atmos_vars: Optional list of GFS atmospheric variable names to fetch.
        target_pressure_levels_hpa: Optional list of pressure levels (in hPa) for atmospheric variables.

    Returns:
        xr.Dataset: Dataset containing all required variables, processed for Aurora.
    """
    logger.info(f"Asynchronously fetching GFS data for run time: {run_time}, lead hours: {lead_hours}")

    def _sync_fetch_and_process(
        target_surface_vars: Optional[List[str]] = None,
        target_atmos_vars: Optional[List[str]] = None,
        target_pressure_levels_hpa: Optional[List[int]] = None
    ):
        """Synchronous function containing the blocking xarray/OPeNDAP logic."""
        # THREADING FIX: Explicitly import netCDF4 in thread context to ensure backend availability
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
            
        logger.debug(f"Executing synchronous fetch for {run_time} in thread.")
        date_str = run_time.strftime('%Y%m%d')
        cycle_str = f"{run_time.hour:02d}"

        dods_base = "http://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs"
        base_url = f"{dods_base}{date_str}/gfs_0p25_{cycle_str}z"
        logger.info(f"Using OPeNDAP URL: {base_url}")

        aurora_pressure_levels_const = [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000]
        aurora_level_indices_const = [22, 20, 19, 18, 17, 16, 14, 12, 10, 8, 5, 3, 0]

        surface_vars_to_fetch = GFS_SURFACE_VARS
        if target_surface_vars is not None:
            surface_vars_to_fetch = [var for var in target_surface_vars if var in GFS_SURFACE_VARS]
            logger.info(f"Fetching targeted surface variables: {surface_vars_to_fetch}")

        atmos_vars_to_fetch = GFS_ATMOS_VARS
        if target_atmos_vars is not None:
            atmos_vars_to_fetch = [var for var in target_atmos_vars if var in GFS_ATMOS_VARS]
            logger.info(f"Fetching targeted atmospheric variables: {atmos_vars_to_fetch}")

        level_indices_to_fetch = aurora_level_indices_const
        actual_pressure_levels_to_fetch_hpa = aurora_pressure_levels_const

        if target_pressure_levels_hpa is not None:
            valid_target_levels = [lvl for lvl in target_pressure_levels_hpa if lvl in aurora_pressure_levels_const]
            if not valid_target_levels:
                logger.warning(f"Target pressure levels {target_pressure_levels_hpa} have no overlap with available GFS pressure levels {aurora_pressure_levels_const} via configured indices. Atmospheric data might be empty.")
                level_indices_to_fetch = []
                actual_pressure_levels_to_fetch_hpa = []
            else:
                level_indices_to_fetch = []
                actual_pressure_levels_to_fetch_hpa = []
                for level_hpa in valid_target_levels:
                    try:
                        idx_in_const_list = aurora_pressure_levels_const.index(level_hpa)
                        level_indices_to_fetch.append(aurora_level_indices_const[idx_in_const_list])
                        actual_pressure_levels_to_fetch_hpa.append(level_hpa)
                    except ValueError:
                        logger.warning(f"Level {level_hpa} unexpectedly not found in aurora_pressure_levels_const.")
                logger.info(f"Fetching targeted pressure levels (hPa): {actual_pressure_levels_to_fetch_hpa} using indices: {level_indices_to_fetch}")
        else:
            logger.info(f"Fetching default pressure levels (hPa): {actual_pressure_levels_to_fetch_hpa} using indices: {level_indices_to_fetch}")

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
                    if vt.tzinfo is None:
                        vt_utc = vt.replace(tzinfo=timezone.utc)
                    else:
                        vt_utc = vt.astimezone(timezone.utc)
                    vt_naive_utc = vt_utc.replace(tzinfo=None)
                    vt_np = np.datetime64(vt_naive_utc)
                    
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
                logger.info(f"Loading surface variables: {surface_vars_to_fetch} at selected times.")
                if surface_vars_to_fetch:
                    surface_ds = full_ds[surface_vars_to_fetch].isel(time=time_indices).load()
                    logger.info("Surface variables loaded.")
                else:
                    surface_ds = xr.Dataset()
                    logger.info("No surface variables targeted for fetching.")

                atmos_ds_list = []
                if atmos_vars_to_fetch and level_indices_to_fetch:
                    for var in atmos_vars_to_fetch:
                        logger.info(f"Loading atmospheric variable: {var} at selected times and levels.")
                        if var in full_ds:
                            if 'lev' in full_ds[var].coords:
                                var_ds = full_ds[[var]].isel(time=time_indices, lev=level_indices_to_fetch).load()
                                logger.debug(f"Loaded {var}, shape: {var_ds[var].shape}")
                                atmos_ds_list.append(var_ds)
                            elif not level_indices_to_fetch:
                                logger.warning(f"Atmospheric variable {var} does not have 'lev' coordinate, but levels were specified. Check GFS_ATMOS_VARS.")
                            else:
                                var_ds = full_ds[[var]].isel(time=time_indices).load()
                                logger.debug(f"Loaded {var} (all levels as none specific requested/applicable), shape: {var_ds[var].shape}")
                                atmos_ds_list.append(var_ds)
                        else:
                            logger.warning(f"Atmospheric variable {var} not found in dataset.")
                elif not atmos_vars_to_fetch:
                    logger.info("No atmospheric variables targeted for fetching.")
                elif not level_indices_to_fetch:
                    logger.info("No pressure levels targeted for atmospheric variables.")

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
        result_dataset = await asyncio.to_thread(_sync_fetch_and_process,
                                                 target_surface_vars=target_surface_vars,
                                                 target_atmos_vars=target_atmos_vars,
                                                 target_pressure_levels_hpa=target_pressure_levels_hpa)
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

    coord_rename_map = {}
    if 'lev' in new_ds.coords:
        coord_rename_map['lev'] = 'pressure_level'
    if 'latitude' in new_ds.coords:
        coord_rename_map['latitude'] = 'lat'
    if 'longitude' in new_ds.coords:
        coord_rename_map['longitude'] = 'lon'
    
    if coord_rename_map:
        new_ds = new_ds.rename(coord_rename_map)
        logger.info(f"Renamed coordinates: {coord_rename_map}")

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
            new_ds = new_ds.reindex(lat=new_ds.lat.values[::-1].copy())

    if 'lon' in new_ds.coords:
        if new_ds.lon.values.min() < -1.0:
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
    cache_dir: Path = Path("./gfs_analysis_cache"),
    target_surface_vars: Optional[List[str]] = None,
    target_atmos_vars: Optional[List[str]] = None,
    target_pressure_levels_hpa: Optional[List[int]] = None,
    progress_callback: Optional[callable] = None
) -> Optional[xr.Dataset]:
    """
    Fetches GFS ANALYSIS data (T+0h) asynchronously for multiple specific times.
    Uses OPeNDAP and file caching.

    Args:
        target_times: List of exact datetime objects for which to fetch analysis.
                      Each time MUST correspond to a GFS cycle time (00, 06, 12, 18 UTC).
        cache_dir: Directory for caching processed analysis files.
        target_surface_vars: Optional list of surface variable names to fetch.
        target_atmos_vars: Optional list of atmospheric variable names to fetch.
        target_pressure_levels_hpa: Optional list of pressure levels (in hPa) for atmospheric variables.
        progress_callback: Optional callback function for progress updates.

    Returns:
        xr.Dataset: Combined dataset of processed analysis variables for target times, or None.
    """
    if not target_times:
        logger.warning("fetch_gfs_analysis_data called with no target_times.")
        return None

    # Emit initial progress
    if progress_callback:
        await progress_callback({
            "operation": "gfs_download",
            "stage": "initializing",
            "progress": 0.0,
            "message": f"Starting GFS analysis fetch for {len(target_times)} time(s)"
        })

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

    # Check for both netCDF and pickle cache files
    pickle_filename = cache_filename.with_suffix('.pkl')
    cache_file_to_use = None
    is_pickle = False
    
    if cache_filename.exists():
        cache_file_to_use = cache_filename
        is_pickle = False
    elif pickle_filename.exists():
        cache_file_to_use = pickle_filename
        is_pickle = True
    
    if cache_file_to_use:
        try:
            if progress_callback:
                await progress_callback({
                    "operation": "gfs_download",
                    "stage": "loading_cache",
                    "progress": 0.5,
                    "message": f"Loading cached GFS analysis from {cache_file_to_use.name}"
                })
            
            logger.info(f"Loading cached GFS analysis data from: {cache_file_to_use}")
            
            if is_pickle:
                import pickle
                with open(cache_file_to_use, 'rb') as f:
                    ds_cached = pickle.load(f)
                logger.info("Successfully loaded GFS analysis from pickle cache")
            else:
                # Try different engines for netCDF loading
                ds_cached = None
                engines_to_try = ['netcdf4', 'scipy', 'h5netcdf']
                
                for engine in engines_to_try:
                    try:
                        ds_cached = xr.open_dataset(cache_file_to_use, engine=engine)
                        logger.debug(f"Successfully loaded cache using '{engine}' engine")
                        break
                    except Exception as engine_err:
                        logger.debug(f"Failed to load with '{engine}' engine: {engine_err}")
                        continue
                
                if ds_cached is None:
                    raise Exception("Failed to load with any netCDF engine")
            
            target_times_np_ns = [np.datetime64(t.replace(tzinfo=None), 'ns') for t in target_times]
            if all(t_np_ns in ds_cached.time.values for t_np_ns in target_times_np_ns):
                logger.info("GFS Analysis cache hit is valid.")
                
                if progress_callback:
                    file_size = cache_file_to_use.stat().st_size if cache_file_to_use.exists() else 0
                    await progress_callback({
                        "operation": "gfs_download",
                        "stage": "completed",
                        "progress": 1.0,
                        "message": f"Successfully loaded from cache ({file_size} bytes)",
                        "bytes_downloaded": file_size,
                        "bytes_total": file_size
                    })
                
                return ds_cached
            else:
                logger.warning("Cached GFS analysis file missing requested times. Re-fetching.")
                if hasattr(ds_cached, 'close'):
                    ds_cached.close()
                cache_file_to_use.unlink()
        except Exception as e:
            logger.warning(f"Failed load/validate GFS analysis cache {cache_file_to_use}: {e}. Re-fetching.")
            if cache_file_to_use.exists():
                try: cache_file_to_use.unlink()
                except OSError: pass

    logger.info(f"GFS Analysis cache miss for times: {time_strings[0]}...{time_strings[-1]}. Fetching from NOMADS.")
    
    if progress_callback:
        await progress_callback({
            "operation": "gfs_download",
            "stage": "downloading",
            "progress": 0.1,
            "message": f"Fetching GFS analysis data from NOMADS for {len(target_times)} times"
        })

    def _sync_fetch_and_process_analysis():
        # THREADING FIX: Explicitly import netCDF4 in thread context to ensure backend availability
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
        
        analysis_slices = []
        progress_info = {"processed_files": 0, "total_files": len(target_times)}
        
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=xr.SerializationWarning)
            
            total_steps = len(target_times)
            for idx, target_time in enumerate(target_times):
                processed_one_successfully_for_target = False
                try:
                    # Update progress info (to be reported back to async context)
                    step_progress = 0.1 + (0.7 * (idx / total_steps))  # 10% to 80% for downloading
                    progress_info.update({
                        "current_step": idx + 1,
                        "step_progress": step_progress,
                        "current_time": target_time.strftime('%Y-%m-%d %H:%M'),
                        "files_completed": idx
                    })
                    
                    base_url = _get_gfs_cycle_url(target_time)
                    if not base_url:
                        logger.error(f"Could not get URL for GFS cycle {target_time}. Skipping.")
                        continue
                        
                    logger.info(f"Opening GFS cycle {target_time.strftime('%Y-%m-%d %H:%M:%S')} from {base_url} for analysis (T+0h)...")
                    full_ds = xr.open_dataset(base_url, decode_times=False, chunks={})
                    logger.info(f"Successfully opened GFS dataset with decode_times=False. Raw time variable attributes: {full_ds.time.attrs}")
                    logger.info(f"Raw time variable sample values: {full_ds.time.values[:5] if len(full_ds.time.values) > 5 else full_ds.time.values}")
                    
                    # MEMORY LEAK FIX: Track dataset for cleanup
                    dataset_to_cleanup = full_ds
                    
                    time_coordinate_decoded_successfully = False
                    try:
                        # Attempt to decode the time coordinate using cftime
                        decoded_time_values_np_array = xr.coding.times.decode_cf_datetime(
                            full_ds.time, 
                            units=full_ds.time.attrs.get('units'), 
                            calendar=full_ds.time.attrs.get('calendar', 'standard'), 
                            use_cftime=True
                        )
                        # Log the sample of the numpy array directly
                        logger.info(f"Manually decoded time with cftime (sample numpy array): {decoded_time_values_np_array[:5] if len(decoded_time_values_np_array) > 5 else decoded_time_values_np_array}")
                        # Replace the time coordinate in full_ds
                        full_ds = full_ds.assign_coords(time=decoded_time_values_np_array)
                        time_coordinate_decoded_successfully = True
                        logger.info(f"Time coordinate in full_ds updated. Sample from full_ds.time.values: {full_ds.time.values[:5] if len(full_ds.time.values) > 5 else full_ds.time.values}")
                    except Exception as e_manual_decode:
                        logger.error(f"Failed to manually decode time with cftime: {e_manual_decode}", exc_info=True)
                        logger.warning("Proceeding with original undecoded (numerical) time coordinate for selection.")

                    analysis_target_dt_np = np.datetime64(target_time.replace(tzinfo=None))
                    analysis_slice = None # Initialize to ensure it's defined for logging in case of error
                    time_difference = np.timedelta64(24*3600, 's') # Default to a large diff

                    if time_coordinate_decoded_successfully:
                        # full_ds.time is now datetime-like (cftime or datetime64)
                        # Check for duplicate time values and handle them
                        time_values = full_ds.time.values
                        unique_times, unique_indices = np.unique(time_values, return_index=True)
                        
                        if len(unique_times) < len(time_values):
                            logger.warning(f"Found {len(time_values) - len(unique_times)} duplicate time values in GFS dataset. Removing duplicates.")
                            # Keep only the first occurrence of each unique time
                            unique_indices_sorted = np.sort(unique_indices)
                            full_ds = full_ds.isel(time=unique_indices_sorted)
                        
                        analysis_slice = full_ds.sel(time=analysis_target_dt_np, method='nearest')
                        # analysis_slice.time.values is a scalar cftime.DatetimeGregorian (or similar cftime object)
                        # Convert cftime object to standard Python datetime, then to np.datetime64
                        cftime_obj = analysis_slice.time.values.item()
                        
                        # Construct standard Python datetime from cftime object components
                        # Keep it naive, as np.datetime64 is naive and analysis_target_dt_np is also naive.
                        py_dt = datetime(
                            year=int(cftime_obj.year), month=int(cftime_obj.month), day=int(cftime_obj.day),
                            hour=int(cftime_obj.hour), minute=int(cftime_obj.minute), second=int(cftime_obj.second),
                            microsecond=int(cftime_obj.microsecond)
                        )
                        
                        # Convert the Python datetime to an ISO 8601 string, then to np.datetime64
                        # This path is often more robust.
                        iso_str = py_dt.isoformat()
                        selected_time_from_slice_dt = np.datetime64(iso_str)
                        
                        time_difference = abs(selected_time_from_slice_dt - analysis_target_dt_np)
                    else:
                        # full_ds.time is numerical (e.g., float64 'days since ...')
                        # Convert analysis_target_dt_np to this numerical domain for selection
                        try:
                            time_units = full_ds.time.attrs.get('units')
                            time_calendar = full_ds.time.attrs.get('calendar', 'standard')
                            if not time_units:
                                logger.error(f"Time units attribute missing from undecoded time coordinate for target {target_time}. Cannot select accurately.")
                                continue # Skip this target_time

                            # Check for duplicate time values in numerical case too
                            time_values = full_ds.time.values
                            unique_times, unique_indices = np.unique(time_values, return_index=True)
                            
                            if len(unique_times) < len(time_values):
                                logger.warning(f"Found {len(time_values) - len(unique_times)} duplicate numerical time values in GFS dataset. Removing duplicates.")
                                # Keep only the first occurrence of each unique time
                                unique_indices_sorted = np.sort(unique_indices)
                                full_ds = full_ds.isel(time=unique_indices_sorted)

                            numerical_target_for_sel = xr.coding.times.encode_cf_datetime(
                                np.array([analysis_target_dt_np]), 
                                units=time_units, 
                                calendar=time_calendar
                            )[0] 

                            analysis_slice = full_ds.sel(time=numerical_target_for_sel, method='nearest')
                            selected_numerical_time_from_slice = analysis_slice.time.values 
                            numerical_diff = abs(selected_numerical_time_from_slice - numerical_target_for_sel)

                            # Convert numerical_diff (in time_units) back to a np.timedelta64
                            # Decode [0, numerical_diff] in the original units to get two datetime objects, then subtract
                            time_axis_for_diff_conversion = xr.coding.times.decode_cf_datetime(
                                np.array([0, numerical_diff]), 
                                units=time_units, 
                                calendar=time_calendar, 
                                use_cftime=True 
                            )
                            time_difference = np.abs(np.datetime64(time_axis_for_diff_conversion[1]) - np.datetime64(time_axis_for_diff_conversion[0]))
                            
                        except Exception as e_numerical_sel:
                            logger.error(f"Error during numerical time selection for target {target_time}: {e_numerical_sel}", exc_info=True)
                            continue 
                    
                    actual_selected_time_for_log = "N/A"
                    if analysis_slice is not None and hasattr(analysis_slice, 'time') and hasattr(analysis_slice.time, 'values'):
                        actual_selected_time_for_log = analysis_slice.time.values

                    if time_difference > np.timedelta64(1, 'h'):
                        logger.error(f"Nearest time found ({actual_selected_time_for_log}) is too far ({time_difference}) from requested analysis time {target_time}. Skipping.")
                        continue

                    current_surface_vars = target_surface_vars if target_surface_vars is not None else GFS_SURFACE_VARS
                    current_atmos_vars = target_atmos_vars if target_atmos_vars is not None else GFS_ATMOS_VARS
                    
                    vars_to_load_final = [v for v in current_surface_vars + current_atmos_vars if v in full_ds]
                    
                    if not vars_to_load_final:
                         logger.warning(f"No targeted GFS variables found in dataset for cycle {target_time}. Surface tried: {current_surface_vars}, Atmos tried: {current_atmos_vars}. Skipping analysis for this time.")
                         continue
                    logger.info(f"For cycle {target_time}, attempting to load variables: {vars_to_load_final}")
                         
                    loaded_slice = analysis_slice[vars_to_load_final].load()
                    loaded_slice = loaded_slice.expand_dims(dim='time', axis=0)
                    target_time_naive = target_time.replace(tzinfo=None)
                    loaded_slice['time'] = [np.datetime64(target_time_naive, 'ns')]
                    
                    logger.info(f"Loaded analysis slice for original target {target_time}")
                    analysis_slices.append(loaded_slice)

                    processed_one_successfully_for_target = True

                except Exception as e:
                    logger.error(f"Failed to fetch/process analysis for original target {target_time}: {e}", exc_info=True)
                finally:
                    # MEMORY LEAK FIX: Always cleanup the full dataset to prevent accumulation
                    if 'dataset_to_cleanup' in locals() and dataset_to_cleanup is not None:
                        try:
                            dataset_to_cleanup.close()
                            del dataset_to_cleanup
                            logger.debug(f"Cleaned up GFS dataset for {target_time}")
                        except Exception as cleanup_err:
                            logger.debug(f"Error during dataset cleanup for {target_time}: {cleanup_err}")
                    
                    # Force garbage collection after each time step to prevent accumulation
                    if idx % 5 == 0:  # Every 5 iterations
                        import gc
                        collected = gc.collect()
                        logger.debug(f"GC collected {collected} objects after processing {idx+1} time steps")

        if not analysis_slices:
            logger.error("Failed to fetch any valid GFS analysis slices.")
            return None, progress_info

        try:
            logger.info(f"Combining {len(analysis_slices)} analysis slices...")
            combined_ds = xr.concat(analysis_slices, dim='time')
            
            # MEMORY LEAK FIX: Clean up individual slices after concat to free memory
            len_analysis_slices = len(analysis_slices)
            for slice_ds in analysis_slices:
                try:
                    slice_ds.close()
                except:
                    pass
            analysis_slices.clear()
            del analysis_slices
            
            # Force garbage collection after concat
            import gc
            collected = gc.collect()
            logger.debug(f"Post-concat cleanup: freed {len_analysis_slices} slices, GC collected {collected} objects")
            
        except Exception as e_concat:
             logger.error(f"Failed to combine analysis slices: {e_concat}")
             # Cleanup on error too
             for slice_ds in analysis_slices:
                 try:
                     slice_ds.close()
                 except:
                     pass
             return None, progress_info
             
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
             current_units = processed_ds['pressure_level'].attrs.get('units', '').lower()
             if current_units == 'pa' or not current_units:
                  log_msg = f"Converting pressure_level coordinate from {current_units if current_units else '[ASSUMED Pa]'} to hPa."
                  logger.info(log_msg)
                  processed_ds['pressure_level'] = processed_ds['pressure_level'] / 100.0
                  processed_ds['pressure_level'].attrs['units'] = 'hPa'
                  processed_ds['pressure_level'].attrs['long_name'] = 'pressure' 
             elif current_units in ['hpa', 'millibar', 'mb']:
                  logger.info(f"Pressure level units ('{current_units}') are already hPa equivalent. Ensuring units attribute is 'hPa'.")
                  processed_ds['pressure_level'].attrs['units'] = 'hPa' 
                  processed_ds['pressure_level'].attrs['long_name'] = 'pressure' 
             else:
                  logger.warning(f"Pressure level units '{current_units}' are not Pa, hPa, or millibar. Proceeding without conversion.")

             try:
                  pressure_levels_for_selection = target_pressure_levels_hpa if target_pressure_levels_hpa is not None else AURORA_PRESSURE_LEVELS_HPA
                  processed_ds = processed_ds.sel(pressure_level=pressure_levels_for_selection)
                  logger.info(f"Selected pressure levels: {pressure_levels_for_selection}")
             except Exception as e_sel_p:
                  logger.warning(f"Could not select pressure levels after unit processing: {e_sel_p}")

        default_units = {
            '2t': 'K', '10u': 'm s-1', '10v': 'm s-1', 'msl': 'Pa',
            't': 'K', 'u': 'm s-1', 'v': 'm s-1', 'q': 'kg kg-1', 'z': 'm2 s-2'
        }
        for var_name, expected_units in default_units.items():
            if var_name in processed_ds and 'units' not in processed_ds[var_name].attrs:
                processed_ds[var_name].attrs['units'] = expected_units

        if 'lat' in processed_ds.coords and len(processed_ds.lat) > 1 and processed_ds.lat.values[0] < processed_ds.lat.values[-1]:
            logger.info("Reversing latitude coordinate.")
            processed_ds = processed_ds.reindex(lat=processed_ds.lat.values[::-1].copy())

        if 'lon' in processed_ds.coords and processed_ds.lon.values.min() < 0:
            logger.info("Adjusting longitude coordinate to [0, 360).")
            processed_ds.coords['lon'] = (processed_ds.coords['lon'] + 360) % 360
            processed_ds = processed_ds.sortby(processed_ds.lon)
        elif 'lon' in processed_ds.coords and processed_ds.lon.values.max() >= 360.0:
             processed_ds = processed_ds.sel(lon=processed_ds.lon < 360.0)
             
        logger.debug("Analysis dataset processing finished.")

        try:
            logger.info(f"Saving processed GFS analysis data to cache: {cache_filename}")
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Try different netCDF engines in order of preference
            engines_to_try = ['netcdf4', 'scipy', 'h5netcdf']
            saved_successfully = False
            
            for engine in engines_to_try:
                try:
                    processed_ds.to_netcdf(cache_filename, engine=engine)
                    logger.info(f"Successfully saved GFS analysis cache using '{engine}' engine")
                    saved_successfully = True
                    break
                except Exception as engine_err:
                    logger.debug(f"Failed to save with '{engine}' engine: {engine_err}")
                    continue
            
            if not saved_successfully:
                # Final fallback: save as pickle (less efficient but works)
                import pickle
                pickle_filename = cache_filename.with_suffix('.pkl')
                logger.warning(f"All netCDF engines failed, falling back to pickle format: {pickle_filename}")
                with open(pickle_filename, 'wb') as f:
                    pickle.dump(processed_ds, f)
                logger.info(f"Successfully saved GFS analysis cache as pickle")
                
        except Exception as e_cache:
            logger.error(f"Failed to save GFS analysis to cache {cache_filename}: {e_cache}")
            if isinstance(e_cache, PermissionError):
                 logger.error(f"** PERMISSION DENIED saving to {cache_dir}. Please check directory permissions. **")
        
        # Update final progress info
        progress_info.update({
            "processed_files": len_analysis_slices,
            "completed": True,
            "step_progress": 0.8  # Processing complete, ready for caching
        })
        
        return processed_ds, progress_info

    try:
        result_dataset, final_progress_info = await asyncio.to_thread(_sync_fetch_and_process_analysis)
        
        # Emit progress updates based on what happened in the sync function
        if progress_callback and final_progress_info:
            if final_progress_info.get("completed"):
                # Emit intermediate progress for files processed
                if final_progress_info.get("processed_files", 0) > 0:
                    await progress_callback({
                        "operation": "gfs_download",
                        "stage": "processing",
                        "progress": final_progress_info.get("step_progress", 0.8),
                        "message": f"Processed {final_progress_info['processed_files']}/{final_progress_info['total_files']} GFS analysis time steps",
                        "files_completed": final_progress_info["processed_files"],
                        "files_total": final_progress_info["total_files"]
                    })
                
                # Final completion progress
                if result_dataset is not None:
                    file_size = cache_filename.stat().st_size if cache_filename.exists() else 0
                    await progress_callback({
                        "operation": "gfs_download",
                        "stage": "completed",
                        "progress": 1.0,
                        "message": f"GFS analysis data successfully processed and cached ({file_size} bytes)",
                        "bytes_downloaded": file_size,
                        "bytes_total": file_size
                    })
            else:
                # Processing failed partway through
                await progress_callback({
                    "operation": "gfs_download",
                    "stage": "error",
                    "progress": final_progress_info.get("step_progress", 0.0),
                    "message": f"Processing failed after {final_progress_info.get('files_completed', 0)}/{final_progress_info['total_files']} files"
                })
        
        return result_dataset
    except Exception as e:
        logger.error(f"Async fetch GFS analysis data failed: {e}")
        if progress_callback:
            await progress_callback({
                "operation": "gfs_download",
                "stage": "error",
                "progress": 0.0,
                "message": f"GFS fetch failed: {str(e)}"
            })
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
