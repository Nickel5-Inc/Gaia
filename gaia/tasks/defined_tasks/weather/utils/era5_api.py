import asyncio
import hashlib
import os
import tempfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import cdsapi
import numpy as np
import pandas as pd
import xarray as xr

# Import netCDF4 at module level to ensure availability in threaded contexts
try:
    import netCDF4

    NETCDF4_AVAILABLE = True
except ImportError:
    NETCDF4_AVAILABLE = False

from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)

AURORA_PRESSURE_LEVELS = [
    "50",
    "100",
    "150",
    "200",
    "250",
    "300",
    "400",
    "500",
    "600",
    "700",
    "850",
    "925",
    "1000",
]

ERA5_SINGLE_LEVEL_VARS = {
    "2m_temperature": "2t",
    "10m_u_component_of_wind": "10u",
    "10m_v_component_of_wind": "10v",
    "mean_sea_level_pressure": "msl",
}

ERA5_PRESSURE_LEVEL_VARS = {
    "temperature": "t",
    "u_component_of_wind": "u",
    "v_component_of_wind": "v",
    "specific_humidity": "q",
    "geopotential": "z",  # Geopotential height (z) = geopotential / g
}


async def fetch_era5_data(
    target_times: List[datetime], cache_dir: Path = Path("./era5_cache")
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
        cache_filename.with_suffix(".pkl"),  # pickle fallback format
    ]

    for potential_cache_file in cache_files_to_check:
        if potential_cache_file.exists():
            try:
                logger.success(f"✅ Loading cached ERA5 data from: {potential_cache_file}")

                if potential_cache_file.suffix == ".pkl":
                    # Load pickle format
                    import pickle

                    with open(potential_cache_file, "rb") as f:
                        ds_combined = pickle.load(f)
                    logger.info("Loaded cached data from pickle format")
                else:
                    # Load netCDF format with corrected engine handling
                    load_successful = False

                    # 1. Try default engine first (most reliable)
                    try:
                        logger.debug("Attempting to load cache with default engine...")
                        ds_combined = xr.open_dataset(potential_cache_file)
                        logger.info(
                            "Successfully loaded cached data with default engine"
                        )
                        load_successful = True
                    except Exception as default_load_err:
                        logger.debug(f"Default load failed: {default_load_err}")

                        # 2. Try explicit netcdf4 engine
                        try:
                            logger.debug(
                                "Attempting to load cache with netcdf4 engine..."
                            )
                            ds_combined = xr.open_dataset(potential_cache_file)
                            logger.info(
                                "Successfully loaded cached data with netcdf4 engine"
                            )
                            load_successful = True
                        except Exception as netcdf4_load_err:
                            logger.debug(f"netcdf4 load failed: {netcdf4_load_err}")

                            # 3. Try with decode_cf=False to handle problematic metadata
                            try:
                                logger.debug(
                                    "Attempting to load cache with decode_cf=False..."
                                )
                                ds_combined = xr.open_dataset(
                                    potential_cache_file, decode_cf=False
                                )
                                logger.info(
                                    "Successfully loaded cached data with decode_cf=False"
                                )
                                load_successful = True
                            except Exception as decode_load_err:
                                logger.debug(
                                    f"decode_cf=False load failed: {decode_load_err}"
                                )

                    if not load_successful:
                        logger.warning(
                            f"Failed to load cache file with all methods: {potential_cache_file}"
                        )
                        continue

                # Validate cache content
                if "time" in ds_combined.coords:
                    target_times_np_ns = [
                        np.datetime64(t.replace(tzinfo=None), "ns")
                        for t in target_times
                    ]
                    if all(
                        t_np_ns in ds_combined.time.values
                        for t_np_ns in target_times_np_ns
                    ):
                        logger.info(
                            f"✓ Cache hit for ERA5 data - loaded from {potential_cache_file.name}"
                        )

                        # CRITICAL FIX: Force data loading for cached datasets too
                        # This prevents lazy-loading issues when cache files might be moved/deleted
                        try:
                            logger.debug(
                                "Loading cached data into memory to prevent file access issues..."
                            )
                            for var_name in ds_combined.data_vars:
                                _ = ds_combined[var_name].values  # Force loading
                            logger.debug("Successfully loaded cached data into memory")
                        except Exception as load_err:
                            logger.warning(
                                f"Error during cached data loading: {load_err}"
                            )
                            # Continue - the data might still be accessible

                        return ds_combined
                    else:
                        logger.warning(
                            "Cached file exists but missing requested times. Re-fetching."
                        )
                        if hasattr(ds_combined, "close"):
                            ds_combined.close()
                        potential_cache_file.unlink()
                else:
                    logger.warning(
                        "Cached file exists but missing 'time' coordinate. Re-fetching."
                    )
                    if hasattr(ds_combined, "close"):
                        ds_combined.close()
                    potential_cache_file.unlink()

            except Exception as e:
                logger.warning(
                    f"Failed to load or validate cache file {potential_cache_file}: {e}. Re-fetching."
                )
                if potential_cache_file.exists():
                    try:
                        potential_cache_file.unlink()
                    except OSError:
                        pass

    logger.info(
        f"Cache miss or invalid cache for times: {time_strings[0]} to {time_strings[-1]}. Fetching from CDS API."
    )

    dates = sorted(list(set(t.strftime("%Y-%m-%d") for t in target_times)))
    times = sorted(list(set(t.strftime("%H:%M") for t in target_times)))

    common_request = {
        "product_type": "reanalysis",
        "format": "netcdf",
        "date": dates,
        "time": times,
        "grid": "0.25/0.25",  # 0.25 degree resolution
    }

    single_level_request = common_request.copy()
    single_level_request["variable"] = list(ERA5_SINGLE_LEVEL_VARS.keys())

    pressure_level_request = common_request.copy()
    pressure_level_request["variable"] = list(ERA5_PRESSURE_LEVEL_VARS.keys())
    pressure_level_request["pressure_level"] = AURORA_PRESSURE_LEVELS

    def _sync_fetch_and_process():
        # THREADING FIX: Re-initialize netcdf4 backend in thread context
        try:
            import netCDF4
            import xarray as xr

            # Force re-registration of netcdf4 backend in this thread
            try:
                # In modern xarray versions, backends are automatically registered
                # Just verify that netcdf4 is available
                available_engines = list(xr.backends.list_engines())
                if "netcdf4" in available_engines:
                    logger.debug("netcdf4 backend is available in thread context")
                else:
                    logger.warning("netcdf4 backend not found in available engines")
            except Exception as backend_check_err:
                logger.warning(
                    f"Could not check netcdf4 backend availability: {backend_check_err}"
                )

            logger.debug(
                f"Thread context - Available engines: {list(xr.backends.list_engines())}"
            )
        except ImportError as netcdf_err:
            logger.warning(f"netCDF4 not available in thread context: {netcdf_err}")

        temp_sl_file = None
        temp_pl_file = None
        try:
            c = cdsapi.Client(quiet=True)

            fd_sl, temp_sl_path = tempfile.mkstemp(suffix="_era5_sl.nc", dir=cache_dir)
            fd_pl, temp_pl_path = tempfile.mkstemp(suffix="_era5_pl.nc", dir=cache_dir)
            os.close(fd_sl)
            os.close(fd_pl)
            temp_sl_file = Path(temp_sl_path)
            temp_pl_file = Path(temp_pl_path)

            api_sl_vars = [
                "2m_temperature",
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
                "mean_sea_level_pressure",
            ]
            api_pl_vars = [
                "temperature",
                "u_component_of_wind",
                "v_component_of_wind",
                "specific_humidity",
                "geopotential",
            ]

            current_single_level_request = single_level_request.copy()
            current_single_level_request["variable"] = api_sl_vars

            current_pressure_level_request = pressure_level_request.copy()
            current_pressure_level_request["variable"] = api_pl_vars

            logger.info(
                f"Requesting ERA5 single level data with vars: {api_sl_vars}..."
            )
            c.retrieve(
                "reanalysis-era5-single-levels",
                current_single_level_request,
                str(temp_sl_file),
            )
            logger.info(f"Single level data downloaded to {temp_sl_file}")

            logger.info(
                f"Requesting ERA5 pressure level data with vars: {api_pl_vars}..."
            )
            c.retrieve(
                "reanalysis-era5-pressure-levels",
                current_pressure_level_request,
                str(temp_pl_file),
            )
            logger.info(f"Pressure level data downloaded to {temp_pl_file}")

            logger.info("Loading and processing downloaded files...")

            # ROBUST BACKEND APPROACH: Use multiple fallback methods for maximum compatibility
            logger.info(
                "Loading downloaded files using robust backend detection with fallbacks..."
            )

            # Load single level file with corrected engine handling
            ds_sl = None
            try:
                logger.debug("Loading single level file with default engine...")
                ds_sl = xr.open_dataset(temp_sl_file)
                logger.debug(
                    "✅ Successfully loaded single level file with default engine"
                )
            except Exception as default_err:
                logger.debug(f"Default load failed for single level: {default_err}")

                # Try explicit netcdf4 engine
                try:
                    logger.debug("Loading single level file with netcdf4 engine...")
                    ds_sl = xr.open_dataset(temp_sl_file)
                    logger.debug(
                        "✅ Successfully loaded single level file with netcdf4 engine"
                    )
                except Exception as netcdf4_err:
                    logger.debug(f"netcdf4 load failed for single level: {netcdf4_err}")

                    # Try with decode_cf=False
                    try:
                        logger.debug(
                            "Loading single level file with decode_cf=False..."
                        )
                        ds_sl = xr.open_dataset(temp_sl_file, decode_cf=False)
                        logger.debug(
                            "✅ Successfully loaded single level file with decode_cf=False"
                        )
                    except Exception as decode_err:
                        logger.debug(
                            f"decode_cf=False load failed for single level: {decode_err}"
                        )

            if ds_sl is None:
                raise ValueError("Failed to load single level file with any method")

            # Load pressure level file with corrected engine handling
            ds_pl = None
            try:
                logger.debug("Loading pressure level file with default engine...")
                ds_pl = xr.open_dataset(temp_pl_file)
                logger.debug(
                    "✅ Successfully loaded pressure level file with default engine"
                )
            except Exception as default_err:
                logger.debug(f"Default load failed for pressure level: {default_err}")

                # Try explicit netcdf4 engine
                try:
                    logger.debug("Loading pressure level file with netcdf4 engine...")
                    ds_pl = xr.open_dataset(temp_pl_file)
                    logger.debug(
                        "✅ Successfully loaded pressure level file with netcdf4 engine"
                    )
                except Exception as netcdf4_err:
                    logger.debug(
                        f"netcdf4 load failed for pressure level: {netcdf4_err}"
                    )

                    # Try with decode_cf=False
                    try:
                        logger.debug(
                            "Loading pressure level file with decode_cf=False..."
                        )
                        ds_pl = xr.open_dataset(temp_pl_file, decode_cf=False)
                        logger.debug(
                            "✅ Successfully loaded pressure level file with decode_cf=False"
                        )
                    except Exception as decode_err:
                        logger.debug(
                            f"decode_cf=False load failed for pressure level: {decode_err}"
                        )

            if ds_pl is None:
                raise ValueError("Failed to load pressure level file with any method")

            logger.info(
                f"Variables in downloaded single-level ERA5: {list(ds_sl.data_vars)}"
            )
            logger.info(
                f"Variables in downloaded pressure-level ERA5: {list(ds_pl.data_vars)}"
            )

            rename_map_sl = {"t2m": "2t", "u10": "10u", "v10": "10v", "msl": "msl"}
            rename_map_pl = {"t": "t", "u": "u", "v": "v", "q": "q", "z": "z"}

            ds_sl_renamed = ds_sl.rename(
                {k: v for k, v in rename_map_sl.items() if k in ds_sl}
            )
            ds_pl_renamed = ds_pl.rename(
                {k: v for k, v in rename_map_pl.items() if k in ds_pl}
            )

            if "2t" not in ds_sl_renamed.data_vars and (
                "t2m" in ds_sl.data_vars or "2m_temperature" in ds_sl.data_vars
            ):
                logger.warning(
                    f"Variable '2t' not found after renaming. Original SL vars: {list(ds_sl.data_vars)}. Renamed SL vars: {list(ds_sl_renamed.data_vars)}"
                )

            logger.info(
                f"Renamed ds_sl_renamed data_vars: {list(ds_sl_renamed.data_vars)}"
            )
            logger.info(
                f"Renamed ds_pl_renamed data_vars: {list(ds_pl_renamed.data_vars)}"
            )

            ds_combined = xr.merge([ds_sl_renamed, ds_pl_renamed])

            # STANDARDIZE DIMENSION ORDER: Transpose pressure level variables to match Aurora/miner format
            # ERA5 CDS format: (time, pressure_level, lat, lon)
            # Aurora/Miner format: (time, lat, lon, pressure_level)
            pressure_level_vars = [
                "t",
                "u",
                "v",
                "q",
                "z",
            ]  # Variables that have pressure levels

            for var_name in pressure_level_vars:
                if var_name in ds_combined.data_vars:
                    var_data = ds_combined[var_name]
                    if "pressure_level" in var_data.dims:
                        # Check current dimension order
                        current_dims = list(var_data.dims)
                        logger.debug(
                            f"ERA5 variable '{var_name}' current dimensions: {current_dims}"
                        )

                        # Expected order: (time, lat, lon, pressure_level)
                        expected_dims = ["time", "lat", "lon", "pressure_level"]

                        # Only transpose if dimensions don't match expected order
                        if current_dims != expected_dims and all(
                            dim in current_dims for dim in expected_dims
                        ):
                            logger.info(
                                f"ERA5: Standardizing '{var_name}' dimensions from {current_dims} to {expected_dims}"
                            )
                            ds_combined[var_name] = var_data.transpose(*expected_dims)
                        elif "latitude" in current_dims or "longitude" in current_dims:
                            # Handle coordinate renaming case - dimensions might use 'latitude'/'longitude'
                            expected_dims_alt = [
                                "time",
                                "latitude",
                                "longitude",
                                "pressure_level",
                            ]
                            if current_dims != expected_dims_alt and all(
                                dim in current_dims for dim in expected_dims_alt
                            ):
                                logger.info(
                                    f"ERA5: Standardizing '{var_name}' dimensions from {current_dims} to {expected_dims_alt}"
                                )
                                ds_combined[var_name] = var_data.transpose(
                                    *expected_dims_alt
                                )
                        else:
                            logger.debug(
                                f"ERA5: Variable '{var_name}' dimensions already in correct order: {current_dims}"
                            )

            rename_coords = {}
            if "latitude" in ds_combined.coords:
                rename_coords["latitude"] = "lat"
            if "longitude" in ds_combined.coords:
                rename_coords["longitude"] = "lon"
            if "valid_time" in ds_combined.coords:
                rename_coords["valid_time"] = "time"
            if rename_coords:
                logger.debug(f"About to rename coordinates: {rename_coords}")
                logger.debug(
                    f"Before rename - coords: {list(ds_combined.coords.keys())}"
                )
                ds_combined = ds_combined.rename(rename_coords)
                logger.info(
                    f"Renamed coordinates: {list(rename_coords.keys())} -> {list(rename_coords.values())}"
                )
                logger.debug(
                    f"After rename - coords: {list(ds_combined.coords.keys())}"
                )

                # Verify time coordinate still exists after rename
                if "time" not in ds_combined.coords:
                    logger.error(f"CRITICAL: Lost time coordinate during rename!")
                    logger.error(f"Rename mapping was: {rename_coords}")
                    logger.error(
                        f"Current coordinates: {list(ds_combined.coords.keys())}"
                    )
                    raise ValueError("Lost time coordinate during coordinate renaming")

            if "z" in ds_combined:
                logger.info("Ensuring Geopotential (z) units and attributes...")
                if (
                    "units" not in ds_combined["z"].attrs
                    or ds_combined["z"].attrs["units"].lower() != "m**2 s**-2"
                ):
                    ds_combined["z"].attrs["units"] = "m**2 s**-2"
                ds_combined["z"].attrs["standard_name"] = "geopotential"
                ds_combined["z"].attrs["long_name"] = "Geopotential"

            if (
                "lat" in ds_combined.coords
                and len(ds_combined.lat) > 1
                and ds_combined.lat.values[0] < ds_combined.lat.values[-1]
            ):
                logger.info(
                    "Reversing latitude coordinate to be decreasing (90 to -90)."
                )
                ds_combined = ds_combined.reindex(lat=ds_combined.lat[::-1])

            if "lon" in ds_combined.coords and ds_combined.lon.values.min() < 0:
                logger.info(
                    "Adjusting longitude coordinate from [-180, 180] to [0, 360)."
                )
                ds_combined.coords["lon"] = (ds_combined.coords["lon"] + 360) % 360
                ds_combined = ds_combined.sortby(ds_combined.lon)

            if "time" in ds_combined.coords:
                target_times_np_ns = [
                    np.datetime64(t.replace(tzinfo=None), "ns") for t in target_times
                ]
                ds_combined = ds_combined.sel(time=target_times_np_ns, method="nearest")
                logger.info("Selected target time steps.")
            else:
                logger.error(
                    "Processed dataset missing 'time' coordinate after potential rename. Cannot select times."
                )
                raise ValueError("Missing 'time' coordinate in processed dataset")

            logger.info("Data processing and coordinate adjustments complete.")

            # Force data loading before temp file cleanup
            logger.info(
                "Loading data into memory to prevent lazy-loading issues with temporary files..."
            )
            try:
                for var_name in ds_combined.data_vars:
                    _ = ds_combined[var_name].values  # Force loading
                logger.debug("Successfully loaded all variables into memory")
            except Exception as load_err:
                logger.warning(f"Error during forced data loading: {load_err}")

            logger.info(f"Saving processed data to cache: {cache_filename}")

            # Simple thread-safe netcdf save (default engine works in all contexts)
            save_successful = False

            try:
                # Strategy 1: Force engine auto-detection
                logger.debug("Saving with auto-detected engine...")
                ds_combined.to_netcdf(cache_filename, engine=None)
                logger.info(
                    f"✅ Successfully saved with auto-detected engine: {cache_filename}"
                )
                save_successful = True
            except Exception as auto_err:
                logger.debug(f"Auto-detected engine failed: {auto_err}")

                try:
                    # Strategy 2: Compute first and save
                    logger.debug("Computing dataset and saving...")
                    ds_computed = ds_combined.compute()
                    ds_computed.to_netcdf(cache_filename)
                    logger.info(
                        f"✅ Successfully saved computed dataset: {cache_filename}"
                    )
                    save_successful = True
                except Exception as computed_save_err:
                    logger.debug(f"Computed dataset save failed: {computed_save_err}")

            if not save_successful:
                logger.error(
                    "Failed to save with all netCDF engines, using pickle fallback..."
                )
                try:
                    pickle_filename = cache_filename.with_suffix(".pkl")
                    import pickle

                    with open(pickle_filename, "wb") as f:
                        pickle.dump(ds_combined, f)
                    logger.warning(
                        f"Saved data in pickle format as fallback: {pickle_filename}"
                    )
                except Exception as pickle_err:
                    logger.error(f"Pickle fallback also failed: {pickle_err}")

            return ds_combined

        except Exception as e:
            logger.error(f"Error during ERA5 sync fetch/process: {e}")
            logger.info(traceback.format_exc())
            return None
        finally:
            if temp_sl_file and temp_sl_file.exists():
                try:
                    temp_sl_file.unlink()
                except OSError:
                    pass
            if temp_pl_file and temp_pl_file.exists():
                try:
                    temp_pl_file.unlink()
                except OSError:
                    pass

    try:
        result_dataset = await asyncio.to_thread(_sync_fetch_and_process)
        return result_dataset
    except Exception as e:
        logger.error(f"Failed to run ERA5 fetch in thread: {e}")
        return None


async def _test_fetch():
    print("Testing ERA5 fetch...")
    test_times = [1, datetime(2025, 4, 9, 6, 0), datetime(2025, 4, 9, 12, 0)]
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

import asyncio
import hashlib
import logging
import os
import tempfile
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import cdsapi
import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)

# ERA5 variable definitions and pressure levels
ERA5_SINGLE_LEVEL_VARS = {
    "2m_temperature": "2t",
    "10m_u_component_of_wind": "10u",
    "10m_v_component_of_wind": "10v",
    "mean_sea_level_pressure": "msl",
}

ERA5_PRESSURE_LEVEL_VARS = {
    "temperature": "t",
    "u_component_of_wind": "u",
    "v_component_of_wind": "v",
    "specific_humidity": "q",
    "geopotential": "z",
}

AURORA_PRESSURE_LEVELS = [
    50,
    100,
    150,
    200,
    250,
    300,
    400,
    500,
    600,
    700,
    850,
    925,
    1000,
]


async def fetch_era5_data_progressive(
    target_times: List[datetime], cache_dir: Path = Path("./era5_cache")
) -> Optional[xr.Dataset]:
    """
    Optimized ERA5 fetch for progressive scoring.
    Fetches and caches individual days, then combines only the requested times.
    This minimizes data transfer and allows for efficient progressive scoring.

    Args:
        target_times: List of datetime objects for which to fetch data
        cache_dir: Directory for caching individual daily files

    Returns:
        Combined xarray.Dataset for requested times, or None if fetch fails
    """
    if not target_times:
        logger.warning("fetch_era5_data_progressive called with no target_times.")
        return None

    cache_dir.mkdir(parents=True, exist_ok=True)
    target_times = sorted(list(set(target_times)))

    # Group target times by date for efficient daily fetching
    daily_groups: Dict[str, List[datetime]] = {}
    for dt in target_times:
        date_key = dt.strftime("%Y-%m-%d")
        if date_key not in daily_groups:
            daily_groups[date_key] = []
        daily_groups[date_key].append(dt)

    logger.info(f"Progressive ERA5 fetch: Processing {len(daily_groups)} unique dates")

    # Fetch individual daily datasets (cached per day) with partial failure tolerance
    daily_datasets = []
    failed_dates = []
    successful_times = []
    cache_hits = 0
    downloads = 0

    for date_key, times_for_date in daily_groups.items():
        logger.debug(
            f"Processing date {date_key} with {len(times_for_date)} time points"
        )

        daily_ds, was_cache_hit = await _fetch_single_day_era5(
            date_key, times_for_date, cache_dir
        )
        if daily_ds is not None:
            daily_datasets.append(daily_ds)
            successful_times.extend(times_for_date)
            if was_cache_hit:
                cache_hits += 1
            else:
                downloads += 1
            logger.debug(f"✅ Successfully fetched ERA5 data for date {date_key}")
        else:
            failed_dates.append(date_key)
            logger.warning(
                f"❌ Failed to fetch ERA5 data for date {date_key} - data may not be available yet"
            )

    # Log cache summary
    if cache_hits > 0 or downloads > 0:
        logger.info(
            f"📊 ERA5 fetch summary: {cache_hits} from cache, {downloads} downloaded from API, {len(failed_dates)} failed"
        )

    if failed_dates:
        logger.warning(
            f"ERA5 fetch partial failure: {len(failed_dates)} dates failed ({failed_dates}), {len(daily_datasets)} succeeded"
        )

    # If we have some successful datasets, continue with partial data
    if not daily_datasets:
        logger.error("No ERA5 data could be fetched for any requested dates")
        return None

    # Combine daily datasets efficiently

    # Combine daily datasets efficiently
    try:
        logger.info(f"Combining {len(daily_datasets)} daily datasets")
        combined_ds = xr.concat(daily_datasets, dim="time")
        combined_ds = combined_ds.sortby("time")

        # Remove duplicate time indices (can occur at day boundaries)
        _, unique_indices = np.unique(combined_ds.time.values, return_index=True)
        if len(unique_indices) < len(combined_ds.time):
            logger.info(
                f"Removing {len(combined_ds.time) - len(unique_indices)} duplicate time indices"
            )
            combined_ds = combined_ds.isel(time=sorted(unique_indices))

        # Select only the exact target times to minimize memory usage
        target_times_np_ns = [
            np.datetime64(t.replace(tzinfo=None), "ns") for t in target_times
        ]
        combined_ds = combined_ds.sel(time=target_times_np_ns, method="nearest")

        # STANDARDIZE DIMENSION ORDER: Ensure consistent dimensions after combining datasets
        # ERA5 CDS format: (time, pressure_level, lat, lon)
        # Aurora/Miner format: (time, lat, lon, pressure_level)
        pressure_level_vars = [
            "t",
            "u",
            "v",
            "q",
            "z",
        ]  # Variables that have pressure levels

        for var_name in pressure_level_vars:
            if var_name in combined_ds.data_vars:
                var_data = combined_ds[var_name]
                if "pressure_level" in var_data.dims:
                    # Check current dimension order
                    current_dims = list(var_data.dims)
                    logger.debug(
                        f"Progressive ERA5 variable '{var_name}' current dimensions: {current_dims}"
                    )

                    # Expected order: (time, lat, lon, pressure_level)
                    expected_dims = ["time", "lat", "lon", "pressure_level"]

                    # Only transpose if dimensions don't match expected order
                    if current_dims != expected_dims and all(
                        dim in current_dims for dim in expected_dims
                    ):
                        logger.info(
                            f"Progressive ERA5: Standardizing '{var_name}' dimensions from {current_dims} to {expected_dims}"
                        )
                        combined_ds[var_name] = var_data.transpose(*expected_dims)
                    else:
                        logger.debug(
                            f"Progressive ERA5: Variable '{var_name}' dimensions already in correct order: {current_dims}"
                        )

        logger.info(
            f"Progressive ERA5 fetch completed: {len(target_times)} time points from {len(daily_groups)} days"
        )
        return combined_ds

    except Exception as e:
        logger.error(f"Error combining daily ERA5 datasets: {e}")
        return None
    finally:
        # Clean up individual daily datasets to free memory
        for ds in daily_datasets:
            if hasattr(ds, "close"):
                ds.close()


async def _fetch_single_day_era5(
    date_str: str, times_for_date: List[datetime], cache_dir: Path
) -> Tuple[Optional[xr.Dataset], bool]:
    """
    Fetch ERA5 data for a single day with per-day caching.
    This enables efficient progressive scoring with minimal redundant downloads.

    Returns:
        Tuple of (dataset, was_cache_hit) where was_cache_hit is True if loaded from cache
    """
    # Create cache filename based on date and times
    times_str = "_".join([t.strftime("%H%M") for t in times_for_date])
    cache_key = f"era5_{date_str}_{hashlib.md5(times_str.encode()).hexdigest()[:8]}"
    cache_file = cache_dir / f"{cache_key}.nc"
    
    logger.debug(f"ERA5 cache check for {date_str}: times={[t.strftime('%H%M') for t in times_for_date]}, key={cache_key}, file={cache_file.name}")

    # Check for existing cache
    if cache_file.exists():
        try:
            logger.debug(f"Loading cached ERA5 data for {date_str}: {cache_file}")
            ds = xr.open_dataset(cache_file)

            # Validate cache contains time coordinate and requested times
            if "time" in ds.coords:
                target_times_np_ns = [
                    np.datetime64(t.replace(tzinfo=None), "ns") for t in times_for_date
                ]
                if all(t_np_ns in ds.time.values for t_np_ns in target_times_np_ns):
                    # Force data loading to prevent lazy-loading issues
                    for var_name in ds.data_vars:
                        _ = ds[var_name].values
                    logger.info(
                        f"✓ Cache hit for ERA5 data on {date_str} - loaded from {cache_file.name}"
                    )
                    return ds, True  # Return dataset and cache hit flag
                else:
                    logger.warning(
                        f"Cache miss: {date_str} cache doesn't contain all requested times"
                    )
                    ds.close()
                    cache_file.unlink()
            else:
                logger.warning(
                    f"Cache invalid: {date_str} cache missing 'time' coordinate"
                )
                ds.close()
                cache_file.unlink()
        except Exception as e:
            logger.warning(f"Error loading cache for {date_str}: {e}")
            if cache_file.exists():
                cache_file.unlink()

    # Fetch data from CDS API for this specific day
    logger.info(f"⬇ Downloading ERA5 data for {date_str} from CDS API (not in cache)")

    times = sorted(list(set(t.strftime("%H:%M") for t in times_for_date)))

    def _sync_fetch_single_day():
        try:
            c = cdsapi.Client(quiet=True)

            # Create temporary files for single and pressure level data
            fd_sl, temp_sl_path = tempfile.mkstemp(
                suffix=f"_era5_sl_{date_str}.nc", dir=cache_dir
            )
            fd_pl, temp_pl_path = tempfile.mkstemp(
                suffix=f"_era5_pl_{date_str}.nc", dir=cache_dir
            )
            os.close(fd_sl)
            os.close(fd_pl)
            temp_sl_file = Path(temp_sl_path)
            temp_pl_file = Path(temp_pl_path)

            common_request = {
                "product_type": "reanalysis",
                "format": "netcdf",
                "date": [date_str],  # Single date
                "time": times,
                "grid": "0.25/0.25",
            }

            # Fetch single level data
            sl_request = common_request.copy()
            sl_request["variable"] = [
                "2m_temperature",
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
                "mean_sea_level_pressure",
            ]

            logger.debug(f"Requesting single level data for {date_str}")
            c.retrieve("reanalysis-era5-single-levels", sl_request, str(temp_sl_file))

            # Fetch pressure level data
            pl_request = common_request.copy()
            pl_request["variable"] = [
                "temperature",
                "u_component_of_wind",
                "v_component_of_wind",
                "specific_humidity",
                "geopotential",
            ]
            pl_request["pressure_level"] = AURORA_PRESSURE_LEVELS

            logger.debug(f"Requesting pressure level data for {date_str}")
            c.retrieve("reanalysis-era5-pressure-levels", pl_request, str(temp_pl_file))

            # Process and combine the downloaded data
            logger.debug(f"Processing downloaded data for {date_str}")

            # Load with robust error handling and coordinate validation
            ds_sl = xr.open_dataset(temp_sl_file)
            ds_pl = xr.open_dataset(temp_pl_file)

            # Debug coordinate information
            logger.debug(
                f"Single level dataset coordinates: {list(ds_sl.coords.keys())}"
            )
            logger.debug(
                f"Pressure level dataset coordinates: {list(ds_pl.coords.keys())}"
            )

            # Check for time coordinate in source files
            if "time" not in ds_sl.coords and "valid_time" not in ds_sl.coords:
                logger.error(
                    f"Single level dataset missing time coordinate! Available coords: {list(ds_sl.coords.keys())}"
                )
            if "time" not in ds_pl.coords and "valid_time" not in ds_pl.coords:
                logger.error(
                    f"Pressure level dataset missing time coordinate! Available coords: {list(ds_pl.coords.keys())}"
                )

            # Rename variables to match expected names
            var_mapping = {**ERA5_SINGLE_LEVEL_VARS, **ERA5_PRESSURE_LEVEL_VARS}

            # Apply renaming in-place by reassigning back to the variables
            for old_name, new_name in var_mapping.items():
                if old_name in ds_sl.data_vars:
                    ds_sl = ds_sl.rename({old_name: new_name})
                if old_name in ds_pl.data_vars:
                    ds_pl = ds_pl.rename({old_name: new_name})

            # Combine datasets with coordinate validation
            logger.debug(f"Before merge - ds_sl coords: {list(ds_sl.coords.keys())}")
            logger.debug(f"Before merge - ds_pl coords: {list(ds_pl.coords.keys())}")

            ds_combined = xr.merge([ds_sl, ds_pl])

            logger.debug(
                f"After merge - combined coords: {list(ds_combined.coords.keys())}"
            )
            logger.debug(
                f"After merge - combined dimensions: {dict(ds_combined.sizes)}"
            )

            # STANDARDIZE DIMENSION ORDER: Transpose pressure level variables to match Aurora/miner format
            # ERA5 CDS format: (time, pressure_level, lat, lon)
            # Aurora/Miner format: (time, lat, lon, pressure_level)
            pressure_level_vars = [
                "t",
                "u",
                "v",
                "q",
                "z",
            ]  # Variables that have pressure levels

            for var_name in pressure_level_vars:
                if var_name in ds_combined.data_vars:
                    var_data = ds_combined[var_name]
                    if "pressure_level" in var_data.dims:
                        # Check current dimension order
                        current_dims = list(var_data.dims)
                        logger.debug(
                            f"ERA5 variable '{var_name}' current dimensions: {current_dims}"
                        )

                        # Expected order: (time, lat, lon, pressure_level)
                        expected_dims = ["time", "lat", "lon", "pressure_level"]

                        # Only transpose if dimensions don't match expected order
                        if current_dims != expected_dims and all(
                            dim in current_dims for dim in expected_dims
                        ):
                            logger.info(
                                f"ERA5: Standardizing '{var_name}' dimensions from {current_dims} to {expected_dims}"
                            )
                            ds_combined[var_name] = var_data.transpose(*expected_dims)
                        else:
                            logger.debug(
                                f"ERA5: Variable '{var_name}' dimensions already in correct order: {current_dims}"
                            )

            # Verify time coordinate exists after merge
            if (
                "time" not in ds_combined.coords
                and "valid_time" not in ds_combined.coords
            ):
                logger.error(
                    f"CRITICAL: Combined dataset missing time coordinate after merge!"
                )
                logger.error(
                    f"Available coordinates: {list(ds_combined.coords.keys())}"
                )
                logger.error(f"Available dimensions: {dict(ds_combined.sizes)}")
                raise ValueError("Missing time coordinate in merged dataset")

            # Coordinate adjustments with validation
            logger.debug(
                f"Before coordinate adjustments - coords: {list(ds_combined.coords.keys())}"
            )

            # Handle coordinate renaming first
            rename_coords = {}
            if "latitude" in ds_combined.coords:
                rename_coords["latitude"] = "lat"
            if "longitude" in ds_combined.coords:
                rename_coords["longitude"] = "lon"
            if "valid_time" in ds_combined.coords:
                rename_coords["valid_time"] = "time"

            if rename_coords:
                logger.debug(f"About to rename coordinates: {rename_coords}")
                ds_combined = ds_combined.rename(rename_coords)
                logger.debug(
                    f"After coordinate rename - coords: {list(ds_combined.coords.keys())}"
                )

                # Verify time coordinate still exists after rename
                if "time" not in ds_combined.coords:
                    logger.error(
                        f"CRITICAL: Lost time coordinate during rename in _fetch_single_day_era5!"
                    )
                    logger.error(f"Rename mapping was: {rename_coords}")
                    logger.error(
                        f"Current coordinates: {list(ds_combined.coords.keys())}"
                    )
                    raise ValueError("Lost time coordinate during coordinate renaming")

            # Standard coordinate adjustments
            if (
                "lat" in ds_combined.coords
                and len(ds_combined.lat) > 1
                and ds_combined.lat.values[0] < ds_combined.lat.values[-1]
            ):
                ds_combined = ds_combined.reindex(lat=ds_combined.lat[::-1])

            if "lon" in ds_combined.coords and ds_combined.lon.values.min() < 0:
                ds_combined.coords["lon"] = (ds_combined.coords["lon"] + 360) % 360
                ds_combined = ds_combined.sortby(ds_combined.lon)

            logger.debug(
                f"After all coordinate adjustments - coords: {list(ds_combined.coords.keys())}"
            )

            # Force data loading before temp file cleanup
            logger.debug(
                f"Before force loading - coords: {list(ds_combined.coords.keys())}"
            )
            for var_name in ds_combined.data_vars:
                _ = ds_combined[var_name].values
            logger.debug(
                f"After force loading - coords: {list(ds_combined.coords.keys())}"
            )

            # Final validation before saving
            if "time" not in ds_combined.coords:
                logger.error(f"CRITICAL: Missing time coordinate before saving!")
                logger.error(
                    f"Available coordinates: {list(ds_combined.coords.keys())}"
                )
                logger.error(f"Available dimensions: {dict(ds_combined.sizes)}")
                raise ValueError("Missing time coordinate before cache save")

            # Save to cache with thread-safe netcdf handling
            logger.debug(f"Saving processed data for {date_str} to cache: {cache_file}")
            save_successful = False

            # Thread-safe save with multiple fallback strategies
            try:
                # Strategy 1: Try with explicit engine=None (force auto-detection)
                logger.debug("Attempting save with auto-detected engine...")
                ds_combined.to_netcdf(cache_file, engine=None)
                save_successful = True
                logger.debug(
                    f"✅ Successfully saved with auto-detected engine: {cache_file}"
                )
            except Exception as auto_err:
                logger.debug(f"Auto-detected engine failed: {auto_err}")

                try:
                    # Strategy 2: Force computation and try default save
                    logger.debug("Computing dataset and saving...")
                    ds_computed = ds_combined.compute()
                    ds_computed.to_netcdf(cache_file)
                    save_successful = True
                    logger.debug(
                        f"✅ Successfully saved computed dataset: {cache_file}"
                    )
                except Exception as computed_err:
                    logger.debug(f"Computed dataset save failed: {computed_err}")

                    try:
                        # Strategy 3: Use pickle as ultimate fallback for threading issues
                        logger.debug("Using pickle fallback for thread safety...")
                        pickle_file = cache_file.with_suffix(".pkl")
                        import pickle

                        with open(pickle_file, "wb") as f:
                            pickle.dump(
                                (
                                    ds_computed
                                    if "ds_computed" in locals()
                                    else ds_combined.compute()
                                ),
                                f,
                            )
                        save_successful = True
                        logger.debug(
                            f"✅ Successfully saved with pickle fallback: {pickle_file}"
                        )

                        # Also try to create a simple marker file indicating pickle format
                        marker_file = cache_file.with_suffix(".pkl_marker")
                        marker_file.write_text(str(pickle_file))

                    except Exception as pickle_err:
                        logger.error(f"All save strategies failed: {pickle_err}")
                        raise RuntimeError(
                            f"Failed to save ERA5 data in thread context: {pickle_err}"
                        )

            if not save_successful:
                raise RuntimeError(
                    "Failed to save ERA5 data to cache with any available engine"
                )

            # Cleanup temp files
            temp_sl_file.unlink()
            temp_pl_file.unlink()

            return ds_combined

        except Exception as e:
            logger.error(f"Error in sync fetch for {date_str}: {e}")
            # Cleanup on error
            for temp_file in [temp_sl_file, temp_pl_file]:
                if temp_file.exists():
                    temp_file.unlink()
            return None

    # Run the sync fetch in an executor to avoid blocking
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, _sync_fetch_single_day)
        return result, False  # Return dataset and false for cache hit (was downloaded)
    except Exception as e:
        logger.error(f"Error fetching ERA5 data for {date_str}: {e}")
        return None, False
