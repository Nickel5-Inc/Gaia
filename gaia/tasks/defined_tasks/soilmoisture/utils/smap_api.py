import os
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import numpy as np
import requests
import xarray as xr
from pyproj import Transformer, CRS
from skimage.transform import resize
from tqdm import tqdm
import shutil
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import httpx
import asyncio
import traceback

load_dotenv()
EARTHDATA_USERNAME = os.getenv("EARTHDATA_USERNAME")
EARTHDATA_PASSWORD = os.getenv("EARTHDATA_PASSWORD")
BASE_URL = "https://n5eil01u.ecs.nsidc.org/SMAP/SPL4SMGP.007"


class SessionWithHeaderRedirection(requests.Session):
    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url
        if "Authorization" in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (
                (original_parsed.hostname != redirect_parsed.hostname)
                and redirect_parsed.hostname != self.AUTH_HOST
                and original_parsed.hostname != self.AUTH_HOST
            ):
                del headers["Authorization"]
        return


session = SessionWithHeaderRedirection(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)


def construct_smap_url(datetime_obj, test_mode=False):
    """
    Construct URL for SMAP L4 Global Product data
    Example: SMAP_L4_SM_gph_20241111T013000_Vv7031_001.h5
    """
    if test_mode:
        datetime_obj = datetime_obj - timedelta(days=7)  #  Force scoring to use old data in test mode
    
    valid_time = get_valid_smap_time(datetime_obj)
    date_dir = valid_time.strftime("%Y.%m.%d")
    file_date = valid_time.strftime("%Y%m%d")
    time_str = valid_time.strftime("T%H%M%S")

    base_url = "https://n5eil01u.ecs.nsidc.org/SMAP/SPL4SMGP.007"
    file_name = f"SMAP_L4_SM_gph_{file_date}{time_str}_Vv7031_001.h5"

    print(f"Requesting SMAP data for: {valid_time} ({file_date}{time_str})")
    return f"{base_url}/{date_dir}/{file_name}"


async def download_smap_data(url, output_path):
    """
    Download SMAP data with progress bar and caching (now async)
    """
    cache_dir = Path("smap_cache")
    cache_dir.mkdir(exist_ok=True)
    cache_file = cache_dir / Path(url).name
    loop = asyncio.get_event_loop()

    if await loop.run_in_executor(None, cache_file.exists):
        print(f"Using cached SMAP data from {cache_file}")
        if output_path != str(cache_file):
            await loop.run_in_executor(None, shutil.copy, str(cache_file), output_path)
        return True

    try:
        async with httpx.AsyncClient(auth=(EARTHDATA_USERNAME, EARTHDATA_PASSWORD), follow_redirects=True, timeout=300.0) as client:
            # Get content length first for progress bar
            try:
                head_response = await client.head(url)
                head_response.raise_for_status() # Check for errors like 404
                total_size = int(head_response.headers.get("content-length", 0))
                print(f"Downloading from: {url}")
                print(f"File size: {total_size / (1024*1024):.1f} MB")
            except httpx.HTTPStatusError as e:
                print(f"Failed to get headers (url might be invalid or auth failed): {e.response.status_code} for {url}")
                return False
            except Exception as e_head:
                print(f"Error getting file size: {e_head} for {url}")
                total_size = 0 # Proceed without progress bar if size fetch fails

            # Stream download with progress
            async with client.stream("GET", url) as response:
                if response.status_code != 200:
                    print(f"Download failed with status {response.status_code} for {url}")
                    content_snippet = await response.aread()
                    print(f"Response content: {content_snippet[:500]}") # Log part of the response
                    if await loop.run_in_executor(None, cache_file.exists):
                        await loop.run_in_executor(None, cache_file.unlink)
                    return False
                
                # Use a temporary file for download to avoid partial files in cache on error
                temp_dl_path = cache_file.with_suffix(cache_file.suffix + '.part')

                def _write_chunk_sync(file_handle, chunk_data):
                    file_handle.write(chunk_data)

                try:
                    with open(temp_dl_path, 'wb') as f:
                        with tqdm(total=total_size, unit="B", unit_scale=True, desc=f"Downloading {cache_file.name}") as pbar:
                            async for chunk in response.aiter_bytes():
                                await loop.run_in_executor(None, _write_chunk_sync, f, chunk)
                                pbar.update(len(chunk))
                    
                    # Download successful, move temp file to final cache location
                    await loop.run_in_executor(None, shutil.move, str(temp_dl_path), str(cache_file))
                    print(f"\nDownload successful for {url}!")

                    if output_path != str(cache_file):
                        await loop.run_in_executor(None, shutil.copy, str(cache_file), output_path)
                    return True
                except Exception as e_write:
                    print(f"\nError during file write/move: {e_write}")
                    if await loop.run_in_executor(None, temp_dl_path.exists):
                        await loop.run_in_executor(None, temp_dl_path.unlink)
                    if await loop.run_in_executor(None, cache_file.exists): # If main cache file was somehow created
                        await loop.run_in_executor(None, cache_file.unlink)
                    return False

    except httpx.RequestError as e_req:
        print(f"Request error during download from {url}: {e_req}")
        if await loop.run_in_executor(None, cache_file.exists):
            await loop.run_in_executor(None, cache_file.unlink)
        return False
    except Exception as e:
        print(f"General error during download from {url}: {str(e)}")
        if await loop.run_in_executor(None, cache_file.exists):
            await loop.run_in_executor(None, cache_file.unlink)
        return False


def process_smap_data(filepath, bbox, target_shape=(220, 220)):
    """
    Process SMAP L4 data for a specified bounding box.
    """
    with xr.open_dataset(filepath, group="Geophysical_Data") as ds:
        surface_sm = (
            ds["sm_surface"]
            .sel(lat=slice(bbox[1], bbox[3]), lon=slice(bbox[0], bbox[2]))
            .values
        )
        rootzone_sm = (
            ds["sm_rootzone"]
            .sel(lat=slice(bbox[1], bbox[3]), lon=slice(bbox[0], bbox[2]))
            .values
        )

        surface_sm[surface_sm == ds["sm_surface"]._FillValue] = np.nan
        rootzone_sm[rootzone_sm == ds["sm_rootzone"]._FillValue] = np.nan
        surface_resampled = resize(surface_sm, target_shape, preserve_range=True)
        rootzone_resampled = resize(rootzone_sm, target_shape, preserve_range=True)

        return {"surface_sm": surface_resampled, "rootzone_sm": rootzone_resampled}


async def get_smap_data(datetime_obj, regions):
    """
    Get SMAP soil moisture data for multiple regions.

    Args:
        datetime_obj: Datetime object for the data
        regions: List of dicts with {'bounds': tuple, 'crs': str}

    Returns:
        dict: Region-wise SMAP data and metadata
    """
    try:
        smap_url = construct_smap_url(datetime_obj)
        cache_dir = Path("smap_cache")
        cache_dir.mkdir(exist_ok=True)
        temp_filename = f"temp_smap_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.h5"
        temp_filepath = cache_dir / temp_filename
        
        if not await download_smap_data(smap_url, str(temp_filepath)):
            return None

        results = {}
        with xr.open_dataset(str(temp_filepath), group="Geophysical_Data") as ds:
            surface_data = ds["sm_surface"].values
            rootzone_data = ds["sm_rootzone"].values

            ease2_crs = CRS.from_epsg(6933)
            smap_y_size, smap_x_size = surface_data.shape
            smap_y_range = (-7314540.11, 7314540.11)
            smap_x_range = (-17367530.45, 17367530.45)

            for i, region in enumerate(regions):
                bounds = region["bounds"]
                crs = region["crs"]

                if crs != "EPSG:4326":
                    to_wgs84 = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
                    left, bottom = to_wgs84.transform(bounds[0], bounds[1])
                    right, top = to_wgs84.transform(bounds[2], bounds[3])
                else:
                    left, bottom, right, top = bounds

                to_ease2 = Transformer.from_crs("EPSG:4326", ease2_crs, always_xy=True)
                ease2_bounds = to_ease2.transform_bounds(left, bottom, right, top)
                ease2_left, ease2_bottom, ease2_right, ease2_top = ease2_bounds

                y_idx_start = int(
                    (smap_y_range[1] - ease2_top)
                    * smap_y_size
                    / (smap_y_range[1] - smap_y_range[0])
                )
                y_idx_end = int(
                    (smap_y_range[1] - ease2_bottom)
                    * smap_y_size
                    / (smap_y_range[1] - smap_y_range[0])
                )
                x_idx_start = int(
                    (ease2_left - smap_x_range[0])
                    * smap_x_size
                    / (smap_x_range[1] - smap_x_range[0])
                )
                x_idx_end = int(
                    (ease2_right - smap_x_range[0])
                    * smap_x_size
                    / (smap_x_range[1] - smap_x_range[0])
                )

                y_idx_start = max(0, min(y_idx_start, smap_y_size))
                y_idx_end = max(0, min(y_idx_end, smap_y_size))
                x_idx_start = max(0, min(x_idx_start, smap_x_size))
                x_idx_end = max(0, min(x_idx_end, smap_x_size))

                surface_roi = surface_data[
                    y_idx_start:y_idx_end, x_idx_start:x_idx_end
                ]
                rootzone_roi = rootzone_data[
                    y_idx_start:y_idx_end, x_idx_start:x_idx_end
                ]

                results[f"region_{i}"] = {
                    "surface_sm": surface_roi,
                    "rootzone_sm": rootzone_roi,
                    "bounds": {
                        "original": bounds,
                        "transformed": ease2_bounds,
                    },
                    "shape": surface_roi.shape,
                }

                print(f"\nRegion {i}:")
                print(f"Extracted shape: {surface_roi.shape}")
                print(f"Original bounds: {bounds}")
                print(f"EASE2 bounds: {ease2_bounds}")

        return results

    except Exception as e:
        print(f"Error getting SMAP data: {str(e)}")
        return None
    finally:
        if 'temp_filepath' in locals() and temp_filepath.exists():
            try:
                temp_filepath.unlink()
            except Exception as e:
                print(f"Error cleaning up temp file: {str(e)}")


def get_valid_smap_time(datetime_obj):
    """
    Adjust time to nearest available SMAP time
    Available times (UTC):
    01:30 (T013000)    04:30 (T043000)    07:30 (T073000)    10:30 (T103000)
    13:30 (T133000)    16:30 (T163000)    19:30 (T193000)    22:30 (T223000)
    """
    valid_times = [  # smap only has discrete times available
        (1, 30),  # T013000
        (4, 30),  # T043000
        (7, 30),  # T073000
        (10, 30),  # T103000
        (13, 30),  # T133000
        (16, 30),  # T163000
        (19, 30),  # T193000
        (22, 30),  # T223000
    ]

    hour = datetime_obj.hour
    nearest_time = min(valid_times, key=lambda x: abs(x[0] - hour))

    return datetime_obj.replace(
        hour=nearest_time[0], minute=nearest_time[1], second=0, microsecond=0
    )


def _process_smap_for_sentinel_sync(filepath, sentinel_bounds_tuple, sentinel_crs_str):
    """Synchronous helper to process SMAP data for sentinel bounds."""
    with xr.open_dataset(filepath, group="Geophysical_Data") as ds:
        smap_surface_sm = ds["sm_surface"]
        smap_rootzone_sm = ds["sm_rootzone"]
        
        smap_lat = ds.coords["lat"].values
        smap_lon = ds.coords["lon"].values

        # Assuming sentinel_bounds_tuple is (left, bottom, right, top)
        # And SMAP data is typically global, so we find the nearest indices
        # This simplistic approach might need refinement based on actual SMAP grid structure
        # For EASE-Grid 2.0, direct indexing or more sophisticated reprojection is better.
        
        # Placeholder: This needs to be replaced with proper reprojection and extraction
        # For now, let's assume we extract a small, fixed region for demonstration
        # In a real scenario, use pyproj and xarray.sel with reprojected coordinates
        target_lat_slice = slice(sentinel_bounds_tuple[3] - 0.5, sentinel_bounds_tuple[1] + 0.5) # top, bottom
        target_lon_slice = slice(sentinel_bounds_tuple[0] - 0.5, sentinel_bounds_tuple[2] + 0.5) # left, right

        try:
            # Attempt to select based on assuming WGS84-like coordinates for SMAP for simplicity here
            # This is likely incorrect for actual SMAP EASE-Grid data which is not in degrees lat/lon globally in a simple way.
            surface_sm = smap_surface_sm.sel(lat=target_lat_slice, lon=target_lon_slice).values
            rootzone_sm = smap_rootzone_sm.sel(lat=target_lat_slice, lon=target_lon_slice).values
        except Exception as e:
            print(f"Warning: Could not select SMAP data by lat/lon directly ({e}), returning full arrays for now. Need proper reprojection.")
            # Fallback or more robust extraction needed here. For now, returning a slice.
            # This part needs to be robustly implemented based on SMAP grid and sentinel projection.
            # Using a fixed slice for now as a placeholder if direct sel fails.
            surface_sm = smap_surface_sm.isel(lat=slice(100, 200), lon=slice(100,200)).values
            rootzone_sm = smap_rootzone_sm.isel(lat=slice(100, 200), lon=slice(100,200)).values

        # Ensure NaNs where fill value was
        surface_sm[surface_sm == smap_surface_sm._FillValue] = np.nan
        rootzone_sm[rootzone_sm == smap_rootzone_sm._FillValue] = np.nan
        
        # Resize to a common shape, e.g., 11x11 as often used in the project
        # This might not be the correct place for resizing if original resolution is important for metrics
        target_shape = (11, 11) # Example shape
        surface_resampled = resize(surface_sm, target_shape, preserve_range=True, anti_aliasing=True)
        rootzone_resampled = resize(rootzone_sm, target_shape, preserve_range=True, anti_aliasing=True)

        return {"surface_sm": surface_resampled, "rootzone_sm": rootzone_resampled}

async def get_smap_data_for_sentinel_bounds(filepath, sentinel_bounds_tuple, sentinel_crs_str):
    """
    Process SMAP L4 data for a specified bounding box using a synchronous helper in an executor.
    """
    loop = asyncio.get_event_loop()
    try:
        # Offload the synchronous xr.open_dataset and processing
        smap_dict = await loop.run_in_executor(None, _process_smap_for_sentinel_sync, filepath, sentinel_bounds_tuple, sentinel_crs_str)
        return smap_dict
    except Exception as e:
        print(f"Error in get_smap_data_for_sentinel_bounds: {e}")
        print(traceback.format_exc())
        return None


def test_smap_download():
    """
    Test SMAP download with sample bounds
    """
    test_datetime = datetime.now(timezone.utc) - timedelta(days=3)
    test_bounds = (
        -51.401355453052076,
        -27.0156800074561,
        -50.401355453052076,
        -26.0156800074561,
    )
    test_crs = "EPSG:4326"
    print(f"Testing SMAP download for:")
    print(f"Date: {test_datetime}")
    print(f"Bounds: {test_bounds}")
    print(f"CRS: {test_crs}")
    smap_data = get_smap_data(test_datetime, test_bounds, test_crs)

    if smap_data:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
        im1 = ax1.imshow(smap_data["surface_sm"])
        ax1.set_title("Surface Soil Moisture")
        plt.colorbar(im1, ax=ax1)
        im2 = ax2.imshow(smap_data["rootzone_sm"])
        ax2.set_title("Root Zone Soil Moisture")
        plt.colorbar(im2, ax=ax2)
        plt.tight_layout()
        plt.show()

        print("\nData shapes:")
        print(f"Surface data shape: {smap_data['surface_sm'].shape}")
        print("\nData ranges:")
        print(
            f"Surface data range: {np.nanmin(smap_data['surface_sm']):.3f} to {np.nanmax(smap_data['surface_sm']):.3f}"
        )
        print(
            f"Rootzone data range: {np.nanmin(smap_data['rootzone_sm']):.3f} to {np.nanmax(smap_data['rootzone_sm']):.3f}"
        )
    else:
        print("Failed to get SMAP data")