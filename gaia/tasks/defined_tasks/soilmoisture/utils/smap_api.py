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


def download_smap_data(url, output_path):
    """
    Download SMAP data with progress bar and caching
    """
    cache_dir = Path("smap_cache")
    cache_dir.mkdir(exist_ok=True)
    cache_file = cache_dir / Path(url).name

    if cache_file.exists():
        print(f"Using cached SMAP data from {cache_file}")
        if output_path != str(cache_file):
            shutil.copy(str(cache_file), output_path)
        return True

    try:
        response = requests.head(url, auth=(EARTHDATA_USERNAME, EARTHDATA_PASSWORD))
        total_size = int(response.headers.get("content-length", 0))
        print(f"Downloading from: {url}")
        print(f"File size: {total_size / (1024*1024):.1f} MB")
        with tqdm(
            total=total_size, unit="B", unit_scale=True, desc="Downloading"
        ) as pbar:
            process = subprocess.Popen(
                [
                    "wget",
                    "--progress=dot",
                    f"--http-user={EARTHDATA_USERNAME}",
                    f"--http-password={EARTHDATA_PASSWORD}",
                    "--no-check-certificate",
                    "--auth-no-challenge",
                    "-O",
                    str(cache_file),
                    url,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )

            for line in process.stderr:
                if "%" in line:
                    try:
                        percent = int(line.split("%")[0].split()[-1])
                        current_size = int(total_size * (percent / 100))
                        pbar.n = current_size
                        pbar.refresh()
                    except:
                        pass

            process.wait()

            if process.returncode == 0:
                print("\nDownload successful!")
                if output_path != str(cache_file):
                    shutil.copy(str(cache_file), output_path)
                return True
            else:
                print("\nDownload failed!")
                if cache_file.exists():
                    cache_file.unlink()
                return False

    except Exception as e:
        print(f"Error during download: {str(e)}")
        if cache_file.exists():
            cache_file.unlink()
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


def get_smap_data(datetime_obj, regions):
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
        temp_filename = f"temp_smap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.h5"
        temp_filepath = cache_dir / temp_filename
        
        if not download_smap_data(smap_url, str(temp_filepath)):
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


def get_smap_data_for_sentinel_bounds(filepath, sentinel_bounds, sentinel_crs):
    """
    Extract SMAP data matching Sentinel-2 bounds.

    Args:
        filepath: Path to SMAP HDF5 file
        sentinel_bounds: Bounds from Sentinel-2 data (left, bottom, right, top)
        sentinel_crs: CRS of Sentinel-2 data
    """
    with xr.open_dataset(filepath, group="Geophysical_Data") as ds:
        surface_data = ds["sm_surface"].values
        rootzone_data = ds["sm_rootzone"].values
        smap_y_size, smap_x_size = surface_data.shape
        
        ease2_crs = CRS.from_epsg(6933)
        smap_y_range = (-7314540.11, 7314540.11)
        smap_x_range = (-17367530.45, 17367530.45)

        if sentinel_crs != "EPSG:4326":
            transformer = Transformer.from_crs(
                sentinel_crs, "EPSG:4326", always_xy=True
            )
            left, bottom = transformer.transform(sentinel_bounds[0], sentinel_bounds[1])
            right, top = transformer.transform(sentinel_bounds[2], sentinel_bounds[3])
        else:
            left, bottom, right, top = sentinel_bounds

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

        surface_roi = surface_data[y_idx_start:y_idx_end, x_idx_start:x_idx_end]
        rootzone_roi = rootzone_data[y_idx_start:y_idx_end, x_idx_start:x_idx_end]

        return {
            "surface_sm": surface_roi,
            "rootzone_sm": rootzone_roi,
            "grid_indices": {
                "y_start": y_idx_start,
                "y_end": y_idx_end,
                "x_start": x_idx_start,
                "x_end": x_idx_end,
            },
            "bounds": {
                "original": sentinel_bounds,
                "transformed": ease2_bounds,
            },
        }


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
