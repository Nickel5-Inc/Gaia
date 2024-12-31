import asyncio
import math
import os
import subprocess
import tempfile
import traceback
import zipfile
from datetime import datetime, timedelta, timezone
import aiohttp
import numpy as np
import rasterio
import xarray as xr
from aiohttp import ClientSession, BasicAuth
from aiohttp.client_exceptions import ClientResponseError
from dotenv import load_dotenv
from pyproj import Transformer
from rasterio.merge import merge
from rasterio.transform import from_bounds
from rasterio.warp import transform_bounds, reproject, Resampling
from skimage.transform import resize
import requests
import boto3
from botocore.config import Config
from botocore.client import UNSIGNED

load_dotenv()
EARTHDATA_USERNAME = os.getenv("EARTHDATA_USERNAME")
EARTHDATA_PASSWORD = os.getenv("EARTHDATA_PASSWORD")

EARTHDATA_AUTH = BasicAuth(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)

class SessionWithHeaderRedirection(requests.Session):
    AUTH_HOST = 'urs.earthdata.nasa.gov'

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url
        if 'Authorization' in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.AUTH_HOST and \
                    original_parsed.hostname != self.AUTH_HOST:
                del headers['Authorization']
        return

session = SessionWithHeaderRedirection(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)

async def fetch_hls_b4_b8(bbox, datetime_obj, download_dir=None):
    """
    Fetch monthly Sentinel-2 B4 and B8 bands asynchronously, falling back to previous month if needed.
    """
    if download_dir is None:
        download_dir = get_data_dir()

    async def try_month(search_date):
        base_url = "https://cmr.earthdata.nasa.gov/search/granules.json"
        bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
        month_start = search_date.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        if month_start.month == 12:
            month_end = month_start.replace(
                year=month_start.year + 1, month=1
            ) - timedelta(seconds=1)
        else:
            month_end = month_start.replace(month=month_start.month + 1) - timedelta(
                seconds=1
            )

        headers = {"Authorization": f'Bearer {os.getenv("EARTHDATA_API_KEY")}'}
        params = {
            "collection_concept_id": "C2021957295-LPCLOUD",
            "temporal": f"{month_start.strftime('%Y-%m-%d')}T00:00:00Z/{month_end.strftime('%Y-%m-%d')}T23:59:59Z",
            "bounding_box": bbox_str,
            "page_size": 100,
            "cloud_cover": "0,50",
        }

        print(f"Searching for Sentinel data for {month_start.strftime('%B %Y')}")
        async with aiohttp.ClientSession() as session:
            async with session.get(
                base_url, params=params, headers=headers
            ) as response:
                if response.status == 200:
                    response_json = await response.json()
                    if "feed" in response_json and "entry" in response_json["feed"]:
                        entries = response_json["feed"]["entry"]
                        if entries:
                            print(f"Found {len(entries)} potential scenes")
                            return await process_entries(entries, search_date)
                else:
                    print(f"Failed to fetch data: {response.status} {response.reason}")
                return None

    async def process_entries(entries, target_date):
        def score_entry(entry):
            cloud_cover = float(entry.get("cloud_cover", 100))
            start_time = entry.get("time_start", "")
            if start_time:
                try:
                    entry_dt = datetime.strptime(
                        start_time.split(".")[0], "%Y-%m-%dT%H:%M:%S"
                    )
                    entry_dt = entry_dt.replace(tzinfo=timezone.utc)
                    time_diff = abs((entry_dt - target_date).total_seconds())
                    return (cloud_cover, time_diff)
                except ValueError:
                    pass
            return (100, float("inf"))

        entries.sort(key=score_entry)

        for entry in entries:
            b4_url = None
            b8_url = None

            for link in entry.get("links", []):
                url = link.get("href", "")
                if url.startswith("https://") and ".tif" in url:
                    if "B04.tif" in url:
                        b4_url = url
                    elif "B08.tif" in url:
                        b8_url = url

            if b4_url and b8_url:
                try:
                    entry_date = datetime.strptime(
                        entry["time_start"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                    ).replace(tzinfo=timezone.utc)

                    cloud_cover = float(entry.get("cloud_cover", "N/A"))
                    print(
                        f"Found data for {entry_date.date()} with {cloud_cover}% cloud cover"
                    )

                    b4_path = os.path.join(
                        download_dir, f"hls_b4_{entry_date.strftime('%Y%m%d')}.tif"
                    )
                    b8_path = os.path.join(
                        download_dir, f"hls_b8_{entry_date.strftime('%Y%m%d')}.tif"
                    )

                    await download_file(b4_url, b4_path)
                    print(f"Downloaded B4 to: {b4_path}")

                    await download_file(b8_url, b8_path)
                    print(f"Downloaded B8 to: {b8_path}")

                    return [b4_path, b8_path]
                except Exception as e:
                    print(f"Error processing entry: {str(e)}")
                    continue
        return None

    async def download_file(url, destination):
        async with aiohttp.ClientSession(auth=EARTHDATA_AUTH) as session:
            async with session.get(url) as response:
                if response.status == 200:
                    with open(destination, "wb") as f:
                        async for chunk in response.content.iter_chunked(1024 * 1024):
                            f.write(chunk)
                else:
                    raise ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status,
                        message=f"Failed to download file from {url}",
                    )

    result = await try_month(datetime_obj)
    if result:
        return result

    try:
        print("No data found for current month, trying previous month...")
        if datetime_obj.month == 1:
            prev_month_date = datetime_obj.replace(year=datetime_obj.year - 1, month=12, day=1)
        else:
            first_of_month = datetime_obj.replace(day=1)
            prev_month_date = first_of_month - timedelta(days=1)
            try:
                prev_month_date = prev_month_date.replace(day=min(datetime_obj.day, prev_month_date.day))
            except ValueError:
                pass
        
        return await try_month(prev_month_date)
    except Exception as e:
        print(f"Error fetching previous month's data: {str(e)}")
        return None


async def download_srtm_tile(lat, lon, download_dir=None):
    if download_dir is None:
        download_dir = get_data_dir()

    try:
        lat_prefix = "N" if lat >= 0 else "S"
        lon_prefix = "E" if lon >= 0 else "W"
        
        # Primary source (Earthdata)
        tile_name = f"{lat_prefix}{abs(lat):02d}{lon_prefix}{abs(lon):03d}.SRTMGL1.hgt.zip"
        earthdata_url = f"https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/2000.02.11/{tile_name}"
        print(f"\n=== SRTM Download Debug for {tile_name} ===")
        
        timeout = aiohttp.ClientTimeout(total=300)
        async with aiohttp.ClientSession(timeout=timeout, auth=EARTHDATA_AUTH) as session:
            try:
                print("Attempting primary Earthdata source...")
                async with session.get(earthdata_url) as response:
                    print(f"Earthdata status code: {response.status}")
                    
                    if response.status == 200:
                        zip_path = os.path.join(download_dir, tile_name)
                        final_tif_path = os.path.join(download_dir, f"{lat_prefix}{abs(lat):02d}{lon_prefix}{abs(lon):03d}.tif")
                        
                        with open(zip_path, "wb") as f:
                            async for chunk in response.content.iter_chunked(1024 * 1024):
                                f.write(chunk)
                        
                        print(f"Downloaded zip file to: {zip_path}")
                        
                        with zipfile.ZipFile(zip_path, "r") as zip_ref:
                            hgt_filename = zip_ref.namelist()[0]
                            zip_ref.extract(hgt_filename, download_dir)
                        
                        hgt_path = os.path.join(download_dir, hgt_filename)
                        
                        import subprocess
                        subprocess.run([
                            'gdal_translate',
                            '-of', 'GTiff',
                            '-co', 'COMPRESS=LZW',
                            '-a_srs', 'EPSG:4326',
                            hgt_path,
                            final_tif_path
                        ], check=True)
                        
                        os.remove(zip_path)
                        os.remove(hgt_path)
                        
                        return final_tif_path
                    else:
                        raise Exception(f"Earthdata source failed with status {response.status}")
                        
            except Exception as e:
                print(f"Earthdata source error: {str(e)}")
                print("Attempting Copernicus DEM fallback source...")
                
                s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
                
                lat_abs = abs(lat)
                lon_abs = abs(lon)
                lat_dir = "N" if lat >= 0 else "S"
                lon_dir = "E" if lon >= 0 else "W"
                
                for resolution_code in ["COG_10", "COG_30"]:
                    bucket = "copernicus-dem-30m"
                    key = (f"Copernicus_DSM_{resolution_code}_{lat_dir}{lat_abs:02d}_00_"
                          f"{lon_dir}{lon_abs:03d}_00_DEM/Copernicus_DSM_{resolution_code}_"
                          f"{lat_dir}{lat_abs:02d}_00_{lon_dir}{lon_abs:03d}_00_DEM.tif")
                    
                    print(f"Trying Copernicus GLO-30 mirror with {resolution_code} at s3://{bucket}/{key}")
                    try:
                        final_tif_path = os.path.join(download_dir, f"{lat_prefix}{abs(lat):02d}{lon_prefix}{abs(lon):03d}.tif")
                        await asyncio.to_thread(s3.download_file, bucket, key, final_tif_path)
                        return final_tif_path
                    except Exception as e:
                        print(f"Copernicus GLO-30 {resolution_code} failed: {str(e)}")
                        continue
                
                bucket = "copernicus-dem-90m"
                key = (f"Copernicus_DSM_COG_90_{lat_dir}{lat_abs:02d}_00_"
                      f"{lon_dir}{lon_abs:03d}_00_DEM/Copernicus_DSM_COG_90_"
                      f"{lat_dir}{lat_abs:02d}_00_{lon_dir}{lon_abs:03d}_00_DEM.tif")
                
                print(f"Trying Copernicus GLO-90 mirror at s3://{bucket}/{key}")
                try:
                    final_tif_path = os.path.join(download_dir, f"{lat_prefix}{abs(lat):02d}{lon_prefix}{abs(lon):03d}.tif")
                    await asyncio.to_thread(s3.download_file, bucket, key, final_tif_path)
                    return final_tif_path
                except Exception as e:
                    print(f"Copernicus GLO-90 failed: {str(e)}")
                    return None

    except Exception as e:
        print(f"Error downloading SRTM tile: {str(e)}")
        return None


async def fetch_srtm(bbox, sentinel_bounds=None, sentinel_crs=None, sentinel_shape=None):
    """Fetch and merge SRTM tiles using Sentinel-2 as reference asynchronously."""
    try:
        print("\n=== Fetching SRTM Data ===")
        temp_dir = tempfile.mkdtemp(dir=get_data_dir())

        if not (sentinel_bounds and sentinel_crs):
            raise ValueError("Sentinel-2 bounds and CRS required")

        wgs84_bounds = transform_bounds(sentinel_crs, "EPSG:4326", *sentinel_bounds)
        print(f"WGS84 bounds for tile calculation: {wgs84_bounds}")
        min_lon = math.floor(wgs84_bounds[0])
        max_lon = math.ceil(wgs84_bounds[2])
        min_lat = math.floor(wgs84_bounds[1])
        max_lat = math.ceil(wgs84_bounds[3])
        print(
            f"Need to fetch SRTM tiles from {min_lat},{min_lon} to {max_lat},{max_lon}"
        )

        tile_tasks = []
        for lat in range(min_lat, max_lat):
            for lon in range(min_lon, max_lon):
                tile_tasks.append(download_srtm_tile(lat, lon, download_dir=temp_dir))

        tile_paths = await asyncio.gather(*tile_tasks)
        tile_paths = [p for p in tile_paths if p]

        if not tile_paths:
            raise ValueError("No SRTM tiles downloaded")

        datasets = [rasterio.open(p) for p in tile_paths]
        mosaic, out_trans = merge(datasets)
        for ds in datasets:
            ds.close()

        output = np.zeros(sentinel_shape, dtype="float32")
        output_transform = from_bounds(
            *sentinel_bounds, sentinel_shape[1], sentinel_shape[0]
        )

        await asyncio.to_thread(
            reproject,
            source=mosaic[0],
            destination=output,
            src_transform=out_trans,
            src_crs="EPSG:4326",
            dst_transform=output_transform,
            dst_crs=sentinel_crs,
            resampling=Resampling.bilinear,
        )

        return output, None, output_transform, sentinel_crs

    except Exception as e:
        print(f"\n=== Error in SRTM processing ===")
        print(f"Error: {str(e)}")
        traceback.print_exc()
        return None, None, None, None


async def fetch_ifs_forecast(
    bbox, datetime_obj, sentinel_bounds=None, sentinel_crs=None, sentinel_shape=None
):
    """Fetch IFS forecast data asynchronously, requesting new data if no cache exists."""
    try:
        current_utc = datetime.now(timezone.utc)
        use_previous_day = current_utc.hour < 7 or (
            current_utc.hour == 7 and current_utc.minute < 0
        )
        target_date = (
            current_utc - timedelta(days=1) if use_previous_day else current_utc
        )
        data_dir = get_data_dir()
        today_cache = os.path.join(
            data_dir, f"ecmwf_forecast_{target_date.strftime('%Y%m%d')}.nc"
        )
        yesterday_cache = os.path.join(
            data_dir,
            f"ecmwf_forecast_{(target_date - timedelta(days=1)).strftime('%Y%m%d')}.nc",
        )

        # Try loading target day's cached data first
        if os.path.exists(today_cache):
            print(f"Loading cached IFS data from: {today_cache}")
            ds = xr.open_dataset(today_cache)
            data = await extract_ifs_variables(
                ds,
                bbox,
                sentinel_bounds=sentinel_bounds,
                sentinel_crs=sentinel_crs,
                sentinel_shape=sentinel_shape,
            )
            if data is not None:
                return data

        # If we're before 2 UTC, try yesterday's data
        if use_previous_day and os.path.exists(yesterday_cache):
            print("Before 2:00 UTC, using previous day's cached data...")
            ds = xr.open_dataset(yesterday_cache)
            data = await extract_ifs_variables(
                ds,
                bbox,
                sentinel_bounds=sentinel_bounds,
                sentinel_crs=sentinel_crs,
                sentinel_shape=sentinel_shape,
            )
            if data is not None:
                return data

        # Download data based on timing
        if use_previous_day:
            print("Before 2:00 UTC, downloading previous day's forecast...")
            target_date = current_utc - timedelta(days=1)
            cache_file = yesterday_cache
        else:
            print("After 2:00 UTC, downloading today's forecast...")
            cache_file = today_cache

        date_str = target_date.strftime("%Y%m%d")
        base_url = "https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com"
        time = "00z"

        timesteps = range(0, 25, 6)  # 0, 6, 12, 18, 24
        urls = [
            f"{base_url}/{date_str}/{time}/ifs/0p25/oper/{date_str}000000-{step}h-oper-fc.grib2"
            for step in timesteps
        ]

        if not os.path.exists(os.path.dirname(cache_file)):
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)

        download_tasks = [download_ifs_file(url) for url in urls]
        grib_paths = await asyncio.gather(*download_tasks)
        grib_paths = [p for p in grib_paths if p]

        if grib_paths:
            success = await process_global_ifs(grib_paths, timesteps, cache_file)
            if success:
                ds = xr.open_dataset(cache_file)
                return await extract_ifs_variables(
                    ds,
                    bbox,
                    sentinel_bounds=sentinel_bounds,
                    sentinel_crs=sentinel_crs,
                    sentinel_shape=sentinel_shape,
                )
        print("Failed to download new forecast data")
        return None

    except Exception as e:
        print(f"Error fetching IFS data: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return None


async def download_ifs_file(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                temp_file = tempfile.NamedTemporaryFile(suffix=".grib2", delete=False)
                with open(temp_file.name, "wb") as f:
                    async for chunk in response.content.iter_chunked(1024 * 1024):
                        f.write(chunk)
                return temp_file.name
            else:
                print(f"Failed to download IFS file from {url}")
                return None


async def process_global_ifs(grib_paths, timesteps, output_path):
    """Process multiple GRIB files into a single NetCDF with timesteps asynchronously."""
    try:
        ecmwf_params = {
            "t2m": 167,
            "tp": 228,
            "ssrd": 169,
            "sot": 260360,
            "sp": 134,
            "d2m": 168,
            "u10": 165,
            "v10": 166,
            "ro": 205,
            "msl": 151,
        }

        datasets = []
        for grib_path, step in zip(grib_paths, timesteps):
            timestep_datasets = []
            for var_name, param_id in ecmwf_params.items():
                try:
                    ds = await asyncio.to_thread(
                        xr.open_dataset,
                        grib_path,
                        engine="cfgrib",
                        filter_by_keys={"paramId": param_id},
                    )

                    ds = ds.drop_vars(
                        ["heightAboveGround", "depthBelowLandLayer"], errors="ignore"
                    )
                    ds = ds.expand_dims("time")
                    ds["time"] = [step]
                    timestep_datasets.append(ds)
                except Exception as e:
                    print(f"Error loading {var_name}: {str(e)}")

            if timestep_datasets:
                combined_timestep = xr.merge(timestep_datasets)
                datasets.append(combined_timestep)

        if datasets:
            combined_ds = xr.concat(datasets, dim="time")
            await asyncio.to_thread(combined_ds.to_netcdf, output_path)
            return True
        return False

    except Exception as e:
        print(f"Error processing IFS data: {str(e)}")
        return False


async def extract_ifs_variables(
    ds, bbox, sentinel_bounds=None, sentinel_crs=None, sentinel_shape=None
):
    """Extract and process IFS variables from dataset asynchronously."""
    try:
        print("Starting IFS variable extraction...")

        if not sentinel_shape:
            raise ValueError("Sentinel-2 shape is required for IFS resampling")

        if sentinel_bounds and sentinel_crs:
            wgs84_bounds = transform_bounds(sentinel_crs, "EPSG:4326", *sentinel_bounds)
            ds_cropped = ds.sel(
                latitude=slice(
                    max(wgs84_bounds[1], wgs84_bounds[3]),
                    min(wgs84_bounds[1], wgs84_bounds[3]),
                ),
                longitude=slice(
                    min(wgs84_bounds[0], wgs84_bounds[2]),
                    max(wgs84_bounds[0], wgs84_bounds[2]),
                ),
            )
        else:
            ds_cropped = ds.sel(
                latitude=slice(max(bbox[1], bbox[3]), min(bbox[1], bbox[3])),
                longitude=slice(min(bbox[0], bbox[2]), max(bbox[0], bbox[2])),
            )

        if ds_cropped.sizes["latitude"] == 0 or ds_cropped.sizes["longitude"] == 0:
            print(f"No data found in bbox: {bbox}")
            return None

        ds_time = ds_cropped.isel(time=0)
        print(f"Raw IFS data shape: {ds_time['t2m'].shape}")
        et0, svp, avp, r_n = calculate_penman_monteith(ds_time)

        if et0 is None:
            print("Failed to calculate Penman-Monteith ET0")
            return None

        bare_soil_evap = partition_evaporation(et0, ds_time)
        if bare_soil_evap is None:
            print("Failed to partition evaporation")
            return None

        print(f"Calculated ET0 shape: {et0.shape}")
        print(f"Calculated bare soil evaporation shape: {bare_soil_evap.shape}")

        soil_temps = {
            "st": ds_time["sot"].isel(soilLayer=0).values,
            "stl2": ds_time["sot"].isel(soilLayer=1).values,
            "stl3": ds_time["sot"].isel(soilLayer=2).values,
        }

        variables_to_process = [
            ("t2m", ds_time["t2m"].values),
            ("tp", ds_time["tp"].values),
            ("ssrd", ds_time["ssrd"].values),
            ("st", soil_temps["st"]),
            ("stl2", soil_temps["stl2"]),
            ("stl3", soil_temps["stl3"]),
            ("sp", ds_time["sp"].values),
            ("d2m", ds_time["d2m"].values),
            ("u10", ds_time["u10"].values),
            ("v10", ds_time["v10"].values),
            ("ro", ds_time["ro"].values),
            ("msl", ds_time["msl"].values),
            ("et0", et0),
            ("bare_soil_evap", bare_soil_evap),
            ("svp", svp),
            ("avp", avp),
            ("r_n", r_n),
        ]

        upsampled_vars = []
        for var_name, data in variables_to_process:
            upsampled = await asyncio.to_thread(
                resize,
                data,
                sentinel_shape,
                order=0,
                preserve_range=True,
                anti_aliasing=False,
            )
            print(f"Upsampled {var_name} from {data.shape} to {upsampled.shape}")
            upsampled_vars.append(upsampled)

        print(f"Successfully processed {len(upsampled_vars)} variables")
        return upsampled_vars

    except Exception as e:
        print(f"Error extracting IFS variables: {str(e)}")
        import traceback

        print(f"Full traceback: {traceback.format_exc()}")
        return None


async def get_soil_data(bbox, datetime_obj=None):
    """Download soil-related datasets and combine them into a single GeoTIFF asynchronously."""
    if datetime_obj is None:
        datetime_obj = datetime.now(timezone.utc)

    try:
        print(f"\n=== Starting Data Collection ===")
        sentinel_data = []
        ifs_resized = []
        srtm_resized = None

        print("\n=== Processing Sentinel Data ===")
        sentinel_paths = await fetch_hls_b4_b8(bbox, datetime_obj)
        if not sentinel_paths:
            print("Failed to fetch Sentinel data")
            return None

        sentinel_transform = None

        async def process_sentinel_band(path):
            with rasterio.open(path) as band_src:
                data = np.zeros((222, 222), dtype="float32")
                reproject(
                    source=rasterio.band(band_src, 1),
                    destination=data,
                    src_transform=band_src.transform,
                    src_crs=band_src.crs,
                    dst_transform=from_bounds(*band_src.bounds, 222, 222),
                    dst_crs=band_src.crs,
                    resampling=Resampling.bilinear,
                )
                return data, band_src.bounds, band_src.crs, band_src.transform

        sentinel_band_tasks = [process_sentinel_band(path) for path in sentinel_paths]
        sentinel_results = await asyncio.gather(*sentinel_band_tasks)

        for data, bounds, crs, transform in sentinel_results:
            sentinel_data.append(data)
            sentinel_bounds = bounds  # Assuming all bands have the same bounds
            sentinel_crs = crs  # Assuming all bands have the same CRS
            sentinel_transform = transform  # Assuming all bands have the same transform

        srtm_array, srtm_file, srtm_transform, srtm_crs = await fetch_srtm(
            bbox,
            sentinel_bounds=sentinel_bounds,
            sentinel_crs=sentinel_crs,
            sentinel_shape=(222, 222),
        )

        ifs_data = await fetch_ifs_forecast(
            bbox,
            datetime_obj,
            sentinel_bounds=sentinel_bounds,
            sentinel_crs=sentinel_crs,
            sentinel_shape=(222, 222),
        )

        profile = {
            "driver": "GTiff",
            "height": 222,
            "width": 222,
            "count": len(sentinel_data) + len(ifs_data) + 2,
            "dtype": "float32",
            "crs": srtm_crs,
            "transform": srtm_transform,
            "sentinel_transform": sentinel_transform,
            "compress": "lzw",
            "tiled": True,
            "blockxsize": 256,
            "blockysize": 256,
        }

        print("\n=== Combining All Data ===")
        output_file = await combine_tiffs(
            sentinel_data,
            ifs_data,
            (srtm_array, srtm_file),
            bbox,
            datetime_obj.strftime("%Y-%m-%d_%H%M"),
            profile,
        )

        with rasterio.open(output_file) as dest:
            print(f"Final combined transform: {dest.transform}")
            print(f"Final combined CRS: {dest.crs}")

        return output_file, sentinel_bounds, sentinel_crs

    except Exception as e:
        print(f"\n=== Error Occurred ===")
        print(f"Error in get_soil_data: {str(e)}")
        import traceback

        print(f"Full traceback: {traceback.format_exc()}")
        return None, None, None


async def combine_tiffs(
    sentinel_data, ifs_data, srtm_data_tuple, bbox, date_str, profile
):
    try:
        srtm_array, srtm_file = srtm_data_tuple
        target_shape = (profile["height"], profile["width"])
        srtm_resized = await asyncio.to_thread(
            resize,
            srtm_array,
            target_shape,
            preserve_range=True,
            order=1,
            mode="constant",
            cval=-9999,
        )

        print("\n=== Data Shape Verification ===")
        print(f"Target shape: {target_shape}")
        print(f"SRTM shape after resize: {srtm_resized.shape}")
        print(f"Sentinel data shapes: {[band.shape for band in sentinel_data]}")
        print(f"IFS data shapes: {[band.shape for band in ifs_data]}")
        output_dir = get_data_dir()
        output_file = os.path.join(
            output_dir,
            f"combined_{bbox[0]}_{bbox[1]}_{bbox[2]}_{bbox[3]}_{date_str}.tif",
        )

        with rasterio.open(output_file, "w", **profile) as dst:
            band_idx = 1
            for band in sentinel_data:
                dst.write(band.astype("float32"), band_idx)
                band_idx += 1

            for band in ifs_data:
                dst.write(band.astype("float32"), band_idx)
                band_idx += 1

            dst.write(srtm_resized.astype("float32"), band_idx)
            band_idx += 1
            ndvi = (sentinel_data[1] - sentinel_data[0]) / (
                sentinel_data[1] + sentinel_data[0]
            )
            dst.write(ndvi.astype("float32"), band_idx)
            dst.update_tags(
                sentinel_transform=str(profile["sentinel_transform"]),
                band_order="1-2:Sentinel(B4,B8), 3-19:IFS+Evap, 20:SRTM, 21:NDVI",
            )
        print(f"Successfully wrote combined data to {output_file}")
        return output_file

    except Exception as e:
        print(f"Error combining data: {str(e)}")
        traceback.print_exc()
        return None


def calculate_penman_monteith(ds):
    """
    FAO 56 Penman-Monteith equation.
    """
    try:
        print("\n=== Penman-Monteith Calculation ===")
        # Temp (K to C)
        t2m_k = ds["t2m"].values
        d2m_k = ds["d2m"].values
        print(f"Raw temperatures (K):")
        print(f"t2m: {t2m_k.mean():.2f}K ({t2m_k.min():.2f} to {t2m_k.max():.2f})")
        print(f"d2m: {d2m_k.mean():.2f}K ({d2m_k.min():.2f} to {d2m_k.max():.2f})")

        t2m = t2m_k - 273.15
        d2m = d2m_k - 273.15
        print(f"\nConverted temperatures (°C):")
        print(f"t2m: {t2m.mean():.2f}°C ({t2m.min():.2f} to {t2m.max():.2f})")
        print(f"d2m: {d2m.mean():.2f}°C ({d2m.min():.2f} to {d2m.max():.2f})")

        # Radiation (J/m² to MJ/m²/day)
        ssrd_raw = ds["ssrd"].values
        print(
            f"\nRaw radiation: {ssrd_raw.mean():.2f} J/m² ({ssrd_raw.min():.2f} to {ssrd_raw.max():.2f})"
        )
        ssrd = ssrd_raw / 1000000
        print(
            f"Converted radiation: {ssrd.mean():.2f} MJ/m²/day ({ssrd.min():.2f} to {ssrd.max():.2f})"
        )

        # Pressure (Pa to kPa)
        sp_raw = ds["sp"].values
        print(
            f"\nRaw pressure: {sp_raw.mean():.2f} Pa ({sp_raw.min():.2f} to {sp_raw.max():.2f})"
        )
        sp = sp_raw / 1000
        print(
            f"Converted pressure: {sp.mean():.2f} kPa ({sp.min():.2f} to {sp.max():.2f})"
        )

        # Wind speed (m/s)
        u10 = ds["u10"].values
        v10 = ds["v10"].values
        print(f"\nWind components (m/s):")
        print(f"u10: {u10.mean():.2f} ({u10.min():.2f} to {u10.max():.2f})")
        print(f"v10: {v10.mean():.2f} ({v10.min():.2f} to {v10.max():.2f})")
        wind_speed = np.sqrt(u10**2 + v10**2)
        print(
            f"Calculated wind speed: {wind_speed.mean():.2f} m/s ({wind_speed.min():.2f} to {wind_speed.max():.2f})"
        )

        # Vapor pressure (kPa)
        svp = 0.6108 * np.exp((17.27 * t2m) / (t2m + 237.3))
        avp = 0.6108 * np.exp((17.27 * d2m) / (d2m + 237.3))
        print(f"\nVapor pressures (kPa):")
        print(f"Saturation VP: {svp.mean():.2f} ({svp.min():.2f} to {svp.max():.2f})")
        print(f"Actual VP: {avp.mean():.2f} ({avp.min():.2f} to {avp.max():.2f})")

        # Psychrometric and slope
        psy = 0.000665 * sp
        delta = (4098 * svp) / ((t2m + 237.3) ** 2)
        print(f"\nPsychrometric constants:")
        print(f"Psychrometric constant: {psy.mean():.4f} kPa/°C")
        print(f"Slope of SVP: {delta.mean():.4f} kPa/°C")

        # ET0
        num = (0.408 * delta * ssrd) + (
            psy * (900 / (t2m + 273)) * wind_speed * (svp - avp)
        )
        den = delta + psy * (1 + 0.34 * wind_speed)
        et0 = num / den
        print(f"\nFinal calculations:")
        print(f"Numerator: {num.mean():.4f}")
        print(f"Denominator: {den.mean():.4f}")
        print(f"ET0: {et0.mean():.2f} mm/day ({et0.min():.2f} to {et0.max():.2f})")

        return et0, svp, avp, ssrd * (1 - 0.23)

    except Exception as e:
        print(f"Error in Penman-Monteith calculation: {str(e)}")
        return None


def partition_evaporation(total_evap, ds):
    """
    Partition total evaporation using available IFS parameters.
    """
    try:

        def kelvin_to_celsius(kelvin_temp):
            return kelvin_temp - 273.15

        def w_to_mj(w_per_m2_day):
            return w_per_m2_day * 0.0864

        t2m = kelvin_to_celsius(ds["t2m"].values)
        soil_temp = kelvin_to_celsius(ds["sot"].isel(soilLayer=0).values)
        temp_gradient = np.clip((soil_temp - t2m) / 10, -1, 1)
        temp_factor = 0.4 + 0.2 * temp_gradient

        precip = ds["tp"].values * 1000
        wetness_factor = np.clip(1 - np.exp(-0.5 * precip), 0, 1)

        rad = w_to_mj(ds["ssrd"].values)
        rad_norm = np.clip(rad / 30, 0, 1)

        bare_soil_fraction = (
            0.5 * temp_factor + 0.3 * (1 - wetness_factor) + 0.2 * rad_norm
        )

        return total_evap * np.clip(bare_soil_fraction, 0.2, 0.5)

    except Exception as e:
        print(f"Error in evaporation partitioning: {str(e)}")
        print(f"Using fallback value of 30% bare soil evaporation")
        return total_evap * 0.3  # Fallback value


def get_target_shape(bbox, target_resolution=500):
    """Calculate target shape using proper geospatial transformations."""
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    minx, miny = transformer.transform(bbox[0], bbox[1])
    maxx, maxy = transformer.transform(bbox[2], bbox[3])
    width_meters = maxx - minx
    height_meters = maxy - miny
    width_pixels = int(round(width_meters / target_resolution))
    height_pixels = int(round(height_meters / target_resolution))

    return (height_pixels, width_pixels)


def get_data_dir():
    """Get the path to the project's data directory, creating it if it doesn't exist."""
    # Get the current working directory (where the validator is running)
    cwd = os.getcwd()
    data_dir = os.path.join(cwd, "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        logger.info(f"Created data directory at: {data_dir}")
    return data_dir
