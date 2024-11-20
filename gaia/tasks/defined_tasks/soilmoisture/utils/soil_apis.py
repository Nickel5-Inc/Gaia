import math
import os
import subprocess
import tempfile
import traceback
import zipfile
from datetime import datetime, timedelta, timezone
import numpy as np
import requests
import rasterio
import xarray as xr
from pyproj import CRS, Transformer
from requests.auth import HTTPBasicAuth
from rasterio.env import Env
from rasterio.merge import merge
from rasterio.transform import from_bounds
from rasterio.warp import transform_bounds, reproject, Resampling
from skimage.transform import resize

EARTHDATA_USERNAME = "username"
EARTHDATA_PASSWORD = "password"

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

def fetch_hls_b4_b8(bbox, datetime_obj, download_dir='/tmp'):
    """
    Fetch monthly Sentinel-2 B4 and B8 bands, falling back to previous month if needed.
    """
    def try_month(search_date):
        base_url = "https://cmr.earthdata.nasa.gov/search/granules.json"
        bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
        month_start = search_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if month_start.month == 12:
            month_end = month_start.replace(year=month_start.year + 1, month=1) - timedelta(seconds=1)
        else:
            month_end = month_start.replace(month=month_start.month + 1) - timedelta(seconds=1)
        
        headers = {
            'Authorization': 'apikey'
        }
        params = {
            "collection_concept_id": "C2021957295-LPCLOUD",
            "temporal": f"{month_start.strftime('%Y-%m-%d')}T00:00:00Z/{month_end.strftime('%Y-%m-%d')}T23:59:59Z",
            "bounding_box": bbox_str,
            "page_size": 100,
            "cloud_cover": "0,50"
        }
        
        download_session = SessionWithHeaderRedirection(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)
        print(f"Searching for Sentinel data for {month_start.strftime('%B %Y')}")
        response = requests.get(base_url, params=params, headers=headers)
        
        if response.status_code == 200:
            response_json = response.json()
            if 'feed' in response_json and 'entry' in response_json['feed']:
                entries = response_json['feed']['entry']
                if entries:
                    print(f"Found {len(entries)} potential scenes")
                    return process_entries(entries, download_session, search_date)
        return None

    def process_entries(entries, download_session, target_date):
        def score_entry(entry):
            cloud_cover = float(entry.get('cloud_cover', 100))
            start_time = entry.get('time_start', '')
            if start_time:
                try:
                    entry_dt = datetime.strptime(start_time.split('.')[0], "%Y-%m-%dT%H:%M:%S")
                    entry_dt = entry_dt.replace(tzinfo=timezone.utc)
                    time_diff = abs((entry_dt - target_date).total_seconds())
                    return (cloud_cover, time_diff)
                except ValueError:
                    pass
            return (100, float('inf'))
    
        entries.sort(key=score_entry)
        for entry in entries:
            b4_url = None
            b8_url = None
            
            for link in entry.get('links', []):
                url = link.get('href', '')
                if url.startswith('https://') and '.tif' in url:
                    if 'B04.tif' in url:
                        b4_url = url
                    elif 'B08.tif' in url:
                        b8_url = url
            
            if b4_url and b8_url:
                try:
                    entry_date = datetime.strptime(
                        entry['time_start'].split('.')[0], 
                        "%Y-%m-%dT%H:%M:%S"
                    ).replace(tzinfo=timezone.utc)
                    
                    cloud_cover = float(entry.get('cloud_cover', 'N/A'))
                    print(f"Found data for {entry_date.date()} with {cloud_cover}% cloud cover")
                    b4_response = download_session.get(b4_url, stream=True)

                    if b4_response.status_code == 200:
                        b4_path = os.path.join(download_dir, f"hls_b4_{entry_date.strftime('%Y%m%d')}.tif")
                        with open(b4_path, 'wb') as f:
                            for chunk in b4_response.iter_content(chunk_size=1024*1024):
                                f.write(chunk)
                        print(f"Downloaded B4 to: {b4_path}")
                    
                    b8_response = download_session.get(b8_url, stream=True)
                    if b8_response.status_code == 200:
                        b8_path = os.path.join(download_dir, f"hls_b8_{entry_date.strftime('%Y%m%d')}.tif")
                        with open(b8_path, 'wb') as f:
                            for chunk in b8_response.iter_content(chunk_size=1024*1024):
                                f.write(chunk)
                        print(f"Downloaded B8 to: {b8_path}")
                        
                        return [b4_path, b8_path]
                except Exception as e:
                    print(f"Error processing entry: {str(e)}")
                    continue
        return None

    result = try_month(datetime_obj)
    if result:
        return result

    print("No data found for current month, trying previous month...")
    if datetime_obj.month == 1:
        prev_month_date = datetime_obj.replace(year=datetime_obj.year-1, month=12)
    else:
        prev_month_date = datetime_obj.replace(month=datetime_obj.month-1)
    
    result = try_month(prev_month_date)
    if result:
        return result
    
    print("No suitable Sentinel-2 data found in current or previous month")
    return None

def download_srtm_tile(lat, lon, download_dir='/tmp'):
    """Download SRTM tile using proper Earthdata authentication."""
    try:
        lat_prefix = 'N' if lat >= 0 else 'S'
        lon_prefix = 'E' if lon >= 0 else 'W'
        tile_name = f"{lat_prefix}{abs(lat):02d}{lon_prefix}{abs(lon):03d}.SRTMGL1.hgt.zip"
        url = f"https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/2000.02.11/{tile_name}"
        tile_path = os.path.join(download_dir, tile_name)
        download_session = SessionWithHeaderRedirection(EARTHDATA_USERNAME, EARTHDATA_PASSWORD)
        response = download_session.get(url, stream=True)
        
        if response.status_code == 200:
            with open(tile_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            print(f"Downloaded SRTM tile: {tile_name}")
            
            with zipfile.ZipFile(tile_path, 'r') as zip_ref:
                hgt_filename = zip_ref.namelist()[0]
                zip_ref.extract(hgt_filename, download_dir)
            
            hgt_path = os.path.join(download_dir, hgt_filename)
            tif_path = os.path.join(download_dir, f"{os.path.splitext(hgt_filename)[0]}.tif")
            subprocess.run([
                'gdal_translate',
                '-of', 'GTiff',
                '-co', 'COMPRESS=LZW',
                '-a_srs', 'EPSG:4326',
                hgt_path,
                tif_path
            ], check=True)

            os.remove(tile_path)
            os.remove(hgt_path)
            
            return tif_path
            
        else:
            print(f"Failed to download {tile_name}: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Error downloading SRTM tile {tile_name}: {str(e)}")
        return None

def fetch_srtm(bbox, sentinel_bounds=None, sentinel_crs=None, sentinel_shape=None):
    """Fetch and merge SRTM tiles using Sentinel-2 as reference."""
    try:
        print("\n=== Fetching SRTM Data ===")
        temp_dir = tempfile.mkdtemp()
        
        if not (sentinel_bounds and sentinel_crs):
            raise ValueError("Sentinel-2 bounds and CRS required")
            
        wgs84_bounds = transform_bounds(sentinel_crs, 'EPSG:4326', *sentinel_bounds)
        print(f"WGS84 bounds for tile calculation: {wgs84_bounds}")
        min_lon = math.floor(wgs84_bounds[0])
        max_lon = math.ceil(wgs84_bounds[2])
        min_lat = math.floor(wgs84_bounds[1])
        max_lat = math.ceil(wgs84_bounds[3])
        print(f"Need to fetch SRTM tiles from {min_lat},{min_lon} to {max_lat},{max_lon}")

        tile_paths = []
        for lat in range(min_lat, max_lat):
            for lon in range(min_lon, max_lon):
                tile_path = download_srtm_tile(lat, lon, download_dir=temp_dir)
                if tile_path:
                    tile_paths.append(tile_path)

        if not tile_paths:
            raise ValueError("No SRTM tiles downloaded")

        datasets = [rasterio.open(p) for p in tile_paths]
        mosaic, out_trans = merge(datasets)
        for ds in datasets:
            ds.close()

        output = np.zeros(sentinel_shape, dtype='float32')
        output_transform = from_bounds(*sentinel_bounds, sentinel_shape[1], sentinel_shape[0])
        
        reproject(
            source=mosaic[0],
            destination=output,
            src_transform=out_trans,
            src_crs='EPSG:4326',
            dst_transform=output_transform,
            dst_crs=sentinel_crs,
            resampling=Resampling.bilinear
        )
        
        return output, None, output_transform, sentinel_crs

    except Exception as e:
        print(f"\n=== Error in SRTM processing ===")
        print(f"Error: {str(e)}")
        traceback.print_exc()
        return None, None, None, None

def get_current_ifs_path():
    """Generate paths and URLs for IFS forecast data."""
    current_date = datetime.now(timezone.utc)
    
    if current_date.hour < 2:
        current_date = current_date - timedelta(days=1)
        print(f"Within first 2 hours of UTC, using data from {current_date.date()}")
    
    date_str = current_date.strftime("%Y%m%d")
    processed_path = f"ecmwf_forecast_{date_str}.nc"
    
    return current_date, date_str, processed_path

def process_global_ifs(grib_paths, timesteps, output_path):
    """Process multiple GRIB files into a single NetCDF with timesteps."""
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
            "msl": 151
        }
        
        datasets = []
        for grib_path, step in zip(grib_paths, timesteps):
            timestep_datasets = []
            for var_name, param_id in ecmwf_params.items():
                try:
                    ds = xr.open_dataset(grib_path, engine="cfgrib", 
                                       filter_by_keys={"paramId": param_id})
                    
                    ds = ds.drop_vars(["heightAboveGround", "depthBelowLandLayer"], 
                                    errors="ignore")
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
            combined_ds.to_netcdf(output_path)
            return True
        return False
        
    except Exception as e:
        print(f"Error processing IFS data: {str(e)}")
        return False

def fetch_ifs_forecast(bbox, datetime_obj, sentinel_bounds=None, sentinel_crs=None, sentinel_shape=None):
    """Fetch IFS forecast data, requesting new data if no cache exists."""
    try:
        current_utc = datetime.now(timezone.utc)
        target_date = current_utc - timedelta(days=1) if current_utc.hour < 2 else current_utc
        cache_file = f"ecmwf_forecast_{target_date.strftime('%Y%m%d')}.nc"
        prev_cache = f"ecmwf_forecast_{(target_date - timedelta(days=1)).strftime('%Y%m%d')}.nc"

        for cache_path in [cache_file, prev_cache]:
            if os.path.exists(cache_path):
                print(f"Loading cached IFS data from: {cache_path}")
                ds = xr.open_dataset(cache_path)
                data = extract_ifs_variables(ds, bbox, 
                                          sentinel_bounds=sentinel_bounds,
                                          sentinel_crs=sentinel_crs,
                                          sentinel_shape=sentinel_shape)
                if data is not None:
                    return data
                    
        print("No cached data found, downloading new forecast data...")
        
        date_str = target_date.strftime('%Y%m%d')
        base_url = "https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com"
        time = "00z"
        
        timesteps = range(6, 25, 6)  # 6, 12, 18, 24
        urls = [
            f"{base_url}/{date_str}/{time}/ifs/0p25/oper/{date_str}000000-{step}h-oper-fc.grib2"
            for step in timesteps
        ]
        
        datasets = []
        for step, url in zip(timesteps, urls):
            with tempfile.NamedTemporaryFile(suffix=".grib2") as temp_file:
                subprocess.run(["curl", "-o", temp_file.name, url], check=True)
                timestep_data = process_global_ifs([temp_file.name], [step], cache_file)
                if timestep_data:
                    datasets.append(timestep_data)
        
        if datasets:
            ds = xr.open_dataset(cache_file)
            return extract_ifs_variables(ds, bbox, 
                                      sentinel_bounds=sentinel_bounds,
                                      sentinel_crs=sentinel_crs,
                                      sentinel_shape=sentinel_shape)
            
        print("Failed to download new forecast data")
        return None

    except Exception as e:
        print(f"Error fetching IFS data: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return None

def extract_ifs_variables(ds, bbox, sentinel_bounds=None, sentinel_crs=None, sentinel_shape=None):
    """Extract and process IFS variables from dataset."""
    try:
        print("Starting IFS variable extraction...")
        
        if not sentinel_shape:
            raise ValueError("Sentinel-2 shape is required for IFS resampling")
            
        if sentinel_bounds and sentinel_crs:
            wgs84_bounds = transform_bounds(sentinel_crs, "EPSG:4326", 
                                         *sentinel_bounds)
            ds_cropped = ds.sel(
                latitude=slice(max(wgs84_bounds[1], wgs84_bounds[3]), 
                             min(wgs84_bounds[1], wgs84_bounds[3])),
                longitude=slice(min(wgs84_bounds[0], wgs84_bounds[2]), 
                              max(wgs84_bounds[0], wgs84_bounds[2])))
        else:
            ds_cropped = ds.sel(
                latitude=slice(max(bbox[1], bbox[3]), min(bbox[1], bbox[3])),
                longitude=slice(min(bbox[0], bbox[2]), max(bbox[0], bbox[2])))
        
        if ds_cropped.sizes['latitude'] == 0 or ds_cropped.sizes['longitude'] == 0:
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
            'st': ds_time['sot'].isel(soilLayer=0).values,
            'stl2': ds_time['sot'].isel(soilLayer=1).values,
            'stl3': ds_time['sot'].isel(soilLayer=2).values
        }

        variables_to_process = [
            ('t2m', ds_time['t2m'].values),
            ('tp', ds_time['tp'].values),
            ('ssrd', ds_time['ssrd'].values),
            ('st', soil_temps['st']),
            ('stl2', soil_temps['stl2']),
            ('stl3', soil_temps['stl3']),
            ('sp', ds_time['sp'].values),
            ('d2m', ds_time['d2m'].values),
            ('u10', ds_time['u10'].values),
            ('v10', ds_time['v10'].values),
            ('ro', ds_time['ro'].values),
            ('msl', ds_time['msl'].values),
            ('et0', et0),
            ('bare_soil_evap', bare_soil_evap),
            ('svp', svp),
            ('avp', avp),
            ('r_n', r_n)
        ]
        
        upsampled_vars = []
        for var_name, data in variables_to_process:
            upsampled = resize(data, sentinel_shape,
                             order=0,
                             preserve_range=True,
                             anti_aliasing=False)
            print(f"Upsampled {var_name} from {data.shape} to {upsampled.shape}")
            upsampled_vars.append(upsampled)
        
        print(f"Successfully processed {len(upsampled_vars)} variables")
        return upsampled_vars
        
    except Exception as e:
        print(f"Error extracting IFS variables: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return None

def get_soil_data(bbox, datetime_obj=None):
    """Download soil-related datasets and combine them into a single GeoTIFF."""
    if datetime_obj is None:
        datetime_obj = datetime.now(timezone.utc)
    
    try:
        print(f"\n=== Starting Data Collection ===")
        sentinel_data = []
        ifs_resized = []
        srtm_resized = None

        print("\n=== Processing Sentinel Data ===")
        sentinel_paths = fetch_hls_b4_b8(bbox, datetime_obj)
        if not sentinel_paths:
            print("Failed to fetch Sentinel data")
            return None
            
        with rasterio.open(sentinel_paths[0]) as src:
            sentinel_bounds = src.bounds
            sentinel_crs = src.crs
            sentinel_shape = (222, 222)
            sentinel_transform = src.transform
            for path in sentinel_paths:
                with rasterio.open(path) as band_src:
                    data = np.zeros((222, 222), dtype='float32')
                    reproject(
                        source=rasterio.band(band_src, 1),
                        destination=data,
                        src_transform=band_src.transform,
                        src_crs=band_src.crs,
                        dst_transform=from_bounds(*sentinel_bounds, 222, 222),
                        dst_crs=sentinel_crs,
                        resampling=Resampling.bilinear
                    )
                    sentinel_data.append(data)

        srtm_array, srtm_file, srtm_transform, srtm_crs = fetch_srtm(
            bbox, 
            sentinel_bounds=sentinel_bounds,
            sentinel_crs=sentinel_crs,
            sentinel_shape=(222, 222)
        )
        
        ifs_data = fetch_ifs_forecast(bbox, datetime_obj,
                                    sentinel_bounds=sentinel_bounds,
                                    sentinel_crs=sentinel_crs,
                                    sentinel_shape=(222, 222))
        
        profile = {
            'driver': 'GTiff',
            'height': sentinel_shape[0],
            'width': sentinel_shape[1],
            'count': len(sentinel_data) + len(ifs_data) + 2,
            'dtype': 'float32',
            'crs': srtm_crs,
            'transform': srtm_transform,
            'sentinel_transform': sentinel_transform,
            'compress': 'lzw',
            'tiled': True,
            'blockxsize': 256,
            'blockysize': 256
        }
        
        print("\n=== Combining All Data ===")
        output_file = combine_tiffs(
            sentinel_data,
            ifs_data,
            (srtm_array, srtm_file),
            bbox,
            datetime_obj.strftime("%Y-%m-%d_%H%M"),
            profile
        )
        
        with rasterio.open(output_file) as dest:
            print(f"Final combined transform: {dest.transform}")
            print(f"Final combined CRS: {dest.crs}")
            
            bounds = dest.bounds
            # [left, bottom, right, top] format
            sentinel_bounds = [bounds.left, bounds.bottom, bounds.right, bounds.top]
            # EPSG code as integer
            sentinel_crs = int(str(dest.crs).split(':')[1])
            
            return {
                'combined_data': output_file,
                'sentinel_bounds': sentinel_bounds,
                'sentinel_crs': sentinel_crs
            }
            
    except Exception as e:
        print(f"\n=== Error Occurred ===")
        print(f"Error in get_soil_data: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return None

def combine_tiffs(sentinel_data, ifs_data, srtm_data_tuple, bbox, date_str, profile):
    try:
        srtm_array, srtm_file = srtm_data_tuple
        target_shape = (profile['height'], profile['width'])
        srtm_resized = resize(srtm_array, target_shape, 
                            preserve_range=True, 
                            order=1,
                            mode='constant',
                            cval=-9999)
        
        print("\n=== Data Shape Verification ===")
        print(f"Target shape: {target_shape}")
        print(f"SRTM shape after resize: {srtm_resized.shape}")
        print(f"Sentinel data shapes: {[band.shape for band in sentinel_data]}")
        print(f"IFS data shapes: {[band.shape for band in ifs_data]}")
        output_file = f"combined_{bbox[0]}_{bbox[1]}_{bbox[2]}_{bbox[3]}_{date_str}.tif"
        
        with rasterio.open(output_file, 'w', **profile) as dst:
            band_idx = 1
            for band in sentinel_data:
                dst.write(band.astype('float32'), band_idx)
                band_idx += 1

            for band in ifs_data:
                dst.write(band.astype('float32'), band_idx)
                band_idx += 1
            
            dst.write(srtm_resized.astype('float32'), band_idx)
            band_idx += 1
            ndvi = (sentinel_data[1] - sentinel_data[0]) / (sentinel_data[1] + sentinel_data[0])
            dst.write(ndvi.astype('float32'), band_idx)
            dst.update_tags(
                sentinel_transform=str(profile['sentinel_transform']),
                band_order="1-2:Sentinel(B4,B8), 3-19:IFS+Evap, 20:SRTM, 21:NDVI"
            )
        print(f"Successfully wrote combined data to {output_file}")
        return output_file
        
    except Exception as e:
        print(f"Error combining data: {str(e)}")
        traceback.print_exc()
        return None


import numpy as np


def calculate_penman_monteith(ds, params=None):
    """

    Args:
        ds (dict): Input data dictionary with keys ('t2m', 'd2m', 'ssrd', 'sp', 'u10', 'v10').
                   Each key should map to a NumPy array or similar.
        params (dict): Optional parameters for constants and coefficients.

    Returns:
        Tuple: (ET0, svp, avp, net_radiation)
               ET0: Evapotranspiration (mm/day)
               svp: Saturation vapor pressure (kPa)
               avp: Actual vapor pressure (kPa)
               net_radiation: Net solar radiation (MJ/m²/day)
    """
    try:
        print("\n=== Penman-Monteith Calculation ===")

        # Constants with default values
        default_params = {
            "albedo": 0.23,  # Albedo coefficient
            "psychrometric_coefficient": 0.000665  # kPa/°C
        }
        if params:
            default_params.update(params)
        albedo = default_params["albedo"]
        psy_coefficient = default_params["psychrometric_coefficient"]

        # Temperature (K to °C)
        t2m = ds['t2m'] - 273.15
        d2m = ds['d2m'] - 273.15

        # Radiation (J/m² to MJ/m²/day)
        ssrd = ds['ssrd'] / 1e6

        # Pressure (Pa to kPa)
        sp = ds['sp'] / 1e3

        # Wind speed (m/s)
        u10 = ds['u10']
        v10 = ds['v10']
        wind_speed = np.sqrt(u10 ** 2 + v10 ** 2)

        # Vapor pressure (kPa)
        svp = 0.6108 * np.exp((17.27 * t2m) / (t2m + 237.3))  # Saturation vapor pressure
        avp = 0.6108 * np.exp((17.27 * d2m) / (d2m + 237.3))  # Actual vapor pressure

        # Psychrometric constant and slope of SVP curve
        psy = psy_coefficient * sp
        delta = (4098 * svp) / ((t2m + 237.3) ** 2)

        # Net radiation
        net_radiation = ssrd * (1 - albedo)

        # Penman-Monteith formula
        num = (0.408 * delta * net_radiation) + (psy * (900 / (t2m + 273)) * wind_speed * (svp - avp))
        den = delta + psy * (1 + 0.34 * wind_speed)
        et0 = num / den

        print(f"ET0: {et0.mean():.2f} mm/day ({et0.min():.2f} to {et0.max():.2f})")
        return et0, svp, avp, net_radiation

    except KeyError as e:
        print(f"Missing data for calculation: {e}")
        return None
    except Exception as e:
        print(f"Error in Penman-Monteith calculation: {str(e)}")
        return None


def partition_evaporation(total_evap, ds):
    """
    Partition total evaporation using available IFS parameters:
    - t2m: 2m temperature
    - st: soil temperature (aggregated)
    - tp: total precipitation
    - ssrd: surface solar radiation downwards
    """
    try:
        def kelvin_to_celsius(kelvin_temp):
            return kelvin_temp - 273.15

        def w_to_mj(w_per_m2_day):
            return w_per_m2_day * 0.0864

        #Soil-Air Temperature Gradient Factor
        #Higher gradient = more evaporation from soil
        t2m = kelvin_to_celsius(ds['t2m'].values)
        soil_temp = kelvin_to_celsius(ds['sot'].isel(soilLayer=0).values)
        temp_gradient = np.clip((soil_temp - t2m) / 10, -1, 1)
        temp_factor = 0.4 + 0.2 * temp_gradient
        
        #Precipitation Factor
        #Recent precipitation = less bare soil evaporation
        precip = ds['tp'].values * 1000
        wetness_factor = np.clip(1 - np.exp(-0.5 * precip), 0, 1)
        
        #Solar Radiation Factor
        #More radiation = more potential for bare soil evaporation
        rad = w_to_mj(ds['ssrd'].values)
        rad_norm = np.clip(rad / 30, 0, 1)
        
        #Physical weighting
        #Temperature gradient (0.5)
        #Wetness (0.3)
        #Radiation (0.2)
        bare_soil_fraction = (0.5 * temp_factor + 
                            0.3 * (1 - wetness_factor) + 
                            0.2 * rad_norm)
        
        #Bare soil evap est. 20-50% of total
        return total_evap * np.clip(bare_soil_fraction, 0.2, 0.5)
        
    except Exception as e:
        print(f"Error in evaporation partitioning: {str(e)}")
        print(f"Using fallback value of 30% bare soil evaporation")
        return total_evap * 0.3  #Fallback val

def get_target_shape(bbox, target_resolution=500):
    """Calculate target shape using proper geospatial transformations."""
    transformer = Transformer.from_crs(
        "EPSG:4326",
        "EPSG:3857",
        always_xy=True
    )
    minx, miny = transformer.transform(bbox[0], bbox[1])
    maxx, maxy = transformer.transform(bbox[2], bbox[3])
    width_meters = maxx - minx
    height_meters = maxy - miny
    width_pixels = int(round(width_meters / target_resolution))
    height_pixels = int(round(height_meters / target_resolution))
    
    return (height_pixels, width_pixels)
