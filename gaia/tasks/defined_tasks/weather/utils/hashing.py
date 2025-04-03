import hashlib
import json
import struct
from typing import Dict, List, Tuple, Any, Optional, Set, Union
import numpy as np
import xarray as xr
import fsspec


NUM_SAMPLES = 1000
HASH_VERSION = "1"


def generate_deterministic_seed(
    forecast_date: str,
    source_model: str,
    grid_resolution: float,
    num_variables: int
) -> int:
    """
    Generate a deterministic seed from forecast metadata for reproducible sampling.
    
    Args:
        forecast_date: Date of the forecast initialization in YYYY-MM-DD format
        source_model: Name of the source model (e.g., "aurora-0.25-finetuned")
        grid_resolution: Resolution of the grid in degrees (e.g., 0.25)
        num_variables: Number of variables in the forecast
        
    Returns:
        A deterministic integer seed
    """
    seed_str = f"{forecast_date}_{source_model}_{grid_resolution}_{num_variables}_{HASH_VERSION}"
    hash_obj = hashlib.sha256(seed_str.encode())
    seed = int.from_bytes(hash_obj.digest()[:4], byteorder='big')

    return seed


def generate_sample_indices(
    rng: np.random.Generator,
    data_shape: Dict[str, Dict[str, Tuple[int, ...]]],
    variables: List[str],
    timesteps: List[int],
    num_samples: int = NUM_SAMPLES
) -> List[Dict[str, Any]]:
    """
    Generate indices for sampling data points from the forecast.
    
    Args:
        rng: Numpy random generator initialized with deterministic seed
        data_shape: Dictionary of variable categories and their shapes
        variables: List of variables to sample from
        timesteps: List of timestep indices to sample from
        num_samples: Number of sample points to generate
        
    Returns:
        List of dictionaries with sampling coordinates
    """
    sample_indices = []
    
    var_categories = {
        "surf_vars": [v for v in variables if v in ["2t", "10u", "10v", "msl"]],
        "atmos_vars": [v for v in variables if v in ["z", "u", "v", "t", "q"]]
    }
    
    samples_per_var = num_samples // len(variables)
    extra_samples = num_samples % len(variables)
    
    var_indices = []
    for var in variables:
        var_samples = samples_per_var + (1 if extra_samples > 0 else 0)
        extra_samples -= 1 if extra_samples > 0 else 0
        var_indices.extend([(var, i) for i in range(var_samples)])
    
    rng.shuffle(var_indices)
    
    for var, _ in var_indices:
        category = None
        for cat, vars_list in var_categories.items():
            if var in vars_list:
                category = cat
                break
        
        if category is None:
            continue
            
        shape = data_shape[category].get(var)
        if shape is None:
            continue
            
        if category == "atmos_vars":
            t_idx = rng.choice(timesteps)
            level_idx = rng.choice(shape[2])
            lat_idx = rng.choice(shape[3])
            lon_idx = rng.choice(shape[4])
            
            sample_indices.append({
                "variable": var,
                "category": category,
                "timestep": int(t_idx),
                "level": int(level_idx),
                "lat": int(lat_idx),
                "lon": int(lon_idx)
            })
        else:
            t_idx = rng.choice(timesteps)
            lat_idx = rng.choice(shape[2])
            lon_idx = rng.choice(shape[3])
            
            sample_indices.append({
                "variable": var,
                "category": category,
                "timestep": int(t_idx),
                "lat": int(lat_idx),
                "lon": int(lon_idx)
            })
    
    return sample_indices


def serialize_float(value: float) -> bytes:
    """
    Serialize a float to bytes using IEEE 754 double precision format.
    
    Args:
        value: Float value to serialize
        
    Returns:
        Bytes representing the float in canonical form
    """
    return struct.pack('>d', float(value))


def canonical_serialization(
    sample_indices: List[Dict[str, Any]],
    data: Dict[str, Any]
) -> bytes:
    """
    Serialize sampled data points in a canonical format.
    
    Args:
        sample_indices: List of dictionaries with sampling coordinates
        data: Dictionary containing the forecast data
        
    Returns:
        Bytes representing the serialized sample data
    """
    serialized = bytearray()
    
    sorted_indices = sorted(
        sample_indices, 
        key=lambda x: (
            x["variable"], 
            x["category"], 
            x["timestep"], 
            x.get("level", 0),
            x["lat"], 
            x["lon"]
        )
    )
    
    for idx in sorted_indices:
        var = idx["variable"]
        category = idx["category"]
        t_idx = idx["timestep"]
        lat_idx = idx["lat"]
        lon_idx = idx["lon"]
        
        try:
            if category == "atmos_vars":
                level_idx = idx["level"]
                value = data[category][var][0, t_idx, level_idx, lat_idx, lon_idx].item()
            else:
                value = data[category][var][0, t_idx, lat_idx, lon_idx].item()
                
            serialized.extend(serialize_float(value))
            
        except (KeyError, IndexError) as e:
            serialized.extend(serialize_float(float('nan')))
    
    return bytes(serialized)


def compute_verification_hash(
    data: Dict[str, Dict[str, np.ndarray]],
    metadata: Dict[str, Any],
    variables: List[str],
    timesteps: List[int]
) -> str:
    """
    Compute a verification hash for a forecast dataset (miner-side).
    
    Args:
        data: Dictionary with forecast data in Aurora-compatible format
        metadata: Dictionary with forecast metadata 
        variables: List of variables to include in hash
        timesteps: List of timestep indices to include in hash
        
    Returns:
        Hex string of the SHA-256 hash
    """
    forecast_date = metadata.get("time")[0].strftime("%Y-%m-%d")
    source_model = metadata.get("source_model", "aurora")
    grid_resolution = metadata.get("resolution", 0.25)
    num_variables = len(variables)
    
    data_shape = {
        category: {
            var_name: data_array.shape 
            for var_name, data_array in var_dict.items()
            if var_name in variables
        }
        for category, var_dict in data.items() 
        if category in ["surf_vars", "atmos_vars"]
    }
    
    seed = generate_deterministic_seed(
        forecast_date, source_model, grid_resolution, num_variables
    )
    
    rng = np.random.Generator(np.random.PCG64(seed))
    
    sample_indices = generate_sample_indices(
        rng, data_shape, variables, timesteps, NUM_SAMPLES
    )
    
    serialized_data = canonical_serialization(sample_indices, data)
    hash_obj = hashlib.sha256(serialized_data)

    return hash_obj.hexdigest()


async def open_remote_dataset_with_kerchunk(
    kerchunk_url: str, 
    variables: Optional[List[str]] = None,
    timesteps: Optional[List[int]] = None
) -> xr.Dataset:
    """
    Open a remote dataset using kerchunk index.
    
    Args:
        kerchunk_url: URL to the kerchunk index JSON
        variables: Optional list of variables to load
        timesteps: Optional list of timestep indices to load
        
    Returns:
        xarray Dataset
    """
    from gaia.tasks.defined_tasks.weather.weather_scoring.validator_scoring_utils import open_remote_dataset_with_kerchunk
    
    return await open_remote_dataset_with_kerchunk(kerchunk_url, variables, timesteps)


async def verify_forecast_hash(
    kerchunk_url: str,
    claimed_hash: str,
    metadata: Dict[str, Any],
    variables: List[str],
    timesteps: List[int]
) -> bool:
    """
    Verify a forecast hash against a claimed value (validator-side).
    
    Args:
        kerchunk_url: URL to the kerchunk index for the forecast
        claimed_hash: Hash value provided by the miner
        metadata: Dictionary with forecast metadata 
        variables: List of variables to include in hash
        timesteps: List of timestep indices to include in hash
        
    Returns:
        Boolean indicating whether the hash is verified
    """
    try:
        ds = await open_remote_dataset_with_kerchunk(kerchunk_url, variables)
    except Exception as e:
        print(f"Error opening dataset: {e}")
        return False
    
    data = {
        "surf_vars": {},
        "atmos_vars": {}
    }
    
    var_mapping = {
        "t2m": ("surf_vars", "2t"),
        "u10": ("surf_vars", "10u"),
        "v10": ("surf_vars", "10v"),
        "msl": ("surf_vars", "msl"),
        "z": ("atmos_vars", "z"),
        "u": ("atmos_vars", "u"),
        "v": ("atmos_vars", "v"),
        "t": ("atmos_vars", "t"),
        "q": ("atmos_vars", "q")
    }
    
    for var in variables:
        xr_var = var
        for ds_name, (category, aurora_name) in var_mapping.items():
            if aurora_name == var and ds_name in ds:
                if category == "surf_vars":
                    data[category][var] = ds[ds_name].values[np.newaxis, :, :, :]
                else:
                    data[category][var] = ds[ds_name].values[np.newaxis, :, :, :, :]
                break
    
    hash_metadata = {
        "time": metadata["time"],
        "source_model": metadata.get("source_model", "aurora"),
        "resolution": metadata.get("resolution", 0.25)
    }
    
    computed_hash = compute_verification_hash(
        data, hash_metadata, variables, timesteps
    )
    
    return computed_hash == claimed_hash


def get_forecast_summary(
    kerchunk_url: str,
    variables: List[str]
) -> Dict[str, Any]:
    """
    Get a summary of forecast properties without loading all data.
    
    Args:
        kerchunk_url: URL to the kerchunk index
        variables: List of variables to include
    Returns:
        Dictionary with summary statistics
    """
    # Placeholder implementation
    # this will:
    # 1. Open the kerchunk index
    # 2. Extract metadata
    # 3. Compute and return basic statistics
    
    return {
        "variables": variables,
        "kerchunk_url": kerchunk_url,
        "status": "placeholder"
    } 