import base64
import pickle
import logging
from typing import Any, Dict, Optional

# High-performance JSON operations for inference service
try:
    from gaia.utils.performance import loads
    JSON_PERFORMANCE_AVAILABLE = True
except ImportError:
    import json
    def loads(s):
        return json.loads(s)
    JSON_PERFORMANCE_AVAILABLE = False

# Conditional import for Batch type hint
try:
    from aurora import Batch
    _AURORA_AVAILABLE = True
except ImportError:
    Batch = Any # type: ignore
    _AURORA_AVAILABLE = False
    logging.warning("Aurora SDK not found. Batch type is Any. Functionality might be limited if Batch objects are expected.")

logger = logging.getLogger(__name__)

async def prepare_input_batch_from_payload(payload_str: str, config: Dict[str, Any]) -> Optional[Batch]:
    """
    Deserializes a base64 encoded, pickled Aurora Batch object from a JSON payload string.
    The payload_str is expected to be a JSON string containing a "serialized_aurora_batch" field.
    """
    try:
        # Use high-performance JSON parsing
        payload_dict = loads(payload_str)
        if JSON_PERFORMANCE_AVAILABLE:
            logger.debug("Using orjson for inference payload JSON parsing")
            
        # Expect "batch_data" from WeatherTask
        if "batch_data" not in payload_dict:
            logger.error("\'batch_data\' field missing in the JSON payload.")
            return None

        serialized_batch_b64 = payload_dict["batch_data"]
        if not isinstance(serialized_batch_b64, str):
            logger.error("\'batch_data\' must be a base64 encoded string.")
            return None

        logger.debug("Decoding base64 serialized batch...")
        pickled_batch_bytes = base64.b64decode(serialized_batch_b64)

        logger.debug("Unpickling batch data...")
        deserialized_batch = pickle.loads(pickled_batch_bytes)

        if _AURORA_AVAILABLE:
            if not isinstance(deserialized_batch, Batch): # type: ignore
                logger.error(f"Deserialized object is not an Aurora Batch. Type: {type(deserialized_batch)}")
                return None
        elif deserialized_batch is None: # Basic check if Aurora not available
            logger.error("Deserialized batch is None.")
            return None

        logger.info("Successfully deserialized input Aurora Batch.")
        return deserialized_batch

    except Exception as e:  # Catch all JSON parsing exceptions
        logger.error(f"JSON parsing error in payload: {e}")
        return None
    except base64.binascii.Error as e:
        logger.error(f"Base64 decoding error: {e}")
        return None
    except pickle.UnpicklingError as e:
        logger.error(f"Unpickling error: {e}")
        return None
    except TypeError as e: # Catches potential errors if data isn't bytes for b64decode or pickle.loads
        logger.error(f"Type error during deserialization: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in prepare_input_batch_from_payload: {e}", exc_info=True)
        return None

# Commenting out serialize_prediction_step as it seems to be for an older HTTP flow
# async def serialize_prediction_step(
#     prediction_step: Batch,
#     step_index: int,
#     config: Dict[str, Any]
# ) -> Optional[str]:
#     \"\"\"
#     Serializes a single prediction step (Aurora Batch object) into a JSON string.
#     The batch is pickled, then base64 encoded.
#     Includes metadata like step index and forecast hours.
#     \"\"\"
#     if _AURORA_AVAILABLE and not isinstance(prediction_step, Batch): # type: ignore
#         logger.error(f\"Object to serialize is not an Aurora Batch. Type: {type(prediction_step)}. Skipping step {step_index}.\")
#         return None
#     elif prediction_step is None: # Basic check if Aurora not available
#         logger.error(f\"Prediction_step is None for step_index {step_index}. Skipping.\")
#         return None
# 
#     try:
#         logger.debug(f\"Pickling prediction step {step_index}...\")
#         pickled_prediction = pickle.dumps(prediction_step)
# 
#         logger.debug(f\"Base64 encoding pickled prediction step {step_index}...\")
#         serialized_data_b64 = base64.b64encode(pickled_prediction).decode('utf-8')
# 
#         model_config = config.get('model', {})
#         data_proc_config = config.get('data_processing', {})
# 
#         output_dict = {
#             \"step_index\": step_index,
#             \"forecast_step_hours\": (step_index + 1) * model_config.get('forecast_step_hours', 6),
#             \"serialized_prediction\": serialized_data_b64,
#             \"format\": data_proc_config.get('serialization_format', 'pickle_base64')
#         }
# 
#         logger.info(f\"Successfully serialized prediction step {step_index}.\")
#         return json.dumps(output_dict)
# 
#     except pickle.PicklingError as e:
#         logger.error(f\"Pickling error for step {step_index}: {e}\")
#         return None
#     except TypeError as e: # Catches errors like trying to b64encode non-bytes
#         logger.error(f\"Type error during serialization for step {step_index}: {e}\")
#         return None
#     except Exception as e:
#         logger.error(f\"Unexpected error in serialize_prediction_step for step {step_index}: {e}\", exc_info=True)
#         return None

# Commenting out serialize_batch_step_to_base64_gzipped_netcdf as it's not used by the current runpodctl tar archive flow
# # --- New helper for serializing a single Batch step to base64 gzipped NetCDF ---
# def serialize_batch_step_to_base64_gzipped_netcdf(
#     batch_step: Batch,
#     step_index: int, # For logging and potentially for forecast time calculation if not in batch_step metadata
#     base_time: Any, # Expecting a datetime or similar for forecast_time calculation
#     config: Dict[str, Any]
# ) -> Optional[str]:
#     \"\"\"
#     Converts a single Aurora Batch step to an xarray.Dataset,
#     serializes to NetCDF, gzips it, and then base64 encodes the result.
#     Returns a base64 string or None if an error occurs.
#     \"\"\"
#     import xarray as xr
#     import numpy as np
#     # import pandas as pd # No longer needed here if base_time is already a pandas Timestamp
#     from datetime import timedelta
#     import io
#     import gzip
# 
#     if _AURORA_AVAILABLE and not isinstance(batch_step, Batch): # type: ignore
#         logger.error(f\"Object to serialize for step {step_index} is not an Aurora Batch. Type: {type(batch_step)}\")
#         return None
#     elif batch_step is None:
#         logger.error(f\"Batch_step is None for step_index {step_index}. Cannot serialize.\")
#         return None
# 
#     try:
#         model_config = config.get('model', {})
#         forecast_step_h = model_config.get('forecast_step_hours', 6)
#         # Calculate forecast time for this step
#         # Ensure base_time is datetime-like. If it's from batch_step.metadata.time, it might be.
#         # This logic mirrors what was in weather_workers.py _blocking_save_and_process
#         current_lead_time_hours = (step_index + 1) * forecast_step_h 
#         # Assuming base_time is the initial time of the forecast (e.g., GFS init time)
#         # This might need to be passed in or derived from the input batch if it contains multiple time steps.
#         # For a single step output from model, step_index usually starts from 0 for the first forecast step.
#         # base_time is expected to be a pandas Timestamp or datetime-like from main.py
#         forecast_time = base_time + timedelta(hours=current_lead_time_hours)
# 
#         logger.debug(f\"Converting prediction Batch step {step_index} (Lead: {current_lead_time_hours}h, Forecast Time: {forecast_time}) to xarray Dataset...\")
#         data_vars = {}
#         # Assuming batch_step.surf_vars and batch_step.atmos_vars exist and are dicts of tensors
#         for var_name, tensor_data in getattr(batch_step, 'surf_vars', {}).items():
#             np_data = tensor_data.squeeze().cpu().numpy()
#             data_vars[var_name] = xr.DataArray(np_data, dims=[\"lat\", \"lon\"], name=var_name)
#         
#         for var_name, tensor_data in getattr(batch_step, 'atmos_vars', {}).items():
#             np_data = tensor_data.squeeze().cpu().numpy()
#             data_vars[var_name] = xr.DataArray(np_data, dims=[\"pressure_level\", \"lat\", \"lon\"], name=var_name)
# 
#         # Assuming batch_step.metadata contains lat, lon, atmos_levels
#         metadata = getattr(batch_step, 'metadata', None)
#         if not metadata:
#             logger.error(f\"Missing metadata in Batch step {step_index}. Cannot create Dataset coords.\")
#             return None
# 
#         lat_coords = metadata.lat.cpu().numpy()
#         lon_coords = metadata.lon.cpu().numpy()
#         level_coords = np.array(metadata.atmos_levels)
#         
#         ds_step = xr.Dataset(
#             data_vars,
#             coords={
#                 \"time\": forecast_time, # Single datetime value for this step
#                 \"lead_time\": current_lead_time_hours, # Adding lead_time as a coordinate
#                 \"pressure_level\": ((\"pressure_level\"), level_coords),
#                 \"lat\": ((\"lat\"), lat_coords),
#                 \"lon\": ((\"lon\"), lon_coords),
#             }
#         )
#         # Expand dims for time to make it a coordinate for easy concatenation later by client
#         ds_step = ds_step.expand_dims(\'time\')
# 
#         logger.debug(f\"Serializing step {step_index} Dataset to NetCDF in memory...\")
#         with io.BytesIO() as nc_buffer:
#             ds_step.to_netcdf(nc_buffer, engine=\"h5netcdf\", compute=True) # or engine=\"netcdf4\"
#             nc_bytes = nc_buffer.getvalue()
#         
#         logger.debug(f\"Gzipping NetCDF bytes for step {step_index}...\")
#         gzipped_nc_bytes = gzip.compress(nc_bytes)
#         
#         logger.debug(f\"Base64 encoding gzipped NetCDF bytes for step {step_index}...\")
#         base64_encoded_str = base64.b64encode(gzipped_nc_bytes).decode('utf-8')
#         
#         logger.info(f\"Successfully serialized Batch step {step_index} to base64 gzipped NetCDF.\")
#         return base64_encoded_str
# 
#     except Exception as e:
#         logger.error(f\"Error serializing Batch step {step_index} to base64 gzipped NetCDF: {e}\", exc_info=True)
#         return None 

# Commenting out save_dataset_step_to_gzipped_netcdf as it's no longer needed with in-memory tar archiving
# # --- New helper for saving an xarray.Dataset step to a gzipped NetCDF file ---
# import xarray as xr
# import gzip
# import io
# from pathlib import Path
# import os
# 
# def save_dataset_step_to_gzipped_netcdf(
#     dataset_step: xr.Dataset,
#     output_dir: Path,
#     filename: str,
#     step_index: int # For logging
# ) -> Optional[Path]:
#     \"\"\"
#     Saves a single xarray.Dataset (a forecast step) to a gzipped NetCDF file.
# 
#     Args:
#         dataset_step: The xarray.Dataset to save.
#         output_dir: The directory (Path object) where the file should be saved.
#         filename: The name for the output file (e.g., \"step_0.nc.gz\").
#         step_index: The index of the step, for logging purposes.
# 
#     Returns:
#         The Path to the saved file, or None if an error occurs.
#     \"\"\"
#     if not isinstance(dataset_step, xr.Dataset):
#         logger.error(f\"[SaveStep {step_index}] Input is not an xarray.Dataset. Type: {type(dataset_step)}\")
#         return None
#     
#     output_file_path = output_dir / filename
# 
#     try:
#         output_dir.mkdir(parents=True, exist_ok=True)
#         
#         # Serialize NetCDF to an in-memory buffer
#         with io.BytesIO() as nc_buffer:
#             dataset_step.to_netcdf(nc_buffer, engine=\"h5netcdf\") # or \"netcdf4\"
#             nc_buffer.seek(0)
#             compressed_data = gzip.compress(nc_buffer.read())
#         
#         # Write compressed data to file
#         with open(output_file_path, \"wb\") as f:
#             f.write(compressed_data)
#             
#         logger.info(f\"[SaveStep {step_index}] Successfully saved and gzipped step to {output_file_path}\")
#         return output_file_path
# 
#     except Exception as e:
#         logger.error(f\"[SaveStep {step_index}] Error saving or gzipping step to {output_file_path}: {e}\", exc_info=True)
#         return None 