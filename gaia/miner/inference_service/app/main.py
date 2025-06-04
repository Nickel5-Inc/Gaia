print("[MAIN_PY_DEBUG_TOP] main.py parsing started.", flush=True)

# Entry point check
print("[MAIN_PY_DEBUG] Script execution started.", flush=True)

import asyncio
import base64
import json
import logging
import os
import pickle
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional, List, Tuple

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Request, Security, status
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
import pandas as pd
from pathlib import Path
import uuid
import tarfile
import runpod
import subprocess
import shutil
import tempfile
import gzip
import io
import time
from datetime import timezone, timedelta
import sys # Ensure sys is imported if you use it for stdout/stderr explicitly later

import boto3 # For R2
from botocore.exceptions import ClientError # For R2 error handling

# --- Global Variables to be populated at startup --- 
APP_CONFIG: Dict[str, Any] = {}
EXPECTED_API_KEY: Optional[str] = None
INITIALIZED = False # Flag for lazy initialization
S3_CLIENT: Optional[boto3.client] = None # For R2
R2_CONFIG: Dict[str, Any] = {} # For R2 connection details

# --- Logging Configuration (Initial basic setup) ---
_log_level_str = os.getenv("LOG_LEVEL", "DEBUG").upper()
_log_level = getattr(logging, _log_level_str, logging.DEBUG)
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    stream=sys.stdout
)
_logger = logging.getLogger(__name__) # Logger for this specific module
_logger.info(f"Initial logging configured with level: {_log_level_str}")

# --- Actual imports from local modules ---
from .data_preprocessor import prepare_input_batch_from_payload
from . import inference_runner as ir_module # Import the module itself
from .inference_runner import (
    initialize_inference_runner,
    run_model_inference,
    BatchType as Batch, # Use BatchType as Batch, or directly BatchType
    _AURORA_AVAILABLE # Import the module-level variable
)
# Note: If Aurora SDK is unavailable, BatchType will be 'Any' as defined in inference_runner.py

import xarray as xr # Add import for xarray
import numpy as np # Add import for numpy

# --- Configuration Loading Function ---
CONFIG_PATH = os.getenv("INFERENCE_CONFIG_PATH", "config/settings.yaml")
def load_config(path: str) -> Dict[str, Any]:
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
        if not isinstance(config, dict):
            _logger.error(f"Config at {path} is not a dictionary. Loaded: {type(config)}")
            return {}
        _logger.info(f"Successfully loaded configuration from {path}.")
        return config
    except FileNotFoundError:
        _logger.error(f"Configuration file not found at {path}. Using empty config.")
        return {}
    except Exception as e:
        _logger.error(f"Error loading or parsing configuration from {path}: {e}", exc_info=True)
        return {}

# --- Logging Setup Function (to be called after config is loaded) ---
def setup_logging_from_config(config: Dict[str, Any]):
    log_config = config.get('logging', {})
    level = log_config.get('level', _log_level_str).upper() # Fallback to initial level
    fmt = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
    # Get the root logger and reconfigure it
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    # Remove existing handlers if any to avoid duplicate logs, then add new one
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter(fmt))
    root_logger.addHandler(stream_handler)
    _logger.info(f"Logging re-configured from settings file. Level: {level}")

# --- API Key Authentication (Header setup) ---
API_KEY_NAME = "X-API-Key"
api_key_header_auth = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
# EXPECTED_API_KEY is populated during initialize_app_for_runpod

app = FastAPI(title="Weather Inference Service") # Lifespan removed as it's not used by RunPod serverless

# --- Helper function for subprocess ---
async def _run_subprocess_command(command_args: List[str], timeout_seconds: int = 300) -> Tuple[bool, str, str]:
    """
    Runs a subprocess command, captures its output, and handles timeouts.

    Args:
        command_args: A list of strings representing the command and its arguments.
        timeout_seconds: How long to wait for the command to complete.

    Returns:
        A tuple: (success: bool, stdout: str, stderr: str)
    """
    try:
        _logger.debug(f"Running subprocess command: {' '.join(command_args)}")
        process = await asyncio.to_thread(
            subprocess.run,
            command_args,
            capture_output=True,
            text=True,
            check=False, # Don't raise exception on non-zero exit; we'll check returncode
            timeout=timeout_seconds
        )
        if process.returncode == 0:
            _logger.debug(f"Subprocess command successful. STDOUT: {process.stdout[:200]}...") # Log snippet
            return True, process.stdout.strip(), process.stderr.strip()
        else:
            _logger.error(f"Subprocess command failed with return code {process.returncode}. COMMAND: {' '.join(command_args)}")
            _logger.error(f"STDERR: {process.stderr}")
            _logger.error(f"STDOUT: {process.stdout}")
            return False, process.stdout.strip(), process.stderr.strip()
    except subprocess.TimeoutExpired:
        _logger.error(f"Subprocess command timed out after {timeout_seconds} seconds. COMMAND: {' '.join(command_args)}")
        return False, "", "TimeoutExpired"
    except Exception as e:
        _logger.error(f"Exception during subprocess command {' '.join(command_args)}: {e}", exc_info=True)
        return False, "", str(e)

# --- RunPodctl Helper Functions ---
async def _execute_runpodctl_receive(
    code: str, 
    final_target_path: Path, 
    original_filename: str, 
    timeout_seconds: int = 600
) -> bool:
    """
    Receives a file using runpodctl. 
    Assumes runpodctl downloads the file with 'original_filename' into the current working directory,
    then moves it to 'final_target_path'.
    """
    # Ensure parent directory for the final target path exists
    final_target_path.parent.mkdir(parents=True, exist_ok=True)
    
    cwd = Path.cwd()
    source_file_after_download = cwd / original_filename
    
    command = ["runpodctl", "receive", code] # No -o flag
    _logger.info(f"Attempting to receive file with runpodctl. Code: {code}. Expecting original name '{original_filename}' in CWD: {cwd}. Final target: {final_target_path}")
    
    success, stdout, stderr = await _run_subprocess_command(command, timeout_seconds=timeout_seconds)
    
    if success:
        _logger.info(f"runpodctl receive command completed. STDOUT: {stdout}. STDERR: {stderr}")
        # Additional check: if 'room not ready' is in stdout, even with exit code 0, treat as failure.
        if "room not ready" in stdout.lower():
            _logger.error(f"runpodctl receive command indicated 'room not ready' despite exit code 0. Treating as failure. STDOUT: {stdout}")
            success = False # Override success based on stdout content

    if success: # Re-check success after potentially overriding it
        if source_file_after_download.exists():
            try:
                shutil.move(str(source_file_after_download), str(final_target_path))
                _logger.info(f"Successfully moved downloaded file from {source_file_after_download} to {final_target_path}")
                return True
            except Exception as e_move:
                _logger.error(f"runpodctl receive successful, but failed to move file from {source_file_after_download} to {final_target_path}: {e_move}", exc_info=True)
                return False
        else:
            _logger.error(f"runpodctl receive command successful, but expected file '{original_filename}' not found in CWD ({cwd}). Listing CWD contents:")
            try:
                contents = list(p.name for p in cwd.iterdir())
                _logger.info(f"CWD ({cwd}) contents: {contents}")
            except Exception as e_ls:
                _logger.error(f"Failed to list CWD contents: {e_ls}")
            return False
    else:
        _logger.error(f"runpodctl receive command failed for code {code}. Stdout: {stdout}, Stderr: {stderr}")
        return False

async def _execute_runpodctl_send(file_path: Path, timeout_seconds: int = 600) -> Optional[str]:
    """Sends a file using runpodctl and returns the one-time code."""
    if not file_path.is_file():
        _logger.error(f"[RunpodctlSend] File not found for sending: {file_path}")
        return None
    command = ["runpodctl", "send", str(file_path)]
    _logger.info(f"Attempting to send file with runpodctl: {file_path}")
    success, stdout, stderr = await _run_subprocess_command(command, timeout_seconds=timeout_seconds)
    if success:
        # Expected output: 
        # Sending 'your_file.tar.gz' (1.2 MiB)
        # Code is: 1234-some-random-words
        # On the other computer run
        # runpodctl receive 1234-some-random-words
        lines = stdout.splitlines()
        for line in lines:
            if "Code is:" in line:
                code = line.split("Code is:", 1)[1].strip()
                _logger.info(f"runpodctl send successful for {file_path}. Code: {code}. Full stdout: {stdout}")
                return code
        _logger.error(f"runpodctl send for {file_path} appeared successful but code line not found in stdout: {stdout}")
        return None
    else:
        _logger.error(f"runpodctl send failed for {file_path}. Stdout: {stdout}, Stderr: {stderr}")
        return None

# --- R2 Helper Functions ---
async def _download_from_r2(object_key: str, download_path: Path) -> bool:
    """Downloads a file from the configured R2 bucket to the given local path."""
    global S3_CLIENT, R2_CONFIG
    if not S3_CLIENT or not R2_CONFIG.get('bucket_name'):
        _logger.error("S3_CLIENT not initialized or R2 bucket_name not configured. Cannot download from R2.")
        return False
    
    bucket_name = R2_CONFIG['bucket_name']
    _logger.info(f"Attempting to download s3://{bucket_name}/{object_key} to {download_path}")
    try:
        # Ensure parent directory for the download path exists
        download_path.parent.mkdir(parents=True, exist_ok=True)
        
        await asyncio.to_thread(
            S3_CLIENT.download_file,
            bucket_name,
            object_key,
            str(download_path)
        )
        _logger.info(f"Successfully downloaded s3://{bucket_name}/{object_key} to {download_path}")
        return True
    except ClientError as e_ce:
        _logger.error(f"ClientError during R2 download of s3://{bucket_name}/{object_key}: {e_ce.response.get('Error', {}).get('Message', str(e_ce))}", exc_info=True)
    except Exception as e:
        _logger.error(f"Unexpected error during R2 download of s3://{bucket_name}/{object_key}: {e}", exc_info=True)
    return False

async def _upload_to_r2(object_key: str, file_path: Path) -> bool:
    """Uploads a local file to the configured R2 bucket."""
    global S3_CLIENT, R2_CONFIG
    if not S3_CLIENT or not R2_CONFIG.get('bucket_name'):
        _logger.error("S3_CLIENT not initialized or R2 bucket_name not configured. Cannot upload to R2.")
        return False
    if not file_path.is_file():
        _logger.error(f"Local file not found for R2 upload: {file_path}")
        return False

    bucket_name = R2_CONFIG['bucket_name']
    _logger.info(f"Attempting to upload {file_path} to s3://{bucket_name}/{object_key}")
    try:
        await asyncio.to_thread(
            S3_CLIENT.upload_file,
            str(file_path),
            bucket_name,
            object_key
        )
        _logger.info(f"Successfully uploaded {file_path} to s3://{bucket_name}/{object_key}")
        return True
    except ClientError as e_ce:
        _logger.error(f"ClientError during R2 upload of {file_path} to s3://{bucket_name}/{object_key}: {e_ce.response.get('Error', {}).get('Message', str(e_ce))}", exc_info=True)
    except Exception as e:
        _logger.error(f"Unexpected error during R2 upload of {file_path} to s3://{bucket_name}/{object_key}: {e}", exc_info=True)
    return False

# --- Pydantic Models for API (can be kept for structure, though not directly used by RunPod handler) ---
class InferencePayload(BaseModel):
    # serialized_aurora_batch: str = Field(..., description="Base64 encoded pickled aurora.Batch object.")
    # This field is now removed from Pydantic model and will be read directly from the request body.
    # If you have other small metadata fields the client needs to send, they can remain here.
    # For example:
    # client_job_id: Optional[str] = None
    pass # Payload might be empty if all data is in raw body, or could contain other metadata

class HealthResponse(BaseModel):
    status: str = "ok"
    model_status: str

# --- API Endpoints (These won't be directly served by RunPod in serverless mode unless a gateway is configured) ---
@app.get("/health", response_model=HealthResponse)
async def health_check():
    model_status_str = "not_loaded"
    if ir_module.INFERENCE_RUNNER:
        if ir_module.INFERENCE_RUNNER.model is not None:
            model_status_str = "loaded_and_ready"
        elif _AURORA_AVAILABLE:
            model_status_str = "model_load_failed_sdk_available"
        else:
            model_status_str = "aurora_sdk_not_available"
    elif not _AURORA_AVAILABLE:
        model_status_str = "aurora_sdk_not_available_runner_not_initialized"
    else:
        model_status_str = "runner_not_initialized_sdk_was_available"
    return HealthResponse(status="ok", model_status=model_status_str)

async def stream_gzipped_netcdf_steps(
    predictions: List[Batch], 
    input_batch_base_time: pd.Timestamp, 
    config: Dict[str, Any],
    tar_archive_path: Path # Path to write the tar.gz file to
) -> Tuple[int, Optional[str]]: # Returns (number_of_steps_archived, error_message_if_any)
    if not predictions:
        _logger.warning("No predictions to stream into archive.")
        return 0, "No predictions provided."

    forecast_steps_count = 0
    try:
        with tarfile.open(tar_archive_path, "w:gz") as tar:
            _logger.info(f"Opened tar archive {tar_archive_path} for writing.")
            for i, prediction_step_batch in enumerate(predictions):
                try:
                    step_filename = f"forecast_step_{i:02d}.nc.gz"
                    # Use the serialization function that produces gzipped NetCDF bytes directly
                    gzipped_netcdf_bytes: Optional[bytes] = None
                    with io.BytesIO() as nc_buffer:
                        # Assuming prediction_step_batch is xr.Dataset or convertible
                        # This needs robust conversion if prediction_step_batch is Aurora Batch
                        if not isinstance(prediction_step_batch, xr.Dataset):
                             # Placeholder conversion logic (similar to serialize_batch_step_to_base64_gzipped_netcdf)
                            if _AURORA_AVAILABLE and isinstance(prediction_step_batch, Batch): # type: ignore
                                _logger.warning(f"Step {i} is Aurora Batch, needs conversion to xr.Dataset for tar.gz. Placeholder conversion used.")
                                data_vars = {}
                                if hasattr(prediction_step_batch, 'data') and isinstance(prediction_step_batch.data, np.ndarray):
                                    data_vars['forecast'] = (('time', 'lat', 'lon'), prediction_step_batch.data)
                                else: data_vars['forecast'] = (('time', 'lat', 'lon'), np.random.rand(1,10,10))
                                forecast_hour_step = config.get('forecast_step_hours', 6)
                                current_step_time = input_batch_base_time + timedelta(hours=(i + 1) * forecast_hour_step)
                                coords = {'time': pd.to_datetime([current_step_time]), 'lat': np.linspace(-90,90,10), 'lon': np.linspace(0,359,10)}
                                temp_xr_step = xr.Dataset(data_vars, coords=coords)
                                temp_xr_step.to_netcdf(nc_buffer, engine="h5netcdf")
                            else:
                                _logger.error(f"Step {i} type {type(prediction_step_batch)} not xr.Dataset, cannot serialize to NetCDF.")
                                continue # Skip this step
                        else:
                             prediction_step_batch.to_netcdf(nc_buffer, engine="h5netcdf")
                        
                        nc_bytes = nc_buffer.getvalue()
                    
                    with io.BytesIO() as gz_buffer:
                        with gzip.GzipFile(fileobj=gz_buffer, mode='wb') as gz_file:
                            gz_file.write(nc_bytes)
                        gzipped_netcdf_bytes = gz_buffer.getvalue()

                    if not gzipped_netcdf_bytes:
                        _logger.warning(f"Gzipped NetCDF serialization of step {i} returned empty. Skipping.")
                        continue
                    
                    tarinfo = tarfile.TarInfo(name=step_filename)
                    tarinfo.size = len(gzipped_netcdf_bytes)
                    tarinfo.mtime = int(time.time())
                    with io.BytesIO(gzipped_netcdf_bytes) as step_fileobj:
                        tar.addfile(tarinfo, step_fileobj)
                    _logger.info(f"Added {step_filename} (size: {tarinfo.size}) to archive {tar_archive_path}.")
                    forecast_steps_count += 1

                except Exception as e_step:
                    _logger.error(f"Error processing prediction step {i} for archive: {e_step}", exc_info=True)
                    # Optionally, continue to next step or re-raise to abort
            _logger.info(f"Finished adding {forecast_steps_count} steps to archive {tar_archive_path}.")
        return forecast_steps_count, None
    except Exception as e_tar:
        _logger.error(f"Failed to create or write to tar archive {tar_archive_path}: {e_tar}", exc_info=True)
        return forecast_steps_count, str(e_tar)

async def _perform_inference_and_archive_locally(
    local_input_file_path: Path, 
    job_run_uuid: str, 
    app_config: Dict[str, Any]
) -> Dict[str, Any]:
    _logger.info(f"[{job_run_uuid}] Starting local inference and archiving with input: {local_input_file_path}")

    initial_batch: Optional[Batch] = None
    base_time_for_coords: Optional[pd.Timestamp] = None
    try:
        with open(local_input_file_path, "rb") as f:
            initial_batch = pickle.load(f)
        if not initial_batch: raise ValueError("Deserialized initial_batch is None.")
        if not (_AURORA_AVAILABLE and isinstance(initial_batch, Batch)) and not (not _AURORA_AVAILABLE and initial_batch is not None): # type: ignore
             raise ValueError(f"Deserialized object is not Aurora Batch or placeholder. Type: {type(initial_batch)}")
        if not (hasattr(initial_batch, 'metadata') and initial_batch.metadata and hasattr(initial_batch.metadata, 'time') and len(initial_batch.metadata.time) > 0):
            raise ValueError("Initial batch missing essential metadata.time for base_time determination.")
        
        base_time_for_coords = pd.to_datetime(initial_batch.metadata.time[0])
        if base_time_for_coords.tzinfo is None: base_time_for_coords = base_time_for_coords.tz_localize('UTC')
        else: base_time_for_coords = base_time_for_coords.tz_convert('UTC')
        _logger.info(f"[{job_run_uuid}] Deserialized initial_batch. Base time for coords: {base_time_for_coords.isoformat()}")
    except Exception as e_pkl:
        _logger.error(f"[{job_run_uuid}] Failed to deserialize initial_batch: {e_pkl}", exc_info=True)
        return {"error": f"Failed to load input batch: {e_pkl}"}

    prediction_steps_batch_list: Optional[List[Batch]] = None
    try:
        _logger.info(f"[{job_run_uuid}] Calling run_model_inference...")
        prediction_steps_batch_list = await run_model_inference(initial_batch, app_config)
        if prediction_steps_batch_list is None: 
            _logger.error(f"[{job_run_uuid}] run_model_inference returned None.")
            return {"output_archive_code": "NO_ARCHIVE_EMPTY_PREDICTIONS", "base_time": base_time_for_coords.isoformat() if base_time_for_coords else "ERROR_BASE_TIME_UNKNOWN", "job_run_uuid": job_run_uuid, "archive_filename": "", "num_steps_archived": 0, "info": "Inference returned no predictions."}
        if not prediction_steps_batch_list: 
            _logger.warning(f"[{job_run_uuid}] run_model_inference returned empty list.")
            return {"output_archive_code": "NO_ARCHIVE_EMPTY_PREDICTIONS", "base_time": base_time_for_coords.isoformat() if base_time_for_coords else "ERROR_BASE_TIME_UNKNOWN", "job_run_uuid": job_run_uuid, "archive_filename": "", "num_steps_archived": 0, "info": "Inference returned empty list of predictions."}
    except Exception as e_run_model:
        _logger.error(f"[{job_run_uuid}] Exception during run_model_inference: {e_run_model}", exc_info=True)
        return {"error": f"Exception during model inference: {e_run_model}"}
    _logger.info(f"[{job_run_uuid}] run_model_inference completed. Received {len(prediction_steps_batch_list)} prediction steps.")

    # Use a temporary directory on the network volume for creating the archive before sending
    network_volume_temp_outputs_base = Path("/network-volume") / "runpod_temp_outputs"
    network_volume_temp_outputs_base.mkdir(parents=True, exist_ok=True) # Ensure base exists
    
    temp_output_archive_dir = Path(tempfile.mkdtemp(prefix=f"runpod_output_{job_run_uuid}_", dir=str(network_volume_temp_outputs_base)))
    archive_filename = "forecast_archive.tar.gz"
    archive_file_to_send = temp_output_archive_dir / archive_filename
    _logger.info(f"[{job_run_uuid}] Output archive will be temporarily created at: {archive_file_to_send}")

    forecast_steps_count, error_msg_archiving = await stream_gzipped_netcdf_steps(
        predictions=prediction_steps_batch_list,
        input_batch_base_time=base_time_for_coords,
        config=app_config,
        tar_archive_path=archive_file_to_send
    )
    if error_msg_archiving or forecast_steps_count == 0:
        _logger.error(f"[{job_run_uuid}] Archiving failed or produced empty archive. Steps: {forecast_steps_count}, Error: {error_msg_archiving}")
        if temp_output_archive_dir.exists(): shutil.rmtree(temp_output_archive_dir)
        return {"error": "ARCHIVE_CREATION_FAILED", "base_time": base_time_for_coords.isoformat() if base_time_for_coords else "ERROR_BASE_TIME_UNKNOWN", "job_run_uuid": job_run_uuid, "error_details": error_msg_archiving or "No steps archived"}

    if not archive_file_to_send.exists() or archive_file_to_send.stat().st_size == 0:
        _logger.error(f"[{job_run_uuid}] Failed to create archive {archive_file_to_send}. It might be empty or not created.")
        if temp_output_archive_dir.exists():
            shutil.rmtree(temp_output_archive_dir)
        return {"error": "Archive creation failed or archive is empty.", "base_time": base_time_for_coords.isoformat() if base_time_for_coords else "ERROR_BASE_TIME_UNKNOWN", "job_run_uuid": job_run_uuid}
    
    _logger.info(f"[{job_run_uuid}] Successfully created local archive: {archive_file_to_send}")

    base_time_iso = "ERROR_BASE_TIME_UNKNOWN"
    if base_time_for_coords: base_time_iso = base_time_for_coords.isoformat()

    output_manifest = {
        "local_archive_path": str(archive_file_to_send), # Return path to local archive
        "temp_archive_parent_dir": str(temp_output_archive_dir), # Return parent dir for cleanup by caller
        "base_time": base_time_iso,
        "job_run_uuid": job_run_uuid,
        "archive_filename_on_miner": archive_filename, 
        "num_steps_archived": forecast_steps_count
    }
    _logger.info(f"[{job_run_uuid}] Local processing and archiving complete. Manifest: {output_manifest}")
    return output_manifest

async def combined_runpod_handler(job: Dict[str, Any]):
    global APP_CONFIG, INITIALIZED, EXPECTED_API_KEY, _logger, S3_CLIENT, R2_CONFIG # Added S3_CLIENT, R2_CONFIG

    job_id = job.get("id", "unknown_job_id")
    _logger.info(f"Job {job_id}: combined_runpod_handler invoked.")
    _logger.debug(f"Job {job_id}: Full raw job object received by handler: {job}") # Log entire job object

    if not INITIALIZED:
        _logger.info(f"Job {job_id}: First call or re-initialization needed. Running initialize_app_for_runpod().")
        try:
            await initialize_app_for_runpod() # initialize_app_for_runpod is already async
            INITIALIZED = True
            _logger.info(f"Job {job_id}: Application initialization completed successfully.")
        except Exception as e_init_handler:
            _logger.critical(f"Job {job_id}: CRITICAL ERROR during in-handler initialize_app_for_runpod: {e_init_handler}", exc_info=True)
            # If init fails, subsequent calls will also fail this check or the APP_CONFIG check.
            # Return an error immediately.
            return {"error": f"Server failed to initialize: {e_init_handler}"}
    else:
        _logger.info(f"Job {job_id}: Application already initialized.")

    job_input = job.get("input", {})
    _logger.debug(f"Job {job_id}: Extracted 'input' field from job object: {job_input}") # Log the extracted job_input
    
    if not job_input or not isinstance(job_input, dict):
        _logger.error(f"Job {job_id}: Invalid or missing 'input' in job payload: {job_input}")
        return {"error": "Invalid job input: 'input' field is missing or not a dictionary."}

    action_raw = job_input.get("action")
    action = str(action_raw).strip().lower() if action_raw else None # Normalize: convert to string, strip whitespace, lowercase

    _logger.info(f"Job {job_id}: Action requested (raw): {action_raw}")
    # More detailed log for debugging the action string
    _logger.info(f"Job {job_id}: Received action string (raw): ''{action_raw}'', Length: {len(action_raw) if action_raw else 0}")
    _logger.info(f"Job {job_id}: Normalized action string for comparison: ''{action}'', Length: {len(action) if action else 0}")
    # ADD MORE DETAILED LOGGING
    _logger.info(f"Job {job_id}: repr(action): {repr(action)}")
    _logger.info(f"Job {job_id}: type(action): {type(action)}")
    if action:
        _logger.info(f"Job {job_id}: action char codes (hex): {[hex(ord(c)) for c in action]}")
    # Pre-calculate the list comprehension for the target string
    target_char_codes_hex = [hex(ord(c)) for c in "run_inference_from_r2"]
    _logger.info(f"Job {job_id}: target string \'run_inference_from_r2\' char codes (hex): {target_char_codes_hex}")

    if not APP_CONFIG:
        _logger.critical(f"Job {job_id}: APP_CONFIG is NOT LOADED. This indicates a critical failure in the main startup sequence.")
        return {"error": "Server critical configuration error: APP_CONFIG not loaded."}
    
    job_run_uuid = job_input.get("job_run_uuid", str(uuid.uuid4()))
    _logger.info(f"Job {job_id}: Effective job_run_uuid for this execution: {job_run_uuid}")

    # Use network volume for temporary job data
    network_volume_base = Path("/network-volume")
    job_temp_base_dir = network_volume_base / "runpod_jobs" / job_run_uuid
    downloaded_input_file_path: Optional[Path] = None # Keep track of downloaded input for cleanup
    
    try:
        job_temp_base_dir.mkdir(parents=True, exist_ok=True)
        _logger.info(f"Job {job_id}: Created base temporary directory for job: {job_temp_base_dir}")

        # --- R2 Based Workflow ---
        if action == "run_inference_from_r2":
            _logger.info(f"Job {job_id}: Executing \'run_inference_from_r2\' action.")
            # ADD DIAGNOSTIC LOGGING FOR R2 CLIENT AND CONFIG
            _logger.info(f"Job {job_id}: R2_CLIENT_CHECK: S3_CLIENT is {'None' if S3_CLIENT is None else 'Set'}. R2_CONFIG: {R2_CONFIG}")
            if not S3_CLIENT or not R2_CONFIG.get('bucket_name'):
                _logger.error(f"Job {job_id}: R2 client not available or not configured. Cannot process \'run_inference_from_r2\'.")
                return {"error": "R2 client not configured on server."}

            input_r2_object_key = job_input.get("input_r2_object_key")
            if not input_r2_object_key:
                _logger.error(f"Job {job_id}: 'input_r2_object_key' is missing for action 'run_inference_from_r2'.")
                return {"error": "Missing 'input_r2_object_key' for R2 inference action."}

            local_input_dir = job_temp_base_dir / "input"
            local_input_dir.mkdir(parents=True, exist_ok=True)
            # Use a consistent name for the downloaded file, or derive from object_key if needed
            downloaded_input_file_path = local_input_dir / f"input_batch_{job_run_uuid}.pkl" 

            _logger.info(f"Job {job_id}: Attempting to download input file from R2: s3://{R2_CONFIG['bucket_name']}/{input_r2_object_key} to {downloaded_input_file_path}")
            download_success = await _download_from_r2(
                object_key=input_r2_object_key,
                download_path=downloaded_input_file_path
            )
            if not download_success:
                _logger.error(f"Job {job_id}: Failed to download input file from R2 (object key: {input_r2_object_key}).")
                return {"error": f"Failed to download input file from R2: {input_r2_object_key}"}
            
            _logger.info(f"Job {job_id}: Successfully downloaded input file to {downloaded_input_file_path}")

            # Perform inference using the downloaded file
            local_processing_manifest = await _perform_inference_and_archive_locally(
                local_input_file_path=downloaded_input_file_path,
                job_run_uuid=job_run_uuid,
                app_config=APP_CONFIG
            )

            if "error" in local_processing_manifest or not local_processing_manifest.get("local_archive_path"):
                _logger.error(f"Job {job_id}: Local inference/archiving failed. Manifest: {local_processing_manifest}")
                return {"error": f"Local inference/archiving failed: {local_processing_manifest.get('error', 'Unknown error')}"}

            local_archive_to_upload = Path(local_processing_manifest["local_archive_path"])
            original_archive_filename = local_processing_manifest["archive_filename_on_miner"]
            
            # Define output R2 object key
            output_r2_object_key = f"outputs/{job_run_uuid}/{original_archive_filename}" # Example structure

            _logger.info(f"Job {job_id}: Attempting to upload output archive {local_archive_to_upload} to R2: s3://{R2_CONFIG['bucket_name']}/{output_r2_object_key}")
            upload_success = await _upload_to_r2(
                object_key=output_r2_object_key,
                file_path=local_archive_to_upload
            )

            # Cleanup the locally created archive file AND its parent temp directory after attempting upload
            temp_archive_parent_dir_str = local_processing_manifest.get("temp_archive_parent_dir")
            if local_archive_to_upload.exists():
                try:
                    local_archive_to_upload.unlink() 
                    _logger.info(f"Job {job_id}: Cleaned up local output archive file {local_archive_to_upload}")
                    # Now try to remove the parent directory if its path was provided
                    if temp_archive_parent_dir_str:
                        temp_archive_parent_path = Path(temp_archive_parent_dir_str)
                        if temp_archive_parent_path.exists() and temp_archive_parent_path.is_dir():
                            shutil.rmtree(temp_archive_parent_path)
                            _logger.info(f"Job {job_id}: Cleaned up temporary archive parent directory {temp_archive_parent_path}")
                        else:
                            _logger.warning(f"Job {job_id}: Temporary archive parent directory {temp_archive_parent_dir_str} not found or not a dir for cleanup.")
                except Exception as e_clean_archive:
                    _logger.warning(f"Job {job_id}: Failed during cleanup of local output archive {local_archive_to_upload} or its parent dir: {e_clean_archive}")


            if not upload_success:
                _logger.error(f"Job {job_id}: Failed to upload output archive to R2 (object key: {output_r2_object_key}).")
                # Return what info we have, but indicate upload failure.
                return {
                    "error": f"Failed to upload output archive to R2: {output_r2_object_key}",
                    "base_time": local_processing_manifest.get("base_time"),
                    "job_run_uuid": job_run_uuid,
                    "num_steps_archived": local_processing_manifest.get("num_steps_archived"),
                }

            _logger.info(f"Job {job_id}: Successfully uploaded output archive to R2.")
            
            final_manifest = {
                "output_r2_bucket_name": R2_CONFIG['bucket_name'],
                "output_r2_object_key": output_r2_object_key,
                "base_time": local_processing_manifest.get("base_time"),
                "job_run_uuid": job_run_uuid,
                "num_steps_archived": local_processing_manifest.get("num_steps_archived"),
                "info": "Inference complete, output uploaded to R2."
            }
            _logger.info(f"Job {job_id}: R2 inference processing completed. Result manifest: {final_manifest}")
            return final_manifest

        # --- Legacy Runpodctl Workflow (kept for now, can be deprecated) ---
        elif action == "run_inference_with_runpodctl":
            _logger.warning(f"Job {job_id}: Executing DEPRECATED 'run_inference_with_runpodctl' action. Consider switching to R2 workflow.")
            input_file_code = job_input.get("input_file_code")
            original_input_filename = job_input.get("original_input_filename") # Get the original filename
            
            if not input_file_code:
                _logger.error(f"Job {job_id}: 'input_file_code' is missing for action 'run_inference_with_runpodctl'.")
                return {"error": "Missing 'input_file_code' for inference action."}
            if not original_input_filename: # Check if original_input_filename is provided
                _logger.error(f"Job {job_id}: 'original_input_filename' is missing for action 'run_inference_with_runpodctl'.")
                return {"error": "Missing 'original_input_filename' for inference action."}

            local_input_dir = job_temp_base_dir / "input"
            local_input_dir.mkdir(parents=True, exist_ok=True)
            # This is the final path where the received file (which was named original_input_filename) should end up.
            local_input_file_path = local_input_dir / "input_batch.pkl" 
            downloaded_input_file_path = local_input_file_path # For cleanup logic

            _logger.info(f"Job {job_id}: Attempting to receive input file with code {input_file_code}. Original name: '{original_input_filename}'. Target path: {local_input_file_path}")
            receive_success = await _execute_runpodctl_receive(
                code=input_file_code, 
                final_target_path=local_input_file_path,
                original_filename=original_input_filename # Pass it here
            )
            
            if not receive_success:
                _logger.error(f"Job {job_id}: Failed to receive input file using runpodctl code {input_file_code}.")
                return {"error": f"Failed to receive input file via runpodctl with code: {input_file_code}"}
            
            _logger.info(f"Job {job_id}: Successfully received input file to {local_input_file_path}")

            try:
                # This now calls the old runpodctl-specific processing function.
                # We might want to unify this if the core inference is identical.
                # For now, assume it still exists or adapt.
                # This path needs _perform_inference_and_process_with_runpodctl (the original one with runpodctl send)
                # For simplicity of this edit, assuming the old name still triggers the old logic.
                # If _perform_inference_and_process_with_runpodctl was fully replaced by _perform_inference_and_archive_locally,
                # this path will need significant rework to use runpodctl send again or be removed.
                
                # NOTE: The previous refactor renamed _perform_inference_and_process_with_runpodctl
                # to _perform_inference_and_archive_locally.
                # This deprecated path for "run_inference_with_runpodctl" will thus FAIL
                # unless we revert that or add more logic here.
                # For now, let's assume this path is fully deprecated and will be removed soon.
                # The current edit focuses on making the R2 path work.

                _logger.critical(f"Job {job_id}: The 'run_inference_with_runpodctl' action is deprecated and its dependent function was refactored. This path is non-functional.")
                return {"error": "Action 'run_inference_with_runpodctl' is deprecated and non-functional due to R2 refactoring."}

                # Placeholder for old logic if it were to be maintained:
                # runpodctl_result_manifest = await _perform_inference_and_process_with_runpodctl( # Imaginary old function
                #     local_input_file_path=local_input_file_path,
                #     job_run_uuid=job_run_uuid, 
                #     app_config=APP_CONFIG
                # )
                # _logger.info(f"Job {job_id}: Runpodctl inference processing completed. Result: {runpodctl_result_manifest}")
                # return runpodctl_result_manifest

            except Exception as e_inf:
                _logger.error(f"Job {job_id}: Unexpected error during inference processing: {e_inf}", exc_info=True)
                return {"error": f"An unexpected error occurred during inference: {str(e_inf)}"}
        else:
            _logger.warning(f"Job {job_id}: Unknown or unsupported action: {action}")
            return {"error": f"Unknown action: {action}"}

    except Exception as e_handler_main:
        _logger.error(f"Job {job_id}: Critical error in combined_runpod_handler for action '{action}': {e_handler_main}", exc_info=True)
        return {"error": f"A critical server error occurred: {str(e_handler_main)}"}
    finally:
        if job_temp_base_dir.exists():
            try:
                shutil.rmtree(job_temp_base_dir)
                _logger.info(f"Job {job_id}: Successfully cleaned up base temporary directory: {job_temp_base_dir}")
            except Exception as e_clean:
                _logger.error(f"Job {job_id}: Error during cleanup of base temporary directory {job_temp_base_dir}: {e_clean}", exc_info=True)
        
        # The `downloaded_input_file_path` under the R2 workflow is inside `job_temp_base_dir`
        # so it gets cleaned up when `job_temp_base_dir` is removed.
        # The `local_archive_to_upload` (output of local processing) is handled for cleanup within the R2 workflow block.

async def initialize_app_for_runpod():
    """Loads config, sets up logging, and initializes the inference runner."""
    global APP_CONFIG, EXPECTED_API_KEY, _logger # Ensure _logger is also treated as global if reconfigured
    
    print("[RUNPOD_INIT] Initializing application for RunPod...", flush=True)
    config_data = load_config(CONFIG_PATH) # Load into a temporary variable first
    APP_CONFIG.clear() # Clear global APP_CONFIG before updating
    APP_CONFIG.update(config_data) # Update global APP_CONFIG
    
    # Re-setup logging with file config if different from initial basicConfig
    # Pass APP_CONFIG directly, as it's now populated.
    setup_logging_from_config(APP_CONFIG) 
    _logger = logging.getLogger(__name__) # Re-assign _logger if setup_logging_from_config changes root logger behavior substantially
    _logger.info("Application startup: Configuration loaded and logging re-configured for RunPod.")

    security_config = APP_CONFIG.get('security', {})
    api_key_env_var_name = security_config.get('api_key_env_var')

    if api_key_env_var_name:
        EXPECTED_API_KEY = os.getenv(api_key_env_var_name)
        if EXPECTED_API_KEY:
            _logger.info(f"API key loaded from environment variable '{api_key_env_var_name}' (specified in config).")
        else:
            _logger.warning(f"Environment variable '{api_key_env_var_name}' (specified in config for API key) is NOT SET. API calls may be unprotected or fail.")
            EXPECTED_API_KEY = None # Ensure it's None
    else:
        # This path is taken if 'api_key_env_var' is not defined in settings.yaml -> security
        _logger.warning("'api_key_env_var' not defined in security settings in configuration file. "
                        "Cannot determine which environment variable holds the API key. API calls may be unprotected or fail.")
        EXPECTED_API_KEY = None

    # Configure runpodctl with the API key if available
    if EXPECTED_API_KEY:
        _logger.info("Attempting to configure runpodctl with the API key.")
        config_success, config_stdout, config_stderr = await _run_subprocess_command(
            ["runpodctl", "config", "--apiKey", EXPECTED_API_KEY],
            timeout_seconds=60  # Give it a reasonable timeout
        )
        if config_success:
            _logger.info(f"runpodctl config command successful. STDOUT: {config_stdout}")
            if config_stderr: # Log stderr even on success, as it might contain useful info
                 _logger.info(f"runpodctl config command STDERR: {config_stderr}")
        else:
            _logger.error(f"runpodctl config command FAILED. STDOUT: {config_stdout}, STDERR: {config_stderr}. Subsequent runpodctl operations will likely fail.")
            # Depending on strictness, you might want to prevent the app from starting
            # or raise an exception here if runpodctl is critical.
    else:
        _logger.warning("No API key available to configure runpodctl. File transfer operations will likely fail.")

    # --- Initialize R2 Client ---    
    global S3_CLIENT, R2_CONFIG # Declare usage of global S3_CLIENT and R2_CONFIG
    R2_CONFIG.clear() # Ensure R2_CONFIG is clean before attempting to populate
    S3_CLIENT = None # Ensure S3_CLIENT is None initially for this initialization attempt

    _logger.info("Attempting to initialize R2 client...")
    r2_settings = APP_CONFIG.get('r2')
    if not r2_settings:
        _logger.error("R2_INIT_FAIL: R2 configuration ('r2' section) not found in settings.yaml. R2 operations will fail.")
    else:
        _logger.info("R2_INIT_INFO: Found 'r2' section in settings.yaml.")
        r2_bucket_env_var_name = r2_settings.get('bucket_env_var')
        r2_endpoint_env_var_name = r2_settings.get('endpoint_url_env_var')
        r2_access_key_id_env_var_name = r2_settings.get('access_key_id_env_var')
        r2_secret_key_env_var_name = r2_settings.get('secret_access_key_env_var')

        _logger.info(f"R2_INIT_INFO: Bucket env var name: {r2_bucket_env_var_name}")
        _logger.info(f"R2_INIT_INFO: Endpoint env var name: {r2_endpoint_env_var_name}")
        _logger.info(f"R2_INIT_INFO: Access Key ID env var name: {r2_access_key_id_env_var_name}")
        _logger.info(f"R2_INIT_INFO: Secret Key env var name: {r2_secret_key_env_var_name}")

        if not all([r2_bucket_env_var_name, r2_endpoint_env_var_name, r2_access_key_id_env_var_name, r2_secret_key_env_var_name]):
            _logger.error("R2_INIT_FAIL: One or more R2 environment variable NAMES are missing in r2 settings (settings.yaml). R2 operations will fail.")
        else:
            _logger.info("R2_INIT_INFO: All R2 environment variable names are present in settings.yaml.")
            # Populate R2_CONFIG dictionary directly from environment variables
            bucket_name_val = os.getenv(r2_bucket_env_var_name)
            endpoint_url_val = os.getenv(r2_endpoint_env_var_name)
            access_key_id_val = os.getenv(r2_access_key_id_env_var_name)
            secret_access_key_val = os.getenv(r2_secret_key_env_var_name)

            _logger.info(f"R2_INIT_INFO: Fetched bucket_name from env ({r2_bucket_env_var_name}): '{bucket_name_val}'")
            _logger.info(f"R2_INIT_INFO: Fetched endpoint_url from env ({r2_endpoint_env_var_name}): '{endpoint_url_val}'")
            _logger.info(f"R2_INIT_INFO: Fetched aws_access_key_id from env ({r2_access_key_id_env_var_name}): '{'******' if access_key_id_val else None}'") # Mask key
            _logger.info(f"R2_INIT_INFO: Fetched aws_secret_access_key from env ({r2_secret_key_env_var_name}): '{'******' if secret_access_key_val else None}'") # Mask secret

            if not all([bucket_name_val, endpoint_url_val, access_key_id_val, secret_access_key_val]):
                missing_env_vars = []
                if not bucket_name_val: missing_env_vars.append(r2_bucket_env_var_name)
                if not endpoint_url_val: missing_env_vars.append(r2_endpoint_env_var_name)
                if not access_key_id_val: missing_env_vars.append(r2_access_key_id_env_var_name)
                if not secret_access_key_val: missing_env_vars.append(r2_secret_key_env_var_name)
                _logger.error(f"R2_INIT_FAIL: One or more R2 environment VARIABLES are not set. Missing R2 env vars: {missing_env_vars}. R2 operations will fail.")
            else:
                _logger.info("R2_INIT_INFO: All required R2 environment variables are set.")
                # Now that we've confirmed all values are present, populate R2_CONFIG
                R2_CONFIG['bucket_name'] = bucket_name_val
                R2_CONFIG['endpoint_url'] = endpoint_url_val
                R2_CONFIG['aws_access_key_id'] = access_key_id_val
                R2_CONFIG['aws_secret_access_key'] = secret_access_key_val
                
                try:
                    _logger.info(f"R2_INIT_INFO: Attempting to initialize S3 client for R2. Endpoint: {R2_CONFIG['endpoint_url']}, Bucket: {R2_CONFIG['bucket_name']}")
                    S3_CLIENT = boto3.client(
                        's3',
                        endpoint_url=R2_CONFIG['endpoint_url'],
                        aws_access_key_id=R2_CONFIG['aws_access_key_id'],
                        aws_secret_access_key=R2_CONFIG['aws_secret_access_key'],
                        config=boto3.session.Config(signature_version='s3v4'), # Important for R2
                        region_name='auto' # R2 is region-less, 'auto' is often fine or can be omitted
                    )
                    _logger.info("R2_INIT_SUCCESS: S3 client for R2 initialized successfully.")
                    # Test connection by listing buckets (optional, can be noisy, but good for debugging now)
                    # try:
                    #     response = S3_CLIENT.list_buckets()
                    #     _logger.info(f"R2_INIT_SUCCESS: Successfully listed R2 buckets: {[b['Name'] for b in response.get('Buckets', [])]}")
                    # except Exception as e_list_buckets:
                    #     _logger.warning(f"R2_INIT_WARN: S3 client initialized, but failed to list buckets: {e_list_buckets}", exc_info=True)

                except Exception as e_s3_init:
                    _logger.critical(f"R2_INIT_FAIL: CRITICAL: Failed to initialize S3 client for R2: {e_s3_init}", exc_info=True)
                    S3_CLIENT = None # Ensure client is None on failure
                    R2_CONFIG.clear() # Clear R2_CONFIG as well if client init fails

    _logger.info("Attempting to initialize inference runner for RunPod...")
    try:
        await initialize_inference_runner(APP_CONFIG) 
        if ir_module.INFERENCE_RUNNER and ir_module.INFERENCE_RUNNER.model:
            _logger.info("Inference runner initialized successfully with model for RunPod.")
        else:
            _logger.error("Inference runner or model FAILED to initialize for RunPod. Service may not be operational.")
    except Exception as e:
        _logger.critical(f"CRITICAL EXCEPTION during initialize_inference_runner for RunPod: {e}", exc_info=True)
        if ir_module:
            ir_module.INFERENCE_RUNNER = None 
        # Consider whether to raise an exception here to halt startup if model is essential

if __name__ == "__main__":
    # The __main__ block will now primarily just start the runpod handler.
    # Initialization is deferred to the first call to the handler.
    print(f"[MAIN_PY_DEBUG_MAIN_ENTRY] Entered __main__ block. __name__ is: {__name__}", flush=True)
    
    # Basic logging is set up above. The full config logging is now part of initialize_app_for_runpod.
    _logger.info("Starting RunPod serverless handler (initialization will occur on first job)...")
    print("[MAIN_PY_DEBUG_MAIN_ENTRY] About to call runpod.serverless.start(). Initialization is now lazy.", flush=True)
    runpod.serverless.start({'handler': combined_runpod_handler}) 