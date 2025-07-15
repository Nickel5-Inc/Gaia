print("[MAIN_PY_DEBUG_TOP] main.py parsing started.", flush=True)

# Entry point check
print("[MAIN_PY_DEBUG] Script execution started.", flush=True)

import asyncio
import base64
import logging
import os
import pickle
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional, List, Tuple

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Request, Security, status
from fastapi.responses import StreamingResponse, Response
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
from botocore.config import Config

# High-performance JSON operations for inference service
try:
    from gaia.utils.performance import dumps, loads
    from fastapi.responses import JSONResponse as _FastAPIJSONResponse
    
    class JSONResponse(_FastAPIJSONResponse):
        """Optimized JSONResponse using orjson for 2-3x faster inference service responses."""
        def render(self, content: Any) -> bytes:
            try:
                # Use high-performance orjson serialization  
                return dumps(content).encode('utf-8')
            except Exception:
                # Fallback to FastAPI's default JSON encoder
                return super().render(content)
    
    _logger = logging.getLogger(__name__)
    if os.getenv("LOG_LEVEL", "DEBUG").upper() in ["DEBUG", "INFO"]:
        _logger.info("🚀 Inference service using orjson for high-performance JSON responses")
    JSON_PERFORMANCE_AVAILABLE = True
    
except ImportError:
    from fastapi.responses import JSONResponse
    _logger = logging.getLogger(__name__)
    if os.getenv("LOG_LEVEL", "DEBUG").upper() in ["DEBUG", "INFO"]:
        _logger.info("⚡ Inference service using standard JSON - install orjson for 2-3x performance boost")
    JSON_PERFORMANCE_AVAILABLE = False

# --- Global Variables to be populated at startup --- 
APP_CONFIG = {}
EXPECTED_API_KEY = None
INITIALIZED = False # Flag for lazy initialization
S3_CLIENT = None  # Will be set to a boto3 S3 client if R2 is configured
R2_CONFIG = {}    # Will be set to a dict with R2 config if available
MODEL_HANDLER = None # Will be set to model handler if inference is enabled
INFERENCE_CONFIG = None

# Semaphore to limit concurrent R2 uploads (prevents connection pool exhaustion)
R2_UPLOAD_SEMAPHORE = asyncio.Semaphore(10)  # Reduced from 20 to 10 to prevent connection pool exhaustion

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
        # Log disk space before download
        try:
            root_stat = shutil.disk_usage("/")
            network_vol_stat = shutil.disk_usage(str(download_path.parent)) # Check space on the target volume
            _logger.info(f"Disk space before download: Root (/) - Total: {root_stat.total // (1024**3)}GB, Used: {root_stat.used // (1024**3)}GB, Free: {root_stat.free // (1024**3)}GB")
            _logger.info(f"Disk space before download: Target Vol ({download_path.parent}) - Total: {network_vol_stat.total // (1024**3)}GB, Used: {network_vol_stat.used // (1024**3)}GB, Free: {network_vol_stat.free // (1024**3)}GB")
        except Exception as e_stat:
            _logger.warning(f"Could not get disk usage stats: {e_stat}")

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
    
    # Use semaphore to limit concurrent uploads
    async with R2_UPLOAD_SEMAPHORE:
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

async def _upload_bytes_to_r2(object_key: str, data: bytes) -> bool:
    """Uploads a bytes object to the configured R2 bucket with robust retry logic."""
    global S3_CLIENT, R2_CONFIG
    if not S3_CLIENT or not R2_CONFIG.get('bucket_name'):
        _logger.error("S3_CLIENT not initialized or R2 bucket_name not configured. Cannot upload bytes to R2.")
        return False
    if not data:
        _logger.error(f"Data for R2 upload to {object_key} is empty.")
        return False

    bucket_name = R2_CONFIG['bucket_name']
    data_size_mb = len(data) / (1024 * 1024)
    _logger.info(f"Attempting to upload {data_size_mb:.2f} MB to s3://{bucket_name}/{object_key}")
    
    # Use semaphore to limit concurrent uploads
    async with R2_UPLOAD_SEMAPHORE:
        return await _robust_upload_bytes_to_r2(S3_CLIENT, bucket_name, object_key, data)

async def _robust_upload_bytes_to_r2(
    s3_client, 
    bucket_name: str, 
    object_key: str, 
    data_bytes: bytes, 
    max_retries: int = 5
) -> bool:
    """
    Robust upload to R2 with exponential backoff retry logic.
    Handles multipart upload failures gracefully.
    """
    base_delay = 2.0  # seconds
    
    for attempt in range(max_retries):
        try:
            # Use TransferConfig for better multipart handling
            from boto3.s3.transfer import TransferConfig
            
            transfer_config = TransferConfig(
                multipart_threshold=1024*1024*64,  # 64MB
                multipart_chunksize=1024*1024*8,   # 8MB chunks
                max_concurrency=5,  # Limit concurrency to prevent connection pool exhaustion
                use_threads=True,
                max_bandwidth=None
            )
            
            # Upload with transfer config
            with io.BytesIO(data_bytes) as f:
                await asyncio.to_thread(
                    s3_client.upload_fileobj,
                    f,
                    bucket_name,
                    object_key,
                    Config=transfer_config
                )
            
            # Verify upload by checking object existence
            try:
                await asyncio.to_thread(
                    s3_client.head_object,
                    Bucket=bucket_name,
                    Key=object_key
                )
                _logger.info(f"Successfully uploaded bytes to s3://{bucket_name}/{object_key}")
                return True
            except Exception as verify_error:
                _logger.warning(f"Upload verification failed for {object_key}: {verify_error}")
                # Continue to retry logic
                
        except Exception as e:
            error_msg = f"Attempt {attempt + 1}/{max_retries} failed for {object_key}: {type(e).__name__}: {str(e)}"
            
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                _logger.warning(f"{error_msg}. Retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)
                
                # Try to clean up any partial multipart uploads
                try:
                    await _cleanup_failed_multipart_uploads(s3_client, bucket_name, object_key)
                except Exception as cleanup_error:
                    _logger.debug(f"Cleanup error for {object_key} (non-critical): {cleanup_error}")
                
                continue
            else:
                _logger.error(f"All {max_retries} upload attempts failed for {object_key}. Final error: {e}")
                return False
    
    return False

async def _cleanup_failed_multipart_uploads(s3_client, bucket_name: str, object_key: str):
    """Clean up any failed multipart uploads for the given object key."""
    try:
        # List multipart uploads
        response = await asyncio.to_thread(
            s3_client.list_multipart_uploads,
            Bucket=bucket_name,
            Prefix=object_key
        )
        
        if 'Uploads' in response:
            for upload in response['Uploads']:
                upload_id = upload['UploadId']
                _logger.info(f"Aborting failed multipart upload: {upload_id} for {object_key}")
                
                await asyncio.to_thread(
                    s3_client.abort_multipart_upload,
                    Bucket=bucket_name,
                    Key=object_key,
                    UploadId=upload_id
                )
                
    except Exception as e:
        _logger.debug(f"Error during multipart cleanup for {object_key}: {e}")
        # This is non-critical, so we don't re-raise

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

async def _process_and_upload_steps(
    predictions: List[Batch],
    input_batch_base_time: pd.Timestamp,
    job_run_uuid: str,
    config: Dict[str, Any],
) -> Tuple[int, Optional[str], Optional[str]]:
    """
    Serializes prediction batches into individual NetCDF files (in memory) and uploads them to R2 in parallel.

    Returns:
        A tuple containing (number_of_steps_uploaded, output_r2_prefix, optional_error_message).
    """
    _logger.info(f"[{job_run_uuid}] Starting to process and upload {len(predictions)} prediction steps to R2.")
    forecast_step_hours = config.get('model', {}).get('forecast_step_hours', 6)
    output_r2_prefix = f"outputs/{job_run_uuid}"
    upload_tasks = []
    processing_errors = []

    for i, step_batch in enumerate(predictions):
        try:
            step_lead_hours = (i + 1) * forecast_step_hours
            
            # --- Convert aurora.Batch to xarray.Dataset ---
            data_vars = {}
            coords = {
                'lat': step_batch.metadata.lat.numpy(),
                'lon': step_batch.metadata.lon.numpy(),
                'time': np.datetime64(pd.to_datetime(step_batch.metadata.time[0]).tz_localize(None), 'ns')
            }

            for var_name, tensor in step_batch.surf_vars.items():
                data_vars[var_name] = (('lat', 'lon'), tensor.squeeze().numpy())

            if step_batch.atmos_vars:
                coords['plev'] = np.array(step_batch.metadata.atmos_levels)
                for var_name, tensor in step_batch.atmos_vars.items():
                    data_vars[var_name] = (('plev', 'lat', 'lon'), tensor.squeeze().numpy())
            
            temp_xr_step = xr.Dataset(data_vars, coords)
            
            # --- Serialize to bytes ---
            nc_bytes = temp_xr_step.to_netcdf(engine="scipy")

            if not nc_bytes:
                _logger.warning(f"[{job_run_uuid}] Step {i} produced empty netcdf bytes. Skipping upload.")
                continue
            
            # --- Define R2 key and create upload task ---
            object_key = f"{output_r2_prefix}/step_{(i+1):03d}_T+{step_lead_hours:03d}h.nc"
            upload_tasks.append(
                _upload_bytes_to_r2(object_key=object_key, data=nc_bytes)
            )

        except Exception as e_step:
            error_msg = f"Error processing prediction step {i} for R2 upload: {e_step}"
            _logger.error(f"[{job_run_uuid}] {error_msg}", exc_info=True)
            processing_errors.append(error_msg)
            # Continue processing other steps even if one fails
        
        finally:
            # Aggressive memory cleanup to prevent memory pressure
            if 'temp_xr_step' in locals(): 
                del temp_xr_step
            if 'nc_bytes' in locals(): 
                del nc_bytes
            if 'step_batch' in locals() and hasattr(step_batch, 'surf_vars'):
                # Clear tensor references to help with memory
                for var_name in list(step_batch.surf_vars.keys()):
                    if hasattr(step_batch.surf_vars[var_name], 'cpu'):
                        step_batch.surf_vars[var_name] = step_batch.surf_vars[var_name].cpu()
            # Force garbage collection every 10 steps to prevent memory buildup
            if i % 10 == 0:
                import gc
                gc.collect()

    if processing_errors:
        _logger.warning(f"[{job_run_uuid}] Encountered {len(processing_errors)} errors during processing steps. Will attempt to upload successfully processed steps.")

    if not upload_tasks:
        _logger.error(f"[{job_run_uuid}] No upload tasks were created for {len(predictions)} predictions. Processing errors: {'; '.join(processing_errors)}")
        return 0, output_r2_prefix, "No steps were successfully processed for upload."

    # --- Execute all uploads in parallel with progress monitoring ---
    _logger.info(f"[{job_run_uuid}] Submitting {len(upload_tasks)} upload tasks to R2 for prefix '{output_r2_prefix}' (max {R2_UPLOAD_SEMAPHORE._value} concurrent).")
    
    # Add timeout to prevent hanging uploads
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*upload_tasks, return_exceptions=True),
            timeout=3600  # 1 hour timeout for all uploads
        )
    except asyncio.TimeoutError:
        _logger.error(f"[{job_run_uuid}] Upload operation timed out after 1 hour")
        return 0, output_r2_prefix, "Upload operation timed out"

    # Process results
    successful_uploads = 0
    failed_uploads = 0
    failed_objects = []
    
    for i, res in enumerate(results):
        if res is True:
            successful_uploads += 1
            # Log progress every 5 successful uploads to reduce log noise
            if successful_uploads % 5 == 0 or successful_uploads == len(results):
                _logger.info(f"[{job_run_uuid}] Upload progress: {successful_uploads}/{len(upload_tasks)} completed ({(successful_uploads/len(upload_tasks)*100):.1f}%)")
        else:
            failed_uploads += 1
            failed_objects.append(f"step_{i+1:03d}")
            _logger.error(f"[{job_run_uuid}] Upload task {i+1} failed. Result/Exception: {res}")
    
    # Log summary
    if failed_uploads > 0:
        _logger.error(f"[{job_run_uuid}] Upload summary: {successful_uploads} succeeded, {failed_uploads} failed. Failed objects: {failed_objects[:5]}{'...' if len(failed_objects) > 5 else ''}")
    else:
        _logger.info(f"[{job_run_uuid}] ✅ All {successful_uploads} uploads completed successfully")
            
    num_steps_uploaded = successful_uploads

    if failed_uploads > 0:
        error_msg = f"Failed to upload all steps. Succeeded: {num_steps_uploaded}/{len(upload_tasks)}. Failures: {failed_uploads}."
        if processing_errors:
            error_msg += f" Pre-upload processing errors: {'; '.join(processing_errors)}"
        _logger.error(f"[{job_run_uuid}] {error_msg}")
        return num_steps_uploaded, output_r2_prefix, error_msg
    else:
        _logger.info(f"[{job_run_uuid}] Successfully uploaded all {num_steps_uploaded} processed steps to R2 under prefix: {output_r2_prefix}")
        final_error_message = f"Processing errors occurred but all processed steps uploaded. Details: {'; '.join(processing_errors)}" if processing_errors else None
        return num_steps_uploaded, output_r2_prefix, final_error_message

async def _perform_inference_and_upload(
    local_input_file_path: Path, 
    job_run_uuid: str, 
    app_config: Dict[str, Any]
) -> Dict[str, Any]:
    _logger.info(f"[{job_run_uuid}] Starting inference and direct R2 upload with input: {local_input_file_path}")

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
            return {"output_r2_prefix": f"outputs/{job_run_uuid}", "base_time": base_time_for_coords.isoformat() if base_time_for_coords else "ERROR_BASE_TIME_UNKNOWN", "job_run_uuid": job_run_uuid, "num_steps_uploaded": 0, "info": "Inference returned no predictions."}
        if not prediction_steps_batch_list: 
            _logger.warning(f"[{job_run_uuid}] run_model_inference returned empty list.")
            return {"output_r2_prefix": f"outputs/{job_run_uuid}", "base_time": base_time_for_coords.isoformat() if base_time_for_coords else "ERROR_BASE_TIME_UNKNOWN", "job_run_uuid": job_run_uuid, "num_steps_uploaded": 0, "info": "Inference returned empty list of predictions."}
    except Exception as e_run_model:
        _logger.error(f"[{job_run_uuid}] Exception during run_model_inference: {e_run_model}", exc_info=True)
        return {"error": f"Exception during model inference: {e_run_model}"}
    _logger.info(f"[{job_run_uuid}] run_model_inference completed. Received {len(prediction_steps_batch_list)} prediction steps.")

    # New direct-to-R2 upload logic
    num_steps, output_prefix, error_msg = await _process_and_upload_steps(
        predictions=prediction_steps_batch_list,
        input_batch_base_time=base_time_for_coords,
        job_run_uuid=job_run_uuid,
        config=app_config
    )
    
    base_time_iso = "ERROR_BASE_TIME_UNKNOWN"
    if base_time_for_coords: base_time_iso = base_time_for_coords.isoformat()

    # If there was a critical error during upload that resulted in an error message,
    # or if no steps were uploaded despite having predictions, return an error manifest.
    if error_msg and num_steps < len(prediction_steps_batch_list):
        _logger.error(f"[{job_run_uuid}] Uploading steps failed or was incomplete. Steps Uploaded: {num_steps}, Error: {error_msg}")
        return {
            "error": "UPLOAD_TO_R2_INCOMPLETE_OR_FAILED",
            "base_time": base_time_iso,
            "job_run_uuid": job_run_uuid,
            "error_details": error_msg,
            "num_steps_uploaded": num_steps,
            "output_r2_prefix": output_prefix
        }

    # Success case: all (or partially processed if that's the logic) steps are uploaded.
    output_manifest = {
        "output_r2_prefix": output_prefix,
        "base_time": base_time_iso,
        "job_run_uuid": job_run_uuid,
        "num_steps_uploaded": num_steps,
        "info": f"Upload process finished. See error details for any non-critical issues. Details: {error_msg}" if error_msg else "All steps processed and uploaded successfully."
    }
    _logger.info(f"[{job_run_uuid}] Inference and direct R2 upload finished. Manifest: {output_manifest}")
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
    network_volume_base = Path("/runpod-volume")
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
            upload_manifest = await _perform_inference_and_upload(
                local_input_file_path=downloaded_input_file_path,
                job_run_uuid=job_run_uuid,
                app_config=APP_CONFIG
            )

            if "error" in upload_manifest:
                _logger.error(f"Job {job_id}: Inference/upload process failed. Manifest: {upload_manifest}")
                # Propagate the detailed error manifest from the upload function
                return upload_manifest

            _logger.info(f"Job {job_id}: Successfully processed and uploaded inference results to R2.")
            
            final_manifest = {
                "output_r2_bucket_name": R2_CONFIG['bucket_name'],
                "output_r2_object_key_prefix": upload_manifest.get("output_r2_prefix"),
                "base_time": upload_manifest.get("base_time"),
                "job_run_uuid": job_run_uuid,
                "num_steps_archived": upload_manifest.get("num_steps_uploaded"), # Using 'num_steps_archived' for compatibility
                "info": upload_manifest.get("info", "Inference complete, outputs uploaded individually to R2.")
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
        # Comprehensive RunPod volume cleanup
        _logger.info(f"Job {job_id}: Starting comprehensive cleanup...")
        
        # Primary cleanup: Remove the job-specific temporary directory
        if job_temp_base_dir.exists():
            try:
                shutil.rmtree(job_temp_base_dir)
                _logger.info(f"Job {job_id}: Successfully cleaned up job temporary directory: {job_temp_base_dir}")
            except Exception as e_clean:
                _logger.error(f"Job {job_id}: Error during cleanup of job temporary directory {job_temp_base_dir}: {e_clean}", exc_info=True)
        
        # Additional cleanup: Remove any orphaned job directories older than 1 hour
        orphaned_cleanup_base = network_volume_base / "runpod_jobs"
        cleanup_count = 0
        try:
            if orphaned_cleanup_base.exists():
                current_time = time.time()
                for job_dir in orphaned_cleanup_base.iterdir():
                    if job_dir.is_dir():
                        try:
                            # Check if directory is older than 1 hour
                            dir_age = current_time - job_dir.stat().st_mtime
                            if dir_age > 3600:  # 1 hour
                                shutil.rmtree(job_dir)
                                cleanup_count += 1
                                _logger.debug(f"Job {job_id}: Cleaned up orphaned job directory: {job_dir}")
                        except Exception as e_orphan:
                            _logger.debug(f"Job {job_id}: Could not clean orphaned directory {job_dir}: {e_orphan}")
                
                if cleanup_count > 0:
                    _logger.info(f"Job {job_id}: Cleaned up {cleanup_count} orphaned job directories older than 1 hour")
        except Exception as e_orphan_cleanup:
            _logger.debug(f"Job {job_id}: Error during orphaned directory cleanup: {e_orphan_cleanup}")
        
        # Final cleanup: Remove runpod_jobs directory entirely if it's empty
        try:
            if orphaned_cleanup_base.exists():
                # Check if directory is empty (no files or subdirectories)
                if not any(orphaned_cleanup_base.iterdir()):
                    orphaned_cleanup_base.rmdir()
                    _logger.info(f"Job {job_id}: Removed empty runpod_jobs directory: {orphaned_cleanup_base}")
                else:
                    remaining_items = list(orphaned_cleanup_base.iterdir())
                    _logger.debug(f"Job {job_id}: runpod_jobs directory not empty, {len(remaining_items)} items remaining")
        except Exception as e_final_cleanup:
            _logger.debug(f"Job {job_id}: Error during final runpod_jobs directory cleanup: {e_final_cleanup}")
        
        # Memory cleanup: Force garbage collection to free up inference artifacts
        try:
            import gc
            collected = gc.collect()
            _logger.debug(f"Job {job_id}: Garbage collection freed {collected} objects")
        except Exception as e_gc:
            _logger.debug(f"Job {job_id}: Error during garbage collection: {e_gc}")
        
        _logger.info(f"Job {job_id}: Cleanup completed successfully")
        
        # Note: The downloaded input files and any intermediate processing files are inside 
        # job_temp_base_dir, so they get cleaned up when that directory is removed.

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
                    
                    # Configure boto3 for high-concurrency uploads with robust multipart handling
                    r2_config = Config(
                        signature_version='s3v4',
                        max_pool_connections=200,  # Increased from 50 to prevent connection pool exhaustion
                        retries={
                            'max_attempts': 5,
                            'mode': 'adaptive',
                            'timeout': 30
                        },
                        tcp_keepalive=True,
                        region_name='auto',
                        multipart_threshold=1024*1024*64,  # 64MB threshold for multipart
                        multipart_chunksize=1024*1024*8,   # 8MB chunk size
                        connect_timeout=10,
                        read_timeout=60,
                        # Use S3 Transfer configuration for better multipart handling
                        s3={
                            'multipart_threshold': 1024*1024*64,
                            'multipart_chunksize': 1024*1024*8,
                            'max_concurrency': 10,
                            'use_threads': True,
                            'max_bandwidth': None
                        }
                    )
                    
                    S3_CLIENT = boto3.client(
                        's3',
                        endpoint_url=R2_CONFIG['endpoint_url'],
                        aws_access_key_id=R2_CONFIG['aws_access_key_id'],
                        aws_secret_access_key=R2_CONFIG['aws_secret_access_key'],
                        config=r2_config
                    )
                    _logger.info("R2_INIT_SUCCESS: S3 client for R2 initialized successfully with high-concurrency config.")
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