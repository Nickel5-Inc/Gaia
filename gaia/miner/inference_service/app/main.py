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

# --- Logging Configuration ---
# Default to INFO, allow override via LOG_LEVEL environment variable (e.g., DEBUG, WARNING, ERROR)
_log_level_str = os.getenv("LOG_LEVEL", "DEBUG").upper()
_log_level = getattr(logging, _log_level_str, logging.DEBUG) # Also ensure default for getattr is DEBUG

# Configure root logger
# This basicConfig call should be one of the first things in your script.
# It ensures that all log messages of the configured level and above
# are directed to standard output (or standard error, depending on the level)
# which Docker will then pick up.
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    stream=sys.stdout  # Explicitly set stream to stdout
)

# Example: If the runpod library itself has a logger and you want to control its verbosity
# logging.getLogger("runpod").setLevel(_log_level) # Or a different level like logging.WARNING

_logger = logging.getLogger(__name__) # Logger for this specific module
_logger.info(f"Logging configured with level: {_log_level_str}")
# --- End Logging Configuration ---

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

# --- Configuration Loading ---
CONFIG_PATH = os.getenv("INFERENCE_CONFIG_PATH", "config/settings.yaml")
APP_CONFIG: Dict[str, Any] = {}

def load_config(path: str) -> Dict[str, Any]:
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
        if not isinstance(config, dict):
            raise ValueError("Config is not a dictionary")
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {path}. Using empty config.")
        return {}
    except Exception as e:
        logging.error(f"Error loading or parsing configuration from {path}: {e}")
        return {}

# --- Logging Setup ---
def setup_logging(config: Dict[str, Any]):
    log_config = config.get('logging', {})
    level = log_config.get('level', 'INFO').upper()
    fmt = log_config.get('format', '%asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.basicConfig(level=level, format=fmt)

# --- API Key Authentication ---
API_KEY_NAME = "X-API-Key"
api_key_header_auth = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

EXPECTED_API_KEY = os.getenv("INFERENCE_SERVICE_API_KEY")
# Fallback to config is handled in lifespan after APP_CONFIG is loaded

async def verify_api_key(api_key_header: Optional[str] = Security(api_key_header_auth)):
    if not EXPECTED_API_KEY:
        logging.debug("No API key configured on server, allowing request without auth.")
        return True

    if api_key_header is None:
        logging.warning("Missing X-API-Key header in request.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API Key in X-API-Key header",
        )
    if api_key_header == EXPECTED_API_KEY:
        return True
    else:
        logging.warning("Invalid API Key received.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
        )

# --- FastAPI Application ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global APP_CONFIG, EXPECTED_API_KEY # , INFERENCE_RUNNER <-- REMOVE from global declaration here if only accessed via ir_module
    # Early log to confirm lifespan start, before logger is fully configured by setup_logging
    print("[LIFESPAN_DEBUG] Lifespan event started.", flush=True)
    
    APP_CONFIG = load_config(CONFIG_PATH)
    setup_logging(APP_CONFIG)
    logging.info("Application startup: Configuration loaded.")
    # Test the logger immediately after setup
    logging.debug("[LIFESPAN_DEBUG] Logger configured by setup_logging.")

    current_expected_key = os.getenv("INFERENCE_SERVICE_API_KEY")
    if not current_expected_key:
        current_expected_key = APP_CONFIG.get('security', {}).get('api_key')
    
    EXPECTED_API_KEY = current_expected_key # Set the global after full evaluation

    if not EXPECTED_API_KEY:
        logging.warning(
            "INFERENCE_SERVICE_API_KEY is not set (neither via environment variable nor in config file). "
            "The /run_inference endpoint will be UNPROTECTED."
        )
    else:
        logging.info("API key protection is CONFIGURED for /run_inference.")

    logging.info("[LIFESPAN_DEBUG] Attempting to initialize inference runner...")
    print(f"[MAIN_PY_DEBUG] Type of initialize_inference_runner to be called: {type(initialize_inference_runner)}", flush=True)
    
    # ---- Print IDs before call ----
    print(f"[MAIN_PY_DEBUG_ID_BEFORE] id(ir_module): {id(ir_module)}", flush=True)
    print(f"[MAIN_PY_DEBUG_ID_BEFORE] id(INFERENCE_RUNNER global var in main.py): {id(ir_module.INFERENCE_RUNNER)}", flush=True) # Access via ir_module
    print(f"[MAIN_PY_DEBUG_ID_BEFORE] Value of INFERENCE_RUNNER in main.py: {ir_module.INFERENCE_RUNNER}", flush=True) # Access via ir_module

    try:
        await initialize_inference_runner(APP_CONFIG)
        logging.info("[LIFESPAN_DEBUG] initialize_inference_runner call completed.")
        
        # ---- Print IDs after call ----
        print(f"[MAIN_PY_DEBUG_ID_AFTER] id(ir_module): {id(ir_module)}", flush=True)
        print(f"[MAIN_PY_DEBUG_ID_AFTER] id(INFERENCE_RUNNER global var in main.py): {id(ir_module.INFERENCE_RUNNER)}", flush=True) # Access via ir_module
        print(f"[MAIN_PY_DEBUG_ID_AFTER] Value of INFERENCE_RUNNER in main.py: {ir_module.INFERENCE_RUNNER}", flush=True) # Access via ir_module

        if ir_module.INFERENCE_RUNNER is not None and ir_module.INFERENCE_RUNNER.model is not None: # Access via ir_module
            logging.info("[LIFESPAN_DEBUG] Global INFERENCE_RUNNER is set and model is loaded.")
        elif ir_module.INFERENCE_RUNNER is not None and ir_module.INFERENCE_RUNNER.model is None: # Access via ir_module
            logging.warning("[LIFESPAN_DEBUG] Global INFERENCE_RUNNER is set, but model is NOT loaded.")
        else: # ir_module.INFERENCE_RUNNER is None
            logging.error("[LIFESPAN_DEBUG] Global INFERENCE_RUNNER is still None after initialization attempt.")
    except Exception as e:
        logging.error(f"[LIFESPAN_DEBUG] EXCEPTION during initialize_inference_runner call: {e}", exc_info=True)
    yield
    logging.info("Application shutdown.")

app = FastAPI(title="Weather Inference Service", lifespan=lifespan)

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
        logging.debug(f"Running subprocess command: {' '.join(command_args)}")
        process = await asyncio.to_thread(
            subprocess.run,
            command_args,
            capture_output=True,
            text=True,
            check=False, # Don't raise exception on non-zero exit; we'll check returncode
            timeout=timeout_seconds
        )
        if process.returncode == 0:
            logging.debug(f"Subprocess command successful. STDOUT: {process.stdout[:200]}...") # Log snippet
            return True, process.stdout.strip(), process.stderr.strip()
        else:
            logging.error(f"Subprocess command failed with return code {process.returncode}. COMMAND: {' '.join(command_args)}")
            logging.error(f"STDERR: {process.stderr}")
            logging.error(f"STDOUT: {process.stdout}")
            return False, process.stdout.strip(), process.stderr.strip()
    except subprocess.TimeoutExpired:
        logging.error(f"Subprocess command timed out after {timeout_seconds} seconds. COMMAND: {' '.join(command_args)}")
        return False, "", "TimeoutExpired"
    except Exception as e:
        logging.error(f"Exception during subprocess command {' '.join(command_args)}: {e}", exc_info=True)
        return False, "", str(e)

# --- RunPodctl Helper Functions ---
async def _execute_runpodctl_receive(code: str, output_path: Path, timeout_seconds: int = 600) -> bool:
    """Receives a file using runpodctl."""
    # Ensure parent directory for output_path exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = ["runpodctl", "receive", code, "-o", str(output_path)]
    logging.info(f"Attempting to receive file with runpodctl, code: {code}, output: {output_path}")
    success, stdout, stderr = await _run_subprocess_command(command, timeout_seconds=timeout_seconds)
    if success:
        logging.info(f"runpodctl receive successful for code {code} to {output_path}. Stdout: {stdout}")
        return True
    else:
        logging.error(f"runpodctl receive failed for code {code}. Stdout: {stdout}, Stderr: {stderr}")
        return False

async def _execute_runpodctl_send(file_path: Path, timeout_seconds: int = 600) -> Optional[str]:
    """Sends a file using runpodctl and returns the one-time code."""
    if not file_path.is_file():
        logging.error(f"[RunpodctlSend] File not found for sending: {file_path}")
        return None
    command = ["runpodctl", "send", str(file_path)]
    logging.info(f"Attempting to send file with runpodctl: {file_path}")
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
                logging.info(f"runpodctl send successful for {file_path}. Code: {code}. Full stdout: {stdout}")
                return code
        logging.error(f"runpodctl send for {file_path} appeared successful but code line not found in stdout: {stdout}")
        return None
    else:
        logging.error(f"runpodctl send failed for {file_path}. Stdout: {stdout}, Stderr: {stderr}")
        return None

# --- Pydantic Models for API ---
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

# --- API Endpoints ---
@app.get("/health", response_model=HealthResponse)
async def health_check():
    model_status_str = "not_loaded"
    if ir_module.INFERENCE_RUNNER: # Access via ir_module
        if ir_module.INFERENCE_RUNNER.model is not None: # Access via ir_module
            model_status_str = "loaded_and_ready"
        # Now use the imported _AURORA_AVAILABLE directly
        elif ir_module.INFERENCE_RUNNER.model is None and _AURORA_AVAILABLE: # Access via ir_module
            model_status_str = "model_load_failed_sdk_available"
        elif not _AURORA_AVAILABLE: # SDK was never available
            model_status_str = "aurora_sdk_not_available"
        else: # Should not be reached if logic is correct, but as a fallback
            model_status_str = "sdk_status_unknown_model_not_loaded"
    elif not _AURORA_AVAILABLE: # ir_module.INFERENCE_RUNNER might not even be attempted if SDK is missing from start
        model_status_str = "aurora_sdk_not_available_runner_not_initialized"
    else: # ir_module.INFERENCE_RUNNER is None but _AURORA_AVAILABLE is True (or was True)
        model_status_str = "runner_not_initialized_sdk_was_available"
    
    return HealthResponse(status="ok", model_status=model_status_str)

async def stream_gzipped_netcdf_steps(
    predictions: List[Batch], 
    input_batch_base_time: Any, # The base time from the input batch, e.g., input_batch.metadata.time[0]
    config: Dict[str, Any]
) -> AsyncGenerator[str, None]:
    if not predictions:
        logging.warning("No predictions to stream.")
        return

    for i, prediction_step_batch in enumerate(predictions):
        try:
            # Use the new serialization function
            base64_gzipped_netcdf_str = serialize_batch_step_to_base64_gzipped_netcdf(
                batch_step=prediction_step_batch,
                step_index=i,
                base_time=input_batch_base_time, # Pass the base_time here
                config=config
            )
            if base64_gzipped_netcdf_str:
                yield base64_gzipped_netcdf_str + "\n"
                await asyncio.sleep(0.001) # Small sleep to allow other tasks, crucial for streaming
            else:
                logging.warning(f"Serialization of step {i} to gzipped NetCDF returned None or empty. Skipping.")
                # Optionally, yield an error message for this step
                error_payload = json.dumps({"error": f"Failed to serialize prediction step {i} to gzipped NetCDF", "step_index": i})
                yield error_payload + "\n"
        except Exception as e:
            logging.error(f"Error during serialization or streaming of prediction step {i} (gzipped NetCDF): {e}")
            error_payload = json.dumps({
                "error": "Failed to serialize or stream prediction step (gzipped NetCDF)",
                "step_index": i,
                "detail": str(e)
            })
            yield error_payload + "\n"

async def _perform_inference_and_process_with_runpodctl(
    local_input_file_path: Path, 
    job_run_uuid: str, # For logging and structuring output paths
    app_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Performs inference using the input file, streams serialized+gzipped forecast steps 
    directly into a single .tar.gz archive on a network volume, sends the archive via runpodctl,
    and returns a manifest with the runpodctl code and base_time.
    """
    logging.info(f"[{job_run_uuid}] Starting inference and archiving process with input: {local_input_file_path}")

    # --- 1. Deserialize Input Batch ---
    initial_batch: Optional[Batch] = None
    base_time_dt_for_coords: Optional[pd.Timestamp] = None # For forecast time calculation
    try:
        with open(local_input_file_path, "rb") as f:
            initial_batch = pickle.load(f)
        if not initial_batch:
            raise ValueError("Deserialized initial_batch is None.")
        if not (_AURORA_AVAILABLE and isinstance(initial_batch, Batch)) and not (not _AURORA_AVAILABLE and initial_batch is not None) : # type: ignore
             raise ValueError(f"Deserialized object is not an Aurora Batch or placeholder. Type: {type(initial_batch)}")

        if not (hasattr(initial_batch, 'metadata') and initial_batch.metadata and 
                hasattr(initial_batch.metadata, 'time') and len(initial_batch.metadata.time) > 0):
            raise ValueError("Initial batch missing essential metadata.time for base_time determination.")
        
        # Store the base time (first timestamp) from the input batch metadata for coordinate calculation
        base_time_dt_for_coords = pd.to_datetime(initial_batch.metadata.time[0])
        if base_time_dt_for_coords.tzinfo is None:
            base_time_dt_for_coords = base_time_dt_for_coords.tz_localize('UTC')
        else:
            base_time_dt_for_coords = base_time_dt_for_coords.tz_convert('UTC')

        logging.info(f"[{job_run_uuid}] Successfully deserialized initial_batch from {local_input_file_path}. Base time for coords: {base_time_dt_for_coords.isoformat()}")
    except Exception as e_pkl:
        logging.error(f"[{job_run_uuid}] Failed to deserialize initial_batch from {local_input_file_path}: {e_pkl}", exc_info=True)
        return {"error": f"Failed to load input batch: {e_pkl}"}

    # --- 2. Run Model Inference ---
    # This now returns a List[BatchType] or None
    prediction_steps_batch_list: Optional[List[Batch]] = None
    try:
        logging.info(f"[{job_run_uuid}] Calling run_model_inference...")
        # Pass initial_batch (which is BatchType) and app_config
        prediction_steps_batch_list = await run_model_inference(initial_batch, app_config) 
        if prediction_steps_batch_list is None:
            logging.error(f"[{job_run_uuid}] run_model_inference returned None. Inference failed or produced no output.")
            return {"error": "Inference process failed or produced no output."}
        if not prediction_steps_batch_list: # Empty list
            logging.warning(f"[{job_run_uuid}] run_model_inference returned an empty list of predictions.")
            # This might be acceptable depending on the model/input, but for now, we treat it as needing an empty archive.
    except Exception as e_run_model:
        logging.error(f"[{job_run_uuid}] Exception during run_model_inference: {e_run_model}", exc_info=True)
        return {"error": f"Exception during model inference execution: {e_run_model}"}

    logging.info(f"[{job_run_uuid}] run_model_inference completed. Received {len(prediction_steps_batch_list)} prediction steps (BatchType).")

    # --- 3. Prepare for Output Archiving ---
    network_volume_base_path_str = app_config.get('storage', {}).get('network_volume_base_path')
    if not network_volume_base_path_str:
        logging.error(f"[{job_run_uuid}] Network volume base path not configured in app_config.")
        return {"error": "Server configuration error: Network volume path missing."}

    job_output_dir_on_volume = Path(network_volume_base_path_str) / "job_outputs" / job_run_uuid
    job_output_dir_on_volume.mkdir(parents=True, exist_ok=True)
    archive_filename = "forecast_archive.tar.gz"
    archive_file_on_volume = job_output_dir_on_volume / archive_filename
    
    logging.info(f"[{job_run_uuid}] Output archive will be saved to: {archive_file_on_volume}")

    # --- 3. Iterative Inference and Streaming to Archive ---
    forecast_steps_count = 0
    try:
        with tarfile.open(archive_file_on_volume, "w:gz") as tar:
            logging.info(f"[{job_run_uuid}] Opened tar archive {archive_file_on_volume} for writing.")
            
            # The run_model_inference function is expected to be an async generator yielding xr.Dataset steps
            async for step_index, prediction_step_xr in run_model_inference(initial_batch, app_config):
                if prediction_step_xr is None:
                    logging.warning(f"[{job_run_uuid}] run_model_inference yielded None for step {step_index}. Skipping.")
                    continue

                logging.info(f"[{job_run_uuid}] Processing forecast step {step_index}...")
                
                # Serialize xr.Dataset to in-memory gzipped NetCDF bytes
                step_filename = f"forecast_step_{step_index:02d}.nc.gz"
                gzipped_netcdf_bytes: Optional[bytes] = None
                try:
                    with io.BytesIO() as nc_buffer:
                        prediction_step_xr.to_netcdf(nc_buffer, engine="h5netcdf") # or netcdf4
                        nc_bytes = nc_buffer.getvalue()
                    
                    with io.BytesIO() as gz_buffer:
                        with gzip.GzipFile(fileobj=gz_buffer, mode='wb') as gz_file:
                            gz_file.write(nc_bytes)
                        gzipped_netcdf_bytes = gz_buffer.getvalue()
                    
                    if not gzipped_netcdf_bytes:
                         raise ValueError("Gzipped NetCDF bytes are empty.")
                         
                    logging.debug(f"[{job_run_uuid}] Serialized and gzipped step {step_index} to {len(gzipped_netcdf_bytes)} bytes.")
                except Exception as e_ser_gz:
                    logging.error(f"[{job_run_uuid}] Failed to serialize/gzip step {step_index}: {e_ser_gz}", exc_info=True)
                    # Decide: skip this step or abort? For now, skip.
                    continue 
                
                # Add to Tar Archive
                tarinfo = tarfile.TarInfo(name=step_filename)
                tarinfo.size = len(gzipped_netcdf_bytes)
                tarinfo.mtime = int(time.time()) # Set modification time
                
                with io.BytesIO(gzipped_netcdf_bytes) as step_fileobj:
                    tar.addfile(tarinfo, step_fileobj)
                
                logging.info(f"[{job_run_uuid}] Added {step_filename} (size: {tarinfo.size}) to archive.")
                forecast_steps_count += 1
                
                # Clean up explicit references to potentially large objects
                del prediction_step_xr, gzipped_netcdf_bytes, nc_bytes
                # Consider gc.collect() if memory becomes an issue after many steps, but usually not needed.

        logging.info(f"[{job_run_uuid}] Finished processing all forecast steps. Added {forecast_steps_count} steps to archive {archive_file_on_volume}.")

    except Exception as e_inf_arc:
        logging.error(f"[{job_run_uuid}] Error during inference or tarball creation: {e_inf_arc}", exc_info=True)
        # Cleanup potentially incomplete archive
        if archive_file_on_volume.exists():
            try: archive_file_on_volume.unlink()
            except OSError: pass
        return {"error": f"Error during inference/archiving: {e_inf_arc}"}

    if forecast_steps_count == 0:
        logging.warning(f"[{job_run_uuid}] No forecast steps were added to the archive. Archive might be empty or not created.")
        # Depending on requirements, this might be an error or an expected outcome (e.g., no prediction for this input)
        # For now, let's assume it's an error if the archive is expected.
        return {"error": "No forecast steps generated or saved to archive."}

    # --- 4. Send Archive via runpodctl ---
    logging.info(f"[{job_run_uuid}] Attempting to send archive {archive_file_on_volume} via runpodctl.")
    output_archive_code = await _execute_runpodctl_send(archive_file_on_volume)

    if not output_archive_code:
        logging.error(f"[{job_run_uuid}] Failed to send archive {archive_file_on_volume} via runpodctl.")
        return {"error": "Failed to send output archive using runpodctl."}
    
    logging.info(f"[{job_run_uuid}] Successfully sent archive. Runpodctl code: {output_archive_code}")

    # --- 5. Return Manifest ---
    base_time_str = None
    try:
        # initial_batch.metadata.time is typically a numpy array of datetime64
        # We need to convert it to a Python datetime, then to ISO string
        pd_timestamp = pd.to_datetime(initial_batch.metadata.time[0])
        py_datetime = pd_timestamp.to_pydatetime(warn=False) # warn=False if already Python datetime
        if py_datetime.tzinfo is None: # Ensure timezone awareness (UTC)
            py_datetime = py_datetime.replace(tzinfo=timezone.utc)
        base_time_str = py_datetime.isoformat()
    except Exception as e_time:
        logging.error(f"[{job_run_uuid}] Could not extract or format base_time from initial_batch: {e_time}", exc_info=True)
        # Fallback or error, for now, return error as base_time is crucial for WeatherTask
        return {"error": "Failed to determine base_time for the forecast."}

    output_manifest = {
        "output_archive_code": output_archive_code,
        "base_time": base_time_str, # ISO format string
        "job_run_uuid": job_run_uuid, # For WeatherTask to cross-reference
        "archive_filename": archive_filename, # Inform WeatherTask of the filename
        "num_steps_archived": forecast_steps_count
    }
    
    logging.info(f"[{job_run_uuid}] Successfully processed and sent archive. Manifest: {output_manifest}")
    return output_manifest

# --- FastAPI Network Volume Endpoints (TO BE REMOVED/REPLACED) ---
# The following endpoints /run_inference, /upload_input, /download_step 
# will be removed or heavily modified as their logic is moving into the combined_runpod_handler
# and _perform_inference_and_process_with_runpodctl.
# For now, we will comment them out. Consider removing them entirely later.

# @app.post("/run_inference")
# async def handle_run_inference(
#     request: Request, 
#     auth_result: bool = Security(verify_api_key)
# ):
#     # ... (previous implementation) ...
#     pass

# @app.post("/upload_input")
# async def handle_upload_input(
#     request: Request, # We'll read raw body
#     auth_result: bool = Security(verify_api_key)
# ):
#     # ... (previous implementation) ...
#     pass

# @app.get("/download_step")
# async def download_step(
#     job_path_on_volume: str, # This will be the job_run_uuid
#     filename: str,
#     auth_result: bool = Security(verify_api_key)
# ):
#     # ... (previous implementation) ...
#     pass 

async def main_init():
    # This function seems to be a remnant and not used. 
    # Lifespan handles initialization.
    # If it has a specific purpose, it should be clarified or integrated.
    logging.info("main_init called - check if this is still needed.")
    pass

async def combined_runpod_handler(job: Dict[str, Any]):
    """
    Main handler for RunPod serverless invocations.
    Dispatches actions based on job_input.
    """
    job_id = job.get("id", "unknown_job_id")
    job_input = job.get("input", {})
    
    if not job_input or not isinstance(job_input, dict):
        logging.error(f"Job {job_id}: Invalid or missing 'input' in job payload: {job_input}")
        return {"error": "Invalid job input: 'input' field is missing or not a dictionary."}

    action = job_input.get("action")
    logging.info(f"Job {job_id}: Action requested: {action}")

    # Ensure APP_CONFIG is loaded (it should be by lifespan, but good check)
    if not APP_CONFIG:
        logging.error(f"Job {job_id}: APP_CONFIG not loaded. Lifespan issue?")
        return {"error": "Server configuration error: APP_CONFIG not loaded."}

    # Create a unique ID for this specific execution run if not provided
    # This will be used for naming temporary directories.
    # WeatherTask should ideally provide this so it can correlate logs and outputs.
    job_run_uuid = job_input.get("job_run_uuid", str(uuid.uuid4()))
    logging.info(f"Job {job_id}: Effective job_run_uuid for this execution: {job_run_uuid}")

    # Define base temporary directory for this specific job run
    # This helps in organizing and cleaning up files.
    job_temp_base_dir = Path(tempfile.gettempdir()) / "runpod_jobs" / job_run_uuid
    try:
        job_temp_base_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Job {job_id}: Created base temporary directory: {job_temp_base_dir}")

        if action == "run_inference_with_runpodctl":
            logging.info(f"Job {job_id}: Executing 'run_inference_with_runpodctl' action.")
            input_file_code = job_input.get("input_file_code")
            
            if not input_file_code:
                logging.error(f"Job {job_id}: 'input_file_code' is missing for action 'run_inference_with_runpodctl'.")
                return {"error": "Missing 'input_file_code' for inference action."}

            local_input_dir = job_temp_base_dir / "input"
            local_input_dir.mkdir(parents=True, exist_ok=True)
            # Define a fixed name for the received input file for simplicity
            local_input_file_path = local_input_dir / "input_batch.pkl"

            logging.info(f"Job {job_id}: Attempting to receive input file with code {input_file_code} to {local_input_file_path}")
            receive_success = await _execute_runpodctl_receive(input_file_code, local_input_file_path)
            
            if not receive_success:
                logging.error(f"Job {job_id}: Failed to receive input file using runpodctl code {input_file_code}.")
                return {"error": f"Failed to receive input file via runpodctl with code: {input_file_code}"}
            
            logging.info(f"Job {job_id}: Successfully received input file to {local_input_file_path}")

            try:
                # The _perform_inference_and_process_with_runpodctl function handles its own temp output dirs based on job_run_uuid
                # and returns the manifest with codes for archives.
                inference_result_manifest = await _perform_inference_and_process_with_runpodctl(
                    local_input_file_path=local_input_file_path,
                    job_run_uuid=job_run_uuid, # Pass the consistent UUID
                    app_config=APP_CONFIG
                )
                logging.info(f"Job {job_id}: Inference processing completed. Result manifest: {inference_result_manifest}")
                return inference_result_manifest # This is the success output for the RunPod job
            except RuntimeError as e_rt:
                logging.error(f"Job {job_id}: Runtime error during inference processing: {e_rt}", exc_info=True)
                return {"error": f"Runtime error during inference: {str(e_rt)}"}
            except ValueError as e_val:
                logging.error(f"Job {job_id}: Value error during inference processing: {e_val}", exc_info=True)
                return {"error": f"Value error during inference: {str(e_val)}"}
            except FileNotFoundError as e_fnf:
                logging.error(f"Job {job_id}: File not found error during inference processing: {e_fnf}", exc_info=True)
                return {"error": f"File not found error during inference: {str(e_fnf)}"}
            except Exception as e_inf:
                logging.error(f"Job {job_id}: Unexpected error during inference processing: {e_inf}", exc_info=True)
                return {"error": f"An unexpected error occurred during inference: {str(e_inf)}"}

        # elif action == "upload_input": # REMOVED
        #     logging.info("DEPRECATED ACTION: 'upload_input' called, no longer supported.")
        #     return {"error": "Action 'upload_input' is deprecated and no longer supported."}

        # elif action == "download_archive": # REMOVED
        #     logging.info("DEPRECATED ACTION: 'download_archive' called, no longer supported.")
        #     return {"error": "Action 'download_archive' is deprecated and no longer supported."}

        else:
            logging.warning(f"Job {job_id}: Unknown or unsupported action: {action}")
            return {"error": f"Unknown action: {action}"}

    except Exception as e_handler_main:
        logging.error(f"Job {job_id}: Critical error in combined_runpod_handler for action '{action}': {e_handler_main}", exc_info=True)
        return {"error": f"A critical server error occurred: {str(e_handler_main)}"}
    finally:
        # Cleanup: Remove the job-specific base temporary directory and all its contents
        if job_temp_base_dir.exists():
            try:
                shutil.rmtree(job_temp_base_dir)
                logging.info(f"Job {job_id}: Successfully cleaned up temporary directory: {job_temp_base_dir}")
            except Exception as e_clean:
                logging.error(f"Job {job_id}: Error during cleanup of temporary directory {job_temp_base_dir}: {e_clean}", exc_info=True)
        else:
            logging.info(f"Job {job_id}: Temporary directory {job_temp_base_dir} not found for cleanup (may have failed before creation or cleaned by sub-function).")

if __name__ == "__main__":

    # Correct entry point for RunPod Serverless GPU/CPU Docker image:
    logging.info("Starting RunPod serverless handler...")
    runpod.serverless.start({'handler': combined_runpod_handler}) 