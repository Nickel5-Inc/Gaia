import asyncio
import base64
import json
import logging
import os
import pickle
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional, List

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Request, Security, status
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
import pandas as pd
from pathlib import Path
import uuid

# --- Actual imports from local modules ---
from .data_preprocessor import prepare_input_batch_from_payload, serialize_prediction_step
from .data_preprocessor import serialize_batch_step_to_base64_gzipped_netcdf, save_dataset_step_to_gzipped_netcdf
from . import inference_runner as ir_module # Import the module itself
from .inference_runner import (
    initialize_inference_runner,
    run_model_inference,
    BatchType as Batch, # Use BatchType as Batch, or directly BatchType
    _AURORA_AVAILABLE # Import the module-level variable
)
# Note: If Aurora SDK is unavailable, BatchType will be 'Any' as defined in inference_runner.py

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

@app.post("/run_inference")
async def handle_run_inference(
    request: Request,
    auth_result: bool = Security(verify_api_key)
):
    if not auth_result:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Authentication failed")

    logging.info("Received /run_inference request for saving to Network Volume.")
    
    network_volume_base_path_str = APP_CONFIG.get('storage', {}).get('network_volume_base_path')
    if not network_volume_base_path_str:
        logging.error("Network volume base path not configured in settings.yaml (storage.network_volume_base_path)")
        raise HTTPException(status_code=500, detail="Server configuration error: Network volume path missing.")
    
    network_volume_base_path = Path(network_volume_base_path_str)
    job_run_uuid = str(uuid.uuid4())
    job_output_dir_on_volume = network_volume_base_path / job_run_uuid

    try:
        job_output_dir_on_volume.mkdir(parents=True, exist_ok=True)
        logging.info(f"Created output directory for job {job_run_uuid} at {job_output_dir_on_volume}")
    except OSError as e_mkdir:
        logging.error(f"Failed to create job output directory {job_output_dir_on_volume}: {e_mkdir}")
        raise HTTPException(status_code=500, detail=f"Server error: Could not create output directory: {e_mkdir}")

    try:
        raw_body = await request.body()
        if not raw_body:
            logging.error("Request body is empty.")
            raise HTTPException(status_code=400, detail="Request body is empty.")
        payload_str = raw_body.decode('utf-8')
    except Exception as e:
        logging.error(f"Error reading or decoding request body: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid request body: {e}")

    prepared_batch = await prepare_input_batch_from_payload(payload_str, APP_CONFIG)
    if prepared_batch is None:
        logging.error("Failed to prepare input batch from payload.")
        raise HTTPException(status_code=400, detail="Failed to process input batch.")

    input_base_time = None
    if hasattr(prepared_batch, 'metadata') and \
       hasattr(prepared_batch.metadata, 'time') and \
       prepared_batch.metadata.time is not None and \
       len(prepared_batch.metadata.time) > 0:
        try:
            input_base_time = pd.to_datetime(prepared_batch.metadata.time[-1])
            logging.info(f"Determined input_base_time from batch metadata: {input_base_time}")
        except Exception as e_time_parse:
            logging.error(f"Could not parse time from prepared_batch.metadata.time[-1]. Error: {e_time_parse}")
    
    if input_base_time is None:
        logging.error("Could not determine a valid base_time from input_batch.metadata.time.")
        # For critical failure, you might raise HTTPException here. Or proceed and try to serialize steps without it if possible.
        # However, `serialize_batch_step_to_base64_gzipped_netcdf` and thus the conversion logic needs base_time.
        raise HTTPException(status_code=500, detail="Failed to determine base time from input for forecast step processing.")

    predictions = await run_model_inference(prepared_batch, APP_CONFIG)
    if not predictions:
        logging.warning("Model inference returned no predictions.")
        # Return a manifest indicating no files if that's acceptable, or an error
        return JSONResponse(
            status_code=200, # Or 500 if no predictions is an error
            content={
                "status": "completed_no_predictions",
                "runpod_job_id_internal": job_run_uuid,
                "output_path_on_volume": job_run_uuid,
                "files": [],
                "total_steps": 0,
                "base_time": str(input_base_time)
            }
        )

    saved_files = []
    forecast_step_h = APP_CONFIG.get('model', {}).get('forecast_step_hours', 6)

    for i, prediction_step_batch_obj in enumerate(predictions):
        # Convert Batch object to xr.Dataset - adapting logic from serialize_batch_step_to_base64_gzipped_netcdf
        # This part needs careful adaptation. `serialize_batch_step_to_base64_gzipped_netcdf` does this conversion internally.
        # Let's assume we need to replicate that conversion here if `prediction_step_batch_obj` is an aurora.Batch.
        
        dataset_for_step: Optional[xr.Dataset] = None
        if _AURORA_AVAILABLE and isinstance(prediction_step_batch_obj, Batch): # type: ignore
            # Temporary conversion logic (simplified from serialize_batch_step_to_base64_gzipped_netcdf)
            # This needs to match how an xr.Dataset is formed for one step for your model.
            # You might need a dedicated function: convert_aurora_batch_step_to_dataset(batch_step, base_time, step_index, config)
            try:
                current_lead_time = pd.Timedelta(hours=(i + 1) * forecast_step_h)
                forecast_time_for_step = input_base_time + current_lead_time
                # This assumes prediction_step_batch_obj.data is already an xr.Dataset or similar
                # If it's raw tensors, more complex conversion is needed based on variable names, levels, etc.
                if hasattr(prediction_step_batch_obj, 'data') and isinstance(prediction_step_batch_obj.data, xr.Dataset):
                    dataset_for_step = prediction_step_batch_obj.data.assign_coords(
                        time=([forecast_time_for_step]),
                        lead_time=([current_lead_time])
                    ).expand_dims('time')
                    # Ensure lat/lon are correctly set if not already
                    if 'latitude' not in dataset_for_step.coords and 'lat' in dataset_for_step.dims:
                        dataset_for_step = dataset_for_step.rename({'lat': 'latitude'})
                    if 'longitude' not in dataset_for_step.coords and 'lon' in dataset_for_step.dims:
                        dataset_for_step = dataset_for_step.rename({'lon': 'longitude'})
                else:
                     logging.error(f"Prediction step {i} data is not an xr.Dataset or not structured as expected.")
                     # Handle error - skip step or fail job?
                     continue # Skip this step for now

            except Exception as e_conv:
                logging.error(f"Error converting prediction step {i} (aurora.Batch) to xr.Dataset: {e_conv}", exc_info=True)
                continue # Skip this step
        elif isinstance(prediction_step_batch_obj, xr.Dataset):
            # If run_model_inference already returns xr.Dataset steps, use them directly
            # Ensure they have the correct time/lead_time coordinates for consistency
            dataset_for_step = prediction_step_batch_obj
            # Add time/lead_time if missing (this is a basic assumption, adjust if needed)
            if 'time' not in dataset_for_step.coords:
                current_lead_time = pd.Timedelta(hours=(i + 1) * forecast_step_h)
                forecast_time_for_step = input_base_time + current_lead_time
                dataset_for_step = dataset_for_step.assign_coords(time=([forecast_time_for_step])).expand_dims('time')
            if 'lead_time' not in dataset_for_step.coords and 'time' in dataset_for_step.coords:
                 dataset_for_step = dataset_for_step.assign_coords(lead_time=('time', [dataset_for_step.time.data[0] - input_base_time]))
        else:
            logging.error(f"Prediction step {i} is neither aurora.Batch nor xr.Dataset. Type: {type(prediction_step_batch_obj)}. Cannot process.")
            continue # Skip this step

        if dataset_for_step is None:
            logging.warning(f"Dataset for step {i} could not be prepared. Skipping save.")
            continue
            
        filename = f"step_{i}.nc.gz"
        saved_path = save_dataset_step_to_gzipped_netcdf(
            dataset_step=dataset_for_step,
            output_dir=job_output_dir_on_volume,
            filename=filename,
            step_index=i
        )
        if saved_path:
            saved_files.append(filename) # Store just the filename for the manifest
        else:
            # Handle error: failed to save a step. Could abort or just log and exclude from manifest.
            logging.error(f"Failed to save step {i} to network volume. Job: {job_run_uuid}")
            # For now, we'll continue and it won't be in the manifest
    
    if not saved_files:
        logging.error(f"No forecast steps were successfully saved to network volume for job {job_run_uuid}.")
        # This could be an error state for the job
        raise HTTPException(status_code=500, detail="Failed to save any forecast steps to output volume.")

    manifest_content = {
        "status": "completed_written_to_volume",
        "runpod_job_id_internal": job_run_uuid,
        "output_path_on_volume": job_run_uuid, # Relative path for client to use
        "files": saved_files,
        "total_steps_generated": len(predictions),
        "total_steps_saved": len(saved_files),
        "base_time": str(input_base_time)
    }
    logging.info(f"Inference completed for job {job_run_uuid}. Manifest: {manifest_content}")
    return JSONResponse(content=manifest_content)

# --- New Endpoint for Downloading Files from Network Volume ---
@app.get("/download_step")
async def download_step(
    job_path_on_volume: str, # This will be the job_run_uuid
    filename: str,
    auth_result: bool = Security(verify_api_key)
):
    if not auth_result:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Authentication failed")

    network_volume_base_path_str = APP_CONFIG.get('storage', {}).get('network_volume_base_path')
    if not network_volume_base_path_str:
        logging.error("Network volume base path not configured for download.")
        raise HTTPException(status_code=500, detail="Server configuration error: Network volume path missing.")

    try:
        # Sanitize inputs to prevent path traversal
        # Ensure job_path_on_volume and filename are simple names, not full paths with '..' etc.
        if ".." in job_path_on_volume or "/" in job_path_on_volume or "\\" in job_path_on_volume:
            raise HTTPException(status_code=400, detail="Invalid job path component.")
        if ".." in filename or "/" in filename or "\\" in filename:
            raise HTTPException(status_code=400, detail="Invalid filename component.")

        file_to_serve = Path(network_volume_base_path_str) / job_path_on_volume / filename
        
        if not file_to_serve.is_file():
            logging.warning(f"Requested file not found or is not a file: {file_to_serve}")
            raise HTTPException(status_code=404, detail=f"File not found: {filename} for job {job_path_on_volume}")

        logging.info(f"Serving file: {file_to_serve}")
        # Return StreamingResponse to send the file
        # The file is already gzipped NetCDF.
        return StreamingResponse(open(file_to_serve, "rb"), media_type="application/octet-stream", headers={
            "Content-Disposition": f"attachment; filename=\"{filename}\""
        })

    except HTTPException: # Re-raise known HTTP exceptions
        raise
    except Exception as e:
        logging.error(f"Error serving file {job_path_on_volume}/{filename}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error serving file.")

async def main_init():
    await initialize_inference_runner(APP_CONFIG)

if __name__ == "__main__":
    if not APP_CONFIG:
        APP_CONFIG = load_config(CONFIG_PATH)
        setup_logging(APP_CONFIG)
        
        current_expected_key = os.getenv("INFERENCE_SERVICE_API_KEY")
        if not current_expected_key:
            current_expected_key = APP_CONFIG.get('security', {}).get('api_key')
        EXPECTED_API_KEY = current_expected_key

        if not EXPECTED_API_KEY:
            logging.warning(
            "Running directly: INFERENCE_SERVICE_API_KEY is not set. Endpoint /run_inference is UNPROTECTED."
        )
        else:
            logging.info("Running directly: API key protection is CONFIGURED for /run_inference.")

        asyncio.run(main_init())

    api_config = APP_CONFIG.get('api', {})
    host = api_config.get('host', '0.0.0.0')
    port = api_config.get('port', 8000)
    uvicorn_kwargs = api_config.get('uvicorn_kwargs', {})

    logging.info(f"Starting Uvicorn server on {host}:{port}")
    uvicorn.run("main:app", host=host, port=port, reload=False, **uvicorn_kwargs)

# Ensure __init__.py exists in the app folder if it doesn't, for Python to treat it as a package.
# For example, create an empty inference_service/app/__init__.py 