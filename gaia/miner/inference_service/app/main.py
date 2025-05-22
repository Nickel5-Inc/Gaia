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
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel

# --- Actual imports from local modules ---
from .data_preprocessor import prepare_input_batch_from_payload, serialize_prediction_step
from .inference_runner import (
    INFERENCE_RUNNER, # The global instance
    initialize_inference_runner,
    run_model_inference,
    BatchType as Batch # Use BatchType as Batch, or directly BatchType
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
    global APP_CONFIG, EXPECTED_API_KEY
    APP_CONFIG = load_config(CONFIG_PATH)
    setup_logging(APP_CONFIG)
    logging.info("Application startup: Configuration loaded.")

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

    try:
        await initialize_inference_runner(APP_CONFIG)
        logging.info("Inference runner initialization sequence completed.")
    except Exception as e:
        logging.error(f"Failed to initialize inference runner: {e}", exc_info=True)
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
    if INFERENCE_RUNNER:
        if hasattr(INFERENCE_RUNNER, 'model') and INFERENCE_RUNNER.model is not None:
            model_status_str = "loaded_and_ready"
        elif INFERENCE_RUNNER.model is None and _AURORA_AVAILABLE: # Check if SDK was available but model failed
            model_status_str = "model_load_failed"
        elif not _AURORA_AVAILABLE:
            model_status_str = "aurora_sdk_not_available"
    return HealthResponse(status="ok", model_status=model_status_str)

async def stream_forecast_steps(predictions: List[Any], config: Dict[str, Any]) -> AsyncGenerator[str, None]:
    if not predictions:
        logging.warning("No predictions to stream.")
        return

    for i, prediction_step in enumerate(predictions):
        try:
            serialized_step_json = await serialize_prediction_step(
                prediction_step=prediction_step,
                step_index=i,
                config=config
            )
            if serialized_step_json:
                yield serialized_step_json + "\n"
                await asyncio.sleep(0.001)
            else:
                logging.warning(f"Serialization of step {i} returned None or empty. Skipping.")
        except Exception as e:
            logging.error(f"Error during serialization or streaming of prediction step {i}: {e}")
            error_payload = json.dumps({
                "error": "Failed to serialize prediction step",
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

    logging.info("Received /run_inference request.")
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

    predictions = await run_model_inference(prepared_batch, APP_CONFIG)
    if predictions is None:
        logging.error("Inference returned no predictions or failed.")
        raise HTTPException(status_code=500, detail="Inference failed or returned no data.")

    logging.info(f"Inference successful. Streaming {len(predictions)} prediction steps.")
    return StreamingResponse(
        stream_forecast_steps(predictions, APP_CONFIG),
        media_type="application/x-ndjson"
    )

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

        async def main_init():
            await initialize_inference_runner(APP_CONFIG)
        asyncio.run(main_init())

    api_config = APP_CONFIG.get('api', {})
    host = api_config.get('host', '0.0.0.0')
    port = api_config.get('port', 8000)
    uvicorn_kwargs = api_config.get('uvicorn_kwargs', {})

    logging.info(f"Starting Uvicorn server on {host}:{port}")
    uvicorn.run("main:app", host=host, port=port, reload=False, **uvicorn_kwargs)

# Ensure __init__.py exists in the app folder if it doesn't, for Python to treat it as a package.
# For example, create an empty inference_service/app/__init__.py 