import os
import uuid
import yaml
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
import logging
import logging.config
from contextlib import asynccontextmanager

# Assuming these modules are in the same directory (app)
from .data_preprocessor import prepare_input_batch_from_payload, serialize_forecast_data
from .inference_runner import initialize_inference_runner, run_model_inference

# --- Configuration Loading ---
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "settings.yaml")

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH
    
    if not os.path.exists(config_path):
        logging.warning(f"Config file not found at {config_path}. Using default empty config.")
        return {}
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        logging.info(f"Configuration loaded from {config_path}")
        return config_data
    except Exception as e:
        logging.error(f"Error loading configuration from {config_path}: {e}")
        return {}

APP_CONFIG = load_config()

# --- Logging Setup ---
# Basic logging setup, can be more sophisticated (e.g., from config file)
log_level = APP_CONFIG.get('logging', {}).get('level', 'INFO').upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- FastAPI Application ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    logger.info("Application startup...")
    await initialize_inference_runner(APP_CONFIG)
    static_dir = APP_CONFIG.get('data', {}).get('static_download_dir', '/app/static_data')
    os.makedirs(static_dir, exist_ok=True)
    logger.info(f"Static data directory ensured at: {static_dir}")
    logger.info("Application startup complete.")
    
    yield
    
    # Shutdown logic
    logger.info("Application shutdown...")
    # Add any cleanup logic here if needed (e.g., releasing resources)
    logger.info("Application shutdown complete.")

app = FastAPI(title="Weather Inference Service", lifespan=lifespan)

# --- Pydantic Models for API ---
class InferencePayload(BaseModel):
    gfs_timestep_1: str = Field(..., description="Base64 encoded pickled xarray Dataset for historical GFS data (e.g., T0).")
    gfs_timestep_2: str = Field(..., description="Base64 encoded pickled xarray Dataset for current GFS data (e.g., T+6).")
    # You might want to add other parameters like job_id from the caller
    # client_job_id: Optional[str] = None 

class InferenceResponse(BaseModel):
    service_job_id: str
    status: str
    message: Optional[str] = None
    forecast_data: Optional[Dict[str, Any]] = None # Or a more specific model
    error: Optional[str] = None

class HealthResponse(BaseModel):
    status: str = "ok"

# --- API Endpoints ---
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    """
    return HealthResponse()

@app.post("/run_inference", response_model=InferenceResponse)
async def handle_run_inference(payload: InferencePayload, request: Request):
    """
    Endpoint to trigger weather model inference.
    Receives GFS data, preprocesses it, runs inference, and returns the forecast.
    """
    service_job_id = str(uuid.uuid4())
    logger.info(f"Received inference request. Service Job ID: {service_job_id}")

    try:
        # 1. Prepare input batch from the payload
        logger.info(f"[{service_job_id}] Preparing input batch...")
        # The prepare_input_batch_from_payload function needs the global APP_CONFIG
        prepared_batch = await prepare_input_batch_from_payload(
            payload_data=payload.model_dump(), 
            config=APP_CONFIG
        )

        if prepared_batch is None:
            logger.error(f"[{service_job_id}] Failed to prepare input batch.")
            raise HTTPException(status_code=400, detail="Failed to prepare input batch from payload.")

        # 2. Run model inference
        logger.info(f"[{service_job_id}] Running model inference...")
        # The run_model_inference function also needs APP_CONFIG
        predictions = await run_model_inference(
            prepared_batch=prepared_batch, 
            config=APP_CONFIG
        )

        if predictions is None:
            logger.error(f"[{service_job_id}] Model inference failed or returned no predictions.")
            # Optionally, provide more details if available from the inference runner
            return InferenceResponse(
                service_job_id=service_job_id, 
                status="error", 
                error="Model inference failed or produced no output."
            )

        # 3. Serialize forecast data
        logger.info(f"[{service_job_id}] Serializing forecast data...")
        serialized_forecast = serialize_forecast_data(predictions)
        
        if "error" in serialized_forecast:
            logger.error(f"[{service_job_id}] Failed to serialize forecast data: {serialized_forecast['error']}")
            return InferenceResponse(
                service_job_id=service_job_id,
                status="error",
                error=f"Failed to serialize forecast data: {serialized_forecast['error']}"
            )

        logger.info(f"[{service_job_id}] Inference completed successfully.")
        return InferenceResponse(
            service_job_id=service_job_id,
            status="completed",
            forecast_data=serialized_forecast
        )

    except HTTPException as http_exc:
        # Re-raise HTTPException so FastAPI handles it
        raise http_exc
    except Exception as e:
        logger.error(f"[{service_job_id}] Unhandled error during inference processing: {e}", exc_info=True)
        return InferenceResponse(
            service_job_id=service_job_id,
            status="error",
            error=f"An unexpected error occurred: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    port = APP_CONFIG.get('api', {}).get('port', 8000)
    host = APP_CONFIG.get('api', {}).get('host', '0.0.0.0')
    # This is for local debugging. The Dockerfile uses uvicorn command directly.
    logger.info(f"Starting Uvicorn server locally on {host}:{port}")
    uvicorn.run(app, host=host, port=port)

# Ensure __init__.py exists in the app folder if it doesn't, for Python to treat it as a package.
# For example, create an empty inference_service/app/__init__.py 