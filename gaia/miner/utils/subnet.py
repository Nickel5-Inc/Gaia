from functools import partial
from fastapi import Depends, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.routing import APIRouter
from pydantic import BaseModel
from fiber.encrypted.miner.dependencies import blacklist_low_stake, verify_request
from fiber.encrypted.miner.security.encryption import decrypt_general_payload
from fiber.logging_utils import get_logger
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
import numpy as np
from datetime import datetime, timezone
from gaia.tasks.defined_tasks.soilmoisture.soil_inputs import SoilMoisturePayload
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask
from gaia.tasks.defined_tasks.soilmoisture.soil_outputs import SoilMoisturePrediction
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
import traceback
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
import json
from pydantic import ValidationError
import os
from pathlib import Path

# Define max request size (5MB in bytes)
MAX_REQUEST_SIZE = 5 * 1024 * 1024  # 5MB

logger = get_logger(__name__)

# --- Forecast File Storage ---
DEFAULT_FORECAST_DIR = Path("./miner_forecasts/")
MINER_FORECAST_DIR = Path(os.getenv("MINER_FORECAST_DIR", DEFAULT_FORECAST_DIR))
MINER_FORECAST_DIR.mkdir(parents=True, exist_ok=True)
logger.info(f"Serving forecast files from: {MINER_FORECAST_DIR.resolve()}")

class DataModel(BaseModel):
    name: str
    timestamp: str
    value: float
    historical_values: list[dict] | None = None


class GeomagneticRequest(BaseModel):
    nonce: str | None = None  # Make nonce optional
    data: DataModel | None = None  # Add data field as optional


class SoilmoistureRequest(BaseModel):
    nonce: str | None = None
    data: SoilMoisturePayload


# --- Models for Weather Forecast Request (Validator Sends Input) ---
class WeatherInputData(BaseModel):
    """ Defines the structure for the input data sent by the validator to trigger a forecast run."""
    forecast_start_time: datetime = Field(..., description="ISO 8601 timestamp for the forecast initialization time.")
    gfs_timestep_1: Dict[str, Any] = Field(..., description="Data for the first GFS input timestep. Structure TBD (e.g., dict mapping variable names to base64 encoded numpy arrays or similar).")
    gfs_timestep_2: Dict[str, Any] = Field(..., description="Data for the second GFS input timestep. Structure TBD.")
    # Optional: Add other necessary metadata like grid info if not implicitly known by the miner
    # grid_metadata: Dict[str, Any] | None = None 
    class Config:
        # Allows using datetime objects directly
        arbitrary_types_allowed = True 

class WeatherForecastRequest(BaseModel):
    """ The overall request model containing the nonce and the weather input data."""
    nonce: str | None = None
    data: WeatherInputData


def factory_router(miner_instance) -> APIRouter:
    """Create router with miner instance available to route handlers."""
    router = APIRouter()

    async def geomagnetic_require(
            decrypted_payload: GeomagneticRequest = Depends(
                partial(decrypt_general_payload, GeomagneticRequest),
            ),
    ):
        """
        Handles geomagnetic prediction requests, ensuring predictions are validated
        and a timestamp is always included for scoring purposes.
        """
        logger.info(f"Received decrypted payload: {decrypted_payload}")
        result = None

        try:
            if decrypted_payload.data:
                response_data = decrypted_payload.model_dump()
                db_manager = MinerDatabaseManager()
                geomagnetic_task = GeomagneticTask(db_manager=db_manager, node_type="miner")
                logger.info(f"Miner executing geomagnetic prediction ...")

                result = geomagnetic_task.miner_execute(response_data, miner_instance)
                logger.info(f"Miner execution completed: {result}")

                if result:
                    if "predicted_values" in result:
                        pred_value = result["predicted_values"]
                        try:
                            pred_value = np.array(pred_value, dtype=float)
                            if np.isnan(pred_value).any() or np.isinf(pred_value).any():
                                logger.warning("Invalid prediction value received, setting to 0.0")
                                result["predicted_values"] = float(0.0)
                            else:
                                result["predicted_values"] = float(pred_value)
                        except (ValueError, TypeError):
                            logger.warning("Could not convert prediction to float, setting to 0.0")
                            result["predicted_values"] = float(0.0)
                    else:
                        logger.error("Missing 'predicted_values' in result. Setting to default 0.0")
                        result["predicted_values"] = float(0.0)

                    if "timestamp" not in result:
                        logger.warning("Missing timestamp in result, using fallback timestamp")
                        result["timestamp"] = datetime.now(timezone.utc).isoformat()
                else:
                    logger.error("Result is empty, returning default response.")
                    result = {
                        "predicted_values": float(0.0),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "miner_hotkey": miner_instance.keypair.ss58_address,
                    }
        except Exception as e:
            logger.error(f"Error in geomagnetic_require: {e}")
            logger.error(traceback.format_exc())
            result = {
                "predicted_values": 0.0,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "miner_hotkey": "error_fallback",
            }

        return JSONResponse(content=result)

    async def soilmoisture_require(
        decrypted_payload: SoilmoistureRequest = Depends(
            partial(decrypt_general_payload, SoilmoistureRequest),
        ),
    ):
        try:
            db_manager = MinerDatabaseManager()
            soil_task = SoilMoistureTask(db_manager=db_manager, node_type="miner")

            # Execute miner task
            result = await soil_task.miner_execute(
                decrypted_payload.model_dump(), miner_instance
            )

            if result is None:
                return JSONResponse(
                    status_code=500,
                    content={"error": "Failed to process soil moisture prediction"},
                )

            return JSONResponse(content=result)

        except Exception as e:
            logger.error(f"Error processing soil moisture request: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse(
                status_code=500, content={"error": f"Internal server error: {str(e)}"}
            )
            
    async def get_forecast_file(filename: str):
        try:
            file_path = (MINER_FORECAST_DIR / filename).resolve()

            if not file_path.is_file() or MINER_FORECAST_DIR.resolve() not in file_path.parents:
                logger.warning(f"Forecast file request denied or file not found: {filename} (Resolved: {file_path})")
                return JSONResponse(status_code=404, content={"error": "File not found or access denied"})
            
            logger.info(f"Serving forecast file: {file_path}")
            return FileResponse(
                path=str(file_path), 
                filename=filename, 
                media_type='application/x-netcdf'
            )
            
        except Exception as e:
            logger.error(f"Error serving forecast file {filename}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse(status_code=500, content={"error": "Internal server error serving file"})

    async def weather_forecast_require(
        decrypted_payload: WeatherForecastRequest = Depends(
            partial(decrypt_general_payload, WeatherForecastRequest),
        ),
    ):
        """
        Handles requests from validators to initiate a weather forecast run 
        using the provided GFS input data.
        """
        logger.info(f"Received decrypted weather forecast request payload")
        try:
            if not hasattr(miner_instance, 'weather_task'):
                logger.error("Miner instance is missing the 'weather_task' attribute.")
                return JSONResponse(status_code=500, content={"error": "Miner not configured for weather task"})
            
            input_data = decrypted_payload.data.model_dump()
            
            logger.info(f"Initiating weather forecast run for start time: {input_data.get('forecast_start_time')}")
            
            # Trigger the asynchronous forecast generation process in WeatherTask
            # This method needs to accept the validator-provided input data.
            # TODO: Implement miner_instance.weather_task.execute_validator_driven_run
            # result = await miner_instance.weather_task.execute_validator_driven_run(input_data, miner_instance)
            
            # Placeholder
            forecast_id = f"forecast_{input_data.get('forecast_start_time')}_{miner_instance.keypair.ss58_address[:8]}"
            logger.info(f"Weather forecast run initiated successfully. ID: {forecast_id}")
            result = {"status": "success", "message": "Forecast run initiated", "forecast_id": forecast_id}
            
            return JSONResponse(content=result)

        except ValidationError as e:
             logger.error(f"Validation error processing weather forecast request: {e}")
             return JSONResponse(status_code=422, content={"error": "Invalid request payload structure", "details": e.errors()})
        except Exception as e:
            logger.error(f"Error processing weather forecast request: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse(status_code=500, content={"error": f"Internal server error: {str(e)}"})

    class WeatherKerchunkRequest(BaseModel):
        nonce: str | None = None
        data: Any 
        
    async def weather_kerchunk_require(
        decrypted_payload: WeatherKerchunkRequest = Depends(
            partial(decrypt_general_payload, WeatherKerchunkRequest),
        ),
    ):
        """
        Handles requests from validators for Kerchunk JSON metadata of a specific forecast.
        The actual logic for finding/generating the JSON resides in the WeatherTask.
        """
        logger.info(f"Received decrypted weather kerchunk request")
        try:
            if not hasattr(miner_instance, 'weather_task'):
                logger.error("Miner instance is missing the 'weather_task' attribute.")
                return JSONResponse(status_code=500, content={"error": "Miner not configured for weather task"})
            
            request_data = decrypted_payload.data 
            logger.info(f"Handling weather kerchunk request...")
            
            # TODO: Implement miner_instance.weather_task.handle_kerchunk_request
            # kerchunk_json = await miner_instance.weather_task.handle_kerchunk_request(request_data, miner_instance)
            
            kerchunk_json = {"version": 1, "refs": {".zgroup": "{\"zarr_format\": 2}"}, "message": "placeholder - implement handle_kerchunk_request in WeatherTask"}
            
            if kerchunk_json:
                return JSONResponse(content=kerchunk_json)
            else:
                logger.warning(f"Kerchunk JSON not found for request data: {request_data}")
                return JSONResponse(status_code=404, content={"error": "Kerchunk JSON not found for specified forecast"})

        except Exception as e:
            logger.error(f"Error processing weather kerchunk request: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse(status_code=500, content={"error": f"Internal server error: {str(e)}"})

    router.add_api_route(
        "/geomagnetic-request",
        geomagnetic_require,
        tags=["Geomagnetic"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        methods=["POST"],
        response_class=JSONResponse,
    )
    router.add_api_route(
        "/soilmoisture-request",
        soilmoisture_require,
        tags=["Soilmoisture"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        methods=["POST"],
        response_class=JSONResponse,
    )
    router.add_api_route(
        "/forecasts/{filename}",
        get_forecast_file,
        tags=["Weather"],
        methods=["GET"],
        response_class=FileResponse
    )
    # Route for triggering weather forecast run
    router.add_api_route(
        "/weather-forecast-request", 
        weather_forecast_require,
        tags=["Weather"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        methods=["POST"],
        response_class=JSONResponse
    )
    # Route for validator to request Kerchunk JSON metadata
    router.add_api_route(
        "/weather-kerchunk-request", 
        weather_kerchunk_require,
        tags=["Weather"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        methods=["POST"],
        response_class=JSONResponse
    )

    return router
