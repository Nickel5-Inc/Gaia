from functools import partial
from fastapi import Depends, Request, HTTPException, Header, Path, Query
from fastapi.responses import JSONResponse, FileResponse, RedirectResponse, Response
from fastapi.routing import APIRouter
from pydantic import BaseModel, Field
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
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from typing import Any, Dict, Optional
import glob
import urllib.parse
import base64

from gaia.tasks.defined_tasks.weather.schemas.weather_inputs import (
    WeatherForecastRequest, WeatherKerchunkRequest, WeatherInputData,
    WeatherInitiateFetchRequest, WeatherGetInputStatusRequest, WeatherStartInferenceRequest
)
from gaia.tasks.defined_tasks.weather.schemas.weather_outputs import (
    WeatherKerchunkResponseData,
    WeatherInitiateFetchResponse, WeatherGetInputStatusResponse, WeatherStartInferenceResponse
)

MAX_REQUEST_SIZE = 800 * 1024 * 1024  # 800MB

logger = get_logger(__name__)

current_file_path = Path(__file__).resolve()
gaia_repo_root = current_file_path.parent.parent.parent.parent 

def find_forecast_dir():
    env_dir = os.getenv("MINER_FORECAST_DIR")
    if env_dir:
        return Path(env_dir)
        
    potential_paths = [
        gaia_repo_root / "miner_forecasts_background",  # Standard path in repo root
        Path.cwd() / "miner_forecasts_background",      # Current working directory
        Path.home() / "Gaia" / "miner_forecasts_background", # Home directory Gaia folder
    ]
    
    for path in potential_paths:
        if path.exists() and path.is_dir():
            logger.info(f"Found forecast directory at: {path}")
            return path
            
    default_path = gaia_repo_root / "miner_forecasts_background"
    logger.info(f"Using default forecast directory: {default_path}")
    return default_path

DEFAULT_FORECAST_DIR = find_forecast_dir()
MINER_FORECAST_DIR = Path(os.getenv("MINER_FORECAST_DIR", str(DEFAULT_FORECAST_DIR)))
MINER_FORECAST_DIR.mkdir(parents=True, exist_ok=True)
logger.info(f"Serving forecast files from: {MINER_FORECAST_DIR.resolve()}")

JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 120

MINER_JWT_SECRET_KEY = os.getenv("MINER_JWT_SECRET_KEY")
if not MINER_JWT_SECRET_KEY:
    logger.warning("MINER_JWT_SECRET_KEY not set in environment. Using default insecure key.")
    MINER_JWT_SECRET_KEY = "insecure_default_key_for_development_only"

security = HTTPBearer()

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
                
                if not hasattr(miner_instance, 'geomagnetic_task'):
                    logger.error("Miner instance is missing the 'geomagnetic_task' attribute.")
                    return JSONResponse(status_code=500, content={"error": "Miner not configured for geomagnetic task"})
                
                logger.info(f"Miner executing geomagnetic prediction ...")
                result = miner_instance.geomagnetic_task.miner_execute(response_data, miner_instance)
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
                        logger.error("Missing 'predicted_values' in result, setting to default 0.0")
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
            if not hasattr(miner_instance, 'soil_task'):
                logger.error("Miner instance is missing the 'soil_task' attribute.")
                return JSONResponse(status_code=500, content={"error": "Miner not configured for soil moisture task"})

            result = await miner_instance.soil_task.miner_execute(
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
            
    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        """Verify JWT token and return decoded payload."""
        try:
            token = credentials.credentials
            payload = jwt.decode(
                token,
                MINER_JWT_SECRET_KEY,
                algorithms=[JWT_ALGORITHM]
            )
            
            if datetime.fromtimestamp(payload["exp"], tz=timezone.utc) < datetime.now(timezone.utc):
                logger.warning(f"Token expired for job_id: {payload.get('job_id')}, file_path: {payload.get('file_path')}")
                raise HTTPException(status_code=401, detail="Token has expired")
            

            logger.debug(f"Token successfully decoded and validated (not expired). Payload: {payload}")
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning(f"JWT token has expired.")
            raise HTTPException(status_code=401, detail="Token has expired (signature)")
        except jwt.PyJWTError as e:
            logger.warning(f"Invalid JWT token: {e}")
            raise HTTPException(status_code=401, detail=f"Invalid token: {e}")

    async def get_forecast_file(request: Request, file_path: str, token: Optional[str] = None):
        """Serve files from the forecast directory after validating JWT token."""
        
        logger.info(f"Received request for file: {file_path}")
        
        if token is None:
            auth_header = request.headers.get("Authorization")
            if auth_header:
                if auth_header.startswith("Bearer "):
                    token = auth_header[7:]
                else:
                    token = auth_header
                logger.info("Found token in Authorization header")
            
            elif "token" in request.query_params:
                token = request.query_params.get("token")
                logger.info("Found token in query params")
            
            elif "?token=" in file_path:
                path_parts = file_path.split("?token=", 1)
                if len(path_parts) == 2:
                    base_path = path_parts[0]
                    token_part = path_parts[1]
                    
                    if "/" in token_part:
                        token_and_path = token_part.split("/", 1)
                        token = token_and_path[0]
                        remaining_path = token_and_path[1]
                        file_path = f"{base_path}/{remaining_path}"
                        logger.info(f"Extracted token from path and reconstructed file_path: {file_path}")
                    else:
                        token = token_part
                        logger.info(f"Extracted token from path: {token[:10]}...")
        
        forecasts_dir = MINER_FORECAST_DIR
        if not forecasts_dir.is_absolute():
            forecasts_dir = forecasts_dir.absolute()
        
        file_path = file_path.lstrip('/')
        
        if file_path.endswith(".zarr") and not file_path.endswith("/"):
            file_path = file_path + "/"
        
        if token:
            try:
                token = token.strip()
                if token.startswith('"') and token.endswith('"'):
                    token = token[1:-1]
                
                payload = jwt.decode(token, MINER_JWT_SECRET_KEY, algorithms=["HS256"])
                logger.info("Successfully validated JWT token")
                
                if "file_path" in payload:
                    token_file_path = payload["file_path"]
                    if not (file_path.startswith(token_file_path) or 
                           token_file_path.startswith(file_path.rstrip('/'))):
                        logger.warning(f"Token path mismatch: {token_file_path} vs {file_path}")
                        raise HTTPException(status_code=403, detail="Path not authorized by token")
                else:
                    logger.warning("Token missing file_path claim")
                    raise HTTPException(status_code=403, detail="Token missing file_path claim")
                    
            except jwt.PyJWTError as jwt_error:
                logger.warning(f"Invalid JWT token: {str(jwt_error)}")
                raise HTTPException(status_code=401, detail="Invalid token")
            except Exception as e:
                logger.warning(f"Error processing token: {str(e)}")
                raise HTTPException(status_code=401, detail="Invalid token")
        
        zarr_suffix = ".zarr"
        if file_path.endswith(zarr_suffix) or file_path.endswith(f"{zarr_suffix}/"):
            zarr_dir_name = file_path.rstrip('/')
            zarr_path = forecasts_dir / zarr_dir_name
            
            if not zarr_path.exists():
                logger.warning(f"Zarr path not found: {zarr_path}")
                pattern = str(forecasts_dir / f"{zarr_dir_name.lower().replace(zarr_suffix, '')}*{zarr_suffix}")
                matches = glob.glob(pattern, case=False)
                if matches:
                    zarr_path = Path(matches[0])
                    logger.info(f"Found case-insensitive match: {zarr_path}")
            
            if not zarr_path.exists():
                logger.error(f"Zarr directory not found: {zarr_path}")
                raise HTTPException(status_code=404, detail=f"File not found: {zarr_dir_name}")
            
            if zarr_path.is_dir():
                if '/' in file_path.strip('/'):
                    zarr_base, internal_path = file_path.strip('/').split('/', 1)
                    full_internal_path = zarr_path / internal_path
                    
                    if full_internal_path.exists() and full_internal_path.is_file():
                        logger.info(f"Returning specific Zarr file: {full_internal_path}")
                        return FileResponse(path=str(full_internal_path), filename=full_internal_path.name)
                
                try:
                    contents = [item.name for item in zarr_path.iterdir()]
                    logger.info(f"Listed zarr directory contents: {contents}")
                    return JSONResponse(content={"files": contents})
                except Exception as e:
                    logger.error(f"Error listing zarr directory: {str(e)}")
                    raise HTTPException(status_code=500, detail=f"Error listing directory: {str(e)}")
        
        full_path = forecasts_dir / file_path.rstrip('/')
        
        if full_path.is_file():
            logger.info(f"Returning file: {full_path}")
            return FileResponse(path=str(full_path), filename=full_path.name)
        
        if full_path.is_dir():
            try:
                contents = [item.name for item in full_path.iterdir()]
                logger.info(f"Listed directory contents: {contents}")
                return JSONResponse(content={"files": contents})
            except Exception as e:
                logger.error(f"Error listing directory: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Error listing directory: {str(e)}")
        
        logger.error(f"File or directory not found: {full_path}")
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

    async def weather_forecast_require(
        decrypted_payload: WeatherForecastRequest = Depends(
            partial(decrypt_general_payload, WeatherForecastRequest),
        ),
    ):
        """
        Handles requests from validators to initiate a weather forecast run 
        using the provided GFS input data.
        """
        logger.info("Entered weather_forecast_require handler.")
        logger.info(f"Successfully decrypted weather forecast payload. Type: {type(decrypted_payload)}")
        try:
            if not hasattr(miner_instance, 'weather_task'):
                logger.error("Miner instance is missing the 'weather_task' attribute.")
                return JSONResponse(status_code=500, content={"error": "Miner not configured for weather task"})
            
            input_data = decrypted_payload.data.model_dump()
            logger.info(f"Initiating weather forecast run for start time: {input_data.get('forecast_start_time')}")
            
            try:
                logger.info("Calling miner_instance.weather_task.miner_execute...")
                result = await miner_instance.weather_task.miner_execute(input_data, miner_instance)
                logger.info(f"miner_instance.weather_task.miner_execute completed. Result: {type(result)}")
            except Exception as task_exec_error:
                logger.error(f"Error during miner_instance.weather_task.miner_execute: {task_exec_error}")
                logger.error(traceback.format_exc())
                return JSONResponse(status_code=500, content={"error": "Error during task execution"})
            
            if not result:
                logger.error("Weather forecast execution failed (result was None or empty)")
                return JSONResponse(
                    status_code=500,
                    content={"error": "Failed to execute weather forecast"}
                )
            
            logger.info(f"Successfully initiated forecast. Job ID: {result.get('job_id')}")
            return JSONResponse(content={
                "status": "success",
                "message": "Forecast run initiated",
                "job_id": result.get("job_id"),
                "forecast_start_time": input_data.get("forecast_start_time")
            })

        except ValidationError as e:
            logger.error(f"Validation error processing weather forecast request: {e}")
            return JSONResponse(
                status_code=422,
                content={"error": "Invalid request payload structure", "details": e.errors()}
            )
        except Exception as e:
            logger.error(f"Unhandled error in weather_forecast_require handler: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse(
                status_code=500,
                content={"error": f"Internal server error: {str(e)}"}
            )

    async def weather_kerchunk_require(
        decrypted_payload: WeatherKerchunkRequest = Depends(
            partial(decrypt_general_payload, WeatherKerchunkRequest),
        ),
    ):
        """
        Handles requests from validators for forecast data (Zarr store or Kerchunk JSON) of a specific forecast.
        The actual logic for finding/generating the store resides in the WeatherTask.
        """
        logger.info(f"Received decrypted weather forecast data request")
        try:
            if not hasattr(miner_instance, 'weather_task') or miner_instance.weather_task is None:
                logger.error("Miner instance is missing the 'weather_task' attribute or it is None.")
                return JSONResponse(status_code=500, content={"error": "Miner not configured for weather task"})

            request_data = decrypted_payload.data
            logger.info(f"Handling weather forecast data request...")

            job_id = request_data.job_id
            if not job_id:
                logger.error("Missing job_id in request data")
                return JSONResponse(status_code=400, content={"error": "Missing job_id in request"})

            response_dict = await miner_instance.weather_task.handle_kerchunk_request(job_id)

            if not isinstance(response_dict, dict):
                 logger.error(f"handle_kerchunk_request returned invalid type: {type(response_dict)}")
                 return JSONResponse(status_code=500, content={"error": "Internal error processing forecast data request"})

            zarr_store_url = response_dict.get('zarr_store_url')
            if not zarr_store_url and 'kerchunk_json_url' in response_dict:
                zarr_store_url = response_dict.get('kerchunk_json_url')
            
            response_data = WeatherKerchunkResponseData(
                status=response_dict.get('status', 'error'),
                message=response_dict.get('message', 'Failed to process'),
                zarr_store_url=zarr_store_url,
                verification_hash=response_dict.get('verification_hash'),
                access_token=response_dict.get('access_token')
            )

            return JSONResponse(content=response_data.model_dump())

        except Exception as e:
            logger.error(f"Error processing weather forecast data request: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse(status_code=500, content={"error": f"Internal server error: {str(e)}"})

    async def weather_initiate_fetch_require(
        decrypted_payload: WeatherInitiateFetchRequest = Depends(
            partial(decrypt_general_payload, WeatherInitiateFetchRequest)
        ),
    ):
        """
        Handles Step 1: Validator requests the miner to fetch GFS data based on timestamps.
        Miner creates a job record and starts a background task for fetching & hashing.
        """
        logger.info("Entered /weather-initiate-fetch handler.")
        try:
            if not hasattr(miner_instance, 'weather_task') or miner_instance.weather_task is None:
                logger.error("Miner not configured for weather task (weather_task missing or None).")
                return JSONResponse(status_code=500, content={"error": "Miner not configured for weather task"})

            response_data = await miner_instance.weather_task.handle_initiate_fetch(
                request_data=decrypted_payload.data 
            )

            if not isinstance(response_data, dict):
                 logger.error(f"handle_initiate_fetch returned invalid type: {type(response_data)}")
                 return JSONResponse(status_code=500, content={"error": "Internal error processing fetch initiation"})

            response_model = WeatherInitiateFetchResponse(**response_data)
            return JSONResponse(content=response_model.model_dump())

        except ValidationError as e:
            logger.error(f"Validation error processing initiate fetch request: {e}")
            return JSONResponse(status_code=422, content={"error": "Invalid request payload", "details": e.errors()})
        except Exception as e:
            logger.error(f"Error in /weather-initiate-fetch handler: {e}", exc_info=True)
            return JSONResponse(status_code=500, content={"error": f"Internal server error: {str(e)}"})

    async def weather_get_input_status_require(
        decrypted_payload: WeatherGetInputStatusRequest = Depends(
            partial(decrypt_general_payload, WeatherGetInputStatusRequest)
        ),
    ):
        """
        Handles Step 3: Validator polls for the status of the GFS fetch/hash process.
        Miner returns the job status and the input hash if available.
        """
        logger.info("Entered /weather-get-input-status handler.")
        try:
            if not hasattr(miner_instance, 'weather_task') or miner_instance.weather_task is None:
                logger.error("Miner not configured for weather task (weather_task missing or None).")
                return JSONResponse(status_code=500, content={"error": "Miner not configured for weather task"})

            job_id = decrypted_payload.data.job_id
            if not job_id:
                 return JSONResponse(status_code=400, content={"error": "Missing job_id in request"})

            response_data = await miner_instance.weather_task.handle_get_input_status(job_id)

            if not isinstance(response_data, dict):
                 logger.error(f"handle_get_input_status returned invalid type: {type(response_data)}")
                 return JSONResponse(status_code=500, content={"error": "Internal error fetching input status"})

            response_model = WeatherGetInputStatusResponse(**response_data)
            return JSONResponse(content=response_model.model_dump())

        except ValidationError as e:
            logger.error(f"Validation error processing get input status request: {e}")
            return JSONResponse(status_code=422, content={"error": "Invalid request payload", "details": e.errors()})
        except Exception as e:
            logger.error(f"Error in /weather-get-input-status handler: {e}", exc_info=True)
            return JSONResponse(status_code=500, content={"error": f"Internal server error: {str(e)}"})

    async def weather_start_inference_require(
        decrypted_payload: WeatherStartInferenceRequest = Depends(
            partial(decrypt_general_payload, WeatherStartInferenceRequest)
        ),
    ):
        """
        Handles Step 5: Validator, after verifying the input hash, triggers the miner
        to start the actual model inference.
        """
        logger.info("Entered /weather-start-inference handler.")
        try:
            if not hasattr(miner_instance, 'weather_task') or miner_instance.weather_task is None:
                logger.error("Miner not configured for weather task (weather_task missing or None).")
                return JSONResponse(status_code=500, content={"error": "Miner not configured for weather task"})

            job_id = decrypted_payload.data.job_id
            if not job_id:
                 return JSONResponse(status_code=400, content={"error": "Missing job_id in request"})

            response_data = await miner_instance.weather_task.handle_start_inference(job_id)

            if not isinstance(response_data, dict):
                 logger.error(f"handle_start_inference returned invalid type: {type(response_data)}")
                 return JSONResponse(status_code=500, content={"error": "Internal error starting inference"})

            response_model = WeatherStartInferenceResponse(**response_data)
            return JSONResponse(content=response_model.model_dump())

        except ValidationError as e:
            logger.error(f"Validation error processing start inference request: {e}")
            return JSONResponse(status_code=422, content={"error": "Invalid request payload", "details": e.errors()})
        except Exception as e:
            logger.error(f"Error in /weather-start-inference handler: {e}", exc_info=True)
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

    if hasattr(miner_instance, 'weather_task') and miner_instance.weather_task is not None:
        router.add_api_route(
            "/forecasts/{file_path:path}",
            get_forecast_file,
            tags=["Weather"],
            methods=["GET", "HEAD"],
            response_class=Response,
            response_model=None
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
        router.add_api_route(
            "/weather-initiate-fetch",
            weather_initiate_fetch_require,
            tags=["Weather"],
            dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
            methods=["POST"],
            response_class=JSONResponse
        )
        router.add_api_route(
            "/weather-get-input-status",
            weather_get_input_status_require,
            tags=["Weather"],
            dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
            methods=["POST"],
            response_class=JSONResponse
        )
        router.add_api_route(
            "/weather-start-inference",
            weather_start_inference_require,
            tags=["Weather"],
            dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
            methods=["POST"],
            response_class=JSONResponse
        )
        logger.info("Weather routes registered (weather task is enabled)")
    else:
        logger.info("Weather routes NOT registered (weather task is disabled)")

    return router
