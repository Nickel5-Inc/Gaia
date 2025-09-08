import asyncio
import base64
import glob
import json
import os
import traceback
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import jwt
# Geomagnetic and soil moisture tasks disabled
import numpy as np
from fastapi import Depends, HTTPException, Path, Request
from fastapi.responses import FileResponse, Response
from fastapi.routing import APIRouter
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fiber.encrypted.miner.dependencies import (blacklist_low_stake,
                                                verify_request)
from fiber.encrypted.miner.security.encryption import decrypt_general_payload
from pydantic import BaseModel, Field, ValidationError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.utils.custom_logger import get_logger

# High-performance JSON operations for miner routes
try:
    from fastapi.responses import JSONResponse as _FastAPIJSONResponse

    from gaia.utils.performance import dumps, loads

    class JSONResponse(_FastAPIJSONResponse):
        """Optimized JSONResponse using orjson for 2-3x faster miner route responses."""

        def render(self, content: Any) -> bytes:
            try:
                # Use high-performance orjson serialization
                return dumps(content).encode("utf-8")
            except Exception:
                # Fallback to FastAPI's default JSON encoder
                return super().render(content)

    logger = get_logger(__name__)
    logger.info("🚀 Miner routes using orjson for high-performance JSON responses")

except ImportError:
    from fastapi.responses import JSONResponse

    logger = get_logger(__name__)
    logger.info(
        "⚡ Miner routes using standard JSON - install orjson for 2-3x performance boost"
    )

from gaia.tasks.defined_tasks.weather.schemas.weather_inputs import (
    WeatherForecastRequest, WeatherGetInputStatusRequest,
    WeatherInitiateFetchRequest, WeatherInputData, WeatherKerchunkRequest,
    WeatherStartInferenceRequest)
from gaia.tasks.defined_tasks.weather.schemas.weather_outputs import (
    WeatherGetInputStatusResponse, WeatherInitiateFetchResponse,
    WeatherKerchunkResponseData, WeatherStartInferenceResponse,
    WeatherTaskStatus)

MAX_REQUEST_SIZE = 800 * 1024 * 1024  # 800MB

current_file_path = Path(__file__).resolve()
gaia_repo_root = current_file_path.parent.parent.parent.parent


def find_forecast_dir():
    env_dir = os.getenv("MINER_FORECAST_DIR")
    if env_dir:
        return Path(env_dir)

    potential_paths = [
        gaia_repo_root / "miner_forecasts_background",  # Standard path in repo root
        Path.cwd() / "miner_forecasts_background",  # Current working directory
        Path.home()
        / "Gaia"
        / "miner_forecasts_background",  # Home directory Gaia folder
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
    logger.warning(
        "MINER_JWT_SECRET_KEY not set in environment. Using default insecure key."
    )
    MINER_JWT_SECRET_KEY = "insecure_default_key_for_development_only"

security = HTTPBearer()

# DataModel class removed (was used only for geomagnetic task)

# ============== HOTKEY BLACKLIST CONFIGURATION ==============
# Option 1: Load from environment variable (comma-separated)
BLACKLISTED_HOTKEYS_ENV = os.getenv("BLACKLISTED_HOTKEYS", "")
BLACKLISTED_HOTKEYS: Set[str] = set(
    hotkey.strip() for hotkey in BLACKLISTED_HOTKEYS_ENV.split(",") if hotkey.strip()
)

# Option 2: Load from file
BLACKLIST_FILE_PATH = os.getenv("BLACKLIST_FILE_PATH", "/root/Gaia-miner/blacklisted_hotkeys.json")
if os.path.exists(BLACKLIST_FILE_PATH):
    try:
        with open(BLACKLIST_FILE_PATH, "r") as f:
            file_blacklist = json.load(f)
            if isinstance(file_blacklist, list):
                BLACKLISTED_HOTKEYS.update(file_blacklist)
            logger.info(f"Loaded {len(file_blacklist)} hotkeys from blacklist file")
    except Exception as e:
        logger.error(f"Failed to load blacklist file: {e}")

logger.info(f"Total blacklisted hotkeys: {len(BLACKLISTED_HOTKEYS)}")

# ============== APPROACH 1: MIDDLEWARE CLASS ==============
class HotkeyBlacklistMiddleware(BaseHTTPMiddleware):
    """
    Middleware that checks if the requesting hotkey is blacklisted.
    This runs for ALL endpoints automatically.
    """
    
    def __init__(self, app: ASGIApp, miner_instance, blacklist: Set[str] = None):
        super().__init__(app)
        self.miner_instance = miner_instance
        self.blacklist = blacklist or BLACKLISTED_HOTKEYS
        self.logger = get_logger(__name__)
    
    async def dispatch(self, request: Request, call_next):
        """Process each request to check for blacklisted hotkeys."""
        # Skip middleware for non-API endpoints (like file serving)
        if request.url.path.startswith("/forecasts/"):
            return await call_next(request)
        
        # Try to extract hotkey from request
        sender_hotkey = await self._extract_hotkey(request)
        
        if sender_hotkey and sender_hotkey in self.blacklist:
            self.logger.warning(
                f"Blocked request from blacklisted hotkey: {sender_hotkey[:12]}... "
                f"Endpoint: {request.url.path}"
            )
            raise HTTPException(
                status_code=403, 
                detail="Access denied: Your hotkey has been blacklisted"
            )
        
        response = await call_next(request)
        return response
    
    async def _extract_hotkey(self, request: Request) -> Optional[str]:
        """
        Extract hotkey from the request. This is tricky because Fiber encrypts the payload.
        The hotkey is typically added by verify_request after decryption.
        """
        # Since the payload is encrypted, we can't easily extract the hotkey here
        # This would require reimplementing parts of Fiber's verification
        # For now, return None - use Approach 2 or 3 instead
        return None

# ============== APPROACH 2: CUSTOM DEPENDENCY ==============
async def check_hotkey_blacklist(
    request: Request,
    decrypted_payload: Any = None
) -> None:
    """
    Dependency that checks if the sender's hotkey is blacklisted.
    This should be used AFTER decrypt_general_payload in the dependency chain.
    """
    # Try to get hotkey from the decrypted payload
    sender_hotkey = None
    
    if hasattr(decrypted_payload, 'sender_hotkey'):
        sender_hotkey = decrypted_payload.sender_hotkey
    elif isinstance(decrypted_payload, dict):
        sender_hotkey = decrypted_payload.get('sender_hotkey')
    elif hasattr(decrypted_payload, 'data'):
        if isinstance(decrypted_payload.data, dict):
            sender_hotkey = decrypted_payload.data.get('sender_hotkey')
    
    if sender_hotkey and sender_hotkey in BLACKLISTED_HOTKEYS:
        logger.warning(
            f"Blocked request from blacklisted hotkey: {sender_hotkey[:12]}... "
            f"Endpoint: {request.url.path}"
        )
        raise HTTPException(
            status_code=403,
            detail="Access denied: Your hotkey has been blacklisted"
        )

# ============== APPROACH 3: ENHANCED BLACKLIST DEPENDENCY ==============
async def enhanced_verify_request_with_blacklist(request: Request) -> dict:
    """
    Enhanced version of Fiber's verify_request that also checks blacklist.
    This dependency should be used alongside or instead of verify_request.
    """
    # First, run Fiber's standard verification
    # This validates the signature and extracts the sender's hotkey
    from fiber.encrypted.miner.dependencies import \
        verify_request as fiber_verify
    
    try:
        # Run the standard Fiber verification
        verification_result = await fiber_verify(request)
        
        # Extract sender hotkey from the verification result
        # Fiber typically adds this to the request headers after verification
        sender_hotkey = None
        
        # Check various places where Fiber might store the hotkey
        if hasattr(request.state, 'hotkey'):
            sender_hotkey = request.state.hotkey
        elif hasattr(request.state, 'sender_hotkey'):
            sender_hotkey = request.state.sender_hotkey
        elif 'sender-hotkey' in request.headers:
            sender_hotkey = request.headers.get('sender-hotkey')
        elif 'hotkey' in request.headers:
            sender_hotkey = request.headers.get('hotkey')
        
        # If we found a hotkey, check blacklist
        if sender_hotkey and sender_hotkey in BLACKLISTED_HOTKEYS:
            logger.warning(
                f"Blocked request from blacklisted hotkey: {sender_hotkey[:12]}... "
                f"Endpoint: {request.url.path}"
            )
            raise HTTPException(
                status_code=403,
                detail="Access denied: Your hotkey has been blacklisted"
            )
        
        return verification_result
        
    except HTTPException:
        # Re-raise HTTP exceptions (including our blacklist rejection)
        raise
    except Exception as e:
        logger.error(f"Error in enhanced verification: {e}")
        raise HTTPException(status_code=500, detail="Verification failed")

# ============== APPROACH 4: CUSTOM BLACKLIST DEPENDENCY ==============
class BlacklistChecker:
    """
    A dependency class that can be configured and injected into routes.
    This allows for more flexibility in how blacklisting is handled.
    """
    
    def __init__(self, blacklist: Set[str] = None):
        self.blacklist = blacklist or BLACKLISTED_HOTKEYS
        self.logger = get_logger(__name__)
    
    async def __call__(
        self, 
        request: Request,
        # These dependencies run first
        _verify = Depends(verify_request),
        _blacklist_check = Depends(blacklist_low_stake)
    ) -> None:
        """
        Check if the sender is in our custom blacklist.
        This runs AFTER Fiber's standard checks.
        """
        # Try to get the hotkey from various sources
        sender_hotkey = None
        
        # Check request state (where Fiber might store it)
        if hasattr(request.state, 'hotkey'):
            sender_hotkey = request.state.hotkey
        elif hasattr(request.state, 'sender_hotkey'):  
            sender_hotkey = request.state.sender_hotkey
            
        # Check headers - validator hotkey should be stored in validator-hotkey header
        if not sender_hotkey:
            for header_name in ['validator-hotkey', 'sender-hotkey', 'hotkey']:
                if header_name in request.headers:
                    sender_hotkey = request.headers.get(header_name)
                    break
        
        if sender_hotkey and sender_hotkey in self.blacklist:
            self.logger.warning(
                f"Blocked request from blacklisted hotkey: {sender_hotkey[:12]}... "
                f"Endpoint: {request.url.path}"
            )
            raise HTTPException(
                status_code=403,
                detail="Access denied: Your hotkey has been blacklisted"
            )

# Create a reusable instance
hotkey_blacklist_checker = BlacklistChecker()

# ============== UTILITY FUNCTIONS ==============
async def update_blacklist(hotkeys: List[str], action: str = "add") -> Dict[str, Any]:
    """
    Dynamically update the blacklist at runtime.
    
    Args:
        hotkeys: List of hotkeys to add or remove
        action: "add" to add hotkeys, "remove" to remove them
    
    Returns:
        Status dictionary with the updated blacklist size
    """
    global BLACKLISTED_HOTKEYS
    
    if action == "add":
        before_size = len(BLACKLISTED_HOTKEYS)
        BLACKLISTED_HOTKEYS.update(hotkeys)
        after_size = len(BLACKLISTED_HOTKEYS)
        logger.info(f"Added {after_size - before_size} hotkeys to blacklist")
    elif action == "remove":
        before_size = len(BLACKLISTED_HOTKEYS)
        BLACKLISTED_HOTKEYS.difference_update(hotkeys)
        after_size = len(BLACKLISTED_HOTKEYS)
        logger.info(f"Removed {before_size - after_size} hotkeys from blacklist")
    else:
        raise ValueError(f"Invalid action: {action}")
    
    # Optionally save to file
    if BLACKLIST_FILE_PATH:
        try:
            with open(BLACKLIST_FILE_PATH, "w") as f:
                json.dump(list(BLACKLISTED_HOTKEYS), f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save blacklist file: {e}")
    
    return {
        "status": "success",
        "action": action,
        "total_blacklisted": after_size,
        "hotkeys_affected": len(hotkeys)
    }


# Geomagnetic and soil moisture request classes removed


class WeatherForecastRequest(BaseModel):
    """The overall request model containing the nonce and the weather input data."""

    nonce: str | None = None
    data: WeatherInputData


def factory_router(miner_instance) -> APIRouter:
    """Create router with miner instance available to route handlers."""
    router = APIRouter()

    # Geomagnetic route handler removed

    # Soil moisture route handler removed

    async def verify_token(
        credentials: HTTPAuthorizationCredentials = Depends(security),
    ):
        """Verify JWT token and return decoded payload."""
        try:
            token = credentials.credentials
            payload = jwt.decode(
                token, MINER_JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM]
            )

            if datetime.fromtimestamp(payload["exp"], tz=timezone.utc) < datetime.now(
                timezone.utc
            ):
                logger.warning(
                    f"Token expired for job_id: {payload.get('job_id')}, file_path: {payload.get('file_path')}"
                )
                raise HTTPException(status_code=401, detail="Token has expired")

            logger.debug(
                f"Token successfully decoded and validated (not expired). Payload: {payload}"
            )
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning(f"JWT token has expired.")
            raise HTTPException(status_code=401, detail="Token has expired (signature)")
        except jwt.PyJWTError as e:
            logger.warning(f"Invalid JWT token: {e}")
            raise HTTPException(status_code=401, detail=f"Invalid token: {e}")

    async def get_forecast_file(
        request: Request, file_path: str, token: Optional[str] = None
    ):
        """Serve files from the forecast directory after validating JWT token."""

        logger.info(f"Received request for file: {file_path}")

        # Check if we're in R2 proxy mode
        file_serving_mode = getattr(
            miner_instance, "weather_task", None
        ) and miner_instance.weather_task.config.get("file_serving_mode", "local")

        if file_serving_mode == "r2_proxy":
            # Proxy the request to R2
            return await proxy_file_from_r2(request, file_path, token)

        # Original local file serving logic
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
                        logger.info(
                            f"Extracted token from path and reconstructed file_path: {file_path}"
                        )
                    else:
                        token = token_part
                        logger.info(f"Extracted token from path: {token[:10]}...")

        forecasts_dir = MINER_FORECAST_DIR
        if not forecasts_dir.is_absolute():
            forecasts_dir = forecasts_dir.absolute()

        file_path = file_path.lstrip("/")

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
                    if not (
                        file_path.startswith(token_file_path)
                        or token_file_path.startswith(file_path.rstrip("/"))
                    ):
                        logger.warning(
                            f"Token path mismatch: {token_file_path} vs {file_path}"
                        )
                        raise HTTPException(
                            status_code=403, detail="Path not authorized by token"
                        )
                else:
                    logger.warning("Token missing file_path claim")
                    raise HTTPException(
                        status_code=403, detail="Token missing file_path claim"
                    )

            except jwt.PyJWTError as jwt_error:
                logger.warning(f"Invalid JWT token: {str(jwt_error)}")
                raise HTTPException(status_code=401, detail="Invalid token")
            except Exception as e:
                logger.warning(f"Error processing token: {str(e)}")
                raise HTTPException(status_code=401, detail="Invalid token")

        zarr_suffix = ".zarr"
        if file_path.endswith(zarr_suffix) or file_path.endswith(f"{zarr_suffix}/"):
            zarr_dir_name = file_path.rstrip("/")
            zarr_path = forecasts_dir / zarr_dir_name

            if not zarr_path.exists():
                logger.warning(f"Zarr path not found: {zarr_path}")
                pattern = str(
                    forecasts_dir
                    / f"{zarr_dir_name.lower().replace(zarr_suffix, '')}*{zarr_suffix}"
                )
                matches = glob.glob(pattern, case=False)
                if matches:
                    zarr_path = Path(matches[0])
                    logger.info(f"Found case-insensitive match: {zarr_path}")

            if not zarr_path.exists():
                logger.error(f"Zarr directory not found: {zarr_path}")
                raise HTTPException(
                    status_code=404, detail=f"File not found: {zarr_dir_name}"
                )

            if zarr_path.is_dir():
                if "/" in file_path.strip("/"):
                    zarr_base, internal_path = file_path.strip("/").split("/", 1)
                    full_internal_path = zarr_path / internal_path

                    if full_internal_path.exists() and full_internal_path.is_file():
                        logger.info(
                            f"Returning specific Zarr file: {full_internal_path}"
                        )
                        return FileResponse(
                            path=str(full_internal_path),
                            filename=full_internal_path.name,
                        )

                try:
                    contents = [item.name for item in zarr_path.iterdir()]
                    logger.info(f"Listed zarr directory contents: {contents}")
                    return JSONResponse(content={"files": contents})
                except Exception as e:
                    logger.error(f"Error listing zarr directory: {str(e)}")
                    raise HTTPException(
                        status_code=500, detail=f"Error listing directory: {str(e)}"
                    )

        full_path = forecasts_dir / file_path.rstrip("/")

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
                raise HTTPException(
                    status_code=500, detail=f"Error listing directory: {str(e)}"
                )

        logger.error(f"File or directory not found: {full_path}")
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

    async def proxy_file_from_r2(
        request: Request, file_path: str, token: Optional[str] = None
    ):
        """Proxy file requests to R2 when file_serving_mode is 'r2_proxy'."""
        logger.info(f"Proxying R2 request for file: {file_path}")

        # Validate token and extract job_id
        if not token:
            # Try to extract token from various sources
            auth_header = request.headers.get("Authorization")
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header[7:]
            elif "token" in request.query_params:
                token = request.query_params.get("token")
            elif "?token=" in file_path:
                path_parts = file_path.split("?token=", 1)
                if len(path_parts) == 2:
                    token = path_parts[1].split("/")[0]
                    file_path = path_parts[0]

        if not token:
            raise HTTPException(
                status_code=401, detail="Token required for R2 proxy access"
            )

        try:
            # Decode JWT to get job information
            payload = jwt.decode(token, MINER_JWT_SECRET_KEY, algorithms=["HS256"])
            job_id = payload.get("job_id")

            if not job_id:
                raise HTTPException(
                    status_code=403, detail="Token missing job_id claim"
                )

            # Get the R2 prefix for this job
            weather_task = miner_instance.weather_task
            if not weather_task:
                raise HTTPException(
                    status_code=500, detail="Weather task not available"
                )

            # Get job details from database to find R2 prefix
            query = (
                "SELECT target_netcdf_path FROM weather_miner_jobs WHERE id = :job_id"
            )
            job_data = await weather_task.db_manager.fetch_one(
                query, {"job_id": job_id}
            )

            if not job_data or not job_data["target_netcdf_path"]:
                raise HTTPException(status_code=404, detail="Job or R2 path not found")

            r2_prefix = job_data["target_netcdf_path"]

            # Get R2 client
            s3_client = await weather_task._get_r2_s3_client()
            if not s3_client:
                raise HTTPException(status_code=500, detail="R2 client not available")

            bucket_name = weather_task.r2_config.get("r2_bucket_name")
            if not bucket_name:
                raise HTTPException(status_code=500, detail="R2 bucket not configured")

            # Handle different file path patterns
            if file_path.endswith(".zarr/") or file_path.endswith(".zarr"):
                # List files in the zarr directory
                zarr_prefix = r2_prefix.rstrip("/") + "/"

                try:
                    response = await asyncio.to_thread(
                        s3_client.list_objects_v2,
                        Bucket=bucket_name,
                        Prefix=zarr_prefix,
                    )

                    if "Contents" not in response:
                        raise HTTPException(status_code=404, detail="No files found")

                    files = []
                    for obj in response["Contents"]:
                        # Extract just the filename from the full key
                        relative_path = obj["Key"][len(zarr_prefix) :]
                        if relative_path:  # Skip empty paths
                            files.append(
                                relative_path.split("/")[0]
                            )  # Get first part after prefix

                    # Remove duplicates and return unique filenames
                    unique_files = list(set(files))
                    return JSONResponse(content={"files": unique_files})

                except Exception as e:
                    logger.error(f"Error listing R2 objects: {e}")
                    raise HTTPException(status_code=500, detail="Error listing files")

            else:
                # Request for a specific file - map to R2 object key
                if file_path.startswith("/"):
                    file_path = file_path[1:]

                # Extract the .nc filename from the path
                parts = file_path.split("/")
                if len(parts) >= 2 and parts[1].endswith(".nc"):
                    nc_filename = parts[1]
                    r2_object_key = f"{r2_prefix}/{nc_filename}"
                else:
                    raise HTTPException(status_code=400, detail="Invalid file path")

                try:
                    # Stream the file from R2
                    response = await asyncio.to_thread(
                        s3_client.get_object, Bucket=bucket_name, Key=r2_object_key
                    )

                    file_content = await asyncio.to_thread(response["Body"].read)

                    return Response(
                        content=file_content,
                        media_type="application/octet-stream",
                        headers={
                            "Content-Disposition": f"attachment; filename={nc_filename}",
                            "Content-Length": str(len(file_content)),
                        },
                    )

                except Exception as e:
                    logger.error(f"Error downloading from R2: {e}")
                    if "NoSuchKey" in str(e):
                        raise HTTPException(
                            status_code=404, detail="File not found in R2"
                        )
                    else:
                        raise HTTPException(
                            status_code=500, detail="Error downloading file"
                        )

        except jwt.PyJWTError as jwt_error:
            logger.warning(f"Invalid JWT token: {str(jwt_error)}")
            raise HTTPException(status_code=401, detail="Invalid token")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error in R2 proxy: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

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
        logger.info(
            f"Successfully decrypted weather forecast payload. Type: {type(decrypted_payload)}"
        )
        try:
            if not hasattr(miner_instance, "weather_task"):
                logger.error("Miner instance is missing the 'weather_task' attribute.")
                return JSONResponse(
                    status_code=500,
                    content={"error": "Miner not configured for weather task"},
                )

            input_data = decrypted_payload.data.model_dump()
            logger.info(
                f"Initiating weather forecast run for start time: {input_data.get('forecast_start_time')}"
            )

            try:
                logger.info("Calling miner_instance.weather_task.miner_execute...")
                result = await miner_instance.weather_task.miner_execute(
                    input_data, miner_instance
                )
                logger.info(
                    f"miner_instance.weather_task.miner_execute completed. Result: {type(result)}"
                )
            except Exception as task_exec_error:
                logger.error(
                    f"Error during miner_instance.weather_task.miner_execute: {task_exec_error}"
                )
                logger.error(traceback.format_exc())
                return JSONResponse(
                    status_code=500, content={"error": "Error during task execution"}
                )

            if not result:
                logger.error(
                    "Weather forecast execution failed (result was None or empty)"
                )
                return JSONResponse(
                    status_code=500,
                    content={"error": "Failed to execute weather forecast"},
                )

            logger.info(
                f"Successfully initiated forecast. Job ID: {result.get('job_id')}"
            )
            return JSONResponse(
                content={
                    "status": "success",
                    "message": "Forecast run initiated",
                    "job_id": result.get("job_id"),
                    "forecast_start_time": input_data.get("forecast_start_time"),
                }
            )

        except ValidationError as e:
            logger.error(f"Validation error processing weather forecast request: {e}")
            return JSONResponse(
                status_code=422,
                content={
                    "error": "Invalid request payload structure",
                    "details": e.errors(),
                },
            )
        except Exception as e:
            logger.error(f"Unhandled error in weather_forecast_require handler: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse(
                status_code=500, content={"error": f"Internal server error: {str(e)}"}
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
            if (
                not hasattr(miner_instance, "weather_task")
                or miner_instance.weather_task is None
            ):
                logger.error(
                    "Miner instance is missing the 'weather_task' attribute or it is None."
                )
                return JSONResponse(
                    status_code=500,
                    content={"error": "Miner not configured for weather task"},
                )

            request_data = decrypted_payload.data
            logger.info(f"Handling weather forecast data request...")

            job_id = request_data.job_id
            if not job_id:
                logger.error("Missing job_id in request data")
                return JSONResponse(
                    status_code=400, content={"error": "Missing job_id in request"}
                )

            response_dict = await miner_instance.weather_task.handle_kerchunk_request(
                job_id
            )

            if not isinstance(response_dict, dict):
                logger.error(
                    f"handle_kerchunk_request returned invalid type: {type(response_dict)}"
                )
                return JSONResponse(
                    status_code=500,
                    content={
                        "error": "Internal error processing forecast data request"
                    },
                )

            zarr_store_url = response_dict.get("zarr_store_url")
            if not zarr_store_url and "kerchunk_json_url" in response_dict:
                zarr_store_url = response_dict.get("kerchunk_json_url")

            response_data = WeatherKerchunkResponseData(
                status=response_dict.get("status", "error"),
                message=response_dict.get("message", "Failed to process"),
                zarr_store_url=zarr_store_url,
                verification_hash=response_dict.get("verification_hash"),
                access_token=response_dict.get("access_token"),
            )

            return JSONResponse(content=response_data.model_dump())

        except Exception as e:
            logger.error(f"Error processing weather forecast data request: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse(
                status_code=500, content={"error": f"Internal server error: {str(e)}"}
            )

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
            if (
                not hasattr(miner_instance, "weather_task")
                or miner_instance.weather_task is None
            ):
                logger.error(
                    "Miner not configured for weather task (weather_task missing or None)."
                )
                return JSONResponse(
                    status_code=500,
                    content={"error": "Miner not configured for weather task"},
                )

            response_data = await miner_instance.weather_task.handle_initiate_fetch(
                request_data=decrypted_payload.data
            )

            if not isinstance(response_data, dict):
                logger.error(
                    f"handle_initiate_fetch returned invalid type: {type(response_data)}"
                )
                return JSONResponse(
                    status_code=500,
                    content={"error": "Internal error processing fetch initiation"},
                )

            response_model = WeatherInitiateFetchResponse(**response_data)
            return JSONResponse(content=response_model.model_dump())

        except ValidationError as e:
            logger.error(f"Validation error processing initiate fetch request: {e}")
            return JSONResponse(
                status_code=422,
                content={"error": "Invalid request payload", "details": e.errors()},
            )
        except Exception as e:
            logger.error(
                f"Error in /weather-initiate-fetch handler: {e}", exc_info=True
            )
            return JSONResponse(
                status_code=500, content={"error": f"Internal server error: {str(e)}"}
            )

    async def weather_poll_job_status(
        decrypted_payload: WeatherGetInputStatusRequest = Depends(
            partial(decrypt_general_payload, WeatherGetInputStatusRequest)
        ),
    ):
        """
        NEW SIMPLIFIED ENDPOINT: Validator polls for job status (GFS fetch + inference).
        Returns current status without input hash verification.
        """
        logger.info("Entered /weather-poll-job-status handler.")
        try:
            if (
                not hasattr(miner_instance, "weather_task")
                or miner_instance.weather_task is None
            ):
                logger.error(
                    "Miner not configured for weather task (weather_task missing or None)."
                )
                return JSONResponse(
                    status_code=500,
                    content={"error": "Miner not configured for weather task"},
                )

            job_id = decrypted_payload.data.job_id
            if not job_id:
                return JSONResponse(
                    status_code=400, content={"error": "Missing job_id in request"}
                )

            # Query job status from database
            query = """
                SELECT status, error_message, runpod_job_id, target_netcdf_path
                FROM weather_miner_jobs 
                WHERE id = :job_id
            """
            result = await miner_instance.weather_task.db_manager.fetch_one(
                query, {"job_id": job_id}
            )

            if not result:
                return JSONResponse(
                    content={
                        "status": WeatherTaskStatus.NOT_FOUND.value,
                        "job_id": job_id,
                        "message": "Job not found",
                    }
                )

            # Map internal status to external status for validator
            internal_status = result["status"]
            if internal_status in ["fetch_queued", "fetching_gfs", "hashing_input"]:
                external_status = WeatherTaskStatus.FETCH_PROCESSING.value
            elif internal_status in ["input_hashed_awaiting_validation", "in_progress"]:
                external_status = WeatherTaskStatus.INFERENCE_RUNNING.value
            elif internal_status == "completed":
                external_status = WeatherTaskStatus.FETCH_COMPLETED.value
            elif internal_status in ["failed", "error", "fetch_error"]:
                external_status = WeatherTaskStatus.FETCH_ERROR.value
            else:
                external_status = internal_status

            response = {
                "status": external_status,
                "job_id": job_id,
                "message": result.get("error_message", ""),
            }

            # Include output path if inference is complete
            if internal_status == "completed" and result.get("target_netcdf_path"):
                response["output_ready"] = True
                
            logger.info(f"Job {job_id} status: {external_status}")
            return JSONResponse(content=response)

        except Exception as e:
            logger.error(
                f"Error in /weather-poll-job-status handler: {e}", exc_info=True
            )
            return JSONResponse(
                status_code=500, content={"error": f"Internal server error: {str(e)}"}
            )

    async def weather_get_input_status_require(
        decrypted_payload: WeatherGetInputStatusRequest = Depends(
            partial(decrypt_general_payload, WeatherGetInputStatusRequest)
        ),
    ):
        """
        LEGACY: Handles Step 3: Validator polls for the status of the GFS fetch/hash process.
        Miner returns the job status and the input hash if available.
        DEPRECATED: Use /weather-poll-job-status instead.
        """
        logger.info("Entered /weather-poll-job-status handler.")
        try:
            if (
                not hasattr(miner_instance, "weather_task")
                or miner_instance.weather_task is None
            ):
                logger.error(
                    "Miner not configured for weather task (weather_task missing or None)."
                )
                return JSONResponse(
                    status_code=500,
                    content={"error": "Miner not configured for weather task"},
                )

            job_id = decrypted_payload.data.job_id
            if not job_id:
                return JSONResponse(
                    status_code=400, content={"error": "Missing job_id in request"}
                )

            response_data = await miner_instance.weather_task.handle_get_input_status(
                job_id
            )

            if not isinstance(response_data, dict):
                logger.error(
                    f"handle_get_input_status returned invalid type: {type(response_data)}"
                )
                return JSONResponse(
                    status_code=500,
                    content={"error": "Internal error fetching input status"},
                )

            response_model = WeatherGetInputStatusResponse(**response_data)
            return JSONResponse(content=response_model.model_dump())

        except ValidationError as e:
            logger.error(f"Validation error processing get input status request: {e}")
            return JSONResponse(
                status_code=422,
                content={"error": "Invalid request payload", "details": e.errors()},
            )
        except Exception as e:
            logger.error(
                f"Error in /weather-poll-job-status handler: {e}", exc_info=True
            )
            return JSONResponse(
                status_code=500, content={"error": f"Internal server error: {str(e)}"}
            )

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
            if (
                not hasattr(miner_instance, "weather_task")
                or miner_instance.weather_task is None
            ):
                logger.error(
                    "Miner not configured for weather task (weather_task missing or None)."
                )
                return JSONResponse(
                    status_code=500,
                    content={"error": "Miner not configured for weather task"},
                )

            job_id = decrypted_payload.data.job_id
            if not job_id:
                return JSONResponse(
                    status_code=400, content={"error": "Missing job_id in request"}
                )

            response_data = await miner_instance.weather_task.handle_start_inference(
                job_id
            )

            if not isinstance(response_data, dict):
                logger.error(
                    f"handle_start_inference returned invalid type: {type(response_data)}"
                )
                return JSONResponse(
                    status_code=500,
                    content={"error": "Internal error starting inference"},
                )

            response_model = WeatherStartInferenceResponse(**response_data)
            return JSONResponse(content=response_model.model_dump())

        except ValidationError as e:
            logger.error(f"Validation error processing start inference request: {e}")
            return JSONResponse(
                status_code=422,
                content={"error": "Invalid request payload", "details": e.errors()},
            )
        except Exception as e:
            logger.error(
                f"Error in /weather-start-inference handler: {e}", exc_info=True
            )
            return JSONResponse(
                status_code=500, content={"error": f"Internal server error: {str(e)}"}
            )

    # Geomagnetic and soil moisture API routes disabled

    if (
        hasattr(miner_instance, "weather_task")
        and miner_instance.weather_task is not None
    ):
        router.add_api_route(
            "/forecasts/{file_path:path}",
            get_forecast_file,
            tags=["Weather"],
            methods=["GET", "HEAD"],
            response_class=Response,
            response_model=None,
        )
        # Route for triggering weather forecast run
        router.add_api_route(
            "/weather-forecast-request",
            weather_forecast_require,
            tags=["Weather"],
            dependencies=[
                Depends(blacklist_low_stake), 
                Depends(verify_request),
                Depends(hotkey_blacklist_checker)  # Add custom blacklist check
            ],
            methods=["POST"],
            response_class=JSONResponse,
        )
        # Route for validator to request Kerchunk JSON metadata
        router.add_api_route(
            "/weather-kerchunk-request",
            weather_kerchunk_require,
            tags=["Weather"],
            dependencies=[
                Depends(blacklist_low_stake), 
                Depends(verify_request),
                Depends(hotkey_blacklist_checker)  # Add custom blacklist check
            ],
            methods=["POST"],
            response_class=JSONResponse,
        )
        router.add_api_route(
            "/weather-initiate-fetch",
            weather_initiate_fetch_require,
            tags=["Weather"],
            dependencies=[
                Depends(blacklist_low_stake), 
                Depends(verify_request),
                Depends(hotkey_blacklist_checker)  # Add custom blacklist check
            ],
            methods=["POST"],
            response_class=JSONResponse,
        )
        # Use the new simplified polling endpoint for job status
        router.add_api_route(
            "/weather-poll-job-status",
            weather_poll_job_status,
            tags=["Weather"],
            dependencies=[
                Depends(blacklist_low_stake), 
                Depends(verify_request),
                Depends(hotkey_blacklist_checker)  # Add custom blacklist check
            ],
            methods=["POST"],
            response_class=JSONResponse,
        )
        router.add_api_route(
            "/weather-start-inference",
            weather_start_inference_require,
            tags=["Weather"],
            dependencies=[
                Depends(blacklist_low_stake), 
                Depends(verify_request),
                Depends(hotkey_blacklist_checker)  # Add custom blacklist check
            ],
            methods=["POST"],
            response_class=JSONResponse,
        )
        logger.info("Weather routes registered (weather task is enabled)")
    else:
        logger.info("Weather routes NOT registered (weather task is disabled)")

    return router
