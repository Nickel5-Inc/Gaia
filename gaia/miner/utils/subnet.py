from functools import partial  # Add this line at the top of your file
from fastapi import Depends, Request
from fastapi.responses import JSONResponse
from fastapi.routing import APIRouter
from pydantic import BaseModel
from fiber.miner.dependencies import blacklist_low_stake, verify_request
from fiber.miner.security.encryption import decrypt_general_payload
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

# Define max request size (5MB in bytes)
MAX_REQUEST_SIZE = 5 * 1024 * 1024  # 5MB

logger = get_logger(__name__)


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

def factory_router(miner_instance) -> APIRouter:
    """Create router with miner instance available to route handlers."""
    router = APIRouter()

    async def geomagnetic_require(
        decrypted_payload: GeomagneticRequest = Depends(
            partial(decrypt_general_payload, GeomagneticRequest),
        ),
    ):
        logger.info(f"Received decrypted payload: {decrypted_payload}")
        result = None
        
        try:
            if decrypted_payload.data:
                logger.info(f"Received data: {decrypted_payload.data}")
                response_data = decrypted_payload.model_dump()
                geomagnetic_task = GeomagneticTask()
                #logger.info(f"Received response data: {response_data}")
                logger.info(f"Miner executing geomagnetic prediction ...")
                result = geomagnetic_task.miner_execute(response_data, miner_instance)
                logger.info(f"Miner execution completed: {result}")

                # Ensure prediction is valid for JSON serialization
                if result and 'predicted_values' in result:
                    pred_value = result['predicted_values']
                    if np.isnan(pred_value) or np.isinf(pred_value):
                        result['predicted_values'] = float(0.0)
        except Exception as e:
            logger.error(f"Error in geomagnetic_require: {e}")
            result = {
                "predicted_values": 0.0,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "miner_hotkey": "error_fallback"
            }
        
        return JSONResponse(content=result)

    async def soilmoisture_require(
        decrypted_payload: SoilmoistureRequest = Depends(
            partial(decrypt_general_payload, SoilmoistureRequest)
        ),
    ):
        """Handle soil moisture prediction requests."""
        logger.info("Received soil moisture request")
        try:
            if decrypted_payload.data:
                soil_task = SoilMoistureTask(node_type="miner")
                payload_data = {
                    "data": decrypted_payload.data.model_dump(),
                    "nonce": decrypted_payload.nonce
                }
                
                result = await soil_task.miner_execute(payload_data, miner_instance)
                
                if result is None:
                    return JSONResponse(
                        status_code=500,
                        content={"error": "Failed to process soil moisture prediction"}
                    )
                    
                return JSONResponse(content=result)
                
        except Exception as e:
            logger.error(f"Error processing soil moisture request: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse(
                status_code=500,
                content={"error": str(e)}
            )

    router.add_api_route(
        "/geomagnetic-request",
        geomagnetic_require,
        tags=["Geomagnetic"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        methods=["POST"],
    )
    router.add_api_route(
        "/soilmoisture-request",
        soilmoisture_require,
        tags=["Soilmoisture"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        methods=["POST"],
    )
    return router
