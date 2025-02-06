import threading
from typing import Any, Dict, Optional
import httpx
from fiber.logging_utils import get_logger
import numpy as np
import asyncio
import math

logger = get_logger(__name__)


class GaiaCommunicator:
    def __init__(self, endpoint: str = "/Validator/Info", client: httpx.AsyncClient = None):
        """
        Initialize the communicator with Gaia API base URL and endpoint.

        Args:
            endpoint: API endpoint path (default is '/Validator/Info').
            client: Optional existing httpx.AsyncClient to use. If not provided, creates a new one.
        """
        api_base = "https://dev-gaia-api.azurewebsites.net"
        self.endpoint = f"{api_base}{endpoint}"
        
        # Configure client with optimized settings for concurrent requests
        if client:
            self.client = client
            self._should_close_client = False
        else:
            self.client = httpx.AsyncClient(
                timeout=30.0,
                verify=False,
                limits=httpx.Limits(
                    max_keepalive_connections=20,
                    max_connections=30,
                    keepalive_expiry=30.0
                ),
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'User-Agent': 'GaiaValidator/1.0'
                }
            )
            self._should_close_client = True
            
        # Concurrency control
        self._request_semaphore = asyncio.Semaphore(10)  # Limit concurrent requests
        self._retry_delays = [1, 2, 4, 8, 16]  # Exponential backoff delays

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._should_close_client:
            await self.client.aclose()

    async def send_data(self, data: Dict[str, Any]) -> None:
        """
        Send detailed data to the Gaia server with improved retry logic and concurrency control.

        Args:
            data: Dictionary containing payload to be sent.
        """
        current_thread = threading.current_thread().name

        if not self._validate_payload(data):
            logger.error(f"| {current_thread} | ❗ Invalid payload structure: {data}")
            return

        # Validate numeric values in predictions
        data = self._validate_predictions(data)
        if not data:
            logger.error(f"| {current_thread} | ❗ No valid predictions after validation")
            return

        async with self._request_semaphore:  # Control concurrent requests
            for attempt, delay in enumerate(self._retry_delays):
                try:
                    response = await self.client.post(self.endpoint, json=data)
                    
                    if response.is_success:
                        logger.info(f"| {current_thread} | ✅ Data sent to Gaia successfully")
                        return
                    
                    if response.status_code == 429:  # Rate limit
                        if attempt < len(self._retry_delays) - 1:
                            logger.warning(f"| {current_thread} | Rate limit hit, retrying in {delay}s...")
                            await asyncio.sleep(delay)
                            continue
                    
                    # Log error details
                    try:
                        error_details = response.json()
                    except ValueError:
                        error_details = response.text
                    
                    logger.warning(f"| {current_thread} | ❗ HTTP error {response.status_code}: {error_details}")
                    if attempt < len(self._retry_delays) - 1:
                        await asyncio.sleep(delay)
                        continue
                    break
                    
                except httpx.RequestError as e:
                    logger.warning(f"| {current_thread} | ❗ Request error: {str(e)}")
                    if attempt < len(self._retry_delays) - 1:
                        await asyncio.sleep(delay)
                        continue
                    raise
                except Exception as e:
                    logger.error(f"| {current_thread} | ❗ Unexpected error: {str(e)}")
                    raise

    def _validate_predictions(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate and clean prediction data."""
        try:
            # Handle geomagnetic predictions
            if data.get("geomagneticPredictions"):
                valid_predictions = []
                for prediction in data["geomagneticPredictions"]:
                    try:
                        numeric_fields = [
                            "geomagneticPredictedValue",
                            "geomagneticGroundTruthValue",
                            "geomagneticScore"
                        ]
                        
                        is_valid = True
                        for field in numeric_fields:
                            value = prediction.get(field)
                            try:
                                float_value = float(value)
                                if math.isnan(float_value) or math.isinf(float_value):
                                    is_valid = False
                                    break
                                prediction[field] = float_value
                            except (ValueError, TypeError):
                                is_valid = False
                                break
                        
                        if is_valid:
                            valid_predictions.append(prediction)
                        
                    except Exception as e:
                        logger.error(f"Error validating prediction: {e}")
                        continue
                
                data["geomagneticPredictions"] = valid_predictions

            # Handle soil moisture predictions
            if data.get("soilMoisturePredictions"):
                for prediction in data["soilMoisturePredictions"]:
                    # Validate and format bounds
                    bounds = prediction.get("sentinelRegionBounds")
                    if isinstance(bounds, list) and len(bounds) == 4:
                        prediction["sentinelRegionBounds"] = f"[{','.join(map(str, bounds))}]"
                    elif not isinstance(bounds, str) or not bounds:
                        prediction["sentinelRegionBounds"] = "[]"
                    
                    # Validate CRS
                    crs = prediction.get("sentinelRegionCrs")
                    if not isinstance(crs, int):
                        prediction["sentinelRegionCrs"] = 4326

                    # Validate array fields
                    array_fields = [
                        "soilSurfacePredictedValues",
                        "soilRootzonePredictedValues",
                        "soilSurfaceGroundTruthValues",
                        "soilRootzoneGroundTruthValues"
                    ]
                    
                    for field in array_fields:
                        value = prediction.get(field)
                        if isinstance(value, (list, np.ndarray)):
                            prediction[field] = str(value).replace('array(', '').replace(')', '')
                        elif not isinstance(value, str):
                            prediction[field] = "[]"

            return data

        except Exception as e:
            logger.error(f"Error in prediction validation: {e}")
            return None

    def _validate_payload(self, data: Dict[str, Any]) -> bool:
        """Validate the payload structure."""
        required_fields = ["minerHotKey", "minerColdKey", "geomagneticPredictions", "soilMoisturePredictions"]
        
        # Check required fields
        for field in required_fields:
            if field not in data:
                logger.error(f"Missing required field: {field}")
                return False

        # Validate prediction arrays
        if not isinstance(data.get("geomagneticPredictions", []), list):
            logger.error("Invalid data type for geomagneticPredictions: Must be a list")
            return False
        if not isinstance(data.get("soilMoisturePredictions", []), list):
            logger.error("Invalid data type for soilMoisturePredictions: Must be a list")
            return False

        return True


if __name__ == "__main__":
    communicator = GaiaCommunicator(endpoint="/Predictions")
    example_payload = {
        "minerHotKey": "hotkey_123",
        "minerColdKey": "coldkey_456",
        "geomagneticPredictions": [
            {
                "predictionId": 1,
                "predictionDate": "2024-12-18T15:00:00Z",
                "geomagneticPredictionTargetDate": "2024-12-18T14:00:00Z",
                "geomagneticPredictionInputDate": "2024-12-18T13:00:00Z",
                "geomagneticPredictedValue": 45.6,
                "geomagneticGroundTruthValue": 42.0,
                "geomagneticScore": 3.6,
                "scoreGenerationDate": "2024-12-18T14:45:30Z"
            }
        ],
        "soilMoisturePredictions": [
            {
                "predictionId": 1,
                "predictionDate": "2024-12-18T15:00:00Z",
                "soilPredictionRegionId": 101,
                "sentinelRegionBounds": "[10.0, 20.0, 30.0, 40.0]",
                "sentinelRegionCrs": 4326,
                "soilPredictionTargetDate": "2024-12-18T14:00:00Z",
                "soilSurfaceRmse": 0.02,
                "soilRootzoneRmse": 0.03,
                "soilSurfacePredictedValues": "[[0.1, 0.2], [0.3, 0.4]]",
                "soilRootzonePredictedValues": "[[0.5, 0.6], [0.7, 0.8]]",
                "soilSurfaceGroundTruthValues": "[[0.15, 0.25], [0.35, 0.45]]",
                "soilRootzoneGroundTruthValues": "[[0.55, 0.65], [0.75, 0.85]]",
                "soilSurfaceStructureScore": 0.9,
                "soilRootzoneStructureScore": 0.92,
                "soilPredictionInput": "input.tif",
                "soilPredictionOutput": "output.tif",
                "scoreGenerationDate": "2024-12-18T14:45:30Z"
            }
        ]
    }

    communicator.send_data(example_payload)

