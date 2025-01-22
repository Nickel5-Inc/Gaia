import threading
from typing import Any, Dict
import httpx
from fiber.logging_utils import get_logger
import numpy as np
import asyncio
import math

logger = get_logger(__name__)


class GaiaCommunicator:
    def __init__(self, endpoint: str = "/Validator/Info"):
        """
        Initialize the communicator with Gaia API base URL and endpoint.

        Args:
            endpoint: API endpoint path (default is '/Validator/Info').
        """
        api_base = "https://dev-gaia-api.azurewebsites.net"
        self.endpoint = f"{api_base}{endpoint}"
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'User-Agent': 'GaiaValidator/1.0'
            }
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def send_data(self, data: Dict[str, Any]) -> None:
        """
        Send detailed data to the Gaia server.

        Args:
            data: Dictionary containing payload to be sent.
        """
        current_thread = threading.current_thread().name
        max_retries = 3
        base_delay = 1

        if not self._validate_payload(data):
            logger.error(f"| {current_thread} | ❗ Invalid payload structure: {data}")
            return

        # Validate and convert numeric values in geomagnetic predictions
        if data.get("geomagneticPredictions"):
            valid_predictions = []
            for prediction in data["geomagneticPredictions"]:
                try:
                    # Check all required numeric fields
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
                                logger.warning(f"Invalid {field}: {value} (NaN or Inf)")
                                break
                            prediction[field] = float_value
                        except (ValueError, TypeError):
                            is_valid = False
                            logger.warning(f"Invalid {field}: {value} (not convertible to float)")
                            break
                    
                    if is_valid:
                        valid_predictions.append(prediction)
                    
                except Exception as e:
                    logger.error(f"Error processing prediction: {e}")
                    continue
            
            # Replace original predictions with only valid ones
            data["geomagneticPredictions"] = valid_predictions
            if not valid_predictions:
                logger.warning("No valid geomagnetic predictions after filtering")

        if data.get("soilMoisturePredictions"):
            for prediction in data["soilMoisturePredictions"]:
                bounds = prediction.get("sentinelRegionBounds")
                if isinstance(bounds, list) and len(bounds) == 4:
                    prediction["sentinelRegionBounds"] = f"[{','.join(map(str, bounds))}]"
                elif not isinstance(bounds, str) or not bounds:
                    prediction["sentinelRegionBounds"] = "[]"
                
                crs = prediction.get("sentinelRegionCrs")
                if not isinstance(crs, int):
                    prediction["sentinelRegionCrs"] = 4326

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


        for attempt in range(max_retries):
            try:
                response = await self.client.post(self.endpoint, json=data)
                
                if response.is_success:
                    logger.info(f"| {current_thread} | ✅ Data sent to Gaia successfully")
                    return
                
                if response.status_code == 429:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"| {current_thread} | Rate limit hit, retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                
                try:
                    error_details = response.json()
                except ValueError:
                    error_details = response.text
                
                logger.warning(f"| {current_thread} | ❗ HTTP error {response.status_code}: {error_details}")
                
            except httpx.RequestError as e:
                logger.warning(f"| {current_thread} | ❗ Error sending data to Gaia API: {str(e)}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                    continue
                raise

    def _validate_payload(self, data: Dict[str, Any]) -> bool:
        """
        Validate the payload structure against the API schema.

        Args:
            data: Payload to validate.

        Returns:
            bool: True if valid, False otherwise.
        """
        required_fields = ["minerHotKey", "minerColdKey", "geomagneticPredictions", "soilMoisturePredictions"]
        for field in required_fields:
            if field not in data:
                logger.error(f"Missing required field: {field}")
                return False

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

