import threading
from typing import Any, Dict
import requests
import bittensor as bt


class GaiaCommunicator:
    def __init__(self, endpoint: str = "/Validator/Info"):
        """
        Initialize the communicator with Gaia API base URL and endpoint.

        Args:
            endpoint: API endpoint path (default is '/Validator/Info').
        """
        api_base = "https://dev-gaia-api.azurewebsites.net"
        self.endpoint = f"{api_base}{endpoint}"

    def send_data(self, data: Dict[str, Any]) -> None:
        """
        Send detailed data to the Gaia server.

        Args:
            data: Dictionary containing payload to be sent.
        """
        current_thread = threading.current_thread().name

        # Validate payload structure
        if not self._validate_payload(data):
            bt.logging.error(f"| {current_thread} | ❗ Invalid payload structure: {data}")
            return

        try:
            # Send the request
            response = requests.post(
                self.endpoint,
                json=data,
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                }
            )
            response.raise_for_status()  # Raise error for HTTP status codes 4xx/5xx
            bt.logging.info(f"| {current_thread} | ✅ Data sent to Gaia successfully: {data}")
            bt.logging.info(f"| {current_thread} | Response: {response.status_code}, {response.json()}")

        except requests.exceptions.HTTPError as e:
            # Improved logging for HTTP errors
            error_details = e.response.json() if e.response and e.response.headers.get(
                'Content-Type') == 'application/json' else e.response.text
            bt.logging.warning(
                f"| {current_thread} | ❗ HTTP error occurred: {e}. Payload: {data}. Response: {error_details}")

        except requests.exceptions.RequestException as e:
            # Handle general request errors
            bt.logging.warning(
                f"| {current_thread} | ❗ Error sending data to Gaia API. Error: {e}. Payload: {data}.")

        except Exception as e:
            # Catch-all for unexpected errors
            bt.logging.error(f"| {current_thread} | ❗ Unexpected error: {e}. Payload: {data}.")

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
                bt.logging.error(f"Missing required field: {field}")
                return False

        # Ensure geomagneticPredictions and soilMoisturePredictions are lists
        if not isinstance(data.get("geomagneticPredictions", []), list):
            bt.logging.error("Invalid data type for geomagneticPredictions: Must be a list")
            return False
        if not isinstance(data.get("soilMoisturePredictions", []), list):
            bt.logging.error("Invalid data type for soilMoisturePredictions: Must be a list")
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

