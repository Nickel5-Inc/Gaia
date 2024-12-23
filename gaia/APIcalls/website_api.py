import threading
from typing import Any, Dict
import requests
import bittensor as bt


class GaiaCommunicator:
    def __init__(self, endpoint: str = "Validator/Info"):
        """
        Initialize the communicator with Gaia API base URL and endpoint.

        Args:
            endpoint: API endpoint path (default is '/Validator/Info').
        """
        api_base = "https://dev-gaia-api.azurewebsites.net"
        self.endpoint = f"{api_base}/{endpoint}"

    def send_data(self, data: Dict[str, Any]) -> None:
        """
        Send detailed data to the Gaia server.

        Args:
            data: Dictionary containing payload to be sent.

        Returns:
            None
        """
        current_thread = threading.current_thread().name

        try:
            response = requests.post(
                self.endpoint,
                json=data,
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                }
            )
            response.raise_for_status()
            bt.logging.info(f"| {current_thread} | ✅ Data sent to Gaia successfully: {data}")
            bt.logging.info(f"| {current_thread} | Response: {response.status_code}, {response.text}")

        except requests.exceptions.HTTPError as e:
            bt.logging.warning(f"| {current_thread} | ❗ HTTP error occurred: {e}. Payload: {data}.")
            if e.response is not None:
                bt.logging.warning(
                    f"| {current_thread} | ❗ Response content: {e.response.text}")
        except requests.exceptions.RequestException as e:
            bt.logging.warning(
                f"| {current_thread} | ❗ Error sending data to Gaia API. Error: {e}. Payload: {data}.")


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
