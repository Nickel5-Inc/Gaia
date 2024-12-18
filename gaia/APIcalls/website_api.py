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
                    'Accept': '*/*',
                    'Content-Type': 'application/json'
                }
            )
            response.raise_for_status()
            bt.logging.info(f"| {current_thread} | ✅ Data sent to Gaia successfully: {data}")

        except requests.exceptions.HTTPError as e:
            bt.logging.warning(f"| {current_thread} | ❗ HTTP error occurred: {e}. Payload: {data}.")
            if e.response is not None:
                bt.logging.warning(
                    f"| {current_thread} | ❗ Response content: {e.response.text}")
        except requests.exceptions.RequestException as e:
            bt.logging.warning(
                f"| {current_thread} | ❗ Error sending data to Gaia API. Error: {e}. Payload: {data}.")

if __name__ == "__main__":
    communicator = GaiaCommunicator()
    example_payload = {
        "minerUID": 123,
        "minerHotKey": "hotkey_123",
        "minerColdKey": "coldkey_456",
        "geomagneticPredictedValue": 45.6,
        "geomagneticScore": 3.4,
        "soilSurfaceRMSE": 0.02,
        "soilRootzoneRMSE": 0.04,
        "soilSurfaceStructureScore": 0.95,
        "soilRootzoneStructureScore": 0.92,
        "scoreGenerationDate": "2024-12-18T12:00:00Z"
    }
    communicator.send_data(data=example_payload)
