import threading
from typing import Any
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

    def send_data(self, version: str) -> None:
        """
        Send version data to the Gaia server.

        Args:
            version: Version string to be sent in the request body.

        Returns:
            None
        """
        current_thread = threading.current_thread().name
        payload = {"version": version}  # Aligns with Gaia API schema

        try:
            response = requests.post(
                self.endpoint,
                json=payload,
                headers={
                    'Accept': '*/*',
                    'Content-Type': 'application/json'
                }
            )
            response.raise_for_status()
            bt.logging.info(f"| {current_thread} | ✅ Data sent to Gaia site successfully: {payload}")

        except requests.exceptions.HTTPError as e:
            bt.logging.warning(f"| {current_thread} | ❗ HTTP error occurred: {e}. Payload: {payload}.")
            if e.response is not None:
                bt.logging.warning(
                    f"| {current_thread} | ❗ Response content: {e.response.text}")
        except requests.exceptions.RequestException as e:
            bt.logging.warning(
                f"| {current_thread} | ❗ Error sending data to Gaia API. Error: {e}. Payload: {payload}.")

if __name__ == "__main__":
    communicator = GaiaCommunicator()
    communicator.send_data(version="1.0.0")
