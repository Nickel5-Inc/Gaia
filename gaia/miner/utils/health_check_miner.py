import os
import sys
import logging
from unittest.mock import patch
from dotenv import load_dotenv
from fiber.chain import chain_utils
from gaia.miner.miner import Miner
import requests


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def check_environment_variables(required_vars):
    """
    Check if required environment variables are set.
    """
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        return False
    logger.info("All required environment variables are set.")
    return True


def check_subtensor_endpoint(endpoint):
    """
    Check if the Subtensor chain endpoint is reachable.
    """
    try:
        # Attempt a POST request with dummy payload (adjust based on API requirements)
        response = requests.post(endpoint, json={"test": "health_check"}, timeout=5)

        if response.status_code in [200, 405]:  # Allow 405 if it's a known expected behavior
            logger.info("Subtensor endpoint is reachable.")
            return True
        else:
            logger.error(f"Subtensor endpoint returned status code {response.status_code}.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to reach Subtensor endpoint: {e}")
    return False


def check_miner_initialization():
    """
    Check if the Miner class initializes without errors.
    """
    try:
        # Simulate arguments
        class Args:
            wallet = None
            hotkey = None
            netuid = None
            port = 8091
            subtensor = type('subtensor', (), {"chain_endpoint": None, "network": None})

        # Mock model loading and any file creation
        with patch("gaia.tasks.defined_tasks.soilmoisture.soil_miner_preprocessing.SoilMinerPreprocessing._load_model") as mock_load_model, \
             patch("os.makedirs") as mock_makedirs, \
             patch("builtins.open") as mock_open, \
             patch("tempfile.NamedTemporaryFile") as mock_tempfile, \
             patch("os.unlink") as mock_unlink:
            # Mock return values to prevent unnecessary operations
            mock_load_model.return_value = None  # Simulate successful model loading
            mock_makedirs.return_value = None
            mock_open.return_value = None
            mock_tempfile.return_value.__enter__.return_value.name = "/mock/tempfile.tif"
            mock_unlink.return_value = None

            # Initialize the Miner class
            miner = Miner(Args())

        logger.info("Miner initialized successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Miner: {e}")
        return False


def run_health_checks():
    """
    Run all health checks for miner.py.
    """
    logger.info("Starting health checks for miner.py...")

    # Mock environment variables for local testing
    os.environ["WALLET_NAME"] = "test_wallet"
    os.environ["HOTKEY_NAME"] = "test_hotkey"
    os.environ["SUBTENSOR_ADDRESS"] = "https://test.finney.opentensor.ai:443/"
    os.environ["NETUID"] = "237"
    os.environ["PORT"] = "8091"

    # Check required environment variables
    required_vars = ["WALLET_NAME", "HOTKEY_NAME", "SUBTENSOR_ADDRESS", "NETUID", "PORT"]
    if not check_environment_variables(required_vars):
        logger.error("Environment variable check failed.")
        return False

    # Check Subtensor endpoint
    subtensor_address = os.getenv("SUBTENSOR_ADDRESS")
    if not check_subtensor_endpoint(subtensor_address):
        logger.error("Subtensor endpoint check failed.")
        return False

    # Check Miner initialization
    if not check_miner_initialization():
        logger.error("Miner initialization check failed.")
        return False

    logger.info("All health checks passed successfully.")
    return True


if __name__ == "__main__":
    if not run_health_checks():
        logger.error("Health checks failed.")
        sys.exit(1)
    logger.info("Health checks completed successfully.")
    sys.exit(0)
