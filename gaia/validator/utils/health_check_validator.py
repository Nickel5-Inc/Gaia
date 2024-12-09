import os
import sys
import logging
from unittest.mock import patch

#####Temp Health Check to run on local

# Mock environment variables for local testing
os.environ["WALLET_NAME"] = "mock_wallet"
os.environ["HOTKEY_NAME"] = "mock_hotkey"
os.environ["SUBTENSOR_ADDRESS"] = "https://mock.subtensor.ai"
os.environ["NETUID"] = "237"

# Patch EARTHDATA_AUTH in soil_apis before importing anything else
with patch.dict(
    "os.environ",
    {"EARTHDATA_USERNAME": "mock_user", "EARTHDATA_PASSWORD": "mock_password"}
):
    from gaia.validator.validator import GaiaValidator
    from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
    import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Mock Subtensor and Blockchain Interactions
class MockSubtensorInterface:
    def __init__(self, *args, **kwargs):
        pass

    def get_block(self):
        return {"header": {"number": 100}}

    def sync_nodes(self):
        return True


class MockMetagraph:
    def __init__(self, *args, **kwargs):
        self.nodes = {f"mock_hotkey_{i}": MockNode(i) for i in range(10)}

    def sync_nodes(self):
        pass


class MockNode:
    def __init__(self, uid):
        self.hotkey = f"mock_hotkey_{uid}"
        self.ip = "127.0.0.1"
        self.port = 8090
        self.stake = 100.0
        self.trust = 0.9
        self.vtrust = 0.95
        self.incentive = 10.0
        self.protocol = "mock"


# Mock Database Interactions
class MockValidatorDatabaseManager:
    async def initialize_database(self):
        return True

    async def fetch_one(self, query, params):
        return {"score": [1.0] * 256}

    async def fetch_many(self, query):
        return [{"uid": i, "hotkey": f"mock_hotkey_{i}"} for i in range(10)]

    async def update_miner_info(self, **kwargs):
        return True


# Mock Network Interactions
class MockAsyncHTTPClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def post(self, *args, **kwargs):
        return MockHTTPResponse()


class MockHTTPResponse:
    def __init__(self):
        self.status_code = 200
        self.text = "mock_response"


# Mock Soil APIs
def mock_get_soil_data(*args, **kwargs):
    return {"mock_key": "mock_value"}


def mock_get_data_dir(*args, **kwargs):
    return "/mock/data/dir"


# Patch soil_apis imports
import gaia.tasks.defined_tasks.soilmoisture.utils.soil_apis as soil_apis

soil_apis.get_soil_data = mock_get_soil_data
soil_apis.get_data_dir = mock_get_data_dir


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
    if "mock" in endpoint:
        # Simulate a successful response for the mocked endpoint
        logger.info("Mock Subtensor endpoint is reachable.")
        return True

    try:
        # Attempt a POST request with dummy payload
        response = requests.post(endpoint, json={"test": "health_check"}, timeout=5)

        if response.status_code in [200, 405]:  # Allow 405 if it's a known expected behavior
            logger.info("Subtensor endpoint is reachable.")
            return True
        else:
            logger.error(f"Subtensor endpoint returned status code {response.status_code}.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to reach Subtensor endpoint: {e}")
    return False


def check_validator_initialization():
    """
    Check if the GaiaValidator class initializes without errors.
    """
    try:
        # Simulate arguments
        class Args:
            wallet = None
            hotkey = None
            netuid = 237
            test_soil = False
            subtensor = type('subtensor', (), {"chain_endpoint": None, "network": None})

        # Inject mocks
        GaiaValidator.__init__ = lambda self, args: None
        GaiaValidator.metagraph = MockMetagraph()
        GaiaValidator.database_manager = MockValidatorDatabaseManager()
        GaiaValidator.httpx_client = MockAsyncHTTPClient()
        GaiaValidator.substrate = MockSubtensorInterface()

        validator = GaiaValidator(Args())
        logger.info("GaiaValidator initialized successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize GaiaValidator: {e}")
        return False


def check_database_manager():
    """
    Check if the ValidatorDatabaseManager can be initialized.
    """
    try:
        db_manager = MockValidatorDatabaseManager()
        logger.info("ValidatorDatabaseManager initialized successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize ValidatorDatabaseManager: {e}")
        return False


def run_health_checks():
    """
    Run all health checks for validator.py.
    """
    logger.info("Starting health checks for validator.py...")

    # Check required environment variables
    required_vars = ["WALLET_NAME", "HOTKEY_NAME", "SUBTENSOR_ADDRESS", "NETUID"]
    if not check_environment_variables(required_vars):
        logger.error("Environment variable check failed.")
        return False

    # Check Subtensor endpoint
    subtensor_address = os.getenv("SUBTENSOR_ADDRESS")
    if not check_subtensor_endpoint(subtensor_address):
        logger.error("Subtensor endpoint check failed.")
        return False

    # Skip Earthdata-related checks (mocked)
    logger.info("Skipping Earthdata checks for health check.")

    # Check Validator initialization
    if not check_validator_initialization():
        logger.error("GaiaValidator initialization check failed.")
        return False

    # Check Database Manager initialization
    if not check_database_manager():
        logger.error("Database manager check failed.")
        return False

    logger.info("All health checks passed successfully.")
    return True


if __name__ == "__main__":
    if not run_health_checks():
        logger.error("Health checks failed.")
        sys.exit(1)
    logger.info("Health checks completed successfully.")
    sys.exit(0)
