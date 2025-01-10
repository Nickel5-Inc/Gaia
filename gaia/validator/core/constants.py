"""Constants used throughout the validator."""

# Timeouts (in seconds)
WATCHDOG_TIMEOUT = 1800  # 30 minutes
MAIN_LOOP_TIMEOUT = 600  # 10 minutes
SCORING_CYCLE_TIMEOUT = 300  # 5 minutes
NETWORK_OP_TIMEOUT = 180  # 3 minutes
DB_OP_TIMEOUT = 60  # 1 minute
STATUS_UPDATE_TIMEOUT = 30  # 30 seconds

# Sleep intervals (in seconds)
MAIN_LOOP_SLEEP = 300  # 5 minutes
WATCHDOG_SLEEP = 300  # 5 minutes
SCORING_SLEEP = 12  # 12 seconds
STATUS_SLEEP = 60  # 1 minute

# Network constants
DEFAULT_NETUID = 237
DEFAULT_CHAIN_ENDPOINT = "wss://test.finney.opentensor.ai:443/"
DEFAULT_NETWORK = "test"
DEFAULT_WALLET = "default"
DEFAULT_HOTKEY = "default"

# Database constants
SCORE_HISTORY_DAYS = 3  # Number of days of score history to maintain

# Validator constants
MAX_MINERS = 256  # Maximum number of miners
WEIGHT_MULTIPLIER = 0.5  # Weight multiplier for each task
GEO_NORMALIZATION_FACTOR = 10  # Factor for normalizing geomagnetic scores 

# Metrics collection settings
METRICS_COLLECTION_INTERVAL = 60  # Collect metrics every 60 seconds
METRICS_HISTORY_SIZE = 100  # Keep last 100 values for each metric
METRICS_COLLECTION_TIMEOUT = 30  # 30 second timeout for metric collection 