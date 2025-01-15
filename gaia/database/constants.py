"""
Database-related constants used throughout the application.
"""

# Default timeout for database transactions (in seconds)
DEFAULT_TRANSACTION_TIMEOUT = 30  # 30 seconds should be sufficient for most operations

# Maximum number of retries for database operations
MAX_RETRIES = 3

# Default batch size for bulk operations
DEFAULT_BATCH_SIZE = 1000

# Status constants
STATUS_PENDING = 'pending'
STATUS_PROCESSING = 'processing'
STATUS_COMPLETED = 'completed'
STATUS_ERROR = 'error'
STATUS_TIMEOUT = 'timeout' 