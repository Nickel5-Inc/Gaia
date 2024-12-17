import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime, timezone
import os
import tempfile

# Basic fixtures for common test requirements
@pytest.fixture
def base_temp_dir():
    """Provide a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir

@pytest.fixture
def mock_database():
    """Provide a mock database manager."""
    mock_db = AsyncMock()
    mock_db.fetch_one = AsyncMock(return_value={})
    mock_db.fetch_many = AsyncMock(return_value=[])
    mock_db.execute = AsyncMock()
    return mock_db

@pytest.fixture
def mock_redis():
    """Provide a mock Redis client."""
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock()
    mock_redis.delete = AsyncMock()
    return mock_redis

@pytest.fixture
def mock_validator():
    """Provide a mock validator instance."""
    mock_val = MagicMock()
    mock_val.query_miners = AsyncMock(return_value={})
    mock_val.metagraph = MagicMock()
    mock_val.metagraph.nodes = ["test_hotkey"]
    return mock_val

@pytest.fixture
def test_timestamp():
    """Provide a fixed timestamp for testing."""
    return datetime.now(timezone.utc)

# Helper for async tests
@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close() 