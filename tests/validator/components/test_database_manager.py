import pytest
import asyncpg
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from gaia.validator.database.database_manager import DatabaseManager

@pytest.fixture
def mock_pool():
    """Mock connection pool."""
    mock = Mock()
    mock.acquire = AsyncMock()
    mock.close = AsyncMock()
    mock.release = AsyncMock()
    return mock

@pytest.fixture
def mock_connection():
    """Mock database connection."""
    mock = Mock()
    mock.fetch = AsyncMock()
    mock.fetchrow = AsyncMock()
    mock.execute = AsyncMock()
    mock.executemany = AsyncMock()
    mock.close = AsyncMock()
    return mock

@pytest.fixture
async def database_manager(mock_pool, mock_connection):
    """Fixture to provide database manager instance."""
    # Mock pool creation
    with patch('asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
        mock_create_pool.return_value = mock_pool
        mock_pool.acquire.return_value.__aenter__.return_value = mock_connection
        
        manager = DatabaseManager(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        await manager.connect()
        return manager

@pytest.mark.asyncio
async def test_connect(database_manager, mock_pool):
    """Test database connection."""
    assert database_manager.pool == mock_pool
    assert database_manager.is_connected() is True

@pytest.mark.asyncio
async def test_connect_failure():
    """Test database connection failure."""
    with patch('asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
        mock_create_pool.side_effect = Exception("Connection failed")
        
        manager = DatabaseManager(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        
        with pytest.raises(Exception):
            await manager.connect()
        
        assert manager.is_connected() is False

@pytest.mark.asyncio
async def test_disconnect(database_manager, mock_pool):
    """Test database disconnection."""
    await database_manager.disconnect()
    
    mock_pool.close.assert_called_once()
    assert database_manager.is_connected() is False

@pytest.mark.asyncio
async def test_reset_pool(database_manager, mock_pool, mock_connection):
    """Test pool reset."""
    await database_manager.reset_pool()
    
    mock_pool.close.assert_called_once()
    assert database_manager.pool is not None
    assert database_manager.is_connected() is True

@pytest.mark.asyncio
async def test_execute(database_manager, mock_connection):
    """Test executing SQL query."""
    query = "INSERT INTO test_table (column) VALUES ($1)"
    params = ["test_value"]
    
    await database_manager.execute(query, *params)
    
    mock_connection.execute.assert_called_once_with(query, *params)

@pytest.mark.asyncio
async def test_execute_many(database_manager, mock_connection):
    """Test executing multiple SQL queries."""
    query = "INSERT INTO test_table (column) VALUES ($1)"
    params_list = [["value1"], ["value2"], ["value3"]]
    
    await database_manager.execute_many(query, params_list)
    
    mock_connection.executemany.assert_called_once_with(query, params_list)

@pytest.mark.asyncio
async def test_fetch_one(database_manager, mock_connection):
    """Test fetching single row."""
    query = "SELECT * FROM test_table WHERE id = $1"
    params = [1]
    expected_result = {"id": 1, "value": "test"}
    mock_connection.fetchrow.return_value = expected_result
    
    result = await database_manager.fetch_one(query, *params)
    
    assert result == expected_result
    mock_connection.fetchrow.assert_called_once_with(query, *params)

@pytest.mark.asyncio
async def test_fetch_many(database_manager, mock_connection):
    """Test fetching multiple rows."""
    query = "SELECT * FROM test_table"
    expected_results = [
        {"id": 1, "value": "test1"},
        {"id": 2, "value": "test2"}
    ]
    mock_connection.fetch.return_value = expected_results
    
    results = await database_manager.fetch_many(query)
    
    assert results == expected_results
    mock_connection.fetch.assert_called_once_with(query)

@pytest.mark.asyncio
async def test_update_validator_state(database_manager, mock_connection):
    """Test updating validator state."""
    await database_manager.update_validator_state(
        uid=1,
        hotkey="test_hotkey",
        stake=1000.0,
        trust=0.8,
        consensus=0.9,
        incentive=0.5,
        dividends=100.0,
        block_number=1000,
        block_hash="0x123...",
        block_timestamp=1234567890,
        active=True
    )
    
    mock_connection.execute.assert_called_once()
    call_args = mock_connection.execute.call_args[0]
    assert "INSERT INTO validator_state" in call_args[0]
    assert "ON CONFLICT (uid) DO UPDATE" in call_args[0]

@pytest.mark.asyncio
async def test_update_miner_info(database_manager, mock_connection):
    """Test updating miner information."""
    await database_manager.update_miner_info(
        uid=1,
        hotkey="test_hotkey",
        stake=1000.0,
        trust=0.8,
        consensus=0.9,
        incentive=0.5,
        dividends=100.0,
        last_update=datetime.now(timezone.utc),
        active=True
    )
    
    mock_connection.execute.assert_called_once()
    call_args = mock_connection.execute.call_args[0]
    assert "INSERT INTO miners" in call_args[0]
    assert "ON CONFLICT (uid) DO UPDATE" in call_args[0]

@pytest.mark.asyncio
async def test_update_weights(database_manager, mock_connection):
    """Test updating weights."""
    weights = {
        0: 0.5,
        1: 0.3,
        2: 0.2
    }
    timestamp = datetime.now(timezone.utc)
    
    await database_manager.update_weights(weights, timestamp)
    
    mock_connection.executemany.assert_called_once()
    call_args = mock_connection.executemany.call_args[0]
    assert "INSERT INTO weights" in call_args[0]
    assert "ON CONFLICT (uid) DO UPDATE" in call_args[0]

@pytest.mark.asyncio
async def test_update_metagraph(database_manager, mock_connection):
    """Test updating metagraph."""
    await database_manager.update_metagraph(
        uid=1,
        hotkey="test_hotkey",
        stake=1000.0,
        trust=0.8,
        consensus=0.9,
        incentive=0.5,
        dividends=100.0,
        last_update=datetime.now(timezone.utc),
        active=True
    )
    
    mock_connection.execute.assert_called_once()
    call_args = mock_connection.execute.call_args[0]
    assert "INSERT INTO metagraph" in call_args[0]
    assert "ON CONFLICT (uid) DO UPDATE" in call_args[0]

@pytest.mark.asyncio
async def test_query_error_handling(database_manager, mock_connection):
    """Test error handling during query execution."""
    mock_connection.execute.side_effect = asyncpg.PostgresError("Query failed")
    
    with pytest.raises(Exception):
        await database_manager.execute("SELECT * FROM test_table")

def test_get_connection_info(database_manager):
    """Test getting connection information."""
    info = database_manager.get_connection_info()
    
    assert info["host"] == "localhost"
    assert info["port"] == 5432
    assert info["database"] == "test_db"
    assert info["user"] == "test_user"
    assert "connected" in info
    assert "last_query_time" in info 