import pytest
from datetime import datetime, timezone
from gaia.validator.database.database_manager import ValidatorDatabaseManager
from gaia.validator.database.cache_manager import CacheManager

@pytest.mark.asyncio
@pytest.mark.database
async def test_database_connection(mock_database):
    """Test database connection and basic operations."""
    db = ValidatorDatabaseManager()
    db._pool = mock_database  # Replace actual pool with mock
    
    # Test basic query
    mock_database.fetch_one.return_value = {"test": "value"}
    result = await db.fetch_one("SELECT * FROM test")
    assert result == {"test": "value"}
    assert mock_database.fetch_one.called

@pytest.mark.asyncio
@pytest.mark.database
async def test_transaction_management(mock_database):
    """Test transaction management."""
    db = ValidatorDatabaseManager()
    db._pool = mock_database
    
    async with db.transaction() as txn:
        await txn.execute("INSERT INTO test VALUES (:value)", {"value": "test"})
        await txn.execute("UPDATE test SET value = :value", {"value": "updated"})
    
    assert mock_database.execute.call_count == 2

@pytest.mark.asyncio
@pytest.mark.database
@pytest.mark.redis
async def test_cache_integration(mock_database, mock_redis):
    """Test database cache integration."""
    cache = CacheManager(mock_redis)
    db = ValidatorDatabaseManager(cache_manager=cache)
    db._pool = mock_database
    
    # Test cache miss
    mock_redis.get.return_value = None
    mock_database.fetch_one.return_value = {"id": 1, "value": "test"}
    
    result = await db.fetch_one_cached(
        "test_key",
        "SELECT * FROM test WHERE id = :id",
        {"id": 1}
    )
    
    assert result == {"id": 1, "value": "test"}
    assert mock_redis.set.called
    
    # Test cache hit
    mock_redis.get.return_value = b'{"id": 1, "value": "test"}'
    result = await db.fetch_one_cached(
        "test_key",
        "SELECT * FROM test WHERE id = :id",
        {"id": 1}
    )
    
    assert result == {"id": 1, "value": "test"}
    assert not mock_database.fetch_one.called

@pytest.mark.asyncio
@pytest.mark.database
async def test_connection_pool_management(mock_database):
    """Test connection pool management."""
    db = ValidatorDatabaseManager()
    db._pool = mock_database
    
    # Test pool acquisition
    async with db.get_connection() as conn:
        await conn.execute("SELECT 1")
    
    assert mock_database.execute.called
    
    # Test pool release
    assert not db._pool._closed

@pytest.mark.asyncio
@pytest.mark.database
async def test_error_handling(mock_database):
    """Test database error handling."""
    db = ValidatorDatabaseManager()
    db._pool = mock_database
    
    # Test query error
    mock_database.fetch_one.side_effect = Exception("Database error")
    
    with pytest.raises(Exception):
        await db.fetch_one("SELECT * FROM test")
    
    # Test transaction rollback
    mock_database.execute.side_effect = Exception("Transaction error")
    
    with pytest.raises(Exception):
        async with db.transaction() as txn:
            await txn.execute("INSERT INTO test VALUES (1)")
    
    assert mock_database.rollback.called

@pytest.mark.asyncio
@pytest.mark.database
async def test_query_result_caching(mock_database, mock_redis):
    """Test query result caching behavior."""
    cache = CacheManager(mock_redis)
    db = ValidatorDatabaseManager(cache_manager=cache)
    db._pool = mock_database
    
    test_data = {"id": 1, "timestamp": datetime.now(timezone.utc)}
    cache_key = "test_query_1"
    
    # Test write-through caching
    mock_database.fetch_one.return_value = test_data
    mock_redis.get.return_value = None
    
    result = await db.fetch_one_cached(
        cache_key,
        "SELECT * FROM test WHERE id = :id",
        {"id": 1}
    )
    
    assert result == test_data
    assert mock_redis.set.called
    
    # Test cache invalidation
    await db.invalidate_cache(cache_key)
    assert mock_redis.delete.called 