import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from gaia.validator.database.schema_manager import SchemaManager

@pytest.fixture
def mock_database_manager():
    """Mock database manager."""
    mock = Mock()
    mock.execute = AsyncMock()
    mock.fetch_one = AsyncMock()
    mock.fetch_many = AsyncMock()
    mock.reset_pool = AsyncMock()
    return mock

@pytest.fixture
def schema_manager(mock_database_manager):
    """Fixture to provide schema manager instance."""
    return SchemaManager(database_manager=mock_database_manager)

@pytest.mark.asyncio
async def test_initialize_schema(schema_manager, mock_database_manager):
    """Test schema initialization."""
    # Mock schema version check
    mock_database_manager.fetch_one.return_value = None
    
    await schema_manager.initialize_schema()
    
    # Verify schema creation
    assert mock_database_manager.execute.call_count > 0
    
    # Check if version table was created
    create_version_calls = [
        call for call in mock_database_manager.execute.call_args_list
        if "CREATE TABLE IF NOT EXISTS schema_version" in str(call)
    ]
    assert len(create_version_calls) > 0

@pytest.mark.asyncio
async def test_initialize_schema_existing(schema_manager, mock_database_manager):
    """Test schema initialization with existing schema."""
    # Mock existing schema version
    mock_database_manager.fetch_one.return_value = {
        "version": schema_manager.CURRENT_VERSION,
        "updated_at": datetime.now(timezone.utc)
    }
    
    await schema_manager.initialize_schema()
    
    # Verify no schema creation was attempted
    assert mock_database_manager.execute.call_count == 0

@pytest.mark.asyncio
async def test_check_schema_version(schema_manager, mock_database_manager):
    """Test schema version check."""
    # Mock current version
    mock_database_manager.fetch_one.return_value = {
        "version": schema_manager.CURRENT_VERSION,
        "updated_at": datetime.now(timezone.utc)
    }
    
    version = await schema_manager.check_schema_version()
    
    assert version == schema_manager.CURRENT_VERSION
    mock_database_manager.fetch_one.assert_called_once()

@pytest.mark.asyncio
async def test_check_schema_version_no_table(schema_manager, mock_database_manager):
    """Test schema version check when version table doesn't exist."""
    mock_database_manager.fetch_one.return_value = None
    
    version = await schema_manager.check_schema_version()
    
    assert version is None
    mock_database_manager.fetch_one.assert_called_once()

@pytest.mark.asyncio
async def test_update_schema_version(schema_manager, mock_database_manager):
    """Test updating schema version."""
    new_version = schema_manager.CURRENT_VERSION + 1
    
    await schema_manager.update_schema_version(new_version)
    
    mock_database_manager.execute.assert_called_once()
    call_args = mock_database_manager.execute.call_args[0]
    assert "INSERT INTO schema_version" in call_args[0]
    assert str(new_version) in str(call_args)

@pytest.mark.asyncio
async def test_create_tables(schema_manager, mock_database_manager):
    """Test creating database tables."""
    await schema_manager.create_tables()
    
    # Verify all required tables were created
    required_tables = [
        "validator_state",
        "miners",
        "weights",
        "metagraph",
        "soil_scores",
        "geomagnetic_scores"
    ]
    
    for table in required_tables:
        create_table_calls = [
            call for call in mock_database_manager.execute.call_args_list
            if f"CREATE TABLE IF NOT EXISTS {table}" in str(call)
        ]
        assert len(create_table_calls) > 0

@pytest.mark.asyncio
async def test_create_tables_failure(schema_manager, mock_database_manager):
    """Test table creation failure."""
    mock_database_manager.execute.side_effect = Exception("Table creation failed")
    
    with pytest.raises(Exception):
        await schema_manager.create_tables()
    
    # Verify recovery attempt
    mock_database_manager.reset_pool.assert_called_once()

@pytest.mark.asyncio
async def test_create_indexes(schema_manager, mock_database_manager):
    """Test creating database indexes."""
    await schema_manager.create_indexes()
    
    # Verify index creation
    index_calls = [
        call for call in mock_database_manager.execute.call_args_list
        if "CREATE INDEX IF NOT EXISTS" in str(call)
    ]
    assert len(index_calls) > 0

@pytest.mark.asyncio
async def test_create_indexes_failure(schema_manager, mock_database_manager):
    """Test index creation failure."""
    mock_database_manager.execute.side_effect = Exception("Index creation failed")
    
    with pytest.raises(Exception):
        await schema_manager.create_indexes()
    
    # Verify recovery attempt
    mock_database_manager.reset_pool.assert_called_once()

@pytest.mark.asyncio
async def test_upgrade_schema(schema_manager, mock_database_manager):
    """Test schema upgrade."""
    # Mock old version
    mock_database_manager.fetch_one.return_value = {
        "version": schema_manager.CURRENT_VERSION - 1,
        "updated_at": datetime.now(timezone.utc)
    }
    
    await schema_manager.upgrade_schema()
    
    # Verify version update
    version_update_calls = [
        call for call in mock_database_manager.execute.call_args_list
        if "INSERT INTO schema_version" in str(call)
    ]
    assert len(version_update_calls) > 0

@pytest.mark.asyncio
async def test_upgrade_schema_current(schema_manager, mock_database_manager):
    """Test schema upgrade when already at current version."""
    # Mock current version
    mock_database_manager.fetch_one.return_value = {
        "version": schema_manager.CURRENT_VERSION,
        "updated_at": datetime.now(timezone.utc)
    }
    
    await schema_manager.upgrade_schema()
    
    # Verify no updates were attempted
    assert mock_database_manager.execute.call_count == 0

@pytest.mark.asyncio
async def test_verify_schema(schema_manager):
    """Test schema verification."""
    # Mock methods
    schema_manager.check_schema_version = AsyncMock(return_value=schema_manager.CURRENT_VERSION)
    schema_manager.upgrade_schema = AsyncMock()
    
    is_valid = await schema_manager.verify_schema()
    
    assert is_valid is True
    schema_manager.check_schema_version.assert_called_once()
    schema_manager.upgrade_schema.assert_not_called()

@pytest.mark.asyncio
async def test_verify_schema_needs_upgrade(schema_manager):
    """Test schema verification when upgrade is needed."""
    # Mock methods
    schema_manager.check_schema_version = AsyncMock(return_value=schema_manager.CURRENT_VERSION - 1)
    schema_manager.upgrade_schema = AsyncMock()
    
    is_valid = await schema_manager.verify_schema()
    
    assert is_valid is True
    schema_manager.check_schema_version.assert_called_once()
    schema_manager.upgrade_schema.assert_called_once()

def test_get_schema_info(schema_manager):
    """Test getting schema information."""
    info = schema_manager.get_schema_info()
    
    assert "current_version" in info
    assert info["current_version"] == schema_manager.CURRENT_VERSION
    assert "last_upgrade" in info 