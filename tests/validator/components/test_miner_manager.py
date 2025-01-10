import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from gaia.validator.components.miner_manager import MinerManager

@pytest.fixture
def mock_database_manager():
    """Mock database manager."""
    mock = Mock()
    mock.fetch_one = AsyncMock()
    mock.fetch_many = AsyncMock()
    mock.execute = AsyncMock()
    mock.update_miner_info = AsyncMock()
    mock.reset_pool = AsyncMock()
    return mock

@pytest.fixture
def mock_metagraph():
    """Mock metagraph."""
    mock = Mock()
    mock.nodes = {
        f"hotkey_{i}": Mock(
            hotkey=f"hotkey_{i}",
            coldkey=f"coldkey_{i}",
            ip="127.0.0.1",
            ip_type="v4",
            port=8080,
            uid=i,
            stake=1000.0,
            incentive=0.5,
            trust=0.8,
            vtrust=0.9,
            protocol="mock_protocol",
            active=True
        )
        for i in range(5)
    }
    mock.sync_nodes = Mock()
    return mock

@pytest.fixture
def mock_substrate():
    """Mock substrate."""
    mock = Mock()
    return mock

@pytest.fixture
def mock_tasks():
    """Mock task instances."""
    soil_task = Mock()
    soil_task.recalculate_recent_scores = AsyncMock()
    
    geo_task = Mock()
    geo_task.recalculate_recent_scores = AsyncMock()
    
    return soil_task, geo_task

@pytest.fixture
def miner_manager(mock_database_manager, mock_metagraph, mock_substrate, mock_tasks):
    """Fixture to provide miner manager instance."""
    soil_task, geo_task = mock_tasks
    return MinerManager(
        database_manager=mock_database_manager,
        metagraph=mock_metagraph,
        substrate=mock_substrate,
        soil_task=soil_task,
        geomagnetic_task=geo_task
    )

@pytest.mark.asyncio
async def test_sync_active_miners(miner_manager, mock_metagraph):
    """Test syncing active miners."""
    active_miners = await miner_manager.sync_active_miners()
    
    assert len(active_miners) == 5
    mock_metagraph.sync_nodes.assert_called_once()
    
    # Verify miner info
    for i in range(5):
        miner = active_miners[i]
        assert miner["hotkey"] == f"hotkey_{i}"
        assert miner["uid"] == i

@pytest.mark.asyncio
async def test_fetch_registered_miners(miner_manager, mock_database_manager):
    """Test fetching registered miners."""
    # Mock database response
    mock_database_manager.fetch_many.return_value = [
        {"uid": i, "hotkey": f"hotkey_{i}"}
        for i in range(3)
    ]
    
    registered_miners = await miner_manager.fetch_registered_miners()
    
    assert len(registered_miners) == 3
    mock_database_manager.fetch_many.assert_called_once()
    
    # Verify cached miners
    assert len(miner_manager.nodes) == 3
    for i in range(3):
        assert miner_manager.nodes[i]["hotkey"] == f"hotkey_{i}"

@pytest.mark.asyncio
async def test_process_deregistered_miners(miner_manager, mock_tasks):
    """Test processing deregistered miners."""
    soil_task, geo_task = mock_tasks
    
    # Setup test data
    active_miners = {
        0: {"hotkey": "new_hotkey_0", "uid": 0},
        1: {"hotkey": "hotkey_1", "uid": 1}
    }
    registered_miners = {
        0: {"hotkey": "old_hotkey_0", "uid": 0},
        1: {"hotkey": "hotkey_1", "uid": 1}
    }
    
    processed_uids = await miner_manager.process_deregistered_miners(
        active_miners,
        registered_miners
    )
    
    assert processed_uids == [0]  # Only UID 0 was deregistered
    soil_task.recalculate_recent_scores.assert_called_once_with([0])
    geo_task.recalculate_recent_scores.assert_called_once_with([0])
    
    # Verify node cache update
    assert miner_manager.nodes[0]["hotkey"] == "new_hotkey_0"

@pytest.mark.asyncio
async def test_update_miner_table(miner_manager, mock_database_manager):
    """Test updating miner table."""
    await miner_manager.update_miner_table()
    
    # Verify database updates
    assert mock_database_manager.update_miner_info.call_count == 5
    
    # Verify node cache updates
    assert len(miner_manager.nodes) == 5
    for i in range(5):
        assert miner_manager.nodes[i]["hotkey"] == f"hotkey_{i}"

@pytest.mark.asyncio
async def test_update_miner_table_failure(miner_manager, mock_database_manager):
    """Test miner table update failure."""
    mock_database_manager.update_miner_info.side_effect = Exception("Update failed")
    
    with pytest.raises(Exception):
        await miner_manager.update_miner_table()
        
    # Verify recovery attempt
    mock_database_manager.reset_pool.assert_called_once()

@pytest.mark.asyncio
async def test_check_deregistrations(miner_manager):
    """Test complete deregistration check cycle."""
    # Mock successful cycle
    miner_manager.sync_active_miners = AsyncMock(return_value={
        0: {"hotkey": "new_hotkey_0", "uid": 0}
    })
    miner_manager.fetch_registered_miners = AsyncMock(return_value={
        0: {"hotkey": "old_hotkey_0", "uid": 0}
    })
    
    processed_uids = await miner_manager.check_deregistrations()
    
    assert processed_uids == [0]
    assert miner_manager.last_successful_dereg_check > 0

@pytest.mark.asyncio
async def test_check_deregistrations_failure(miner_manager, mock_database_manager):
    """Test deregistration check failure and recovery."""
    miner_manager.sync_active_miners = AsyncMock(side_effect=Exception("Sync failed"))
    
    processed_uids = await miner_manager.check_deregistrations()
    
    assert processed_uids == []
    mock_database_manager.reset_pool.assert_called_once()
    mock_metagraph.sync_nodes.assert_called_once()

def test_get_active_miners_count(miner_manager):
    """Test getting active miners count."""
    count = miner_manager.get_active_miners_count()
    assert count == 5

def test_get_miner_info(miner_manager):
    """Test getting miner information."""
    # Setup test data
    miner_manager.nodes = {
        0: {"hotkey": "test_hotkey", "uid": 0}
    }
    
    info = miner_manager.get_miner_info(0)
    assert info["hotkey"] == "test_hotkey"
    assert info["uid"] == 0
    
    # Test unknown miner
    assert miner_manager.get_miner_info(999) is None 