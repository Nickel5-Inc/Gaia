import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from gaia.validator.network.metagraph_manager import MetagraphManager

@pytest.fixture
def mock_substrate():
    """Mock substrate interface."""
    mock = Mock()
    mock.get_block_number = AsyncMock(return_value=1000)
    mock.get_block_hash = AsyncMock(return_value="0x123...")
    mock.get_block_timestamp = AsyncMock(return_value=1234567890)
    mock.get_active_nodes = AsyncMock(return_value=[0, 1, 2, 3, 4])
    mock.get_node_info = AsyncMock(return_value={
        "hotkey": "test_hotkey",
        "coldkey": "test_coldkey",
        "stake": 1000.0,
        "incentive": 0.5,
        "trust": 0.8,
        "vtrust": 0.9,
        "protocol": "mock_protocol",
        "active": True
    })
    return mock

@pytest.fixture
def mock_database_manager():
    """Mock database manager."""
    mock = Mock()
    mock.fetch_one = AsyncMock()
    mock.fetch_many = AsyncMock()
    mock.execute = AsyncMock()
    mock.update_metagraph = AsyncMock()
    mock.reset_pool = AsyncMock()
    return mock

@pytest.fixture
def metagraph_manager(mock_substrate, mock_database_manager):
    """Fixture to provide metagraph manager instance."""
    return MetagraphManager(
        substrate=mock_substrate,
        database_manager=mock_database_manager,
        netuid=1
    )

@pytest.mark.asyncio
async def test_sync_metagraph(metagraph_manager, mock_substrate):
    """Test syncing metagraph state."""
    await metagraph_manager.sync_metagraph()
    
    # Verify substrate calls
    mock_substrate.get_active_nodes.assert_called_once_with(1)  # netuid=1
    assert mock_substrate.get_node_info.call_count == 5  # Called for each active node
    
    # Verify metagraph state
    assert len(metagraph_manager.nodes) == 5
    for uid in range(5):
        node = metagraph_manager.nodes[uid]
        assert node["hotkey"] == "test_hotkey"
        assert node["stake"] == 1000.0
        assert node["active"] is True

@pytest.mark.asyncio
async def test_sync_metagraph_failure(metagraph_manager, mock_substrate):
    """Test metagraph sync failure."""
    mock_substrate.get_active_nodes.side_effect = Exception("Sync failed")
    
    with pytest.raises(Exception):
        await metagraph_manager.sync_metagraph()
    
    assert len(metagraph_manager.nodes) == 0

@pytest.mark.asyncio
async def test_update_metagraph(metagraph_manager, mock_database_manager):
    """Test updating metagraph in database."""
    # Setup test nodes
    metagraph_manager.nodes = {
        i: {
            "uid": i,
            "hotkey": f"hotkey_{i}",
            "stake": 1000.0,
            "active": True
        }
        for i in range(5)
    }
    
    await metagraph_manager.update_metagraph()
    
    # Verify database update
    assert mock_database_manager.update_metagraph.call_count == 5
    mock_database_manager.update_metagraph.assert_any_call(
        uid=0,
        hotkey="hotkey_0",
        stake=1000.0,
        active=True
    )

@pytest.mark.asyncio
async def test_update_metagraph_failure(metagraph_manager, mock_database_manager):
    """Test metagraph update failure."""
    mock_database_manager.update_metagraph.side_effect = Exception("Update failed")
    
    # Setup test nodes
    metagraph_manager.nodes = {0: {"uid": 0, "hotkey": "test_hotkey"}}
    
    with pytest.raises(Exception):
        await metagraph_manager.update_metagraph()
    
    # Verify recovery attempt
    mock_database_manager.reset_pool.assert_called_once()

@pytest.mark.asyncio
async def test_get_active_nodes(metagraph_manager):
    """Test getting active nodes."""
    # Setup test nodes
    metagraph_manager.nodes = {
        0: {"uid": 0, "active": True},
        1: {"uid": 1, "active": True},
        2: {"uid": 2, "active": False},
        3: {"uid": 3, "active": True}
    }
    
    active_nodes = await metagraph_manager.get_active_nodes()
    
    assert len(active_nodes) == 3
    assert all(node["active"] for node in active_nodes.values())
    assert 2 not in active_nodes  # Inactive node should not be included

@pytest.mark.asyncio
async def test_get_stake_weighted_trust(metagraph_manager):
    """Test calculating stake-weighted trust."""
    # Setup test nodes
    metagraph_manager.nodes = {
        0: {"uid": 0, "stake": 1000.0, "trust": 0.8},
        1: {"uid": 1, "stake": 2000.0, "trust": 0.6},
        2: {"uid": 2, "stake": 500.0, "trust": 0.9}
    }
    
    trust = await metagraph_manager.get_stake_weighted_trust()
    
    # Verify trust calculation
    expected_trust = (1000.0 * 0.8 + 2000.0 * 0.6 + 500.0 * 0.9) / (1000.0 + 2000.0 + 500.0)
    assert abs(trust - expected_trust) < 1e-6

@pytest.mark.asyncio
async def test_verify_registration(metagraph_manager, mock_substrate):
    """Test verifying validator registration."""
    # Test registered validator
    mock_substrate.is_registered = AsyncMock(return_value=True)
    is_registered = await metagraph_manager.verify_registration()
    assert is_registered is True
    
    # Test unregistered validator
    mock_substrate.is_registered = AsyncMock(return_value=False)
    is_registered = await metagraph_manager.verify_registration()
    assert is_registered is False

@pytest.mark.asyncio
async def test_register_validator(metagraph_manager, mock_substrate):
    """Test registering validator."""
    mock_substrate.register = AsyncMock(return_value=True)
    
    success = await metagraph_manager.register_validator()
    
    assert success is True
    mock_substrate.register.assert_called_once_with(1)  # netuid=1

@pytest.mark.asyncio
async def test_register_validator_failure(metagraph_manager, mock_substrate):
    """Test validator registration failure."""
    mock_substrate.register = AsyncMock(return_value=False)
    
    success = await metagraph_manager.register_validator()
    
    assert success is False
    mock_substrate.register.assert_called_once()

def test_get_metagraph_info(metagraph_manager):
    """Test getting metagraph information."""
    # Setup test state
    metagraph_manager.nodes = {
        0: {"uid": 0, "hotkey": "hotkey_0", "active": True},
        1: {"uid": 1, "hotkey": "hotkey_1", "active": False}
    }
    metagraph_manager.last_update = datetime.now(timezone.utc)
    
    info = metagraph_manager.get_metagraph_info()
    
    assert info["total_nodes"] == 2
    assert info["active_nodes"] == 1
    assert "last_update" in info 