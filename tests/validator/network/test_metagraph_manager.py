import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from gaia.validator.network.metagraph_manager import MetagraphManager
from gaia.validator.core.constants import (
    NETWORK_OP_TIMEOUT,
    MAX_MINERS,
    DEFAULT_NETUID
)

class MockNode:
    """Mock metagraph node."""
    def __init__(self, uid, active=True):
        self.uid = uid
        self.hotkey = f"hotkey_{uid}"
        self.ip = "127.0.0.1"
        self.port = 8080
        self.stake = 1000.0
        self.last_update = 1000
        self.active = active
        self.validator_permit = True

@pytest.fixture
def mock_network_manager():
    """Mock network manager."""
    mock = Mock()
    mock.substrate = Mock()
    mock.submit_transaction = AsyncMock(return_value="0x123")
    return mock

@pytest.fixture
def mock_metagraph():
    """Mock metagraph."""
    mock = Mock()
    mock.nodes = {
        f"hotkey_{i}": MockNode(i)
        for i in range(5)
    }
    return mock

@pytest.fixture
def metagraph_manager(mock_network_manager, mock_metagraph):
    """Fixture to provide metagraph manager instance."""
    with patch("gaia.validator.network.metagraph_manager.Metagraph") as mock_metagraph_class:
        mock_metagraph_class.return_value = mock_metagraph
        manager = MetagraphManager(
            network_manager=mock_network_manager,
            netuid=DEFAULT_NETUID
        )
        return manager

@pytest.mark.asyncio
async def test_sync_metagraph(metagraph_manager, mock_metagraph):
    """Test metagraph synchronization."""
    success = await metagraph_manager.sync_metagraph()
    assert success is True
    assert metagraph_manager.last_sync > 0
    assert metagraph_manager.sync_errors == 0
    assert len(metagraph_manager.nodes_cache) == 5

@pytest.mark.asyncio
async def test_sync_metagraph_failure(metagraph_manager):
    """Test metagraph sync failure."""
    metagraph_manager.metagraph.sync_nodes.side_effect = Exception("Sync failed")
    
    success = await metagraph_manager.sync_metagraph()
    assert success is False
    assert metagraph_manager.sync_errors == 1

@pytest.mark.asyncio
async def test_get_active_nodes(metagraph_manager):
    """Test getting active nodes."""
    await metagraph_manager.sync_metagraph()
    
    active_nodes = await metagraph_manager.get_active_nodes()
    assert len(active_nodes) == 5
    assert all(node.startswith("hotkey_") for node in active_nodes)

@pytest.mark.asyncio
async def test_get_validator_permit(metagraph_manager):
    """Test checking validator permit."""
    await metagraph_manager.sync_metagraph()
    
    has_permit = await metagraph_manager.get_validator_permit("hotkey_0")
    assert has_permit is True

@pytest.mark.asyncio
async def test_get_validator_permit_unknown_hotkey(metagraph_manager):
    """Test checking validator permit for unknown hotkey."""
    await metagraph_manager.sync_metagraph()
    
    has_permit = await metagraph_manager.get_validator_permit("unknown_hotkey")
    assert has_permit is False

@pytest.mark.asyncio
async def test_get_stake(metagraph_manager):
    """Test getting stake amount."""
    await metagraph_manager.sync_metagraph()
    
    stake = await metagraph_manager.get_stake("hotkey_0")
    assert stake == 1000.0

@pytest.mark.asyncio
async def test_get_stake_unknown_hotkey(metagraph_manager):
    """Test getting stake for unknown hotkey."""
    await metagraph_manager.sync_metagraph()
    
    stake = await metagraph_manager.get_stake("unknown_hotkey")
    assert stake == 0.0

@pytest.mark.asyncio
async def test_get_node_info(metagraph_manager):
    """Test getting node information."""
    await metagraph_manager.sync_metagraph()
    
    info = await metagraph_manager.get_node_info("hotkey_0")
    assert info is not None
    assert info["uid"] == 0
    assert info["stake"] == 1000.0
    assert info["active"] is True

@pytest.mark.asyncio
async def test_get_node_info_unknown_hotkey(metagraph_manager):
    """Test getting info for unknown hotkey."""
    await metagraph_manager.sync_metagraph()
    
    info = await metagraph_manager.get_node_info("unknown_hotkey")
    assert info is None

@pytest.mark.asyncio
async def test_set_weights(metagraph_manager, mock_network_manager):
    """Test setting weights."""
    await metagraph_manager.sync_metagraph()
    
    weights = {
        "hotkey_0": 0.5,
        "hotkey_1": 0.3,
        "hotkey_2": 0.2
    }
    mock_keypair = Mock()
    
    success = await metagraph_manager.set_weights(weights, mock_keypair)
    assert success is True
    
    # Verify transaction submission
    mock_network_manager.submit_transaction.assert_called_once_with(
        call_module="SubtensorModule",
        call_function="set_weights",
        params={
            "netuid": DEFAULT_NETUID,
            "dests": [0, 1, 2],
            "weights": [0.5, 0.3, 0.2]
        },
        keypair=mock_keypair
    )

@pytest.mark.asyncio
async def test_set_weights_too_many_nodes(metagraph_manager):
    """Test setting weights with too many nodes."""
    await metagraph_manager.sync_metagraph()
    
    # Create weights for more than MAX_MINERS nodes
    weights = {
        f"hotkey_{i}": 1.0/MAX_MINERS
        for i in range(MAX_MINERS + 1)
    }
    mock_keypair = Mock()
    
    success = await metagraph_manager.set_weights(weights, mock_keypair)
    assert success is False

@pytest.mark.asyncio
async def test_set_weights_inactive_nodes(metagraph_manager):
    """Test setting weights with inactive nodes."""
    # Add inactive node
    metagraph_manager.metagraph.nodes["hotkey_inactive"] = MockNode(99, active=False)
    await metagraph_manager.sync_metagraph()
    
    weights = {
        "hotkey_0": 0.5,
        "hotkey_inactive": 0.5
    }
    mock_keypair = Mock()
    
    success = await metagraph_manager.set_weights(weights, mock_keypair)
    assert success is True
    
    # Verify only active node included
    mock_network_manager.submit_transaction.assert_called_once()
    call_args = mock_network_manager.submit_transaction.call_args[1]
    assert call_args["params"]["dests"] == [0]
    assert call_args["params"]["weights"] == [0.5]

def test_get_metagraph_state(metagraph_manager):
    """Test getting metagraph state."""
    state = metagraph_manager.get_metagraph_state()
    
    assert state["total_nodes"] == 5
    assert state["active_nodes"] == 5
    assert state["sync_errors"] == 0
    assert state["netuid"] == DEFAULT_NETUID 