import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from gaia.validator.network.network_manager import NetworkManager

@pytest.fixture
def mock_substrate():
    """Mock substrate interface."""
    mock = Mock()
    mock.connect = AsyncMock()
    mock.disconnect = AsyncMock()
    mock.is_connected = Mock(return_value=True)
    mock.get_block_number = AsyncMock(return_value=1000)
    mock.get_block_hash = AsyncMock(return_value="0x123...")
    mock.get_block_timestamp = AsyncMock(return_value=1234567890)
    return mock

@pytest.fixture
def mock_database_manager():
    """Mock database manager."""
    mock = Mock()
    mock.connect = AsyncMock()
    mock.disconnect = AsyncMock()
    mock.is_connected = Mock(return_value=True)
    mock.reset_pool = AsyncMock()
    return mock

@pytest.fixture
def network_manager(mock_substrate, mock_database_manager):
    """Fixture to provide network manager instance."""
    return NetworkManager(
        substrate=mock_substrate,
        database_manager=mock_database_manager,
        chain_endpoint="ws://test.chain",
        network="test",
        netuid=1,
        wallet_name="test_wallet",
        hotkey_name="test_hotkey"
    )

@pytest.mark.asyncio
async def test_connect(network_manager, mock_substrate, mock_database_manager):
    """Test connecting to network and database."""
    success = await network_manager.connect()
    
    assert success is True
    mock_substrate.connect.assert_called_once()
    mock_database_manager.connect.assert_called_once()
    
    # Verify connection state
    assert network_manager.is_connected() is True

@pytest.mark.asyncio
async def test_connect_substrate_failure(network_manager, mock_substrate):
    """Test connection failure with substrate."""
    mock_substrate.connect.side_effect = Exception("Connection failed")
    
    success = await network_manager.connect()
    
    assert success is False
    mock_substrate.connect.assert_called_once()
    assert network_manager.is_connected() is False

@pytest.mark.asyncio
async def test_connect_database_failure(network_manager, mock_database_manager):
    """Test connection failure with database."""
    mock_database_manager.connect.side_effect = Exception("Connection failed")
    
    success = await network_manager.connect()
    
    assert success is False
    mock_database_manager.connect.assert_called_once()
    assert network_manager.is_connected() is False

@pytest.mark.asyncio
async def test_disconnect(network_manager, mock_substrate, mock_database_manager):
    """Test disconnecting from network and database."""
    await network_manager.disconnect()
    
    mock_substrate.disconnect.assert_called_once()
    mock_database_manager.disconnect.assert_called_once()
    assert network_manager.is_connected() is False

@pytest.mark.asyncio
async def test_disconnect_failure(network_manager, mock_substrate):
    """Test disconnection failure."""
    mock_substrate.disconnect.side_effect = Exception("Disconnection failed")
    
    await network_manager.disconnect()
    
    mock_substrate.disconnect.assert_called_once()
    assert network_manager.is_connected() is False

@pytest.mark.asyncio
async def test_check_connection(network_manager, mock_substrate):
    """Test checking connection status."""
    # Test connected state
    mock_substrate.is_connected.return_value = True
    is_connected = await network_manager.check_connection()
    assert is_connected is True
    
    # Test disconnected state
    mock_substrate.is_connected.return_value = False
    is_connected = await network_manager.check_connection()
    assert is_connected is False

@pytest.mark.asyncio
async def test_reconnect(network_manager):
    """Test reconnection process."""
    # Mock initial connection state
    network_manager.is_connected = Mock(return_value=False)
    network_manager.connect = AsyncMock(return_value=True)
    network_manager.disconnect = AsyncMock()
    
    success = await network_manager.reconnect()
    
    assert success is True
    network_manager.disconnect.assert_called_once()
    network_manager.connect.assert_called_once()

@pytest.mark.asyncio
async def test_reconnect_failure(network_manager):
    """Test reconnection failure."""
    network_manager.is_connected = Mock(return_value=False)
    network_manager.connect = AsyncMock(return_value=False)
    network_manager.disconnect = AsyncMock()
    
    success = await network_manager.reconnect()
    
    assert success is False
    network_manager.disconnect.assert_called_once()
    network_manager.connect.assert_called_once()

@pytest.mark.asyncio
async def test_get_block_info(network_manager, mock_substrate):
    """Test getting block information."""
    block_number, block_hash, timestamp = await network_manager.get_block_info()
    
    assert block_number == 1000
    assert block_hash == "0x123..."
    assert timestamp == 1234567890
    mock_substrate.get_block_number.assert_called_once()
    mock_substrate.get_block_hash.assert_called_once()
    mock_substrate.get_block_timestamp.assert_called_once()

@pytest.mark.asyncio
async def test_get_block_info_failure(network_manager, mock_substrate):
    """Test block info retrieval failure."""
    mock_substrate.get_block_number.side_effect = Exception("Failed to get block")
    
    with pytest.raises(Exception):
        await network_manager.get_block_info()

@pytest.mark.asyncio
async def test_verify_connection(network_manager):
    """Test connection verification."""
    # Test successful verification
    network_manager.is_connected = Mock(return_value=True)
    network_manager.check_connection = AsyncMock(return_value=True)
    
    is_verified = await network_manager.verify_connection()
    assert is_verified is True
    
    # Test failed verification
    network_manager.check_connection = AsyncMock(return_value=False)
    network_manager.reconnect = AsyncMock(return_value=False)
    
    is_verified = await network_manager.verify_connection()
    assert is_verified is False

def test_get_connection_info(network_manager):
    """Test getting connection information."""
    info = network_manager.get_connection_info()
    
    assert info["chain_endpoint"] == "ws://test.chain"
    assert info["network"] == "test"
    assert info["netuid"] == 1
    assert info["wallet_name"] == "test_wallet"
    assert info["hotkey_name"] == "test_hotkey"
    assert "connected" in info
    assert "last_connection_check" in info 