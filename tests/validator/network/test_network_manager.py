import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from gaia.validator.network.network_manager import NetworkManager
from gaia.validator.core.constants import (
    NETWORK_OP_TIMEOUT,
    DEFAULT_CHAIN_ENDPOINT,
    DEFAULT_NETWORK
)

@pytest.fixture
def mock_substrate():
    """Mock substrate interface."""
    mock = Mock()
    mock.get_block.return_value = {"header": {"number": 1000}}
    mock.compose_call.return_value = Mock()
    mock.create_signed_extrinsic.return_value = Mock()
    mock.submit_extrinsic.return_value = Mock(extrinsic_hash="0x123")
    return mock

@pytest.fixture
def network_manager(mock_substrate):
    """Fixture to provide network manager instance."""
    with patch("gaia.validator.network.network_manager.interface") as mock_interface:
        mock_interface.get_substrate.return_value = mock_substrate
        manager = NetworkManager(
            chain_endpoint=DEFAULT_CHAIN_ENDPOINT,
            network=DEFAULT_NETWORK
        )
        return manager

@pytest.mark.asyncio
async def test_connect(network_manager, mock_substrate):
    """Test network connection."""
    success = await network_manager.connect()
    assert success is True
    assert network_manager.substrate == mock_substrate
    assert network_manager.last_connection is not None
    assert network_manager.connection_errors == 0

@pytest.mark.asyncio
async def test_connect_failure(network_manager):
    """Test network connection failure."""
    with patch("gaia.validator.network.network_manager.interface.get_substrate") as mock_get:
        mock_get.side_effect = Exception("Connection failed")
        
        success = await network_manager.connect()
        assert success is False
        assert network_manager.connection_errors == 1
        assert network_manager.substrate is None

@pytest.mark.asyncio
async def test_check_connection(network_manager):
    """Test connection check."""
    await network_manager.connect()
    
    success = await network_manager.check_connection()
    assert success is True
    assert network_manager.last_block == 1000

@pytest.mark.asyncio
async def test_check_connection_failure(network_manager):
    """Test connection check failure."""
    await network_manager.connect()
    network_manager.substrate.get_block.side_effect = Exception("Connection lost")
    
    success = await network_manager.check_connection()
    assert success is False

@pytest.mark.asyncio
async def test_execute_substrate_call(network_manager):
    """Test substrate call execution."""
    await network_manager.connect()
    
    # Mock method to call
    mock_method = AsyncMock(return_value="success")
    network_manager.substrate.mock_method = mock_method
    
    result = await network_manager.execute_substrate_call(
        "mock_method",
        "arg1",
        kwarg1="value1"
    )
    
    assert result == "success"
    mock_method.assert_called_once_with("arg1", kwarg1="value1")

@pytest.mark.asyncio
async def test_execute_substrate_call_retry(network_manager):
    """Test substrate call retry on failure."""
    await network_manager.connect()
    
    # Mock method that fails once then succeeds
    mock_method = AsyncMock(side_effect=[Exception("First try"), "success"])
    network_manager.substrate.mock_method = mock_method
    
    result = await network_manager.execute_substrate_call("mock_method")
    
    assert result == "success"
    assert mock_method.call_count == 2

@pytest.mark.asyncio
async def test_get_network_state(network_manager):
    """Test getting network state."""
    await network_manager.connect()
    network_manager.substrate.get_runtime_version.return_value = {"spec_version": 100}
    
    state = await network_manager.get_network_state()
    
    assert state["current_block"] == 1000
    assert state["network"] == DEFAULT_NETWORK
    assert state["endpoint"] == DEFAULT_CHAIN_ENDPOINT
    assert state["connection_errors"] == 0
    assert state["runtime_version"]["spec_version"] == 100

@pytest.mark.asyncio
async def test_submit_transaction(network_manager):
    """Test transaction submission."""
    await network_manager.connect()
    
    mock_keypair = Mock()
    result = await network_manager.submit_transaction(
        call_module="TestModule",
        call_function="test_function",
        params={"param1": "value1"},
        keypair=mock_keypair
    )
    
    assert result == "0x123"
    network_manager.substrate.compose_call.assert_called_once()
    network_manager.substrate.create_signed_extrinsic.assert_called_once()
    network_manager.substrate.submit_extrinsic.assert_called_once()

@pytest.mark.asyncio
async def test_submit_transaction_failure(network_manager):
    """Test transaction submission failure."""
    await network_manager.connect()
    network_manager.substrate.submit_extrinsic.side_effect = Exception("Submit failed")
    
    mock_keypair = Mock()
    result = await network_manager.submit_transaction(
        call_module="TestModule",
        call_function="test_function",
        params={},
        keypair=mock_keypair
    )
    
    assert result is None

@pytest.mark.asyncio
async def test_close(network_manager):
    """Test network connection closure."""
    await network_manager.connect()
    await network_manager.close()
    
    assert network_manager.substrate is None 