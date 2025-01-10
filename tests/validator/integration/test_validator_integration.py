import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from gaia.validator.validator import GaiaValidatorV2
from gaia.validator.core.constants import (
    DEFAULT_CHAIN_ENDPOINT,
    DEFAULT_NETWORK,
    DEFAULT_NETUID,
    DEFAULT_WALLET,
    DEFAULT_HOTKEY
)

class IntegrationTestConfig:
    """Test configuration for integration tests."""
    DB_HOST = "localhost"
    DB_PORT = 5432
    DB_NAME = "test_gaia"
    DB_USER = "test_user"
    DB_PASS = "test_pass"
    CHAIN_ENDPOINT = "ws://test.chain"
    NETWORK = "test"
    NETUID = 1
    WALLET = "test_wallet"
    HOTKEY = "test_hotkey"

@pytest.fixture
async def mock_substrate():
    """Mock substrate with realistic chain interaction behavior."""
    mock = Mock()
    # Network operations
    mock.connect = AsyncMock()
    mock.disconnect = AsyncMock()
    mock.is_connected = Mock(return_value=True)
    
    # Block info
    mock.get_block_number = AsyncMock(return_value=1000)
    mock.get_block_hash = AsyncMock(return_value="0x123...")
    mock.get_block_timestamp = AsyncMock(return_value=1234567890)
    
    # Registration
    mock.is_registered = AsyncMock(return_value=True)
    mock.register = AsyncMock(return_value=True)
    
    # Node operations
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
    
    # Weight operations
    mock.set_weights = AsyncMock(return_value=True)
    return mock

@pytest.fixture
async def database_pool():
    """Create test database pool."""
    with patch('asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
        mock_pool = Mock()
        mock_conn = Mock()
        
        # Setup connection methods
        mock_conn.fetch = AsyncMock()
        mock_conn.fetchrow = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.executemany = AsyncMock()
        
        # Setup pool methods
        mock_pool.acquire = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.close = AsyncMock()
        
        mock_create_pool.return_value = mock_pool
        yield mock_pool

@pytest.fixture
async def validator(mock_substrate, database_pool):
    """Create validator instance with mocked dependencies."""
    class MockArgs:
        def __init__(self):
            self.wallet = IntegrationTestConfig.WALLET
            self.hotkey = IntegrationTestConfig.HOTKEY
            self.netuid = IntegrationTestConfig.NETUID
            self.chain_endpoint = IntegrationTestConfig.CHAIN_ENDPOINT
            self.network = IntegrationTestConfig.NETWORK
            self.db_host = IntegrationTestConfig.DB_HOST
            self.db_port = IntegrationTestConfig.DB_PORT
            self.db_name = IntegrationTestConfig.DB_NAME
            self.db_user = IntegrationTestConfig.DB_USER
            self.db_pass = IntegrationTestConfig.DB_PASS
    
    with patch('gaia.validator.validator_v2.interface') as mock_interface:
        mock_interface.get_substrate.return_value = mock_substrate
        validator = GaiaValidatorV2(MockArgs())
        yield validator

@pytest.mark.asyncio
async def test_validator_startup_flow(validator, mock_substrate, database_pool):
    """Test complete validator startup flow."""
    # Initialize components
    success = await validator.initialize_components()
    assert success is True
    
    # Verify component initialization
    assert validator.network_manager is not None
    assert validator.database_manager is not None
    assert validator.schema_manager is not None
    assert validator.metagraph_manager is not None
    assert validator.validator_state is not None
    assert validator.weight_manager is not None
    assert validator.miner_manager is not None
    
    # Setup neuron
    success = await validator.setup_neuron()
    assert success is True
    
    # Verify registration checks
    mock_substrate.is_registered.assert_called_once()
    assert validator.validator_state.uid is not None

@pytest.mark.asyncio
async def test_weight_setting_flow(validator, mock_substrate, database_pool):
    """Test complete weight calculation and setting flow."""
    # Initialize validator
    await validator.initialize_components()
    await validator.setup_neuron()
    
    # Create scoring cycle task
    task = asyncio.create_task(validator._run_scoring_cycle())
    await asyncio.sleep(0.1)  # Let the cycle run briefly
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        pass
    
    # Verify weight calculation flow
    assert mock_substrate.get_active_nodes.called
    assert mock_substrate.set_weights.called
    
    # Verify database updates
    conn = await database_pool.acquire().__aenter__()
    assert conn.execute.called  # Weight updates should be stored
    assert conn.executemany.called  # Batch updates should occur

@pytest.mark.asyncio
async def test_miner_monitoring_flow(validator, mock_substrate, database_pool):
    """Test miner monitoring and deregistration handling."""
    # Initialize validator
    await validator.initialize_components()
    await validator.setup_neuron()
    
    # Setup test miners
    active_miners = {
        i: {
            "uid": i,
            "hotkey": f"hotkey_{i}",
            "stake": 1000.0,
            "active": True
        }
        for i in range(5)
    }
    validator.miner_manager.nodes = active_miners.copy()
    
    # Simulate miner deregistration
    mock_substrate.get_active_nodes.return_value = [1, 2, 3, 4]  # Miner 0 deregistered
    
    # Run deregistration check
    processed_uids = await validator.miner_manager.check_deregistrations()
    
    # Verify deregistration handling
    assert 0 in processed_uids
    assert len(processed_uids) == 1
    
    # Verify database updates
    conn = await database_pool.acquire().__aenter__()
    assert conn.execute.called  # Miner state should be updated

@pytest.mark.asyncio
async def test_error_recovery_flow(validator, mock_substrate, database_pool):
    """Test error recovery mechanisms."""
    # Initialize validator
    await validator.initialize_components()
    await validator.setup_neuron()
    
    # Simulate network failure
    mock_substrate.is_connected.return_value = False
    mock_substrate.connect.side_effect = [Exception("Connection failed"), True]
    
    # Verify network recovery
    is_connected = await validator.network_manager.check_connection()
    assert is_connected is False
    
    success = await validator.network_manager.reconnect()
    assert success is True
    assert mock_substrate.connect.call_count == 2
    
    # Simulate database failure
    conn = await database_pool.acquire().__aenter__()
    conn.execute.side_effect = Exception("Database error")
    
    # Verify database recovery
    with pytest.raises(Exception):
        await validator.database_manager.execute("SELECT 1")
    
    assert validator.database_manager.pool is not None
    assert database_pool.close.called  # Pool should be reset

@pytest.mark.asyncio
async def test_state_synchronization_flow(validator, mock_substrate, database_pool):
    """Test state synchronization between components."""
    # Initialize validator
    await validator.initialize_components()
    await validator.setup_neuron()
    
    # Update metagraph state
    await validator.metagraph_manager.sync_metagraph()
    
    # Verify state propagation
    assert len(validator.metagraph_manager.nodes) > 0
    assert validator.validator_state.block_number is not None
    assert validator.validator_state.last_update is not None
    
    # Verify database synchronization
    conn = await database_pool.acquire().__aenter__()
    assert conn.execute.called  # State should be persisted
    
    # Update weights
    await validator.weight_manager.update_weights()
    
    # Verify weight synchronization
    assert validator.weight_manager.last_weight_update_time > 0
    assert mock_substrate.set_weights.called 