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

# Mock classes
class MockArgs:
    def __init__(self):
        self.wallet = None
        self.hotkey = None
        self.netuid = None
        self.test_soil = False
        self.subtensor = Mock(chain_endpoint=None, network=None)

class MockNetworkManager:
    def __init__(self, *args, **kwargs):
        self.connect = AsyncMock(return_value=True)
        self.close = AsyncMock()
        self.keypair = Mock()
        self.substrate = Mock()

class MockDatabaseManager:
    def __init__(self, *args, **kwargs):
        self.connect = AsyncMock(return_value=True)
        self.close = AsyncMock()
        self.fetch_one = AsyncMock()
        self.fetch_many = AsyncMock()
        self.execute = AsyncMock()

class MockSchemaManager:
    def __init__(self, *args, **kwargs):
        self.initialize_schema = AsyncMock(return_value=True)

class MockMetagraphManager:
    def __init__(self, *args, **kwargs):
        self.sync_metagraph = AsyncMock(return_value=True)
        self.set_weights = AsyncMock(return_value=True)
        self.metagraph = Mock()

class MockValidatorState:
    def __init__(self, *args, **kwargs):
        self.load_state = AsyncMock(return_value=True)
        self.check_registration = AsyncMock(return_value=123)  # Mock UID
        self.register_validator = AsyncMock(return_value=True)
        self.validator_permit = True
        self.uid = 123
        self.stake = 1000.0

class MockWeightManager:
    def __init__(self, *args, **kwargs):
        self.calculate_weights = AsyncMock(return_value=[1.0] * 256)
        self.check_weight_setting_interval = AsyncMock(return_value=(100, 50))
        self.verify_weight_setting_permission = AsyncMock(return_value=True)
        self.update_last_weights_block = AsyncMock()

class MockMinerManager:
    def __init__(self, *args, **kwargs):
        self.check_deregistrations = AsyncMock()

class MockTask:
    def __init__(self, *args, **kwargs):
        self.validator_execute = AsyncMock()

@pytest.fixture
def mock_components():
    """Fixture to provide mock components."""
    with patch("gaia.validator.validator_v2.NetworkManager", MockNetworkManager), \
         patch("gaia.validator.validator_v2.DatabaseManager", MockDatabaseManager), \
         patch("gaia.validator.validator_v2.SchemaManager", MockSchemaManager), \
         patch("gaia.validator.validator_v2.MetagraphManager", MockMetagraphManager), \
         patch("gaia.validator.validator_v2.ValidatorState", MockValidatorState), \
         patch("gaia.validator.validator_v2.WeightManager", MockWeightManager), \
         patch("gaia.validator.validator_v2.MinerManager", MockMinerManager), \
         patch("gaia.validator.validator_v2.GeomagneticTask", MockTask), \
         patch("gaia.validator.validator_v2.SoilMoistureTask", MockTask), \
         patch("gaia.validator.validator_v2.MinerScoreSender") as mock_sender:
        
        mock_sender.return_value.run_async = AsyncMock()
        yield

@pytest.fixture
def validator(mock_components):
    """Fixture to provide initialized validator instance."""
    args = MockArgs()
    return GaiaValidatorV2(args)

@pytest.mark.asyncio
async def test_validator_initialization(validator):
    """Test validator initialization."""
    assert validator.chain_endpoint == DEFAULT_CHAIN_ENDPOINT
    assert validator.network == DEFAULT_NETWORK
    assert validator.netuid == DEFAULT_NETUID
    assert validator.wallet_name == DEFAULT_WALLET
    assert validator.hotkey_name == DEFAULT_HOTKEY
    
    assert validator.network_manager is not None
    assert validator.database_manager is not None
    assert validator.schema_manager is not None
    assert validator.soil_task is not None
    assert validator.geomagnetic_task is not None

@pytest.mark.asyncio
async def test_initialize_components(validator):
    """Test component initialization."""
    success = await validator.initialize_components()
    assert success is True
    
    # Verify all components initialized
    assert validator.metagraph_manager is not None
    assert validator.validator_state is not None
    assert validator.miner_manager is not None
    assert validator.weight_manager is not None
    assert validator.watchdog is not None
    assert validator.status_monitor is not None
    assert validator.metrics_collector is not None

@pytest.mark.asyncio
async def test_setup_neuron(validator):
    """Test neuron setup."""
    # Initialize components first
    await validator.initialize_components()
    
    success = await validator.setup_neuron()
    assert success is True
    
    # Verify registration checks
    validator.validator_state.check_registration.assert_called_once()
    assert not validator.validator_state.register_validator.called  # Should not register if already registered

@pytest.mark.asyncio
async def test_setup_neuron_registration_needed(validator):
    """Test neuron setup when registration is needed."""
    await validator.initialize_components()
    
    # Mock registration needed
    validator.validator_state.check_registration.return_value = False
    
    success = await validator.setup_neuron()
    assert success is True
    
    # Verify registration attempted
    validator.validator_state.check_registration.assert_called_once()
    validator.validator_state.register_validator.assert_called_once()

@pytest.mark.asyncio
async def test_create_worker_tasks(validator):
    """Test worker task creation."""
    await validator.initialize_components()
    
    workers = await validator._create_worker_tasks()
    assert len(workers) == 8  # Verify all expected tasks created
    
    # Verify each task type
    task_types = [task._coro.__name__ for task in workers]
    assert "validator_execute" in task_types  # Soil task
    assert "validator_execute" in task_types  # Geo task
    assert "run_async" in task_types  # Score sender
    assert "start_monitoring" in task_types  # Watchdog
    assert "_run_scoring_cycle" in task_types  # Scoring cycle

@pytest.mark.asyncio
async def test_run_scoring_cycle(validator):
    """Test scoring cycle execution."""
    await validator.initialize_components()
    
    # Mock asyncio.sleep to prevent infinite loop
    with patch("asyncio.sleep", AsyncMock()):
        # Create task and run briefly
        task = asyncio.create_task(validator._run_scoring_cycle())
        await asyncio.sleep(0.1)
        task.cancel()
        
        try:
            await task
        except asyncio.CancelledError:
            pass
        
    # Verify weight setting flow
    validator.validator_state.check_registration.assert_called()
    validator.weight_manager.check_weight_setting_interval.assert_called()
    validator.weight_manager.verify_weight_setting_permission.assert_called()
    validator.weight_manager.calculate_weights.assert_called()
    validator.metagraph_manager.set_weights.assert_called()

@pytest.mark.asyncio
async def test_cleanup(validator):
    """Test cleanup process."""
    await validator._cleanup()
    
    # Verify connections closed
    validator.database_manager.close.assert_called_once()
    validator.network_manager.close.assert_called_once()

@pytest.mark.asyncio
async def test_main_loop_initialization_failure(validator):
    """Test main loop handles initialization failure."""
    validator.initialize_components = AsyncMock(return_value=False)
    
    await validator.main()
    
    # Verify early return on initialization failure
    validator.initialize_components.assert_called_once()
    assert not validator.setup_neuron.called

@pytest.mark.asyncio
async def test_main_loop_setup_failure(validator):
    """Test main loop handles setup failure."""
    validator.initialize_components = AsyncMock(return_value=True)
    validator.setup_neuron = AsyncMock(return_value=False)
    
    await validator.main()
    
    # Verify early return on setup failure
    validator.initialize_components.assert_called_once()
    validator.setup_neuron.assert_called_once()
    assert not validator._create_worker_tasks.called 