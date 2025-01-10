import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from gaia.validator.core.validator_state import ValidatorState

@pytest.fixture
def mock_database_manager():
    """Mock database manager."""
    mock = Mock()
    mock.fetch_one = AsyncMock()
    mock.fetch_many = AsyncMock()
    mock.execute = AsyncMock()
    mock.update_validator_state = AsyncMock()
    mock.reset_pool = AsyncMock()
    return mock

@pytest.fixture
def mock_metagraph():
    """Mock metagraph."""
    mock = Mock()
    mock.my_uid = 1
    mock.my_hotkey = "test_hotkey"
    mock.my_coldkey = "test_coldkey"
    mock.my_stake = 1000.0
    mock.my_incentive = 0.5
    mock.my_trust = 0.8
    mock.my_vtrust = 0.9
    mock.my_protocol = "mock_protocol"
    mock.my_active = True
    return mock

@pytest.fixture
def mock_substrate():
    """Mock substrate."""
    mock = Mock()
    mock.get_block_number = AsyncMock(return_value=1000)
    mock.get_block_hash = AsyncMock(return_value="0x123...")
    mock.get_block_timestamp = AsyncMock(return_value=1234567890)
    return mock

@pytest.fixture
def validator_state(mock_database_manager, mock_metagraph, mock_substrate):
    """Fixture to provide validator state instance."""
    return ValidatorState(
        database_manager=mock_database_manager,
        metagraph=mock_metagraph,
        substrate=mock_substrate
    )

@pytest.mark.asyncio
async def test_initialize_state(validator_state, mock_database_manager):
    """Test initializing validator state."""
    # Mock database response
    mock_database_manager.fetch_one.return_value = {
        "uid": 1,
        "hotkey": "test_hotkey",
        "coldkey": "test_coldkey",
        "stake": 1000.0,
        "incentive": 0.5,
        "trust": 0.8,
        "vtrust": 0.9,
        "protocol": "mock_protocol",
        "active": True,
        "last_update": datetime.now(timezone.utc)
    }
    
    await validator_state.initialize_state()
    
    # Verify state initialization
    assert validator_state.uid == 1
    assert validator_state.hotkey == "test_hotkey"
    assert validator_state.coldkey == "test_coldkey"
    assert validator_state.stake == 1000.0
    assert validator_state.incentive == 0.5
    assert validator_state.trust == 0.8
    assert validator_state.vtrust == 0.9
    assert validator_state.protocol == "mock_protocol"
    assert validator_state.active is True
    assert validator_state.last_update is not None

@pytest.mark.asyncio
async def test_initialize_state_no_record(validator_state, mock_database_manager):
    """Test initializing state with no existing record."""
    mock_database_manager.fetch_one.return_value = None
    
    await validator_state.initialize_state()
    
    # Verify default state
    assert validator_state.uid == 1  # From mock_metagraph
    assert validator_state.hotkey == "test_hotkey"
    assert validator_state.last_update is None

@pytest.mark.asyncio
async def test_update_state(validator_state, mock_database_manager, mock_substrate):
    """Test updating validator state."""
    await validator_state.update_state()
    
    # Verify substrate calls
    mock_substrate.get_block_number.assert_called_once()
    mock_substrate.get_block_hash.assert_called_once()
    mock_substrate.get_block_timestamp.assert_called_once()
    
    # Verify database update
    mock_database_manager.update_validator_state.assert_called_once()
    
    # Verify state update
    assert validator_state.block_number == 1000
    assert validator_state.block_hash == "0x123..."
    assert validator_state.block_timestamp == 1234567890

@pytest.mark.asyncio
async def test_update_state_failure(validator_state, mock_database_manager):
    """Test state update failure."""
    mock_database_manager.update_validator_state.side_effect = Exception("Update failed")
    
    with pytest.raises(Exception):
        await validator_state.update_state()
    
    # Verify recovery attempt
    mock_database_manager.reset_pool.assert_called_once()

@pytest.mark.asyncio
async def test_check_registration(validator_state):
    """Test checking validator registration."""
    # Test registered state
    is_registered = await validator_state.check_registration()
    assert is_registered is True
    
    # Test unregistered state
    validator_state.uid = None
    is_registered = await validator_state.check_registration()
    assert is_registered is False

@pytest.mark.asyncio
async def test_verify_permit(validator_state):
    """Test verifying validator permit."""
    # Test valid permit
    validator_state.active = True
    validator_state.stake = 1000.0
    has_permit = await validator_state.verify_permit()
    assert has_permit is True
    
    # Test invalid permit - inactive
    validator_state.active = False
    has_permit = await validator_state.verify_permit()
    assert has_permit is False
    
    # Test invalid permit - insufficient stake
    validator_state.active = True
    validator_state.stake = 0.0
    has_permit = await validator_state.verify_permit()
    assert has_permit is False

def test_get_state_info(validator_state):
    """Test getting validator state information."""
    # Setup test state
    validator_state.uid = 1
    validator_state.hotkey = "test_hotkey"
    validator_state.block_number = 1000
    validator_state.last_update = datetime.now(timezone.utc)
    
    info = validator_state.get_state_info()
    
    assert info["uid"] == 1
    assert info["hotkey"] == "test_hotkey"
    assert info["block_number"] == 1000
    assert "last_update" in info

def test_is_active(validator_state):
    """Test checking if validator is active."""
    validator_state.active = True
    assert validator_state.is_active() is True
    
    validator_state.active = False
    assert validator_state.is_active() is False

def test_get_stake(validator_state):
    """Test getting validator stake."""
    validator_state.stake = 1000.0
    assert validator_state.get_stake() == 1000.0

def test_get_trust(validator_state):
    """Test getting validator trust scores."""
    validator_state.trust = 0.8
    validator_state.vtrust = 0.9
    
    assert validator_state.get_trust() == 0.8
    assert validator_state.get_vtrust() == 0.9 