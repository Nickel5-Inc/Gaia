import pytest
import numpy as np
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from gaia.validator.components.weight_manager import WeightManager

@pytest.fixture
def mock_database_manager():
    """Mock database manager."""
    mock = Mock()
    mock.fetch_one = AsyncMock()
    mock.fetch_many = AsyncMock()
    mock.execute = AsyncMock()
    mock.update_weights = AsyncMock()
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
    mock.nodes = {
        f"hotkey_{i}": Mock(
            hotkey=f"hotkey_{i}",
            uid=i,
            stake=1000.0,
            incentive=0.5,
            trust=0.8,
            vtrust=0.9,
            active=True
        )
        for i in range(5)
    }
    return mock

@pytest.fixture
def mock_substrate():
    """Mock substrate."""
    mock = Mock()
    mock.get_block_number = AsyncMock(return_value=1000)
    mock.get_block_hash = AsyncMock(return_value="0x123...")
    mock.get_block_timestamp = AsyncMock(return_value=1234567890)
    mock.set_weights = AsyncMock()
    return mock

@pytest.fixture
def mock_tasks():
    """Mock task instances."""
    soil_task = Mock()
    soil_task.get_recent_scores = AsyncMock(return_value={
        i: 0.8 for i in range(5)
    })
    
    geo_task = Mock()
    geo_task.get_recent_scores = AsyncMock(return_value={
        i: 0.9 for i in range(5)
    })
    
    return soil_task, geo_task

@pytest.fixture
def weight_manager(mock_database_manager, mock_metagraph, mock_substrate, mock_tasks):
    """Fixture to provide weight manager instance."""
    soil_task, geo_task = mock_tasks
    return WeightManager(
        database_manager=mock_database_manager,
        metagraph=mock_metagraph,
        substrate=mock_substrate,
        soil_task=soil_task,
        geomagnetic_task=geo_task
    )

@pytest.mark.asyncio
async def test_fetch_task_scores(weight_manager, mock_tasks):
    """Test fetching task scores."""
    soil_task, geo_task = mock_tasks
    
    soil_scores, geo_scores = await weight_manager.fetch_task_scores()
    
    # Verify task score fetching
    soil_task.get_recent_scores.assert_called_once()
    geo_task.get_recent_scores.assert_called_once()
    
    # Verify scores
    assert len(soil_scores) == 5
    assert len(geo_scores) == 5
    for i in range(5):
        assert soil_scores[i] == 0.8
        assert geo_scores[i] == 0.9

@pytest.mark.asyncio
async def test_calculate_aggregate_weights(weight_manager):
    """Test calculating aggregate weights."""
    # Setup test scores
    soil_scores = {i: 0.8 for i in range(5)}
    geo_scores = {i: 0.9 for i in range(5)}
    
    weights = await weight_manager.calculate_aggregate_weights(soil_scores, geo_scores)
    
    # Verify weights calculation
    assert len(weights) == 5
    for i in range(5):
        assert weights[i] > 0  # Weights should be positive
        assert weights[i] <= 1  # Weights should be normalized

@pytest.mark.asyncio
async def test_normalize_weights(weight_manager):
    """Test weight normalization."""
    # Setup test weights
    weights = {i: float(i + 1) for i in range(5)}  # [1.0, 2.0, 3.0, 4.0, 5.0]
    
    normalized = await weight_manager.normalize_weights(weights)
    
    # Verify normalization
    assert len(normalized) == 5
    assert abs(sum(normalized.values()) - 1.0) < 1e-6  # Sum should be close to 1
    for i in range(5):
        assert 0 <= normalized[i] <= 1  # Each weight should be between 0 and 1

@pytest.mark.asyncio
async def test_set_weights(weight_manager, mock_substrate):
    """Test setting weights on chain."""
    # Setup test weights
    weights = {i: 0.2 for i in range(5)}  # Equal weights summing to 1
    
    await weight_manager.set_weights(weights)
    
    # Verify substrate call
    mock_substrate.set_weights.assert_called_once()
    
    # Verify weight update timestamp
    assert weight_manager.last_weight_set_time > 0

@pytest.mark.asyncio
async def test_set_weights_failure(weight_manager, mock_substrate):
    """Test weight setting failure."""
    mock_substrate.set_weights.side_effect = Exception("Set weights failed")
    
    with pytest.raises(Exception):
        await weight_manager.set_weights({0: 1.0})

@pytest.mark.asyncio
async def test_check_weight_setting_interval(weight_manager):
    """Test checking weight setting interval."""
    # Test when weights should be set (no previous set time)
    weight_manager.last_weight_set_time = 0
    should_set = await weight_manager.check_weight_setting_interval()
    assert should_set is True
    
    # Test when weights should not be set (recently set)
    weight_manager.last_weight_set_time = datetime.now(timezone.utc).timestamp()
    should_set = await weight_manager.check_weight_setting_interval()
    assert should_set is False

@pytest.mark.asyncio
async def test_handle_nan_weights(weight_manager):
    """Test handling NaN weights."""
    # Setup test weights with NaN values
    weights = {
        0: np.nan,
        1: 0.5,
        2: np.nan,
        3: 0.5,
        4: 0.0
    }
    
    cleaned_weights = await weight_manager.handle_nan_weights(weights)
    
    # Verify NaN handling
    assert len(cleaned_weights) == 5
    for uid, weight in cleaned_weights.items():
        assert not np.isnan(weight)  # No NaN values
        assert weight >= 0  # Non-negative weights

@pytest.mark.asyncio
async def test_update_weights(weight_manager, mock_database_manager):
    """Test complete weight update cycle."""
    await weight_manager.update_weights()
    
    # Verify database update
    mock_database_manager.update_weights.assert_called_once()
    
    # Verify weight update timestamp
    assert weight_manager.last_weight_update_time > 0

@pytest.mark.asyncio
async def test_update_weights_failure(weight_manager, mock_database_manager):
    """Test weight update failure."""
    mock_database_manager.update_weights.side_effect = Exception("Update failed")
    
    with pytest.raises(Exception):
        await weight_manager.update_weights()
    
    # Verify recovery attempt
    mock_database_manager.reset_pool.assert_called_once()

def test_get_last_update_time(weight_manager):
    """Test getting last update time."""
    # Set test timestamps
    weight_manager.last_weight_update_time = 1234567890
    weight_manager.last_weight_set_time = 1234567891
    
    assert weight_manager.get_last_update_time() == 1234567890
    assert weight_manager.get_last_set_time() == 1234567891 