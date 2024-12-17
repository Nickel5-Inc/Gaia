import pytest
from datetime import datetime, timezone
import numpy as np
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask

@pytest.mark.asyncio
async def test_task_initialization(mock_database, mock_validator):
    """Test basic task initialization."""
    task = GeomagneticTask(db_manager=mock_database)
    assert task is not None
    assert task.name == "GeomagneticTask"
    assert task.model is not None

@pytest.mark.asyncio
async def test_circuit_breaker_mechanism(mock_database):
    """Test circuit breaker functionality."""
    task = GeomagneticTask(db_manager=mock_database)
    
    # Test initial state
    state = await task._check_circuit_state("geomag_api")
    assert state["is_closed"] is True
    
    # Test failure recording
    await task._record_failure("geomag_api")
    state = await task._check_circuit_state("geomag_api")
    assert state["is_closed"] is True
    assert state["failure_count"] == 1
    
    # Test circuit opening
    for _ in range(3):  # Assuming threshold is 3
        await task._record_failure("geomag_api")
    state = await task._check_circuit_state("geomag_api")
    assert state["is_closed"] is False

@pytest.mark.asyncio
async def test_validate_prediction(mock_database):
    """Test prediction validation."""
    task = GeomagneticTask(db_manager=mock_database)
    
    # Test valid prediction
    valid_value = -25.5
    validated = await task.validate_prediction.submit(valid_value)
    assert validated == valid_value
    
    # Test invalid prediction
    invalid_value = "not a number"
    validated = await task.validate_prediction.submit(invalid_value)
    assert validated is None
    
    # Test out of range prediction
    extreme_value = -1000.0
    validated = await task.validate_prediction.submit(extreme_value)
    assert validated is None

@pytest.mark.asyncio
async def test_normalize_score(mock_database):
    """Test score normalization."""
    task = GeomagneticTask(db_manager=mock_database)
    
    # Test perfect score
    perfect_score = await task.normalize_score.submit(0.0)
    assert perfect_score == 1.0
    
    # Test typical score
    typical_score = await task.normalize_score.submit(50.0)
    assert 0 < typical_score < 1
    
    # Test poor score
    poor_score = await task.normalize_score.submit(200.0)
    assert poor_score > 0
    assert poor_score < 0.2

@pytest.mark.asyncio
async def test_process_miner_responses(mock_database, mock_validator, test_timestamp):
    """Test miner response processing."""
    task = GeomagneticTask(db_manager=mock_database)
    
    # Mock responses
    responses = {
        "test_hotkey": {
            "text": '{"predicted_values": -30.5, "timestamp": "2024-03-19T12:00:00Z"}'
        }
    }
    
    # Configure mock database response for miner lookup
    mock_database.fetch_one.return_value = {"uid": "test_uid"}
    
    processed = await task.process_miner_responses.submit(
        responses=responses,
        current_hour_start=test_timestamp,
        validator=mock_validator
    )
    
    assert len(processed) == 1
    assert processed[0]["miner_uid"] == "test_uid"
    assert isinstance(processed[0]["predicted_value"], float)

@pytest.mark.asyncio
async def test_retry_failed_predictions(mock_database):
    """Test retry mechanism for failed predictions."""
    task = GeomagneticTask(db_manager=mock_database)
    
    # Mock failed predictions
    mock_database.fetch_all.return_value = [{
        "id": "test_id",
        "miner_uid": "test_uid",
        "predicted_value": -25.5,
        "retry_count": 1,
        "query_time": datetime.now(timezone.utc)
    }]
    
    recovered = await task.retry_failed_predictions.submit(max_retries=3)
    assert len(recovered) == 1
    assert recovered[0]["id"] == "test_id"
    assert mock_database.execute.called  # Verify database update was attempted 