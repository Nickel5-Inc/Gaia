import pytest
from datetime import datetime, timezone
import numpy as np
from gaia.tasks.defined_tasks.geomagnetic.validation import (
    validate_geomag_data,
    validate_miner_query,
    validate_miner_response,
    validate_scored_prediction
)

@pytest.mark.asyncio
async def test_validate_geomag_data():
    """Test geomagnetic data validation."""
    # Test valid data
    valid_data = {
        'timestamp': datetime.now(timezone.utc),
        'dst_value': -25.5,
        'historical_data': [
            {'timestamp': datetime.now(timezone.utc), 'Dst': -20.0}
        ]
    }
    validated = validate_geomag_data(valid_data)
    assert validated is not None
    assert isinstance(validated.dst_value, float)
    
    # Test invalid data
    with pytest.raises(ValueError):
        validate_geomag_data({
            'timestamp': 'invalid',
            'dst_value': 'not a number',
            'historical_data': None
        })

@pytest.mark.asyncio
async def test_validate_miner_query():
    """Test miner query validation."""
    valid_query = {
        'nonce': '123',
        'data': {
            'name': 'Test Data',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'value': -25.5,
            'historical_values': []
        }
    }
    validated = validate_miner_query(valid_query)
    assert validated is not None
    assert validated.nonce == '123'
    
    # Test invalid query
    with pytest.raises(ValueError):
        validate_miner_query({
            'nonce': None,
            'data': {}
        })

@pytest.mark.asyncio
async def test_validate_miner_response(mock_validator):
    """Test miner response validation."""
    valid_response = {
        'text': '{"predicted_values": -30.5, "timestamp": "2024-03-19T12:00:00Z"}'
    }
    context = {'current_time': datetime.now(timezone.utc)}
    
    validated = await validate_miner_response(valid_response, context)
    assert validated is not None
    assert isinstance(validated['predicted_value'], float)
    
    # Test invalid response
    invalid_response = {'text': 'invalid json'}
    result = await validate_miner_response(invalid_response, context)
    assert result is None

@pytest.mark.asyncio
async def test_validate_scored_prediction():
    """Test scored prediction validation."""
    valid_prediction = {
        'prediction': {
            'miner_uid': '123',
            'miner_hotkey': 'test_key',
            'predicted_value': -25.5,
            'query_time': datetime.now(timezone.utc)
        },
        'ground_truth': -26.0,
        'score': 0.95,
        'score_time': datetime.now(timezone.utc)
    }
    validated = validate_scored_prediction(valid_prediction)
    assert validated is not None
    assert isinstance(validated.score, float)
    assert validated.score >= 0
    
    # Test invalid prediction
    with pytest.raises(ValueError):
        validate_scored_prediction({
            'prediction': {},
            'ground_truth': None,
            'score': 'invalid',
            'score_time': 'invalid'
        }) 