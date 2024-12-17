import pytest
import json
from unittest.mock import patch, AsyncMock
from datetime import datetime, timezone
from gaia.tasks.defined_tasks.soilmoisture.utils.soil_apis import (
    fetch_hls_b4_b8,
    fetch_srtm,
    fetch_ifs_forecast,
    download_srtm_tile
)

@pytest.mark.asyncio
@pytest.mark.api
async def test_hls_data_fetch(base_temp_dir):
    """Test Sentinel HLS data fetching."""
    bbox = [-122.5, 37.5, -122.0, 38.0]
    test_date = datetime.now(timezone.utc)
    
    with patch('aiohttp.ClientSession.get') as mock_get:
        # Mock CMR search response
        mock_get.return_value.__aenter__.return_value.status = 200
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(
            return_value={
                "feed": {
                    "entry": [{
                        "time_start": test_date.isoformat(),
                        "cloud_cover": "10",
                        "links": [{
                            "href": "https://test.com/B04.tif",
                            "rel": "data"
                        }, {
                            "href": "https://test.com/B08.tif",
                            "rel": "data"
                        }]
                    }]
                }
            }
        )
        
        result = await fetch_hls_b4_b8(bbox, test_date, download_dir=base_temp_dir)
        assert result is not None
        assert len(result) == 2

@pytest.mark.asyncio
@pytest.mark.api
async def test_srtm_data_fetch(base_temp_dir):
    """Test SRTM data fetching."""
    with patch('aiohttp.ClientSession.get') as mock_get:
        mock_get.return_value.__aenter__.return_value.status = 200
        
        result = await download_srtm_tile(38, -122, download_dir=base_temp_dir)
        assert result is not None
        assert ".tif" in result

@pytest.mark.asyncio
@pytest.mark.api
async def test_ifs_forecast_fetch(base_temp_dir):
    """Test IFS forecast data fetching."""
    bbox = [-122.5, 37.5, -122.0, 38.0]
    test_date = datetime.now(timezone.utc)
    
    with patch('aiohttp.ClientSession.get') as mock_get:
        mock_get.return_value.__aenter__.return_value.status = 200
        
        result = await fetch_ifs_forecast(
            bbox,
            test_date,
            sentinel_bounds=[0, 0, 1, 1],
            sentinel_crs="EPSG:4326",
            sentinel_shape=(10, 10)
        )
        assert result is not None

@pytest.mark.asyncio
@pytest.mark.api
async def test_api_error_handling():
    """Test API error handling."""
    bbox = [-122.5, 37.5, -122.0, 38.0]
    test_date = datetime.now(timezone.utc)
    
    with patch('aiohttp.ClientSession.get') as mock_get:
        # Simulate API error
        mock_get.return_value.__aenter__.return_value.status = 500
        
        result = await fetch_hls_b4_b8(bbox, test_date)
        assert result is None

@pytest.mark.asyncio
@pytest.mark.api
async def test_api_retry_mechanism():
    """Test API retry mechanism."""
    bbox = [-122.5, 37.5, -122.0, 38.0]
    test_date = datetime.now(timezone.utc)
    
    with patch('aiohttp.ClientSession.get') as mock_get:
        # Simulate temporary failure then success
        mock_get.return_value.__aenter__.return_value.status = 500
        mock_get.return_value.__aenter__.return_value.status = 200
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(
            return_value={"feed": {"entry": []}}
        )
        
        result = await fetch_hls_b4_b8(bbox, test_date)
        assert mock_get.call_count > 1

@pytest.mark.asyncio
@pytest.mark.api
async def test_api_timeout_handling():
    """Test API timeout handling."""
    bbox = [-122.5, 37.5, -122.0, 38.0]
    test_date = datetime.now(timezone.utc)
    
    with patch('aiohttp.ClientSession.get') as mock_get:
        # Simulate timeout
        mock_get.side_effect = TimeoutError()
        
        result = await fetch_hls_b4_b8(bbox, test_date)
        assert result is None

@pytest.mark.asyncio
@pytest.mark.api
async def test_api_rate_limiting():
    """Test API rate limiting behavior."""
    bbox = [-122.5, 37.5, -122.0, 38.0]
    test_date = datetime.now(timezone.utc)
    
    with patch('aiohttp.ClientSession.get') as mock_get:
        # Simulate rate limit response
        mock_get.return_value.__aenter__.return_value.status = 429
        
        result = await fetch_hls_b4_b8(bbox, test_date)
        assert result is None 