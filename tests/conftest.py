"""
Global pytest configuration and fixtures for Gaia Validator v4.0 tests.
"""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, MagicMock
from typing import Dict, Any
import xarray as xr
import numpy as np

from gaia.validator.utils.config import Settings
from gaia.validator.utils.ipc_types import WorkUnit, ResultUnit


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_config():
    """Create a test configuration with safe default values."""
    return Settings(
        NUM_COMPUTE_WORKERS=2,
        MAX_JOBS_PER_WORKER=5,
        SUPERVISOR_CHECK_INTERVAL_S=1,
        WORKER_QUEUE_TIMEOUT_S=5,
        PROCESS_MAX_RSS_MB={
            "io": 512,
            "compute": 256,
            "supervisor": 128
        },
        IPC_QUEUE_SIZE=100,
        DATABASE=Settings.DatabaseSettings(
            HOST="localhost",
            PORT=5432,
            NAME="test_gaia_validator",
            USER="test_user",
            PASSWORD="test_password",
            POOL_MIN_SIZE=1,
            POOL_MAX_SIZE=5
        ),
        WEATHER=Settings.WeatherSettings(
            GFS_CACHE_DIR="./test_gfs_cache",
            VALIDATOR_HASH_WAIT_MINUTES=1,
            INITIAL_SCORING_LEAD_HOURS=[6, 12],
            DAY1_VARIABLES_LEVELS_TO_SCORE=[
                {"name": "2t", "level": None, "standard_name": "2m_temperature"},
                {"name": "msl", "level": None, "standard_name": "mean_sea_level_pressure"}
            ]
        )
    )


@pytest.fixture
def temp_cache_dir():
    """Create a temporary cache directory for tests."""
    temp_dir = tempfile.mkdtemp(prefix="gaia_test_cache_")
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def mock_work_unit():
    """Create a mock WorkUnit for testing."""
    return WorkUnit(
        job_id="test_job_123",
        task_name="weather.test.mock",
        payload={"test_param": "test_value"},
        timeout_seconds=60
    )


@pytest.fixture
def mock_result_unit():
    """Create a mock ResultUnit for testing."""
    return ResultUnit(
        job_id="test_job_123",
        task_name="weather.test.mock",
        success=True,
        result={"test_result": "success"},
        worker_pid=12345,
        execution_time_ms=150.5
    )


@pytest.fixture
def sample_weather_dataset():
    """Create a sample weather dataset for testing."""
    # Create sample coordinates
    lat = np.linspace(90, -90, 5)  # 5 latitude points
    lon = np.linspace(0, 359, 8)  # 8 longitude points
    time = np.array(['2023-01-01T00:00:00', '2023-01-01T06:00:00'], dtype='datetime64[ns]')
    pressure_levels = [500, 850]  # hPa
    
    # Create sample data
    coords = {
        'time': time,
        'lat': lat,
        'lon': lon,
        'level': pressure_levels
    }
    
    # Surface variables
    temp_2m = np.random.normal(275, 10, (2, 5, 8))  # 2m temperature in Kelvin
    msl_pressure = np.random.normal(101325, 1000, (2, 5, 8))  # Sea level pressure in Pa
    
    # Atmospheric variables  
    temp_atmos = np.random.normal(250, 20, (2, 2, 5, 8))  # Temperature at pressure levels
    geopotential = np.random.normal(50000, 5000, (2, 2, 5, 8))  # Geopotential height
    
    # Create dataset
    ds = xr.Dataset({
        '2t': (['time', 'lat', 'lon'], temp_2m),
        'msl': (['time', 'lat', 'lon'], msl_pressure),
        't': (['time', 'level', 'lat', 'lon'], temp_atmos),
        'z': (['time', 'level', 'lat', 'lon'], geopotential),
    }, coords=coords)
    
    # Add attributes
    ds['2t'].attrs = {'units': 'K', 'long_name': '2 metre temperature'}
    ds['msl'].attrs = {'units': 'Pa', 'long_name': 'Mean sea level pressure'}
    ds['t'].attrs = {'units': 'K', 'long_name': 'Temperature'}
    ds['z'].attrs = {'units': 'm**2 s**-2', 'long_name': 'Geopotential'}
    
    return ds


@pytest.fixture
def mock_database_pool():
    """Create a mock asyncpg connection pool for testing."""
    mock_pool = MagicMock()
    
    # Mock common database operations
    async def mock_fetchval(*args, **kwargs):
        return 1  # Mock ID return
    
    async def mock_fetchrow(*args, **kwargs):
        return {"id": 1, "status": "pending"}
    
    async def mock_fetch(*args, **kwargs):
        return [{"id": 1, "status": "pending"}, {"id": 2, "status": "completed"}]
    
    async def mock_execute(*args, **kwargs):
        return "INSERT 0 1"
    
    mock_pool.fetchval = mock_fetchval
    mock_pool.fetchrow = mock_fetchrow  
    mock_pool.fetch = mock_fetch
    mock_pool.execute = mock_execute
    
    return mock_pool


@pytest.fixture
def sample_miner_response():
    """Create a sample miner response record for testing."""
    return {
        "id": 1,
        "run_id": 100,
        "miner_uid": 42,
        "miner_hotkey": "5D4Ak4YjKmStf5dG8vK8X1jB9xGmZkX7HvZzKqSvZkX7HvZz",
        "job_id": "weather_job_100_42",
        "response_time": "2023-01-01T12:00:00Z",
        "status": "verified"
    }


@pytest.fixture
def mock_io_engine():
    """Create a mock IO-Engine for testing."""
    mock_engine = MagicMock()
    mock_engine.config = test_config()
    mock_engine.http_client = MagicMock()
    
    # Mock dispatch method
    async def mock_dispatch_and_wait(work_unit):
        # Simulate successful computation
        return ResultUnit(
            job_id=work_unit.job_id,
            task_name=work_unit.task_name,
            success=True,
            result={"mock_score": 0.85},
            worker_pid=12345,
            execution_time_ms=250.0
        )
    
    mock_engine.dispatch_and_wait = mock_dispatch_and_wait
    return mock_engine


@pytest.fixture
def mock_miner_client():
    """Create a mock WeatherMinerClient for testing."""
    from gaia.tasks.defined_tasks.weather.validator.miner_client import WeatherMinerClient
    
    mock_client = MagicMock(spec=WeatherMinerClient)
    
    # Mock miner communication methods
    async def mock_initiate_fetch(*args, **kwargs):
        return {
            "miner1_hotkey": {"status": "fetch_accepted", "job_id": "job_1"},
            "miner2_hotkey": {"status": "fetch_accepted", "job_id": "job_2"}
        }
    
    async def mock_get_status(*args, **kwargs):
        return {
            "miner1_hotkey": {"status": "input_hashed_awaiting_validation", "input_data_hash": "hash123"},
            "miner2_hotkey": {"status": "input_hashed_awaiting_validation", "input_data_hash": "hash123"}
        }
    
    async def mock_trigger_inference(*args, **kwargs):
        return {"miner1_hotkey": True, "miner2_hotkey": True}
    
    mock_client.initiate_fetch_from_miners = mock_initiate_fetch
    mock_client.get_input_status_from_miners = mock_get_status
    mock_client.trigger_inference_on_miners = mock_trigger_inference
    
    return mock_client