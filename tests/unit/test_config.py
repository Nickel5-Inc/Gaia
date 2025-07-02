"""
Unit tests for the configuration module.
"""

import pytest
import os
import tempfile
from unittest.mock import patch, mock_open

from gaia.validator.utils.config import Settings


@pytest.mark.unit
class TestSettings:
    """Test the Settings configuration class."""
    
    def test_default_settings(self):
        """Test that default settings are loaded correctly."""
        settings = Settings()
        
        # Test process management defaults
        assert settings.NUM_COMPUTE_WORKERS == 8
        assert settings.MAX_JOBS_PER_WORKER == 100
        assert settings.SUPERVISOR_CHECK_INTERVAL_S == 30
        
        # Test memory limits
        assert settings.PROCESS_MAX_RSS_MB["io"] == 2048
        assert settings.PROCESS_MAX_RSS_MB["compute"] == 1024
        assert settings.PROCESS_MAX_RSS_MB["supervisor"] == 256
        
        # Test IPC configuration
        assert settings.IPC_QUEUE_SIZE == 1000
    
    def test_weather_settings_defaults(self):
        """Test weather-specific configuration defaults."""
        settings = Settings()
        
        # Test weather settings
        assert settings.WEATHER.GFS_CACHE_DIR == "./gfs_analysis_cache"
        assert settings.WEATHER.VALIDATOR_HASH_WAIT_MINUTES == 10
        assert len(settings.WEATHER.INITIAL_SCORING_LEAD_HOURS) > 0
        assert len(settings.WEATHER.DAY1_VARIABLES_LEVELS_TO_SCORE) > 0
        
        # Test scoring parameters
        assert 0 <= settings.WEATHER.DAY1_ALPHA_SKILL <= 1
        assert 0 <= settings.WEATHER.DAY1_BETA_ACC <= 1
        assert settings.WEATHER.DAY1_ALPHA_SKILL + settings.WEATHER.DAY1_BETA_ACC == 1.0
    
    def test_database_settings_defaults(self):
        """Test database configuration defaults."""
        settings = Settings()
        
        assert settings.DATABASE.HOST == "localhost"
        assert settings.DATABASE.PORT == 5432
        assert settings.DATABASE.NAME == "gaia_validator"
        assert settings.DATABASE.POOL_MIN_SIZE == 5
        assert settings.DATABASE.POOL_MAX_SIZE == 20
    
    def test_substrate_settings_defaults(self):
        """Test substrate network configuration defaults."""
        settings = Settings()
        
        assert settings.SUBSTRATE.NETWORK == "finney"
        assert settings.SUBSTRATE.NETUID == 15
        assert settings.SUBSTRATE.SUBTENSOR_ADDRESS is not None
    
    @patch.dict(os.environ, {
        "NUM_COMPUTE_WORKERS": "12",
        "DATABASE_HOST": "test-db-host",
        "WEATHER_VALIDATOR_HASH_WAIT_MINUTES": "5"
    })
    def test_environment_variable_override(self):
        """Test that environment variables override defaults."""
        settings = Settings()
        
        assert settings.NUM_COMPUTE_WORKERS == 12
        assert settings.DATABASE.HOST == "test-db-host" 
        assert settings.WEATHER.VALIDATOR_HASH_WAIT_MINUTES == 5
    
    def test_invalid_configuration_validation(self):
        """Test that invalid configurations are caught."""
        # Test invalid worker count
        with pytest.raises(ValueError, match="NUM_COMPUTE_WORKERS must be positive"):
            Settings(NUM_COMPUTE_WORKERS=0)
        
        # Test invalid memory limits
        with pytest.raises(ValueError, match="Memory limits must be positive"):
            Settings(PROCESS_MAX_RSS_MB={"compute": -1})
    
    def test_weather_variables_validation(self):
        """Test validation of weather scoring variables."""
        # Test valid variables
        valid_vars = [
            {"name": "2t", "level": None, "standard_name": "2m_temperature"},
            {"name": "z", "level": 500, "standard_name": "geopotential"}
        ]
        settings = Settings(WEATHER=Settings.WeatherSettings(
            DAY1_VARIABLES_LEVELS_TO_SCORE=valid_vars
        ))
        assert len(settings.WEATHER.DAY1_VARIABLES_LEVELS_TO_SCORE) == 2
        
        # Test missing required fields
        invalid_vars = [{"name": "2t"}]  # Missing standard_name
        with pytest.raises(ValueError, match="Variable configuration missing required field"):
            Settings(WEATHER=Settings.WeatherSettings(
                DAY1_VARIABLES_LEVELS_TO_SCORE=invalid_vars
            ))
    
    def test_scoring_parameters_validation(self):
        """Test validation of scoring parameters."""
        # Test valid alpha/beta combination
        settings = Settings(WEATHER=Settings.WeatherSettings(
            DAY1_ALPHA_SKILL=0.6,
            DAY1_BETA_ACC=0.4
        ))
        assert settings.WEATHER.DAY1_ALPHA_SKILL + settings.WEATHER.DAY1_BETA_ACC == 1.0
        
        # Test invalid alpha/beta combination
        with pytest.raises(ValueError, match="Alpha and Beta must sum to 1.0"):
            Settings(WEATHER=Settings.WeatherSettings(
                DAY1_ALPHA_SKILL=0.6,
                DAY1_BETA_ACC=0.5  # Sum = 1.1, invalid
            ))
    
    def test_database_url_construction(self):
        """Test that database URL is constructed correctly."""
        settings = Settings(
            DATABASE=Settings.DatabaseSettings(
                HOST="testhost",
                PORT=5433,
                NAME="testdb",
                USER="testuser",
                PASSWORD="testpass"
            )
        )
        
        expected_url = "postgresql://testuser:testpass@testhost:5433/testdb"
        assert settings.DATABASE.get_url() == expected_url
    
    def test_cache_directory_creation(self):
        """Test that cache directories are created if they don't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = os.path.join(temp_dir, "test_cache")
            
            settings = Settings(WEATHER=Settings.WeatherSettings(
                GFS_CACHE_DIR=cache_path
            ))
            
            # Directory should be created automatically
            assert os.path.exists(cache_path)
            assert os.path.isdir(cache_path)
    
    @patch("builtins.open", mock_open(read_data="TEST_VAR=test_value"))
    def test_env_file_loading(self):
        """Test loading configuration from .env file."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.exists", return_value=True):
                # This would normally load from .env file
                # In our mock, we're simulating the file content
                pass
    
    def test_r2_settings_validation(self):
        """Test R2 storage configuration validation."""
        settings = Settings()
        
        # Test that R2 settings have required fields
        assert hasattr(settings.R2, 'ENDPOINT_URL')
        assert hasattr(settings.R2, 'BUCKET_NAME')
        
        # Test validation of R2 cleanup settings
        if settings.R2.CLEANUP_ENABLED:
            assert settings.R2.CLEANUP_RETENTION_DAYS > 0
    
    def test_http_client_settings(self):
        """Test HTTP client configuration."""
        settings = Settings()
        
        assert settings.HTTP_TIMEOUT_SECONDS > 0
        assert settings.HTTP_MAX_CONNECTIONS > 0
        assert settings.HTTP_RETRY_ATTEMPTS >= 0
    
    def test_prometheus_metrics_settings(self):
        """Test Prometheus metrics configuration."""
        settings = Settings()
        
        if settings.PROMETHEUS_ENABLED:
            assert settings.PROMETHEUS_PORT > 0
            assert settings.PROMETHEUS_PORT < 65536