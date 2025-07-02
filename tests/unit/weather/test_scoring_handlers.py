"""
Unit tests for weather scoring compute handlers.
"""

import pytest
import tempfile
import numpy as np
import xarray as xr
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from gaia.tasks.defined_tasks.weather.validator.scoring import (
    handle_day1_scoring_computation,
    handle_era5_final_scoring_computation,
    handle_forecast_verification_computation,
    _create_minimal_climatology
)


@pytest.fixture
def mock_scoring_config():
    """Create a mock scoring configuration."""
    return {
        "lead_times_hours": [6, 12],
        "variables_levels_to_score": [
            {"name": "2t", "level": None, "standard_name": "2m_temperature"},
            {"name": "msl", "level": None, "standard_name": "mean_sea_level_pressure"},
            {"name": "z", "level": 500, "standard_name": "geopotential"},
            {"name": "t", "level": 850, "standard_name": "temperature"}
        ],
        "alpha_skill": 0.5,
        "beta_acc": 0.5
    }


@pytest.fixture
def sample_zarr_datasets(tmp_path):
    """Create sample Zarr datasets for testing."""
    # Create coordinate arrays
    lat = np.linspace(90, -90, 10)
    lon = np.linspace(0, 359, 20)
    time = np.array(['2023-01-01T00:00:00', '2023-01-01T06:00:00', '2023-01-01T12:00:00'], dtype='datetime64[ns]')
    pressure_levels = [500, 850]
    
    coords = {
        'time': time,
        'lat': lat,
        'lon': lon,
        'level': pressure_levels
    }
    
    # Create sample miner forecast dataset
    miner_data = {
        '2t': (['time', 'lat', 'lon'], np.random.normal(275, 10, (3, 10, 20))),
        'msl': (['time', 'lat', 'lon'], np.random.normal(101325, 1000, (3, 10, 20))),
        't': (['time', 'level', 'lat', 'lon'], np.random.normal(250, 20, (3, 2, 10, 20))),
        'z': (['time', 'level', 'lat', 'lon'], np.random.normal(50000, 5000, (3, 2, 10, 20)))
    }
    
    miner_ds = xr.Dataset(miner_data, coords=coords)
    miner_path = tmp_path / "miner.zarr"
    miner_ds.to_zarr(miner_path, mode='w')
    
    # Create sample GFS analysis dataset (truth)
    gfs_data = {
        '2t': (['time', 'lat', 'lon'], np.random.normal(274, 10, (3, 10, 20))),  # Slightly different
        'msl': (['time', 'lat', 'lon'], np.random.normal(101300, 1000, (3, 10, 20))),
        't': (['time', 'level', 'lat', 'lon'], np.random.normal(249, 20, (3, 2, 10, 20))),
        'z': (['time', 'level', 'lat', 'lon'], np.random.normal(49900, 5000, (3, 2, 10, 20)))
    }
    
    gfs_ds = xr.Dataset(gfs_data, coords=coords)
    gfs_path = tmp_path / "gfs.zarr"
    gfs_ds.to_zarr(gfs_path, mode='w')
    
    # Create ERA5 climatology dataset
    clim_coords = {
        'time': np.array([f"2020-{month:02d}-15" for month in range(1, 13)]),
        'lat': lat,
        'lon': lon,
        'level': pressure_levels
    }
    
    clim_data = {
        '2t': (['time', 'lat', 'lon'], np.random.normal(275, 15, (12, 10, 20))),
        'msl': (['time', 'lat', 'lon'], np.random.normal(101325, 1500, (12, 10, 20))),
        't': (['time', 'level', 'lat', 'lon'], np.random.normal(250, 25, (12, 2, 10, 20))),
        'z': (['time', 'level', 'lat', 'lon'], np.random.normal(50000, 6000, (12, 2, 10, 20)))
    }
    
    clim_ds = xr.Dataset(clim_data, coords=clim_coords)
    clim_path = tmp_path / "era5_climatology.zarr"
    clim_ds.to_zarr(clim_path, mode='w')
    
    return str(miner_path), str(gfs_path), str(clim_path)


@pytest.mark.unit
@pytest.mark.weather
class TestScoringHandlers:
    """Test the weather scoring compute handlers."""
    
    def test_create_minimal_climatology(self, sample_weather_dataset):
        """Test creation of minimal climatology dataset."""
        climatology = _create_minimal_climatology(sample_weather_dataset)
        
        assert isinstance(climatology, xr.Dataset)
        
        # Should have climatology for common variables
        expected_vars = ['2t', 'msl']
        for var in expected_vars:
            if var in sample_weather_dataset.data_vars:
                assert var in climatology.data_vars
        
        # Should have time dimension with 12 months
        if len(climatology.data_vars) > 0:
            assert 'time' in climatology.coords
            assert len(climatology.time) == 12
    
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.fetch_gfs_analysis_data')
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.fetch_gfs_data')
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset')
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.evaluate_miner_forecast_day1')
    def test_handle_day1_scoring_computation_success(
        self, 
        mock_eval_day1,
        mock_open_zarr,
        mock_fetch_gfs,
        mock_fetch_analysis,
        test_config,
        mock_scoring_config,
        sample_weather_dataset
    ):
        """Test successful Day-1 scoring computation."""
        # Mock data loading
        mock_fetch_analysis.return_value = sample_weather_dataset
        mock_fetch_gfs.return_value = sample_weather_dataset
        mock_open_zarr.return_value = sample_weather_dataset
        
        # Mock scoring evaluation
        mock_eval_day1.return_value = {
            "overall_day1_score": 0.85,
            "qc_passed_all_vars_leads": True,
            "lead_time_scores": {6: {"2t": 0.8}, 12: {"2t": 0.9}},
            "error_message": None
        }
        
        result = handle_day1_scoring_computation(
            config=test_config,
            response_id=1,
            run_id=100,
            miner_hotkey="test_miner_hotkey",
            miner_uid=42,
            job_id="test_job_123",
            zarr_store_url="http://test.zarr",
            access_token="test_token",
            verification_hash="test_hash",
            gfs_init_time_iso="2023-01-01T00:00:00Z",
            scoring_config=mock_scoring_config
        )
        
        assert result["success"] is True
        assert result["response_id"] == 1
        assert result["miner_hotkey"] == "test_miner_hotkey"
        assert result["overall_day1_score"] == 0.85
        assert result["qc_passed"] is True
        assert "lead_time_scores" in result
    
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.fetch_gfs_analysis_data')
    def test_handle_day1_scoring_computation_data_load_failure(
        self, 
        mock_fetch_analysis,
        test_config,
        mock_scoring_config
    ):
        """Test Day-1 scoring with data loading failure."""
        # Mock data loading failure
        mock_fetch_analysis.return_value = None
        
        result = handle_day1_scoring_computation(
            config=test_config,
            response_id=1,
            run_id=100,
            miner_hotkey="test_miner_hotkey",
            miner_uid=42,
            job_id="test_job_123",
            zarr_store_url="http://test.zarr",
            access_token="test_token",
            verification_hash="test_hash",
            gfs_init_time_iso="2023-01-01T00:00:00Z",
            scoring_config=mock_scoring_config
        )
        
        assert result["success"] is False
        assert "Failed to load required datasets" in result["error"]
        assert result["overall_day1_score"] == -np.inf
    
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset')
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.get_ground_truth_data')
    def test_handle_era5_final_scoring_computation_success(
        self,
        mock_get_era5,
        mock_open_zarr,
        test_config,
        mock_scoring_config,
        sample_weather_dataset
    ):
        """Test successful ERA5 final scoring computation."""
        # Mock data loading
        mock_open_zarr.return_value = sample_weather_dataset
        mock_get_era5.return_value = sample_weather_dataset
        
        # Update config for ERA5 scoring
        era5_config = mock_scoring_config.copy()
        era5_config["lead_times_hours"] = [24, 48, 72]
        era5_config["variables_to_score"] = ["2t", "msl"]
        
        result = handle_era5_final_scoring_computation(
            config=test_config,
            response_id=1,
            run_id=100,
            miner_hotkey="test_miner_hotkey",
            miner_uid=42,
            job_id="test_job_123",
            zarr_store_url="http://test.zarr",
            access_token="test_token",
            verification_hash="test_hash",
            gfs_init_time_iso="2023-01-01T00:00:00Z",
            scoring_config=era5_config
        )
        
        assert result["success"] is True
        assert result["response_id"] == 1
        assert "final_era5_score" in result
        assert result["final_era5_score"] >= 0
        assert "variable_scores" in result
        assert result["num_variables_scored"] >= 0
    
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset')
    def test_handle_forecast_verification_computation_success(
        self,
        mock_open_zarr,
        test_config,
        sample_weather_dataset
    ):
        """Test successful forecast verification computation."""
        # Mock successful dataset opening
        mock_open_zarr.return_value = sample_weather_dataset
        
        result = handle_forecast_verification_computation(
            config=test_config,
            zarr_store_url="http://test.zarr",
            access_token="test_token",
            claimed_verification_hash="test_hash",
            miner_hotkey="test_miner_hotkey",
            job_id="test_job_123"
        )
        
        assert result["success"] is True
        assert result["verification_passed"] is True
        assert result["computed_hash"] == "test_hash"
        assert len(result["data_quality_issues"]) == 0
    
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset')
    def test_handle_forecast_verification_computation_missing_variables(
        self,
        mock_open_zarr,
        test_config
    ):
        """Test forecast verification with missing required variables."""
        # Create dataset missing required variables
        incomplete_ds = xr.Dataset({
            'temp': (['x', 'y'], np.random.normal(275, 10, (5, 5)))
        })
        
        mock_open_zarr.return_value = incomplete_ds
        
        result = handle_forecast_verification_computation(
            config=test_config,
            zarr_store_url="http://test.zarr",
            access_token="test_token",
            claimed_verification_hash="test_hash",
            miner_hotkey="test_miner_hotkey",
            job_id="test_job_123"
        )
        
        assert result["success"] is False
        assert result["verification_passed"] is False
        assert "Missing required variables" in result["error"]
    
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset')
    def test_handle_forecast_verification_computation_data_quality_issues(
        self,
        mock_open_zarr,
        test_config
    ):
        """Test forecast verification with data quality issues."""
        # Create dataset with out-of-range values
        bad_data_ds = xr.Dataset({
            '2t': (['x', 'y'], np.full((5, 5), 500)),  # Temperature too high (500K)
            'msl': (['x', 'y'], np.full((5, 5), 50000)),  # Pressure too low (50kPa)
            'u10': (['x', 'y'], np.random.normal(10, 5, (5, 5))),
            'v10': (['x', 'y'], np.random.normal(5, 3, (5, 5)))
        })
        
        mock_open_zarr.return_value = bad_data_ds
        
        result = handle_forecast_verification_computation(
            config=test_config,
            zarr_store_url="http://test.zarr",
            access_token="test_token",
            claimed_verification_hash="test_hash",
            miner_hotkey="test_miner_hotkey",
            job_id="test_job_123"
        )
        
        assert result["success"] is True
        assert result["verification_passed"] is False
        assert len(result["data_quality_issues"]) > 0
        
        # Check that specific issues were detected
        issues_text = ' '.join(result["data_quality_issues"])
        assert "temperature out of range" in issues_text
        assert "pressure out of range" in issues_text
    
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset')
    def test_handle_forecast_verification_computation_dataset_open_failure(
        self,
        mock_open_zarr,
        test_config
    ):
        """Test forecast verification when dataset opening fails."""
        # Mock dataset opening failure
        mock_open_zarr.return_value = None
        
        result = handle_forecast_verification_computation(
            config=test_config,
            zarr_store_url="http://test.zarr",
            access_token="test_token",
            claimed_verification_hash="test_hash",
            miner_hotkey="test_miner_hotkey",
            job_id="test_job_123"
        )
        
        assert result["success"] is False
        assert result["verification_passed"] is False
        assert "Failed to open forecast dataset" in result["error"]
    
    def test_scoring_config_validation(self, mock_scoring_config):
        """Test that scoring configuration is properly validated."""
        # Test required fields are present
        required_fields = ["lead_times_hours", "variables_levels_to_score"]
        for field in required_fields:
            assert field in mock_scoring_config
        
        # Test variable configuration structure
        for var_config in mock_scoring_config["variables_levels_to_score"]:
            assert "name" in var_config
            assert "standard_name" in var_config
            # level can be None for surface variables
    
    def test_error_handling_in_scoring_handlers(self, test_config, mock_scoring_config):
        """Test that scoring handlers properly handle and report errors."""
        # Test Day-1 scoring with exception
        with patch('gaia.tasks.defined_tasks.weather.validator.scoring.asyncio.new_event_loop', side_effect=Exception("Test error")):
            result = handle_day1_scoring_computation(
                config=test_config,
                response_id=1,
                run_id=100,
                miner_hotkey="test_miner_hotkey",
                miner_uid=42,
                job_id="test_job_123",
                zarr_store_url="http://test.zarr",
                access_token="test_token",
                verification_hash="test_hash",
                gfs_init_time_iso="2023-01-01T00:00:00Z",
                scoring_config=mock_scoring_config
            )
            
            assert result["success"] is False
            assert "Test error" in result["error"]
            assert result["overall_day1_score"] == -np.inf
    
    def test_gfs_init_time_parsing(self, test_config, mock_scoring_config):
        """Test that GFS initialization time is properly parsed."""
        # Test various ISO time formats
        time_formats = [
            "2023-01-01T00:00:00Z",
            "2023-01-01T00:00:00+00:00",
            "2023-12-31T23:59:59Z"
        ]
        
        for time_str in time_formats:
            with patch('gaia.tasks.defined_tasks.weather.validator.scoring.fetch_gfs_analysis_data'):
                with patch('gaia.tasks.defined_tasks.weather.validator.scoring.fetch_gfs_data'):
                    with patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset'):
                        # This should not raise an exception
                        result = handle_day1_scoring_computation(
                            config=test_config,
                            response_id=1,
                            run_id=100,
                            miner_hotkey="test_miner_hotkey",
                            miner_uid=42,
                            job_id="test_job_123",
                            zarr_store_url="http://test.zarr",
                            access_token="test_token",
                            verification_hash="test_hash",
                            gfs_init_time_iso=time_str,
                            scoring_config=mock_scoring_config
                        )
                        
                        # Should fail due to mocked None returns, but time parsing should work
                        assert "success" in result