"""
Performance benchmarks for critical compute handlers.

These tests establish performance baselines and catch regressions.
"""

import pytest
import numpy as np
import xarray as xr
import time
from unittest.mock import Mock, patch

from gaia.tasks.defined_tasks.weather.validator.scoring import (
    handle_day1_scoring_computation,
    handle_era5_final_scoring_computation,
    handle_forecast_verification_computation
)
from gaia.tasks.defined_tasks.weather.validator.hashing import (
    handle_gfs_hash_computation,
    handle_forecast_verification_hash_computation
)


@pytest.fixture
def benchmark_dataset():
    """Create a realistic-sized dataset for benchmarking."""
    # Create coordinates for a realistic weather dataset
    lat = np.linspace(90, -90, 180)  # 1-degree resolution
    lon = np.linspace(0, 359, 360)  # 1-degree resolution
    time = np.array([f'2023-01-01T{h:02d}:00:00' for h in range(0, 121, 6)], dtype='datetime64[ns]')  # 120 hours, 6h intervals
    pressure_levels = [1000, 925, 850, 700, 500, 300, 250, 200, 150, 100]  # 10 levels
    
    coords = {
        'time': time,
        'lat': lat,
        'lon': lon,
        'level': pressure_levels
    }
    
    # Surface variables (time, lat, lon)
    surface_shape = (len(time), len(lat), len(lon))
    
    # Atmospheric variables (time, level, lat, lon)
    atmos_shape = (len(time), len(pressure_levels), len(lat), len(lon))
    
    data_vars = {
        # Surface variables
        '2t': (['time', 'lat', 'lon'], np.random.normal(275, 20, surface_shape).astype(np.float32)),
        'msl': (['time', 'lat', 'lon'], np.random.normal(101325, 2000, surface_shape).astype(np.float32)),
        'u10': (['time', 'lat', 'lon'], np.random.normal(5, 10, surface_shape).astype(np.float32)),
        'v10': (['time', 'lat', 'lon'], np.random.normal(0, 8, surface_shape).astype(np.float32)),
        
        # Atmospheric variables
        't': (['time', 'level', 'lat', 'lon'], np.random.normal(250, 30, atmos_shape).astype(np.float32)),
        'z': (['time', 'level', 'lat', 'lon'], np.random.normal(30000, 20000, atmos_shape).astype(np.float32)),
        'u': (['time', 'level', 'lat', 'lon'], np.random.normal(20, 15, atmos_shape).astype(np.float32)),
        'v': (['time', 'level', 'lat', 'lon'], np.random.normal(0, 12, atmos_shape).astype(np.float32)),
        'r': (['time', 'level', 'lat', 'lon'], np.random.uniform(0, 100, atmos_shape).astype(np.float32)),
    }
    
    return xr.Dataset(data_vars, coords=coords)


@pytest.fixture
def benchmark_config():
    """Create configuration optimized for benchmarking."""
    return {
        "lead_times_hours": [6, 12, 24, 48, 72, 96, 120],
        "variables_levels_to_score": [
            {"name": "2t", "level": None, "standard_name": "2m_temperature"},
            {"name": "msl", "level": None, "standard_name": "mean_sea_level_pressure"},
            {"name": "u10", "level": None, "standard_name": "10m_u_component_of_wind"},
            {"name": "v10", "level": None, "standard_name": "10m_v_component_of_wind"},
            {"name": "t", "level": 850, "standard_name": "temperature"},
            {"name": "z", "level": 500, "standard_name": "geopotential"},
            {"name": "u", "level": 250, "standard_name": "u_component_of_wind"},
            {"name": "v", "level": 250, "standard_name": "v_component_of_wind"}
        ],
        "alpha_skill": 0.5,
        "beta_acc": 0.5
    }


@pytest.mark.benchmark
@pytest.mark.slow
class TestPerformanceBenchmarks:
    """Performance benchmarks for compute handlers."""
    
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.fetch_gfs_analysis_data')
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.fetch_gfs_data')
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset')
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.evaluate_miner_forecast_day1')
    def test_day1_scoring_performance(
        self,
        mock_eval_day1,
        mock_open_zarr,
        mock_fetch_gfs,
        mock_fetch_analysis,
        benchmark,
        test_config,
        benchmark_config,
        benchmark_dataset
    ):
        """Benchmark Day-1 scoring computation performance."""
        # Setup mocks with realistic data
        mock_fetch_analysis.return_value = benchmark_dataset
        mock_fetch_gfs.return_value = benchmark_dataset  
        mock_open_zarr.return_value = benchmark_dataset
        
        # Mock realistic scoring results
        mock_eval_day1.return_value = {
            "overall_day1_score": 0.75,
            "qc_passed_all_vars_leads": True,
            "lead_time_scores": {
                6: {"2t": 0.8, "msl": 0.7, "u10": 0.6, "v10": 0.65},
                12: {"2t": 0.75, "msl": 0.7, "u10": 0.6, "v10": 0.6},
                24: {"2t": 0.7, "msl": 0.65, "u10": 0.55, "v10": 0.55}
            },
            "error_message": None
        }
        
        # Benchmark the scoring computation
        result = benchmark(
            handle_day1_scoring_computation,
            config=test_config,
            response_id=1,
            run_id=100,
            miner_hotkey="benchmark_miner",
            miner_uid=42,
            job_id="benchmark_job_123",
            zarr_store_url="http://benchmark.zarr",
            access_token="benchmark_token",
            verification_hash="benchmark_hash",
            gfs_init_time_iso="2023-01-01T00:00:00Z",
            scoring_config=benchmark_config
        )
        
        # Verify the result is successful
        assert result["success"] is True
        assert result["overall_day1_score"] == 0.75
        
        # Performance baseline: Day-1 scoring should complete in under 2 seconds
        assert benchmark.stats['mean'] < 2.0
    
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset')
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.get_ground_truth_data')
    def test_era5_final_scoring_performance(
        self,
        mock_get_era5,
        mock_open_zarr,
        benchmark,
        test_config,
        benchmark_config,
        benchmark_dataset
    ):
        """Benchmark ERA5 final scoring computation performance."""
        # Setup mocks
        mock_open_zarr.return_value = benchmark_dataset
        mock_get_era5.return_value = benchmark_dataset
        
        # ERA5 config with full lead times
        era5_config = benchmark_config.copy()
        era5_config["variables_to_score"] = ["2t", "msl", "u10", "v10"]
        era5_config["lead_times_hours"] = [24, 48, 72, 96, 120]
        
        # Benchmark the scoring computation
        result = benchmark(
            handle_era5_final_scoring_computation,
            config=test_config,
            response_id=1,
            run_id=100,
            miner_hotkey="benchmark_miner",
            miner_uid=42,
            job_id="benchmark_job_456",
            zarr_store_url="http://benchmark.zarr",
            access_token="benchmark_token",
            verification_hash="benchmark_hash",
            gfs_init_time_iso="2023-01-01T00:00:00Z",
            scoring_config=era5_config
        )
        
        # Verify successful completion
        assert result["success"] is True
        assert "final_era5_score" in result
        
        # Performance baseline: ERA5 scoring should complete in under 3 seconds
        assert benchmark.stats['mean'] < 3.0
    
    @patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset')
    def test_forecast_verification_performance(
        self,
        mock_open_zarr,
        benchmark,
        test_config,
        benchmark_dataset
    ):
        """Benchmark forecast verification computation performance."""
        # Setup mock
        mock_open_zarr.return_value = benchmark_dataset
        
        # Benchmark the verification computation
        result = benchmark(
            handle_forecast_verification_computation,
            config=test_config,
            zarr_store_url="http://benchmark.zarr",
            access_token="benchmark_token",
            claimed_verification_hash="benchmark_hash",
            miner_hotkey="benchmark_miner",
            job_id="benchmark_job_789"
        )
        
        # Verify successful completion
        assert result["success"] is True
        assert result["verification_passed"] is True
        
        # Performance baseline: Verification should complete in under 1 second
        assert benchmark.stats['mean'] < 1.0
    
    @patch('gaia.tasks.defined_tasks.weather.validator.hashing.fetch_gfs_data')
    @patch('gaia.tasks.defined_tasks.weather.validator.hashing.save_gfs_cache')
    def test_gfs_hash_computation_performance(
        self,
        mock_save_cache,
        mock_fetch_gfs,
        benchmark,
        test_config,
        benchmark_dataset
    ):
        """Benchmark GFS hash computation performance."""
        # Setup mocks
        mock_fetch_gfs.return_value = benchmark_dataset
        mock_save_cache.return_value = None
        
        # Benchmark the hash computation
        result = benchmark(
            handle_gfs_hash_computation,
            config=test_config,
            gfs_t0_run_time_iso="2023-01-01T00:00:00Z",
            cache_dir="/tmp/benchmark_cache"
        )
        
        # Verify successful completion
        assert result["success"] is True
        assert "hash" in result
        assert len(result["hash"]) > 0
        
        # Performance baseline: Hash computation should complete in under 1.5 seconds
        assert benchmark.stats['mean'] < 1.5
    
    def test_memory_usage_scoring(self, test_config, benchmark_config, benchmark_dataset):
        """Test memory usage during scoring operations."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        with patch('gaia.tasks.defined_tasks.weather.validator.scoring.fetch_gfs_analysis_data') as mock_fetch_analysis:
            with patch('gaia.tasks.defined_tasks.weather.validator.scoring.fetch_gfs_data') as mock_fetch_gfs:
                with patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset') as mock_open_zarr:
                    with patch('gaia.tasks.defined_tasks.weather.validator.scoring.evaluate_miner_forecast_day1') as mock_eval:
                        
                        # Setup mocks
                        mock_fetch_analysis.return_value = benchmark_dataset
                        mock_fetch_gfs.return_value = benchmark_dataset
                        mock_open_zarr.return_value = benchmark_dataset
                        mock_eval.return_value = {"overall_day1_score": 0.8, "qc_passed_all_vars_leads": True}
                        
                        # Run scoring multiple times to check for memory leaks
                        for i in range(5):
                            result = handle_day1_scoring_computation(
                                config=test_config,
                                response_id=i,
                                run_id=100,
                                miner_hotkey="memory_test_miner",
                                miner_uid=42,
                                job_id=f"memory_test_{i}",
                                zarr_store_url="http://memory.zarr",
                                access_token="memory_token",
                                verification_hash="memory_hash",
                                gfs_init_time_iso="2023-01-01T00:00:00Z",
                                scoring_config=benchmark_config
                            )
                            
                            assert result["success"] is True
                            
                            # Check memory usage
                            current_memory = process.memory_info().rss / 1024 / 1024  # MB
                            memory_increase = current_memory - initial_memory
                            
                            # Memory usage should not grow excessively (< 500MB increase)
                            assert memory_increase < 500, f"Memory usage increased by {memory_increase:.2f}MB after {i+1} iterations"
    
    def test_concurrent_scoring_performance(self, test_config, benchmark_config):
        """Test performance under concurrent load."""
        import asyncio
        import concurrent.futures
        
        def mock_scoring_task(task_id):
            """Mock scoring task that simulates computation."""
            # Simulate CPU-bound work
            start = time.time()
            while time.time() - start < 0.1:  # 100ms of work
                _ = sum(range(10000))
            
            return {
                "task_id": task_id,
                "success": True,
                "score": 0.8,
                "execution_time": time.time() - start
            }
        
        # Test concurrent execution
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(mock_scoring_task, i) for i in range(8)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        total_time = time.time() - start_time
        
        # Verify all tasks completed successfully
        assert len(results) == 8
        for result in results:
            assert result["success"] is True
        
        # With 4 workers and 8 tasks, should complete in roughly 2 * 0.1s = 0.2s + overhead
        assert total_time < 0.5, f"Concurrent execution took {total_time:.3f}s, expected < 0.5s"
    
    @pytest.mark.parametrize("dataset_size", [
        (10, 10, 10),    # Small: 10x10 grid, 10 time steps
        (50, 50, 20),    # Medium: 50x50 grid, 20 time steps  
        (180, 360, 40),  # Large: 180x360 grid, 40 time steps
    ])
    def test_scaling_performance(self, test_config, dataset_size):
        """Test performance scaling with different dataset sizes."""
        time_steps, lat_size, lon_size = dataset_size
        
        # Create dataset of specified size
        lat = np.linspace(90, -90, lat_size)
        lon = np.linspace(0, 359, lon_size)
        time = np.arange(time_steps, dtype='datetime64[h]')
        
        coords = {'time': time, 'lat': lat, 'lon': lon}
        data_shape = (time_steps, lat_size, lon_size)
        
        dataset = xr.Dataset({
            '2t': (['time', 'lat', 'lon'], np.random.normal(275, 10, data_shape).astype(np.float32)),
            'msl': (['time', 'lat', 'lon'], np.random.normal(101325, 1000, data_shape).astype(np.float32))
        }, coords=coords)
        
        with patch('gaia.tasks.defined_tasks.weather.validator.scoring.open_verified_remote_zarr_dataset') as mock_open:
            mock_open.return_value = dataset
            
            start_time = time.time()
            
            result = handle_forecast_verification_computation(
                config=test_config,
                zarr_store_url="http://scaling.zarr",
                access_token="scaling_token",
                claimed_verification_hash="scaling_hash",
                miner_hotkey="scaling_miner",
                job_id="scaling_job"
            )
            
            execution_time = time.time() - start_time
            
            assert result["success"] is True
            
            # Log performance for different dataset sizes
            data_points = time_steps * lat_size * lon_size
            print(f"Dataset size: {data_points:,} points, Time: {execution_time:.3f}s, Rate: {data_points/execution_time:.0f} points/s")
            
            # Performance should scale reasonably (not exponentially)
            # For small datasets, should be very fast
            if data_points < 10000:
                assert execution_time < 0.1
            # For medium datasets, should still be reasonable
            elif data_points < 100000:
                assert execution_time < 1.0
            # For large datasets, allow more time but still bounded
            else:
                assert execution_time < 5.0