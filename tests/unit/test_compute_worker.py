"""
Unit tests for the compute worker module.
"""

import pytest
import time
import multiprocessing as mp
import queue
from unittest.mock import Mock, patch, MagicMock

from gaia.validator.app.compute_worker import _lazy_load_handler, HANDLER_REGISTRY, main
from gaia.validator.utils.ipc_types import WorkUnit, ResultUnit


@pytest.mark.unit
class TestComputeWorker:
    """Test the compute worker functionality."""
    
    def test_lazy_load_handler_weather_hash(self):
        """Test lazy loading of weather hashing handlers."""
        # Test GFS hash handler
        with patch('gaia.tasks.defined_tasks.weather.validator.hashing.handle_gfs_hash_computation') as mock_handler:
            handler = _lazy_load_handler("weather.hash.gfs_compute")
            assert handler == mock_handler
            assert HANDLER_REGISTRY["weather.hash.gfs_compute"] == mock_handler
    
    def test_lazy_load_handler_weather_scoring(self):
        """Test lazy loading of weather scoring handlers."""
        # Test Day-1 scoring handler
        with patch('gaia.tasks.defined_tasks.weather.validator.scoring.handle_day1_scoring_computation') as mock_handler:
            handler = _lazy_load_handler("weather.scoring.day1_compute")
            assert handler == mock_handler
            assert HANDLER_REGISTRY["weather.scoring.day1_compute"] == mock_handler
    
    def test_lazy_load_handler_unknown_task(self):
        """Test that unknown task names return None."""
        handler = _lazy_load_handler("unknown.task.name")
        assert handler is None
    
    def test_handler_registry_initialization(self):
        """Test that handler registry is properly initialized."""
        expected_handlers = [
            "weather.hash.gfs_compute",
            "weather.hash.verification_compute",
            "weather.hash.forecast_verify",
            "weather.scoring.day1_compute",
            "weather.scoring.era5_final_compute",
            "weather.scoring.forecast_verification"
        ]
        
        for handler_name in expected_handlers:
            assert handler_name in HANDLER_REGISTRY
            # Initially should be None (lazy-loaded)
            assert HANDLER_REGISTRY[handler_name] is None
    
    @patch('gaia.validator.app.compute_worker._lazy_load_handler')
    def test_worker_main_successful_job(self, mock_lazy_load):
        """Test compute worker main loop with successful job execution."""
        # Setup mock configuration
        mock_config = Mock()
        mock_config.MAX_JOBS_PER_WORKER = 1
        mock_config.WORKER_QUEUE_TIMEOUT_S = 5
        mock_config.PROCESS_MAX_RSS_MB = {"compute": 1024}
        
        # Setup mock queues
        work_q = Mock()
        result_q = Mock()
        
        # Create a test work unit
        test_work_unit = WorkUnit(
            job_id="test_job_123",
            task_name="weather.hash.gfs_compute",
            payload={"test_param": "test_value"},
            timeout_seconds=60
        )
        
        # Mock the work queue to return our test work unit
        work_q.get.return_value = test_work_unit
        
        # Mock handler that returns a successful result
        mock_handler = Mock(return_value={"hash": "abc123"})
        mock_lazy_load.return_value = mock_handler
        
        # Mock resource limit setting
        with patch('resource.setrlimit'):
            # Run the worker main function
            main(mock_config, work_q, result_q, worker_id=1)
        
        # Verify work was retrieved from queue
        work_q.get.assert_called_once_with(timeout=5)
        
        # Verify handler was loaded and called
        mock_lazy_load.assert_called_once_with("weather.hash.gfs_compute")
        mock_handler.assert_called_once_with(mock_config, test_param="test_value")
        
        # Verify result was put back on result queue
        result_q.put.assert_called_once()
        result_call_args = result_q.put.call_args[0][0]
        
        assert isinstance(result_call_args, ResultUnit)
        assert result_call_args.job_id == "test_job_123"
        assert result_call_args.success is True
        assert result_call_args.result == {"hash": "abc123"}
    
    @patch('gaia.validator.app.compute_worker._lazy_load_handler')
    def test_worker_main_handler_error(self, mock_lazy_load):
        """Test compute worker handling of handler execution errors."""
        mock_config = Mock()
        mock_config.MAX_JOBS_PER_WORKER = 1
        mock_config.WORKER_QUEUE_TIMEOUT_S = 5
        mock_config.PROCESS_MAX_RSS_MB = {"compute": 1024}
        
        work_q = Mock()
        result_q = Mock()
        
        test_work_unit = WorkUnit(
            job_id="test_job_456",
            task_name="weather.scoring.day1_compute",
            payload={"test_param": "test_value"},
            timeout_seconds=60
        )
        
        work_q.get.return_value = test_work_unit
        
        # Mock handler that raises an exception
        mock_handler = Mock(side_effect=ValueError("Test error"))
        mock_lazy_load.return_value = mock_handler
        
        with patch('resource.setrlimit'):
            main(mock_config, work_q, result_q, worker_id=2)
        
        # Verify error result was put on queue
        result_q.put.assert_called_once()
        result_call_args = result_q.put.call_args[0][0]
        
        assert isinstance(result_call_args, ResultUnit)
        assert result_call_args.job_id == "test_job_456"
        assert result_call_args.success is False
        assert "ValueError: Test error" in result_call_args.error
    
    @patch('gaia.validator.app.compute_worker._lazy_load_handler')
    def test_worker_main_unknown_handler(self, mock_lazy_load):
        """Test compute worker handling of unknown task handlers."""
        mock_config = Mock()
        mock_config.MAX_JOBS_PER_WORKER = 1
        mock_config.WORKER_QUEUE_TIMEOUT_S = 5
        mock_config.PROCESS_MAX_RSS_MB = {"compute": 1024}
        
        work_q = Mock()
        result_q = Mock()
        
        test_work_unit = WorkUnit(
            job_id="test_job_789",
            task_name="unknown.task.name",
            payload={},
            timeout_seconds=60
        )
        
        work_q.get.return_value = test_work_unit
        
        # Mock lazy loader returns None for unknown task
        mock_lazy_load.return_value = None
        
        with patch('resource.setrlimit'):
            main(mock_config, work_q, result_q, worker_id=3)
        
        # Verify error result was put on queue
        result_q.put.assert_called_once()
        result_call_args = result_q.put.call_args[0][0]
        
        assert isinstance(result_call_args, ResultUnit)
        assert result_call_args.success is False
        assert "No handler found for task: unknown.task.name" in result_call_args.error
    
    def test_worker_main_queue_timeout(self):
        """Test compute worker handling of queue timeout."""
        mock_config = Mock()
        mock_config.MAX_JOBS_PER_WORKER = 5
        mock_config.WORKER_QUEUE_TIMEOUT_S = 1
        mock_config.PROCESS_MAX_RSS_MB = {"compute": 1024}
        
        work_q = Mock()
        result_q = Mock()
        
        # Mock queue timeout
        work_q.get.side_effect = queue.Empty()
        
        with patch('resource.setrlimit'):
            with patch('builtins.print') as mock_print:
                main(mock_config, work_q, result_q, worker_id=4)
        
        # Verify worker printed timeout message and exited
        mock_print.assert_any_call("ComputeWorker-4: Queue idle, exiting after 5 jobs.")
    
    def test_worker_memory_limit_setting(self):
        """Test that worker sets memory limits correctly."""
        mock_config = Mock()
        mock_config.MAX_JOBS_PER_WORKER = 1
        mock_config.WORKER_QUEUE_TIMEOUT_S = 1
        mock_config.PROCESS_MAX_RSS_MB = {"compute": 512}
        
        work_q = Mock()
        result_q = Mock()
        work_q.get.side_effect = queue.Empty()
        
        with patch('resource.setrlimit') as mock_setrlimit:
            main(mock_config, work_q, result_q, worker_id=5)
        
        # Verify memory limit was set (512 MB = 536870912 bytes)
        expected_limit = 512 * 1024 * 1024
        mock_setrlimit.assert_called_once_with(
            mock_setrlimit.call_args[0][0],  # resource.RLIMIT_AS
            (expected_limit, expected_limit)
        )
    
    def test_result_unit_timing(self):
        """Test that execution timing is properly recorded."""
        mock_config = Mock()
        mock_config.MAX_JOBS_PER_WORKER = 1
        mock_config.WORKER_QUEUE_TIMEOUT_S = 5
        mock_config.PROCESS_MAX_RSS_MB = {"compute": 1024}
        
        work_q = Mock()
        result_q = Mock()
        
        test_work_unit = WorkUnit(
            job_id="timing_test",
            task_name="weather.hash.gfs_compute",
            payload={},
            timeout_seconds=60
        )
        
        work_q.get.return_value = test_work_unit
        
        # Mock handler that sleeps to test timing
        def slow_handler(*args, **kwargs):
            time.sleep(0.1)  # Sleep 100ms
            return {"result": "success"}
        
        with patch('gaia.validator.app.compute_worker._lazy_load_handler', return_value=slow_handler):
            with patch('resource.setrlimit'):
                main(mock_config, work_q, result_q, worker_id=6)
        
        result_call_args = result_q.put.call_args[0][0]
        assert result_call_args.execution_time_ms >= 100  # Should be at least 100ms
        assert result_call_args.execution_time_ms < 1000  # Should be less than 1 second
    
    def test_worker_process_id_recording(self):
        """Test that worker process ID is recorded in results."""
        mock_config = Mock()
        mock_config.MAX_JOBS_PER_WORKER = 1
        mock_config.WORKER_QUEUE_TIMEOUT_S = 5
        mock_config.PROCESS_MAX_RSS_MB = {"compute": 1024}
        
        work_q = Mock()
        result_q = Mock()
        
        test_work_unit = WorkUnit(
            job_id="pid_test",
            task_name="weather.hash.gfs_compute",
            payload={},
            timeout_seconds=60
        )
        
        work_q.get.return_value = test_work_unit
        
        mock_handler = Mock(return_value={"result": "success"})
        
        with patch('gaia.validator.app.compute_worker._lazy_load_handler', return_value=mock_handler):
            with patch('resource.setrlimit'):
                with patch('os.getpid', return_value=12345):
                    main(mock_config, work_q, result_q, worker_id=7)
        
        result_call_args = result_q.put.call_args[0][0]
        assert result_call_args.worker_pid == 12345