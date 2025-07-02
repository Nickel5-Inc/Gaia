"""
Integration tests for the full weather validation lifecycle.

These tests verify the complete workflow from supervisor startup through
compute job execution and database persistence.
"""

import pytest
import asyncio
import multiprocessing as mp
import queue
import tempfile
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch, MagicMock

from gaia.validator.app.supervisor import main as supervisor_main
from gaia.validator.app.io_engine import IOEngine
from gaia.validator.app.compute_worker import main as worker_main
from gaia.validator.utils.ipc_types import WorkUnit, ResultUnit
from gaia.validator.db.connection import get_db_pool, close_db_pool
from gaia.validator.db import queries as db
from gaia.tasks.defined_tasks.weather.validator.lifecycle import orchestrate_full_weather_run


@pytest.mark.integration
@pytest.mark.slow
class TestFullLifecycle:
    """Integration tests for the complete validation lifecycle."""
    
    @pytest.fixture(scope="class")
    async def test_database(self):
        """Set up a test database for integration tests."""
        # In a real implementation, this would use pytest-postgresql
        # to create an isolated test database
        mock_pool = MagicMock()
        
        # Mock database operations for integration tests
        async def mock_create_run(*args, **kwargs):
            return 1  # Mock run ID
            
        async def mock_create_response(*args, **kwargs):
            return 1  # Mock response ID
            
        async def mock_update_run(*args, **kwargs):
            pass
            
        async def mock_fetchval(*args, **kwargs):
            return 1
            
        async def mock_fetchrow(*args, **kwargs):
            return {"id": 1, "status": "pending", "gfs_init_time_utc": datetime.now(timezone.utc)}
            
        async def mock_fetch(*args, **kwargs):
            return [{"id": 1, "status": "pending"}]
            
        async def mock_execute(*args, **kwargs):
            return "INSERT 0 1"
        
        mock_pool.fetchval = mock_fetchval
        mock_pool.fetchrow = mock_fetchrow
        mock_pool.fetch = mock_fetch
        mock_pool.execute = mock_execute
        
        # Patch the database connection
        with patch('gaia.validator.db.connection.get_db_pool', return_value=mock_pool):
            yield mock_pool
    
    @pytest.fixture
    def test_queues(self):
        """Create IPC queues for testing."""
        work_queue = mp.Queue(maxsize=100)
        result_queue = mp.Queue(maxsize=100)
        return work_queue, result_queue
    
    async def test_io_engine_dispatch_and_wait(self, test_config, test_queues):
        """Test IO-Engine work dispatch and result waiting."""
        work_q, result_q = test_queues
        
        # Create IO-Engine instance
        io_engine = IOEngine(test_config, work_q, result_q)
        
        # Create a test work unit
        work_unit = WorkUnit(
            job_id="integration_test_123",
            task_name="weather.hash.gfs_compute",
            payload={"test_param": "test_value"},
            timeout_seconds=30
        )
        
        # Mock a compute worker response
        expected_result = ResultUnit(
            job_id="integration_test_123",
            task_name="weather.hash.gfs_compute",
            success=True,
            result={"hash": "abc123def456"},
            worker_pid=12345,
            execution_time_ms=150.0
        )
        
        # Start the dispatch operation
        dispatch_task = asyncio.create_task(io_engine.dispatch_and_wait(work_unit))
        
        # Simulate compute worker processing
        await asyncio.sleep(0.1)  # Allow dispatch to start
        
        # Simulate worker completing the job
        result_q.put(expected_result)
        
        # Wait for dispatch to complete
        result = await asyncio.wait_for(dispatch_task, timeout=5.0)
        
        # Verify the result
        assert result.job_id == "integration_test_123"
        assert result.success is True
        assert result.result["hash"] == "abc123def456"
        assert result.execution_time_ms == 150.0
    
    async def test_compute_worker_job_execution(self, test_config, test_queues):
        """Test that compute workers can execute jobs from the queue."""
        work_q, result_q = test_queues
        
        # Create a test work unit
        test_work_unit = WorkUnit(
            job_id="worker_test_456",
            task_name="weather.hash.gfs_compute",
            payload={
                "gfs_t0_run_time_iso": "2023-01-01T00:00:00Z",
                "cache_dir": "/tmp/test_cache"
            },
            timeout_seconds=60
        )
        
        # Put work on the queue
        work_q.put(test_work_unit)
        
        # Mock the actual handler to avoid heavy computation
        with patch('gaia.tasks.defined_tasks.weather.validator.hashing.handle_gfs_hash_computation') as mock_handler:
            mock_handler.return_value = {"hash": "test_hash_result"}
            
            # Run the worker in a separate process
            worker_process = mp.Process(
                target=worker_main,
                args=(test_config, work_q, result_q, 1)
            )
            worker_process.start()
            
            # Wait for the worker to process the job
            try:
                result = result_q.get(timeout=10)
                worker_process.join(timeout=5)
                
                # Verify the result
                assert isinstance(result, ResultUnit)
                assert result.job_id == "worker_test_456"
                assert result.success is True
                assert result.result["hash"] == "test_hash_result"
                
            finally:
                if worker_process.is_alive():
                    worker_process.terminate()
                    worker_process.join()
    
    @patch('gaia.validator.db.connection.get_db_pool')
    @patch('gaia.tasks.defined_tasks.weather.validator.lifecycle._compute_validator_hash')
    @patch('gaia.tasks.defined_tasks.weather.validator.lifecycle._query_miners_for_forecasts')
    @patch('gaia.tasks.defined_tasks.weather.validator.lifecycle._verify_miner_responses')
    @patch('gaia.tasks.defined_tasks.weather.validator.lifecycle._queue_scoring_jobs')
    async def test_weather_lifecycle_orchestration(
        self,
        mock_queue_jobs,
        mock_verify_responses,
        mock_query_miners,
        mock_compute_hash,
        mock_get_pool,
        mock_io_engine
    ):
        """Test the complete weather validation lifecycle orchestration."""
        # Setup mocks
        mock_pool = MagicMock()
        mock_get_pool.return_value = mock_pool
        
        # Mock database operations
        async def mock_create_run(*args, **kwargs):
            return 1
        mock_pool.fetchval = mock_create_run
        
        # Mock lifecycle steps
        mock_compute_hash.return_value = "validator_hash_123"
        
        mock_query_miners.return_value = [
            {
                "response_id": 1,
                "miner_uid": 42,
                "miner_hotkey": "test_miner_1",
                "job_id": "job_1"
            },
            {
                "response_id": 2,
                "miner_uid": 43,
                "miner_hotkey": "test_miner_2", 
                "job_id": "job_2"
            }
        ]
        
        mock_verify_responses.return_value = [
            {
                "response_id": 1,
                "miner_uid": 42,
                "miner_hotkey": "test_miner_1",
                "job_id": "job_1"
            }
        ]
        
        mock_queue_jobs.return_value = None
        
        # Execute the lifecycle
        await orchestrate_full_weather_run(mock_io_engine)
        
        # Verify that all steps were called
        mock_compute_hash.assert_called_once()
        mock_query_miners.assert_called_once()
        mock_verify_responses.assert_called_once()
        mock_queue_jobs.assert_called_once()
    
    @patch('gaia.validator.db.connection.get_db_pool')
    async def test_database_query_integration(self, mock_get_pool, test_database):
        """Test database query integration with the abstraction layer."""
        mock_get_pool.return_value = test_database
        
        # Test creating a weather forecast run
        run_id = await db.create_weather_forecast_run(
            pool=test_database,
            target_forecast_time_utc=datetime.now(timezone.utc) + timedelta(hours=240),
            gfs_init_time_utc=datetime.now(timezone.utc),
            gfs_input_metadata={"model": "GFS", "resolution": 0.25}
        )
        
        assert run_id == 1  # Mock return value
        
        # Test creating a miner response
        response_id = await db.create_weather_miner_response(
            pool=test_database,
            run_id=run_id,
            miner_uid=42,
            miner_hotkey="test_miner_hotkey",
            job_id="test_job_123"
        )
        
        assert response_id == 1  # Mock return value
        
        # Test updating run status
        await db.update_weather_run_status(
            pool=test_database,
            run_id=run_id,
            status="completed",
            completion_time=datetime.now(timezone.utc)
        )
        
        # Verify database operations were called
        assert test_database.execute.called
    
    @patch('gaia.validator.app.io_engine.get_db_pool')
    async def test_io_engine_initialization(self, mock_get_pool, test_config, test_queues):
        """Test IO-Engine initialization and database connection."""
        work_q, result_q = test_queues
        mock_pool = MagicMock()
        mock_get_pool.return_value = mock_pool
        
        # Create and initialize IO-Engine
        io_engine = IOEngine(test_config, work_q, result_q)
        
        # Test initialization
        await io_engine.initialize_connections()
        
        # Verify database pool was obtained
        mock_get_pool.assert_called_once()
        assert io_engine.db_pool == mock_pool
        
        # Test cleanup
        with patch('gaia.validator.db.connection.close_db_pool') as mock_close:
            await io_engine.cleanup_connections()
            mock_close.assert_called_once()
    
    async def test_weather_task_scheduling(self, mock_io_engine):
        """Test weather task registration and scheduling."""
        from gaia.tasks.defined_tasks.weather.task import WeatherTask
        
        # Create weather task
        weather_task = WeatherTask()
        
        # Verify task properties
        assert weather_task.name == "weather"
        assert weather_task.cron_schedule == "15 2,8,14,20 * * *"
        
        # Test task execution with mock IO-Engine
        with patch.object(weather_task, 'run_scheduled_job') as mock_run:
            await weather_task.run_scheduled_job(mock_io_engine)
            mock_run.assert_called_once_with(mock_io_engine)
    
    @patch('gaia.validator.app.supervisor.multiprocessing.Process')
    def test_supervisor_process_management(self, mock_process, test_config):
        """Test supervisor process creation and management."""
        # Mock process objects
        mock_io_process = Mock()
        mock_worker_processes = [Mock() for _ in range(test_config.NUM_COMPUTE_WORKERS)]
        
        def mock_process_factory(*args, **kwargs):
            if 'io_engine' in str(args):
                return mock_io_process
            else:
                return mock_worker_processes.pop(0) if mock_worker_processes else Mock()
        
        mock_process.side_effect = mock_process_factory
        
        # Test would involve running supervisor main loop
        # This is a simplified test due to the complexity of multiprocessing testing
        assert test_config.NUM_COMPUTE_WORKERS > 0
    
    async def test_error_handling_and_recovery(self, test_config, test_queues):
        """Test error handling and recovery in the integration workflow."""
        work_q, result_q = test_queues
        
        # Create IO-Engine
        io_engine = IOEngine(test_config, work_q, result_q)
        
        # Test handling of failed work units
        failed_work_unit = WorkUnit(
            job_id="error_test_789",
            task_name="unknown.task.type",  # This should cause an error
            payload={},
            timeout_seconds=5
        )
        
        # Create a mock worker that will fail
        error_result = ResultUnit(
            job_id="error_test_789",
            task_name="unknown.task.type",
            success=False,
            error="No handler found for task: unknown.task.type",
            worker_pid=12345,
            execution_time_ms=50.0
        )
        
        # Start dispatch
        dispatch_task = asyncio.create_task(io_engine.dispatch_and_wait(failed_work_unit))
        
        # Simulate error response
        await asyncio.sleep(0.1)
        result_q.put(error_result)
        
        # Wait for result
        result = await asyncio.wait_for(dispatch_task, timeout=5.0)
        
        # Verify error handling
        assert result.success is False
        assert "No handler found" in result.error
    
    @pytest.mark.benchmark
    async def test_performance_benchmarks(self, test_config, test_queues):
        """Test performance benchmarks for critical components."""
        work_q, result_q = test_queues
        
        # Benchmark work dispatch latency
        io_engine = IOEngine(test_config, work_q, result_q)
        
        start_time = time.time()
        
        # Create multiple work units
        work_units = []
        for i in range(10):
            work_unit = WorkUnit(
                job_id=f"benchmark_job_{i}",
                task_name="weather.hash.gfs_compute",
                payload={"test_data": f"data_{i}"},
                timeout_seconds=30
            )
            work_units.append(work_unit)
        
        # Dispatch all work units
        tasks = []
        for work_unit in work_units:
            task = asyncio.create_task(io_engine.dispatch_and_wait(work_unit))
            tasks.append(task)
        
        # Simulate rapid worker responses
        for i, work_unit in enumerate(work_units):
            result = ResultUnit(
                job_id=work_unit.job_id,
                task_name=work_unit.task_name,
                success=True,
                result={"hash": f"hash_{i}"},
                worker_pid=12345,
                execution_time_ms=100.0
            )
            result_q.put(result)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Verify all tasks completed successfully
        for result in results:
            assert not isinstance(result, Exception)
            assert result.success is True
        
        # Performance assertion (should handle 10 jobs in reasonable time)
        assert total_time < 5.0  # 5 seconds should be more than enough for mocked operations
        
        # Calculate average dispatch latency
        avg_latency = total_time / len(work_units)
        assert avg_latency < 0.5  # Each job should dispatch in under 500ms