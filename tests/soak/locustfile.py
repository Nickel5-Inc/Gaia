"""
Locust-based soak testing for the Gaia Validator v4.0.

This file defines load testing scenarios to simulate realistic 
workloads for the 72-hour soak test.
"""

from locust import User, task, between, events
import random
import time
import multiprocessing as mp
import queue
import asyncio
from datetime import datetime, timezone, timedelta

from gaia.validator.utils.ipc_types import WorkUnit, ResultUnit
from gaia.validator.app.io_engine import IOEngine
from gaia.validator.utils.config import Settings


class ValidatorWorkloadUser(User):
    """
    Simulates realistic validator workload patterns.
    
    This user class generates various types of work units that mirror
    the actual validation workflow.
    """
    
    wait_time = between(30, 120)  # Wait 30-120 seconds between tasks (realistic cadence)
    
    def on_start(self):
        """Initialize the user with test configuration."""
        self.config = Settings(
            NUM_COMPUTE_WORKERS=4,  # Reduced for soak testing
            MAX_JOBS_PER_WORKER=20,
            SUPERVISOR_CHECK_INTERVAL_S=10,
            IPC_QUEUE_SIZE=200
        )
        
        # Create IPC queues for testing
        self.work_queue = mp.Queue(maxsize=200)
        self.result_queue = mp.Queue(maxsize=200)
        
        # Track jobs for timing
        self.pending_jobs = {}
        
        # Counter for unique job IDs
        self.job_counter = 0
    
    def _generate_job_id(self):
        """Generate a unique job ID."""
        self.job_counter += 1
        return f"soak_test_{self.user_id}_{self.job_counter}_{int(time.time())}"
    
    @task(weight=30)
    def weather_hash_computation(self):
        """Simulate GFS hash computation workload."""
        job_id = self._generate_job_id()
        
        work_unit = WorkUnit(
            job_id=job_id,
            task_name="weather.hash.gfs_compute",
            payload={
                "gfs_t0_run_time_iso": datetime.now(timezone.utc).isoformat(),
                "cache_dir": "/tmp/soak_test_cache"
            },
            timeout_seconds=120
        )
        
        start_time = time.time()
        self.pending_jobs[job_id] = start_time
        
        try:
            # Put work on queue (non-blocking)
            self.work_queue.put_nowait(work_unit)
            
            # Wait for result with timeout
            result = self._wait_for_result(job_id, timeout=60)
            
            if result and result.success:
                events.request.fire(
                    request_type="weather_hash",
                    name="gfs_compute",
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=len(str(result.result)),
                    exception=None,
                    context=self.context()
                )
            else:
                events.request.fire(
                    request_type="weather_hash",
                    name="gfs_compute",
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=0,
                    exception=Exception("Hash computation failed"),
                    context=self.context()
                )
                
        except queue.Full:
            events.request.fire(
                request_type="weather_hash",
                name="gfs_compute",
                response_time=int((time.time() - start_time) * 1000),
                response_length=0,
                exception=Exception("Work queue full"),
                context=self.context()
            )
        except Exception as e:
            events.request.fire(
                request_type="weather_hash",
                name="gfs_compute",
                response_time=int((time.time() - start_time) * 1000),
                response_length=0,
                exception=e,
                context=self.context()
            )
    
    @task(weight=20)
    def day1_scoring_computation(self):
        """Simulate Day-1 scoring workload."""
        job_id = self._generate_job_id()
        
        work_unit = WorkUnit(
            job_id=job_id,
            task_name="weather.scoring.day1_compute",
            payload={
                "response_id": random.randint(1, 1000),
                "run_id": random.randint(100, 999),
                "miner_hotkey": f"soak_test_miner_{random.randint(1, 100)}",
                "miner_uid": random.randint(1, 256),
                "zarr_store_url": "http://soak-test.zarr",
                "access_token": "soak_test_token",
                "verification_hash": f"hash_{random.randint(1000, 9999)}",
                "gfs_init_time_iso": datetime.now(timezone.utc).isoformat()
            },
            timeout_seconds=180
        )
        
        start_time = time.time()
        self.pending_jobs[job_id] = start_time
        
        try:
            self.work_queue.put_nowait(work_unit)
            result = self._wait_for_result(job_id, timeout=120)
            
            if result and result.success:
                events.request.fire(
                    request_type="weather_scoring",
                    name="day1_compute",
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=len(str(result.result)),
                    exception=None,
                    context=self.context()
                )
            else:
                events.request.fire(
                    request_type="weather_scoring",
                    name="day1_compute",
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=0,
                    exception=Exception("Day-1 scoring failed"),
                    context=self.context()
                )
                
        except Exception as e:
            events.request.fire(
                request_type="weather_scoring",
                name="day1_compute",
                response_time=int((time.time() - start_time) * 1000),
                response_length=0,
                exception=e,
                context=self.context()
            )
    
    @task(weight=15)
    def era5_final_scoring_computation(self):
        """Simulate ERA5 final scoring workload."""
        job_id = self._generate_job_id()
        
        work_unit = WorkUnit(
            job_id=job_id,
            task_name="weather.scoring.era5_final_compute",
            payload={
                "response_id": random.randint(1, 1000),
                "run_id": random.randint(100, 999),
                "miner_hotkey": f"soak_test_miner_{random.randint(1, 100)}",
                "miner_uid": random.randint(1, 256),
                "zarr_store_url": "http://soak-test.zarr",
                "access_token": "soak_test_token",
                "verification_hash": f"hash_{random.randint(1000, 9999)}",
                "gfs_init_time_iso": (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
            },
            timeout_seconds=300
        )
        
        start_time = time.time()
        self.pending_jobs[job_id] = start_time
        
        try:
            self.work_queue.put_nowait(work_unit)
            result = self._wait_for_result(job_id, timeout=240)
            
            if result and result.success:
                events.request.fire(
                    request_type="weather_scoring",
                    name="era5_final_compute",
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=len(str(result.result)),
                    exception=None,
                    context=self.context()
                )
            else:
                events.request.fire(
                    request_type="weather_scoring",
                    name="era5_final_compute",
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=0,
                    exception=Exception("ERA5 scoring failed"),
                    context=self.context()
                )
                
        except Exception as e:
            events.request.fire(
                request_type="weather_scoring",
                name="era5_final_compute",
                response_time=int((time.time() - start_time) * 1000),
                response_length=0,
                exception=e,
                context=self.context()
            )
    
    @task(weight=25)
    def forecast_verification(self):
        """Simulate forecast verification workload."""
        job_id = self._generate_job_id()
        
        work_unit = WorkUnit(
            job_id=job_id,
            task_name="weather.hash.forecast_verify",
            payload={
                "zarr_store_url": "http://soak-test.zarr",
                "access_token": "soak_test_token",
                "claimed_verification_hash": f"hash_{random.randint(1000, 9999)}",
                "miner_hotkey": f"soak_test_miner_{random.randint(1, 100)}"
            },
            timeout_seconds=90
        )
        
        start_time = time.time()
        self.pending_jobs[job_id] = start_time
        
        try:
            self.work_queue.put_nowait(work_unit)
            result = self._wait_for_result(job_id, timeout=60)
            
            if result and result.success:
                events.request.fire(
                    request_type="weather_verification",
                    name="forecast_verify",
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=len(str(result.result)),
                    exception=None,
                    context=self.context()
                )
            else:
                events.request.fire(
                    request_type="weather_verification",
                    name="forecast_verify",
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=0,
                    exception=Exception("Verification failed"),
                    context=self.context()
                )
                
        except Exception as e:
            events.request.fire(
                request_type="weather_verification",
                name="forecast_verify",
                response_time=int((time.time() - start_time) * 1000),
                response_length=0,
                exception=e,
                context=self.context()
            )
    
    @task(weight=10)
    def queue_health_check(self):
        """Monitor queue health and depth."""
        start_time = time.time()
        
        try:
            # Check work queue depth
            work_queue_depth = self.work_queue.qsize()
            result_queue_depth = self.result_queue.qsize()
            
            # Log queue metrics
            events.request.fire(
                request_type="queue_health",
                name="work_queue_depth",
                response_time=int((time.time() - start_time) * 1000),
                response_length=work_queue_depth,
                exception=None if work_queue_depth < 50 else Exception(f"Work queue depth high: {work_queue_depth}"),
                context=self.context()
            )
            
            events.request.fire(
                request_type="queue_health",
                name="result_queue_depth",
                response_time=int((time.time() - start_time) * 1000),
                response_length=result_queue_depth,
                exception=None if result_queue_depth < 50 else Exception(f"Result queue depth high: {result_queue_depth}"),
                context=self.context()
            )
            
        except Exception as e:
            events.request.fire(
                request_type="queue_health",
                name="health_check",
                response_time=int((time.time() - start_time) * 1000),
                response_length=0,
                exception=e,
                context=self.context()
            )
    
    def _wait_for_result(self, job_id, timeout=60):
        """Wait for a specific job result with timeout."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Check for result (non-blocking)
                result = self.result_queue.get_nowait()
                
                if result.job_id == job_id:
                    # Found our result
                    if job_id in self.pending_jobs:
                        del self.pending_jobs[job_id]
                    return result
                else:
                    # Put back result for another job
                    self.result_queue.put_nowait(result)
                    
            except queue.Empty:
                # No results available, wait a bit
                time.sleep(0.1)
                continue
        
        # Timeout reached
        if job_id in self.pending_jobs:
            del self.pending_jobs[job_id]
        return None
    
    def context(self):
        """Return context information for this user."""
        return {
            "user_id": getattr(self, 'user_id', 'unknown'),
            "pending_jobs": len(self.pending_jobs),
            "work_queue_depth": self.work_queue.qsize(),
            "result_queue_depth": self.result_queue.qsize()
        }


class BurstWorkloadUser(User):
    """
    Simulates burst workload patterns that can stress the system.
    
    This user creates sudden spikes in demand to test system resilience.
    """
    
    wait_time = between(5, 15)  # Shorter wait times for burst patterns
    
    def on_start(self):
        """Initialize burst user."""
        self.config = Settings()
        self.work_queue = mp.Queue(maxsize=500)
        self.result_queue = mp.Queue(maxsize=500)
        self.job_counter = 0
    
    @task
    def burst_hash_computation(self):
        """Create burst of hash computation jobs."""
        start_time = time.time()
        jobs_submitted = 0
        
        # Submit 5-10 jobs rapidly
        burst_size = random.randint(5, 10)
        
        for i in range(burst_size):
            job_id = f"burst_{self.user_id}_{self.job_counter}_{i}"
            self.job_counter += 1
            
            work_unit = WorkUnit(
                job_id=job_id,
                task_name="weather.hash.gfs_compute",
                payload={
                    "gfs_t0_run_time_iso": datetime.now(timezone.utc).isoformat(),
                    "cache_dir": f"/tmp/burst_cache_{i}"
                },
                timeout_seconds=60
            )
            
            try:
                self.work_queue.put_nowait(work_unit)
                jobs_submitted += 1
            except queue.Full:
                break
        
        # Record burst submission performance
        events.request.fire(
            request_type="burst_workload",
            name="hash_burst",
            response_time=int((time.time() - start_time) * 1000),
            response_length=jobs_submitted,
            exception=None if jobs_submitted > 0 else Exception("Failed to submit burst jobs"),
            context={"burst_size": burst_size, "jobs_submitted": jobs_submitted}
        )


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when the test starts."""
    print("=== Gaia Validator Soak Test Starting ===")
    print(f"Target URL: {environment.host}")
    print(f"Users: {environment.runner.target_user_count}")
    print(f"Test duration: {environment.runner.run_time if hasattr(environment.runner, 'run_time') else 'indefinite'}")
    print("==========================================")


@events.test_stop.add_listener  
def on_test_stop(environment, **kwargs):
    """Called when the test stops."""
    print("=== Gaia Validator Soak Test Completed ===")
    
    # Print summary statistics
    stats = environment.runner.stats
    print(f"Total requests: {stats.total.num_requests}")
    print(f"Total failures: {stats.total.num_failures}")
    print(f"Average response time: {stats.total.avg_response_time:.2f}ms")
    print(f"Max response time: {stats.total.max_response_time}ms")
    print(f"RPS: {stats.total.current_rps:.2f}")
    
    # Check soak test success criteria
    failure_rate = stats.total.num_failures / max(stats.total.num_requests, 1)
    avg_response_time = stats.total.avg_response_time
    
    print("\n=== Soak Test Success Criteria ===")
    print(f"Failure rate: {failure_rate*100:.2f}% (target: <5%)")
    print(f"Average response time: {avg_response_time:.2f}ms (target: <2000ms)")
    
    if failure_rate < 0.05:
        print("✓ Failure rate criterion PASSED")
    else:
        print("✗ Failure rate criterion FAILED")
    
    if avg_response_time < 2000:
        print("✓ Response time criterion PASSED")
    else:
        print("✗ Response time criterion FAILED")
    
    print("==========================================")


# Configuration for different test scenarios
class SoakTestConfig:
    """Configuration for soak testing scenarios."""
    
    # 72-hour soak test configuration
    SOAK_72H = {
        "users": 20,           # Concurrent users
        "spawn_rate": 2,       # Users spawned per second
        "run_time": "72h",     # Test duration
        "host": "http://localhost:8000"
    }
    
    # Burst test configuration
    BURST_TEST = {
        "users": 50,           # Higher user count for burst
        "spawn_rate": 10,      # Rapid spawn rate
        "run_time": "30m",     # Shorter duration
        "host": "http://localhost:8000"
    }
    
    # Stress test configuration
    STRESS_TEST = {
        "users": 100,          # High user count
        "spawn_rate": 5,       # Moderate spawn rate
        "run_time": "2h",      # Medium duration
        "host": "http://localhost:8000"
    }