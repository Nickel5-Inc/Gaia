# Phase 2: Core Infrastructure Implementation

**Objective:** Build the multi-process backbone of the application. This phase involves creating the Supervisor, IO-Engine, and Compute Worker processes, and defining the communication protocol between them. The goal is to have a running, stable, but "empty" application skeleton before migrating the business logic.

---

### Task 2.1: Implement IPC Data Types

**Action:** Create the Pydantic models that define the strict contract for messages passed between the IO-Engine and the Compute Pool.

**File:** `gaia/validator/utils/ipc_types.py`
**Content:**
```python
from typing import Dict, Any, Optional, Tuple
from pydantic import BaseModel

class SharedMemoryNumpy(BaseModel):
    """Describes a NumPy array stored in a shared memory segment."""
    name: str
    shape: Tuple[int, ...]
    dtype: str

class WorkUnit(BaseModel):
    """A request sent from the IO-Engine to the Compute Pool."""
    job_id: str
    task_name: str # e.g., "weather.score.day1", "substrate.get_block"
    payload: Dict[str, Any]
    shared_data: Optional[Dict[str, SharedMemoryNumpy]] = None
    timeout_seconds: int = 300

class ResultUnit(BaseModel):
    """A response sent from a Compute Worker back to the IO-Engine."""
    job_id: str
    task_name: str
    success: bool
    result: Optional[Any] = None # For small, serializable results
    shared_data_result: Optional[Dict[str, SharedMemoryNumpy]] = None # For large array results
    error: Optional[str] = None
    worker_pid: int
    execution_time_ms: float
```

---

### Task 2.2: Implement the Supervisor

**Action:** Repurpose `gaia/validator/validator.py` to become the Supervisor. This process is the application's main entrypoint. It is synchronous and has minimal dependencies to ensure maximum stability.

**File:** `gaia/validator/validator.py`
**Content:**
```python
import multiprocessing
import time
import psutil
import signal
import os
from gaia.validator.app.io_engine import main as io_main
from gaia.validator.app.compute_worker import main as compute_main
from gaia.validator.utils.config import settings # Assuming Phase 0 is complete

def main():
    """The main entrypoint for the Gaia Validator application."""
    config = settings

    # Use a manager for queues if more complex state needs to be shared in the future.
    # For now, simple queues are sufficient.
    work_queue = multiprocessing.Queue(maxsize=config.IPC_QUEUE_SIZE)
    result_queue = multiprocessing.Queue(maxsize=config.IPC_QUEUE_SIZE)

    children = {}

    def shutdown_gracefully(signum, frame):
        print("Supervisor: Signal received, shutting down all child processes...")
        for child in children.values():
            if child.is_alive():
                # Send SIGTERM to allow for graceful shutdown
                child.terminate()
        # Wait for processes to exit
        for child in children.values():
            child.join(timeout=5)
            if child.is_alive():
                # Force kill if still alive
                child.kill()
        print("Supervisor: Shutdown complete.")
        exit(0)

    signal.signal(signal.SIGINT, shutdown_gracefully)
    signal.signal(signal.SIGTERM, shutdown_gracefully)

    while True:
        # Spawn IO Engine
        io_proc = multiprocessing.Process(
            target=io_main,
            name="io_engine",
            args=(config, work_queue, result_queue),
            daemon=True
        )
        io_proc.start()
        children['io_engine'] = io_proc

        # Spawn Compute Pool
        for i in range(config.NUM_COMPUTE_WORKERS):
            name = f"compute_worker_{i}"
            compute_proc = multiprocessing.Process(
                target=compute_main,
                name=name,
                args=(config, work_queue, result_queue, i), # Pass worker ID
                daemon=True
            )
            compute_proc.start()
            children[name] = compute_proc

        print(f"Supervisor: All {len(children)} processes started. Monitoring...")
        monitor_loop(children, config)

        # This point is only reached if a child failed and needs restarting
        print("Supervisor: Child process failure detected, restarting all processes in 5s...")
        time.sleep(5)


def monitor_loop(children, config):
    """Monitors child processes for crashes or excessive memory usage."""
    while True:
        time.sleep(config.SUPERVISOR_CHECK_INTERVAL_S)
        for name, proc in list(children.items()):
            if not proc.is_alive():
                print(f"Supervisor: Process {name} (PID {proc.pid}) died unexpectedly with exit code {proc.exitcode}!")
                # Terminate siblings before restarting
                for p in children.values(): p.terminate()
                return # Exit inner loop to trigger a full restart

            try:
                rss_mb = psutil.Process(proc.pid).memory_info().rss / (1024 * 1024)
                limit_mb = config.PROCESS_MAX_RSS_MB.get(name.split('_')[0], 2048)
                if rss_mb > limit_mb:
                    print(f"Supervisor: Process {name} (PID {proc.pid}) exceeded memory limit ({rss_mb:.2f}MB > {limit_mb}MB)! Terminating all.")
                    for p in children.values(): p.terminate()
                    return # Exit inner loop to trigger a full restart
            except psutil.NoSuchProcess:
                continue # Process may have just been terminated; the is_alive() check will catch it next iteration

if __name__ == "__main__":
    # 'spawn' is safer and more consistent across platforms than 'fork'
    multiprocessing.set_start_method('spawn', force=True)
    main()
```

---

### Task 2.3: Implement the Compute Worker

**Action:** Create the Compute Worker main loop. This process is synchronous and simple: get work, do work, return result, repeat.

**File:** `gaia/validator/app/compute_worker.py`
**Content:**
```python
import time
import resource
import os
from gaia.validator.utils.ipc_types import ResultUnit

# This registry will be populated during the logic migration phase
HANDLER_REGISTRY = {}

def main(config, work_q, result_q, worker_id: int):
    """The main execution loop for a compute worker process."""
    pid = os.getpid()
    worker_name = f"ComputeWorker-{worker_id}"
    print(f"{worker_name} (PID: {pid}) started.")

    # Set a hard memory limit for this process to prevent runaway jobs
    limit_mb = config.PROCESS_MAX_RSS_MB['compute_worker']
    limit_bytes = limit_mb * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))

    for job_number in range(config.MAX_JOBS_PER_WORKER):
        try:
            work_unit = work_q.get(timeout=config.WORKER_QUEUE_TIMEOUT_S)
            print(f"{worker_name}: Received job {job_number+1}/{config.MAX_JOBS_PER_WORKER}: {work_unit.job_id}")

            start_time = time.monotonic()
            try:
                handler = HANDLER_REGISTRY.get(work_unit.task_name)
                if not handler:
                    raise ValueError(f"No handler found for task: {work_unit.task_name}")

                # This is where the magic happens: the handler is a pure function
                result_payload = handler(config, **work_unit.payload)

                result = ResultUnit(
                    job_id=work_unit.job_id,
                    task_name=work_unit.task_name,
                    success=True,
                    result=result_payload,
                    worker_pid=pid,
                    execution_time_ms=0
                )
            except Exception as e:
                print(f"{worker_name} ERROR processing job {work_unit.job_id}: {e}")
                result = ResultUnit(
                    job_id=work_unit.job_id,
                    task_name=work_unit.task_name,
                    success=False,
                    error=f"{type(e).__name__}: {e}",
                    worker_pid=pid,
                    execution_time_ms=0
                )

            result.execution_time_ms = (time.monotonic() - start_time) * 1000
            result_q.put(result)

        except queue.Empty:
            # It's okay to time out, just means the queue is idle.
            # The worker can exit if it's been idle for too long.
            print(f"{worker_name}: Queue idle, exiting after {config.MAX_JOBS_PER_WORKER} jobs.")
            break
        except Exception as e:
            # Catch-all for unexpected errors in the worker loop itself
            print(f"{worker_name} CRITICAL ERROR: {e}")
            # Exit to be restarted by the supervisor
            break

    print(f"{worker_name} (PID: {pid}) finished max jobs or was idle, exiting cleanly.")
```

---

### Task 2.4: Implement the IO-Engine

**Action:** Create the IO-Engine main loop. This process is asynchronous and manages all state and external communication.

**File:** `gaia/validator/app/io_engine.py`
**Content:**
```python
import asyncio
import uvloop
import queue
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from gaia.validator.utils.ipc_types import WorkUnit

class IOEngine:
    def __init__(self, config, work_q, result_q):
        self.config = config
        self.work_q = work_q
        self.result_q = result_q
        self.job_futures = {} # Maps job_id to asyncio.Future
        self.scheduler = AsyncIOScheduler()
        # self.db_pool = None # Will be initialized in run()
        # self.http_client = None # Will be initialized in run()

    async def dispatch_and_wait(self, work_unit: WorkUnit):
        """Dispatches a job to the compute pool and waits for the result."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.job_futures[work_unit.job_id] = future

        try:
            # Use run_in_executor for the blocking queue.put() call
            await loop.run_in_executor(None, self.work_q.put, work_unit)
        except Exception as e:
            future.set_exception(e)
            self.job_futures.pop(work_unit.job_id, None)

        return await asyncio.wait_for(future, timeout=work_unit.timeout_seconds)

    async def result_listener(self):
        """Runs as a background task, listening for results from compute workers."""
        loop = asyncio.get_running_loop()
        while True:
            try:
                # Use run_in_executor for the blocking queue.get() call
                result_unit = await loop.run_in_executor(None, self.result_q.get)
                if future := self.job_futures.pop(result_unit.job_id, None):
                    if result_unit.error:
                        future.set_exception(Exception(f"Compute Worker Error: {result_unit.error}"))
                    else:
                        future.set_result(result_unit)
                else:
                    # Log an orphaned result if necessary
                    print(f"IOEngine: Received orphaned result for job_id: {result_unit.job_id}")
            except queue.Empty:
                continue # This is normal
            except Exception as e:
                print(f"IOEngine: Error in result listener: {e}")

    def setup_scheduled_jobs(self):
        """This will be populated with real jobs in the migration phase."""
        # from gaia.tasks.defined_tasks.weather.task import WeatherTask
        # tasks = [WeatherTask()]
        # for task in tasks:
        #     self.scheduler.add_job(task.run_scheduled_job, "cron", ..., args=[self])
        self.scheduler.add_job(lambda: print("IOEngine: Scheduled job heartbeat."), 'interval', seconds=300)
        self.scheduler.start()

    async def run(self):
        """The main entrypoint for the IO-Engine process."""
        print("IO-Engine: Starting...")
        # from gaia.validator.db.connection import DBPoolManager
        # self.db_pool = await DBPoolManager.get_pool()
        # self.http_client = httpx.AsyncClient(...)

        self.setup_scheduled_jobs()
        asyncio.create_task(self.result_listener(), name="result_listener_task")

        # Keep the engine alive indefinitely
        while True:
            await asyncio.sleep(3600)

def main(config, work_q, result_q, worker_id=None): # worker_id is unused here
    uvloop.install()
    engine = IOEngine(config, work_q, result_q)
    asyncio.run(engine.run())
``` 