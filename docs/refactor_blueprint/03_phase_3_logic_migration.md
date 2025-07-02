# Phase 3: Logic Migration

**Objective:** Systematically move all business logic from the `validator.py` and `weather/` monoliths into the new Supervisor-IO-Compute architecture. This phase is executed one task at a time, ensuring the system remains verifiable at each step.

---

### Task 3.1: Implement Centralized Configuration

**Goal:** Remove all scattered `os.getenv` calls and replace them with a single, type-safe Pydantic Settings object.

**Action:** Create `gaia/validator/utils/config.py` with the following content. This class will automatically read from environment variables or a `.env` file.

**File:** `gaia/validator/utils/config.py`
**Content:**
```python
import os
from typing import List, Dict, Any
from pydantic_settings import BaseSettings, SettingsConfigDict

class WeatherSettings(BaseSettings):
    """Settings specific to the Weather task, prefixed with WEATHER_"""
    MAX_CONCURRENT_INFERENCES: int = 1
    INFERENCE_STEPS: int = 40
    # ... (add all other fields from _load_config here)
    DB_RUN_RETENTION_DAYS: int = 90

    model_config = SettingsConfigDict(env_prefix='WEATHER_')

class Settings(BaseSettings):
    """The main application settings object."""
    # Process Management
    NUM_COMPUTE_WORKERS: int = max(1, os.cpu_count() - 2 if os.cpu_count() else 2)
    SUPERVISOR_CHECK_INTERVAL_S: int = 15
    MAX_JOBS_PER_WORKER: int = 100
    WORKER_QUEUE_TIMEOUT_S: int = 300
    IPC_QUEUE_SIZE: int = 1000

    # Memory limits in Megabytes
    PROCESS_MAX_RSS_MB_IO_ENGINE: int = 1024
    PROCESS_MAX_RSS_MB_COMPUTE_WORKER: int = 2048

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://user:password@localhost/gaia"

    # Nested Task Settings
    WEATHER: WeatherSettings = WeatherSettings()

    # Pydantic Settings Configuration
    model_config = SettingsConfigDict(
        env_file='.env',
        env_nested_delimiter='__' # Allows WEATHER__DB_RUN_RETENTION_DAYS in .env
    )

# Create a singleton instance for easy access across the application
settings = Settings()
```

**Verification:**
- The `_load_config` method in `weather_task.py` can be deleted.
- The `validator.py` Supervisor can import and use the `settings` object directly.

---

### Task 3.2: Implement the Abstracted Database Layer

**Goal:** Centralize all SQL queries, making the application logic independent of the database schema and easier to test.

**Action:** Create the `asyncpg` connection manager and the first set of query functions.

**File:** `gaia/validator/db/connection.py`
**Content:**
```python
import asyncpg
from gaia.validator.utils.config import settings

POOL = None

async def get_db_pool() -> asyncpg.Pool:
    """Returns a singleton instance of the asyncpg connection pool."""
    global POOL
    if POOL is None:
        POOL = await asyncpg.create_pool(
            dsn=settings.DATABASE_URL,
            min_size=2,
            max_size=20 # Sized for a single IO process
        )
    return POOL

async def close_db_pool():
    if POOL:
        await POOL.close()
```

**File:** `gaia/validator/db/queries.py`
**Content:**
```python
from typing import Optional
import asyncpg
from datetime import datetime, timezone

async def update_run_status(
    pool: asyncpg.Pool,
    run_id: int,
    status: str,
    error_message: Optional[str] = None
):
    """Updates the status and optionally an error message for a given run."""
    sql = """
        UPDATE weather_forecast_runs
        SET status = $1, error_message = $2, last_updated = $3
        WHERE id = $4
    """
    await pool.execute(sql, status, error_message, datetime.now(timezone.utc), run_id)

# ... Add one function for every SQL query in the original codebase ...
```

**Verification:**
- All `db_manager` instances should be removed.
- All direct `await session.execute(...)` calls should be replaced with calls to functions in `validator/db/queries.py`.

---

### Task 3.3: Migrate a Compute-Bound Task (Hashing)

**Goal:** Move the first piece of business logic into a Compute Worker handler.

**Action:** Refactor the GFS hashing logic.

1.  **Create the Handler:**
    **File:** `gaia/tasks/defined_tasks/weather/validator/hashing.py`
    **Content:**
    ```python
    def handle_gfs_hash_computation(config, gfs_t0_run_time_iso: str, cache_dir: str) -> str:
        """
        Synchronous handler for GFS hash computation.
        Heavy imports are done locally to keep worker memory footprint small.
        """
        from datetime import datetime
        from gaia.tasks.defined_tasks.weather.utils.hashing import compute_input_data_hash_sync

        gfs_t0_run_time = datetime.fromisoformat(gfs_t0_run_time_iso)
        gfs_t_minus_6 = gfs_t0_run_time - timedelta(hours=6)

        # Assumes compute_input_data_hash can be made synchronous
        # This might require refactoring it to not be an async function
        validator_hash = compute_input_data_hash_sync(
            t0_run_time=gfs_t0_run_time,
            t_minus_6_run_time=gfs_t_minus_6,
            cache_dir=cache_dir
        )
        if not validator_hash:
            raise ValueError("Hash computation returned None")
        return validator_hash
    ```

2.  **Register the Handler:**
    **File:** `gaia/validator/app/compute_worker.py`
    **Action:** Add the new handler to the registry.
    ```python
    from gaia.tasks.defined_tasks.weather.validator import hashing as weather_hashing
    
    HANDLER_REGISTRY = {
        "weather.hash.compute": weather_hashing.handle_gfs_hash_computation,
        # ... other handlers will be added here
    }
    ```

---

### Task 3.4: Migrate Orchestration Logic

**Goal:** Rebuild the application's state machine in the IO-Engine's new task structure.

**Action:** Create the lean `WeatherTask` and its `lifecycle` orchestrator.

1.  **Create the `WeatherTask`:**
    **File:** `gaia/tasks/defined_tasks/weather/task.py`
    **Content:**
    ```python
    from gaia.tasks.defined_tasks.base import GaiaTask
    from .validator import lifecycle

    class WeatherTask(GaiaTask):
        @property
        def name(self) -> str: return "weather"

        @property
        def cron_schedule(self) -> str: return "5 18 * * *" # Every day at 18:05 UTC

        async def run_scheduled_job(self, io_engine) -> None:
            await lifecycle.orchestrate_full_weather_run(io_engine)
    ```

2.  **Implement the Lifecycle Orchestrator (Conceptual):**
    **File:** `gaia/tasks/defined_tasks/weather/validator/lifecycle.py`
    **Content:** This file contains the "brains" of the operation, breaking down the old `validator_execute` loop.
    ```python
    import asyncio
    from gaia.validator.app.io_engine import IOEngine
    from gaia.validator.db import queries as db
    from gaia.validator.utils.ipc_types import WorkUnit

    async def orchestrate_full_weather_run(io: IOEngine):
        run_id = await db.create_new_run(io.db_pool)
        print(f"Lifecycle: Started Weather Run ID: {run_id}")

        # Step 1: Query miners (Network I/O)
        miner_responses = await io.query_all_miners_for_weather()
        await db.record_miner_responses(io.db_pool, run_id, miner_responses)

        # Step 2: Dispatch hash computation (CPU-bound)
        work = WorkUnit(
            job_id=f"hash-{run_id}",
            task_name="weather.hash.compute",
            payload={
                'gfs_t0_run_time_iso': '...',
                'cache_dir': io.settings.WEATHER.GCS_CACHE_DIR
            }
        )
        result_unit = await io.dispatch_and_wait(work)
        validator_hash = result_unit.result

        # ... Subsequent steps (verify hashes, trigger scoring, etc.) follow the same pattern
    ```

3.  **Update the IO-Engine:**
    **File:** `gaia/validator/app/io_engine.py`
    **Action:** Modify `setup_scheduled_jobs` to discover and run tasks.
    ```python
    def setup_scheduled_jobs(self):
        from gaia.tasks.defined_tasks.weather.task import WeatherTask
        tasks = [WeatherTask()] # Add other tasks here
        for task in tasks:
            self.scheduler.add_job(task.run_scheduled_job, "cron", ..., args=[self])
        self.scheduler.start()
    ```

This completes the blueprint for migrating the core logic into the new architecture. The remaining tasks involve methodically applying this pattern to all other functions and logic blocks as detailed in the Phase 1 checklists. 