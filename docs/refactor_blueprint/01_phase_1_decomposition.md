# Phase 1: Codebase Decomposition & Structure

**Objective:** Create the new directory structure and begin the methodical migration of code from monoliths into new, single-responsibility modules. This phase is about moving code, not changing its logic. Its purpose is to create a clean, organized foundation before implementing the new multi-process architecture.

---

### Task 1.1: Create the New Directory Skeleton

**Action:** Execute the following commands from the `gaia/` directory root to create the target directory structure. This provides the "shelves" where we will place our new, refactored modules.

**Commands:**
```bash
# Create the core application structure under /validator
mkdir -p validator/app validator/db validator/substrate validator/utils

# Create the new, standardized task structure
mkdir -p tasks/defined_tasks/weather/validator
mkdir -p tasks/defined_tasks/geomagnetic/validator
mkdir -p tasks/defined_tasks/soilmoisture/validator

# Create __init__.py files to define Python packages
touch validator/__init__.py
touch validator/app/__init__.py
touch validator/db/__init__.py
touch validator/substrate/__init__.py
touch validator/utils/__init__.py

touch tasks/defined_tasks/base.py
touch tasks/defined_tasks/weather/__init__.py
touch tasks/defined_tasks/weather/validator/__init__.py
touch tasks/defined_tasks/geomagnetic/__init__.py
touch tasks/defined_tasks/geomagnetic/validator/__init__.py
touch tasks/defined_tasks/soilmoisture/__init__.py
touch tasks/defined_tasks/soilmoisture/validator/__init__.py
```

---

### Task 1.2: Define the `GaiaTask` Abstract Base Class

**Action:** Create the `GaiaTask` ABC. This is the cornerstone of the new task system, providing a standardized interface that allows the IO-Engine to manage and schedule any task without knowing its specific implementation details.

**File:** `gaia/tasks/defined_tasks/base.py`
**Content:**
```python
from abc import ABC, abstractmethod

class GaiaTask(ABC):
    """
    An abstract base class that defines the interface for all validation tasks.
    The IO-Engine will interact with tasks solely through this interface.
    """
    @property
    @abstractmethod
    def name(self) -> str:
        """A unique, machine-readable name for the task (e.g., 'weather')."""
        ...

    @property
    @abstractmethod
    def cron_schedule(self) -> str:
        """An APScheduler-compatible cron string (e.g., '5 18 * * *' for every day at 18:05 UTC)."""
        ...

    @abstractmethod
    async def run_scheduled_job(self, io_engine) -> None:
        """
        The single entry point called by the scheduler. This method is responsible
        for orchestrating the task's entire validation lifecycle for one run,
        including dispatching work to the compute pool and updating the database.
        """
        ...
```

---

### Task 1.3: Detailed Module Migration Mappings (Checklist)

**Action:** This section serves as the explicit checklist for refactoring. An engineer should go through these tables, move one function or logical block at a time from its old location to the new one, update the call site, and verify the change.

#### **Checklist for `gaia/validator/validator.py`**

| Original Function(s) / Logic Block | Destination Module | Destination Function/Class | Process | Status |
|---|---|---|---|:---:|
| `GaiaValidator.__init__`, `main`, `run_validator_logic` | `validator/app/cli.py`, `validator/validator.py` | `cli.main()`, `validator.main()` (Supervisor) | Supervisor | ☐ |
| `_signal_handler`, `_initiate_shutdown`| `validator/validator.py` | `Supervisor.signal_handler` | Supervisor | ☐ |
| `setup_neuron`, `_get_substrate_interface`, `_fetch_nodes...` | `validator/substrate/utils.py` | `get_substrate_connection`, `sync_metagraph` | **Compute** | ☐ |
| `query_miners`, `perform_handshake_with_retry` | `validator/app/io_engine.py` | `IOEngine.query_miners` | IO | ☐ |
| `_get_current_block_cached` | `validator/app/io_engine.py` | `IOEngine.get_cached_block` | IO | ☐ |
| `main_scoring`, `_calc_task_weights` | `validator/app/io_engine.py` | `IOEngine.scoring_cycle` (Scheduled Job) | IO | ☐ |
| `_perform_weight_calculations_sync` | `tasks/base.py` (logic), `scoring.py` (handler) | `handle_weight_calculation` | **Compute** | ☐ |
| `database_monitor`, `status_logger` | `validator/app/io_engine.py` | `IOEngine.run_monitoring_jobs` | IO | ☐ |
| `handle_miner_deregistration_loop` | `validator/app/io_engine.py` | `IOEngine.deregistration_job` | IO | ☐ |

**Note on `substrate`:** The most critical part of this refactor is isolating all `substrate-interface` calls. These functions will be moved to `validator/substrate/utils.py`, which will **only** be imported inside the disposable compute worker processes. This contains the suspected memory leaks.

---

#### **Checklist for `gaia/tasks/defined_tasks/weather/` Monoliths**

| Original File / Function(s) | Destination Module | Destination Function/Class | Process | Status |
|---|---|---|---|:---:|
| `weather_task.py:WeatherTask` | `weather/task.py` | `WeatherTask(GaiaTask)` | IO | ☐ |
| `weather_task.py:_load_config` | `validator/utils/config.py` | `Settings` (Pydantic model) | Supervisor | ☑️ |
| `weather_task.py:validator_execute`| `weather/validator/lifecycle.py` | `orchestrate_run` | IO | ☐ |
| `weather_task.py:validator_score` | `weather/validator/lifecycle.py` | `trigger_scoring_jobs` | IO | ☐ |
| `weather_logic.py:build_score_row` | `weather/validator/scoring.py` | `handle_build_score_row` | **Compute** | ☐ |
| `weather_workers.py:initial_scoring_worker` | `weather/validator/scoring.py` | `handle_day1_scoring` | **Compute** | ☐ |
| `weather_workers.py:finalize_scores_worker` | `weather/validator/scoring.py` | `handle_era5_scoring` | **Compute** | ☐ |
| `weather_workers.py:cleanup_worker` | `validator/app/io_engine.py` | `cleanup_job` (scheduled) | IO | ☐ |
| `weather_workers.py:fetch_and_hash_gfs_task` | `weather/validator/hashing.py` | `handle_gfs_hash` | **Compute** | ☐ |
| `weather_logic.py:get_ground_truth_data`| `weather/validator/scoring.py`| `get_ground_truth_data` | **Compute** | ☐ |
| All SQL strings (`_update_run_status`, etc.) | `validator/db/queries.py` | `update_run_status`, etc. | IO | ☐ |
| `weather_scoring/metrics.py:calculate_rmse` | `weather/validator/scoring.py` | `calculate_rmse` (Numba optimized) | **Compute** | ☐ |

*(This checklist should be updated with checkmarks as the refactoring proceeds.)* 