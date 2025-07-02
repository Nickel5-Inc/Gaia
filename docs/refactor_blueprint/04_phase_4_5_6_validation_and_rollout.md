# Phase 4, 5, & 6: Validation, Deployment, and Rollout

**Objective:** To rigorously verify the new system, prepare it for production, and execute a safe, controlled rollout. These final phases turn the refactored code into a production-ready application.

---

## Phase 4: Integration, Testing, & Validation

**Goal:** Prove the new architecture is correct, performant, and stable.

### Task 4.1: Unit & Integration Testing

*   **Unit Test Coverage:**
    *   **Goal:** Achieve >90% line coverage on all new, non-scaffold code in `utils/`, `db/`, `validator/`, and `tasks/` modules.
    *   **Example (`tests/unit/weather/test_scoring_handler.py`):**
        ```python
        from gaia.tasks.defined_tasks.weather.validator.scoring import handle_day1_scoring
        import pytest
        import xarray as xr
        import numpy as np

        @pytest.fixture
        def mock_data_paths(tmp_path):
            """Creates mock Zarr datasets for testing the scoring handler."""
            miner_path = tmp_path / "miner.zarr"
            gfs_path = tmp_path / "gfs.zarr"

            # Create sample datasets
            coords = {'lat': [0], 'lon': [0]}
            miner_ds = xr.Dataset({'2t': (('lat', 'lon'), np.array([[275.0]]))}, coords)
            gfs_ds = xr.Dataset({'2t': (('lat', 'lon'), np.array([[274.0]]))}, coords)

            miner_ds.to_zarr(miner_path, mode='w')
            gfs_ds.to_zarr(gfs_path, mode='w')

            return str(miner_path), str(gfs_path), "fake/climatology.zarr"

        def test_handle_day1_scoring(mock_data_paths):
            miner_path, gfs_path, clim_path = mock_data_paths
            # The handler should be a pure function, making it easy to test.
            result = handle_day1_scoring(miner_path, gfs_path, clim_path)

            assert isinstance(result, dict)
            assert 'day1_rmse_2t' in result
            assert np.isclose(result['day1_rmse_2t'], 1.0) # RMSE of (275-274) is 1
        ```

*   **Integration Test Suite:**
    *   **Setup:** The `tests/integration` suite will use a `docker-compose.test.yml` to spin up a dedicated PostgreSQL container.
    *   **Test Case Example (`tests/integration/test_full_lifecycle.py`):**
        1.  Start the application stack (Supervisor, IO-Engine, Compute Pool).
        2.  Use `asyncpg` to directly connect to the test DB and insert a "verified" miner forecast record for a test `run_id`.
        3.  Instantiate the `IOEngine` class in the test.
        4.  Create a `WorkUnit` for `weather.score.day1` with the test `run_id`.
        5.  Call `io_engine.dispatch_and_wait(work_unit)` and await the result.
        6.  Assert that the returned `ResultUnit` is successful and contains the expected score structure.
        7.  Use `asyncpg` again to query the test DB and confirm that the `score_table` was updated correctly.
        8.  Shut down the application stack and the DB container.

### Task 4.2: Performance & Soak Testing

*   **Performance Benchmarking:**
    *   **Action:** Use `pytest-benchmark` to measure the execution time of critical compute handlers.
    *   **Goal:** Establish a performance baseline (e.g., "Day 1 scoring for one miner takes 850ms on average") to track regressions and validate optimizations like Numba.

*   **72-Hour Soak Test:**
    *   **Action:** Deploy the full application in a staging environment that mirrors production hardware. Use a script (`locust` or custom) to generate a constant, realistic load of validation tasks.
    *   **Success Criteria:**
        1.  **Zero Crashes:** No unexpected process exits in the Supervisor logs.
        2.  **Stable Memory:** The RSS of the IO-Engine must remain flat (e.g., <5% growth over 24h). The RSS of Compute Workers should follow a stable saw-tooth pattern as they are recycled, with the baseline of the saw-tooth never trending upwards.
        3.  **Low Latency:** `gaia_io_event_loop_lag_seconds` must remain below 50ms at the 99th percentile.
        4.  **No Queue Buildup:** `gaia_ipc_queue_depth` must remain near zero (<10), indicating compute capacity is keeping up with demand.
        5.  **Data Correctness:** Manually verify the scores and weights being set on the testnet match expected values from a control script.

---

## Phase 5: Observability & Deployment

**Objective:** Prepare the application for production operation with robust monitoring and a repeatable deployment process.

### Task 5.1: Finalize Observability Stack

*   **Structured Logging:**
    *   **Action:** Implement a central logging configuration using `structlog`.
    *   **File:** `gaia/validator/app/logging_config.py`
    *   **Content:**
        ```python
        import structlog
        from structlog.contextvars import bind_contextvars
        import logging

        def setup_logging(log_level="INFO"):
            logging.basicConfig(level=log_level, format="%(message)s")
            structlog.configure(
                processors=[
                    structlog.contextvars.merge_contextvars,
                    structlog.processors.add_log_level,
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.processors.JSONRenderer(),
                ],
                logger_factory=structlog.stdlib.LoggerFactory(),
                cache_logger_on_first_use=True,
            )
        # Usage in any module:
        # log = structlog.get_logger()
        # bind_contextvars(job_id=123)
        # log.info("Job started")
        ```

*   **Metrics & Alerting:**
    *   **Action:** Create a pre-configured Grafana dashboard JSON model and define key alerts.
    *   **Grafana Dashboard Panels:**
        - **Process Health:** CPU & Memory RSS (for Supervisor, IO-Engine, and each Compute Worker), `gaia_child_process_restarts_total`.
        - **Application Throughput:** IPC Queue Depth, Compute Job Rate (`rate(gaia_compute_job_duration_seconds_count[5m])`), Job Latency (`histogram_quantile(0.95, ...)`).
        - **IO-Engine Health:** Event Loop Lag.
    *   **Example PromQL Alert (for Alertmanager):**
        ```yaml
        - alert: ValidatorIPCQueueFull
          expr: gaia_ipc_queue_depth{queue="work"} > 500
          for: 5m
          labels: { severity: critical }
          annotations:
            summary: "Validator IPC work queue is full!"
            description: "The work queue between the IO-Engine and Compute Pool is over 500. The system is back-pressured and compute cannot keep up. Check compute worker logs."
        ```

### Task 5.2: Production Deployment

*   **`Dockerfile`:**
    *   **Action:** Create a multi-stage `Dockerfile` for a lean production image.
        ```dockerfile
        # ---- Builder Stage ----
        FROM python:3.9-slim as builder
        WORKDIR /install
        COPY requirements.txt .
        RUN pip install --prefix="/install" -r requirements.txt

        # ---- Final Stage ----
        FROM python:3.9-slim
        WORKDIR /app
        COPY --from=builder /install /usr/local
        COPY . .
        # The entrypoint for the entire application
        CMD ["python", "-m", "gaia.validator.validator"]
        ```

*   **`docker-compose.yml`:**
    *   **Action:** Define the full production stack for easy deployment.
        ```yaml
        version: '3.8'
        services:
          validator:
            build: .
            restart: unless-stopped
            volumes:
              - ./data/cache:/app/gfs_analysis_cache # Persist data caches
            ports:
              - "9090:9090" # Prometheus metrics endpoint
            env_file: .env # Production environment variables
            # Add resource limits for production
            mem_limit: 4g
            cpus: 4.0

          postgres:
            image: postgres:14-alpine
            restart: unless-stopped
            volumes:
              - postgres_data:/var/lib/postgresql/data
            environment:
              POSTGRES_DB: ${POSTGRES_DB}
              POSTGRES_USER: ${POSTGRES_USER}
              POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}

        volumes:
          postgres_data:
        ```

---

## Phase 6: Rollout & Risk Management

**Objective:** To deploy the new architecture safely with minimal downtime and clear rollback paths.

| Week | Major Goal | Key Activities | Go/No-Go Criteria | Rollback Plan |
|---|---|---|---|---|
| **1-2** | **Phase 0 & 1:** Setup & Decomp | Setup tooling (`ruff`, `mypy`). Create all new files/directories. Move pure utility functions and config logic. | All unit tests for moved code pass. `mypy` is clean on new modules. Codebase is formatted. | Revert Git commits for this phase. |
| **3-4** | **Phase 2 & 3.1:** Infrastructure & DB | Implement Supervisor/IO/Compute main loops. Implement DB connection manager. Migrate **all** DB queries to the new `queries.py` module. | All integration tests for IPC and the DB query layer pass. | Revert Git commits for this phase. |
| **5-6** | **Phase 3.2-3.4:** Full Logic Migration | Methodically move all hashing, scoring, and orchestration logic into the new architecture. At the end of this, the old monoliths should be empty shells. | All integration tests pass. Benchmarks show no performance regression vs. the old system. | Revert Git commits. The system should still be runnable in its old mode until this phase is complete. |
| **7** | **Phase 4:** Staging Soak Test | Deploy the complete new architecture to a staging environment. Run the 72-hour soak test under simulated, realistic load. | All soak test success criteria (no crashes, stable memory, low latency, no queue buildup) are met. | Keep old version running in production. Address issues found in staging. Do not proceed. |
| **8** | **Phase 5 & 6:** Production Cutover | Finalize Grafana dashboards and alerts. Tag `v4.0`. Deploy to production using a blue/green strategy if possible (run both old and new versions, only route traffic to new). | Monitor production metrics closely for 24 hours. No new critical errors in logs. Memory and CPU usage are stable and as expected. | Immediately switch DNS/load balancer traffic back to the old (blue) deployment. Investigate production issue. | 