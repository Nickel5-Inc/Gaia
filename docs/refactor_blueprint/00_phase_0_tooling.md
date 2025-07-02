# Phase 0: Environment & Tooling Setup

**Objective:** Prepare the development environment to support and enforce the new architecture's higher quality standards *before* any code is refactored. This ensures that all new code adheres to the same strict standards from the beginning.

---

### Task 0.1: Update Project Dependencies

**Action:** Add all necessary dependencies for the new architecture to the project's `requirements.txt`.

**Command:**
```bash
pip install "pydantic-settings" "psutil" "apscheduler" "uvloop" "asyncpg" "numba" "py-spy" "prometheus-client" "structlog" "pytest" "pytest-asyncio" "pytest-mock" "alembic"
```

**Dependency Rationale:**
-   **Core Architecture:**
    -   `pydantic-settings`: For type-safe, environment-aware configuration, preventing misconfigurations.
    -   `psutil`: For process monitoring (memory, CPU), essential for the Supervisor's health checks.
    -   `apscheduler`: For robust, cron-like job scheduling in the IO-Engine, replacing `while True: sleep` loops.
    -   `uvloop`: A high-performance drop-in replacement for the standard asyncio event loop, boosting I/O throughput.
    -   `asyncpg`: The fastest available Python driver for PostgreSQL, crucial for a responsive IO-Engine.
-   **Performance & Analysis:**
    -   `numba`: For Just-In-Time (JIT) compilation of numerical hotspots (e.g., scoring math) to machine code.
    -   `py-spy`: For sampling the running application to identify CPU bottlenecks without stopping the process.
-   **Observability:**
    -   `prometheus-client`: To expose metrics from the Supervisor for monitoring with Prometheus/Grafana.
    -   `structlog`: For structured, JSON-based logging, which is machine-parseable and essential for modern log analysis.
-   **Testing & DB Management:**
    -   `pytest`, `pytest-asyncio`, `pytest-mock`: The standard toolkit for robust unit and integration testing of async code.
    -   `alembic`: For safe, version-controlled database schema migrations.

---

### Task 0.2: Configure Linter & Formatter (`ruff`)

**Action:** Create or update `ruff.toml` (or `pyproject.toml`) in the project root. This single tool will handle linting and formatting, ensuring consistent code style and catching common bugs.

**File:** `ruff.toml`
**Content:**
```toml
# Select all common rules, then add specifics.
# See https://docs.astral.sh/ruff/rules/ for a full list.
select = [
    "E",  # pycodestyle errors
    "W",  # pycodestyle warnings
    "F",  # pyflakes
    "I",  # isort (import sorting)
    "C",  # flake8-comprehensions (encourages efficient comprehensions)
    "B",  # flake8-bugbear (finds likely bugs)
    "UP", # pyupgrade (upgrades syntax to modern versions)
    "TID",# flake8-tidy-imports
    "T20",# flake8-print (flags print statements, encouraging proper logging)
]
ignore = ["E501"] # Delegate line length to the formatter.
line-length = 88

[isort]
known-first-party = ["gaia"]

[per-file-ignores]
# Allow `__init__.py` files to have unused imports for package exposure.
"__init__.py" = ["F401"]
```

**Verification:**
-   Run `ruff format .` to format the entire codebase.
-   Run `ruff check . --fix` to automatically fix all fixable linting errors.
-   These two commands must be added as a required CI step for all future pull requests.

---

### Task 0.3: Configure Static Type Checker (`mypy`)

**Action:** Create `mypy.ini` in the project root to enforce strict static typing. This is non-negotiable for preventing bugs in a complex, multi-process application.

**File:** `mypy.ini`
**Content:**
```ini
[mypy]
python_version = 3.9

# Core Strictness Flags
warn_return_any = True
warn_unused_configs = True
disallow_untyped_defs = True
disallow_any_unimported = True
no_implicit_optional = True
check_untyped_defs = True

# Disallow silent `Any` types, which hide bugs and defeat the purpose of typing.
disallow_any_generics = True
disallow_subclassing_any = True

# For better error messages.
show_error_codes = True

# Plugin support is required for libraries that use advanced Python features.
plugins = pydantic.mypy
```

**Verification:**
-   `mypy .` must pass with zero errors for any new or modified files.
-   A CI step should be added to run `mypy` on all changed files in a pull request. 