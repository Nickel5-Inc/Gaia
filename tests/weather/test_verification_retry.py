import asyncio
import sys
import pathlib
from datetime import datetime, timezone, timedelta
import types

import pytest

# -----------------------------------------------------------------------------
# Insert lightweight stubs for heavy optional deps BEFORE importing weather_logic
# -----------------------------------------------------------------------------
import types

# Stub xarray, pandas, numpy to avoid heavy dependencies in unit-test context
def _ensure_stub(mod_name: str):
    if mod_name not in sys.modules:
        stub = types.ModuleType(mod_name)
        sys.modules[mod_name] = stub
        # minimal extras
        if mod_name == 'pandas':
            def Timestamp(*args, **kwargs):
                class _T:
                    def tz_localize(self, *a, **k): return self
                    def tz_convert(self, *a, **k): return self
                return _T()
            stub.Timestamp = Timestamp
        if mod_name == 'numpy':
            import math
            stub.float32 = float
            stub.float64 = float
            stub.nan = math.nan

for _m in ("xarray", "pandas", "numpy", "xskillscore"):
    _ensure_stub(_m)

# Ensure project root is on sys.path so that 'gaia' package resolves when tests run via plain pytest
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Now we can safely import the target function (which itself imports pandas/xarray)
from gaia.tasks.defined_tasks.weather.processing.weather_logic import verify_miner_response

# Stub pandas and numpy to avoid heavy deps
for mod_name in ('pandas','numpy'):
    if mod_name not in sys.modules:
        stub = types.ModuleType(mod_name)
        sys.modules[mod_name] = stub
        if mod_name == 'pandas':
            def Timestamp(*args, **kwargs):
                class _T: 
                    def tz_localize(self, *a, **k): return self
                    def tz_convert(self, *a, **k): return self
                return _T()
            stub.Timestamp = Timestamp

# Stub fiber.logging_utils for logger dependency
if 'fiber.logging_utils' not in sys.modules:
    fiber_mod = types.ModuleType('fiber')
    logging_utils_stub = types.ModuleType('fiber.logging_utils')
    class _DummyLogger:
        def info(self,*a,**k): pass
        def warning(self,*a,**k): pass
        def error(self,*a,**k): pass
        def debug(self,*a,**k): pass
    logging_utils_stub.get_logger = lambda name=None: _DummyLogger()
    fiber_mod.logging_utils = logging_utils_stub
    sys.modules['fiber'] = fiber_mod
    sys.modules['fiber.logging_utils'] = logging_utils_stub

# Further lightweight stubs for boto3 and jwt expected in weather code paths
for _m in ('boto3','jwt'):
    _ensure_stub(_m)

class _FakeDBManager:
    """Very light stub that records updates to weather_miner_responses."""
    def __init__(self):
        # key: id -> row dict
        self.rows = {}
        # store last executed params for assertions
        self.last_execute = None

    async def fetch_one(self, query: str, params: dict):
        # Very naive parsing based on presence of id or rid param
        resp_id = params.get("id") or params.get("resp_id") or params.get("rid")
        if resp_id is not None:
            row = self.rows.get(resp_id)
            return row
        return None

    async def fetch_all(self, query: str, params: dict):
        # Not used in this test
        return []

    async def execute(self, query: str, params: dict = None):
        # Store last call for debug
        self.last_execute = (query, params)
        if params is None:
            params = {}
        # Simulate UPDATE statements by altering self.rows
        if "UPDATE weather_miner_responses" in query:
            resp_id = params.get("id") or params.get("resp_id")
            if resp_id not in self.rows:
                self.rows[resp_id] = {"id": resp_id}
            row = self.rows[resp_id]
            # crude extraction: iterate params keys present in row updates
            for k, v in params.items():
                if k in ["verified", "new_status", "err_msg", "status", "m_hash", "v_hash", "match", "nrt", "rc" ]:
                    # map names
                    if k == "new_status":
                        row["status"] = v
                    elif k == "err_msg":
                        row["error_message"] = v
                    elif k == "verified":
                        row["verification_passed"] = v
                    elif k == "nrt":
                        row["next_retry_time"] = v
                    elif k == "rc":
                        row["retry_count"] = v
                    else:
                        row[k] = v
        return True

    # Context manager compatibility methods used by code but not needed here
    async def session(self, *args, **kwargs):
        return self  # simplistic

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass


@pytest.mark.asyncio
async def test_verify_miner_response_failure_schedules_retry(monkeypatch):
    """Ensure failed manifest verification schedules a retry."""
    db = _FakeDBManager()

    # Seed a response row with status 'inference_triggered'
    resp_id = 1
    db.rows[resp_id] = {
        "id": resp_id,
        "miner_hotkey": "minerHK",
        "job_id": "job123",
        "run_id": 42,
        "status": "inference_triggered",
        "retry_count": 0,
        "next_retry_time": None,
    }

    # Minimal fake task & run_details
    class _FakeTask:
        def __init__(self):
            self.db_manager = db
            self.config = {"verification_timeout_seconds": 5}
            self.validator = None  # not used here

    task = _FakeTask()

    run_details = {"id": 42}
    response_details = db.rows[resp_id]

    # Monkeypatch _request_fresh_token to return token data
    from gaia.tasks.defined_tasks.weather.processing import weather_logic as wl
    async def _dummy_request(*args, **kwargs):
        return ("token", "https://dummy/url", "hash123")
    monkeypatch.setattr(wl, "_request_fresh_token", _dummy_request)

    # Monkeypatch open_verified_remote_zarr_dataset to simulate failure (returns None)
    async def _dummy_open(*args, **kwargs):
        return None
    monkeypatch.setattr(wl, "open_verified_remote_zarr_dataset", _dummy_open)

    # Run function
    await verify_miner_response(task, run_details, response_details)

    row = db.rows[resp_id]
    assert row["status"] == "retry_scheduled"
    assert row["retry_count"] == 1
    assert isinstance(row["next_retry_time"], datetime)
    assert row["next_retry_time"] > datetime.now(timezone.utc) - timedelta(seconds=1) 