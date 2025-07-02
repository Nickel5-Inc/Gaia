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
    task_name: str  # e.g., "weather.score.day1", "substrate.get_block"
    payload: Dict[str, Any]
    shared_data: Optional[Dict[str, SharedMemoryNumpy]] = None
    timeout_seconds: int = 300


class ResultUnit(BaseModel):
    """A response sent from a Compute Worker back to the IO-Engine."""
    job_id: str
    task_name: str
    success: bool
    result: Optional[Any] = None  # For small, serializable results
    shared_data_result: Optional[Dict[str, SharedMemoryNumpy]] = None  # For large array results
    error: Optional[str] = None
    worker_pid: int
    execution_time_ms: float