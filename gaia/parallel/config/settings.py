"""Parallel Processing Configuration.

This module defines the configuration settings for parallel task execution.
"""

from typing import Dict, Any
from dataclasses import dataclass
from enum import Enum, auto

class TaskType(Enum):
    """Types of tasks that can be parallelized."""
    IO_BOUND = auto()      # e.g., network requests, file operations
    CPU_BOUND = auto()     # e.g., data processing, scoring
    MEMORY_BOUND = auto()  # e.g., large model operations
    MIXED = auto()         # e.g., combined operations

@dataclass
class LocalResourceConfig:
    """Resource configuration for local task execution."""
    max_workers: int
    thread_mode: bool  # True for threads, False for processes
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "n_workers": self.max_workers,
            "threads_per_worker": 1,
            "processes": not self.thread_mode
        }

DEFAULT_CONFIGS = {
    TaskType.IO_BOUND: LocalResourceConfig(
        max_workers=4,
        thread_mode=True
    ),
    TaskType.CPU_BOUND: LocalResourceConfig(
        max_workers=4,
        thread_mode=False
    ),
    TaskType.MEMORY_BOUND: LocalResourceConfig(
        max_workers=2,
        thread_mode=False
    ),
    TaskType.MIXED: LocalResourceConfig(
        max_workers=3,
        thread_mode=True
    )
}

def get_resource_config(task_type: TaskType) -> LocalResourceConfig:
    """Get resource configuration for a specific task type.
    
    Args:
        task_type: Type of task to be executed
        
    Returns:
        LocalResourceConfig with appropriate settings
    """
    return DEFAULT_CONFIGS[task_type] 