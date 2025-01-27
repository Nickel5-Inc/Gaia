"""Parallel Processing Configuration.

This module defines the configuration settings for parallel task execution.
It uses a modular approach that makes it easy to add new task types and adjust settings.
"""

from typing import Dict, Any
from dataclasses import dataclass
from enum import Enum, auto

class NodeType(Enum):
    """Types of nodes in the system."""
    VALIDATOR = auto()
    MINER = auto()

class TaskType(Enum):
    """Types of tasks that can be parallelized."""
    IO_BOUND = auto()      # e.g., network requests, file operations
    CPU_BOUND = auto()     # e.g., data processing, scoring
    MEMORY_BOUND = auto()  # e.g., large model operations
    MIXED = auto()         # e.g., combined operations

@dataclass
class ResourceConfig:
    """Resource configuration for a task type."""
    n_workers: int
    threads_per_worker: int
    memory_limit: str
    scheduler_port: int = 8786
    dashboard_port: int = 8787
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for Dask."""
        return {
            "n_workers": self.n_workers,
            "threads_per_worker": self.threads_per_worker,
            "memory_limit": self.memory_limit,
            "scheduler_port": self.scheduler_port,
            "dashboard_port": self.dashboard_port
        }

# Default configurations for different task types
DEFAULT_CONFIGS = {
    TaskType.IO_BOUND: ResourceConfig(
        n_workers=4,
        threads_per_worker=2,
        memory_limit="4GB"
    ),
    TaskType.CPU_BOUND: ResourceConfig(
        n_workers=4,
        threads_per_worker=1,
        memory_limit="4GB"
    ),
    TaskType.MEMORY_BOUND: ResourceConfig(
        n_workers=2,
        threads_per_worker=1,
        memory_limit="8GB"
    ),
    TaskType.MIXED: ResourceConfig(
        n_workers=3,
        threads_per_worker=2,
        memory_limit="6GB"
    )
}

NODE_CONFIGS = {
    NodeType.VALIDATOR: {
        TaskType.IO_BOUND: ResourceConfig(
            n_workers=6,
            threads_per_worker=2,
            memory_limit="4GB"
        ),
    },
    NodeType.MINER: {
        TaskType.CPU_BOUND: ResourceConfig(
            n_workers=2,
            threads_per_worker=2,
            memory_limit="6GB"
        ),
    }
}

def get_resource_config(node_type: NodeType, task_type: TaskType) -> ResourceConfig:
    """Get resource configuration for a specific node and task type.
    
    Args:
        node_type: Type of node (validator or miner)
        task_type: Type of task to be executed
        
    Returns:
        ResourceConfig with appropriate settings
    """
    node_config = NODE_CONFIGS.get(node_type, {})
    if task_type in node_config:
        return node_config[task_type]
    
    return DEFAULT_CONFIGS[task_type] 