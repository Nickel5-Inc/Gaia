"""Parallel Processing with Prefect-Dask.

This module provides utilities for parallel execution using Prefect's Dask integration.
"""

from typing import Optional, Dict
from contextlib import asynccontextmanager
from distributed import LocalCluster, Client
from prefect_dask import DaskTaskRunner
from datetime import timedelta
import asyncio
import atexit
import multiprocessing
from multiprocessing import freeze_support
from fiber.logging_utils import get_logger
from gaia.parallel.config.settings import TaskType, LocalResourceConfig

logger = get_logger(__name__)

class SharedDaskExecutor:
    """
    Manages a shared Dask cluster for all validator tasks.
    Implements resource pools for different task types.
    """
    _instance = None
    _cluster: Optional[LocalCluster] = None
    _client: Optional[Client] = None
    _runners: Dict[TaskType, DaskTaskRunner] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            if multiprocessing.get_start_method(allow_none=True) != 'fork':
                multiprocessing.set_start_method('fork', force=True)
            self._setup_cluster()
            atexit.register(self.cleanup)

    def _setup_cluster(self):
        """Initialize the shared Dask cluster with resource pools."""
        if self._cluster is None:
            try:
                self._cluster = LocalCluster(
                    n_workers=4,
                    threads_per_worker=2,
                    memory_limit='4GB',
                    dashboard_address=':0',
                    lifetime=timedelta(hours=1),
                    lifetime_stagger='5 minutes',
                    lifetime_restart=True,
                    processes=True,
                    protocol='tcp://',
                    scheduler_port=0,
                    silence_logs='WARNING',
                    resources={
                        'io': 8,
                        'cpu': 4,
                        'memory': 4
                    }
                )
                
                self._client = Client(self._cluster)
                logger.info(
                    f"Initialized shared Dask cluster - "
                    f"Dashboard: {self._cluster.dashboard_link}"
                )
                
            except Exception as e:
                logger.error(f"Failed to initialize Dask cluster: {e}")
                raise

    def get_runner(self, task_type: TaskType) -> DaskTaskRunner:
        """Get a task runner configured for specific task type."""
        if task_type not in self._runners:
            config = {
                TaskType.IO_BOUND: {
                    'minimum': 1,
                    'maximum': 8
                },
                TaskType.CPU_BOUND: {
                    'minimum': 1,
                    'maximum': 4
                },
                TaskType.MEMORY_BOUND: {
                    'minimum': 1,
                    'maximum': 2
                },
                TaskType.MIXED: {
                    'minimum': 1,
                    'maximum': 4
                }
            }[task_type]
            
            self._runners[task_type] = DaskTaskRunner(
                cluster=self._cluster,
                adapt_kwargs=config
            )
            
        return self._runners[task_type]

    def cleanup(self):
        """Clean up cluster resources."""
        if self._client:
            self._client.close()
        if self._cluster:
            self._cluster.close()

_executor = None

def get_executor() -> SharedDaskExecutor:
    """Get the global shared executor instance."""
    global _executor
    if _executor is None:
        _executor = SharedDaskExecutor()
    return _executor

def get_task_runner(task_type: TaskType) -> DaskTaskRunner:
    """Get a task runner for the specified task type.
    
    Args:
        task_type: Type of task to get runner for
        
    Returns:
        DaskTaskRunner: Configured task runner instance
    """
    executor = get_executor()
    return executor.get_runner(task_type)

def initialize_dask():
    """Initialize Dask with proper process handling."""
    freeze_support()
    return get_executor()

if __name__ == "__main__":
    initialize_dask() 