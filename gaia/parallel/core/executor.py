"""Parallel Processing with Prefect-Dask.

This module provides utilities for parallel execution using Prefect's Dask integration.
"""

from typing import Optional
from contextlib import contextmanager
from prefect import flow
from prefect_dask import DaskTaskRunner, get_dask_client

from gaia.parallel.config.settings import TaskType, LocalResourceConfig, get_resource_config
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

def get_dask_runner(task_type: TaskType, config: Optional[LocalResourceConfig] = None) -> DaskTaskRunner:
    """Get a Dask task runner configured for the task type.
    
    Args:
        task_type: Type of task to be executed
        config: Optional custom resource configuration
        
    Returns:
        Configured DaskTaskRunner with appropriate settings for the task type
    """
    resource_config = config or get_resource_config(task_type)
    
    cluster_kwargs = {
        "n_workers": resource_config.max_workers,
        "processes": not resource_config.thread_mode,
        "threads_per_worker": 2 if resource_config.thread_mode else 1,
        "memory_limit": "4GB"
    }
    
    return DaskTaskRunner(cluster_kwargs=cluster_kwargs)

@contextmanager
def parallel_context(task_type: TaskType):
    """Context manager for parallel execution using Dask.
    
    Args:
        task_type: Type of task to be executed
        
    Example:
        @task
        def process_data(df):
            with parallel_context(TaskType.CPU_BOUND):
                result = df.compute()
            return result
    """
    with get_dask_client() as client:
        yield client

def parallel_flow(task_type: TaskType, **flow_kwargs):
    """Decorator to create a parallel flow using Dask task runner.
    
    Args:
        task_type: Type of task to be executed
        **flow_kwargs: Additional kwargs for the Prefect flow decorator
        
    Returns:
        Decorated flow that runs with Dask parallelization
        
    Example:
        @task
        def read_data(path: str) -> dask.dataframe.DataFrame:
            return dask.dataframe.read_parquet(path)
            
        @task
        def process_data(df: dask.dataframe.DataFrame):
            with parallel_context(TaskType.CPU_BOUND):
                result = df.groupby('column').mean().compute()
            return result
            
        @parallel_flow(TaskType.CPU_BOUND)
        def analysis_flow():
            df = read_data.submit("data.parquet")
            result = process_data.submit(df)
            return result
    """
    def decorator(func):
        runner = get_dask_runner(task_type)
        return flow(task_runner=runner, **flow_kwargs)(func)
    return decorator 