"""Parallel Processing Utilities.

This module provides helper functions and decorators for parallel task execution.
"""

import functools
from typing import TypeVar, List, Callable, Any, Optional, Dict, Union
from enum import Enum

from prefect import task, flow
from gaia.parallel.config.settings import NodeType, TaskType, ResourceConfig
from gaia.parallel.core.executor import parallel_executor, get_task_runner
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

T = TypeVar('T')  # Type variable for generic input
R = TypeVar('R')  # Type variable for generic output

def parallelize(
    node_type: NodeType,
    task_type: TaskType,
    batch_size: Optional[int] = None,
    config: Optional[ResourceConfig] = None
) -> Callable:
    """Decorator to parallelize a function using Dask.
    
    Args:
        node_type: Type of node (validator or miner)
        task_type: Type of task to be executed
        batch_size: Optional batch size for processing
        config: Optional custom resource configuration
        
    Returns:
        Decorated function that executes in parallel
    """
    def decorator(func: Callable[[List[T]], List[R]]) -> Callable[[List[T]], List[R]]:
        @functools.wraps(func)
        async def wrapper(items: List[T], **kwargs) -> List[R]:
            async with parallel_executor(node_type, task_type, config) as executor:
                return await executor.map(func, items, batch_size, **kwargs)
        return wrapper
    return decorator

def parallel_flow(
    node_type: NodeType,
    task_type: TaskType,
    name: Optional[str] = None,
    retries: int = 0,
    retry_delay_seconds: int = 0,
    config: Optional[ResourceConfig] = None,
    **flow_kwargs
) -> Callable:
    """Decorator to create a parallel Prefect flow.
    
    Args:
        node_type: Type of node (validator or miner)
        task_type: Type of task to be executed
        name: Optional flow name
        retries: Number of retries
        retry_delay_seconds: Delay between retries
        config: Optional custom resource configuration
        **flow_kwargs: Additional flow configuration
        
    Returns:
        Decorated flow that executes using Dask
    """
    def decorator(func: Callable) -> Callable:
        task_runner = get_task_runner(node_type, task_type, config)
        
        return flow(
            name=name or func.__name__,
            task_runner=task_runner,
            retries=retries,
            retry_delay_seconds=retry_delay_seconds,
            **flow_kwargs
        )(func)
    return decorator

def parallel_task(
    node_type: NodeType,
    task_type: TaskType,
    name: Optional[str] = None,
    retries: int = 0,
    retry_delay_seconds: int = 0,
    config: Optional[ResourceConfig] = None,
    **task_kwargs
) -> Callable:
    """Decorator to create a parallel Prefect task.
    
    Args:
        node_type: Type of node (validator or miner)
        task_type: Type of task to be executed
        name: Optional task name
        retries: Number of retries
        retry_delay_seconds: Delay between retries
        config: Optional custom resource configuration
        **task_kwargs: Additional task configuration
        
    Returns:
        Decorated task that executes using Dask
    """
    def decorator(func: Callable) -> Callable:
        @task(
            name=name or func.__name__,
            retries=retries,
            retry_delay_seconds=retry_delay_seconds,
            **task_kwargs
        )
        async def wrapper(*args, **kwargs):
            async with parallel_executor(node_type, task_type, config) as executor:
                return await executor.submit(func, *args, **kwargs)
        return wrapper
    return decorator

async def parallel_batch_process(
    items: List[T],
    process_func: Callable[[T], R],
    node_type: NodeType,
    task_type: TaskType,
    batch_size: int = 10,
    config: Optional[ResourceConfig] = None,
    **kwargs
) -> List[R]:
    """Process items in batches using parallel execution.
    
    Args:
        items: List of items to process
        process_func: Function to apply to each item
        node_type: Type of node (validator or miner)
        task_type: Type of task to be executed
        batch_size: Size of each batch
        config: Optional custom resource configuration
        **kwargs: Additional arguments for process_func
        
    Returns:
        List of processed results
    """
    async with parallel_executor(node_type, task_type, config) as executor:
        return await executor.map(
            process_func,
            items,
            batch_size=batch_size,
            **kwargs
        ) 