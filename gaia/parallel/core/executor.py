"""Parallel Processing Executor.

This module provides the core functionality for parallel task execution
using Python's built-in concurrent.futures for local execution.
"""

import asyncio
from typing import TypeVar, List, Callable, Any, Optional
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial

from prefect import task

from gaia.parallel.config.settings import TaskType, LocalResourceConfig, get_resource_config
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

T = TypeVar('T')  # Type variable for generic input
R = TypeVar('R')  # Type variable for generic output

class LocalParallelExecutor:
    """Manages parallel task execution using concurrent.futures."""
    
    def __init__(
        self,
        task_type: TaskType,
        config: Optional[LocalResourceConfig] = None
    ):
        """Initialize the parallel executor.
        
        Args:
            task_type: Type of task to be executed
            config: Optional custom resource configuration
        """
        self.task_type = task_type
        self.config = config or get_resource_config(task_type)
        self._executor = None
        
    async def __aenter__(self) -> 'LocalParallelExecutor':
        """Set up the appropriate executor."""
        try:
            executor_class = ThreadPoolExecutor if self.config.thread_mode else ProcessPoolExecutor
            self._executor = executor_class(max_workers=self.config.max_workers)
            logger.info(
                f"Initialized {'thread' if self.config.thread_mode else 'process'} pool "
                f"with {self.config.max_workers} workers"
            )
            return self
            
        except Exception as e:
            logger.error(f"Failed to initialize executor: {str(e)}")
            await self.__aexit__(type(e), e, e.__traceback__)
            raise
            
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up executor resources."""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
    
    async def map(
        self,
        func: Callable[[T], R],
        items: List[T],
        batch_size: Optional[int] = None,
        **kwargs
    ) -> List[R]:
        """Execute a function over a list of items in parallel.
        
        Args:
            func: Function to execute on each item
            items: List of items to process
            batch_size: Optional batch size for processing
            **kwargs: Additional arguments to pass to the function
            
        Returns:
            List of results
        """
        if not self._executor:
            raise RuntimeError("Executor not initialized")
            
        loop = asyncio.get_event_loop()
        func_with_kwargs = partial(func, **kwargs) if kwargs else func
        
        if batch_size:
            results = []
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                batch_results = await loop.run_in_executor(
                    self._executor,
                    lambda: list(map(func_with_kwargs, batch))
                )
                results.extend(batch_results)
            return results
        else:
            return await loop.run_in_executor(
                self._executor,
                lambda: list(map(func_with_kwargs, items))
            )
            
    async def submit(
        self,
        func: Callable[..., R],
        *args,
        **kwargs
    ) -> R:
        """Submit a single function for execution.
        
        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Function result
        """
        if not self._executor:
            raise RuntimeError("Executor not initialized")
            
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            lambda: func(*args, **kwargs)
        )

@asynccontextmanager
async def parallel_executor(
    task_type: TaskType,
    config: Optional[LocalResourceConfig] = None
):
    """Context manager for parallel execution.
    
    Args:
        task_type: Type of task to be executed
        config: Optional custom resource configuration
        
    Yields:
        LocalParallelExecutor instance
    """
    async with LocalParallelExecutor(task_type, config) as executor:
        yield executor

def parallel_task(task_type: TaskType, **task_kwargs):
    """Decorator to run a function as a parallel task.
    
    Args:
        task_type: Type of task to be executed
        **task_kwargs: Additional kwargs for the Prefect task decorator
        
    Returns:
        Decorated function that runs in parallel
    """
    def decorator(func):
        @task(**task_kwargs)
        async def wrapper(*args, **kwargs):
            async with parallel_executor(task_type) as executor:
                return await executor.submit(func, *args, **kwargs)
        return wrapper
    return decorator 