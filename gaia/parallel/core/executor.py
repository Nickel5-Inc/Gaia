"""Parallel Processing Executor.

This module provides the core functionality for parallel task execution.
It handles cluster management, task distribution, and resource cleanup.
"""

import asyncio
from typing import TypeVar, List, Callable, Any, Optional, Dict
from contextlib import asynccontextmanager

from dask.distributed import Client, LocalCluster
from prefect import task, flow
from prefect_dask import DaskTaskRunner

from gaia.parallel.config.settings import (
    NodeType, 
    TaskType, 
    ResourceConfig,
    get_resource_config
)
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

T = TypeVar('T')  # Type variable for generic input
R = TypeVar('R')  # Type variable for generic output

class ParallelExecutor:
    """Manages parallel task execution using Dask."""
    
    def __init__(
        self,
        node_type: NodeType,
        task_type: TaskType,
        config: Optional[ResourceConfig] = None
    ):
        """Initialize the parallel executor.
        
        Args:
            node_type: Type of node (validator or miner)
            task_type: Type of task to be executed
            config: Optional custom resource configuration
        """
        self.node_type = node_type
        self.task_type = task_type
        self.config = config or get_resource_config(node_type, task_type)
        self._client: Optional[Client] = None
        self._cluster: Optional[LocalCluster] = None
        
    async def __aenter__(self) -> 'ParallelExecutor':
        """Set up the Dask cluster and client."""
        try:
            self._cluster = LocalCluster(**self.config.to_dict())
            self._client = Client(self._cluster)
            logger.info(
                f"Initialized Dask cluster with {self.config.n_workers} workers "
                f"and {self.config.threads_per_worker} threads per worker"
            )
            return self
            
        except Exception as e:
            logger.error(f"Failed to initialize Dask cluster: {str(e)}")
            await self.__aexit__(type(e), e, e.__traceback__)
            raise
            
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up Dask resources."""
        if self._client:
            await self._client.close()
            self._client = None
            
        if self._cluster:
            self._cluster.close()
            self._cluster = None
    
    @property
    def client(self) -> Optional[Client]:
        """Get the current Dask client."""
        return self._client
    
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
        if not self._client:
            raise RuntimeError("Dask client not initialized")
            
        if batch_size:
            # Process in batches
            results = []
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                batch_futures = [
                    self._client.submit(func, item, **kwargs)
                    for item in batch
                ]
                batch_results = await self._client.gather(batch_futures)
                results.extend(batch_results)
            return results
        else:
            futures = [
                self._client.submit(func, item, **kwargs)
                for item in items
            ]
            return await self._client.gather(futures)
            
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
        if not self._client:
            raise RuntimeError("Dask client not initialized")
            
        future = self._client.submit(func, *args, **kwargs)
        return await self._client.gather(future)

@asynccontextmanager
async def parallel_executor(
    node_type: NodeType,
    task_type: TaskType,
    config: Optional[ResourceConfig] = None
):
    """Context manager for parallel execution.
    
    Args:
        node_type: Type of node (validator or miner)
        task_type: Type of task to be executed
        config: Optional custom resource configuration
        
    Yields:
        ParallelExecutor instance
    """
    async with ParallelExecutor(node_type, task_type, config) as executor:
        yield executor

def get_task_runner(
    node_type: NodeType,
    task_type: TaskType,
    config: Optional[ResourceConfig] = None
) -> DaskTaskRunner:
    """Get a Prefect task runner configured for Dask.
    
    Args:
        node_type: Type of node (validator or miner)
        task_type: Type of task to be executed
        config: Optional custom resource configuration
        
    Returns:
        Configured DaskTaskRunner
    """
    resource_config = config or get_resource_config(node_type, task_type)
    return DaskTaskRunner(
        cluster_kwargs=resource_config.to_dict()
    ) 