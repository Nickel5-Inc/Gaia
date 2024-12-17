from prefect import flow, task, get_run_logger
from prefect.task_runners import RayTaskRunner
from multiprocessing import cpu_count
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)

class BaseFlow(ABC):
    """Base class for all validator flows.
    
    Provides common functionality and structure for validator flows:
    - Standard Ray task runner configuration
    - Flow metadata and versioning
    - Abstract methods for flow execution
    - Common logging and error handling
    """
    
    def __init__(self, name: str, description: str, version: str = "1.0.0"):
        self.name = name
        self.description = description
        self.version = version
        self.logger = get_run_logger()

    def get_ray_runner(self) -> RayTaskRunner:
        """Get configured Ray task runner."""
        return RayTaskRunner(
            address="auto",
            ray_init_kwargs={
                "num_cpus": cpu_count(),
                "include_dashboard": True,
                "dashboard_host": "0.0.0.0",
                "dashboard_port": 8265,
            }
        )

    @abstractmethod
    async def run(self, **kwargs):
        """Main flow execution method to be implemented by subclasses."""
        pass

    def get_flow_metadata(self) -> Dict[str, Any]:
        """Get flow metadata for Prefect."""
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "task_runner": self.get_ray_runner(),
        }

    def create_flow(self):
        """Create a Prefect flow with standard configuration."""
        metadata = self.get_flow_metadata()
        
        @flow(**metadata)
        async def _flow(**kwargs):
            try:
                return await self.run(**kwargs)
            except Exception as e:
                self.logger.error(f"Error in flow {self.name}: {e}")
                raise
        
        return _flow 