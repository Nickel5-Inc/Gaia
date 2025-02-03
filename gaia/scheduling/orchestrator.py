from typing import Dict, List, Optional
from prefect import serve
from .config import get_schedule_config
from gaia.tasks.base.task import BaseTask
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class TaskOrchestrator:
    """
    Orchestrates task scheduling and execution.
    """
    
    def __init__(self):
        self.tasks = {}
        
    def register_task(self, task: BaseTask) -> None:
        """Register a task with the orchestrator."""
        self.tasks[task.name] = task
        logger.info(f"Registered task: {task.name}")
        
    def register_soil_moisture_task(self, task: BaseTask) -> None:
        """Register soil moisture task with special window handling."""
        self.tasks[task.name] = task
        logger.info(f"Registered soil moisture task: {task.name}")
        
        task.validator_execute.serve(
            name="soil-validator",
            work_pool_name="local-process-pool",
            work_queue_name="soil_moisture",
            interval=300
        )
        
    def register_geomagnetic_task(self, task: BaseTask) -> None:
        """Register geomagnetic task."""
        self.tasks[task.name] = task
        logger.info(f"Registered geomagnetic task: {task.name}")
        
        task.execute_flow.serve(
            name="geomagnetic-validator",
            work_pool_name="local-process-pool",
            work_queue_name="data_processing",
            interval=3600
        )
        
    def start(self) -> None:
        """Start serving all flows."""
        serve() 