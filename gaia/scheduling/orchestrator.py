from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule, CronSchedule
from typing import Dict, List, Optional, Any
import asyncio
from datetime import datetime, timedelta
from .config import ScheduleConfig, get_schedule_config
from gaia.tasks.base.task import Task
from gaia.scheduling.deployments import create_deployment

"""
Task Orchestrator Module

This module provides the main orchestration logic for task scheduling and execution.
It handles:
1. Task registration and deployment
2. Schedule management
3. Resource monitoring
4. Health monitoring
5. Flow coordination

All tasks are executed as local processes for simplicity and direct system access.
"""

class TaskOrchestrator:
    """
    Manages the lifecycle and scheduling of all tasks in the system.
    Handles task registration, deployment, monitoring, and coordination.
    """
    
    def __init__(self):
        self.tasks: Dict[str, Task] = {}
        self.deployments: Dict[str, Deployment] = {}
        self.active_runs: Dict[str, List[str]] = {}
        self.health_checks: Dict[str, datetime] = {}
        
    async def register_task(
        self, 
        task: Task, 
        config: Optional[ScheduleConfig] = None
    ) -> None:
        """
        Register a task with the orchestrator and create its deployment
        """
        task_name = task.name
        if task_name in self.tasks:
            raise ValueError(f"Task {task_name} already registered")
            
        # Get or create config
        config = config or get_schedule_config(task_name)
        
        # Create deployment
        deployment = await create_deployment(
            task=task,
            config=config
        )
        
        # Store task and deployment
        self.tasks[task_name] = task
        self.deployments[task_name] = deployment
        self.active_runs[task_name] = []
        
        # Initialize health monitoring
        await self._initialize_monitoring(task_name)

    @flow(name="task_monitoring_flow")
    async def monitor_tasks(self):
        """
        Monitor task health and handle issues
        """
        while True:
            try:
                for task_name, task in self.tasks.items():
                    # Check task health
                    if task.state['status'] == 'failed':
                        await self._handle_task_failure(task_name)
                    # Check resource usage
                    if await self._check_resource_limits(task_name):
                        await self._scale_resources(task_name)
            
                    self.health_checks[task_name] = datetime.now()
                    
            except Exception as e:
                print(f"Error in monitoring: {str(e)}")
                
            await asyncio.sleep(60)

    async def _handle_task_failure(self, task_name: str):
        """Handle task failures and implement recovery strategies"""
        task = self.tasks[task_name]
        config = get_schedule_config(task_name)
        
        if len(task.state['errors']) < config.max_retries:
            await self.restart_task(task_name)
        else:
            await self._implement_fallback(task_name)

    async def restart_task(self, task_name: str):
        """Restart a failed task"""
        if task_name not in self.tasks:
            raise ValueError(f"Task {task_name} not found")
            
        deployment = self.deployments[task_name]
        await deployment.pause()
        
        # Clean up any running instances
        await self._cleanup_task_runs(task_name)
        
        # Reset task state
        self.tasks[task_name].state['status'] = 'idle'
        self.tasks[task_name].state['errors'] = []
        
        # Resume deployment
        await deployment.resume()

    async def _cleanup_task_runs(self, task_name: str):
        """Clean up any running instances of a task"""
        active_runs = self.active_runs.get(task_name, [])
        for run_id in active_runs:
            try:
                await self._terminate_run(run_id)
            except Exception as e:
                print(f"Error cleaning up run {run_id}: {str(e)}")
        
        self.active_runs[task_name] = []

    async def _check_resource_limits(self, task_name: str) -> bool:
        """Check if task is approaching resource limits"""
        task = self.tasks[task_name]
        config = get_schedule_config(task_name)
        
        # Resource checks (cpu and memory)
        if task.state['metrics'].get('memory_usage', 0) > float(config.memory_limit[:-1]) * 0.9:
            return True
            
        # Check CPU usage
        if task.state['metrics'].get('cpu_usage', 0) > config.cpu_limit * 0.9:
            return True
            
        return False

    async def _scale_resources(self, task_name: str):
        """Scale resources for a task if needed"""
        config = get_schedule_config(task_name)
        
        # This scales the resources for the task by 1.5x
        new_config = ScheduleConfig(
            **config.dict(),
            memory_limit=f"{float(config.memory_limit[:-1]) * 1.5}G",
            cpu_limit=config.cpu_limit * 1.5
        )

        await self._update_deployment(task_name, new_config)

    async def _update_deployment(self, task_name: str, new_config: ScheduleConfig):
        """Update an existing deployment with new configuration"""
        # Have to make new deployment first, then replace old one
        task = self.tasks[task_name]
        new_deployment = await self._create_deployment(task, new_config)
        # Replace old deployment
        old_deployment = self.deployments[task_name]
        await old_deployment.pause()
        self.deployments[task_name] = new_deployment

    async def _implement_fallback(self, task_name: str):
        """Implement fallback strategy for repeatedly failing tasks"""
        #TODO - Implement this
        print(f"Task {task_name} has failed maximum retries")
        await self._notify_monitoring(task_name)
        
        if hasattr(self.tasks[task_name], 'fallback_mode'):
            await self.tasks[task_name].fallback_mode()

    async def _notify_monitoring(self, task_name: str):
        """Notify monitoring system of critical failures"""
        #TODO - Need to implement this, depends on monitoring system we decide to use
        # or if we migrate the validator monitoring to the validator monitoring here
        pass 