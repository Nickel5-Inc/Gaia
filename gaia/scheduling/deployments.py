from prefect.deployments import Deployment
from prefect.infrastructure.process import Process
from typing import List, Dict, Optional
import asyncio
import os
from .config import ScheduleConfig, get_schedule_config
from gaia.tasks.base.task import Task


"""
Task Deployment Module

Handles the creation and management of Prefect deployments for tasks.
Provides utilities for:
1. Creating individual deployments
2. Deploying multiple tasks in local process mode
3. Managing deployment infrastructure
4. Handling deployment updates
"""

async def create_deployment(
    task: Task,
    config: Optional[ScheduleConfig] = None
) -> Deployment:
    """
    Create a local process deployment for a task
    """
    config = config or get_schedule_config(task.name)
    
    # Local process execution
    infrastructure = Process(
        env={
            "PREFECT_LOGGING_LEVEL": "INFO",
            "PYTHONPATH": os.getcwd()  # Ensure imports work
        },
        working_dir=os.getcwd(),
        cpu_limit=config.cpu_limit
    )

    deployment = Deployment.build_from_flow(
        flow=task.run,
        name=f"{task.name}_deployment",
        version=task.metadata.version,
        tags=[task.name, "gaia", "validator"],
        infrastructure=infrastructure,
        work_queue_name=config.queue_name,
        work_pool_name="local-process-pool"
    )
    
    return deployment

async def deploy_all_tasks(tasks: List[Task]) -> Dict[str, Deployment]:
    """
    Deploy multiple tasks in parallel using local process execution
    """
    deployments = {}
    
    async def deploy_task(task: Task):
        deployment = await create_deployment(task)
        await deployment.apply()
        return task.name, deployment
    
    results = await asyncio.gather(
        *[deploy_task(task) for task in tasks],
        return_exceptions=True
    )
    
    for task_name, deployment in results:
        if isinstance(deployment, Exception):
            print(f"Failed to deploy {task_name}: {str(deployment)}")
            continue
        deployments[task_name] = deployment
    
    return deployments

async def update_deployment(
    deployment: Deployment,
    config: ScheduleConfig
) -> Deployment:
    """
    Update an existing deployment with new configuration
    """
    await deployment.pause()
    
    # Create new deployment with updated config
    new_deployment = Deployment.build_from_flow(
        flow=deployment.flow,
        name=deployment.name,
        version=deployment.version,
        tags=deployment.tags,
        infrastructure=Process(
            env={
                "PREFECT_LOGGING_LEVEL": "INFO",
                "PYTHONPATH": os.getcwd()
            },
            working_dir=os.getcwd(),
            cpu_limit=config.cpu_limit
        ),
        work_queue_name=config.queue_name,
        work_pool_name="local-process-pool"
    )
    
    # Apply new deployment
    await new_deployment.apply()
    
    return new_deployment

def get_deployment_status(deployment: Deployment) -> Dict:
    """
    Get current status of a deployment
    """
    return {
        'name': deployment.name,
        'status': deployment.status(),
        'last_updated': deployment.updated,
        'schedule_active': deployment.is_schedule_active,
        'infrastructure': deployment.infrastructure.dict(),
        'work_queue': deployment.work_queue_name
    } 