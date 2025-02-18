from prefect.deployments import Deployment
from prefect.infrastructure.process import Process
from typing import List, Dict, Optional
import asyncio
import os
from .config import ScheduleConfig, get_schedule_config
from gaia.tasks.base.task import Task
from prefect.schedules.clocks import CronSchedule


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
        if task.name == "GeomagneticTask":
            deployment = await create_geomagnetic_deployment(task)
        else:
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

async def create_soil_moisture_deployments(task: Task) -> List[Deployment]:
    """Create deployments for soil moisture prep and execute windows."""
    deployments = []
    
    prep_config = get_schedule_config('soil_moisture_prep')
    for window in SOIL_MOISTURE_WINDOWS["prep"]:
        deployment = Deployment.build_from_flow(
            flow=task._handle_prep_window,
            name=f"soil_prep_{window['hour']:02d}{window['minute']:02d}",
            schedule=CronSchedule(
                cron=f"{window['minute']} {window['hour']} * * *",
                timezone="UTC"
            ),
            tags=["soil_moisture", "prep"],
            infrastructure=Process(
                env={"PYTHONPATH": os.getcwd()},
                working_dir=os.getcwd(),
                cpu_limit=prep_config.cpu_limit
            ),
            work_queue_name=prep_config.queue_name
        )
        deployments.append(deployment)
    
    exec_config = get_schedule_config('soil_moisture_execute')
    for window in SOIL_MOISTURE_WINDOWS["execute"]:
        deployment = Deployment.build_from_flow(
            flow=task._handle_execution_window,
            name=f"soil_execute_{window['hour']:02d}{window['minute']:02d}",
            schedule=CronSchedule(
                cron=f"{window['minute']} {window['hour']} * * *",
                timezone="UTC"
            ),
            tags=["soil_moisture", "execute"],
            infrastructure=Process(
                env={"PYTHONPATH": os.getcwd()},
                working_dir=os.getcwd(),
                cpu_limit=exec_config.cpu_limit
            ),
            work_queue_name=exec_config.queue_name
        )
        deployments.append(deployment)
    
    score_config = get_schedule_config('soil_moisture_scoring')
    score_deployment = Deployment.build_from_flow(
        flow=task.validator_score,
        name="soil_scoring",
        schedule=CronSchedule(cron="0 * * * *", timezone="UTC"),  # Every hour
        tags=["soil_moisture", "scoring"],
        infrastructure=Process(
            env={"PYTHONPATH": os.getcwd()},
            working_dir=os.getcwd(),
            cpu_limit=score_config.cpu_limit
        ),
        work_queue_name=score_config.queue_name
    )
    deployments.append(score_deployment)
    
    return deployments

async def create_geomagnetic_deployment(task: Task) -> Deployment:
    """Create deployment for geomagnetic validator task."""
    config = get_schedule_config('geomagnetic_task')
    
    deployment = Deployment.build_from_flow(
        flow=task.geo_validator_workflow,
        name="geomagnetic_validator",
        schedule=CronSchedule(cron=config.cron, timezone="UTC"),
        tags=["geomagnetic", "validator"],
        infrastructure=Process(
            env={
                "PREFECT_LOGGING_LEVEL": "INFO",
                "PYTHONPATH": os.getcwd()
            },
            working_dir=os.getcwd(),
            cpu_limit=config.cpu_limit,
            memory_limit=config.memory_limit
        ),
        work_queue_name=config.queue_name,
        work_pool_name="local-process-pool",
        retries=config.max_retries,
        retry_delay_seconds=int(config.retry_delay.total_seconds())
    )
    
    return deployment 