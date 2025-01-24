from datetime import timedelta
from prefect.schedules import IntervalSchedule, CronSchedule
from pydantic import BaseModel
from typing import Dict, Any, Optional

"""
Scheduling Configuration Module

This module defines the scheduling patterns and configurations for all tasks in the system.
It provides a centralized place to manage timing, intervals, and execution patterns.
"""

class ScheduleConfig(BaseModel):
    """Configuration for task scheduling"""
    interval: Optional[timedelta] = None
    cron: Optional[str] = None
    max_retries: int = 3
    retry_delay: timedelta = timedelta(minutes=5)
    timeout: timedelta = timedelta(hours=1)
    
    # Process resource limits
    cpu_limit: float = 1.0
    
    # Concurrency settings
    max_concurrent: int = 1
    queue_name: str = "default"

# Default configurations for different task types
DEFAULT_CONFIGS = {
    'data_collection': ScheduleConfig(
        interval=timedelta(hours=1),
        max_retries=5,
        timeout=timedelta(hours=2),
        cpu_limit=2.0,
    ),
    'scoring': ScheduleConfig(
        interval=timedelta(minutes=30),
        max_concurrent=3,
    ),
    'monitoring': ScheduleConfig(
        interval=timedelta(minutes=5),
    )
}

# Task-specific schedule configurations
TASK_SCHEDULES = {
    # Soil Moisture Task
    'soil_moisture': ScheduleConfig(
        interval=timedelta(hours=1),
        max_retries=3,
        cpu_limit=2.0,
        queue_name="data_processing"
    ),
    
    # Geomagnetic Task
    'geomagnetic': ScheduleConfig(
        cron="0 */1 * * *",  # Every hour
        max_retries=3,
        queue_name="data_processing"
    ),
    
    # Scoring and Weight Updates
    'weight_update': ScheduleConfig(
        interval=timedelta(minutes=30),
        max_concurrent=1,
        queue_name="scoring"
    ),
    
    # System Monitoring
    'system_monitor': ScheduleConfig(
        interval=timedelta(minutes=5),
        max_concurrent=1,
        queue_name="monitoring"
    )
}

def get_schedule_config(task_name: str) -> ScheduleConfig:
    """Get schedule configuration for a task"""
    if task_name in TASK_SCHEDULES:
        return TASK_SCHEDULES[task_name]
    
    # Try to match task type with default configs
    for task_type, config in DEFAULT_CONFIGS.items():
        if task_type in task_name:
            return config
    
    # Return base configuration if no specific match
    return ScheduleConfig() 