from datetime import timedelta
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
    memory_limit: str = "2G"
    
    # Concurrency settings
    max_concurrent: int = 1
    queue_name: str = "default"
    scoring_delay: Optional[timedelta] = None  # How long to wait before scoring
    prediction_window: Optional[timedelta] = None  # How far into future predictions are made
    block_interval: Optional[int] = None
    min_interval_blocks: Optional[int] = None
    chunk_size: Optional[int] = None
    batch_size: Optional[int] = None

# Default configurations
DEFAULT_CONFIGS = {
    'core': ScheduleConfig(
        max_retries=3,
        retry_delay=timedelta(minutes=1),
        timeout=timedelta(hours=24),
        cpu_limit=4.0,
        memory_limit="8G",
        queue_name="core"
    ),
    'processing': ScheduleConfig(
        max_retries=5,
        timeout=timedelta(hours=2),
        cpu_limit=2.0,
        memory_limit="4G",
        queue_name="processing"
    ),
    'monitoring': ScheduleConfig(
        interval=timedelta(minutes=5),
        max_retries=3,
        timeout=timedelta(minutes=30),
        cpu_limit=1.0,
        memory_limit="1G",
        queue_name="monitoring"
    )
}

# Task-specific schedule configurations
TASK_SCHEDULES = {
    'geomagnetic_task': ScheduleConfig(
        cron="0 * * * *",
        max_retries=3,
        retry_delay=timedelta(minutes=1),
        timeout=timedelta(minutes=30),
        cpu_limit=2.0,
        memory_limit="4G",
        queue_name="data_processing",
        chunk_size=32,
        batch_size=10
    ),
    
    'soil_moisture_task': ScheduleConfig(
        interval=timedelta(hours=1),
        max_retries=3,
        retry_delay=timedelta(minutes=5),
        timeout=timedelta(hours=2),
        cpu_limit=2.0,
        memory_limit="4G",
        queue_name="data_processing",
        chunk_size=32,
        scoring_delay=timedelta(days=3)
    ),
    
    'validator_core_flow': ScheduleConfig(
        max_retries=3,
        timeout=timedelta(hours=24),
        cpu_limit=4.0,
        memory_limit="8G",
        queue_name="core"
    ),
    
    'scoring_flow': ScheduleConfig(
        block_interval=12,
        min_interval_blocks=100,  # Minimum blocks between weight sets
        max_retries=3,
        timeout=timedelta(minutes=15),
        cpu_limit=2.0,
        memory_limit="4G",
        queue_name="scoring"
    ),
    
    'task_processing_flow': ScheduleConfig(
        max_retries=5,
        timeout=timedelta(hours=2),
        cpu_limit=2.0,
        memory_limit="4G",
        queue_name="processing",
        chunk_size=32
    ),
    
    'monitoring_flow': ScheduleConfig(
        interval=timedelta(minutes=5),
        max_retries=3,
        timeout=timedelta(minutes=30),
        cpu_limit=1.0,
        memory_limit="1G",
        queue_name="monitoring"
    ),
    
    'miner_query_flow': ScheduleConfig(
        max_retries=3,
        retry_delay=timedelta(seconds=30),
        timeout=timedelta(minutes=10),
        cpu_limit=2.0,
        memory_limit="4G",
        queue_name="processing",
        chunk_size=32,
        batch_size=10
    ),
    
    'soil_moisture_prep': ScheduleConfig(
        max_retries=3,
        retry_delay=timedelta(minutes=5),
        timeout=timedelta(minutes=30),
        cpu_limit=2.0,
        memory_limit="4G",
        queue_name="soil_moisture",
        prediction_window=timedelta(hours=6),
        scoring_delay=timedelta(days=3)
    ),
    
    'soil_moisture_execute': ScheduleConfig(
        max_retries=3,
        retry_delay=timedelta(minutes=5),
        timeout=timedelta(minutes=30),
        cpu_limit=2.0,
        memory_limit="4G",
        queue_name="soil_moisture"
    ),
    
    'soil_moisture_scoring': ScheduleConfig(
        interval=timedelta(hours=1),  # Check every hour for scoreable predictions
        max_retries=3,
        retry_delay=timedelta(minutes=5),
        timeout=timedelta(hours=1),
        cpu_limit=2.0,
        memory_limit="4G",
        queue_name="soil_moisture"
    )
}

def get_schedule_config(name: str, flow_type: str = None) -> ScheduleConfig:
    """Get schedule configuration for a flow or task
    
    Args:
        name: Name of the flow or task
        flow_type: Type of flow (core, processing, monitoring, etc.)
    """
    if name in TASK_SCHEDULES:
        return TASK_SCHEDULES[name]
    
    if flow_type and flow_type in DEFAULT_CONFIGS:
        return DEFAULT_CONFIGS[flow_type]
    
    return ScheduleConfig()

# Soil moisture window definitions
SOIL_MOISTURE_WINDOWS = {
    "prep": [
        {"hour": 1, "minute": 30},  # 1:30 UTC
        {"hour": 9, "minute": 30},  # 9:30 UTC
        {"hour": 13, "minute": 30}, # 13:30 UTC
        {"hour": 19, "minute": 30}, # 19:30 UTC
    ],
    "execute": [
        {"hour": 2, "minute": 0},   # 2:00 UTC
        {"hour": 10, "minute": 0},  # 10:00 UTC
        {"hour": 14, "minute": 0},  # 14:00 UTC
        {"hour": 20, "minute": 0},  # 20:00 UTC
    ]
} 