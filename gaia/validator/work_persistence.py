from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
from sqlalchemy import select, update, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from gaia.validator.database.database_manager import ValidatorDatabaseManager
from gaia.validator.database.models import TaskState
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class WorkPersistenceManager:
    """Manages persistence of task execution state using SQLAlchemy ORM."""

    def __init__(self, db_manager: ValidatorDatabaseManager):
        self.db_manager = db_manager

    async def initialize(self):
        """Initialize work persistence - no longer needed as migrations handle this."""
        pass  # Schema is handled by SQLAlchemy models and migrations

    async def register_task(self, flow_name: str, task_name: str, metadata: Optional[Dict[str, Any]] = None) -> int:
        """Register a new task and return its ID."""
        async with self.db_manager.get_connection() as session:
            task = TaskState(
                flow_name=flow_name,
                task_name=task_name,
                status='pending',
                metadata=metadata or {}
            )
            session.add(task)
            await session.commit()
            await session.refresh(task)  # Refresh to get the generated ID
            return task.id

    async def update_task_status(self, task_id: int, status: str, error_message: Optional[str] = None):
        """Update task status and error message."""
        async with self.db_manager.get_connection() as session:
            stmt = (
                update(TaskState)
                .where(TaskState.id == task_id)
                .values(
                    status=status,
                    error_message=error_message,
                    updated_at=datetime.utcnow()
                )
            )
            await session.execute(stmt)
            await session.commit()

    async def get_incomplete_tasks(self, flow_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all incomplete tasks, optionally filtered by flow name."""
        async with self.db_manager.get_connection() as session:
            query = (
                select(TaskState)
                .where(TaskState.status.in_(['pending', 'running']))
            )
            
            if flow_name:
                query = query.where(TaskState.flow_name == flow_name)
            
            result = await session.execute(query)
            tasks = result.scalars().all()
            
            return [
                {
                    'task_id': task.id,
                    'flow_name': task.flow_name,
                    'task_name': task.task_name,
                    'status': task.status,
                    'error_message': task.error_message,
                    'metadata': task.metadata,
                    'created_at': task.created_at,
                    'updated_at': task.updated_at
                }
                for task in tasks
            ]

    async def cleanup_stale_tasks(self, hours: int = 24):
        """Clean up tasks that have been running for too long."""
        stale_time = datetime.utcnow() - timedelta(hours=hours)
        
        async with self.db_manager.get_connection() as session:
            stmt = (
                update(TaskState)
                .where(and_(
                    TaskState.status == 'running',
                    TaskState.updated_at < stale_time
                ))
                .values(
                    status='failed',
                    error_message='Task timed out',
                    updated_at=datetime.utcnow()
                )
            )
            await session.execute(stmt)
            await session.commit()

    async def get_task_history(
        self,
        flow_name: Optional[str] = None,
        task_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get task execution history with optional filters."""
        async with self.db_manager.get_connection() as session:
            query = select(TaskState)
            
            if flow_name:
                query = query.where(TaskState.flow_name == flow_name)
            if task_name:
                query = query.where(TaskState.task_name == task_name)
            if status:
                query = query.where(TaskState.status == status)
            
            query = query.order_by(TaskState.created_at.desc()).limit(limit)
            
            result = await session.execute(query)
            tasks = result.scalars().all()
            
            return [
                {
                    'task_id': task.id,
                    'flow_name': task.flow_name,
                    'task_name': task.task_name,
                    'status': task.status,
                    'error_message': task.error_message,
                    'metadata': task.metadata,
                    'created_at': task.created_at,
                    'updated_at': task.updated_at
                }
                for task in tasks
            ]

    async def get_task_stats(self, time_window_hours: int = 24) -> List[Dict[str, Any]]:
        """Get task execution statistics within time window."""
        async with self.db_manager.get_connection() as session:
            time_cutoff = datetime.utcnow() - timedelta(hours=time_window_hours)
            
            query = (
                select(
                    TaskState.flow_name,
                    TaskState.task_name,
                    TaskState.status,
                    func.count().label('count'),
                    func.avg(
                        func.extract('epoch', TaskState.updated_at - TaskState.created_at)
                    ).label('avg_duration')
                )
                .where(TaskState.created_at > time_cutoff)
                .group_by(TaskState.flow_name, TaskState.task_name, TaskState.status)
            )
            
            result = await session.execute(query)
            return [
                {
                    'flow_name': row.flow_name,
                    'task_name': row.task_name,
                    'status': row.status,
                    'count': row.count,
                    'avg_duration': row.avg_duration
                }
                for row in result
            ]

    @asynccontextmanager
    async def task_context(self, flow_name: str, task_name: str, metadata: Optional[Dict[str, Any]] = None):
        """Context manager for task execution with automatic status updates."""
        task_id = await self.register_task(flow_name, task_name, metadata)
        try:
            await self.update_task_status(task_id, "running")
            yield task_id
            await self.update_task_status(task_id, "completed")
        except Exception as e:
            await self.update_task_status(task_id, "failed", str(e))
            raise 