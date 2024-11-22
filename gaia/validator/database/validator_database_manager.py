import asyncpg
from gaia.database.database_manager import BaseDatabaseManager
from typing import Optional, List, Dict, Any
import os
import json
from pathlib import Path
from datetime import datetime
from sqlalchemy import text


class ValidatorDatabaseManager(BaseDatabaseManager):
    """
    Database manager specifically for validator nodes.
    Handles all validator-specific database operations.
    Implements singleton pattern to ensure only one database connection pool exists.
    """

    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, "validator")
        return cls._instance

    def __init__(
            self,
            database: str = "validator_db",
            host: str = "localhost",
            port: int = 5432,
            user: str = "postgres",
            password: str = "postgres",
    ):
        """
        Initialize the validator database manager (only once).
        """
        if not self._initialized:
            super().__init__(
                "validator",
                database=database,
                host=host,
                port=port,
                user=user,
                password=password,
            )
            self._initialized = True

    async def get_connection(self):
        """
        Provides a database connection for use within async context.
        Ensures connections are managed properly (opened/closed).
        """
        if self.engine is None:
            raise RuntimeError("Database engine is not initialized.")

        async with self.engine.connect() as connection:
            yield connection

    @BaseDatabaseManager.with_transaction
    async def initialize_database(self, session):
        """Initialize the queue tables and task-specific tables"""
        # Create process queue table
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS process_queue (
                    id SERIAL PRIMARY KEY,
                    process_type VARCHAR(50) NOT NULL,  -- 'network' or 'compute'
                    process_name VARCHAR(100) NOT NULL,
                    task_id INTEGER, -- id of the calling task
                    task_name VARCHAR(100), -- name of the calling task
                    priority INTEGER DEFAULT 0, 
                    status VARCHAR(50) DEFAULT 'pending', -- pending, processing, completed, failed 
                    payload BYTEA,
                    start_processing_time TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP WITH TIME ZONE,
                    completed_at TIMESTAMP WITH TIME ZONE,
                    complete_by TIMESTAMP WITH TIME ZONE,
                    expected_execution_time INTEGER,
                    execution_time INTEGER,
                    error TEXT,
                    retries INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3
                )
                """
            )
        )

        # Create indexes separately
        await session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_process_queue_status ON process_queue(status)"
            )
        )
        await session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_process_queue_priority ON process_queue(priority)"
            )
        )

        task_schemas = await self.load_task_schemas()
        await self.initialize_task_tables(task_schemas)

    ##### QUEUE TABLE FUNCTIONS #####
    async def add_to_queue(
            self,
            process_type: str,
            process_name: str,
            payload: bytes,
            task_id: Optional[int] = None,
            task_name: Optional[str] = None,
            priority: int = 0,
            complete_by: Optional[datetime] = None,
            expected_execution_time: Optional[int] = None,
            session: Optional[asyncpg.Connection] = None,
    ):
        """
        Add a process to the queue.

        Args:
            process_type (str): Type of process ('network' or 'compute')
            process_name (str): Name of the process
            payload (bytes): Binary data payload for the process
            task_id (Optional[int]): ID of the calling task if applicable
            task_name (Optional[str]): Name of the calling task
            priority (int): Priority level (higher = more priority)
            complete_by (Optional[datetime]): Deadline for task completion
            expected_execution_time (Optional[int]): Expected execution time in seconds
        """
        await session.execute(
            """
            INSERT INTO process_queue (
                process_type,
                process_name,
                payload,
                task_id,
                task_name,
                priority,
                complete_by,
                expected_execution_time
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            process_type,
            process_name,
            payload,
            task_id,
            task_name,
            priority,
            complete_by,
            expected_execution_time,
        )

    async def get_next_task(
            self, task_type: str = None, session: Optional[asyncpg.Connection] = None
    ):
        """Get the next task from the queue"""
        query = """
            UPDATE process_queue 
            SET status = 'processing', started_at = CURRENT_TIMESTAMP
            WHERE id = (
                SELECT id FROM process_queue
                WHERE status = 'pending'
                AND retries < max_retries
                {}
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
        """.format(
            "AND task_type = $1" if task_type else ""
        )

        return await session.fetchrow(query, task_type if task_type else None)

    async def complete_task(
            self,
            task_id: int,
            error: str = None,
            session: Optional[asyncpg.Connection] = None,
    ):
        """Mark a task as completed or failed"""
        if error:
            await session.execute(
                """
                UPDATE process_queue 
                SET status = 'failed',
                    completed_at = CURRENT_TIMESTAMP,
                    error = $2,
                    retries = retries + 1
                WHERE id = $1
                """,
                task_id,
                error,
            )
        else:
            await session.execute(
                """
                UPDATE process_queue 
                SET status = 'completed',
                    completed_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                task_id,
            )

    ##### LOAD TASK SPECIFIC TABLES #####
    async def load_task_schemas(self) -> Dict[str, Dict[str, Any]]:
        """
        Load database schemas for all tasks from their respective schema.json files.
        """
        try:
            tasks_dir = Path(__file__).parent.parent / "tasks" / "defined_tasks"
            schemas = {}

            for task_dir in tasks_dir.iterdir():
                if task_dir.is_dir():
                    schema_file = task_dir / "schema.json"
                    if not schema_file.exists():
                        continue

                    with open(schema_file, "r") as f:
                        schema = json.load(f)
                    if not all(key in schema for key in ["table_name", "columns"]):
                        raise ValueError(
                            f"Invalid schema in {schema_file}. "
                            "Must contain 'table_name' and 'columns'"
                        )

                    await self._create_task_table(schema)
                    schemas[task_dir.name] = schema
            return schemas

        except Exception as e:
            print(f"Error loading schemas: {e}")
            return {}

    async def _create_task_table(self, schema: Dict[str, Any]):
        """
        Create a database table for a task based on its schema definition.
        """
        columns = [
            f"{col_name} {col_type}" for col_name, col_type in schema["columns"].items()
        ]
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema['table_name']} (
                {','.join(columns)}
            )
        """
        async for conn in self.get_connection():
            async with conn.transaction():
                await conn.execute(create_table_query)

                if "indexes" in schema:
                    for index in schema["indexes"]:
                        await self.create_index(
                            schema["table_name"],
                            index["column"],
                            unique=index.get("unique", False),
                        )

    async def initialize_task_tables(self, task_schemas: Dict[str, Dict[str, Any]]):
        """Initialize validator-specific task tables."""
        for schema in task_schemas.values():
            if schema.get("database_type") in ["validator", "both"]:
                await self._create_task_table(schema)
