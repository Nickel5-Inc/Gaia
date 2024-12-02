from gaia.database.database_manager import BaseDatabaseManager
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy import text
import json
from pathlib import Path


class MinerDatabaseManager(BaseDatabaseManager):
    """
    Database manager specifically for miner nodes.
    Handles all miner-specific database operations.
    Implements singleton pattern to ensure only one database connection pool exists.
    """

    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, "miner")
        return cls._instance

    def __init__(
        self,
        database: str = "miner_db",
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "postgres",
    ):
        """
        Initialize the miner database manager (only once).
        """
        if not self._initialized:
            super().__init__(
                node_type="miner",
                database=database,
                host=host,
                port=port,
                user=user,
                password=password,
            )
            self._initialized = True

    @BaseDatabaseManager.with_transaction
    async def initialize_database(self, session):
        """
        Initialize the miner database with queue and task-specific tables.
        """
        # Create miner task queue table
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS task_queue (
                    id SERIAL PRIMARY KEY,
                    task_name VARCHAR(100) NOT NULL,
                    miner_id VARCHAR(100),
                    predicted_value JSONB,
                    status VARCHAR(50) DEFAULT 'pending',
                    query_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    retries INTEGER DEFAULT 0
                )
                """
            )
        )

        # Create indexes for faster queries
        await session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_task_queue_status ON task_queue(status)"
            )
        )
        await session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_task_queue_query_time ON task_queue(query_time)"
            )
        )

        # Load and initialize task schemas
        task_schemas = await self.load_task_schemas()
        await self.initialize_task_tables(task_schemas)

    async def add_to_queue(
        self,
        task_name: str,
        miner_id: str,
        predicted_value: dict,
        query_time: datetime,
        retries: int = 0,
        status: str = "pending",
    ):
        """
        Add a task to the miner's task queue.

        Args:
            task_name (str): Name of the task.
            miner_id (str): ID of the miner submitting the task.
            predicted_value (dict): JSON object containing the prediction.
            query_time (datetime): Timestamp for when the prediction was made.
            retries (int): Number of retries for the task (default is 0).
            status (str): Current status of the task (default is "pending").
        """
        query = """
        INSERT INTO task_queue (task_name, miner_id, predicted_value, query_time, retries, status)
        VALUES (:task_name, :miner_id, :predicted_value, :query_time, :retries, :status)
        """
        params = {
            "task_name": task_name,
            "miner_id": miner_id,
            "predicted_value": json.dumps(predicted_value),
            "query_time": query_time,
            "retries": retries,
            "status": status,
        }

        async with self.get_connection() as conn:
            await conn.execute(text(query), params)

    async def get_next_task(self, status: str = "pending") -> Optional[Dict[str, Any]]:
        """
        Fetch the next task from the queue.

        Args:
            status (str): Status of the tasks to fetch (default is "pending").

        Returns:
            Optional[Dict[str, Any]]: The next task in the queue.
        """
        query = """
        SELECT * FROM task_queue
        WHERE status = :status
        ORDER BY query_time ASC
        LIMIT 1
        """
        params = {"status": status}

        async with self.get_connection() as conn:
            result = await conn.execute(text(query), params)
            row = await result.fetchone()
            return dict(row._mapping) if row else None

    async def update_task_status(self, task_id: int, status: str, retries: int = 0):
        """
        Update the status and retry count of a task.

        Args:
            task_id (int): ID of the task to update.
            status (str): New status for the task.
            retries (int): Retry count (default is 0).
        """
        query = """
        UPDATE task_queue
        SET status = :status, retries = :retries
        WHERE id = :task_id
        """
        params = {"status": status, "retries": retries, "task_id": task_id}

        async with self.get_connection() as conn:
            await conn.execute(text(query), params)

    async def load_task_schemas(self) -> Dict[str, Dict[str, Any]]:
        """
        Load task schemas for miner tasks from `schema.json`.

        Returns:
            Dict[str, Dict[str, Any]]: Schema definitions for miner tasks.
        """
        schemas = {}
        tasks_dir = Path(__file__).parent.parent / "tasks" / "defined_tasks"

        for task_dir in tasks_dir.iterdir():
            if task_dir.is_dir():
                schema_file = task_dir / "schema.json"
                if schema_file.exists():
                    with open(schema_file, "r") as f:
                        schema = json.load(f)
                        if "table_name" in schema and "columns" in schema:
                            schemas[task_dir.name] = schema
                            await self._create_task_table(schema)
        return schemas

    async def _create_task_table(self, schema: Dict[str, Any]):
        """
        Create a table for miner tasks based on the schema.

        Args:
            schema (Dict[str, Any]): Schema definition.
        """
        columns = [
            f"{name} {definition}" for name, definition in schema["columns"].items()
        ]
        query = f"""
        CREATE TABLE IF NOT EXISTS {schema['table_name']} (
            {', '.join(columns)}
        )
        """
        async with self.get_connection() as conn:
            await conn.execute(text(query))
