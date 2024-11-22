from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime
from gaia.database.database_manager import BaseDatabaseManager


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
            # Pass the required 'node_type' argument to the parent __new__ method
            cls._instance = super().__new__(cls, node_type="validator")
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
            # Initialize SQLAlchemy engine
            self.engine = create_async_engine(
                f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
            )
            self.async_session = sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )
            self._initialized = True

    async def get_connection(self):
        """
        Provide a database session/connection.
        """
        async with self.async_session() as session:
            yield session

    @BaseDatabaseManager.with_transaction
    async def initialize_database(self, session):
        """
        Initialize database tables and schemas for validator tasks.
        """
        # Create process queue table
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS process_queue (
                    id SERIAL PRIMARY KEY,
                    process_type VARCHAR(50) NOT NULL,
                    process_name VARCHAR(100) NOT NULL,
                    task_id INTEGER,
                    task_name VARCHAR(100),
                    priority INTEGER DEFAULT 0,
                    status VARCHAR(50) DEFAULT 'pending',
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

        # Add indexes
        await session.execute(
            text("CREATE INDEX IF NOT EXISTS idx_process_queue_status ON process_queue(status)")
        )
        await session.execute(
            text("CREATE INDEX IF NOT EXISTS idx_process_queue_priority ON process_queue(priority)")
        )

        # Load schemas
        task_schemas = await self.load_task_schemas()
        await self.initialize_task_tables(task_schemas)

    async def _create_task_table(self, schema: Dict[str, Any]):
        """
        Create a database table based on the provided schema.
        """
        # Build the CREATE TABLE query
        columns = [
            f"{col_name} {col_type}" for col_name, col_type in schema["columns"].items()
        ]
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema['table_name']} (
                {','.join(columns)}
            )
        """
        async with self.engine.connect() as conn:
            async with conn.begin():
                await conn.execute(text(create_table_query))
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

    async def load_task_schemas(self) -> Dict[str, Dict[str, Any]]:
        """
        Load database schemas for all tasks from their respective schema.json files.
        Searches through the defined_tasks directory for schema definitions.

        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping task names to their schema definitions
        """
        # Get the absolute path to the defined_tasks directory
        base_dir = Path(__file__).parent.parent.parent
        tasks_dir = base_dir / "tasks" / "defined_tasks"

        if not tasks_dir.exists():
            raise FileNotFoundError(f"Tasks directory not found at {tasks_dir}")

        schemas = {}

        # Loop through all subdirectories in the tasks directory
        for task_dir in tasks_dir.iterdir():
            if task_dir.is_dir():
                schema_file = task_dir / "schema.json"

                # Skip if no schema file exists
                if not schema_file.exists():
                    continue

                try:
                    # Load and validate the schema
                    with open(schema_file, "r") as f:
                        schema = json.load(f)

                    # Validate required schema components
                    if not all(key in schema for key in ["table_name", "columns"]):
                        raise ValueError(
                            f"Invalid schema in {schema_file}. "
                            "Must contain 'table_name' and 'columns'"
                        )

                    # Create the table for this task
                    await self._create_task_table(schema)

                    # Store the validated schema
                    schemas[task_dir.name] = schema

                except json.JSONDecodeError as e:
                    print(f"Error parsing schema.json in {task_dir.name}: {e}")
                except Exception as e:
                    print(f"Error processing schema for {task_dir.name}: {e}")

        return schemas
