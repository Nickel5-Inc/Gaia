from gaia.database.database_manager import BaseDatabaseManager, DatabaseError
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy import text
import json
from pathlib import Path
from fiber.logging_utils import get_logger
from functools import wraps
import time

logger = get_logger(__name__)

def track_operation(operation_type: str):
    """Decorator to track database operations."""
    def decorator(func):
        @wraps(func)
        async def wrapper(self: 'MinerDatabaseManager', *args, **kwargs):
            try:
                start_time = time.time()
                result = await func(self, *args, **kwargs)
                duration = time.time() - start_time
                
                # Track operation
                self._operation_stats[f'{operation_type}_operations'] += 1
                
                # Track long-running queries
                if duration > self.MINER_QUERY_TIMEOUT / 2:
                    self._operation_stats['long_running_queries'].append({
                        'operation': func.__name__,
                        'duration': duration,
                        'timestamp': start_time
                    })
                    logger.warning(f"Long-running {operation_type} operation detected: {duration:.2f}s")
                
                return result
            except Exception as e:
                logger.error(f"Error in {operation_type} operation: {str(e)}")
                raise
        return wrapper
    return decorator

class MinerDatabaseManager(BaseDatabaseManager):
    """
    Database manager specifically for miner nodes.
    Handles all miner-specific database operations.
    Implements singleton pattern to ensure only one database connection pool exists.
    """

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls, node_type="miner")
        return cls._instance

    def __init__(
        self,
        database: str = "miner_db",
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "postgres",
    ):
        """Initialize the miner database manager (only once)."""
        if not hasattr(self, '_initialized'):
            super().__init__(
                node_type="miner",
                database=database,
                host=host,
                port=port,
                user=user,
                password=password,
            )
            # Operation statistics for monitoring
            self._operation_stats = {
                'ddl_operations': 0,
                'read_operations': 0,
                'write_operations': 0,
                'long_running_queries': []
            }
            
            # Custom timeouts for miner operations
            self.MINER_QUERY_TIMEOUT = 60  # 1 minute
            self.MINER_TRANSACTION_TIMEOUT = 300  # 5 minutes
            
            self._engine = None
            self._engine_initialized = False
            self._initialized = True

    async def get_operation_stats(self) -> Dict[str, Any]:
        """Get current operation statistics."""
        stats = self._operation_stats.copy()
        stats.update({
            'active_sessions': len(self._active_sessions),
            'active_operations': self._active_operations,
            'pool_health': self._pool_health_status,
            'circuit_breaker_status': self._circuit_breaker['status']
        })
        return stats

    @track_operation('ddl')
    async def initialize_database(self):
        """Initialize the miner database with queue and task-specific tables."""
        try:
            # Create miner task queue table
            await self.execute(
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

            # Create indexes for faster queries
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_task_queue_status ON task_queue(status)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_task_queue_query_time ON task_queue(query_time)"
            )

            # Load and initialize task schemas
            task_schemas = await self.load_task_schemas()
            await self.initialize_task_tables(task_schemas)
            logger.info("Successfully initialized miner database")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise DatabaseError(f"Failed to initialize database: {str(e)}")

    @track_operation('write')
    async def add_to_queue(
        self,
        task_name: str,
        miner_id: str,
        predicted_value: dict,
        query_time: datetime,
        retries: int = 0,
        status: str = "pending",
    ):
        """Add a task to the miner's task queue."""
        try:
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
            await self.execute(query, params)
        except Exception as e:
            logger.error(f"Error adding task to queue: {str(e)}")
            raise DatabaseError(f"Failed to add task to queue: {str(e)}")

    @track_operation('read')
    async def get_next_task(self, status: str = "pending") -> Optional[Dict[str, Any]]:
        """Fetch the next task from the queue."""
        try:
            query = """
            SELECT * FROM task_queue
            WHERE status = :status
            ORDER BY query_time ASC
            LIMIT 1
            """
            return await self.fetch_one(query, {"status": status})
        except Exception as e:
            logger.error(f"Error getting next task: {str(e)}")
            raise DatabaseError(f"Failed to get next task: {str(e)}")

    @track_operation('write')
    async def update_task_status(self, task_id: int, status: str, retries: int = 0):
        """Update the status and retry count of a task."""
        try:
            query = """
            UPDATE task_queue
            SET status = :status, retries = :retries
            WHERE id = :task_id
            """
            await self.execute(query, {
                "status": status,
                "retries": retries,
                "task_id": task_id
            })
        except Exception as e:
            logger.error(f"Error updating task status: {str(e)}")
            raise DatabaseError(f"Failed to update task status: {str(e)}")

    @track_operation('ddl')
    async def _create_task_table(self, schema: Dict[str, Any]):
        """Create a table for miner tasks based on the schema."""
        try:
            columns = [
                f"{name} {definition}" for name, definition in schema["columns"].items()
            ]
            
            # Add foreign key constraints if specified
            if 'foreign_keys' in schema:
                for fk in schema['foreign_keys']:
                    fk_def = f"FOREIGN KEY ({fk['column']}) REFERENCES {fk['references']}"
                    if 'on_delete' in fk:
                        fk_def += f" ON DELETE {fk['on_delete']}"
                    columns.append(fk_def)
            
            query = f"""
            CREATE TABLE IF NOT EXISTS {schema['table_name']} (
                {', '.join(columns)}
            )
            """
            await self.execute(query)
            
            # Create indexes if specified
            if 'indexes' in schema:
                for index in schema['indexes']:
                    index_name = f"{schema['table_name']}_{index['column']}_idx"
                    unique = "UNIQUE" if index.get('unique', False) else ""
                    create_index_sql = f"""
                        CREATE INDEX IF NOT EXISTS {index_name}
                        ON {schema['table_name']} ({index['column']})
                        {unique};
                    """
                    await self.execute(create_index_sql)
                    
        except Exception as e:
            logger.error(f"Error creating task table: {str(e)}")
            raise DatabaseError(f"Failed to create task table: {str(e)}")

    @track_operation('ddl')
    async def initialize_task_tables(self, task_schemas: Dict[str, Dict[str, Any]]):
        """Initialize miner-specific task tables."""
        for schema_name, schema in task_schemas.items():
            try:
                if isinstance(schema, dict) and 'table_name' not in schema:
                    for table_name, table_schema in schema.items():
                        if isinstance(table_schema, dict) and table_schema.get("database_type") in ["miner", "both"]:
                            await self._create_task_table(table_schema)
                else:
                    if schema.get("database_type") in ["miner", "both"]:
                        await self._create_task_table(schema)
            except Exception as e:
                logger.error(f"Error initializing table for schema {schema_name}: {e}")
                raise DatabaseError(f"Failed to initialize table for schema {schema_name}: {str(e)}")

    async def load_task_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Load task schemas for miner tasks from schema.json files."""
        try:
            schemas = {}
            base_dir = Path(__file__).parent.parent.parent
            tasks_dir = base_dir / "tasks" / "defined_tasks"

            if not tasks_dir.exists():
                raise FileNotFoundError(f"Tasks directory not found at {tasks_dir}")

            for task_dir in tasks_dir.iterdir():
                if task_dir.is_dir():
                    schema_file = task_dir / "schema.json"
                    if not schema_file.exists():
                        continue

                    try:
                        with open(schema_file, "r") as f:
                            schema = json.load(f)
                            
                        if isinstance(schema, dict):
                            if "table_name" in schema:
                                if not all(key in schema for key in ["table_name", "columns"]):
                                    raise ValueError(
                                        f"Invalid schema in {schema_file}. "
                                        "Must contain 'table_name' and 'columns'"
                                    )
                            else:
                                for table_name, table_schema in schema.items():
                                    if not all(
                                        key in table_schema for key in ["table_name", "columns"]
                                    ):
                                        raise ValueError(
                                            f"Invalid table schema for {table_name} in {schema_file}. "
                                            "Must contain 'table_name' and 'columns'"
                                        )

                            schemas[task_dir.name] = schema
                        else:
                            raise ValueError(f"Invalid schema format in {schema_file}")

                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing schema.json in {task_dir.name}: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing schema for {task_dir.name}: {e}")
                        continue

            return schemas
        except Exception as e:
            logger.error(f"Error loading task schemas: {str(e)}")
            raise DatabaseError(f"Failed to load task schemas: {str(e)}")

    async def close_all_connections(self):
        """Close all database connections gracefully."""
        try:
            logger.info("Closing all database connections...")
            await self.close()  # Using the base class close method
            
            # Clear operation stats
            self._operation_stats = {
                'ddl_operations': 0,
                'read_operations': 0,
                'write_operations': 0,
                'long_running_queries': []
            }
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")
            raise DatabaseError(f"Failed to close database connections: {str(e)}")
