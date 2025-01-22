import traceback
import gc
import numpy as np
from sqlalchemy import text
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, Optional, List, Callable, TypeVar
from datetime import datetime, timedelta, timezone
from gaia.database.database_manager import BaseDatabaseManager, DatabaseError
from fiber.logging_utils import get_logger
import random
import time
from functools import wraps
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

logger = get_logger(__name__)

T = TypeVar('T')

def track_operation(operation_type: str):
    """Decorator to track database operations."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(self: 'ValidatorDatabaseManager', *args, **kwargs) -> T:
            try:
                start_time = time.time()
                result = await func(self, *args, **kwargs)
                duration = time.time() - start_time
                
                # Track operation
                self._operation_stats[f'{operation_type}_operations'] += 1
                
                # Track long-running queries
                if duration > self.VALIDATOR_QUERY_TIMEOUT / 2:
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

class ValidatorDatabaseManager(BaseDatabaseManager):
    """
    Database manager specifically for validator nodes.
    Handles all validator-specific database operations.
    """
    
    def __new__(cls, *args, **kwargs) -> 'ValidatorDatabaseManager':
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls, node_type="validator")
            cls._instance._initialized = False
            
            # Initialize all required base class attributes
            cls._instance._circuit_breaker = {
                'failures': 0,
                'last_failure_time': 0,
                'status': 'closed'  # 'closed', 'open', 'half-open'
            }
            
            # Connection management
            cls._instance._active_sessions = set()
            cls._instance._active_operations = 0
            cls._instance._operation_lock = asyncio.Lock()
            cls._instance._session_lock = asyncio.Lock()
            cls._instance._pool_semaphore = asyncio.Semaphore(cls.MAX_CONNECTIONS)
            
            # Pool health monitoring
            cls._instance._last_pool_check = 0
            cls._instance._pool_health_status = True
            cls._instance._pool_recovery_lock = asyncio.Lock()
            
            # Resource monitoring
            cls._instance._resource_stats = {
                'cpu_percent': 0,
                'memory_rss': 0,
                'open_files': 0,
                'connections': 0,
                'last_check': 0
            }
            
            # Operation statistics
            cls._instance._operation_stats = {
                'ddl_operations': 0,
                'read_operations': 0,
                'write_operations': 0,
                'long_running_queries': []
            }
            
            # Initialize engine placeholders
            cls._instance._engine = None
            cls._instance._session_factory = None
            
            # Initialize database connection parameters with defaults
            cls._instance.db_url = None
            cls._instance.VALIDATOR_QUERY_TIMEOUT = 60  # 1 minute
            cls._instance.VALIDATOR_TRANSACTION_TIMEOUT = 300  # 5 minutes
            
        return cls._instance

    def __init__(
        self,
        database: str = "validator_db",
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "postgres",
    ) -> None:
        """Initialize the validator database manager."""
        if not hasattr(self, '_initialized') or not self._initialized:
            # Call base class init first to set up necessary attributes
            super().__init__(
                node_type="validator",
                database=database,
                host=host,
                port=port,
                user=user,
                password=password,
            )
            
            # Set database URL
            self.db_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
            
            # Custom timeouts for validator operations
            self.VALIDATOR_QUERY_TIMEOUT = 60  # 1 minute
            self.VALIDATOR_TRANSACTION_TIMEOUT = 300  # 5 minutes
            
            self._initialized = True

    async def _initialize_validator_database(self) -> None:
        """Initialize validator-specific database schema and columns."""
        try:
            schema_path = Path(__file__).parent.parent.parent / "tasks" / "defined_tasks" / "soilmoisture" / "schema.json"
            with open(schema_path) as f:
                schema = json.load(f)

            for table_name, table_schema in schema.items():
                if table_schema.get("database_type") != "validator":
                    continue

                exists_query = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = :table_name
                    )
                """
                result = await self.fetch_one(exists_query, {"table_name": table_name})
                table_exists = result["exists"] if result else False

                if not table_exists:
                    columns = []
                    for col_name, col_type in table_schema["columns"].items():
                        columns.append(f"{col_name} {col_type}")

                    if "foreign_keys" in table_schema:
                        for fk in table_schema["foreign_keys"]:
                            fk_def = f"FOREIGN KEY ({fk['column']}) REFERENCES {fk['references']}"
                            if "on_delete" in fk:
                                fk_def += f" ON DELETE {fk['on_delete']}"
                            columns.append(fk_def)

                    create_table_sql = f"""
                        CREATE TABLE IF NOT EXISTS {table_schema['table_name']} (
                            {', '.join(columns)}
                        );
                    """
                    await self.execute(create_table_sql)
                    logger.info(f"Created table {table_name}")

                    if "indexes" in table_schema:
                        for index in table_schema["indexes"]:
                            index_name = f"{table_name}_{index['column']}_idx"
                            unique = "UNIQUE" if index.get("unique", False) else ""
                            create_index_sql = f"""
                                CREATE INDEX IF NOT EXISTS {index_name}
                                ON {table_name} ({index['column']})
                                {unique};
                            """
                            await self.execute(create_index_sql)
                        logger.info(f"Created indexes for {table_name}")
                else:
                    for col_name, col_type in table_schema["columns"].items():
                        check_column_sql = """
                            SELECT EXISTS (
                                SELECT 1 
                                FROM information_schema.columns 
                                WHERE table_name = :table_name 
                                AND column_name = :column_name
                            )
                        """
                        result = await self.fetch_one(
                            check_column_sql, 
                            {"table_name": table_name, "column_name": col_name}
                        )
                        column_exists = result["exists"] if result else False

                        if not column_exists:
                            add_column_sql = f"""
                                ALTER TABLE {table_name} 
                                ADD COLUMN {col_name} {col_type};
                            """
                            await self.execute(add_column_sql)
                            logger.info(f"Added column {col_name} to {table_name}")

            logger.info("Validator database initialization completed")
            
        except Exception as e:
            logger.error(f"Error initializing validator database: {str(e)}")
            logger.error(traceback.format_exc())
            raise DatabaseError(f"Failed to initialize validator database: {str(e)}")

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

    async def _initialize_engine(self) -> None:
        """Initialize database engine and session factory with validator-specific settings."""
        try:
            if not self.db_url:
                raise DatabaseError("Database URL not initialized")
            
            self._engine = create_async_engine(
                self.db_url,
                pool_size=self.MAX_CONNECTIONS,
                max_overflow=10,
                pool_timeout=self.DEFAULT_CONNECTION_TIMEOUT,
                pool_recycle=300,  # Recycle connections every 5 minutes
                pool_pre_ping=True,  # Enable connection health checks
                echo=False,  # Set to True for SQL query logging
                connect_args={
                    "command_timeout": self.VALIDATOR_QUERY_TIMEOUT,
                    "timeout": self.DEFAULT_CONNECTION_TIMEOUT,
                }
            )
            
            self._session_factory = async_sessionmaker(
                self._engine,
                expire_on_commit=False,
                class_=AsyncSession,
                autobegin=False  # Prevent automatic transaction creation
            )
            
            # Test the connection
            async with self._engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            
            logger.info(f"Successfully initialized database engine for {self.node_type}")
        except Exception as e:
            logger.error(f"Failed to initialize database engine: {str(e)}")
            raise DatabaseError(f"Failed to initialize database engine: {str(e)}")

    @track_operation('ddl')
    @BaseDatabaseManager.with_transaction()
    async def initialize_database(self, session: AsyncSession):
        """Initialize database tables and schemas for validator tasks."""
        try:
            # Initialize engine first
            await self._initialize_engine()
            
            # Create core tables
            await self._create_node_table(session)
            await self._create_trigger_function(session)
            await self._create_trigger(session)
            await self._initialize_rows(session)
            await self.create_score_table(session)
            
            logger.info("Successfully created core tables")
            
            # Initialize task tables
            task_schemas = await self.load_task_schemas()
            await self.initialize_task_tables(task_schemas, session)
            logger.info("Successfully initialized task tables")
            
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise DatabaseError(f"Failed to initialize database: {str(e)}")

    @track_operation('ddl')
    async def _create_task_table(self, schema: Dict[str, Any], table_name: str, session: AsyncSession) -> None:
        """Create a table for a specific task using the provided schema."""
        try:
            table_schema = schema.get(table_name, schema)
            if not isinstance(table_schema, dict) or 'columns' not in table_schema:
                logger.error(f"Invalid schema format for table {table_name}")
                return

            columns = []
            for col_name, col_type in table_schema['columns'].items():
                columns.append(f"{col_name} {col_type}")

            # Add foreign key constraints if specified
            if 'foreign_keys' in table_schema:
                for fk in table_schema['foreign_keys']:
                    fk_def = f"FOREIGN KEY ({fk['column']}) REFERENCES {fk['references']}"
                    if 'on_delete' in fk:
                        fk_def += f" ON DELETE {fk['on_delete']}"
                    columns.append(fk_def)

            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_schema['table_name']} (
                    {', '.join(columns)}
                );
            """
            await self.execute(create_table_sql, session=session)

            if 'indexes' in table_schema:
                for index in table_schema['indexes']:
                    index_name = f"{table_schema['table_name']}_{index['column']}_idx"
                    unique = "UNIQUE" if index.get('unique', False) else ""
                    create_index_sql = f"""
                        CREATE INDEX IF NOT EXISTS {index_name}
                        ON {table_schema['table_name']} ({index['column']})
                        {unique};
                    """
                    await self.execute(create_index_sql, session=session)

            logger.info(f"Successfully created table {table_schema['table_name']} with indexes")

        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            raise DatabaseError(f"Failed to create table {table_name}: {str(e)}")

    @track_operation('ddl')
    async def initialize_task_tables(self, task_schemas: Dict[str, Dict[str, Any]], session: AsyncSession):
        """Initialize validator-specific task tables."""
        for schema_name, schema in task_schemas.items():
            try:
                if isinstance(schema, dict) and 'table_name' not in schema:
                    for table_name, table_schema in schema.items():
                        if isinstance(table_schema, dict) and table_schema.get("database_type") in ["validator", "both"]:
                            await self._create_task_table(schema, table_name, session)
                else:
                    if schema.get("database_type") in ["validator", "both"]:
                        await self._create_task_table({schema_name: schema}, schema_name, session)
            except Exception as e:
                logger.error(f"Error initializing table for schema {schema_name}: {e}")
                raise DatabaseError(f"Failed to initialize table for schema {schema_name}: {str(e)}")

    @track_operation('ddl')
    async def create_index(self, table_name: str, column_name: str, unique: bool = False):
        """Create an index on a specific column in a table."""
        try:
            index_name = f"idx_{table_name}_{column_name}"
            unique_str = "UNIQUE" if unique else ""
            query = f"""
                CREATE {unique_str} INDEX IF NOT EXISTS {index_name}
                ON {table_name} ({column_name});
            """
            await self.execute(query)
        except Exception as e:
            logger.error(f"Error creating index on {table_name}.{column_name}: {str(e)}")
            raise DatabaseError(f"Failed to create index: {str(e)}")

    @track_operation('ddl')
    async def create_score_table(self, session: AsyncSession):
        """Create the score table for task scoring."""
        try:
            # Check if table exists
            result = await self.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'score_table'
                );
            """, session=session)
            table_exists = result.scalar()
            logger.info(f"Score table exists: {table_exists}")

            if not table_exists:
                logger.info("Creating score table...")
                await self.execute("""
                    CREATE TABLE score_table (
                        task_name VARCHAR,
                        task_id TEXT,
                        score FLOAT[],
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        status VARCHAR DEFAULT 'pending'
                    )
                """, session=session)
                logger.info("Score table created successfully")

                # Create index on created_at
                logger.info("Creating score table timestamp index...")
                await self.execute("""
                    CREATE INDEX score_created_at_idx ON score_table(created_at)
                """, session=session)
                logger.info("Score table index created successfully")

        except Exception as e:
            logger.error(f"Error creating score table: {str(e)}")
            raise DatabaseError(f"Failed to create score table: {str(e)}")

    @track_operation('ddl')
    async def _create_node_table(self, session: AsyncSession):
        """Create the base node table."""
        try:
            await self.execute("""
                CREATE TABLE IF NOT EXISTS node_table (
                    uid INTEGER PRIMARY KEY,
                    hotkey TEXT,
                    coldkey TEXT,
                    ip TEXT,
                    ip_type TEXT,
                    port INTEGER,
                    incentive FLOAT,
                    stake FLOAT,
                    trust FLOAT,
                    vtrust FLOAT,
                    protocol TEXT,
                    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    CHECK (uid >= 0 AND uid < 256)
                )
            """, session=session)
        except Exception as e:
            logger.error(f"Error creating node table: {str(e)}")
            raise DatabaseError(f"Failed to create node table: {str(e)}")

    @track_operation('ddl')
    async def _create_trigger_function(self, session: AsyncSession):
        """Create the trigger function for size checking."""
        try:
            await self.execute("""
                CREATE OR REPLACE FUNCTION check_node_table_size()
                RETURNS TRIGGER AS $$
                BEGIN
                    IF (SELECT COUNT(*) FROM node_table) > 256 THEN
                        RAISE EXCEPTION 'Cannot exceed 256 rows in node_table';
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
            """, session=session)
        except Exception as e:
            logger.error(f"Error creating trigger function: {str(e)}")
            raise DatabaseError(f"Failed to create trigger function: {str(e)}")

    @track_operation('ddl')
    async def _create_trigger(self, session: AsyncSession):
        """Create trigger to enforce node table size limit."""
        try:
            result = await self.execute("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM pg_trigger 
                    WHERE tgname = 'enforce_node_table_size'
                )
            """, session=session)
            trigger_exists = result.scalar()
            
            if not trigger_exists:
                await self.execute("""
                    CREATE TRIGGER enforce_node_table_size
                    BEFORE INSERT ON node_table
                    FOR EACH ROW
                    EXECUTE FUNCTION check_node_table_size()
                """, session=session)
        except Exception as e:
            logger.error(f"Error creating trigger: {str(e)}")
            raise DatabaseError(f"Failed to create trigger: {str(e)}")

    @track_operation('write')
    async def _initialize_rows(self, session: AsyncSession):
        """Initialize the node table with 256 empty rows."""
        try:
            await self.execute("""
                INSERT INTO node_table (uid)
                SELECT generate_series(0, 255) as uid
                WHERE NOT EXISTS (SELECT 1 FROM node_table LIMIT 1);
            """, session=session)
        except Exception as e:
            logger.error(f"Error initializing rows: {str(e)}")
            raise DatabaseError(f"Failed to initialize rows: {str(e)}")

    @track_operation('read')
    async def get_recent_scores(self, task_type: str) -> List[float]:
        """Fetch scores using session for READ operation"""
        try:
            three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)

            if task_type == "soil":
                query = """
                SELECT score, created_at
                FROM score_table
                WHERE task_name LIKE 'soil_moisture_region_%'
                AND created_at >= :three_days_ago
                ORDER BY created_at DESC
                """
            else:
                query = """
                SELECT score, created_at
                FROM score_table
                WHERE task_name = :task_type
                AND created_at >= :three_days_ago
                ORDER BY created_at DESC
                """

            rows = await self.fetch_all(query, {
                "task_type": task_type, 
                "three_days_ago": three_days_ago
            })

            final_scores = [float("nan")] * 256
            for row in rows:
                score_array = row["score"]
                for uid, score in enumerate(score_array):
                    if not np.isnan(score) and np.isnan(final_scores[uid]):
                        final_scores[uid] = score

            return final_scores

        except Exception as e:
            logger.error(f"Error fetching recent scores for {task_type}: {str(e)}")
            return [float("nan")] * 256

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
            logger.error(traceback.format_exc())
            raise DatabaseError(f"Failed to close database connections: {str(e)}")

    async def load_task_schemas(self) -> Dict[str, Dict[str, Any]]:
        """
        Load database schemas for all tasks from their respective schema.json files.
        Searches through the defined_tasks directory for schema definitions.
        """
        try:
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

    @track_operation('write')
    async def update_miner_info(
        self,
        index: int,
        hotkey: str,
        coldkey: str,
        ip: Optional[str] = None,
        ip_type: Optional[str] = None,
        port: Optional[int] = None,
        incentive: Optional[float] = None,
        stake: Optional[float] = None,
        trust: Optional[float] = None,
        vtrust: Optional[float] = None,
        protocol: Optional[str] = None,
    ):
        """
        Update miner information at a specific index.

        Args:
            index (int): Index in the table (0-255)
            hotkey (str): Miner's hotkey
            coldkey (str): Miner's coldkey
            ip (str, optional): Miner's IP address
            ip_type (str, optional): Type of IP address
            port (int, optional): Port number
            incentive (float, optional): Miner's incentive
            stake (float, optional): Miner's stake
            trust (float, optional): Miner's trust score
            vtrust (float, optional): Miner's vtrust score
            protocol (str, optional): Protocol used
        """
        try:
            query = """
            UPDATE node_table 
            SET 
                hotkey = :hotkey,
                coldkey = :coldkey,
                ip = :ip,
                ip_type = :ip_type,
                port = :port,
                incentive = :incentive,
                stake = :stake,
                trust = :trust,
                vtrust = :vtrust,
                protocol = :protocol,
                last_updated = CURRENT_TIMESTAMP
            WHERE uid = :index
            """
            params = {
                "index": index,
                "hotkey": hotkey,
                "coldkey": coldkey,
                "ip": ip,
                "ip_type": ip_type,
                "port": port,
                "incentive": incentive,
                "stake": stake,
                "trust": trust,
                "vtrust": vtrust,
                "protocol": protocol,
            }
            await self.execute(query, params)
        except Exception as e:
            logger.error(f"Error updating miner info for index {index}: {str(e)}")
            raise DatabaseError(f"Failed to update miner info: {str(e)}")

    @track_operation('write')
    async def clear_miner_info(self, index: int, new_hotkey: Optional[str] = None, new_coldkey: Optional[str] = None):
        """
        Clear miner information from the node table and history tables.
        Task-specific cleanup of predictions and scores is handled by the 
        recalculate_recent_scores methods in each task.
        If new_hotkey is provided, updates the node table with the new information.

        Args:
            index (int): Index in the table (0-255)
            new_hotkey (str, optional): New hotkey to set after clearing
            new_coldkey (str, optional): New coldkey to set after clearing
        """
        try:
            # First check if the node exists
            check_query = "SELECT uid FROM node_table WHERE uid = :index"
            result = await self.fetch_one(check_query, {"index": index})
            if not result:
                logger.warning(f"No node found for index {index}, creating new entry")
                insert_query = "INSERT INTO node_table (uid) VALUES (:index)"
                await self.execute(insert_query, {"index": index})

            # Clear node table entry
            node_query = """
            UPDATE node_table 
            SET 
                hotkey = NULL,
                coldkey = NULL,
                ip = NULL,
                ip_type = NULL,
                port = NULL,
                incentive = NULL,
                stake = NULL,
                trust = NULL,
                vtrust = NULL,
                protocol = NULL,
                last_updated = CURRENT_TIMESTAMP
            WHERE uid = :index
            """
            await self.execute(node_query, {"index": index})

            # Clear history records
            geo_history_query = """
            DELETE FROM geomagnetic_history 
            WHERE miner_uid = :index
            """
            await self.execute(geo_history_query, {"index": index})

            soil_history_query = """
            DELETE FROM soil_moisture_history 
            WHERE miner_uid = :index
            """
            await self.execute(soil_history_query, {"index": index})

            logger.info(f"Successfully cleared node table entry and history records for miner {index}")

            # Update with new hotkey information if provided
            if new_hotkey is not None:
                await self.update_miner_info(
                    index=index,
                    hotkey=new_hotkey,
                    coldkey=new_coldkey or "",  # Default to empty string if not provided
                )
                logger.info(f"Updated node table with new hotkey information for index {index}")

        except Exception as e:
            logger.error(f"Error clearing miner info for index {index}: {str(e)}")
            logger.error(traceback.format_exc())
            # Don't raise the error, just log it and continue
            return False
        
        return True

    @track_operation('read')
    async def get_miner_info(self, index: int):
        """
        Get miner information for a specific index.

        Args:
            index (int): Index in the table (0-255)

        Returns:
            dict: Miner information or None if not found
        """
        try:
            query = """
            SELECT * FROM node_table 
            WHERE uid = :index
            """
            result = await self.fetch_one(query, {"index": index})
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Error getting miner info for index {index}: {str(e)}")
            raise DatabaseError(f"Failed to get miner info: {str(e)}")

    @track_operation('read')
    async def get_all_active_miners(self):
        """
        Get information for all miners with non-null hotkeys.

        Returns:
            list[dict]: List of active miner information
        """
        try:
            query = """
            SELECT * FROM node_table 
            WHERE hotkey IS NOT NULL
            ORDER BY uid
            """
            results = await self.fetch_all(query)
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error getting active miners: {str(e)}")
            raise DatabaseError(f"Failed to get active miners: {str(e)}")
