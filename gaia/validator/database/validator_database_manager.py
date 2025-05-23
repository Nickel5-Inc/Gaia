import math
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
import torch



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
    _instance = None # For ValidatorDatabaseManager's own singleton instance tracking

    def __new__(cls, *args, **kwargs) -> 'ValidatorDatabaseManager':
        if cls._instance is None:
            # BaseDatabaseManager.__new__ handles singleton logic based on node_type.
            # This call will return an instance of ValidatorDatabaseManager, managed
            # as a singleton for node_type="validator" by the base class.
            instance = super().__new__(cls, node_type="validator", *args, **kwargs)
            cls._instance = instance
            # Defer actual attribute initialization to __init__
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
        # Check if this specific singleton instance has already been initialized.
        if not getattr(self, '_validator_db_manager_initialized', False):
            # Call base class init first to set up common attributes like db_url,
            # _active_sessions, _engine, _engine_initialized flag, etc.
            super().__init__(
                node_type="validator",
                database=database,
                host=host,
                port=port,
                user=user,
                password=password,
            )
            
            # ValidatorDatabaseManager-specific attributes
            self.database = database # Store database name for convenience
            
            # db_url is set by super().__init__ based on the provided parameters.
            # If Validator needs a different db_url construction, it can be overridden here,
            # but the current BaseDatabaseManager.__init__ seems to construct it correctly.
            # self.db_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}" # This would override base if needed

            # Custom timeouts for validator operations
            self.VALIDATOR_QUERY_TIMEOUT = 60  # 1 minute
            self.VALIDATOR_TRANSACTION_TIMEOUT = 300  # 5 minutes
            
            # Operation statistics, specific to ValidatorDatabaseManager
            self._operation_stats = {
                'ddl_operations': 0,
                'read_operations': 0,
                'write_operations': 0,
                'long_running_queries': []
            }
            
            self._storage_locked = False  # Validator-specific storage lock flag
            
            # Mark this ValidatorDatabaseManager instance as initialized.
            self._validator_db_manager_initialized = True

    async def _initialize_validator_database(self) -> None:
        """Initialize validator-specific database schema and columns."""
        try:
            schema_path = Path(__file__).parent.parent.parent / "tasks" / "defined_tasks" / "soilmoisture" / "schema.json"
            with open(schema_path) as f:
                schema = json.load(f)

            for table_name, table_schema in schema.items():
                if table_schema.get("database_type") != "validator":
                    continue

                # Use a single session for all operations within this loop iteration
                async with self.session() as session:
                    async with session.begin(): # Start a transaction
                        exists_query = """
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = :table_name
                            )
                        """
                        result = await self.fetch_one(exists_query, {"table_name": table_name}, session=session)
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
                            await self.execute(create_table_sql, session=session)
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
                                    await self.execute(create_index_sql, session=session)
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
                                    {"table_name": table_name, "column_name": col_name},
                                    session=session
                                )
                                column_exists = result["exists"] if result else False

                                if not column_exists:
                                    add_column_sql = f"""
                                        ALTER TABLE {table_name} 
                                        ADD COLUMN {col_name} {col_type};
                                    """
                                    await self.execute(add_column_sql, session=session)
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
                # self.db_url is initialized in BaseDatabaseManager.__init__
                raise DatabaseError("Database URL not initialized")
            
            # First create a connection to default postgres database to check/create our database
            default_url = self.db_url.rsplit('/', 1)[0] + '/postgres'
            temp_engine = create_async_engine(default_url)
            
            try:
                # Check if our database exists
                async with temp_engine.connect() as conn:
                    result = await conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{self.database}'"))
                    exists = result.scalar() is not None
                    
                    if not exists:
                        logger.info(f"Database {self.database} does not exist, creating...")
                        # Need to be outside transaction for CREATE DATABASE
                        await conn.execute(text("COMMIT"))
                        await conn.execute(text(f"CREATE DATABASE {self.database}"))
                        logger.info(f"Created database {self.database}")
            finally:
                await temp_engine.dispose()
            
            # Now create our main engine
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
            self._engine_initialized = True # Ensure flag is set on success
        except Exception as e:
            logger.error(f"Failed to initialize database engine: {str(e)}")
            self._engine = None # Clear engine state on failure
            self._session_factory = None # Clear session factory on failure
            self._engine_initialized = False # Ensure flag is set to False on failure
            raise DatabaseError(f"Failed to initialize database engine: {str(e)}")

    @track_operation('ddl')
    @BaseDatabaseManager.with_transaction()
    async def initialize_database(self, session: AsyncSession):
        """Initialize database tables and schemas for validator tasks."""
        try:
            # Initialize engine first
            await self._initialize_engine()
            
            # Create core tables using the provided session
            await self._create_node_table(session)
            await self._create_trigger_function(session)
            await self._create_trigger(session)
            await self._initialize_rows(session)
            await self.create_score_table(session)
            
            # Create baseline predictions table using the provided session
            await self.create_baseline_predictions_table(session)
            
            logger.info("Successfully created core tables")
            
            # Initialize validator-specific tables from task schemas.
            # _initialize_validator_database now manages its own sessions internally
            # as it iterates and performs checks.
            await self._initialize_validator_database() 
            
            # Initialize task tables using the provided session
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
    async def create_index(self, table_name: str, column_name: str, unique: bool = False, session: Optional[AsyncSession] = None):
        """Create an index on a specific column in a table."""
        try:
            index_name = f"idx_{table_name}_{column_name}"
            unique_str = "UNIQUE" if unique else ""
            query = f"""
                CREATE {unique_str} INDEX IF NOT EXISTS {index_name}
                ON {table_name} ({column_name});
            """
            await self.execute(query, session=session)
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

        Raises:
            ValueError: If index is not between 0 and 255
            DatabaseError: If database operation fails
        """
        try:
            # Validate index is within bounds
            if not (0 <= index < 256):
                raise ValueError(f"Invalid index {index}. Must be between 0 and 255")

            # Check if row exists
            exists_query = "SELECT 1 FROM node_table WHERE uid = :index"
            result = await self.fetch_one(exists_query, {"index": index})
            if not result:
                raise ValueError(f"No row exists for index {index}. The node table must be properly initialized with 256 rows.")

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

    @track_operation('write')
    async def remove_miner_from_score_tables(
        self,
        uids: List[int],
        task_names: List[str],
        filter_start_time: Optional[datetime] = None,
        filter_end_time: Optional[datetime] = None
    ) -> None:
        """
        Partially remove specified miners from 'score_table' rows for given task types,
        preserving data for all other miners. Sets the departing miners' array values to 0.0.
        Filters by a time window if filter_start_time and filter_end_time are provided.

        Args:
            uids (List[int]): List of miner UIDs to be zeroed out.
            task_names (List[str]): List of task names to apply the removal.
            filter_start_time (Optional[datetime]): If provided, only process rows where task_id >= this time.
            filter_end_time (Optional[datetime]): If provided, only process rows where task_id <= this time.
        """
        if not uids:
            return

        log_message_parts = [f"Zeroing out scores for UIDs {uids}"]
        if filter_start_time:
            log_message_parts.append(f"from {filter_start_time.isoformat()}")
        if filter_end_time:
            log_message_parts.append(f"to {filter_end_time.isoformat()}")
        logger.info(" ".join(log_message_parts))

        total_rows_updated = 0
        for task_name in task_names:
            # Each task_name can be processed in its own transaction for atomicity per task.
            async with self.session() as session: # Get a session instance
                async with session.begin(): # Start a transaction for this session
                    try:
                        # 1) Select score rows, potentially filtered by time
                        query_base = """
                            SELECT task_id, score
                            FROM score_table
                            WHERE task_name = :task_name
                        """
                        params = {"task_name": task_name}

                        time_conditions = []
                        if filter_start_time:
                            time_conditions.append("task_id::float >= :start_timestamp")
                            params["start_timestamp"] = filter_start_time.timestamp()
                        if filter_end_time:
                            time_conditions.append("task_id::float <= :end_timestamp")
                            params["end_timestamp"] = filter_end_time.timestamp()

                        if time_conditions:
                            query = query_base + " AND " + " AND ".join(time_conditions)
                        else:
                            query = query_base # No time filter

                        rows = await self.fetch_all(query, params, session=session)

                        if not rows:
                            logger.info(f"No '{task_name}' score rows found to update for the given criteria.")
                            continue

                        logger.info(f"Found {len(rows)} {task_name} score rows to process.")
                        rows_updated_for_task = 0
                        scores_updated_for_task = 0

                        for row in rows:
                            try:
                                # 2) Parse the score array JSON (or however it's stored)
                                all_scores = row["score"]
                                if not isinstance(all_scores, list):
                                    logger.warning(f"Score field is not a list for score_row with task_id {row['task_id']}")
                                    continue

                                changed = False
                                changes_in_row = 0
                                for uid_to_zero in uids: # Renamed uid to uid_to_zero to avoid conflict
                                    if 0 <= uid_to_zero < len(all_scores):
                                        current_score = all_scores[uid_to_zero]
                                        # Check if current score is NOT 0.0 or NaN (represented as string or float)
                                        is_nan_or_zero = (isinstance(current_score, str) or 
                                                         (isinstance(current_score, float) and (math.isnan(current_score) or current_score == 0.0)))
                                        logger.debug(f"Score for UID {uid_to_zero} in row {row['task_id']}: {current_score} (is_nan_or_zero: {is_nan_or_zero})")
                                        if not is_nan_or_zero:
                                            all_scores[uid_to_zero] = 0.0 # Set to 0.0 instead of NaN
                                            changed = True
                                            changes_in_row += 1

                                if changed:
                                    # 3) Update the score array in place using task_id
                                    update_sql = """
                                        UPDATE score_table
                                        SET score = :score
                                        WHERE task_name = :task_name
                                          AND task_id = :task_id
                                    """
                                    await self.execute(
                                        update_sql,
                                        {
                                            "score": all_scores,
                                            "task_name": task_name,
                                            "task_id": row["task_id"]
                                        },
                                        session=session
                                    )
                                    rows_updated_for_task += 1
                                    scores_updated_for_task += changes_in_row
                                    logger.debug(
                                        f"Updated {changes_in_row} scores in {task_name} row with task_id {row['task_id']}"
                                    )

                            except Exception as e_inner: # Renamed e to e_inner
                                logger.error(
                                    f"Error zeroing out miner scores in '{task_name}' score row with task_id {row['task_id']}: {e_inner}"
                                )
                                logger.error(traceback.format_exc())
                        
                        total_rows_updated += rows_updated_for_task
                        logger.info(
                            f"Task {task_name}: Zeroed out {scores_updated_for_task} scores across {rows_updated_for_task} rows"
                        )

                    except Exception as e_outer: # Renamed e to e_outer
                        logger.error(f"Error in remove_miner_from_score_tables for task '{task_name}': {e_outer}")
                        logger.error(traceback.format_exc())
                        # The transaction will be rolled back by the context manager by session.begin()

        logger.info(f"Score zeroing complete. Total rows updated: {total_rows_updated}")

    @track_operation('ddl')
    async def create_baseline_predictions_table(self, session: AsyncSession):
        """Create a simple table to store baseline model predictions."""
        try:
            logger.info("Creating baseline_predictions table if it doesn't exist")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS baseline_predictions (
                id SERIAL PRIMARY KEY,
                task_name TEXT NOT NULL,
                task_id TEXT NOT NULL,
                region_id TEXT,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                prediction JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
            await self.execute(create_table_sql, session=session)
            
            index_sql = """
            CREATE INDEX IF NOT EXISTS idx_baseline_task ON baseline_predictions (task_name, task_id);
            """
            await self.execute(index_sql, session=session)
            
            logger.info("Successfully created baseline_predictions table")
        except Exception as e:
            logger.error(f"Error creating baseline_predictions table: {e}")
            logger.error(traceback.format_exc())
            raise DatabaseError(f"Failed to create baseline_predictions table: {str(e)}")
            
    @track_operation('write')
    @BaseDatabaseManager.with_transaction()
    async def store_baseline_prediction(self, 
                                       session: AsyncSession, # Added session parameter due to decorator
                                       task_name: str, 
                                       task_id: str, 
                                       timestamp: datetime, 
                                       prediction: Any, 
                                       region_id: Optional[str] = None) -> bool:
        """
        Store a baseline model prediction in the database.
        This method is wrapped in a transaction.
        
        Args:
            session: The active database session (provided by decorator).
            task_name: Name of the task (e.g., 'geomagnetic', 'soil_moisture')
            task_id: ID of the specific task execution
            timestamp: Timestamp for when the prediction was made
            prediction: The model's prediction (will be JSON serialized)
            region_id: For soil moisture task, the region identifier (optional)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            table_check_sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'baseline_predictions'
            );
            """
            # Use the provided session for all DB operations within this method
            result = await self.fetch_one(table_check_sql, session=session)
            
            if result is None or (isinstance(result, (list, tuple)) and (len(result) == 0 or not result[0])) or not result['exists']:
                logger.info("Creating baseline_predictions table...")
                # The create_baseline_predictions_table already uses the session passed to it.
                await self.create_baseline_predictions_table(session=session)
            
            if isinstance(prediction, (int, float)):
                logger.info(f"DB: Storing {task_name} prediction: {prediction} (region={region_id})")
                if task_name == "geomagnetic":
                    prediction = {"value": prediction}
            else:
                logger.info(f"DB: Storing {task_name} prediction (region={region_id})")
            
            if isinstance(prediction, (np.ndarray, torch.Tensor)):
                prediction = prediction.tolist()
                
            try:
                prediction_json = json.dumps(prediction, default=self._json_serializer)
                logger.info(f"DB: Serialized prediction: {prediction_json[:100]}..." if len(prediction_json) > 100 else prediction_json)
            except Exception as e_serialize: # Renamed e to e_serialize
                logger.error(f"JSON serialization error: {e_serialize}")
                logger.error(f"Failed to serialize prediction of type {type(prediction)}")
                if hasattr(e_serialize, '__traceback__'):
                    logger.error(traceback.format_tb(e_serialize.__traceback__))
                return False # Propagate failure
            
            insert_sql = """
            INSERT INTO baseline_predictions 
            (task_name, task_id, region_id, timestamp, prediction)
            VALUES (:task_name, :task_id, :region_id, :timestamp, :prediction)
            """
            
            params = {
                "task_name": task_name,
                "task_id": task_id,
                "region_id": region_id,
                "timestamp": timestamp,
                "prediction": prediction_json
            }
            
            try:
                await self.execute(insert_sql, params, session=session)
                logger.info(f"DB: Successfully stored {task_name} prediction")
                return True
            except Exception as db_error:
                logger.error(f"DB insert error: {db_error}")
                if hasattr(db_error, '__traceback__'):
                    logger.error(traceback.format_tb(db_error.__traceback__))
                return False # Propagate failure
            
        except Exception as e_outer: # Renamed e to e_outer
            logger.error(f"DB: Error storing prediction: {e_outer}")
            logger.error(f"Prediction value type: {type(prediction)}")
            if hasattr(e_outer, '__traceback__'):
                logger.error(traceback.format_tb(e_outer.__traceback__))
            return False # Propagate failure
            
    @track_operation('read')
    async def get_baseline_prediction(self, 
                                    task_name: str, 
                                    task_id: str, 
                                    region_id: Optional[str] = None) -> Optional[Dict]:
        """
        Retrieve a baseline prediction from the database.
        
        Args:
            task_name: Name of the task
            task_id: ID of the specific task execution
            region_id: For soil moisture task, the region identifier (optional)
            
        Returns:
            Optional[Dict]: The prediction data or None if not found
        """
        # This is a read operation, so it's okay for fetch_one to manage its own session if not in a transaction.
        try:
            query = """
            SELECT * FROM baseline_predictions 
            WHERE task_name = :task_name 
            AND task_id = :task_id
            """
            
            params = {
                "task_name": task_name,
                "task_id": task_id
            }
            
            if region_id:
                query += " AND region_id = :region_id"
                params["region_id"] = region_id
            
            query += " ORDER BY created_at DESC LIMIT 1"
            
            result = await self.fetch_one(query, params) # fetch_one will handle session if not provided
            
            if not result:
                logger.warning(f"No baseline prediction found for {task_name}, task_id: {task_id}, region: {region_id}")
                return None
                
            if isinstance(result['prediction'], dict):
                prediction_data = result['prediction']
            else:
                prediction_data = json.loads(result['prediction'])
                
            return {
                "task_name": result['task_name'],
                "task_id": result['task_id'],
                "region_id": result['region_id'],
                "timestamp": result['timestamp'],
                "prediction": prediction_data,
                "created_at": result['created_at']
            }
            
        except Exception as e:
            logger.error(f"Error retrieving baseline prediction: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _json_serializer(self, obj):
        """
        Custom JSON serializer for objects not serializable by default json code.
        """
        if isinstance(obj, (datetime, np.datetime64)):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, torch.Tensor):
            return obj.cpu().numpy().tolist()
        raise TypeError(f"Type {type(obj)} not serializable")

    @track_operation('write')
    async def execute(self, query: str, params: Optional[Dict] = None, session: Optional[AsyncSession] = None) -> Any:
        """Execute a SQL query with parameters."""
        try:
            if self._storage_locked and any(keyword in query.lower() for keyword in ['insert', 'update', 'delete']):
                logger.warning("Storage is locked - skipping write operation")
                return None
                
            if session:
                result = await session.execute(text(query), params or {})
                return result
            else:
                async with self.session() as new_session:
                    async with new_session.begin():
                        result = await new_session.execute(text(query), params or {})
                    return result
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(traceback.format_exc())
            raise DatabaseError(f"Failed to execute query: {str(e)}")