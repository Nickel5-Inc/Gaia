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
            
            # Store database name
            self.database = database
            
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
            
            # Create baseline predictions table
            await self.create_baseline_predictions_table(session)
            
            logger.info("Successfully created core tables")
            
            # Initialize validator-specific tables from task schemas
            await self._initialize_validator_database()
            
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

    @track_operation('write')
    async def clear_miner_info(
        self,
        index: int
    ) -> bool:
        """
        Clear all information for a miner from the database, treating every change as a
        full deregistration. Delete the node's hotkey/coldkey/metadata, remove historical
        geomagnetic and soil moisture data, and update scores in the score tables.

        Args:
            index (int): The miner UID (0–255).

        Returns:
            bool: True on success, False on failure.
        """
        try:
            # First, read the current hotkey for logging purposes
            original_info = await self.get_miner_info(index)
            original_hotkey = original_info["hotkey"] if original_info else None

            # Clear node_table entry
            node_query = """
            UPDATE node_table 
            SET hotkey = NULL, coldkey = NULL, ip = NULL, ip_type = NULL, port = NULL,
                incentive = NULL, stake = NULL, trust = NULL, vtrust = NULL, protocol = NULL
            WHERE uid = :index
            """
            await self.execute(node_query, {"index": index})

            # Delete geomagnetic and soil moisture history if the miner had a hotkey
            if original_hotkey:
                geo_history_query = """
                DELETE FROM geomagnetic_history 
                WHERE miner_hotkey = :old_hotkey
                """
                await self.execute(geo_history_query, {"old_hotkey": original_hotkey})

                soil_history_query = """
                DELETE FROM soil_moisture_history 
                WHERE miner_hotkey = :old_hotkey
                """
                await self.execute(soil_history_query, {"old_hotkey": original_hotkey})

            # After clearing, treat as a full deregistration and update score tables
            await self.remove_miner_from_score_tables(
                uids=[index],
                task_names=["soil_moisture", "geomagnetic"],
                window_days=1  # Adjust the window as needed
            )
            logger.info(f"Partially removed UID {index} from existing daily score rows for all tasks")

            logger.info(f"Successfully cleared miner info for UID {index}")
            return True

        except Exception as e:
            logger.error(f"Error clearing miner info for UID {index}: {str(e)}")
            logger.error(traceback.format_exc())
            return False

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
        window_days: int = 1
    ) -> None:
        """
        Partially remove specified miners from daily 'score_table' rows for given task types,
        preserving data for all other miners. Sets the departing miners' array values to NaN.
    
        Args:
            uids (List[int]): List of miner UIDs to be zeroed out.
            task_names (List[str]): List of task names to apply the removal.
            window_days (int): Number of days to look back for score rows.
        """
        if not uids:
            return

        str_uids = [str(uid) for uid in uids]
        current_time = datetime.now(timezone.utc)
        history_window = current_time - timedelta(days=window_days)
        logger.info(f"Removing scores for UIDs {uids} from {history_window} to {current_time}")

        total_rows_updated = 0
        for task_name in task_names:
            try:
                # 1) Select daily score rows in the specified time window
                query = """
                    SELECT task_id, score
                    FROM score_table
                    WHERE task_name = :task_name
                      AND task_id::float >= :start_timestamp
                      AND task_id::float <= :end_timestamp
                """
                params = {
                    "task_name": task_name,
                    "start_timestamp": history_window.timestamp(),
                    "end_timestamp": current_time.timestamp(),
                }
                rows = await self.fetch_all(query, params)

                if not rows:
                    logger.info(f"No '{task_name}' score rows found to update.")
                    continue

                logger.info(f"Found {len(rows)} {task_name} score rows in time window")
                rows_updated = 0
                scores_updated = 0

                for row in rows:
                    try:
                        # 2) Parse the score array JSON (or however it's stored)
                        all_scores = row["score"]
                        if not isinstance(all_scores, list):
                            logger.warning(f"Score field is not a list for score_row with task_id {row['task_id']}")
                            continue

                        changed = False
                        changes_in_row = 0
                        for uid in uids:
                            if 0 <= uid < len(all_scores):
                                current_score = all_scores[uid]
                                is_nan = isinstance(current_score, str) or (isinstance(current_score, float) and math.isnan(current_score))
                                logger.debug(f"Score for UID {uid} in row {row['task_id']}: {current_score} (is_nan: {is_nan})")
                                if not is_nan:
                                    all_scores[uid] = float("nan")
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
                            )
                            rows_updated += 1
                            scores_updated += changes_in_row
                            logger.debug(
                                f"Updated {changes_in_row} scores in {task_name} row with task_id {row['task_id']}"
                            )

                    except Exception as e:
                        logger.error(
                            f"Error removing miner from '{task_name}' score row with task_id {row['task_id']}: {e}"
                        )
                        logger.error(traceback.format_exc())

                total_rows_updated += rows_updated
                logger.info(
                    f"Task {task_name}: Updated {scores_updated} scores across {rows_updated} rows"
                )

            except Exception as e:
                logger.error(f"Error in remove_miner_from_score_tables for task '{task_name}': {e}")
                logger.error(traceback.format_exc())

        logger.info(f"Score removal complete. Total rows updated: {total_rows_updated}")

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
    async def store_baseline_prediction(self, 
                                       task_name: str, 
                                       task_id: str, 
                                       timestamp: datetime, 
                                       prediction: Any, 
                                       region_id: Optional[str] = None) -> bool:
        """
        Store a baseline model prediction in the database.
        
        Args:
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
            result = await self.fetch_one(table_check_sql)
            
            if result is None or (isinstance(result, (list, tuple)) and (len(result) == 0 or not result[0])):
                logger.info("Creating baseline_predictions table...")
                async with self.session() as session:
                    await self.create_baseline_predictions_table(session)
            
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
            except Exception as e:
                logger.error(f"JSON serialization error: {e}")
                logger.error(f"Failed to serialize prediction of type {type(prediction)}")
                if hasattr(e, '__traceback__'):
                    logger.error(traceback.format_tb(e.__traceback__))
                return False
            
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
                await self.execute(insert_sql, params)
                logger.info(f"DB: Successfully stored {task_name} prediction")
                return True
            except Exception as db_error:
                logger.error(f"DB insert error: {db_error}")
                if hasattr(db_error, '__traceback__'):
                    logger.error(traceback.format_tb(db_error.__traceback__))
                return False
            
        except Exception as e:
            logger.error(f"DB: Error storing prediction: {e}")
            logger.error(f"Prediction value type: {type(prediction)}")
            if hasattr(e, '__traceback__'):
                logger.error(traceback.format_tb(e.__traceback__))
            return False
            
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
            
            result = await self.fetch_one(query, params)
            
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