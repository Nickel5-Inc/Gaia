from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone, timedelta
import traceback
import numpy as np
import json
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from gaia.database.database_manager import BaseDatabaseManager
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

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
        """Initialize the validator database manager (only once)."""
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

    @BaseDatabaseManager.with_transaction
    async def initialize_database(self, session: AsyncSession) -> None:
        """Initialize database tables and schemas for validator tasks."""
        try:
            await self._create_node_table(session)
            await self._create_trigger_function(session)
            await self._create_trigger(session)
            await self._initialize_rows(session)
            await self.create_score_table(session)

            # Create process queue table
            await session.execute(
                text("""
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
                """)
            )

            # Initialize task tables
            task_schemas = await self.load_task_schemas()
            await self.initialize_task_tables(session, task_schemas)
            
            logger.info("Successfully initialized all database tables")
            
        except Exception as e:
            logger.error(f"Error during database initialization: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @BaseDatabaseManager.with_transaction
    async def _create_task_table(self, session: AsyncSession, schema: Dict[str, Any], table_name: Optional[str] = None) -> None:
        """Create a database table for a task based on its schema definition."""
        try:
            if table_name:
                table_schema = schema[table_name]
            else:
                table_schema = schema

            # Build the CREATE TABLE query
            columns = [
                f"{col_name} {col_type}"
                for col_name, col_type in table_schema["columns"].items()
            ]

            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_schema['table_name']} (
                    {','.join(columns)}
                )
            """

            await session.execute(text(create_table_query))
            logger.debug(f"Table {table_schema['table_name']} created or already exists.")

            # Create any specified indexes
            if "indexes" in table_schema:
                for index in table_schema["indexes"]:
                    await self.create_index(
                        session,
                        table_schema["table_name"],
                        index["column"],
                        unique=index.get("unique", False),
                    )

        except Exception as e:
            table_id = table_name or table_schema.get("table_name", "unknown")
            logger.error(f"Error creating table {table_id}: {e}")
            raise

    @BaseDatabaseManager.with_transaction
    async def initialize_task_tables(self, session: AsyncSession, task_schemas: Dict[str, Dict[str, Any]]) -> None:
        """Initialize validator-specific task tables."""
        for schema in task_schemas.values():
            # Check if this is a multi-table schema
            if isinstance(schema, dict) and any(
                isinstance(v, dict) and "table_name" in v for v in schema.values()
            ):
                # Handle multi-table schema
                for table_name, table_schema in schema.items():
                    if table_schema.get("database_type") in ["validator", "both"]:
                        await self._create_task_table(session, schema, table_name)
            else:
                # Handle single-table schema
                if schema.get("database_type") in ["validator", "both"]:
                    await self._create_task_table(session, schema)

    async def load_task_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Load database schemas for all tasks from their respective schema.json files."""
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

                    # Check if this is a multi-table schema
                    if any(
                        isinstance(v, dict) and "table_name" in v
                        for v in schema.values()
                    ):
                        # Validate each table in the schema
                        for table_name, table_schema in schema.items():
                            if not all(
                                key in table_schema for key in ["table_name", "columns"]
                            ):
                                raise ValueError(
                                    f"Invalid table schema for {table_name} in {schema_file}. "
                                    "Must contain 'table_name' and 'columns'"
                                )
                    else:
                        # Validate single table schema
                        if not all(key in schema for key in ["table_name", "columns"]):
                            raise ValueError(
                                f"Invalid schema in {schema_file}. "
                                "Must contain 'table_name' and 'columns'"
                            )

                    # Store the validated schema
                    schemas[task_dir.name] = schema

                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing schema.json in {task_dir.name}: {e}")
                except Exception as e:
                    logger.error(f"Error processing schema for {task_dir.name}: {e}")

        return schemas

    @BaseDatabaseManager.with_transaction
    async def create_index(self, session: AsyncSession, table_name: str, column_name: str, unique: bool = False) -> None:
        """Create an index on a specific column in a table."""
        index_name = f"idx_{table_name}_{column_name}"
        unique_str = "UNIQUE" if unique else ""

        create_index_query = f"""
            CREATE {unique_str} INDEX IF NOT EXISTS {index_name}
            ON {table_name} ({column_name});
        """

        await session.execute(text(create_index_query))

    @BaseDatabaseManager.with_transaction
    async def create_score_table(self, session: AsyncSession) -> None:
        """Create a table for storing miner scores for all tasks."""
        query = """
        CREATE TABLE IF NOT EXISTS score_table (
            task_name VARCHAR(100) NOT NULL,
            task_id TEXT NOT NULL,
            score FLOAT[] NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(50) DEFAULT 'pending'
        )
        """
        await session.execute(text(query))

    @BaseDatabaseManager.with_transaction
    async def _create_node_table(self, session: AsyncSession) -> None:
        """Create the base node table."""
        sql = """
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
        );
        """
        await session.execute(text(sql))

    @BaseDatabaseManager.with_transaction
    async def _create_trigger_function(self, session: AsyncSession) -> None:
        """Create the trigger function for size checking."""
        sql = """
        CREATE OR REPLACE FUNCTION check_node_table_size()
        RETURNS TRIGGER AS $$
        BEGIN
            IF (SELECT COUNT(*) FROM node_table) > 256 THEN
                RAISE EXCEPTION 'Cannot exceed 256 rows in node_table';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        await session.execute(text(sql))

    @BaseDatabaseManager.with_transaction
    async def _create_trigger(self, session: AsyncSession) -> None:
        """Create the trigger for enforcing table size."""
        # First drop the existing trigger if it exists
        drop_trigger = """
        DROP TRIGGER IF EXISTS enforce_node_table_size ON node_table;
        """
        await session.execute(text(drop_trigger))

        # Then create the new trigger
        create_trigger = """
        CREATE TRIGGER enforce_node_table_size
        BEFORE INSERT ON node_table
        EXECUTE FUNCTION check_node_table_size();
        """
        await session.execute(text(create_trigger))

    @BaseDatabaseManager.with_transaction
    async def _initialize_rows(self, session: AsyncSession) -> None:
        """Initialize the table with 256 empty rows."""
        sql = """
        INSERT INTO node_table (uid)
        SELECT generate_series(0, 255) as uid
        WHERE NOT EXISTS (SELECT 1 FROM node_table LIMIT 1);
        """
        await session.execute(text(sql))

    @BaseDatabaseManager.with_transaction
    async def create_miner_table(self, session: AsyncSession) -> None:
        """Create a table for storing miner information with exactly 256 rows."""
        try:
            await self._create_node_table(session)
            await self._create_trigger_function(session)
            await self._create_trigger(session)
            await self._initialize_rows(session)

        except Exception as e:
            logger.error(f"Error creating miner table: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @BaseDatabaseManager.with_transaction
    async def update_miner_info(
        self,
        session: AsyncSession,
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
    ) -> None:
        """Update miner information at a specific index."""
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
        await session.execute(text(query), params)

    @BaseDatabaseManager.with_transaction
    async def clear_miner_info(self, session: AsyncSession, index: int) -> None:
        """Clear miner information at a specific index, setting values back to NULL."""
        query = """
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
        await session.execute(text(query), {"index": index})

    @BaseDatabaseManager.with_session
    async def get_miner_info(self, session: AsyncSession, index: int) -> Optional[Dict[str, Any]]:
        """Get miner information for a specific index."""
        query = """
        SELECT * FROM node_table 
        WHERE uid = :index
        """
        result = await session.execute(text(query), {"index": index})
        row = result.first()
        return dict(row._mapping) if row else None

    @BaseDatabaseManager.with_session
    async def get_all_active_miners(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """Get information for all miners with non-null hotkeys."""
        query = """
        SELECT * FROM node_table 
        WHERE hotkey IS NOT NULL
        ORDER BY uid
        """
        result = await session.execute(text(query))
        return [dict(row._mapping) for row in result]

    @BaseDatabaseManager.with_session
    async def get_recent_scores(self, session: AsyncSession, task_type: str) -> List[float]:
        """Fetch and average scores for the given task type over the last 3 days."""
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

            result = await session.execute(
                text(query),
                {"task_type": task_type, "three_days_ago": three_days_ago}
            )
            rows = result.fetchall()

            final_scores = [float("nan")] * 256

            for row in rows:
                score_array = row.score
                for uid, score in enumerate(score_array):
                    if not np.isnan(score) and np.isnan(final_scores[uid]):
                        final_scores[uid] = score

            return final_scores

        except Exception as e:
            logger.error(f"Error fetching recent scores for {task_type}: {str(e)}")
            return [float("nan")] * 256
