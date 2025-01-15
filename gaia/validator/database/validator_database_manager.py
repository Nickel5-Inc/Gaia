import traceback
import gc
import numpy as np
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, Optional, List
from datetime import datetime, timedelta, timezone
from gaia.database.database_manager import BaseDatabaseManager, DatabaseTimeout
from fiber.logging_utils import get_logger
import random
import time

logger = get_logger(__name__)


class ValidatorDatabaseManager(BaseDatabaseManager):
    """
    Database manager specifically for validator nodes.
    Handles all validator-specific database operations.
    """
    
    def __new__(cls, *args, **kwargs) -> 'ValidatorDatabaseManager':
        return super().__new__(cls, node_type="validator")

    def __init__(
        self,
        database: str = "validator_db",
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "postgres",
    ) -> None:
        """Initialize the validator database manager."""
        super().__init__(
            "validator",
            database=database,
            host=host,
            port=port,
            user=user,
            password=password,
        )
        self._operation_stats = {
            'ddl_operations': 0,
            'read_operations': 0,
            'write_operations': 0,
            'long_running_queries': []
        }

    @BaseDatabaseManager.with_timeout(DEFAULT_TRANSACTION_TIMEOUT)
    async def initialize_database(self):
        """Initialize database tables and schemas for validator tasks."""
        async with self.transaction() as session:
            try:
                await self._create_node_table(session)
                await self._create_trigger_function(session)
                await self._create_trigger(session)
                await self._initialize_rows(session)
                await self.create_score_table(session)
                
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
                logger.info("Successfully created core tables")
            except Exception as e:
                logger.error(f"Error creating core tables: {str(e)}")
                raise

        # Second transaction: Initialize task tables
        try:
            task_schemas = await self.load_task_schemas()
            async with self.transaction() as session:
                await self.initialize_task_tables(task_schemas, session)
                logger.info("Successfully initialized task tables")
        except Exception as e:
            logger.error(f"Error initializing task tables: {str(e)}")
            raise

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

            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_schema['table_name']} (
                    {', '.join(columns)}
                );
            """
            await session.execute(text(create_table_sql))

            if 'indexes' in table_schema:
                for index in table_schema['indexes']:
                    index_name = f"{table_schema['table_name']}_{index['column']}_idx"
                    unique = "UNIQUE" if index.get('unique', False) else ""
                    create_index_sql = f"""
                        CREATE INDEX IF NOT EXISTS {index_name}
                        ON {table_schema['table_name']} ({index['column']})
                        {unique};
                    """
                    await session.execute(text(create_index_sql))

            logger.info(f"Successfully created table {table_schema['table_name']} with indexes")

        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            raise

    @BaseDatabaseManager.with_timeout(DEFAULT_TRANSACTION_TIMEOUT)
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
                raise

    async def load_task_schemas(self) -> Dict[str, Dict[str, Any]]:
        """
        Load database schemas for all tasks from their respective schema.json files.
        Searches through the defined_tasks directory for schema definitions.
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
                except Exception as e:
                    logger.error(f"Error processing schema for {task_dir.name}: {e}")

        return schemas

    @BaseDatabaseManager.with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def create_index(self, table_name: str, column_name: str, unique: bool = False):
        """
        Create an index on a specific column in a table.

        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column to create index on
            unique (bool): Whether the index should enforce uniqueness
        """
        index_name = f"idx_{table_name}_{column_name}"
        unique_str = "UNIQUE" if unique else ""
        query = f"""
            CREATE {unique_str} INDEX IF NOT EXISTS {index_name}
            ON {table_name} ({column_name});
        """
        await self.execute(query)

    @BaseDatabaseManager.with_timeout(DEFAULT_TRANSACTION_TIMEOUT)
    async def create_score_table(self, session: AsyncSession):
        """Create a table for storing miner scores for all tasks."""
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS score_table (
                task_name VARCHAR(100) NOT NULL,
                task_id TEXT NOT NULL,
                score FLOAT[] NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(50) DEFAULT 'pending'
            )
        """))

    async def _create_node_table(self, session: AsyncSession):
        """Create the base node table."""
        await session.execute(text("""
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
        """))

    async def _create_trigger_function(self, session):
        """Create the trigger function for size checking."""
        await session.execute(text("""
            CREATE OR REPLACE FUNCTION check_node_table_size()
            RETURNS TRIGGER AS $$
            BEGIN
                IF (SELECT COUNT(*) FROM node_table) > 256 THEN
                    RAISE EXCEPTION 'Cannot exceed 256 rows in node_table';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        """))

    async def _create_trigger(self, session):
        """Create trigger to enforce node table size limit."""
        result = await session.execute(text("""
            SELECT EXISTS (
                SELECT 1 
                FROM pg_trigger 
                WHERE tgname = 'enforce_node_table_size'
            )
        """))
        trigger_exists = await result.scalar()
        
        if not trigger_exists:
            await session.execute(text("""
                CREATE TRIGGER enforce_node_table_size
                BEFORE INSERT ON node_table
                FOR EACH ROW
                EXECUTE FUNCTION check_node_table_size()
            """))

    async def _initialize_rows(self, session):
        """Initialize the node table with 256 empty rows."""
        await session.execute(text("""
            INSERT INTO node_table (uid)
            SELECT generate_series(0, 255) as uid
            WHERE NOT EXISTS (SELECT 1 FROM node_table LIMIT 1);
        """))

    @BaseDatabaseManager.with_timeout(DEFAULT_TRANSACTION_TIMEOUT)
    async def create_miner_table(self):
        """
        Create a table for storing miner information with exactly 256 rows.
        Initially all rows are null except for an ID column that serves as the index.
        """
        async with self.transaction() as session:
            try:
                await self._create_node_table(session)
                await self._create_trigger_function(session)
                await self._create_trigger(session)
                await self._initialize_rows(session)
            except Exception as e:
                logger.error(f"Error creating miner table: {str(e)}")
                logger.error(traceback.format_exc())
                raise

    @BaseDatabaseManager.with_timeout(DEFAULT_QUERY_TIMEOUT)
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

    @BaseDatabaseManager.with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def clear_miner_info(self, index: int):
        """
        Clear miner information at a specific index, setting values back to NULL.

        Args:
            index (int): Index in the table (0-255)
        """
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
        await self.execute(query, {"index": index})

    @BaseDatabaseManager.with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def get_miner_info(self, index: int):
        """
        Get miner information for a specific index.

        Args:
            index (int): Index in the table (0-255)

        Returns:
            dict: Miner information or None if not found
        """
        query = """
        SELECT * FROM node_table 
        WHERE uid = :index
        """
        result = await self.fetch_one(query, {"index": index})
        return dict(result) if result else None

    @BaseDatabaseManager.with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def get_all_active_miners(self):
        """
        Get information for all miners with non-null hotkeys.

        Returns:
            list[dict]: List of active miner information
        """
        query = """
        SELECT * FROM node_table 
        WHERE hotkey IS NOT NULL
        ORDER BY uid
        """
        results = await self.fetch_many(query)
        return [dict(row) for row in results]

    @BaseDatabaseManager.with_timeout(DEFAULT_QUERY_TIMEOUT)
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

            rows = await self.fetch_many(query, {
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
            await super().close()
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")
            logger.error(traceback.format_exc())
