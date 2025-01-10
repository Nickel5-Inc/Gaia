import os
import json
from typing import Dict, List, Optional
from datetime import datetime, timezone

from fiber.logging_utils import get_logger
from gaia.validator.utils.task_decorator import task_step
from gaia.validator.core.constants import DB_OP_TIMEOUT

logger = get_logger(__name__)

# Schema version tracking
CURRENT_SCHEMA_VERSION = "1.0.0"

# Table definitions
SCHEMA_TABLES = {
    "schema_version": """
        CREATE TABLE IF NOT EXISTS schema_version (
            version VARCHAR(20) PRIMARY KEY,
            applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        )
    """,
    
    "validator_state": """
        CREATE TABLE IF NOT EXISTS validator_state (
            wallet_name VARCHAR(100),
            hotkey_name VARCHAR(100),
            registration_block BIGINT,
            last_update TIMESTAMP WITH TIME ZONE,
            validator_permit BOOLEAN DEFAULT FALSE,
            stake DECIMAL(20, 8) DEFAULT 0,
            uid INTEGER,
            PRIMARY KEY (wallet_name, hotkey_name)
        )
    """,
    
    "miner_scores": """
        CREATE TABLE IF NOT EXISTS miner_scores (
            hotkey VARCHAR(100),
            task_type VARCHAR(50),
            score DECIMAL(10, 4),
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            block_number BIGINT,
            PRIMARY KEY (hotkey, task_type, timestamp)
        )
    """,
    
    "weight_history": """
        CREATE TABLE IF NOT EXISTS weight_history (
            hotkey VARCHAR(100),
            weight DECIMAL(10, 8),
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            block_number BIGINT,
            PRIMARY KEY (hotkey, timestamp)
        )
    """,
    
    "task_metrics": """
        CREATE TABLE IF NOT EXISTS task_metrics (
            task_name VARCHAR(100),
            duration DECIMAL(10, 3),
            success BOOLEAN,
            error_message TEXT,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (task_name, timestamp)
        )
    """
}

# Index definitions
SCHEMA_INDEXES = {
    "miner_scores_hotkey_idx": """
        CREATE INDEX IF NOT EXISTS miner_scores_hotkey_idx
        ON miner_scores (hotkey)
    """,
    
    "miner_scores_timestamp_idx": """
        CREATE INDEX IF NOT EXISTS miner_scores_timestamp_idx
        ON miner_scores (timestamp)
    """,
    
    "weight_history_hotkey_idx": """
        CREATE INDEX IF NOT EXISTS weight_history_hotkey_idx
        ON weight_history (hotkey)
    """,
    
    "weight_history_timestamp_idx": """
        CREATE INDEX IF NOT EXISTS weight_history_timestamp_idx
        ON weight_history (timestamp)
    """,
    
    "task_metrics_timestamp_idx": """
        CREATE INDEX IF NOT EXISTS task_metrics_timestamp_idx
        ON task_metrics (timestamp)
    """
}

class SchemaManager:
    """
    Manages database schema and migrations.
    
    This class handles:
    1. Schema initialization
    2. Schema version tracking
    3. Schema migrations
    4. Index management
    5. Schema validation
    """
    
    def __init__(self, database_manager):
        self.database_manager = database_manager
        
    @task_step(
        name="initialize_schema",
        description="Initialize database schema",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=5
    )
    async def initialize_schema(self) -> bool:
        """
        Initialize the database schema.
        
        Returns:
            bool: True if initialization successful
        """
        try:
            # Create schema version table first
            await self.database_manager.execute(SCHEMA_TABLES["schema_version"])
            
            # Check current version
            current_version = await self._get_schema_version()
            if current_version == CURRENT_SCHEMA_VERSION:
                logger.info(f"Schema already at version {CURRENT_SCHEMA_VERSION}")
                return True
                
            # Create tables
            for table_name, create_sql in SCHEMA_TABLES.items():
                if table_name != "schema_version":
                    await self.database_manager.execute(create_sql)
                    logger.info(f"Created table: {table_name}")
                    
            # Create indexes
            for index_name, create_sql in SCHEMA_INDEXES.items():
                await self.database_manager.execute(create_sql)
                logger.info(f"Created index: {index_name}")
                
            # Update schema version
            await self._update_schema_version(
                CURRENT_SCHEMA_VERSION,
                "Initial schema creation"
            )
            
            logger.info(f"Schema initialized to version {CURRENT_SCHEMA_VERSION}")
            return True
            
        except Exception as e:
            logger.error(f"Schema initialization failed: {e}")
            return False

    async def _get_schema_version(self) -> Optional[str]:
        """Get current schema version."""
        try:
            result = await self.database_manager.fetch_one(
                "SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1"
            )
            return result["version"] if result else None
        except Exception:
            return None

    async def _update_schema_version(self, version: str, description: str):
        """Update schema version."""
        await self.database_manager.execute(
            """
            INSERT INTO schema_version (version, description)
            VALUES ($1, $2)
            """,
            version,
            description
        )

    @task_step(
        name="validate_schema",
        description="Validate database schema",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def validate_schema(self) -> bool:
        """
        Validate the database schema.
        
        Returns:
            bool: True if schema is valid
        """
        try:
            # Check all tables exist
            existing_tables = await self._get_existing_tables()
            for table_name in SCHEMA_TABLES.keys():
                if table_name not in existing_tables:
                    logger.error(f"Missing table: {table_name}")
                    return False
                    
            # Check all indexes exist
            existing_indexes = await self._get_existing_indexes()
            for index_name in SCHEMA_INDEXES.keys():
                if index_name not in existing_indexes:
                    logger.error(f"Missing index: {index_name}")
                    return False
                    
            logger.info("Schema validation successful")
            return True
            
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return False

    async def _get_existing_tables(self) -> List[str]:
        """Get list of existing tables."""
        result = await self.database_manager.fetch_all(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
            """
        )
        return [row["tablename"] for row in result]

    async def _get_existing_indexes(self) -> List[str]:
        """Get list of existing indexes."""
        result = await self.database_manager.fetch_all(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
            """
        )
        return [row["indexname"] for row in result]

    @task_step(
        name="cleanup_old_data",
        description="Clean up old data from tables",
        timeout_seconds=300,  # 5 minutes
        max_retries=1,
        retry_delay_seconds=30
    )
    async def cleanup_old_data(self, days: int = 30) -> Dict[str, int]:
        """
        Clean up old data from tables.
        
        Args:
            days: Number of days of data to keep
            
        Returns:
            dict: Rows deleted from each table
        """
        try:
            cleanup_queries = {
                "miner_scores": """
                    DELETE FROM miner_scores
                    WHERE timestamp < NOW() - INTERVAL '$1 days'
                """,
                "weight_history": """
                    DELETE FROM weight_history
                    WHERE timestamp < NOW() - INTERVAL '$1 days'
                """,
                "task_metrics": """
                    DELETE FROM task_metrics
                    WHERE timestamp < NOW() - INTERVAL '$1 days'
                """
            }
            
            results = {}
            for table, query in cleanup_queries.items():
                # Get count before deletion
                count_before = await self._get_row_count(table)
                
                # Delete old data
                await self.database_manager.execute(
                    query.replace("$1", str(days))
                )
                
                # Get count after deletion
                count_after = await self._get_row_count(table)
                rows_deleted = count_before - count_after
                
                results[table] = rows_deleted
                logger.info(f"Deleted {rows_deleted} rows from {table}")
                
            return results
            
        except Exception as e:
            logger.error(f"Data cleanup failed: {e}")
            return {}

    async def _get_row_count(self, table: str) -> int:
        """Get row count for a table."""
        result = await self.database_manager.fetch_one(
            f"SELECT COUNT(*) as count FROM {table}"
        )
        return result["count"] if result else 0

    @task_step(
        name="get_schema_info",
        description="Get schema information",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def get_schema_info(self) -> Dict:
        """
        Get information about the database schema.
        
        Returns:
            dict: Schema information
        """
        try:
            # Get current version
            version = await self._get_schema_version()
            
            # Get table information
            tables = await self.database_manager.fetch_all(
                """
                SELECT
                    tablename,
                    pg_size_pretty(pg_total_relation_size(quote_ident(tablename))) as size
                FROM pg_tables
                WHERE schemaname = 'public'
                """
            )
            
            # Get index information
            indexes = await self.database_manager.fetch_all(
                """
                SELECT
                    tablename,
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE schemaname = 'public'
                """
            )
            
            return {
                "version": version,
                "tables": [dict(t) for t in tables],
                "indexes": [dict(i) for i in indexes]
            }
            
        except Exception as e:
            logger.error(f"Failed to get schema info: {e}")
            return {} 