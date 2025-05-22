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
            cls._instance._storage_locked = False  # Add storage lock flag
            
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
            
            # Store database name (might still be useful for logging/config)
            self.database = database
            
            # Set database URL (crucial)
            self.db_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
            
            # Custom timeouts for validator operations
            self.VALIDATOR_QUERY_TIMEOUT = 60  # 1 minute
            self.VALIDATOR_TRANSACTION_TIMEOUT = 300  # 5 minutes
            
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

    async def _initialize_engine(self) -> None:
        """Initialize database engine and session factory. Assumes DB exists."""
        try:
            if not self.db_url:
                logger.error("Database URL not set during engine initialization.")
                raise DatabaseError("Database URL not initialized")

            # Log initialization attempt
            masked_url = str(self.db_url)
            try:
                # Attempt to mask credentials if present in the URL
                split_at = masked_url.find('@')
                if split_at != -1:
                    split_protocol = masked_url.find('://')
                    if split_protocol != -1:
                       masked_url = masked_url[:split_protocol+3] + '***:***@' + masked_url[split_at+1:]
            except Exception:
                 pass # Keep original URL if masking fails
            logger.info(f"Attempting to initialize main database engine for: {masked_url}")

            # Create our main engine pointing directly to the application DB
            self._engine = create_async_engine(
                self.db_url,
                pool_size=self.MAX_CONNECTIONS, # Use class attribute 
                max_overflow=10,
                pool_timeout=self.DEFAULT_CONNECTION_TIMEOUT, # Use base class attribute
                pool_recycle=300,
                pool_pre_ping=True,
                echo=False,
                connect_args={
                    "command_timeout": self.VALIDATOR_QUERY_TIMEOUT, # Use validator timeout
                    "timeout": self.DEFAULT_CONNECTION_TIMEOUT, # Use base class connection timeout
                }
            )
            
            # Initialize session factory
            self._session_factory = async_sessionmaker(
                self._engine,
                expire_on_commit=False,
                class_=AsyncSession,
                autobegin=False
            )
            
            # Test the connection to the application database
            async with self._engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            
            logger.info(f"Successfully initialized database engine for {self.node_type} node.")
        except Exception as e:
            logger.error(f"Failed to initialize main database engine: {str(e)}")
            logger.error(traceback.format_exc())
            # Ensure engine and factory are None if init fails
            self._engine = None
            self._session_factory = None
            raise DatabaseError(f"Failed to initialize database engine: {str(e)}") from e

    async def initialize_database(self):
        """Placeholder for any non-schema initialization needed at startup."""
        # This method previously called the DDL creation methods.
        # Now, it assumes the schema exists (created by Alembic).
        # If there are other non-schema setup tasks (e.g., populating
        # volatile cache from DB, specific startup checks), they could go here.
        # For now, it might do nothing or just ensure the engine is ready.
        try:
            logger.info("Ensuring database engine is initialized (schema assumed to exist)...")
            # Ensure engine is created and connection is tested
            await self.ensure_engine_initialized() 
            logger.info("Database engine initialization check complete.")
            # Removed calls to: 
            # _create_node_table, _create_trigger_function, _create_trigger, 
            # _initialize_rows, create_score_table, create_baseline_predictions_table, 
            # _initialize_validator_database, load_task_schemas, initialize_task_tables
        except Exception as e:
            logger.error(f"Error during simplified database initialization check: {str(e)}")
            # Decide if this should re-raise or just log
            raise DatabaseError(f"Failed during simplified initialization: {str(e)}") from e

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

                rows = await self.fetch_all(query, params)

                if not rows:
                    logger.info(f"No '{task_name}' score rows found to update for the given criteria.")
                    continue

                logger.info(f"Found {len(rows)} {task_name} score rows to process.")
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
                                # Check if current score is NOT 0.0 or NaN (represented as string or float)
                                is_nan_or_zero = (isinstance(current_score, str) or 
                                                 (isinstance(current_score, float) and (math.isnan(current_score) or current_score == 0.0)))
                                logger.debug(f"Score for UID {uid} in row {row['task_id']}: {current_score} (is_nan_or_zero: {is_nan_or_zero})")
                                if not is_nan_or_zero:
                                    all_scores[uid] = 0.0 # Set to 0.0 instead of NaN
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
                            f"Error zeroing out miner scores in '{task_name}' score row with task_id {row['task_id']}: {e}"
                        )
                        logger.error(traceback.format_exc())

                total_rows_updated += rows_updated
                logger.info(
                    f"Task {task_name}: Zeroed out {scores_updated} scores across {rows_updated} rows"
                )

            except Exception as e:
                logger.error(f"Error in remove_miner_from_score_tables for task '{task_name}': {e}")
                logger.error(traceback.format_exc())

        logger.info(f"Score zeroing complete. Total rows updated: {total_rows_updated}")

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
            if isinstance(prediction, (np.ndarray, torch.Tensor)):
                prediction = prediction.tolist()
                
            try:
                prediction_json = json.dumps(prediction, default=self._json_serializer)
            except Exception as e:
                logger.error(f"JSON serialization error: {e}")
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
            
            await self.execute(insert_sql, params)
            return True
            
        except Exception as e:
            logger.error(f"DB: Error storing prediction: {e}")
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