import math
import traceback
import gc
import numpy as np
from sqlalchemy import text, update
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
import os
from gaia.database.validator_schema import node_table

# High-performance JSON operations
try:
    from gaia.utils.performance import dumps, loads
    JSON_PERFORMANCE_AVAILABLE = True
except ImportError:
    import json
    def dumps(obj, **kwargs):
        return json.dumps(obj, **kwargs)
    def loads(s):
        return json.loads(s)
    JSON_PERFORMANCE_AVAILABLE = False

logger = get_logger(__name__)

T = TypeVar('T')

def track_operation(operation_type: str):
    """Decorator to track database operations."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(self: 'ValidatorDatabaseManager', *args, **kwargs) -> T:
            if self.system_running_event:
                await self.system_running_event.wait()

            query_text_for_log = "N/A"
            if args:
                if isinstance(args[0], str):
                    query_text_for_log = args[0][:200].replace('\n', ' ') + "..."
                elif hasattr(args[0], '__str__'):
                    query_text_for_log = str(args[0])[:200].replace('\n', ' ') + "..."
            
            if query_text_for_log == "N/A" and kwargs.get('query'):
                 if isinstance(kwargs['query'], str):
                    query_text_for_log = kwargs['query'][:200].replace('\n', ' ') + "..."
                 elif hasattr(kwargs['query'], '__str__'):
                    query_text_for_log = str(kwargs['query'])[:200].replace('\n', ' ') + "..."

            if func.__name__ == "batch_update_miners":
                query_text_for_log = "Batch operation (see function logs for individual queries)"
            elif func.__name__ == "update_miner_info" and args:
                query_text_for_log = f"UPDATE node_table SET ... WHERE uid={args[0] if args else 'N/A'}"


            op_id = random.randint(10000, 99999)
            
            overall_start_time = time.perf_counter()
            logger.info(f"[DBTrack {op_id}] ENTERING {operation_type} op: {func.__name__}, Query: {query_text_for_log}")
            
            db_call_start_time = 0.0
            db_call_duration = 0.0
            result = None

            try:
                db_call_start_time = time.perf_counter()
                result = await func(self, *args, **kwargs)
                db_call_duration = time.perf_counter() - db_call_start_time
                
                self._operation_stats[f'{operation_type}_operations'] += 1
                
                if db_call_duration > self.VALIDATOR_QUERY_TIMEOUT / 2:
                    self._operation_stats['long_running_queries'].append({
                        'operation': func.__name__,
                        'query_snippet': query_text_for_log,
                        'duration': db_call_duration,
                        'timestamp': time.time()
                    })
                    logger.warning(f"[DBTrack {op_id}] Long-running DB call for {operation_type} op: {func.__name__} detected: {db_call_duration:.4f}s. Query: {query_text_for_log}")
                else:
                    pass

            except Exception as e:
                db_call_duration = time.perf_counter() - db_call_start_time
                logger.error(f"[DBTrack {op_id}] ERROR in {operation_type} op: {func.__name__} after {db_call_duration:.4f}s in DB call. Query: {query_text_for_log}. Error: {str(e)}", exc_info=True)
                raise
            finally:
                overall_duration = time.perf_counter() - overall_start_time
                if abs(overall_duration - db_call_duration) > 0.1 or db_call_duration > self.VALIDATOR_QUERY_TIMEOUT / 4:
                    logger.info(f"[DBTrack {op_id}] EXITING {operation_type} op: {func.__name__}. DB call: {db_call_duration:.4f}s, Total in wrapper: {overall_duration:.4f}s. Query: {query_text_for_log}")
            
            return result
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
        system_running_event: Optional[asyncio.Event] = None
    ) -> None:
        """Initialize the validator database manager."""
        if not hasattr(self, '_initialized') or not self._initialized:
            db_name_env = os.getenv("DB_NAME", "validator_db")
            db_host_env = os.getenv("DB_HOST", "localhost")
            db_port_env = int(os.getenv("DB_PORT", 5432))
            db_user_env = os.getenv("DB_USER", "postgres")
            db_password_env = os.getenv("DB_PASSWORD", "postgres")

            super().__init__(
                node_type="validator",
                database=db_name_env,
                host=db_host_env,
                port=db_port_env,
                user=db_user_env,
                password=db_password_env,
            )
            
            # Store database name (might still be useful for logging/config)
            # self.database is now set by the super().__init__ call if it uses its 'database' param correctly
            # self.db_url is also now set by the super().__init__ call

            # Custom timeouts for validator operations
            self.VALIDATOR_QUERY_TIMEOUT = 60  # 1 minute
            self.VALIDATOR_TRANSACTION_TIMEOUT = 300  # 5 minutes
            
            self.node_table = node_table
            self.system_running_event = system_running_event
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
        if self.system_running_event:
            await self.system_running_event.wait()
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
                    "server_settings": {"application_name": f"gaia_validator_{os.getpid()}"} # Explicitly set application_name
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
        if self.system_running_event:
            await self.system_running_event.wait()
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
        """Update miner information in the node_table."""
        if self._storage_locked:
            logger.warning("Database storage is locked, update_miner_info operation skipped.")
            return

        async with self.session(f"update_miner_info_uid_{index}") as s:
            try:
                # The session context manager 's' already handles begin/commit/rollback.
                # No need for an inner 'async with s.begin()'
                
                # Check if miner exists
                existing_miner = await s.execute(
                    self.node_table.select().where(self.node_table.c.uid == index)
                )
                miner_row = existing_miner.fetchone()

                update_values = {
                    "hotkey": hotkey,
                    "coldkey": coldkey,
                    "ip": ip,
                    "ip_type": ip_type,
                    "port": port,
                    "incentive": float(incentive) if incentive is not None else None,
                    "stake": float(stake) if stake is not None else None,
                    "trust": float(trust) if trust is not None else None,
                    "vtrust": float(vtrust) if vtrust is not None else None,
                    "protocol": protocol,
                    "last_updated": datetime.now(timezone.utc)
                }
                
                # Remove None values to avoid overwriting existing data with None
                update_values = {k: v for k, v in update_values.items() if v is not None}


                if miner_row:
                    # Update existing miner
                    stmt = (
                        self.node_table.update()
                        .where(self.node_table.c.uid == index)
                        .values(**update_values)
                    )
                else:
                    # Insert new miner
                    stmt = self.node_table.insert().values(uid=index, **update_values)
                
                await s.execute(stmt)
                # No explicit commit needed here, handled by the session context manager.

                logger.debug(f"Successfully updated/inserted miner info for UID {index} with hotkey {hotkey}")

            except Exception as e:
                logger.error(f"Error updating miner info for UID {index} using SQLAlchemy update: {str(e)}")
                logger.error(traceback.format_exc())
                raise DatabaseError(f"Failed to update miner info for UID {index}: {str(e)}") from e

    @track_operation('write')
    async def batch_update_miners(self, miners_data: List[Dict[str, Any]]) -> None:
        """
        Update multiple miners using upsert (INSERT ... ON CONFLICT DO UPDATE).
        Args:
            miners_data: List of dictionaries containing miner update data.
                        Each dict should have 'index' and other miner fields.
        """
        if not miners_data:
            return
            
        valid_miners_to_update = []
        for miner_data in miners_data:
            index = miner_data.get('index')
            if index is None or not (0 <= index < 256):
                logger.warning(f"Skipping invalid miner index: {index}")
                continue
            valid_miners_to_update.append(miner_data)
        
        if not valid_miners_to_update:
            logger.warning("No valid miners to update after filtering")
            return

        updated_count = 0
        inserted_count = 0
        try:
            async with self.lightweight_session() as session:
                async with session.begin():
                    for miner_data in valid_miners_to_update:
                        index_val = miner_data['index']
                        
                        # Prepare values for upsert
                        insert_values = {
                            'uid': index_val,
                            'last_updated': datetime.now(timezone.utc)
                        }
                        update_values = {
                            'last_updated': datetime.now(timezone.utc)
                        }
                        
                        # Add fields that are present in miner_data
                        for field in ['hotkey', 'coldkey', 'ip', 'ip_type', 'port', 'incentive', 'stake', 'trust', 'vtrust', 'protocol']:
                            if field in miner_data:
                                insert_values[field] = miner_data[field]
                                update_values[field] = miner_data[field]

                        if not update_values:
                            logger.warning(f"No values to update for miner index {index_val}. Skipping.")
                            continue

                        # Use PostgreSQL's ON CONFLICT DO UPDATE for proper upsert
                        upsert_query = """
                        INSERT INTO node_table (uid, hotkey, coldkey, ip, ip_type, port, incentive, stake, trust, vtrust, protocol, last_updated)
                        VALUES (:uid, :hotkey, :coldkey, :ip, :ip_type, :port, :incentive, :stake, :trust, :vtrust, :protocol, :last_updated)
                        ON CONFLICT (uid) DO UPDATE SET
                            hotkey = EXCLUDED.hotkey,
                            coldkey = EXCLUDED.coldkey,
                            ip = EXCLUDED.ip,
                            ip_type = EXCLUDED.ip_type,
                            port = EXCLUDED.port,
                            incentive = EXCLUDED.incentive,
                            stake = EXCLUDED.stake,
                            trust = EXCLUDED.trust,
                            vtrust = EXCLUDED.vtrust,
                            protocol = EXCLUDED.protocol,
                            last_updated = EXCLUDED.last_updated
                        """
                        
                        result = await session.execute(text(upsert_query), insert_values)
                        
                        # Check if this was an insert or update by checking if the row existed before
                        check_query = "SELECT 1 FROM node_table WHERE uid = :uid AND last_updated = :last_updated"
                        exists_result = await session.execute(text(check_query), {
                            'uid': index_val, 
                            'last_updated': insert_values['last_updated']
                        })
                        
                        if exists_result.fetchone():
                            # We can't easily distinguish insert vs update with ON CONFLICT, 
                            # so we'll assume success and count all operations
                            updated_count += 1
                            
            logger.info(f"Successfully batch processed {updated_count} miners (upserts executed in one transaction).")
                        
        except Exception as e:
            logger.error(f"Error in batch_update_miners: {str(e)}")
            logger.error(traceback.format_exc())
            raise DatabaseError(f"Failed to batch update miners: {str(e)}")

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
                # Use high-performance JSON serialization
                prediction_json = dumps(prediction, default=self._json_serializer)
                if JSON_PERFORMANCE_AVAILABLE:
                    logger.debug("Using orjson for database prediction serialization")
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
                
            raw_prediction_from_db = result['prediction']
            prediction_data: Any

            if isinstance(raw_prediction_from_db, (dict, list)):
                prediction_data = raw_prediction_from_db
            elif isinstance(raw_prediction_from_db, str):
                try:
                    # Use high-performance JSON deserialization
                    prediction_data = loads(raw_prediction_from_db)
                    if JSON_PERFORMANCE_AVAILABLE:
                        logger.debug("Using orjson for database prediction deserialization")
                except Exception as e:
                    logger.error(f"Failed to parse JSON string from DB for baseline prediction '{task_name}' task_id '{task_id}': {raw_prediction_from_db}. Error: {e}")
                    return None 
            elif isinstance(raw_prediction_from_db, (int, float, bool)) or raw_prediction_from_db is None:
                prediction_data = raw_prediction_from_db
            else:
                logger.error(f"Unexpected type for baseline prediction from DB for '{task_name}' task_id '{task_id}': {type(raw_prediction_from_db)}. Value: {raw_prediction_from_db}")
                return None 
                
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
                # If an external session is passed, assume the caller manages the transaction
                result = await session.execute(text(query), params or {})
                return result
            else:
                # Create a new session and manage the transaction explicitly
                # BaseDatabaseManager.session() now ensures a transaction is started on new_session.
                async with self.session(operation_name=f"execute_new_session_query_snippet_{query[:30]}") as new_session:
                    try:
                        # No longer need new_session.begin() here.
                        result = await new_session.execute(text(query), params or {})
                        # BaseDatabaseManager.session() will handle the commit on successful exit.
                        logger.debug(f"Query executed successfully within session {id(new_session)} for query: {query[:100]}...")
                        return result
                    except asyncio.CancelledError:
                        logger.warning(f"Execute operation cancelled for session {id(new_session)} query: {query[:100]}...")
                        # Rollback will be handled by BaseDatabaseManager.session's except block.
                        raise # Re-raise CancelledError to be caught by BaseDatabaseManager.session
                    except Exception as e_inner:
                        logger.error(f"Error during query for session {id(new_session)} (query: {query[:100]}...): {e_inner}.")
                        # Rollback will be handled by BaseDatabaseManager.session's except block.
                        raise # Re-raise the original query execution error to be caught by BaseDatabaseManager.session
        except Exception as e:
            # Avoid re-logging if already logged by the inner exception block
            if not isinstance(e, DatabaseError): # Assuming DatabaseError is raised by self.session() or explicitly
                 logger.error(f"Error executing query (outer): {str(e)}")
                 logger.error(traceback.format_exc())
            raise DatabaseError(f"Failed to execute query: {str(e)}")