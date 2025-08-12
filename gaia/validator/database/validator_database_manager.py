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

T = TypeVar("T")


def track_operation(operation_type: str):
    """Decorator to track database operations."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(self: "ValidatorDatabaseManager", *args, **kwargs) -> T:
            if self.system_running_event:
                await self.system_running_event.wait()

            query_text_for_log = "N/A"
            if args:
                if isinstance(args[0], str):
                    query_text_for_log = args[0][:200].replace("\n", " ") + "..."
                elif hasattr(args[0], "__str__"):
                    query_text_for_log = str(args[0])[:200].replace("\n", " ") + "..."

            if query_text_for_log == "N/A" and kwargs.get("query"):
                if isinstance(kwargs["query"], str):
                    query_text_for_log = (
                        kwargs["query"][:200].replace("\n", " ") + "..."
                    )
                elif hasattr(kwargs["query"], "__str__"):
                    query_text_for_log = (
                        str(kwargs["query"])[:200].replace("\n", " ") + "..."
                    )

            if func.__name__ == "batch_update_miners":
                query_text_for_log = (
                    "Batch operation (see function logs for individual queries)"
                )
            elif func.__name__ == "update_miner_info" and args:
                query_text_for_log = (
                    f"UPDATE node_table SET ... WHERE uid={args[0] if args else 'N/A'}"
                )

            op_id = random.randint(10000, 99999)

            overall_start_time = time.perf_counter()
            # Reduce verbosity for advisory lock noise
            _lower_noise = False
            _q = (query_text_for_log or "").lower()
            if "pg_advisory" in _q:
                _lower_noise = True

            if _lower_noise:
                logger.debug(
                    f"[DBTrack {op_id}] ENTERING {operation_type} op: {func.__name__}, Query: {query_text_for_log}"
                )
            else:
                logger.info(
                    f"[DBTrack {op_id}] ENTERING {operation_type} op: {func.__name__}, Query: {query_text_for_log}"
                )

            db_call_start_time = 0.0
            db_call_duration = 0.0
            result = None

            try:
                db_call_start_time = time.perf_counter()
                result = await func(self, *args, **kwargs)
                db_call_duration = time.perf_counter() - db_call_start_time

                self._operation_stats[f"{operation_type}_operations"] += 1

                if db_call_duration > self.VALIDATOR_QUERY_TIMEOUT / 2:
                    self._operation_stats["long_running_queries"].append(
                        {
                            "operation": func.__name__,
                            "query_snippet": query_text_for_log,
                            "duration": db_call_duration,
                            "timestamp": time.time(),
                        }
                    )
                    if _lower_noise:
                        logger.debug(
                            f"[DBTrack {op_id}] Long-running DB call for {operation_type} op: {func.__name__} detected: {db_call_duration:.4f}s. Query: {query_text_for_log}"
                        )
                    else:
                        logger.warning(
                            f"[DBTrack {op_id}] Long-running DB call for {operation_type} op: {func.__name__} detected: {db_call_duration:.4f}s. Query: {query_text_for_log}"
                        )
                else:
                    pass

            except Exception as e:
                db_call_duration = time.perf_counter() - db_call_start_time
                if _lower_noise:
                    logger.debug(
                        f"[DBTrack {op_id}] ERROR in {operation_type} op: {func.__name__} after {db_call_duration:.4f}s in DB call. Query: {query_text_for_log}. Error: {str(e)}",
                        exc_info=True,
                    )
                else:
                    logger.error(
                        f"[DBTrack {op_id}] ERROR in {operation_type} op: {func.__name__} after {db_call_duration:.4f}s in DB call. Query: {query_text_for_log}. Error: {str(e)}",
                        exc_info=True,
                    )
                raise
            finally:
                overall_duration = time.perf_counter() - overall_start_time
                if (
                    abs(overall_duration - db_call_duration) > 0.1
                    or db_call_duration > self.VALIDATOR_QUERY_TIMEOUT / 4
                ):
                    if _lower_noise:
                        logger.debug(
                            f"[DBTrack {op_id}] EXITING {operation_type} op: {func.__name__}. DB call: {db_call_duration:.4f}s, Total in wrapper: {overall_duration:.4f}s. Query: {query_text_for_log}"
                        )
                    else:
                        logger.info(
                            f"[DBTrack {op_id}] EXITING {operation_type} op: {func.__name__}. DB call: {db_call_duration:.4f}s, Total in wrapper: {overall_duration:.4f}s. Query: {query_text_for_log}"
                        )

            return result

        return wrapper

    return decorator


class ValidatorDatabaseManager(BaseDatabaseManager):
    """
    Database manager specifically for validator nodes.
    Handles all validator-specific database operations.
    """

    def __new__(cls, *args, **kwargs) -> "ValidatorDatabaseManager":
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls, node_type="validator")
            cls._instance._initialized = False
            cls._instance._storage_locked = False  # Add storage lock flag

            # Initialize all required base class attributes
            cls._instance._circuit_breaker = {
                "failures": 0,
                "last_failure_time": 0,
                "status": "closed",  # 'closed', 'open', 'half-open'
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
                "cpu_percent": 0,
                "memory_rss": 0,
                "open_files": 0,
                "connections": 0,
                "last_check": 0,
            }

            # Initialize engine placeholders
            cls._instance._engine = None
            cls._instance._session_factory = None

            # Initialize database connection parameters with defaults
            cls._instance.db_url = None
            cls._instance.VALIDATOR_QUERY_TIMEOUT = 60  # 1 minute
            cls._instance.VALIDATOR_TRANSACTION_TIMEOUT = 300  # 5 minutes

            # Advisory lock key used to signal cluster-wide maintenance/pause.
            # Convention: exclusive lock held by maintenance; workers probe with shared try-lock.
            cls._instance.DB_PAUSE_LOCK_KEY = 746227728439  # arbitrary bigint, stable

        return cls._instance

    def __init__(
        self,
        database: str = "validator_db",
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "postgres",
        system_running_event: Optional[asyncio.Event] = None,
    ) -> None:
        """Initialize the validator database manager."""
        if not hasattr(self, "_initialized") or not self._initialized:
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

    async def _ping_ready(self) -> bool:
        """Return True if the database responds to a simple SELECT 1."""
        try:
            await self.fetch_one("SELECT 1")
            return True
        except Exception as e:
            msg = str(e)
            # Expected transient states during restarts/backups
            transient = (
                "shutting down" in msg
                or "starting up" in msg
                or "CannotConnectNow" in msg
            )
            if transient:
                return False
            # For other errors, re-raise so callers can see them
            raise

    async def _is_paused_by_lock(self) -> bool:
        """Return True if an exclusive advisory lock is held elsewhere (maintenance)."""
        try:
            row = await self.fetch_one(
                "SELECT pg_try_advisory_lock_shared(:key) AS ok",
                {"key": self.DB_PAUSE_LOCK_KEY},
            )
            ok = bool(row and (row.get("ok") in (True, 1, "t")))
            if ok:
                # Release immediately; we only probe state
                await self.execute(
                    "SELECT pg_advisory_unlock_shared(:key)",
                    {"key": self.DB_PAUSE_LOCK_KEY},
                )
                return False
            return True
        except Exception:
            # If DB is not reachable, treat as unavailable rather than paused
            return False

    async def wait_until_available(self, max_wait_seconds: int = 180) -> None:
        """Block until DB is reachable and not under maintenance (advisory lock).

        This is cooperative: if a maintenance process holds an exclusive
        advisory lock on DB_PAUSE_LOCK_KEY, workers will wait here.
        """
        start = time.time()
        backoff = 0.5
        while True:
            try:
                paused = await self._is_paused_by_lock()
                ready = await self._ping_ready()
            except Exception:
                paused = False
                ready = False

            if ready and not paused:
                return

            if time.time() - start > max_wait_seconds:
                return  # give up silently; caller may retry later

            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 5.0)

    async def get_operation_stats(self) -> Dict[str, Any]:
        """Get current operation statistics."""
        stats = self._operation_stats.copy()
        stats.update(
            {
                "active_sessions": len(self._active_sessions),
                "active_operations": self._active_operations,
                "pool_health": self._pool_health_status,
                "circuit_breaker_status": self._circuit_breaker["status"],
            }
        )
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
                split_at = masked_url.find("@")
                if split_at != -1:
                    split_protocol = masked_url.find("://")
                    if split_protocol != -1:
                        masked_url = (
                            masked_url[: split_protocol + 3]
                            + "***:***@"
                            + masked_url[split_at + 1 :]
                        )
            except Exception:
                pass  # Keep original URL if masking fails
            logger.info(
                f"Attempting to initialize main database engine for: {masked_url}"
            )

            # Create our main engine pointing directly to the application DB
            self._engine = create_async_engine(
                self.db_url,
                pool_size=self.MAX_CONNECTIONS,  # Use class attribute
                max_overflow=10,
                pool_timeout=self.DEFAULT_CONNECTION_TIMEOUT,  # Use base class attribute
                pool_recycle=300,
                pool_pre_ping=True,
                echo=False,
                connect_args={
                    "command_timeout": self.VALIDATOR_QUERY_TIMEOUT,  # Use validator timeout
                    "timeout": self.DEFAULT_CONNECTION_TIMEOUT,  # Use base class connection timeout
                    "server_settings": {
                        "application_name": f"gaia_validator_{os.getpid()}"
                    },  # Explicitly set application_name
                },
            )

            # Initialize session factory
            self._session_factory = async_sessionmaker(
                self._engine,
                expire_on_commit=False,
                class_=AsyncSession,
                autobegin=False,
            )

            # Test the connection to the application database
            async with self._engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

            logger.info(
                f"Successfully initialized database engine for {self.node_type} node."
            )
        except Exception as e:
            logger.error(f"Failed to initialize main database engine: {str(e)}")
            logger.error(traceback.format_exc())
            # Ensure engine and factory are None if init fails
            self._engine = None
            self._session_factory = None
            raise DatabaseError(
                f"Failed to initialize database engine: {str(e)}"
            ) from e

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
            logger.info(
                "Ensuring database engine is initialized (schema assumed to exist)..."
            )
            # Ensure engine is created and connection is tested
            await self.ensure_engine_initialized()
            logger.info("Database engine initialization check complete.")
            # Removed calls to:
            # _create_node_table, _create_trigger_function, _create_trigger,
            # _initialize_rows, create_score_table, create_baseline_predictions_table,
            # _initialize_validator_database, load_task_schemas, initialize_task_tables
        except Exception as e:
            logger.error(
                f"Error during simplified database initialization check: {str(e)}"
            )
            # Decide if this should re-raise or just log
            raise DatabaseError(
                f"Failed during simplified initialization: {str(e)}"
            ) from e

    @track_operation("read")
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

            rows = await self.fetch_all(
                query, {"task_type": task_type, "three_days_ago": three_days_ago}
            )

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

            # Reset operation stats to initial state (keeping all required keys)
            self._operation_stats = {
                "ddl_operations": 0,
                "read_operations": 0,
                "write_operations": 0,
                "long_running_queries": [],
                "total_sessions_acquired": 0,
                "total_session_time_ms": 0.0,
                "max_session_time_ms": 0.0,
                "min_session_time_ms": float("inf"),
                "avg_session_time_ms": 0.0,
                "top_long_sessions": [],
            }
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")
            logger.error(traceback.format_exc())
            raise DatabaseError(f"Failed to close database connections: {str(e)}")

    async def get_raw_connection(self):
        """Get a raw AsyncConnection for use with external libraries like MinerPerformanceCalculator."""
        if not self._engine:
            raise DatabaseError("Database engine not initialized")
        return await self._engine.connect()

    @track_operation("write")
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
            logger.warning(
                "Database storage is locked, update_miner_info operation skipped."
            )
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
                    "last_updated": datetime.now(timezone.utc),
                }

                # Remove None values to avoid overwriting existing data with None
                update_values = {
                    k: v for k, v in update_values.items() if v is not None
                }

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

                logger.debug(
                    f"Successfully updated/inserted miner info for UID {index} with hotkey {hotkey}"
                )

            except Exception as e:
                logger.error(
                    f"Error updating miner info for UID {index} using SQLAlchemy update: {str(e)}"
                )
                logger.error(traceback.format_exc())
                raise DatabaseError(
                    f"Failed to update miner info for UID {index}: {str(e)}"
                ) from e

    @track_operation("write")
    async def batch_update_miners(self, miners_data: List[Dict[str, Any]]) -> None:
        """
        Update multiple miners using chunked upsert operations to prevent timeouts.
        Args:
            miners_data: List of dictionaries containing miner update data.
                        Each dict should have 'index' and other miner fields.
        """
        if not miners_data:
            return

        valid_miners_to_update = []
        for miner_data in miners_data:
            index = miner_data.get("index")
            if index is None or not (0 <= index < 256):
                logger.warning(f"Skipping invalid miner index: {index}")
                continue
            valid_miners_to_update.append(miner_data)

        if not valid_miners_to_update:
            logger.warning("No valid miners to update after filtering")
            return

        # CHUNKING: Process miners in chunks to prevent timeouts and database locks
        chunk_size = 50  # Process 50 miners at a time
        total_processed = 0

        logger.info(
            f"Processing {len(valid_miners_to_update)} miners in chunks of {chunk_size}"
        )

        try:
            for chunk_start in range(0, len(valid_miners_to_update), chunk_size):
                chunk_end = min(chunk_start + chunk_size, len(valid_miners_to_update))
                chunk_miners = valid_miners_to_update[chunk_start:chunk_end]

                logger.debug(
                    f"Processing miner chunk {chunk_start//chunk_size + 1}: miners {chunk_start+1}-{chunk_end}"
                )

                chunk_processed = 0

                # Process this chunk in a separate session with timeout
                try:
                    # Use asyncio.wait_for to add timeout to the entire chunk operation
                    chunk_processed = await asyncio.wait_for(
                        self._process_miner_chunk(chunk_miners),
                        timeout=30.0,  # 30 second timeout per chunk
                    )
                    total_processed += chunk_processed

                except asyncio.TimeoutError:
                    logger.error(
                        f"Chunk {chunk_start//chunk_size + 1} timed out after 30 seconds"
                    )
                    # Try to process individually as fallback
                    for miner_data in chunk_miners:
                        try:
                            await asyncio.wait_for(
                                self._process_single_miner(miner_data),
                                timeout=5.0,  # 5 second timeout per individual miner
                            )
                            total_processed += 1
                        except asyncio.TimeoutError:
                            logger.error(
                                f"Individual miner UID {miner_data.get('index')} timed out"
                            )
                        except Exception as individual_e:
                            logger.error(
                                f"Error processing individual miner UID {miner_data.get('index')}: {individual_e}"
                            )

                except Exception as chunk_e:
                    logger.error(
                        f"Error processing chunk {chunk_start//chunk_size + 1}: {chunk_e}"
                    )
                    # Try individual fallback for this chunk
                    for miner_data in chunk_miners:
                        try:
                            await self._process_single_miner(miner_data)
                            total_processed += 1
                        except Exception as individual_e:
                            logger.error(
                                f"Error processing individual miner UID {miner_data.get('index')}: {individual_e}"
                            )

                # Small delay between chunks to reduce database pressure
                await asyncio.sleep(0.1)

            logger.info(
                f"Successfully batch processed {total_processed}/{len(valid_miners_to_update)} miners"
            )

        except Exception as e:
            logger.error(f"Error in batch_update_miners: {str(e)}")
            logger.error(traceback.format_exc())
            raise DatabaseError(f"Failed to batch update miners: {str(e)}")

    async def _process_miner_chunk(self, chunk_miners: List[Dict[str, Any]]) -> int:
        """Process a chunk of miners in a single transaction."""
        processed_count = 0

        async with self.lightweight_session() as session:
            async with session.begin():
                for miner_data in chunk_miners:
                    index_val = miner_data["index"]

                    # Prepare values for upsert
                    insert_values = {
                        "uid": index_val,
                        "last_updated": datetime.now(timezone.utc),
                    }

                    # Add fields that are present in miner_data
                    for field in [
                        "hotkey",
                        "coldkey",
                        "ip",
                        "ip_type",
                        "port",
                        "incentive",
                        "stake",
                        "trust",
                        "vtrust",
                        "protocol",
                    ]:
                        if field in miner_data:
                            insert_values[field] = miner_data[field]

                    if len(insert_values) <= 2:  # Only uid and last_updated
                        logger.warning(
                            f"No values to update for miner index {index_val}. Skipping."
                        )
                        continue

                    # Use PostgreSQL's ON CONFLICT DO UPDATE for efficient upsert
                    # REMOVED: unnecessary existence check that was causing the timeout
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

                    await session.execute(text(upsert_query), insert_values)
                    processed_count += 1

        return processed_count

    async def _process_single_miner(self, miner_data: Dict[str, Any]) -> None:
        """Process a single miner as fallback when batch operations fail."""
        await self.update_miner_info(
            index=miner_data["index"],
            hotkey=miner_data.get("hotkey"),
            coldkey=miner_data.get("coldkey"),
            ip=miner_data.get("ip"),
            ip_type=miner_data.get("ip_type"),
            port=miner_data.get("port"),
            incentive=miner_data.get("incentive"),
            stake=miner_data.get("stake"),
            trust=miner_data.get("trust"),
            vtrust=miner_data.get("vtrust"),
            protocol=miner_data.get("protocol"),
        )

    @track_operation("read")
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

    @track_operation("read")
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

    @track_operation("write")
    async def remove_miner_from_score_tables(
        self,
        uids: List[int],
        task_names: List[str],
        filter_start_time: Optional[datetime] = None,
        filter_end_time: Optional[datetime] = None,
    ) -> None:
        """
        Partially remove specified miners from 'score_table' rows for given task types,
        preserving data for all other miners. Sets the departing miners' score columns to 0.0.
        Filters by a time window if filter_start_time and filter_end_time are provided.

        Args:
            uids (List[int]): List of miner UIDs to be zeroed out.
            task_names (List[str]): List of task names to apply the removal.
            filter_start_time (Optional[datetime]): If provided, only process rows where created_at >= this time.
            filter_end_time (Optional[datetime]): If provided, only process rows where created_at <= this time.
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
                # Build UPDATE query with individual column updates
                for uid in uids:
                    if 0 <= uid < 256:
                        column_name = f"uid_{uid}_score"

                        # Build base query
                        update_sql = f"""
                            UPDATE score_table
                            SET {column_name} = 0.0
                            WHERE task_name = :task_name
                              AND {column_name} IS NOT NULL
                              AND {column_name} != 0.0
                        """

                        params = {"task_name": task_name}

                        # Add time filters if provided (use created_at timestamp column)
                        time_conditions = []
                        if filter_start_time:
                            time_conditions.append("created_at >= :start_time")
                            params["start_time"] = filter_start_time
                        if filter_end_time:
                            time_conditions.append("created_at <= :end_time")
                            params["end_time"] = filter_end_time

                        if time_conditions:
                            update_sql += " AND " + " AND ".join(time_conditions)

                        # Execute update
                        result = await self.execute(update_sql, params)

                        if hasattr(result, "rowcount") and result.rowcount > 0:
                            total_rows_updated += result.rowcount
                            logger.debug(
                                f"Zeroed out {result.rowcount} scores for UID {uid} in task {task_name}"
                            )

                logger.info(
                    f"Task {task_name}: Completed score zeroing for UIDs {uids}"
                )

            except Exception as e:
                logger.error(
                    f"Error in remove_miner_from_score_tables for task '{task_name}': {e}"
                )
                logger.error(traceback.format_exc())

        logger.info(f"Score zeroing complete. Total updates: {total_rows_updated}")

    @track_operation("write")
    async def store_baseline_prediction(
        self,
        task_name: str,
        task_id: str,
        timestamp: datetime,
        prediction: Any,
        region_id: Optional[str] = None,
    ) -> bool:
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
                "prediction": prediction_json,
            }

            await self.execute(insert_sql, params)
            return True

        except Exception as e:
            logger.error(f"DB: Error storing prediction: {e}")
            return False

    @track_operation("read")
    async def get_baseline_prediction(
        self, task_name: str, task_id: str, region_id: Optional[str] = None
    ) -> Optional[Dict]:
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
            # Use more specific query optimization
            # Only select needed columns to reduce data transfer
            query = """
            SELECT id, task_name, task_id, region_id, timestamp, prediction, created_at
            FROM baseline_predictions 
            WHERE task_name = :task_name 
            AND task_id = :task_id
            """

            params = {"task_name": task_name, "task_id": task_id}

            if region_id:
                query += " AND region_id = :region_id"
                params["region_id"] = region_id

            # Add explicit ordering and limiting for faster execution
            query += " ORDER BY created_at DESC LIMIT 1"

            # Use a shorter timeout for this specific query since it should be fast
            result = await asyncio.wait_for(
                self.fetch_one(query, params),
                timeout=30.0,  # 30 second timeout instead of default 120s
            )

            if not result:
                logger.debug(
                    f"No baseline prediction found for {task_name}, task_id: {task_id}, region: {region_id}"
                )
                return None

            raw_prediction_from_db = result["prediction"]
            prediction_data: Any

            if isinstance(raw_prediction_from_db, (dict, list)):
                prediction_data = raw_prediction_from_db
            elif isinstance(raw_prediction_from_db, str):
                try:
                    # Use high-performance JSON deserialization
                    prediction_data = loads(raw_prediction_from_db)
                    if JSON_PERFORMANCE_AVAILABLE:
                        logger.debug(
                            "Using orjson for database prediction deserialization"
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to parse JSON string from DB for baseline prediction '{task_name}' task_id '{task_id}': {raw_prediction_from_db}. Error: {e}"
                    )
                    return None
            elif (
                isinstance(raw_prediction_from_db, (int, float, bool))
                or raw_prediction_from_db is None
            ):
                prediction_data = raw_prediction_from_db
            else:
                logger.error(
                    f"Unexpected type for baseline prediction from DB for '{task_name}' task_id '{task_id}': {type(raw_prediction_from_db)}. Value: {raw_prediction_from_db}"
                )
                return None

            return {
                "task_name": result["task_name"],
                "task_id": result["task_id"],
                "region_id": result["region_id"],
                "timestamp": result["timestamp"],
                "prediction": prediction_data,
                "created_at": result["created_at"],
            }

        except asyncio.TimeoutError:
            logger.error(
                f"Baseline prediction query timed out for {task_name}, task_id: {task_id}, region: {region_id}"
            )
            # For timeout, return None but log as error for monitoring
            return None
        except Exception as e:
            logger.error(f"Error retrieving baseline prediction: {e}")
            logger.error(traceback.format_exc())
            return None

    @track_operation("read")
    async def get_baseline_predictions_by_task_name(
        self, task_name: str, limit: int = 100
    ) -> List[Dict]:
        """
        Retrieve recent baseline predictions for a specific task type.
        This method is optimized for queries that only filter by task_name.

        Args:
            task_name: Name of the task
            limit: Maximum number of predictions to return

        Returns:
            List[Dict]: List of prediction data
        """
        try:
            # Optimized query for task_name-only filtering
            query = """
            SELECT id, task_name, task_id, region_id, timestamp, created_at
            FROM baseline_predictions 
            WHERE task_name = :task_name
            ORDER BY created_at DESC 
            LIMIT :limit
            """

            params = {"task_name": task_name, "limit": limit}

            # Use a shorter timeout for this query
            results = await asyncio.wait_for(
                self.fetch_all(query, params), timeout=60.0  # 60 second timeout
            )

            if not results:
                logger.debug(
                    f"No baseline predictions found for task_name: {task_name}"
                )
                return []

            return [dict(row) for row in results]

        except asyncio.TimeoutError:
            logger.error(
                f"Baseline predictions query by task_name timed out for {task_name}"
            )
            return []
        except Exception as e:
            logger.error(f"Error retrieving baseline predictions by task_name: {e}")
            logger.error(traceback.format_exc())
            return []

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

    @track_operation("write")
    async def execute(
        self,
        query: str,
        params: Optional[Dict] = None,
        session: Optional[AsyncSession] = None,
    ) -> Any:
        """Execute a SQL query with parameters."""
        try:
            if self._storage_locked and any(
                keyword in query.lower() for keyword in ["insert", "update", "delete"]
            ):
                logger.warning("Storage is locked - skipping write operation")
                return None

            if session:
                # If an external session is passed, assume the caller manages the transaction
                # Handle both string queries (wrap with text()) and SQLAlchemy objects (execute directly)
                if isinstance(query, str):
                    result = await session.execute(text(query), params or {})
                else:
                    result = await session.execute(query, params or {})
                return result
            else:
                # Create a new session and manage the transaction explicitly
                # BaseDatabaseManager.session() now ensures a transaction is started on new_session.
                # Handle both string queries and SQLAlchemy objects
                query_str = str(query) if hasattr(query, "__str__") else query
                query_snippet = (
                    query_str[:30]
                    if isinstance(query_str, str)
                    else "SQLAlchemy_object"
                )

                async with self.session(
                    operation_name=f"execute_new_session_query_snippet_{query_snippet}"
                ) as new_session:
                    try:
                        # No longer need new_session.begin() here.
                        # Handle both string queries (wrap with text()) and SQLAlchemy objects (execute directly)
                        if isinstance(query, str):
                            result = await new_session.execute(
                                text(query), params or {}
                            )
                        else:
                            # Query is already a SQLAlchemy object, execute directly
                            result = await new_session.execute(query, params or {})
                        # BaseDatabaseManager.session() will handle the commit on successful exit.
                        query_log = (
                            query_str[:100]
                            if isinstance(query_str, str)
                            else str(query)[:100]
                        )
                        logger.debug(
                            f"Query executed successfully within session {id(new_session)} for query: {query_log}..."
                        )
                        return result
                    except asyncio.CancelledError:
                        query_log = (
                            query_str[:100]
                            if isinstance(query_str, str)
                            else str(query)[:100]
                        )
                        logger.warning(
                            f"Execute operation cancelled for session {id(new_session)} query: {query_log}..."
                        )
                        # Rollback will be handled by BaseDatabaseManager.session's except block.
                        raise  # Re-raise CancelledError to be caught by BaseDatabaseManager.session
                    except Exception as e_inner:
                        query_log = (
                            query_str[:100]
                            if isinstance(query_str, str)
                            else str(query)[:100]
                        )
                        logger.error(
                            f"Error during query for session {id(new_session)} (query: {query_log}...): {e_inner}."
                        )
                        # Rollback will be handled by BaseDatabaseManager.session's except block.
                        raise  # Re-raise the original query execution error to be caught by BaseDatabaseManager.session
        except Exception as e:
            # Avoid re-logging if already logged by the inner exception block
            if not isinstance(
                e, DatabaseError
            ):  # Assuming DatabaseError is raised by self.session() or explicitly
                logger.error(f"Error executing query (outer): {str(e)}")
                logger.error(traceback.format_exc())
            raise DatabaseError(f"Failed to execute query: {str(e)}")

    @track_operation("write")
    async def cleanup_old_baseline_predictions(
        self, days_to_keep: int = 30, batch_size: int = 1000
    ) -> int:
        """
        Clean up old baseline predictions to prevent table bloat.

        Args:
            days_to_keep: Number of days of predictions to keep
            batch_size: Number of records to delete per batch

        Returns:
            int: Number of records deleted
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)

            # First, count how many records would be deleted
            count_query = """
            SELECT COUNT(*) as count 
            FROM baseline_predictions 
            WHERE created_at < :cutoff_date
            """

            count_result = await self.fetch_one(
                count_query, {"cutoff_date": cutoff_date}
            )
            total_to_delete = count_result["count"] if count_result else 0

            if total_to_delete == 0:
                logger.info("No old baseline predictions to clean up")
                return 0

            logger.info(
                f"Found {total_to_delete} old baseline predictions to delete (older than {days_to_keep} days)"
            )

            # Delete in batches to avoid long-running transactions
            total_deleted = 0
            while True:
                delete_query = """
                DELETE FROM baseline_predictions 
                WHERE id IN (
                    SELECT id FROM baseline_predictions 
                    WHERE created_at < :cutoff_date 
                    ORDER BY created_at ASC 
                    LIMIT :batch_size
                )
                """

                result = await self.execute(
                    delete_query, {"cutoff_date": cutoff_date, "batch_size": batch_size}
                )

                deleted_count = (
                    result.rowcount if hasattr(result, "rowcount") else batch_size
                )
                total_deleted += deleted_count

                if deleted_count < batch_size:
                    # No more records to delete
                    break

                # Small delay between batches to reduce database load
                await asyncio.sleep(0.1)

            logger.info(f"Cleaned up {total_deleted} old baseline predictions")
            return total_deleted

        except Exception as e:
            logger.error(f"Error cleaning up old baseline predictions: {e}")
            logger.error(traceback.format_exc())
            return 0

    @track_operation("read")
    async def get_database_performance_stats(self) -> Dict[str, Any]:
        """
        Get database performance statistics to help identify issues.

        Returns:
            Dict containing database performance metrics
        """
        try:
            stats = {}

            # Connection pool stats
            if self._engine and hasattr(self._engine.pool, "size"):
                pool = self._engine.pool
                stats["connection_pool"] = {
                    "size": pool.size(),
                    "checked_in": pool.checkedin(),
                    "checked_out": pool.checkedout(),
                    "overflow": pool.overflow(),
                    "invalidated": pool.invalidated(),
                }

            # Table size statistics
            table_stats_query = """
            SELECT 
                schemaname,
                tablename,
                attname,
                n_distinct,
                correlation,
                null_frac
            FROM pg_stats 
            WHERE schemaname = 'public' 
            AND tablename = 'baseline_predictions'
            ORDER BY attname;
            """

            table_stats = await self.fetch_all(table_stats_query)
            stats["table_statistics"] = table_stats

            # Index usage statistics
            index_stats_query = """
            SELECT 
                indexrelname as index_name,
                idx_tup_read,
                idx_tup_fetch,
                idx_scan,
                schemaname,
                tablename
            FROM pg_stat_user_indexes 
            WHERE tablename = 'baseline_predictions'
            ORDER BY idx_scan DESC;
            """

            index_stats = await self.fetch_all(index_stats_query)
            stats["index_usage"] = index_stats

            # Recent slow queries (if pg_stat_statements is available)
            try:
                slow_queries_query = """
                SELECT 
                    query,
                    calls,
                    total_time,
                    mean_time,
                    rows
                FROM pg_stat_statements 
                WHERE query ILIKE '%baseline_predictions%'
                ORDER BY mean_time DESC 
                LIMIT 10;
                """

                slow_queries = await self.fetch_all(slow_queries_query)
                stats["slow_queries"] = slow_queries
            except Exception:
                # pg_stat_statements extension might not be available
                stats["slow_queries"] = "pg_stat_statements extension not available"

            # Table size information
            table_size_query = """
            SELECT 
                pg_size_pretty(pg_total_relation_size('baseline_predictions')) as total_size,
                pg_size_pretty(pg_relation_size('baseline_predictions')) as table_size,
                pg_size_pretty(pg_total_relation_size('baseline_predictions') - pg_relation_size('baseline_predictions')) as index_size,
                (SELECT COUNT(*) FROM baseline_predictions) as row_count;
            """

            size_info = await self.fetch_one(table_size_query)
            stats["table_size"] = size_info

            return stats

        except Exception as e:
            logger.error(f"Error getting database performance stats: {e}")
            return {"error": str(e)}

    @track_operation("write")
    async def optimize_baseline_predictions_table(self) -> Dict[str, Any]:
        """
        Run maintenance operations on the baseline_predictions table.

        Returns:
            Dict containing results of optimization operations
        """
        try:
            results = {}

            # Analyze table statistics
            analyze_query = "ANALYZE baseline_predictions;"
            await self.execute(analyze_query)
            results["analyze"] = "completed"

            # Vacuum the table (not FULL to avoid locking)
            vacuum_query = "VACUUM baseline_predictions;"
            await self.execute(vacuum_query)
            results["vacuum"] = "completed"

            # Check for unused indexes
            unused_indexes_query = """
            SELECT 
                schemaname,
                tablename,
                indexname,
                idx_scan,
                idx_tup_read,
                idx_tup_fetch
            FROM pg_stat_user_indexes 
            WHERE tablename = 'baseline_predictions'
            AND idx_scan = 0;
            """

            unused_indexes = await self.fetch_all(unused_indexes_query)
            results["unused_indexes"] = unused_indexes

            # Clean up old predictions (keep last 30 days)
            cleaned_count = await self.cleanup_old_baseline_predictions(days_to_keep=30)
            results["cleaned_old_records"] = cleaned_count

            logger.info(f"Baseline predictions table optimization completed: {results}")
            return results

        except Exception as e:
            logger.error(f"Error optimizing baseline predictions table: {e}")
            return {"error": str(e)}

    @track_operation("write")
    async def cleanup_old_geomagnetic_predictions(
        self, days_to_keep: int = 7, batch_size: int = 1000
    ) -> int:
        """
        Clean up old geomagnetic predictions to prevent table bloat.

        Args:
            days_to_keep: Number of days of predictions to keep
            batch_size: Number of records to delete per batch

        Returns:
            int: Number of records deleted
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)

            # First, count how many records would be deleted
            count_query = """
            SELECT COUNT(*) as count_to_delete 
            FROM geomagnetic_predictions 
            WHERE query_time < :cutoff_date
            """
            count_result = await self.fetch_one(
                count_query, {"cutoff_date": cutoff_date}
            )
            total_to_delete = count_result["count_to_delete"] if count_result else 0

            if total_to_delete == 0:
                logger.info(
                    f"No old geomagnetic predictions to clean up (cutoff: {cutoff_date})"
                )
                return 0

            logger.info(
                f"Cleaning up {total_to_delete} old geomagnetic prediction records (older than {cutoff_date})"
            )

            # Delete in batches to avoid long-running transactions
            total_deleted = 0
            while True:
                delete_query = """
                DELETE FROM geomagnetic_predictions 
                WHERE id IN (
                    SELECT id FROM geomagnetic_predictions 
                    WHERE query_time < :cutoff_date 
                    ORDER BY query_time ASC 
                    LIMIT :batch_size
                )
                """

                result = await self.execute(
                    delete_query, {"cutoff_date": cutoff_date, "batch_size": batch_size}
                )

                if hasattr(result, "rowcount") and result.rowcount == 0:
                    break

                total_deleted += batch_size

                # Log progress every 5000 records
                if total_deleted % 5000 == 0:
                    logger.info(
                        f"Deleted {total_deleted}/{total_to_delete} old geomagnetic predictions..."
                    )

                # Brief pause between batches to avoid overwhelming the database
                await asyncio.sleep(0.1)

                if total_deleted >= total_to_delete:
                    break

            logger.info(
                f"Successfully cleaned up {total_deleted} old geomagnetic prediction records"
            )
            return total_deleted

        except Exception as e:
            logger.error(f"Error cleaning up old geomagnetic predictions: {e}")
            logger.error(traceback.format_exc())
            return 0

    @track_operation("write")
    async def optimize_geomagnetic_predictions_table(self) -> Dict[str, Any]:
        """
        Run maintenance operations on the geomagnetic_predictions table.

        Returns:
            Dict containing results of optimization operations
        """
        try:
            results = {}

            # Analyze table statistics
            analyze_query = "ANALYZE geomagnetic_predictions;"
            await self.execute(analyze_query)
            results["analyze"] = "completed"

            # Vacuum the table (not FULL to avoid locking)
            vacuum_query = "VACUUM geomagnetic_predictions;"
            await self.execute(vacuum_query)
            results["vacuum"] = "completed"

            # Check table size and bloat
            size_query = """
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
            FROM pg_tables 
            WHERE tablename = 'geomagnetic_predictions';
            """

            size_info = await self.fetch_one(size_query)
            if size_info:
                results["table_size"] = size_info["size"]
                results["table_size_bytes"] = size_info["size_bytes"]

            # Check for unused indexes
            unused_indexes_query = """
            SELECT 
                schemaname,
                tablename,
                indexname,
                idx_scan,
                idx_tup_read,
                idx_tup_fetch
            FROM pg_stat_user_indexes 
            WHERE tablename = 'geomagnetic_predictions'
            AND idx_scan = 0;
            """

            unused_indexes = await self.fetch_all(unused_indexes_query)
            results["unused_indexes"] = unused_indexes

            # Clean up old predictions (keep last 7 days)
            cleaned_count = await self.cleanup_old_geomagnetic_predictions(
                days_to_keep=7
            )
            results["cleaned_old_records"] = cleaned_count

            # Check for orphaned records (miners no longer in metagraph)
            orphan_check_query = """
            SELECT COUNT(*) as orphan_count
            FROM geomagnetic_predictions gp
            WHERE NOT EXISTS (
                SELECT 1 FROM node_table nt 
                WHERE nt.hotkey = gp.miner_hotkey
            );
            """

            orphan_result = await self.fetch_one(orphan_check_query)
            if orphan_result and orphan_result["orphan_count"] > 0:
                results["orphaned_records"] = orphan_result["orphan_count"]
                logger.warning(
                    f"Found {orphan_result['orphan_count']} orphaned geomagnetic prediction records"
                )

            logger.info(
                f"Geomagnetic predictions table optimization completed: {results}"
            )
            return results

        except Exception as e:
            logger.error(f"Error optimizing geomagnetic predictions table: {e}")
            logger.error(traceback.format_exc())
            return {"error": str(e)}

    @track_operation("write")
    async def emergency_geomagnetic_cleanup(self) -> Dict[str, Any]:
        """
        Emergency cleanup for geomagnetic_predictions table when performance is severely degraded.

        Returns:
            Dict containing cleanup results
        """
        try:
            results = {}
            logger.warning("Starting emergency geomagnetic predictions cleanup...")

            # 1. Force analyze and vacuum
            await self.execute("VACUUM ANALYZE geomagnetic_predictions;")
            results["vacuum_analyze"] = "completed"

            # 2. Clean up very old records (older than 3 days)
            old_cleanup = await self.cleanup_old_geomagnetic_predictions(days_to_keep=3)
            results["old_records_cleaned"] = old_cleanup

            # 3. Remove orphaned records (miners not in node_table)
            orphan_cleanup_query = """
            DELETE FROM geomagnetic_predictions gp
            WHERE NOT EXISTS (
                SELECT 1 FROM node_table nt 
                WHERE nt.hotkey = gp.miner_hotkey
            );
            """

            orphan_result = await self.execute(orphan_cleanup_query)
            orphan_count = getattr(orphan_result, "rowcount", 0)
            results["orphaned_records_cleaned"] = orphan_count

            # 4. Reindex the table to fix any corruption
            await self.execute("REINDEX TABLE geomagnetic_predictions;")
            results["reindex"] = "completed"

            # 5. Update table statistics
            await self.execute("ANALYZE geomagnetic_predictions;")
            results["final_analyze"] = "completed"

            logger.info(f"Emergency geomagnetic cleanup completed: {results}")
            return results

        except Exception as e:
            logger.error(f"Error during emergency geomagnetic cleanup: {e}")
            logger.error(traceback.format_exc())
            return {"error": str(e)}
