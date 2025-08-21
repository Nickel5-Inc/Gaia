from abc import ABC
import asyncio
import time
from typing import Any, Dict, List, Optional, TypeVar, Callable
from functools import wraps
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from contextlib import asynccontextmanager
from gaia.utils.custom_logger import get_logger
from gaia.utils.global_memory_manager import (
    create_thread_cleanup_helper,
    register_thread_cleanup,
)

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
import traceback
from datetime import datetime, timezone  # Added import
import os  # Ensure os is imported
import concurrent.futures

logger = get_logger(__name__)

T = TypeVar("T")


class DatabaseError(Exception):
    """Base exception for database errors"""

    pass


class DatabaseTimeout(DatabaseError):
    """Exception raised when a database operation times out"""

    pass


class DatabaseConnectionError(DatabaseError):
    """Exception raised when database connection fails"""

    pass


class TransactionError(DatabaseError):
    """Exception raised when transaction operations fail"""

    pass


class CircuitBreakerError(DatabaseError):
    """Exception raised when circuit breaker is open"""

    pass


class BaseDatabaseManager(ABC):
    """
    Abstract base class for PostgreSQL database management with SQLAlchemy async support.
    Implements singleton pattern to ensure only one instance exists per node type.
    """

    _instances = {}
    _lock = asyncio.Lock()

    # Default timeouts
    DEFAULT_QUERY_TIMEOUT = (
        60  # Reduced from 120 to 60 seconds for faster failure detection
    )
    DEFAULT_TRANSACTION_TIMEOUT = 120  # Reduced from 180 to 120 seconds
    DEFAULT_CONNECTION_TIMEOUT = 30  # Reduced from 60 to 30 seconds

    # Operation constants
    DEFAULT_BATCH_SIZE = 1000
    MAX_RETRIES = 3
    MAX_CONNECTIONS = 50  # Increased from 40 to 50 for better throughput
    MAX_OVERFLOW = 15  # Increased from 10 to 15, total max = 65 connections

    # Pool health check settings
    POOL_HEALTH_CHECK_INTERVAL = 20  # Reduced from 30 to 20 for faster detection
    POOL_RECOVERY_ATTEMPTS = 3

    # Circuit breaker settings
    CIRCUIT_BREAKER_THRESHOLD = 3  # REDUCED from 5 to 3 for faster response
    CIRCUIT_BREAKER_RECOVERY_TIME = 20  # REDUCED from 30 to 20 seconds

    # Connection pool aggressive settings for high-load environments
    POOL_PRE_PING_TIMEOUT = 3  # Reduced from 5 to 3 for faster ping timeout
    POOL_AGGRESSIVE_CLEANUP_THRESHOLD = 0.7  # Clean when 70% of connections in use
    POOL_FORCE_RESET_THRESHOLD = 0.9  # Force reset when 90% of connections in use

    # New timeout constants for finer control
    CONNECTION_TEST_TIMEOUT = 5  # REDUCED from 10 for faster failure detection
    ENGINE_COMMAND_TIMEOUT = 10  # REDUCED from 15 for faster timeout

    # Operation statuses
    STATUS_PENDING = "pending"
    STATUS_PROCESSING = "processing"
    STATUS_COMPLETED = "completed"
    STATUS_ERROR = "error"
    STATUS_TIMEOUT = "timeout"

    # Thread pool for heavy database operations
    _db_thread_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None
    DB_THREAD_POOL_MAX_WORKERS = 4  # Limited to prevent overwhelming DB

    # Note: Log level 5 is used for TRACE-level session debugging (more verbose than DEBUG level 10)
    # This reduces console noise while still allowing detailed session tracking when needed

    def __new__(cls, node_type: str, *args, **kwargs):
        """Ensure singleton instance per node type"""
        if node_type not in cls._instances:
            cls._instances[node_type] = super().__new__(cls)
        return cls._instances[node_type]

    def __init__(
        self,
        node_type: str,
        host: Optional[str] = "localhost",  # Can be hostname or socket path
        port: Optional[int] = 5432,  # Port for TCP, None for socket
        database: str = "bittensor",
        user: str = "postgres",
        password: str = "postgres",
        connection_type: Optional[str] = None,  # Explicitly passed connection type
        enable_monitoring: bool = True,  # New flag for enabling/disabling monitoring
    ):
        """Initialize database connection parameters and engine."""
        if not hasattr(self, "initialized"):
            self.node_type = node_type
            self.monitoring_enabled = enable_monitoring  # Store the flag

            # Prioritize parameters passed from subclass (MinerDatabaseManager)
            # These have already considered their specific environment variables.
            actual_user = user
            actual_password = password
            actual_database = database
            actual_host = (
                host  # This is the resolved host or socket path from MinerDBManager
            )
            actual_port = port  # This is the resolved port (or None for socket) from MinerDBManager

            # Use connection_type passed from subclass, or default to "tcp"
            # Subclasses should resolve DB_CONNECTION_TYPE or their specific env var.
            resolved_connection_type = (
                connection_type
                if connection_type
                else os.getenv("DB_CONNECTION_TYPE", "tcp").lower()
            )

            logger.info(
                f"BaseDatabaseManager init for node_type '{node_type}' with connection_type: '{resolved_connection_type}', monitoring: {self.monitoring_enabled}"
            )

            if resolved_connection_type == "socket":
                if not actual_host or not actual_host.startswith("/"):
                    logger.error(
                        f"Socket connection type specified, but host ('{actual_host}') is not a valid socket path. Attempting to use default /var/run/postgresql."
                    )
                    actual_host = "/var/run/postgresql"  # Fallback, consider making this configurable or erroring out

                self.db_url = f"postgresql+asyncpg://{actual_user}:{actual_password}@/{actual_database}?host={actual_host}"
                logger.info(
                    f"BaseDatabaseManager: Configuring database for Unix domain socket connection. Socket path: '{actual_host}', DB: '{actual_database}', User: '{actual_user}'"
                )
            else:  # Default to TCP/IP
                if (
                    actual_port is None
                ):  # Should not happen if TCP is chosen and port was default
                    logger.warning(
                        f"TCP connection type specified, but port is None. Defaulting to 5432."
                    )
                    actual_port = 5432
                if not actual_host or actual_host.startswith(
                    "/"
                ):  # Host looks like a path, but TCP is selected
                    logger.warning(
                        f"TCP connection type specified, but host ('{actual_host}') looks like a path. Defaulting to 'localhost'."
                    )
                    actual_host = "localhost"

                self.db_url = f"postgresql+asyncpg://{actual_user}:{actual_password}@{actual_host}:{actual_port}/{actual_database}"
                logger.info(
                    f"BaseDatabaseManager: Configuring database for TCP/IP connection. Host: '{actual_host}', Port: {actual_port}, DB: '{actual_database}', User: '{actual_user}'"
                )

            self._active_sessions = set()
            self._active_operations = 0

            self._last_pool_check = 0
            self._pool_health_status = True
            self._pool_recovery_lock = asyncio.Lock()

            self._circuit_breaker = {
                "failures": 0,
                "last_failure_time": 0,
                "status": "closed",
            }

            self._resource_stats = {
                "cpu_percent": 0,
                "memory_rss": 0,
                "open_files": 0,
                "connections": 0,
                "last_check": 0,
            }

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
                "top_long_sessions": [],  # List of dicts: [{'duration_ms': float, 'operation_name': str, 'acquired_at_iso': str}]
            }

            self._engine = None
            self._engine_initialized = False
            self.initialized = True
            self.db_loop = None

            # Initialize thread pool for heavy DB operations
            self._init_thread_pool()

            # Register cleanup for database-related caches that might accumulate in background threads
            self._setup_global_memory_cleanup()

    def _init_thread_pool(self):
        """Initialize thread pool for heavy database operations."""
        if BaseDatabaseManager._db_thread_pool is None:
            BaseDatabaseManager._db_thread_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.DB_THREAD_POOL_MAX_WORKERS,
                thread_name_prefix="db_worker",
            )
            logger.info(
                f"Initialized database thread pool with {self.DB_THREAD_POOL_MAX_WORKERS} workers"
            )

    def _setup_global_memory_cleanup(self):
        """Setup global memory cleanup for database-related caches in background threads."""
        try:
            # Register cleanup for SQLAlchemy connection pools and query caches
            def cleanup_db_caches():
                if hasattr(self, "_engine") and self._engine:
                    # Clear SQLAlchemy query cache
                    try:
                        if hasattr(self._engine, "_compiled_cache"):
                            self._engine._compiled_cache.clear()
                    except Exception:
                        pass

                    # Clear connection pool if needed (be careful not to break active connections)
                    try:
                        pool = getattr(self._engine, "pool", None)
                        if pool and hasattr(pool, "_clear_cache"):
                            pool._clear_cache()
                    except Exception:
                        pass

            register_thread_cleanup(
                f"db_manager_{self.node_type}_caches", cleanup_db_caches
            )
            logger.debug(
                f"Registered global memory cleanup for database manager ({self.node_type})"
            )

        except Exception as e:
            logger.debug(
                f"Failed to setup global memory cleanup for database manager: {e}"
            )

    @classmethod
    def _cleanup_thread_pool(cls):
        """Cleanup thread pool on shutdown."""
        if cls._db_thread_pool:
            cls._db_thread_pool.shutdown(wait=True)
            cls._db_thread_pool = None
            logger.info("Database thread pool shutdown completed")

    async def run_in_thread_pool(self, func, *args, **kwargs):
        """Run a function in the database thread pool to avoid blocking the async loop."""
        loop = asyncio.get_event_loop()
        if self._db_thread_pool is None:
            self._init_thread_pool()

        try:
            return await loop.run_in_executor(
                self._db_thread_pool, func, *args, **kwargs
            )
        except Exception as e:
            logger.error(f"Error in thread pool execution: {e}")
            raise

    def get_session_stats(self) -> Dict[str, Any]:
        """Returns a dictionary of current session statistics."""

        return {
            "total_sessions_acquired": self._operation_stats.get(
                "total_sessions_acquired", 0
            ),
            "total_session_time_ms": self._operation_stats.get(
                "total_session_time_ms", 0.0
            ),
            "max_session_time_ms": self._operation_stats.get(
                "max_session_time_ms", 0.0
            ),
            "min_session_time_ms": self._operation_stats.get(
                "min_session_time_ms", float("inf")
            ),
            "avg_session_time_ms": self._operation_stats.get(
                "avg_session_time_ms", 0.0
            ),
            "top_long_sessions": self._operation_stats.get("top_long_sessions", []),
        }

    async def ensure_engine_initialized(self):
        """Ensure the engine is initialized before use."""
        if not self._engine_initialized:
            await self._initialize_engine()
            self._engine_initialized = True



    async def _update_circuit_breaker(self, success: bool) -> None:
        if not self.monitoring_enabled:
            return  # Skip if monitoring is disabled
        if success:
            if self._circuit_breaker["status"] == "half-open":
                self._circuit_breaker["status"] = "closed"
                logger.info("Circuit breaker closed after successful recovery")
            self._circuit_breaker["failures"] = 0
        else:
            self._circuit_breaker["failures"] += 1
            self._circuit_breaker["last_failure_time"] = time.time()

            if (
                self._circuit_breaker["failures"] >= self.CIRCUIT_BREAKER_THRESHOLD
                and self._circuit_breaker["status"] != "open"
            ):
                self._circuit_breaker["status"] = "open"
                logger.warning("Circuit breaker opened due to repeated failures")

    async def _monitor_resources(self) -> Dict[str, Any]:
        if (
            not PSUTIL_AVAILABLE or not self.monitoring_enabled
        ):  # Also check monitoring_enabled
            return None
        try:
            process = psutil.Process()
            self._resource_stats.update(
                {
                    "cpu_percent": process.cpu_percent(),
                    "memory_rss": process.memory_info().rss,
                    "open_files": len(process.open_files()),
                    "connections": len(process.connections()),
                    "last_check": time.time(),
                }
            )
            if self._resource_stats["cpu_percent"] > 80:
                logger.warning(
                    f"High CPU usage: {self._resource_stats['cpu_percent']}%"
                )
            if self._resource_stats["memory_rss"] > (1024 * 1024 * 1024):
                logger.warning(
                    f"High memory usage: "
                    f"{self._resource_stats['memory_rss'] / (1024*1024):.2f}MB"
                )
            return self._resource_stats
        except Exception as e:
            logger.error(f"Error monitoring resources: {e}")
            return None

    async def check_health(self) -> Dict[str, Any]:
        # Get detailed pool statistics
        pool_stats = self._get_pool_statistics()

        health_info = {
            "status": "healthy",
            "timestamp": time.time(),
            "pool": {
                "active_sessions": len(self._active_sessions),
                "operations": self._active_operations,
                "last_pool_check": self._last_pool_check,
                "pool_healthy": self._pool_health_status,
                "statistics": pool_stats,
            },
            "circuit_breaker": {
                "status": self._circuit_breaker["status"],
                "failures": self._circuit_breaker["failures"],
                "last_failure": self._circuit_breaker["last_failure_time"],
            },
            "resources": await self._monitor_resources(),
            "connection_test": False,
            "errors": [],
        }

        # Check for connection leaks
        leak_info = await self._detect_connection_leaks()
        health_info["leak_detection"] = leak_info

        # Warn if pool utilization is high
        utilization = pool_stats.get("utilization", 0.0)
        if utilization > 0.7:
            health_info["errors"].append(f"High pool utilization: {utilization:.1%}")
            if utilization > 0.9:
                health_info["status"] = "critical"

        try:
            async with self.session(
                operation_name="check_health_select_1"
            ) as session:  # Added operation_name
                await session.execute(text("SELECT 1"))
            health_info["connection_test"] = True
        except Exception as e:
            health_info["status"] = "unhealthy"
            health_info["errors"].append(str(e))

        return health_info

    async def _check_pool_health(self) -> bool:
        current_time = time.time()
        if current_time - self._last_pool_check < self.POOL_HEALTH_CHECK_INTERVAL:
            return self._pool_health_status
        async with self._pool_recovery_lock:
            try:
                if not self._engine or not self._engine_initialized:
                    logger.error("Database engine not initialized")
                    self._pool_health_status = False
                    return False

                # Get pool statistics for monitoring
                pool_stats = self._get_pool_statistics()
                pool_utilization = pool_stats.get("utilization", 0.0)

                # Log pool stats for visibility
                logger.info(
                    f"Pool health check - Utilization: {pool_utilization:.1%}, "
                    f"Active: {pool_stats.get('checked_out', 0)}, "
                    f"Available: {pool_stats.get('checked_in', 0)}, "
                    f"Total: {pool_stats.get('size', 0)}"
                )

                # Aggressive cleanup if pool utilization is high
                if pool_utilization >= self.POOL_AGGRESSIVE_CLEANUP_THRESHOLD:
                    logger.warning(
                        f"High pool utilization ({pool_utilization:.1%}) - performing aggressive cleanup"
                    )
                    await self._aggressive_pool_cleanup()

                # Force pool reset if utilization is critical
                if pool_utilization >= self.POOL_FORCE_RESET_THRESHOLD:
                    logger.error(
                        f"Critical pool utilization ({pool_utilization:.1%}) - forcing pool reset"
                    )
                    await self.reset_pool()
                    return True  # Reset successful, pool is now healthy

                healthy = await self._ensure_pool()
                if not healthy:
                    logger.warning("Pool health check failed - attempting recovery")
                    recovery_successful = False
                    for attempt in range(self.POOL_RECOVERY_ATTEMPTS):
                        try:
                            logger.info(
                                f"Recovery attempt {attempt + 1}/{self.POOL_RECOVERY_ATTEMPTS}"
                            )
                            for (
                                session_obj
                            ) in (
                                self._active_sessions.copy()
                            ):  # Iterate over actual session objects if stored, else ids
                                try:
                                    if hasattr(session_obj, "close"):
                                        await session_obj.close()
                                except Exception as e:
                                    logger.error(f"Error closing session: {e}")
                            self._active_sessions.clear()
                            self._active_operations = 0
                            if self._engine:
                                await self._engine.dispose()
                            await self._initialize_engine()
                            if await self._ensure_pool():
                                logger.info("Pool recovery successful")
                                recovery_successful = True
                                break
                        except Exception as e:
                            logger.error(f"Recovery attempt {attempt + 1} failed: {e}")
                            await asyncio.sleep(2**attempt)
                            continue
                    if not recovery_successful:
                        logger.error("All recovery attempts failed")
                        self._pool_health_status = False
                        return False
                    healthy = True
                self._pool_health_status = healthy
                self._last_pool_check = current_time
                return healthy
            except Exception as e:
                logger.error(f"Pool health check failed: {e}")
                logger.error(f"Stack trace: {traceback.format_exc()}")
                self._pool_health_status = False
                return False

    def _get_pool_statistics(self) -> Dict[str, Any]:
        """Get detailed connection pool statistics."""
        stats = {
            "size": 0,
            "checked_in": 0,
            "checked_out": 0,
            "overflow": 0,
            "invalid": 0,
            "utilization": 0.0,
            "total_capacity": 0,
        }

        if not self._engine or not hasattr(self._engine, "pool"):
            return stats

        try:
            pool = self._engine.pool
            stats["size"] = pool.size()
            stats["checked_in"] = pool.checkedin()
            stats["checked_out"] = pool.checked_out()
            stats["overflow"] = pool.overflow()
            stats["invalid"] = pool.invalid()

            total_capacity = self.MAX_CONNECTIONS + self.MAX_OVERFLOW
            stats["total_capacity"] = total_capacity

            if total_capacity > 0:
                stats["utilization"] = stats["checked_out"] / total_capacity

        except Exception as e:
            logger.debug(f"Error getting pool statistics: {e}")

        return stats

    async def _aggressive_pool_cleanup(self):
        """Perform aggressive connection pool cleanup to free up connections."""
        if not self._engine:
            return

        try:
            logger.info("Starting aggressive pool cleanup")

            # Force close any idle connections that have been sitting around
            pool = self._engine.pool
            if hasattr(pool, "invalidate"):
                # Invalidate connections that might be stale
                pool.invalidate()
                logger.info("Invalidated potentially stale connections")

            # Clear any tracked sessions that might be holding connections
            if len(self._active_sessions) > 0:
                logger.warning(
                    f"Clearing {len(self._active_sessions)} tracked sessions that may be holding connections"
                )
                self._active_sessions.clear()
                self._active_operations = 0

            # Force garbage collection to clean up any unreferenced connections
            import gc

            collected = gc.collect()
            logger.info(f"Aggressive cleanup: garbage collected {collected} objects")

        except Exception as e:
            logger.error(f"Error during aggressive pool cleanup: {e}")

    async def _detect_connection_leaks(self) -> Dict[str, Any]:
        """Detect potential connection leaks and provide diagnostics."""
        leak_info = {
            "potential_leaks": False,
            "session_count": len(self._active_sessions),
            "operations_count": self._active_operations,
            "pool_stats": self._get_pool_statistics(),
            "recommendations": [],
        }

        pool_stats = leak_info["pool_stats"]
        utilization = pool_stats.get("utilization", 0.0)

        # Check for signs of connection leaks
        if utilization > 0.8:
            leak_info["potential_leaks"] = True
            leak_info["recommendations"].append(
                f"High pool utilization: {utilization:.1%}"
            )

        if pool_stats.get("checked_out", 0) > pool_stats.get("size", 0) * 0.7:
            leak_info["potential_leaks"] = True
            leak_info["recommendations"].append(
                "Many connections checked out for extended period"
            )

        if len(self._active_sessions) > 10:
            leak_info["potential_leaks"] = True
            leak_info["recommendations"].append(
                f"High number of tracked sessions: {len(self._active_sessions)}"
            )

        return leak_info

    async def _initialize_engine(self) -> bool:
        if not self.db_url:
            raise DatabaseError("Database URL not initialized")
        try:
            current_loop = asyncio.get_running_loop()
            masked_url = str(self.db_url).replace(self.db_url.split("@")[0], "***")
            logger.info(
                f"Initializing database engine with URL: {masked_url} on loop {current_loop}"
            )
            self._engine = create_async_engine(
                self.db_url,
                pool_pre_ping=True,
                pool_size=self.MAX_CONNECTIONS,
                max_overflow=self.MAX_OVERFLOW,
                pool_timeout=self.DEFAULT_CONNECTION_TIMEOUT,
                pool_recycle=1800,  # REDUCED from 3600 to 1800 for faster connection refresh
                pool_use_lifo=True,
                echo=False,
                # Optimized connection arguments for better performance
                connect_args={
                    "command_timeout": self.ENGINE_COMMAND_TIMEOUT,
                    "timeout": self.DEFAULT_CONNECTION_TIMEOUT,
                    "server_settings": {
                        "jit": "off",
                        "application_name": f"gaia_{self.node_type}_{os.getpid()}",
                        "tcp_keepalives_idle": "30",  # Send keepalive every 30 seconds
                        "tcp_keepalives_interval": "10",  # Retry every 10 seconds
                        "tcp_keepalives_count": "3",  # Drop after 3 failed attempts
                        "statement_timeout": "60000",  # 60 second statement timeout
                        "lock_timeout": "30000",  # 30 second lock timeout
                    },
                },
                # Additional engine options for stability
                pool_reset_on_return="commit",  # Reset connections on return
            )
            async with self._engine.connect() as conn:
                await asyncio.wait_for(
                    conn.execute(text("SELECT 1")), timeout=self.CONNECTION_TEST_TIMEOUT
                )
            self._session_factory = async_sessionmaker(
                self._engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autobegin=False,
            )
            self.db_loop = current_loop
            logger.info(
                f"Database engine initialized successfully on loop {self.db_loop}"
            )
            self._engine_initialized = True
            return True
        except Exception as e:
            logger.error(f"Failed to initialize database engine: {e}")
            logger.error(traceback.format_exc())
            self._engine = None
            self._session_factory = None
            self._engine_initialized = False
            return False

    async def _ensure_pool(self) -> bool:
        if not self._engine:
            logger.error("Cannot ensure pool - engine not initialized")
            return False
        conn = None
        try:
            conn = await self._engine.connect()
            await asyncio.wait_for(
                conn.execute(text("SELECT 1")), timeout=self.DEFAULT_CONNECTION_TIMEOUT
            )
            return True
        except Exception as e:
            logger.error(f"Error ensuring pool: {e}")
            return False
        finally:
            if conn:
                try:
                    await conn.close()
                except Exception as e:
                    logger.error(f"Error closing connection in _ensure_pool: {e}")

    def _increment_active_sessions(self) -> int:
        self._active_operations += 1
        return self._active_operations

    def _decrement_active_sessions(self) -> int:
        self._active_operations = max(
            0, self._active_operations - 1
        )  # Ensure it doesn't go below 0
        return self._active_operations

    async def _mark_operation_failed(self):
        await self._update_circuit_breaker(False)

    @asynccontextmanager
    async def get_connection(self):
        conn = None
        try:
            conn = await self._engine.connect()
            yield conn
        finally:
            if conn:
                try:
                    await conn.close()
                except Exception as e:
                    logger.error(f"Error closing connection: {e}")

    async def cleanup_stale_connections(self):
        if not self._engine:
            return
        try:
            pool = self._engine.pool
            if pool:
                size = pool.size()
                checkedin = pool.checkedin()
                overflow = pool.overflow()
                if overflow > 0 or checkedin > size * 0.8:
                    logger.info(
                        f"Cleaning up connection pool. Size: {size}, "
                        f"Checked-in: {checkedin}, Overflow: {overflow}"
                    )
                    async with self.get_connection() as conn:  # This will use a session with op_name
                        await conn.execute(text("SELECT 1"))
                    await self._engine.dispose()
        except Exception as e:
            logger.error(f"Error cleaning up stale connections: {e}")

    def _update_top_long_sessions(
        self, duration_ms: float, name: str, acquired_at_iso_str: str
    ):
        """Helper to update the list of top longest sessions."""
        if not self.monitoring_enabled:
            return  # Skip if monitoring is disabled
        try:
            # Ensure the list exists in stats
            if "top_long_sessions" not in self._operation_stats:
                self._operation_stats["top_long_sessions"] = []

            current_top_sessions = self._operation_stats["top_long_sessions"]

            # Add the new session info as a dictionary
            current_top_sessions.append(
                {
                    "duration_ms": duration_ms,
                    "operation_name": name,
                    "acquired_at_iso": acquired_at_iso_str,  # Use the passed ISO string directly
                }
            )

            # Sort by duration_ms in descending order
            current_top_sessions.sort(key=lambda x: x["duration_ms"], reverse=True)

            # Keep only the top 3
            self._operation_stats["top_long_sessions"] = current_top_sessions[:3]
        except Exception as e:
            logger.error(
                f"Error updating top long sessions: {e} - Stats: {self._operation_stats}"
            )

    def with_timeout(timeout: float):
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @wraps(func)
            async def wrapper(self, *args, **kwargs) -> T:
                call_timeout = kwargs.get("timeout")
                effective_timeout = (
                    call_timeout if call_timeout is not None else timeout
                )
                try:
                    return await asyncio.wait_for(
                        func(self, *args, **kwargs), timeout=effective_timeout
                    )
                except asyncio.TimeoutError:
                    op_name_for_log = getattr(func, "__name__", "Unnamed_Operation")
                    logger.error(
                        f"Operation {op_name_for_log} timed out after {effective_timeout}s"
                    )
                    raise DatabaseTimeout(
                        f"Operation {op_name_for_log} timed out after {effective_timeout}s"
                    )

            return wrapper

        return decorator

    @asynccontextmanager
    async def lightweight_session(self):
        await self.ensure_engine_initialized()
        if not self._engine or not self._session_factory:
            raise DatabaseConnectionError("Engine/Session factory not initialized")
        session_instance: Optional[AsyncSession] = None
        try:
            session_instance = self._session_factory()
            yield session_instance
        except Exception as e:
            logger.error(f"Lightweight session error: {str(e)}")
            if isinstance(e, (DatabaseError, SQLAlchemyError)):
                raise
            raise DatabaseConnectionError(f"Session error: {str(e)}") from e
        finally:
            if session_instance:
                try:
                    await session_instance.close()
                except Exception as e:
                    logger.error(f"Error closing lightweight session: {e}")

    @asynccontextmanager
    async def session(
        self,
        operation_name: str = "Unnamed Session",
        operation_type: str = "read",
        provided_session: Optional[AsyncSession] = None,
    ):
        await self.ensure_engine_initialized()  # Ensure engine is ready

        overall_start_time = time.monotonic()
        session_id_str = "provided" if provided_session else "new"
        specific_op_name = operation_name

        actual_session_instance: Optional[AsyncSession] = None
        session_acquired_time = 0.0
        yield_start_time = 0.0
        yield_end_time = 0.0
        session_init_duration = 0.0

        transaction_started_here = False
        acquired_at_iso_str = datetime.now(timezone.utc).isoformat()
        e_outer = None
        session_id_for_log = "unknown"  # Initialize to prevent UnboundLocalError

        try:
            if provided_session:
                actual_session_instance = provided_session
                session_id_for_log = f"provided_{id(actual_session_instance)}"
                if self.monitoring_enabled:
                    logger.debug(
                        f"Session {session_id_for_log} ({specific_op_name}): Using provided session."
                    )
            else:
                acquire_start_time = time.monotonic()
                if not self._session_factory:
                    logger.error(
                        f"Session (op:{specific_op_name}): Session factory not initialized!"
                    )
                    raise DatabaseConnectionError(
                        "Session factory not initialized for new session."
                    )
                actual_session_instance = self._session_factory()
                session_init_duration = (time.monotonic() - acquire_start_time) * 1000
                session_id_for_log = f"new_{id(actual_session_instance)}"
                if self.monitoring_enabled:
                    logger.log(
                        5,
                        f"Session {session_id_for_log} ({specific_op_name}): New session acquired from factory in {session_init_duration:.2f}ms.",
                    )

            session_acquired_time = time.monotonic()
            session_id_str = session_id_for_log

            if not actual_session_instance:
                logger.error(
                    f"Session ({specific_op_name}): Failed to obtain a session instance."
                )
                raise DatabaseConnectionError("Failed to obtain session instance.")

            if not actual_session_instance.in_transaction():
                if self.monitoring_enabled:
                    logger.log(
                        5,
                        f"Session {session_id_for_log} ({specific_op_name}): Beginning new transaction.",
                    )
                await actual_session_instance.begin()
                transaction_started_here = True
            else:
                if self.monitoring_enabled:
                    logger.log(
                        5,
                        f"Session {session_id_for_log} ({specific_op_name}): Already in transaction.",
                    )

            if self.monitoring_enabled:
                self._active_sessions.add(session_id_str)
            active_sessions_count = self._increment_active_sessions()
            if self.monitoring_enabled:
                logger.log(
                    5,
                    f"Session {session_id_for_log} ({specific_op_name}) ready. Active sessions: {active_sessions_count} (Set size: {len(self._active_sessions) if self.monitoring_enabled else 'N/A'})",
                )

            yield_start_time = time.monotonic()
            yield actual_session_instance
            yield_end_time = time.monotonic()

            if transaction_started_here:
                commit_start_time = time.monotonic()
                await actual_session_instance.commit()
                commit_duration = (time.monotonic() - commit_start_time) * 1000
                if self.monitoring_enabled:
                    logger.log(
                        5,
                        f"Session {session_id_for_log} ({specific_op_name}): Transaction committed in {commit_duration:.2f}ms (normal exit).",
                    )

        except asyncio.CancelledError:
            e_outer = asyncio.CancelledError("Session cancelled")
            logger.warning(
                f"Session {session_id_for_log} ({specific_op_name}): Operation cancelled."
            )
            if (
                transaction_started_here
                and actual_session_instance
                and actual_session_instance.in_transaction()
            ):
                if self.monitoring_enabled:
                    logger.log(
                        5,
                        f"Session {session_id_for_log} ({specific_op_name}): Attempting rollback due to cancellation.",
                    )
                try:
                    rollback_start_time = time.monotonic()
                    await actual_session_instance.rollback()
                    rollback_duration = (time.monotonic() - rollback_start_time) * 1000
                    if self.monitoring_enabled:
                        logger.info(
                            f"Session {session_id_for_log} ({specific_op_name}): Transaction rolled back in {rollback_duration:.2f}ms due to cancellation."
                        )
                except Exception as r_err:
                    logger.error(
                        f"Session {session_id_for_log} ({specific_op_name}): Error during rollback after cancellation: {r_err}",
                        exc_info=True,
                    )
            if self.monitoring_enabled:
                self._operation_stats.setdefault("cancelled_operations", 0)
                self._operation_stats["cancelled_operations"] += 1
            await self._mark_operation_failed()
            raise
        except Exception as e:
            e_outer = e
            logger.error(
                f"Session {session_id_for_log} ({specific_op_name}): Error during session: {str(e_outer)}",
                exc_info=True,
            )
            if (
                transaction_started_here
                and actual_session_instance
                and actual_session_instance.in_transaction()
            ):
                if self.monitoring_enabled:
                    logger.log(
                        5,
                        f"Session {session_id_for_log} ({specific_op_name}): Attempting rollback due to exception: {str(e_outer)}.",
                    )
                try:
                    rollback_start_time = time.monotonic()
                    await actual_session_instance.rollback()
                    rollback_duration = (time.monotonic() - rollback_start_time) * 1000
                    if self.monitoring_enabled:
                        logger.info(
                            f"Session {session_id_for_log} ({specific_op_name}): Transaction rolled back in {rollback_duration:.2f}ms due to exception."
                        )
                except Exception as r_err:
                    logger.error(
                        f"Session {session_id_for_log} ({specific_op_name}): Error during rollback after exception: {r_err}",
                        exc_info=True,
                    )

            await self._mark_operation_failed()
            raise
        finally:
            session_release_start_time = time.monotonic()
            if actual_session_instance and not provided_session:
                try:
                    if (
                        actual_session_instance.is_active
                        and actual_session_instance.in_transaction()
                        and transaction_started_here
                    ):
                        logger.error(
                            f"Session {session_id_for_log} ({specific_op_name}): Session being closed but still in transaction started here! Forcing rollback. Exception was: {e_outer}"
                        )
                        try:
                            await actual_session_instance.rollback()
                            if self.monitoring_enabled:
                                logger.info(
                                    f"Session {session_id_for_log} ({specific_op_name}): Defensive rollback executed in finally."
                                )
                        except Exception as rb_finally_err:
                            logger.error(
                                f"Session {session_id_for_log} ({specific_op_name}): Error during defensive rollback in finally: {rb_finally_err}"
                            )

                    await actual_session_instance.close()
                    if self.monitoring_enabled:
                        logger.log(
                            5,
                            f"Session {session_id_for_log} ({specific_op_name}): New session closed.",
                        )
                except Exception as close_err:
                    logger.error(
                        f"Session {session_id_for_log} ({specific_op_name}): Error closing new session: {close_err}",
                        exc_info=True,
                    )

            if self.monitoring_enabled:
                self._active_sessions.discard(session_id_str)
            active_sessions_count_after = self._decrement_active_sessions()

            if self.monitoring_enabled:
                total_duration_ms = (time.monotonic() - overall_start_time) * 1000
                time_in_yield_ms = (
                    (yield_end_time - yield_start_time) * 1000
                    if yield_start_time > 0 and yield_end_time > 0
                    else 0.0
                )

                self._operation_stats["total_sessions_acquired"] += 1
                self._operation_stats["total_session_time_ms"] += total_duration_ms
                self._operation_stats["max_session_time_ms"] = max(
                    self._operation_stats["max_session_time_ms"], total_duration_ms
                )
                if total_duration_ms < self._operation_stats["min_session_time_ms"]:
                    self._operation_stats["min_session_time_ms"] = (
                        total_duration_ms
                        if total_duration_ms > 0
                        else self._operation_stats.get(
                            "min_session_time_ms", float("inf")
                        )
                    )

                if self._operation_stats["total_sessions_acquired"] > 0:
                    self._operation_stats["avg_session_time_ms"] = (
                        self._operation_stats["total_session_time_ms"]
                        / self._operation_stats["total_sessions_acquired"]
                    )

                self._update_top_long_sessions(
                    total_duration_ms, specific_op_name, acquired_at_iso_str
                )

                current_time = time.monotonic()
                if e_outer and not isinstance(e_outer, asyncio.CancelledError):
                    if self._circuit_breaker["status"] == "half-open":
                        logger.info(
                            f"Circuit breaker: Successful operation in half-open state. Closing breaker. ({specific_op_name})"
                        )
                        self._circuit_breaker["status"] = "closed"
                        self._circuit_breaker["failures"] = 0
                elif not e_outer and self._circuit_breaker["status"] == "half-open":
                    logger.info(
                        f"Circuit breaker: Successful operation in half-open state. Closing breaker. ({specific_op_name})"
                    )
                    self._circuit_breaker["status"] = "closed"
                    self._circuit_breaker["failures"] = 0

                session_release_duration_ms = (
                    time.monotonic() - session_release_start_time
                ) * 1000
                logger.log(
                    5,
                    f"Session {session_id_for_log} ({specific_op_name}): Released. Total time: {total_duration_ms:.2f}ms, Factory init: {session_init_duration:.2f}ms, In yield: {time_in_yield_ms:.2f}ms, Release code: {session_release_duration_ms:.2f}ms. Active now: {active_sessions_count_after} (Set size: {len(self._active_sessions) if self.monitoring_enabled else 'N/A'})",
                )

                overhead_ms = total_duration_ms - time_in_yield_ms
                if overhead_ms > 50 and total_duration_ms > 100:
                    logger.info(
                        f"Session {session_id_for_log} ({specific_op_name}): Significant overhead detected. Total: {total_duration_ms:.2f}ms, Yield: {time_in_yield_ms:.2f}ms, Overhead: {overhead_ms:.2f}ms"
                    )
            else:  # Monitoring disabled
                logger.log(
                    5,
                    f"Session {session_id_for_log} ({specific_op_name}): Released. Monitoring disabled. Active now: {active_sessions_count_after}",
                )

    @with_timeout(CONNECTION_TEST_TIMEOUT)
    async def _test_connection(self, session: AsyncSession) -> bool:
        await session.execute(text("SELECT 1"))
        return True

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_one(
        self, query: str, params: Optional[Dict] = None, timeout: Optional[float] = None
    ) -> Optional[Dict]:
        start_time = time.time()
        # cooperative yield to reduce event-loop contention before acquiring a session
        await asyncio.sleep(0)
        # Handle both string queries and SQLAlchemy objects
        query_str = str(query) if hasattr(query, "__str__") else query
        query_snippet = (
            query_str[:100] if isinstance(query_str, str) else "SQLAlchemy_object"
        )
        op_name = f"fetch_one:{query_snippet}"
        async with self.session(
            operation_name=op_name
        ) as session:  # session() ensures a transaction
            try:
                # No longer need session.begin() here, self.session() handles it.
                # Handle both string queries (wrap with text()) and SQLAlchemy objects (execute directly)
                # cooperative yield before executing the statement
                await asyncio.sleep(0)
                if isinstance(query, str):
                    result = await session.execute(text(query), params or {})
                else:
                    result = await session.execute(query, params or {})
                row = result.first()
                duration = time.time() - start_time
                if duration > self.DEFAULT_QUERY_TIMEOUT / 2:
                    logger.warning(
                        f"Slow query detected ({op_name}): {duration:.2f}s\nQuery: {query}"
                    )
                return dict(row._mapping) if row else None
            except SQLAlchemyError as e:
                logger.error(
                    f"Database error in fetch_one ({op_name}): {str(e)}\nQuery: {query}"
                )
                # The main session context manager will handle rollback.
                raise DatabaseError(f"Error executing query ({op_name}): {str(e)}")

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_all(
        self, query: str, params: Optional[Dict] = None, timeout: Optional[float] = None
    ) -> List[Dict]:
        start_time = time.time()
        await asyncio.sleep(0)
        # Handle both string queries and SQLAlchemy objects
        query_str = str(query) if hasattr(query, "__str__") else query
        query_snippet = (
            query_str[:100] if isinstance(query_str, str) else "SQLAlchemy_object"
        )
        op_name = f"fetch_all:{query_snippet}"
        async with self.session(
            operation_name=op_name
        ) as session:  # session() ensures a transaction
            try:
                # No longer need session.begin() here, self.session() handles it.
                # Handle both string queries (wrap with text()) and SQLAlchemy objects (execute directly)
                await asyncio.sleep(0)
                if isinstance(query, str):
                    result = await session.execute(text(query), params or {})
                else:
                    result = await session.execute(query, params or {})
                rows = result.all()

                # Enhanced handling and cleanup for large result sets
                if len(rows) > 1000:
                    query_log = (
                        query_str[:200]
                        if isinstance(query_str, str)
                        else str(query)[:200]
                    )
                    logger.warning(
                        f"Large result set ({op_name}): {len(rows)} rows\n"
                        f"Query: {query_log}..."
                    )

                    # Force garbage collection for very large result sets
                    if len(rows) > 5000:
                        import gc

                        collected = gc.collect()
                        logger.warning(
                            f"Very large result set detected ({len(rows)} rows), forced GC collected {collected} objects"
                        )

                # Convert to dict format
                converted_rows = [dict(row._mapping) for row in rows]

                # Clear SQLAlchemy result objects to free memory immediately
                del rows
                del result

                duration = time.time() - start_time
                if duration > self.DEFAULT_QUERY_TIMEOUT / 2:
                    logger.warning(
                        f"Slow query detected ({op_name}): {duration:.2f}s\nQuery: {query}"
                    )

                return converted_rows
            except SQLAlchemyError as e:
                logger.error(
                    f"Database error in fetch_all ({op_name}): {str(e)}\nQuery: {query}"
                )
                # The main session context manager will handle rollback.
                raise DatabaseError(f"Error executing query ({op_name}): {str(e)}")

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_all_threaded(
        self, query: str, params: Optional[Dict] = None, timeout: Optional[float] = None
    ) -> List[Dict]:
        """
        Fetch all rows using thread pool for heavy operations.
        Use this for large result sets to avoid blocking the async loop.
        """
        start_time = time.time()
        # Handle both string queries and SQLAlchemy objects
        query_str = str(query) if hasattr(query, "__str__") else query
        query_snippet = (
            query_str[:100] if isinstance(query_str, str) else "SQLAlchemy_object"
        )
        op_name = f"fetch_all_threaded:{query_snippet}"

        # Prepare the synchronous function to run in thread
        def sync_fetch_and_process():
            import asyncio

            # Create a new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                return loop.run_until_complete(
                    self._sync_fetch_helper(query, params, op_name)
                )
            finally:
                loop.close()

        try:
            # Run the heavy database operation in thread pool
            result = await self.run_in_thread_pool(sync_fetch_and_process)

            duration = time.time() - start_time
            if duration > self.DEFAULT_QUERY_TIMEOUT / 2:
                logger.warning(
                    f"Slow threaded query detected ({op_name}): {duration:.2f}s\nQuery: {query}"
                )

            return result

        except Exception as e:
            logger.error(
                f"Error in threaded fetch_all ({op_name}): {str(e)}\nQuery: {query}"
            )
            raise DatabaseError(f"Error executing threaded query ({op_name}): {str(e)}")

    async def _sync_fetch_helper(
        self, query: str, params: Optional[Dict], op_name: str
    ) -> List[Dict]:
        """Helper method for threaded fetch operations."""
        async with self.session(operation_name=op_name) as session:
            try:
                # Handle both string queries (wrap with text()) and SQLAlchemy objects (execute directly)
                if isinstance(query, str):
                    result = await session.execute(text(query), params or {})
                else:
                    result = await session.execute(query, params or {})
                rows = result.all()

                # Process rows in the thread to avoid main loop blocking
                converted_rows = [dict(row._mapping) for row in rows]

                # Memory cleanup in thread
                del rows
                del result

                # Log large result sets
                if len(converted_rows) > 1000:
                    logger.info(
                        f"Large threaded result set ({op_name}): {len(converted_rows)} rows"
                    )

                    # Force garbage collection for very large result sets
                    if len(converted_rows) > 5000:
                        import gc

                        collected = gc.collect()
                        logger.info(
                            f"Threaded query GC: collected {collected} objects for {len(converted_rows)} rows"
                        )

                return converted_rows

            except SQLAlchemyError as e:
                logger.error(
                    f"Database error in threaded fetch_all ({op_name}): {str(e)}"
                )
                raise DatabaseError(
                    f"Error executing threaded query ({op_name}): {str(e)}"
                )

    @with_timeout(DEFAULT_TRANSACTION_TIMEOUT)  # Default timeout for execute
    async def execute(
        self,
        query: str,
        params: Optional[Dict] = None,
        timeout: Optional[float] = None,  # Allows per-call override
        session: Optional[AsyncSession] = None,  # Existing session to use
    ) -> Any:
        # Handle both string queries and SQLAlchemy objects
        query_str = str(query) if hasattr(query, "__str__") else query
        query_snippet = (
            query_str[:100] if isinstance(query_str, str) else "SQLAlchemy_object"
        )
        op_name = f"execute:{query_snippet}"
        if session is not None:
            try:
                # When a session is provided, we assume the caller manages the transaction.
                # Handle both string queries (wrap with text()) and SQLAlchemy objects (execute directly)
                if isinstance(query, str):
                    result = await session.execute(text(query), params or {})
                else:
                    result = await session.execute(query, params or {})
                return result
            except SQLAlchemyError as e:
                logger.error(
                    f"Database error in execute (with existing session, op: {op_name}): {str(e)}\nQuery: {query}"
                )
                raise DatabaseError(
                    f"Error executing query (with existing session, op: {op_name}): {str(e)}"
                )
        else:
            async with self.session(
                operation_name=op_name
            ) as new_session:  # session() ensures a transaction
                try:
                    # No longer need new_session.begin() here, self.session() handles it.
                    # Handle both string queries (wrap with text()) and SQLAlchemy objects (execute directly)
                    if isinstance(query, str):
                        result = await new_session.execute(text(query), params or {})
                    else:
                        result = await new_session.execute(query, params or {})
                    return result
                except SQLAlchemyError as e:
                    logger.error(
                        f"Database error in execute (new session, op: {op_name}): {str(e)}\nQuery: {query}"
                    )
                    # The main session context manager will handle rollback.
                    raise DatabaseError(
                        f"Error executing query (new session, op: {op_name}): {str(e)}"
                    )

    @with_timeout(DEFAULT_TRANSACTION_TIMEOUT)
    async def execute_many(
        self,
        query: str,
        params_list: List[Dict],
        timeout: Optional[float] = None,
        batch_size: Optional[int] = None,
    ) -> None:
        if not params_list:
            return
        batch_size = batch_size or self.DEFAULT_BATCH_SIZE
        total_items = len(params_list)

        avg_param_size = (
            sum(len(str(p)) for p in params_list[:100]) / min(100, total_items)
            if total_items > 0
            else 0
        )
        if avg_param_size > 1000:
            batch_size = min(batch_size, 100)

        start_time = time.time()
        # Handle both string queries and SQLAlchemy objects
        query_str = str(query) if hasattr(query, "__str__") else query
        query_snippet = (
            query_str[:100] if isinstance(query_str, str) else "SQLAlchemy_object"
        )
        op_name = f"execute_many:{query_snippet}"

        # execute_many always manages its own transaction here
        async with self.session(
            operation_name=op_name
        ) as session:  # Pass operation_name
            i = 0  # Initialize i to prevent "referenced before assignment" errors
            try:
                # Check if transaction is already active, if not begin one
                if session.in_transaction():
                    # Transaction already active, execute directly
                    for i in range(0, total_items, batch_size):
                        batch = params_list[i : i + batch_size]
                        batch_start_time = time.time()

                        # Handle both string queries (wrap with text()) and SQLAlchemy objects (execute directly)
                        if isinstance(query, str):
                            await session.execute(text(query), batch)
                        else:
                            await session.execute(query, batch)

                        batch_duration = time.time() - batch_start_time
                        if batch_duration > 5:
                            logger.warning(
                                f"Slow batch detected ({op_name}): {batch_duration:.2f}s "
                                f"(Items {i}-{i+len(batch)})"
                            )
                        if i > 0 and i % (batch_size * 10) == 0:
                            progress = (i / total_items) * 100
                            elapsed = time.time() - start_time
                            rate = i / elapsed if elapsed > 0 else 0
                            logger.info(
                                f"Batch progress ({op_name}): {progress:.1f}% "
                                f"({i}/{total_items}) "
                                f"Rate: {rate:.1f} items/s"
                            )
                            await self._monitor_resources()
                else:
                    # No transaction active, begin one for all batches
                    async with session.begin():
                        for i in range(0, total_items, batch_size):
                            batch = params_list[i : i + batch_size]
                            batch_start_time = time.time()

                            # Handle both string queries (wrap with text()) and SQLAlchemy objects (execute directly)
                            if isinstance(query, str):
                                await session.execute(text(query), batch)
                            else:
                                await session.execute(query, batch)
                            # Commit is handled by the outer session.begin() context manager

                            batch_duration = time.time() - batch_start_time
                            if batch_duration > 5:
                                logger.warning(
                                    f"Slow batch detected ({op_name}): {batch_duration:.2f}s "
                                    f"(Items {i}-{i+len(batch)})"
                                )
                            if i > 0 and i % (batch_size * 10) == 0:
                                progress = (i / total_items) * 100
                                elapsed = time.time() - start_time
                                rate = i / elapsed if elapsed > 0 else 0
                                logger.info(
                                    f"Batch progress ({op_name}): {progress:.1f}% "
                                    f"({i}/{total_items}) "
                                    f"Rate: {rate:.1f} items/s"
                                )
                                await self._monitor_resources()

                total_duration = time.time() - start_time
                logger.info(
                    f"Batch operation completed ({op_name}): {total_items} items "
                    f"in {total_duration:.2f}s "
                    f"({(total_items/total_duration if total_duration > 0 else 0):.1f} items/s)"
                )
            except SQLAlchemyError as e:
                logger.error(
                    f"Batch operation failed ({op_name}) at item approx {i}: {str(e)}\n"  # 'i' might be from previous scope if error in begin()
                    f"Query: {query}"
                )
                # Rollback is handled by session.begin() context manager on error
                raise DatabaseError(
                    f"Error executing batch query ({op_name}): {str(e)}"
                )

    async def execute_with_retry(
        self,
        query: str,
        params: Optional[Dict] = None,
        max_retries: Optional[int] = None,
        initial_delay: float = 0.1,
    ) -> Any:
        max_retries = max_retries or self.MAX_RETRIES
        last_error = None
        # Handle both string queries and SQLAlchemy objects
        query_str = str(query) if hasattr(query, "__str__") else query
        query_snippet = (
            query_str[:80] if isinstance(query_str, str) else "SQLAlchemy_object"
        )
        op_name = (
            f"execute_with_retry:{query_snippet}"  # Shorter op_name for retry wrapper
        )

        for attempt in range(max_retries):
            try:
                # The execute call will use its own session with a more specific op_name
                return await self.execute(query, params)
            except (
                DatabaseError
            ) as e:  # Catch DatabaseError, which includes DatabaseTimeout
                last_error = e
                if attempt < max_retries - 1:
                    delay = initial_delay * (2**attempt)
                    logger.warning(
                        f"Retry attempt {attempt + 1}/{max_retries} for {op_name} "
                        f"after {delay:.1f}s delay. Error: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error(
                    f"All retry attempts failed for {op_name}: {query}\n"
                    f"Final error: {str(last_error)}"
                )
                raise last_error  # Re-raise the last error after all retries exhausted

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def table_exists(self, table_name: str) -> bool:
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = :table_name
            )
        """
        # fetch_one will create its own session with appropriate operation_name
        result = await self.fetch_one(query, {"table_name": table_name})
        return result["exists"] if result else False

    async def cleanup_stale_operations(
        self, table_name: str, status_column: str = "status", timeout_minutes: int = 60
    ) -> None:
        query = f"""
            UPDATE {table_name}
            SET {status_column} = :error_status
            WHERE {status_column} = :processing_status
            AND created_at < NOW() - :timeout * INTERVAL '1 minute'
        """
        params = {
            "error_status": self.STATUS_ERROR,
            "processing_status": self.STATUS_PROCESSING,
            "timeout": timeout_minutes,
        }
        # execute will create its own session with appropriate operation_name
        await self.execute(query, params)

    async def reset_pool(self) -> None:
        try:
            logger.info("Starting database pool reset")
            active_session_ids_copy = list(self._active_sessions)  # Copy IDs
            for session_id in active_session_ids_copy:
                # Cannot directly close sessions by ID if not storing actual objects
                logger.debug(f"Pool reset: Active session ID {session_id} was present.")
            self._active_sessions.clear()
            self._active_operations = 0

            if self._engine:
                try:
                    logger.info("Disposing of existing engine")
                    await self._engine.dispose()
                except Exception as e:
                    logger.error(f"Error disposing engine: {e}")

            self._engine = None
            self._engine_initialized = False
            self._session_factory = None

            logger.info("Reinitializing database engine")
            await self._initialize_engine()  # This sets _engine_initialized to True on success

            logger.info("Verifying new connection pool")
            if (
                not await self._ensure_pool()
            ):  # This also re-sets _engine_initialized if it fails
                self._engine_initialized = (
                    False  # Ensure it's false if ensure_pool fails
                )
                raise DatabaseError("Failed to verify new connection pool after reset")

            self._last_pool_check = time.time()
            self._pool_health_status = True
            self._circuit_breaker["failures"] = 0
            self._circuit_breaker["status"] = "closed"

            logger.info("Database pool reset completed successfully")

        except Exception as e:
            logger.error(f"Error resetting connection pool: {e}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            self._pool_health_status = False
            self._engine_initialized = (
                False  # Ensure this is false on any error during reset
            )
            # Do not re-raise DatabaseError here if already DatabaseError, to avoid nesting.
            if not isinstance(e, DatabaseError):
                raise DatabaseError(f"Failed to reset connection pool: {str(e)}")
            else:
                raise  # Re-raise original DatabaseError

    async def close(self) -> None:
        """Close all database connections and cleanup resources."""
        try:
            # Similar to reset_pool, direct closing of sessions by ID is not feasible
            # if only IDs are stored. Rely on engine disposal.
            self._active_sessions.clear()
            self._active_operations = 0

            if self._engine:
                await self._engine.dispose()

            # Cleanup thread pool
            self._cleanup_thread_pool()

        except Exception as e:
            logger.error(f"Error during database cleanup: {e}")
            # Do not re-raise DatabaseError here if already DatabaseError
            if not isinstance(e, DatabaseError):
                raise DatabaseError(f"Failed to cleanup database resources: {str(e)}")
            else:
                raise

    @staticmethod
    def with_transaction():
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @wraps(func)
            async def wrapper(self: "BaseDatabaseManager", *args, **kwargs) -> T:
                op_name = func.__name__  # Get the name of the decorated function
                # self.session() handles beginning the transaction internally via _session_factory and autobegin=False
                # then async with session.begin() is used inside the session block.
                # The session obtained from self.session() will start a transaction when session.begin() is called.
                async with self.session(
                    operation_name=op_name
                ) as session:  # Pass operation_name
                    async with session.begin():  # Start the actual transaction
                        return await func(self, session, *args, **kwargs)

            return wrapper

        return decorator

    async def initialize_database(self) -> None:
        try:
            logger.info("Base database initialization completed")
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            logger.error(traceback.format_exc())
            if not isinstance(e, DatabaseError):
                raise DatabaseError(f"Failed to initialize database: {str(e)}")
            else:
                raise
