from abc import ABC
import asyncio
import time
from typing import Any, Dict, List, Optional, TypeVar, Callable
from functools import wraps
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from contextlib import asynccontextmanager
from fiber.logging_utils import get_logger
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
import traceback
from datetime import datetime, timezone # Added import

logger = get_logger(__name__)

T = TypeVar('T')

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
    DEFAULT_QUERY_TIMEOUT = 120  # Changed from 30 seconds
    DEFAULT_TRANSACTION_TIMEOUT = 180  # 3 minutes
    DEFAULT_CONNECTION_TIMEOUT = 60  # 10 seconds

    # Operation constants
    DEFAULT_BATCH_SIZE = 1000
    MAX_RETRIES = 3
    MAX_CONNECTIONS = 20
    
    # Pool health check settings
    POOL_HEALTH_CHECK_INTERVAL = 60  # seconds
    POOL_RECOVERY_ATTEMPTS = 3

    # Circuit breaker settings
    CIRCUIT_BREAKER_THRESHOLD = 5
    CIRCUIT_BREAKER_RECOVERY_TIME = 60  # seconds

    # New timeout constants for finer control
    CONNECTION_TEST_TIMEOUT = 20  # More aggressive timeout for simple SELECT 1 in _test_connection
    ENGINE_COMMAND_TIMEOUT = 20   # Default command timeout for asyncpg, affects pool_pre_ping

    # Operation statuses
    STATUS_PENDING = 'pending'
    STATUS_PROCESSING = 'processing'
    STATUS_COMPLETED = 'completed'
    STATUS_ERROR = 'error'
    STATUS_TIMEOUT = 'timeout'

    def __new__(cls, node_type: str, *args, **kwargs):
        """Ensure singleton instance per node type"""
        if node_type not in cls._instances:
            cls._instances[node_type] = super().__new__(cls)
        return cls._instances[node_type]

    def __init__(
        self,
        node_type: str,
        host: str = "localhost",
        port: int = 5432,
        database: str = "bittensor",
        user: str = "postgres",
        password: str = "postgres",
    ):
        """Initialize database connection parameters and engine."""
        if not hasattr(self, "initialized"):
            self.node_type = node_type
            self.db_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
            
            self._active_sessions = set()
            self._active_operations = 0
            
            self._last_pool_check = 0
            self._pool_health_status = True
            self._pool_recovery_lock = asyncio.Lock()
            
            self._circuit_breaker = {
                'failures': 0,
                'last_failure_time': 0,
                'status': 'closed'
            }
            
            self._resource_stats = {
                'cpu_percent': 0,
                'memory_rss': 0,
                'open_files': 0,
                'connections': 0,
                'last_check': 0
            }
            
            self._operation_stats = {
                'ddl_operations': 0,
                'read_operations': 0,
                'write_operations': 0,
                'long_running_queries': [],
                'total_sessions_acquired': 0,
                'total_session_time_ms': 0.0,
                'max_session_time_ms': 0.0,
                'min_session_time_ms': float('inf'),
                'avg_session_time_ms': 0.0,
                'top_long_sessions': [] # List of dicts: [{'duration_ms': float, 'operation_name': str, 'acquired_at_iso': str}]
            }
            
            self._engine = None
            self._engine_initialized = False
            self.initialized = True
            self.db_loop = None

    def get_session_stats(self) -> Dict[str, Any]:
        """Returns a dictionary of current session statistics."""
        return {
            'total_sessions_acquired': self._operation_stats.get('total_sessions_acquired', 0),
            'total_session_time_ms': self._operation_stats.get('total_session_time_ms', 0.0),
            'max_session_time_ms': self._operation_stats.get('max_session_time_ms', 0.0),
            'min_session_time_ms': self._operation_stats.get('min_session_time_ms', float('inf')),
            'avg_session_time_ms': self._operation_stats.get('avg_session_time_ms', 0.0),
            'top_long_sessions': self._operation_stats.get('top_long_sessions', [])
        }

    async def ensure_engine_initialized(self):
        """Ensure the engine is initialized before use."""
        if not self._engine_initialized:
            await self._initialize_engine()
            self._engine_initialized = True

    async def _check_circuit_breaker(self) -> bool:
        if self._circuit_breaker['status'] == 'open':
            if (time.time() - self._circuit_breaker['last_failure_time'] 
                > self.CIRCUIT_BREAKER_RECOVERY_TIME):
                self._circuit_breaker['status'] = 'half-open'
                self._circuit_breaker['failures'] = 0
                logger.info("Circuit breaker entering half-open state")
            else:
                return False
        return True

    async def _update_circuit_breaker(self, success: bool) -> None:
        if success:
            if self._circuit_breaker['status'] == 'half-open':
                self._circuit_breaker['status'] = 'closed'
                logger.info("Circuit breaker closed after successful recovery")
            self._circuit_breaker['failures'] = 0
        else:
            self._circuit_breaker['failures'] += 1
            self._circuit_breaker['last_failure_time'] = time.time()
            
            if (self._circuit_breaker['failures'] >= self.CIRCUIT_BREAKER_THRESHOLD and 
                self._circuit_breaker['status'] != 'open'):
                self._circuit_breaker['status'] = 'open'
                logger.warning("Circuit breaker opened due to repeated failures")

    async def _monitor_resources(self) -> Dict[str, Any]:
        if not PSUTIL_AVAILABLE:
            return None
        try:
            process = psutil.Process()
            self._resource_stats.update({
                'cpu_percent': process.cpu_percent(),
                'memory_rss': process.memory_info().rss,
                'open_files': len(process.open_files()),
                'connections': len(process.connections()),
                'last_check': time.time()
            })
            if self._resource_stats['cpu_percent'] > 80:
                logger.warning(f"High CPU usage: {self._resource_stats['cpu_percent']}%")
            if self._resource_stats['memory_rss'] > (1024 * 1024 * 1024):
                logger.warning(
                    f"High memory usage: "
                    f"{self._resource_stats['memory_rss'] / (1024*1024):.2f}MB"
                )
            return self._resource_stats
        except Exception as e:
            logger.error(f"Error monitoring resources: {e}")
            return None

    async def check_health(self) -> Dict[str, Any]:
        health_info = {
            'status': 'healthy',
            'timestamp': time.time(),
            'pool': {
                'active_sessions': len(self._active_sessions),
                'operations': self._active_operations,
                'last_pool_check': self._last_pool_check,
                'pool_healthy': self._pool_health_status
            },
            'circuit_breaker': {
                'status': self._circuit_breaker['status'],
                'failures': self._circuit_breaker['failures'],
                'last_failure': self._circuit_breaker['last_failure_time']
            },
            'resources': await self._monitor_resources(),
            'connection_test': False,
            'errors': []
        }
        try:
            async with self.session(operation_name="check_health_select_1") as session: # Added operation_name
                await session.execute(text("SELECT 1"))
            health_info['connection_test'] = True
        except Exception as e:
            health_info['status'] = 'unhealthy'
            health_info['errors'].append(str(e))
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
                healthy = await self._ensure_pool()
                if not healthy:
                    logger.warning("Pool health check failed - attempting recovery")
                    recovery_successful = False
                    for attempt in range(self.POOL_RECOVERY_ATTEMPTS):
                        try:
                            logger.info(f"Recovery attempt {attempt + 1}/{self.POOL_RECOVERY_ATTEMPTS}")
                            for session_obj in self._active_sessions.copy(): # Iterate over actual session objects if stored, else ids
                                try:
                                   if hasattr(session_obj, 'close'): await session_obj.close()
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
                            await asyncio.sleep(2 ** attempt)
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

    async def _initialize_engine(self) -> bool:
        if not self.db_url:
            raise DatabaseError("Database URL not initialized")
        try:
            current_loop = asyncio.get_running_loop()
            masked_url = str(self.db_url).replace(self.db_url.split('@')[0], '***')
            logger.info(f"Initializing database engine with URL: {masked_url} on loop {current_loop}")
            self._engine = create_async_engine(
                self.db_url,
                pool_pre_ping=True,
                pool_size=self.MAX_CONNECTIONS,
                max_overflow=10,
                pool_timeout=self.DEFAULT_CONNECTION_TIMEOUT,
                pool_recycle=3600,
                pool_use_lifo=True,
                echo=False,
                connect_args={
                    "command_timeout": self.ENGINE_COMMAND_TIMEOUT,
                    "timeout": self.DEFAULT_CONNECTION_TIMEOUT,
                    "server_settings": {
                        "jit": "off"
                    },
                },
            )
            async with self._engine.connect() as conn:
                await asyncio.wait_for(conn.execute(text("SELECT 1")), timeout=self.CONNECTION_TEST_TIMEOUT)
            self._session_factory = async_sessionmaker(
                self._engine, class_=AsyncSession, expire_on_commit=False, autobegin=False
            )
            self.db_loop = current_loop
            logger.info(f"Database engine initialized successfully on loop {self.db_loop}")
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
                conn.execute(text("SELECT 1")),
                timeout=self.DEFAULT_CONNECTION_TIMEOUT
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
                    logger.info(f"Cleaning up connection pool. Size: {size}, "
                              f"Checked-in: {checkedin}, Overflow: {overflow}")
                    async with self.get_connection() as conn: # This will use a session with op_name
                        await conn.execute(text("SELECT 1"))
                    await self._engine.dispose()
        except Exception as e:
            logger.error(f"Error cleaning up stale connections: {e}")

    def _update_top_long_sessions(self, duration_ms: float, name: str, acquired_at_iso_str: str):
        """Helper to update the list of top longest sessions."""
        try:
            # Ensure the list exists in stats
            if 'top_long_sessions' not in self._operation_stats:
                self._operation_stats['top_long_sessions'] = []
            
            current_top_sessions = self._operation_stats['top_long_sessions']
            
            # Add the new session info as a dictionary
            current_top_sessions.append({
                'duration_ms': duration_ms, 
                'operation_name': name, 
                'acquired_at_iso': acquired_at_iso_str # Use the passed ISO string directly
            })
            
            # Sort by duration_ms in descending order
            current_top_sessions.sort(key=lambda x: x['duration_ms'], reverse=True)
            
            # Keep only the top 3
            self._operation_stats['top_long_sessions'] = current_top_sessions[:3]
        except Exception as e:
            logger.error(f"Error updating top long sessions: {e} - Stats: {self._operation_stats}")

    def with_timeout(timeout: float):
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @wraps(func)
            async def wrapper(self, *args, **kwargs) -> T:
                call_timeout = kwargs.get('timeout')
                effective_timeout = call_timeout if call_timeout is not None else timeout
                try:
                    return await asyncio.wait_for(
                        func(self, *args, **kwargs), 
                        timeout=effective_timeout
                    )
                except asyncio.TimeoutError:
                    op_name_for_log = getattr(func, '__name__', 'Unnamed_Operation')
                    logger.error(f"Operation {op_name_for_log} timed out after {effective_timeout}s")
                    raise DatabaseTimeout(f"Operation {op_name_for_log} timed out after {effective_timeout}s")
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
    async def session(self, operation_name: str = "Unnamed Session", operation_type: str = "read", provided_session: Optional[AsyncSession] = None):
        await self.ensure_engine_initialized() # Ensure engine is ready

        start_time = time.monotonic()
        session_id_str = "provided" if provided_session else "new"
        
        # Attempt to get a more specific operation name if it's a query
        if operation_name.startswith("fetch_") or operation_name.startswith("execute"):
            try:
                # Simplified extraction, assuming format like "fetch_all:SELECT ..." or "execute:UPDATE ..."
                parts = operation_name.split(":", 1)
                if len(parts) > 1:
                    query_preview = parts[1].strip()[:50].replace('\\n', ' ') + "..."
                    specific_op_name = f"{parts[0]}:{query_preview}"
                else:
                    specific_op_name = operation_name
            except: # Fallback if parsing fails
                specific_op_name = operation_name
        else:
            specific_op_name = operation_name

        session_instance: Optional[AsyncSession] = None
        transaction_started_here = False
        acquired_at_iso_str = datetime.now(timezone.utc).isoformat()
        e = None # Initialize e to None

        try:
            if provided_session:
                session_instance = provided_session
                session_id_str = f"provided_{id(session_instance)}"
                # logger.debug(f"Using provided session {id(session_instance)} for {specific_op_name}")
                if not session_instance.in_transaction() and not session_instance.is_active: # type: ignore
                    # This typically means the session was closed or rolled back by an outer manager
                    # For safety, we might want to begin a new transaction or raise an error
                    # For now, let's assume if provided, it's managed externally or ready.
                    # However, if it's not in_transaction AND not active, it's problematic.
                    # Let's try to begin one if it's not active.
                    logger.warning(f"Provided session {id(session_instance)} for {specific_op_name} is not in transaction and not active. Attempting to begin.")
                    try:
                        logger.debug(f"Session {id(session_instance)} ({specific_op_name}): Attempting to begin new transaction on provided (but inactive) session.")
                        await session_instance.begin()
                        transaction_started_here = True
                        logger.debug(f"Session {id(session_instance)} ({specific_op_name}): New transaction started successfully on provided session.")
                    except Exception as e_begin_provided:
                        logger.error(f"Session {id(session_instance)} ({specific_op_name}): FAILED to begin new transaction on provided session: {e_begin_provided}", exc_info=True)
                        raise # Re-raise if we can't even begin
            else:
                if not self._engine or not self._session_factory:
                    logger.error("Database engine or session factory not initialized before acquiring session.")
                    raise DatabaseConnectionError("Engine/Session factory not initialized")
                
                session_instance = self._session_factory()
                session_id_str = f"new_{id(session_instance)}"
                self._operation_stats['total_sessions_acquired'] += 1
                # logger.debug(f"Acquired new session {id(session_instance)} for {specific_op_name}")

            # Ensure a transaction is started if not already active (and not using a provided session that might manage its own tx)
            # Or if it IS a provided session, but we detected it was inactive and started one above.
            if not session_instance.in_transaction(): # type: ignore
                logger.debug(f"Session {id(session_instance)} ({specific_op_name}): Not in transaction. Attempting to begin.")
                try:
                    await session_instance.begin() # type: ignore
                    transaction_started_here = True
                    logger.debug(f"Session {id(session_instance)} ({specific_op_name}): Transaction started successfully.")
                except Exception as e_begin:
                    logger.error(f"Session {id(session_instance)} ({specific_op_name}): FAILED to begin transaction: {e_begin}", exc_info=True)
                    # If begin fails, we might not want to proceed, or yield a non-transacting session.
                    # For now, let's re-raise as it's fundamental.
                    if session_instance and not provided_session: # Only close if newly acquired
                        await session_instance.close()
                    raise
            else:
                logger.debug(f"Session {id(session_instance)} ({specific_op_name}): Already in transaction.")

            self._active_sessions.add(session_id_str) # Add to set before incrementing general counter
            active_sessions_count = self._increment_active_sessions()
            logger.debug(f"Session {id(session_instance)} ({specific_op_name}) ready. Active sessions: {active_sessions_count} (Set size: {len(self._active_sessions)})")

            yield session_instance

            # If we started the transaction here, we are responsible for committing it.
            if transaction_started_here and session_instance and session_instance.in_transaction(): # type: ignore
                logger.debug(f"Session {id(session_instance)} ({specific_op_name}): Attempting to commit transaction.")
                try:
                    await session_instance.commit() # type: ignore
                    logger.debug(f"Session {id(session_instance)} ({specific_op_name}): Transaction committed successfully.")
                except Exception as e_commit:
                    logger.error(f"Session {id(session_instance)} ({specific_op_name}): FAILED to commit transaction: {e_commit}", exc_info=True)
                    # Attempt to rollback if commit failed
                    try:
                        logger.warning(f"Session {id(session_instance)} ({specific_op_name}): Commit failed, attempting rollback.")
                        await session_instance.rollback()
                        logger.info(f"Session {id(session_instance)} ({specific_op_name}): Rollback successful after failed commit.")
                    except Exception as e_rollback_after_commit_fail:
                        logger.error(f"Session {id(session_instance)} ({specific_op_name}): FAILED to rollback after failed commit: {e_rollback_after_commit_fail}", exc_info=True)
                    raise # Re-raise the original commit error
            elif transaction_started_here and session_instance and not session_instance.in_transaction(): # type: ignore
                logger.warning(f"Session {id(session_instance)} ({specific_op_name}): Transaction was started here, but session is no longer in transaction before explicit commit. (Potentially committed or rolled back by yielded code).")
            elif not transaction_started_here and session_instance and session_instance.in_transaction(): #type: ignore
                logger.debug(f"Session {id(session_instance)} ({specific_op_name}): Transaction was not started here, not attempting commit. (External transaction management).")


        except asyncio.CancelledError:
            logger.warning(f"Session {id(session_instance)} ({specific_op_name}): Operation cancelled.")
            if session_instance and session_instance.in_transaction(): # type: ignore
                logger.debug(f"Session {id(session_instance)} ({specific_op_name}): Attempting rollback due to cancellation.")
                try:
                    await session_instance.rollback() # type: ignore
                    logger.info(f"Session {id(session_instance)} ({specific_op_name}): Rollback successful due to cancellation.")
                except Exception as e_rollback_cancel:
                    logger.error(f"Session {id(session_instance)} ({specific_op_name}): FAILED to rollback due to cancellation: {e_rollback_cancel}", exc_info=True)
            self._operation_stats.setdefault('cancelled_operations', 0)
            self._operation_stats['cancelled_operations'] += 1
            await self._mark_operation_failed()
            raise
        except Exception as e:
            logger.error(f"Session {id(session_instance)} ({specific_op_name}): Error during session: {str(e)}", exc_info=True)
            if session_instance and session_instance.in_transaction(): # type: ignore
                logger.debug(f"Session {id(session_instance)} ({specific_op_name}): Attempting rollback due to exception: {str(e)}.")
                try:
                    await session_instance.rollback() # type: ignore
                    logger.info(f"Session {id(session_instance)} ({specific_op_name}): Rollback successful due to exception.")
                except Exception as e_rollback_exception:
                    logger.error(f"Session {id(session_instance)} ({specific_op_name}): FAILED to rollback due to exception: {e_rollback_exception}", exc_info=True)
            await self._mark_operation_failed()
            raise
        finally:
            session_release_time = time.perf_counter()
            session_duration_ms = (session_release_time - start_time) * 1000

            self._operation_stats['total_session_time_ms'] += session_duration_ms
            if self._operation_stats['total_sessions_acquired'] > 0:
                self._operation_stats['avg_session_time_ms'] = self._operation_stats['total_session_time_ms'] / self._operation_stats['total_sessions_acquired']
            if session_duration_ms > self._operation_stats['max_session_time_ms']:
                self._operation_stats['max_session_time_ms'] = session_duration_ms
            if session_duration_ms < self._operation_stats['min_session_time_ms']:
                self._operation_stats['min_session_time_ms'] = session_duration_ms

            logger.debug(f"Session {id(session_instance)} ({specific_op_name}) held for {session_duration_ms:.2f}ms")
            self._update_top_long_sessions(session_duration_ms, specific_op_name, acquired_at_iso_str)
            
            if session_id_str in self._active_sessions: 
                self._active_sessions.remove(session_id_str)
                logger.debug(f"Session {session_id_str} released ({specific_op_name}). Active sessions: {len(self._active_sessions)}")
            
            self._active_operations -= 1
            if transaction_started_here and session_instance and not session_instance.in_transaction() and not isinstance(e, BaseException):
                await self._update_circuit_breaker(True)
            elif not transaction_started_here and not isinstance(e, BaseException):
                await self._update_circuit_breaker(True)

    @with_timeout(CONNECTION_TEST_TIMEOUT)
    async def _test_connection(self, session: AsyncSession) -> bool:
        await session.execute(text("SELECT 1"))
        return True

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_one(
        self, 
        query: str, 
        params: Optional[Dict] = None,
        timeout: Optional[float] = None
    ) -> Optional[Dict]:
        start_time = time.time()
        op_name = f"fetch_one:{query[:100]}"
        async with self.session(operation_name=op_name) as session: # session() ensures a transaction
            try:
                # No longer need session.begin() here, self.session() handles it.
                result = await session.execute(text(query), params or {})
                row = result.first()
                duration = time.time() - start_time
                if duration > self.DEFAULT_QUERY_TIMEOUT / 2: 
                    logger.warning(f"Slow query detected ({op_name}): {duration:.2f}s\nQuery: {query}")
                return dict(row._mapping) if row else None
            except SQLAlchemyError as e:
                logger.error(f"Database error in fetch_one ({op_name}): {str(e)}\nQuery: {query}")
                # The main session context manager will handle rollback.
                raise DatabaseError(f"Error executing query ({op_name}): {str(e)}")

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_all(
        self, 
        query: str, 
        params: Optional[Dict] = None,
        timeout: Optional[float] = None
    ) -> List[Dict]:
        start_time = time.time()
        op_name = f"fetch_all:{query[:100]}"
        async with self.session(operation_name=op_name) as session: # session() ensures a transaction
            try:
                # No longer need session.begin() here, self.session() handles it.
                result = await session.execute(text(query), params or {})
                rows = result.all()
                if len(rows) > 1000:
                    logger.warning(
                        f"Large result set ({op_name}): {len(rows)} rows\n"
                        f"Query: {query}"
                    )
                duration = time.time() - start_time
                if duration > self.DEFAULT_QUERY_TIMEOUT / 2: 
                    logger.warning(f"Slow query detected ({op_name}): {duration:.2f}s\nQuery: {query}")
                return [dict(row._mapping) for row in rows]
            except SQLAlchemyError as e:
                logger.error(f"Database error in fetch_all ({op_name}): {str(e)}\nQuery: {query}")
                # The main session context manager will handle rollback.
                raise DatabaseError(f"Error executing query ({op_name}): {str(e)}")

    @with_timeout(DEFAULT_TRANSACTION_TIMEOUT) # Default timeout for execute
    async def execute(
        self, 
        query: str, 
        params: Optional[Dict] = None,
        timeout: Optional[float] = None, # Allows per-call override
        session: Optional[AsyncSession] = None # Existing session to use
    ) -> Any:
        op_name = f"execute:{query[:100]}"
        if session is not None: 
            try:
                # When a session is provided, we assume the caller manages the transaction.
                result = await session.execute(text(query), params or {})
                return result
            except SQLAlchemyError as e:
                logger.error(f"Database error in execute (with existing session, op: {op_name}): {str(e)}\nQuery: {query}")
                raise DatabaseError(f"Error executing query (with existing session, op: {op_name}): {str(e)}")
        else: 
            async with self.session(operation_name=op_name) as new_session: # session() ensures a transaction
                try:
                    # No longer need new_session.begin() here, self.session() handles it.
                    result = await new_session.execute(text(query), params or {})
                    return result 
                except SQLAlchemyError as e:
                    logger.error(f"Database error in execute (new session, op: {op_name}): {str(e)}\nQuery: {query}")
                    # The main session context manager will handle rollback.
                    raise DatabaseError(f"Error executing query (new session, op: {op_name}): {str(e)}")

    @with_timeout(DEFAULT_TRANSACTION_TIMEOUT)
    async def execute_many(
        self, 
        query: str, 
        params_list: List[Dict],
        timeout: Optional[float] = None,
        batch_size: Optional[int] = None
    ) -> None:
        if not params_list:
            return
        batch_size = batch_size or self.DEFAULT_BATCH_SIZE
        total_items = len(params_list)
        
        avg_param_size = sum(len(str(p)) for p in params_list[:100]) / min(100, total_items) if total_items > 0 else 0
        if avg_param_size > 1000:
            batch_size = min(batch_size, 100)
        
        start_time = time.time()
        op_name = f"execute_many:{query[:100]}"

        # execute_many always manages its own transaction here
        async with self.session(operation_name=op_name) as session: # Pass operation_name
            try:
                # Begin transaction once for all batches
                async with session.begin():
                    for i in range(0, total_items, batch_size):
                        batch = params_list[i:i + batch_size]
                        batch_start_time = time.time()
                        
                        await session.execute(text(query), batch)
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
                    f"Batch operation failed ({op_name}) at item approx {i}: {str(e)}\n" # 'i' might be from previous scope if error in begin()
                    f"Query: {query}"
                )
                # Rollback is handled by session.begin() context manager on error
                raise DatabaseError(f"Error executing batch query ({op_name}): {str(e)}")

    async def execute_with_retry(
        self, 
        query: str, 
        params: Optional[Dict] = None, 
        max_retries: Optional[int] = None,
        initial_delay: float = 0.1
    ) -> Any:
        max_retries = max_retries or self.MAX_RETRIES
        last_error = None
        op_name = f"execute_with_retry:{query[:80]}" # Shorter op_name for retry wrapper

        for attempt in range(max_retries):
            try:
                # The execute call will use its own session with a more specific op_name
                return await self.execute(query, params) 
            except DatabaseError as e: # Catch DatabaseError, which includes DatabaseTimeout
                last_error = e
                if attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt)
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
                raise last_error # Re-raise the last error after all retries exhausted

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
        self, 
        table_name: str, 
        status_column: str = 'status',
        timeout_minutes: int = 60
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
            "timeout": timeout_minutes
        }
        # execute will create its own session with appropriate operation_name
        await self.execute(query, params)

    async def reset_pool(self) -> None:
        try:
            logger.info("Starting database pool reset")
            active_session_ids_copy = list(self._active_sessions) # Copy IDs
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
            await self._initialize_engine() # This sets _engine_initialized to True on success
            
            logger.info("Verifying new connection pool")
            if not await self._ensure_pool(): # This also re-sets _engine_initialized if it fails
                self._engine_initialized = False # Ensure it's false if ensure_pool fails
                raise DatabaseError("Failed to verify new connection pool after reset")
            
            self._last_pool_check = time.time()
            self._pool_health_status = True
            self._circuit_breaker['failures'] = 0
            self._circuit_breaker['status'] = 'closed'
            
            logger.info("Database pool reset completed successfully")
                
        except Exception as e:
            logger.error(f"Error resetting connection pool: {e}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            self._pool_health_status = False
            self._engine_initialized = False # Ensure this is false on any error during reset
            # Do not re-raise DatabaseError here if already DatabaseError, to avoid nesting.
            if not isinstance(e, DatabaseError):
                 raise DatabaseError(f"Failed to reset connection pool: {str(e)}")
            else:
                raise # Re-raise original DatabaseError

    async def close(self) -> None:
        """Close all database connections and cleanup resources."""
        try:
            # Similar to reset_pool, direct closing of sessions by ID is not feasible
            # if only IDs are stored. Rely on engine disposal.
            self._active_sessions.clear()
            self._active_operations = 0

            if self._engine:
                await self._engine.dispose()
                
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
            async def wrapper(self: 'BaseDatabaseManager', *args, **kwargs) -> T:
                op_name = func.__name__ # Get the name of the decorated function
                # self.session() handles beginning the transaction internally via _session_factory and autobegin=False
                # then async with session.begin() is used inside the session block.
                # The session obtained from self.session() will start a transaction when session.begin() is called.
                async with self.session(operation_name=op_name) as session: # Pass operation_name
                    async with session.begin(): # Start the actual transaction
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