from abc import ABC
import asyncio
import time
from typing import Any, Dict, List, Optional, TypeVar, Callable
from functools import wraps
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from contextlib import asynccontextmanager
from fiber.logging_utils import get_logger
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
import traceback

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
    DEFAULT_QUERY_TIMEOUT = 60  # 60 seconds
    DEFAULT_TRANSACTION_TIMEOUT = 180  # 3 minutes
    DEFAULT_CONNECTION_TIMEOUT = 600  # 600 seconds (10 minutes)

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
    ENGINE_COMMAND_TIMEOUT = 600   # Default command timeout for asyncpg, 600 seconds (10 minutes)

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
            
            # Connection management
            self._active_sessions = set()
            self._active_operations = 0
            self._operation_lock = asyncio.Lock()
            self._session_lock = asyncio.Lock()
            
            # Pool health monitoring
            self._last_pool_check = 0
            self._pool_health_status = True
            self._pool_recovery_lock = asyncio.Lock()
            
            # Circuit breaker state
            self._circuit_breaker = {
                'failures': 0,
                'last_failure_time': 0,
                'status': 'closed'  # 'closed', 'open', 'half-open'
            }
            
            # Resource monitoring
            self._resource_stats = {
                'cpu_percent': 0,
                'memory_rss': 0,
                'open_files': 0,
                'connections': 0,
                'last_check': 0
            }
            
            # Engine will be initialized when first needed
            self._engine = None
            self._engine_initialized = False
            self.initialized = True

    async def ensure_engine_initialized(self):
        """Ensure the engine is initialized before use."""
        if not self._engine_initialized:
            await self._initialize_engine()
            self._engine_initialized = True

    async def _check_circuit_breaker(self) -> bool:
        """
        Check if circuit breaker allows operations.
        
        Returns:
            bool: True if circuit breaker is closed, False if open
        """
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
        """Update circuit breaker state based on operation success."""
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
        """
        Monitor and log system resource usage.
        
        Returns:
            Dict[str, Any]: Resource usage statistics
        """
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

            # Log warnings for high resource usage
            if self._resource_stats['cpu_percent'] > 80:
                logger.warning(f"High CPU usage: {self._resource_stats['cpu_percent']}%")
            
            if self._resource_stats['memory_rss'] > (1024 * 1024 * 1024):  # 1GB
                logger.warning(
                    f"High memory usage: "
                    f"{self._resource_stats['memory_rss'] / (1024*1024):.2f}MB"
                )

            return self._resource_stats
        except Exception as e:
            logger.error(f"Error monitoring resources: {e}")
            return None

    async def check_health(self) -> Dict[str, Any]:
        """
        Perform comprehensive health check of the database system.
        
        Returns:
            Dict[str, Any]: Health check results
        """
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
            # Test basic connectivity
            async with self.session() as session:
                await session.execute(text("SELECT 1"))
            health_info['connection_test'] = True
        except Exception as e:
            health_info['status'] = 'unhealthy'
            health_info['errors'].append(str(e))

        return health_info

    async def _check_pool_health(self) -> bool:
        """
        Periodic pool health check with recovery.
        
        Returns:
            bool: True if pool is healthy, False otherwise
        """
        current_time = time.time()
        if current_time - self._last_pool_check < self.POOL_HEALTH_CHECK_INTERVAL:
            return self._pool_health_status

        async with self._pool_recovery_lock:
            try:
                # First check if engine exists and is initialized
                if not self._engine or not self._engine_initialized:
                    logger.error("Database engine not initialized")
                    self._pool_health_status = False
                    return False

                # Test basic connectivity
                healthy = await self._ensure_pool()
                
                if not healthy:
                    logger.warning("Pool health check failed - attempting recovery")
                    recovery_successful = False
                    
                    for attempt in range(self.POOL_RECOVERY_ATTEMPTS):
                        try:
                            logger.info(f"Recovery attempt {attempt + 1}/{self.POOL_RECOVERY_ATTEMPTS}")
                            
                            # Close all active sessions first
                            async with self._session_lock:
                                for session in self._active_sessions.copy():
                                    try:
                                        await session.close()
                                    except Exception as e:
                                        logger.error(f"Error closing session: {e}")
                                self._active_sessions.clear()
                                self._active_operations = 0

                            # Dispose of the engine
                            if self._engine:
                                await self._engine.dispose()
                            
                            # Reinitialize engine
                            await self._initialize_engine()
                            
                            # Verify the new pool
                            if await self._ensure_pool():
                                logger.info("Pool recovery successful")
                                recovery_successful = True
                                break
                                
                        except Exception as e:
                            logger.error(f"Recovery attempt {attempt + 1} failed: {e}")
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
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
        """Initialize the database engine."""
        if not self.db_url:
            raise DatabaseError("Database URL not initialized")

        try:
            # Log initialization with masked credentials
            masked_url = str(self.db_url).replace(self.db_url.split('@')[0], '***')
            logger.info(f"Initializing database engine with URL: {masked_url}")

            # Configure engine with connection pooling settings
            self._engine = create_async_engine(
                self.db_url,
                pool_pre_ping=True,
                pool_size=self.MAX_CONNECTIONS,
                max_overflow=10,
                pool_timeout=self.DEFAULT_CONNECTION_TIMEOUT,
                pool_recycle=300,  # Recycle connections every 5 minutes
                pool_use_lifo=True,  # Use LIFO to better reuse connections
                echo=False,
                connect_args={
                    "command_timeout": self.ENGINE_COMMAND_TIMEOUT,
                    "timeout": self.DEFAULT_CONNECTION_TIMEOUT,
                },
            )

            # Test connection immediately
            async with self._engine.connect() as conn:
                await asyncio.wait_for(conn.execute(text("SELECT 1")), timeout=self.CONNECTION_TEST_TIMEOUT)

            # Initialize session factory
            self._session_factory = async_sessionmaker(
                self._engine, class_=AsyncSession, expire_on_commit=False, autobegin=False
            )

            logger.info("Database engine initialized successfully")
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
        """
        Ensure database pool exists and is healthy.
        
        Returns:
            bool: True if pool is healthy, False otherwise
        """
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

    @asynccontextmanager
    async def get_connection(self):
        """Get a raw database connection with proper cleanup."""
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
        """Clean up stale connections in the pool."""
        if not self._engine:
            return

        try:
            pool = self._engine.pool
            if pool:
                # Get pool statistics
                size = pool.size()
                checkedin = pool.checkedin()
                overflow = pool.overflow()
                
                if overflow > 0 or checkedin > size * 0.8:  # If pool is getting full
                    logger.info(f"Cleaning up connection pool. Size: {size}, "
                              f"Checked-in: {checkedin}, Overflow: {overflow}")
                    
                    # Force a few connections to close and be recreated
                    async with self.get_connection() as conn:
                        await conn.execute(text("SELECT 1"))
                    
                    # Let SQLAlchemy know it can dispose of overflow connections
                    await self._engine.dispose()
        except Exception as e:
            logger.error(f"Error cleaning up stale connections: {e}")
            # Don't raise - this is a background cleanup task

    def with_timeout(timeout: float):
        """
        Decorator that adds timeout to a database operation.
        
        Args:
            timeout (float): Timeout in seconds
            
        Returns:
            Callable: Decorated function with timeout
        """
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @wraps(func)
            async def wrapper(self, *args, **kwargs) -> T:
                try:
                    return await asyncio.wait_for(
                        func(self, *args, **kwargs),
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    logger.error(f"Operation timed out after {timeout}s")
                    raise DatabaseTimeout(f"Operation timed out after {timeout}s")
            return wrapper
        return decorator

    @asynccontextmanager
    async def session(self):
        """
        Context manager for database sessions.
        Handles connection pooling and cleanup.
        
        Yields:
            AsyncSession: Database session
        """
        await self.ensure_engine_initialized()
        if not self._engine or not self._session_factory:
            logger.error("Database engine or session factory not initialized. Cannot create session.")
            raise DatabaseConnectionError("Engine/Session factory not initialized")

        # ADDED: Log pool status
        if self._engine and hasattr(self._engine.pool, 'status'):
            logger.debug(f"Pool status at session entry: {self._engine.pool.status()}")
        # END ADDED

        if not await self._check_circuit_breaker():
            raise CircuitBreakerError("Circuit breaker is open")
            
        session_instance: Optional[AsyncSession] = None
        start_time = time.time() # This is overall session time
        caller = traceback.extract_stack()[-3]  # Get calling frame
        session_id_str = "unassigned"
        
        try:
            # ADDED: Log pool status just before session creation
            if self._engine and hasattr(self._engine.pool, 'status'):
                logger.debug(f"Pool status before _session_factory() call: {self._engine.pool.status()}")
            # END ADDED
            
            t_before_session_factory = time.time()
            session_instance = self._session_factory()
            t_after_session_factory = time.time()
            logger.debug(f"Session factory call for {id(session_instance)} took: {t_after_session_factory - t_before_session_factory:.4f}s")
            
            session_id_str = str(id(session_instance))

            async with self._session_lock:
                self._active_sessions.add(session_instance)
                self._active_operations += 1
            
            yield session_instance
                
            await self._update_circuit_breaker(True)
                
        except Exception as e:
            await self._update_circuit_breaker(False)
            logger.error(
                f"Session error in {session_id_str} from "
                f"{caller.filename}:{caller.lineno} ({caller.name}): {str(e)}"
            )
            if isinstance(e, (DatabaseError, SQLAlchemyError)):
                raise
            raise DatabaseConnectionError(f"Session error for {session_id_str}: {str(e)}") from e
        
        finally:
            if session_instance:
                try:
                    async with self._session_lock:
                        if session_instance in self._active_sessions:
                            self._active_sessions.remove(session_instance)
                            self._active_operations -= 1
                    await session_instance.close()
                    
                    duration = time.time() - start_time
                    if duration > self.DEFAULT_QUERY_TIMEOUT / 2:
                        logger.warning(
                            f"Long-running session {session_id_str} usage detected: "
                            f"{duration:.2f}s from {caller.filename}:{caller.lineno} ({caller.name})"
                        )
                except Exception as e:
                    logger.error(
                        f"Error cleaning up session {session_id_str} from "
                        f"{caller.filename}:{caller.lineno} ({caller.name}): {e}"
                    )

    @with_timeout(CONNECTION_TEST_TIMEOUT)
    async def _test_connection(self, session: AsyncSession) -> bool:
        """Test database connection"""
        await session.execute(text("SELECT 1"))
        return True

    async def fetch_one(
        self, 
        query: str, 
        params: Optional[Dict] = None,
        timeout: Optional[float] = None,
        session: Optional[AsyncSession] = None
    ) -> Optional[Dict]:
        """
        Fetch a single row from the database.
        If called within a transaction context, uses the existing transaction.
        Otherwise, creates a new transaction.
        
        Args:
            query (str): SQL query to execute
            params (Optional[Dict]): Query parameters
            timeout (Optional[float]): Operation timeout in seconds
            session (Optional[AsyncSession]): Optional existing session to use
            
        Returns:
            Optional[Dict]: Single row result or None
        """
        start_time = time.time()
        
        current_session: AsyncSession
        try:
            if session:
                current_session = session
                # Caller manages the transaction for a provided session
                result = await current_session.execute(text(query), params or {})
                row = result.first()
            else:
                async with self.session() as new_session:
                    current_session = new_session
                    # For read-only, begin() is not strictly necessary for some DBs,
                    # but good practice for consistency or if query might have side effects.
                    async with current_session.begin():
                        result = await current_session.execute(text(query), params or {})
                    row = result.first() # result is available after transaction block

            # Log slow queries
            duration = time.time() - start_time
            if duration > (timeout or self.DEFAULT_QUERY_TIMEOUT) / 2: # Use provided timeout if available
                logger.warning(f"Slow query detected: {duration:.2f}s\\nQuery: {query}")
            
            return dict(row._mapping) if row else None
        except SQLAlchemyError as e:
            logger.error(f"Database error in fetch_one: {str(e)}\\nQuery: {query}")
            raise DatabaseError(f"Error executing query: {str(e)}")

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_all(
        self, 
        query: str, 
        params: Optional[Dict] = None,
        timeout: Optional[float] = None, # This timeout param is not used by the decorator
        session: Optional[AsyncSession] = None
    ) -> List[Dict]:
        overall_start_time = time.time() # For fetch_all total duration
        
        current_session: AsyncSession
        try:
            if session:
                current_session = session
                # Caller manages the transaction for a provided session
                result = await current_session.execute(text(query), params or {})
                rows = result.all()
            else:
                # Create new transaction if not in one
                logger.debug(f"fetch_all for query (first 100 chars): {query[:100]} - entering 'new session' block.")
                async with self.session() as new_session: # Session timing for the warning in logs starts here
                    current_session = new_session
                    session_id = id(current_session)
                    logger.debug(f"fetch_all (session {session_id}): Acquired session. Beginning transaction.")
                    t_before_begin = time.time()
                    # For read-only, begin() is not strictly necessary for some DBs,
                    # but good practice for consistency or if query might have side effects.
                    async with current_session.begin(): 
                        t_after_begin = time.time()
                        logger.debug(f"fetch_all (session {session_id}): Transaction began in {t_after_begin - t_before_begin:.4f}s. Executing query.")
                        
                        t_before_execute = time.time()
                        result = await current_session.execute(text(query), params or {})
                        t_after_execute = time.time()
                        logger.debug(f"fetch_all (session {session_id}): session.execute took {t_after_execute - t_before_execute:.4f}s.")

                    # result is available after transaction block
                    t_before_fetchall = time.time()
                    rows = result.all() 
                    t_after_fetchall = time.time()
                    logger.debug(f"fetch_all (session {session_id}): result.all() took {t_after_fetchall - t_before_fetchall:.4f}s. Rows: {len(rows)}")

            # Log large result sets
            if len(rows) > 1000:
                logger.warning(
                    f"Large result set (session {id(current_session)}): {len(rows)} rows\\n"
                    f"Query: {query}"
                )
            
            # Log slow queries (this is overall time for this path)
            duration = time.time() - overall_start_time 
            if duration > (timeout or self.DEFAULT_QUERY_TIMEOUT) / 2: # Use provided timeout if available
                logger.warning(f"Slow fetch_all path (session {id(current_session)}) detected: {duration:.2f}s for query (first 100): {query[:100]}")
            
            return [dict(row._mapping) for row in rows]
        except Exception as e: 
            current_session_id_val = id(current_session) if 'current_session' in locals() and current_session else "unknown"
            logger.error(f"Database error in fetch_all (session {current_session_id_val} path): {str(e)}\\nQuery: {query}", exc_info=True)
            if isinstance(e, (DatabaseError, SQLAlchemyError, asyncio.CancelledError)):
                raise
            raise DatabaseError(f"Error executing query in fetch_all (session {current_session_id_val}): {str(e)}") from e

    @with_timeout(DEFAULT_TRANSACTION_TIMEOUT)
    async def execute(
        self, 
        query: str, 
        params: Optional[Dict] = None,
        timeout: Optional[float] = None,
        session: Optional[AsyncSession] = None
    ) -> Any:
        """
        Execute a single database operation.
        If session is provided, uses it. Otherwise creates a new session.
        
        Args:
            query (str): SQL query to execute
            params (Optional[Dict]): Query parameters
            timeout (Optional[float]): Operation timeout in seconds
            session (Optional[AsyncSession]): Existing session to use
            
        Returns:
            Any: Query result
        """
        if session is not None:
            # Use existing session
            try:
                result = await session.execute(text(query), params or {})
                return result
            except SQLAlchemyError as e:
                logger.error(f"Database error in execute: {str(e)}\nQuery: {query}")
                raise DatabaseError(f"Error executing query: {str(e)}")
        else:
            # Create new session for single operation
            async with self.session() as session:
                try:
                    result = await session.execute(text(query), params or {})
                    return result
                except SQLAlchemyError as e:
                    logger.error(f"Database error in execute: {str(e)}\nQuery: {query}")
                    raise DatabaseError(f"Error executing query: {str(e)}")

    @with_timeout(DEFAULT_TRANSACTION_TIMEOUT)
    async def execute_many(
        self, 
        query: str, 
        params_list: List[Dict],
        timeout: Optional[float] = None,
        batch_size: Optional[int] = None,
        session: Optional[AsyncSession] = None
    ) -> None:
        """
        Execute multiple operations in batches.
        If called within a transaction context, uses the existing transaction.
        Otherwise, creates a new transaction.
        
        Args:
            query (str): SQL query to execute
            params_list (List[Dict]): List of parameter dictionaries
            timeout (Optional[float]): Operation timeout in seconds
            batch_size (Optional[int]): Size of batches for processing
            session (Optional[AsyncSession]): Optional existing session to use
        """
        if not params_list:
            return

        # Determine optimal batch size
        batch_size = batch_size or self.DEFAULT_BATCH_SIZE
        total_items = len(params_list)
        
        # Adaptive batch sizing based on data size
        # Ensure total_items is not zero before division
        avg_param_size_divisor = min(100, total_items) if total_items > 0 else 1
        avg_param_size = sum(len(str(p)) for p in params_list[:100]) / avg_param_size_divisor
        if avg_param_size > 1000:  # Large parameters
            batch_size = min(batch_size, 100)
        
        start_time = time.time()
        
        current_session: AsyncSession
        try:
            if session:
                current_session = session
                # Caller manages the transaction for a provided session
                # session.commit() should not be called here.
                for i in range(0, total_items, batch_size):
                    batch = params_list[i:i + batch_size]
                    batch_start_time_loop = time.time()
                    await current_session.execute(text(query), batch)
                    # Log progress and timing (similar to the 'else' block)
                    batch_duration = time.time() - batch_start_time_loop
                    if batch_duration > 5:
                        logger.warning(f"Slow batch (provided session) detected: {batch_duration:.2f}s (Items {i}-{i+len(batch)})")
                    if i > 0 and i % (batch_size * 10) == 0:
                        progress = (i / total_items) * 100
                        elapsed = time.time() - start_time
                        rate = i / elapsed if elapsed > 0 else 0
                        logger.info(f"Batch progress (provided session): {progress:.1f}% ({i}/{total_items}) Rate: {rate:.1f} items/s")
                        await self._monitor_resources()
            else:
                # Create new transaction if not in one
                async with self.session() as new_session:
                    current_session = new_session
                    async with current_session.begin(): # Handles commit/rollback
                        for i in range(0, total_items, batch_size):
                            batch = params_list[i:i + batch_size]
                            batch_start_time_loop = time.time()
                            
                            await current_session.execute(text(query), batch)
                            # No explicit commit here, session.begin() handles it.
                            
                            batch_duration = time.time() - batch_start_time_loop
                            if batch_duration > 5:
                                logger.warning(f"Slow batch (new session) detected: {batch_duration:.2f}s (Items {i}-{i+len(batch)})")
                            if i > 0 and i % (batch_size * 10) == 0:
                                progress = (i / total_items) * 100
                                elapsed = time.time() - start_time
                                rate = i / elapsed if elapsed > 0 else 0
                                logger.info(f"Batch progress (new session): {progress:.1f}% ({i}/{total_items}) Rate: {rate:.1f} items/s")
                                await self._monitor_resources()
            
            total_duration = time.time() - start_time
            logger.info(
                f"Batch operation completed: {total_items} items "
                f"in {total_duration:.2f}s "
                f"({total_items/total_duration if total_duration > 0 else 0:.1f} items/s)"
            )

        except SQLAlchemyError as e:
            # Determine current item index 'i' if possible for logging
            current_item_index_str = ""
            if 'i' in locals():
                 current_item_index_str = f"at item {locals()['i']}"
            logger.error(
                f"Batch operation failed {current_item_index_str}: {str(e)}\\n"
                f"Query: {query}"
            )
            raise DatabaseError(f"Error executing batch query: {str(e)}")
        except Exception as e: # Catch other potential errors
            current_item_index_str = ""
            if 'i' in locals():
                 current_item_index_str = f"at item {locals()['i']}"
            logger.error(f"Unexpected error in execute_many {current_item_index_str}: {e}", exc_info=True)
            raise DatabaseError(f"Unexpected error in execute_many: {str(e)}")

    async def execute_with_retry(
        self, 
        query: str, 
        params: Optional[Dict] = None, 
        max_retries: Optional[int] = None,
        initial_delay: float = 0.1
    ) -> Any:
        """
        Execute a query with retry logic for transient failures.
        
        Args:
            query (str): SQL query to execute
            params (Optional[Dict]): Query parameters
            max_retries (Optional[int]): Maximum number of retry attempts
            initial_delay (float): Initial delay between retries (seconds)
            
        Returns:
            Any: Query result
        """
        max_retries = max_retries or self.MAX_RETRIES
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return await self.execute(query, params)
            except DatabaseError as e:
                last_error = e
                if attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(
                        f"Retry attempt {attempt + 1}/{max_retries} "
                        f"after {delay:.1f}s delay"
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error(
                    f"All retry attempts failed for query: {query}\n"
                    f"Final error: {str(last_error)}"
                )
                raise last_error

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name (str): Name of the table to check
            
        Returns:
            bool: True if table exists, False otherwise
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = :table_name
            )
        """
        result = await self.fetch_one(query, {"table_name": table_name})
        return result["exists"] if result else False

    async def cleanup_stale_operations(
        self, 
        table_name: str, 
        status_column: str = 'status',
        timeout_minutes: int = 60
    ) -> None:
        """
        Clean up stale operations in a table.
        
        Args:
            table_name (str): Name of the table to clean
            status_column (str): Name of the status column
            timeout_minutes (int): Minutes after which to consider an operation stale
        """
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
        await self.execute(query, params)

    async def reset_pool(self) -> None:
        """Reset the connection pool and recreate engine."""
        try:
            logger.info("Starting database pool reset")
            
            # First close all active sessions
            async with self._session_lock:
                active_session_count = len(self._active_sessions)
                if active_session_count > 0:
                    logger.warning(f"Closing {active_session_count} active sessions")
                    
                for session in self._active_sessions.copy():
                    try:
                        await session.close()
                    except Exception as e:
                        logger.error(f"Error closing session during pool reset: {e}")
                self._active_sessions.clear()
                self._active_operations = 0

            # Dispose of the engine if it exists
            if self._engine:
                try:
                    logger.info("Disposing of existing engine")
                    await self._engine.dispose()
                except Exception as e:
                    logger.error(f"Error disposing engine: {e}")
            
            # Reset state
            self._engine = None
            self._engine_initialized = False
            self._session_factory = None
            
            # Reinitialize the engine with fresh settings
            logger.info("Reinitializing database engine")
            await self._initialize_engine()
            self._engine_initialized = True
            
            # Verify the new pool
            logger.info("Verifying new connection pool")
            if not await self._ensure_pool():
                raise DatabaseError("Failed to verify new connection pool")
            
            # Reset monitoring stats
            self._last_pool_check = time.time()
            self._pool_health_status = True
            self._circuit_breaker['failures'] = 0
            self._circuit_breaker['status'] = 'closed'
            
            logger.info("Database pool reset completed successfully")
                
        except Exception as e:
            logger.error(f"Error resetting connection pool: {e}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            # Ensure we're in a known bad state
            self._pool_health_status = False
            self._engine_initialized = False
            raise DatabaseError(f"Failed to reset connection pool: {str(e)}")

    async def close(self) -> None:
        """Close all database connections and cleanup resources."""
        try:
            async with self._session_lock:
                for session in self._active_sessions.copy():
                    try:
                        await session.close()
                    except Exception as e:
                        logger.error(f"Error closing session during cleanup: {e}")
                self._active_sessions.clear()
                self._active_operations = 0

            if self._engine:
                await self._engine.dispose()
                
        except Exception as e:
            logger.error(f"Error during database cleanup: {e}")
            raise DatabaseError(f"Failed to cleanup database resources: {str(e)}")

    @staticmethod
    def with_transaction():
        """
        Decorator that wraps a function in a session with an active transaction.
        The decorated function must accept a 'session: AsyncSession' parameter as its first argument after 'self'.
        
        Returns:
            Callable: Decorated function with session and transaction handling
        """
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @wraps(func)
            async def wrapper(self: 'BaseDatabaseManager', *args, **kwargs) -> T:
                # Note: The decorated function 'func' is expected to have 'session' as its first arg after self.
                # Example: async def some_method(self, session: AsyncSession, other_arg: str)
                async with self.session() as session: # Get a session from the pool
                    async with session.begin(): # Start a transaction
                        # Pass the session with an active transaction to the wrapped function
                        return await func(self, session, *args, **kwargs)
            return wrapper
        return decorator

    async def initialize_database(self) -> None:
        """Initialize database tables and add any missing columns."""
        try:
            # Base initialization code here - no validator specific code
            logger.info("Base database initialization completed")
            
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            logger.error(traceback.format_exc())
            raise DatabaseError(f"Failed to initialize database: {str(e)}")
