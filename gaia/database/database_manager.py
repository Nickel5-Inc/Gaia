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
    DEFAULT_QUERY_TIMEOUT = 30  # 30 seconds
    DEFAULT_TRANSACTION_TIMEOUT = 180  # 3 minutes
    DEFAULT_CONNECTION_TIMEOUT = 10  # 10 seconds

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
            self._pool_semaphore = asyncio.Semaphore(self.MAX_CONNECTIONS)
            
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
                pool_size=5,
                max_overflow=10,
                pool_timeout=self.DEFAULT_CONNECTION_TIMEOUT,
                pool_recycle=300,  # Recycle connections every 5 minutes
                pool_use_lifo=True,  # Use LIFO to better reuse connections
                echo=False,
                connect_args={
                    "command_timeout": self.DEFAULT_QUERY_TIMEOUT,
                    "timeout": self.DEFAULT_CONNECTION_TIMEOUT,
                },
            )

            # Test connection immediately
            async with self._engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

            # Initialize session factory
            self._session_factory = async_sessionmaker(
                self._engine, class_=AsyncSession, expire_on_commit=False
            )

            logger.info("Database engine initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize database engine: {e}")
            logger.error(traceback.format_exc())
            self._engine = None
            self._session_factory = None
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
        
        if not await self._check_circuit_breaker():
            raise CircuitBreakerError("Circuit breaker is open")
            
        if not await self._check_pool_health():
            raise DatabaseConnectionError("Database pool is unhealthy")

        session = None
        start_time = time.time()
        caller = traceback.extract_stack()[-3]  # Get calling frame
        session_id = None
        
        try:
            async with self._pool_semaphore:
                async with self._session_lock:
                    session = self._session_factory()
                    session_id = id(session)
                    # logger.debug(
                    #     f"Creating new session {session_id} from "
                    #     f"{caller.filename}:{caller.lineno} ({caller.name})"
                    # )
                    self._active_sessions.add(session)
                    self._active_operations += 1
                
                async with session.begin():
                    await self._test_connection(session)
                    yield session
                
                # Update circuit breaker on success
                await self._update_circuit_breaker(True)
                
        except Exception as e:
            await self._update_circuit_breaker(False)
            logger.error(
                f"Session error in {session_id} from "
                f"{caller.filename}:{caller.lineno}: {str(e)}"
            )
            raise DatabaseConnectionError(f"Session error: {str(e)}")
        
        finally:
            if session:
                try:
                    async with self._session_lock:
                        self._active_sessions.remove(session)
                        self._active_operations -= 1
                    await session.close()
                    # logger.debug(
                    #     f"Closed session {session_id} from "
                    #     f"{caller.filename}:{caller.lineno}"
                    # )
                    
                    # Log long-running sessions
                    duration = time.time() - start_time
                    if duration > self.DEFAULT_QUERY_TIMEOUT / 2:
                        logger.warning(
                            f"Long-running session {session_id} detected: "
                            f"{duration:.2f}s from {caller.filename}:{caller.lineno}"
                        )
                except Exception as e:
                    logger.error(
                        f"Error cleaning up session {session_id} from "
                        f"{caller.filename}:{caller.lineno}: {e}"
                    )

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def _test_connection(self, session: AsyncSession) -> bool:
        """Test database connection"""
        await session.execute(text("SELECT 1"))
        return True

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_one(
        self, 
        query: str, 
        params: Optional[Dict] = None,
        timeout: Optional[float] = None
    ) -> Optional[Dict]:
        """
        Fetch a single row from the database.
        If called within a transaction context, uses the existing transaction.
        Otherwise, creates a new transaction.
        
        Args:
            query (str): SQL query to execute
            params (Optional[Dict]): Query parameters
            timeout (Optional[float]): Operation timeout in seconds
            
        Returns:
            Optional[Dict]: Single row result or None
        """
        start_time = time.time()
        
        # Check if we're already in a transaction
        if hasattr(self, '_in_transaction') and self._in_transaction:
            # Get the current session from the active transaction
            session = next(iter(self._active_sessions))
            try:
                result = await session.execute(text(query), params or {})
                row = result.first()
                
                # Log slow queries
                duration = time.time() - start_time
                if duration > self.DEFAULT_QUERY_TIMEOUT / 2:
                    logger.warning(f"Slow query detected: {duration:.2f}s\nQuery: {query}")
                
                return dict(row._mapping) if row else None
            except SQLAlchemyError as e:
                logger.error(f"Database error in fetch_one: {str(e)}\nQuery: {query}")
                raise DatabaseError(f"Error executing query: {str(e)}")
        else:
            # Create new transaction if not in one
            async with self.session() as session:
                try:
                    result = await session.execute(text(query), params or {})
                    row = result.first()
                    
                    # Log slow queries
                    duration = time.time() - start_time
                    if duration > self.DEFAULT_QUERY_TIMEOUT / 2:
                        logger.warning(f"Slow query detected: {duration:.2f}s\nQuery: {query}")
                    
                    return dict(row._mapping) if row else None
                except SQLAlchemyError as e:
                    logger.error(f"Database error in fetch_one: {str(e)}\nQuery: {query}")
                    raise DatabaseError(f"Error executing query: {str(e)}")

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_all(
        self, 
        query: str, 
        params: Optional[Dict] = None,
        timeout: Optional[float] = None
    ) -> List[Dict]:
        """
        Fetch all rows from the database.
        If called within a transaction context, uses the existing transaction.
        Otherwise, creates a new transaction.
        
        Args:
            query (str): SQL query to execute
            params (Optional[Dict]): Query parameters
            timeout (Optional[float]): Operation timeout in seconds
            
        Returns:
            List[Dict]: List of result rows
        """
        start_time = time.time()
        
        # Check if we're already in a transaction
        if hasattr(self, '_in_transaction') and self._in_transaction:
            # Get the current session from the active transaction
            session = next(iter(self._active_sessions))
            try:
                result = await session.execute(text(query), params or {})
                rows = result.all()
                
                # Log large result sets
                if len(rows) > 1000:
                    logger.warning(
                        f"Large result set: {len(rows)} rows\n"
                        f"Query: {query}"
                    )
                
                # Log slow queries
                duration = time.time() - start_time
                if duration > self.DEFAULT_QUERY_TIMEOUT / 2:
                    logger.warning(f"Slow query detected: {duration:.2f}s\nQuery: {query}")
                
                return [dict(row._mapping) for row in rows]
            except SQLAlchemyError as e:
                logger.error(f"Database error in fetch_all: {str(e)}\nQuery: {query}")
                raise DatabaseError(f"Error executing query: {str(e)}")
        else:
            # Create new transaction if not in one
            async with self.session() as session:
                try:
                    result = await session.execute(text(query), params or {})
                    rows = result.all()
                    
                    # Log large result sets
                    if len(rows) > 1000:
                        logger.warning(
                            f"Large result set: {len(rows)} rows\n"
                            f"Query: {query}"
                        )
                    
                    # Log slow queries
                    duration = time.time() - start_time
                    if duration > self.DEFAULT_QUERY_TIMEOUT / 2:
                        logger.warning(f"Slow query detected: {duration:.2f}s\nQuery: {query}")
                    
                    return [dict(row._mapping) for row in rows]
                except SQLAlchemyError as e:
                    logger.error(f"Database error in fetch_all: {str(e)}\nQuery: {query}")
                    raise DatabaseError(f"Error executing query: {str(e)}")

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
        batch_size: Optional[int] = None
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
        """
        if not params_list:
            return

        # Determine optimal batch size
        batch_size = batch_size or self.DEFAULT_BATCH_SIZE
        total_items = len(params_list)
        
        # Adaptive batch sizing based on data size
        avg_param_size = sum(len(str(p)) for p in params_list[:100]) / min(100, total_items)
        if avg_param_size > 1000:  # Large parameters
            batch_size = min(batch_size, 100)
        
        start_time = time.time()
        
        # Check if we're already in a transaction
        if hasattr(self, '_in_transaction') and self._in_transaction:
            # Get the current session from the active transaction
            session = next(iter(self._active_sessions))
            try:
                for i in range(0, total_items, batch_size):
                    batch = params_list[i:i + batch_size]
                    batch_start = time.time()
                    
                    # Execute batch
                    await session.execute(text(query), batch)
                    
                    # Log progress and timing
                    batch_duration = time.time() - batch_start
                    if batch_duration > 5:  # Log slow batches
                        logger.warning(
                            f"Slow batch detected: {batch_duration:.2f}s "
                            f"(Items {i}-{i+len(batch)})"
                        )
                    
                    # Log progress periodically
                    if i > 0 and i % (batch_size * 10) == 0:
                        progress = (i / total_items) * 100
                        elapsed = time.time() - start_time
                        rate = i / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"Batch progress: {progress:.1f}% "
                            f"({i}/{total_items}) "
                            f"Rate: {rate:.1f} items/s"
                        )
                        
                        # Monitor resources during long operations
                        await self._monitor_resources()
                
                # Log final statistics
                total_duration = time.time() - start_time
                logger.info(
                    f"Batch operation completed: {total_items} items "
                    f"in {total_duration:.2f}s "
                    f"({total_items/total_duration:.1f} items/s)"
                )
                
            except SQLAlchemyError as e:
                logger.error(
                    f"Batch operation failed at item {i}: {str(e)}\n"
                    f"Query: {query}"
                )
                raise DatabaseError(f"Error executing batch query: {str(e)}")
        else:
            # Create new transaction if not in one
            async with self.transaction() as session:
                try:
                    for i in range(0, total_items, batch_size):
                        batch = params_list[i:i + batch_size]
                        batch_start = time.time()
                        
                        # Execute batch
                        await session.execute(text(query), batch)
                        await session.commit()
                        
                        # Log progress and timing
                        batch_duration = time.time() - batch_start
                        if batch_duration > 5:  # Log slow batches
                            logger.warning(
                                f"Slow batch detected: {batch_duration:.2f}s "
                                f"(Items {i}-{i+len(batch)})"
                            )
                        
                        # Log progress periodically
                        if i > 0 and i % (batch_size * 10) == 0:
                            progress = (i / total_items) * 100
                            elapsed = time.time() - start_time
                            rate = i / elapsed if elapsed > 0 else 0
                            logger.info(
                                f"Batch progress: {progress:.1f}% "
                                f"({i}/{total_items}) "
                                f"Rate: {rate:.1f} items/s"
                            )
                            
                            # Monitor resources during long operations
                            await self._monitor_resources()
                    
                    # Log final statistics
                    total_duration = time.time() - start_time
                    logger.info(
                        f"Batch operation completed: {total_items} items "
                        f"in {total_duration:.2f}s "
                        f"({total_items/total_duration:.1f} items/s)"
                    )
                    
                except SQLAlchemyError as e:
                    logger.error(
                        f"Batch operation failed at item {i}: {str(e)}\n"
                        f"Query: {query}"
                    )
                    raise DatabaseError(f"Error executing batch query: {str(e)}")

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

    def with_transaction():
        """
        Decorator that wraps a function in a session.
        The decorated function must accept a session parameter.
        
        Returns:
            Callable: Decorated function with session handling
        """
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @wraps(func)
            async def wrapper(self, *args, **kwargs) -> T:
                async with self.session() as session:
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
