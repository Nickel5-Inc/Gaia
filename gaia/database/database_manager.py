import asyncio
from abc import ABC, abstractmethod
from typing import Optional, Any, List, Dict
from contextlib import asynccontextmanager
from functools import wraps
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from fiber.logging_utils import get_logger
import random
import time

logger = get_logger(__name__)

class DatabaseTimeout(Exception):
    """Raised when a database operation times out."""
    pass

class BaseDatabaseManager(ABC):
    """
    Abstract base class for PostgreSQL database management with SQLAlchemy async support.
    Implements singleton pattern to ensure only one instance exists per node type.
    """

    _instances = {}
    _lock = asyncio.Lock()
    _engine = None
    _session_factory = None

    DEFAULT_QUERY_TIMEOUT = 30  # 30 seconds
    DEFAULT_TRANSACTION_TIMEOUT = 30
    DEFAULT_CONNECTION_TIMEOUT = 10  # 10 seconds

    MAX_RETRIES = 3
    DEFAULT_BATCH_SIZE = 1000
    STATUS_PENDING = 'pending'
    STATUS_PROCESSING = 'processing'
    STATUS_COMPLETED = 'completed'
    STATUS_ERROR = 'error'
    STATUS_TIMEOUT = 'timeout'

    @staticmethod
    def with_timeout(timeout: float):
        """Decorator that adds timeout to a database operation"""
        def decorator(func):
            @wraps(func)
            async def wrapper(self, *args, **kwargs):
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

    def __new__(cls, node_type: str, *args, **kwargs):
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
        """
        Initialize the database manager.

        Args:
            node_type (str): Type of node ('validator' or 'miner')
            host (str): Database host
            port (int): Database port
            database (str): Database name
            user (str): Database user
            password (str): Database password
        """
        if not hasattr(self, "initialized"):
            self.node_type = node_type
            self.host = host
            self.port = port
            self.database = database
            self.user = user
            self.password = password

            self.db_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
            self.initialized = True

            self._engine = create_async_engine(
                self.db_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                echo=False,
                connect_args={
                    "command_timeout": self.DEFAULT_QUERY_TIMEOUT,
                    "timeout": self.DEFAULT_CONNECTION_TIMEOUT,
                }
            )

            self._session_factory = async_sessionmaker(
                self._engine, expire_on_commit=False, class_=AsyncSession
            )

            self.last_successful_connection = 0
            self.consecutive_failures = 0
            self.MAX_CONSECUTIVE_FAILURES = 3
            self._session_lock = asyncio.Lock()
            self._active_sessions = set()
            self._operation_timeouts = {
                'query': 30,
                'transaction': 180,
                'connection': 10
            }

            self._max_connections = 20
            self._pool_semaphore = asyncio.Semaphore(self._max_connections)
            self._active_operations = 0
            self._operation_lock = asyncio.Lock()
            self._transaction_lock = asyncio.Lock()

    def _get_or_create_loop(self):
        """Get the current event loop or create a new one for this thread."""
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            logger.warning("No running event loop found - this should not happen in production")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    async def _ensure_pool(self):
        """Ensure database pool exists and is healthy."""
        try:
            session = await self._engine.connect()
            try:
                await asyncio.wait_for(
                    session.execute(text("SELECT 1")),
                    timeout=10
                )
                logger.info("Database connection verified")
                return True
            finally:
                await session.close()
            
        except Exception as e:
            logger.error(f"Error ensuring pool: {e}")
            return False

    @asynccontextmanager
    async def session(self):
        """Internal session context manager. Not part of public API.
        Handles connection pooling, timeouts, and cleanup."""
        try:
            async with self._pool_semaphore:
                session = None
                try:
                    session = self._session_factory()
                    await asyncio.wait_for(
                        self._test_connection(session),
                        timeout=self._operation_timeouts['connection']
                    )
                    yield session
                except asyncio.TimeoutError:
                    logger.error("Session creation timed out")
                    raise DatabaseTimeout("Session creation timed out")
                finally:
                    if session:
                        await session.close()
        except asyncio.TimeoutError:
            logger.error("Connection pool exhausted")
            raise DatabaseTimeout("No connections available in pool")

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_one(self, query: str, params: Optional[Dict] = None):
        """Fetch single row with timeout"""
        async with self.session() as session:
            result = await session.execute(text(query), params)
            row = result.first()
            return dict(row._mapping) if row else None

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_many(self, query: str, params: Optional[Dict] = None):
        """Fetch multiple rows with timeout"""
        async with self.session() as session:
            result = await session.execute(text(query), params)
            return [dict(row._mapping) for row in result]

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_all(self, query: str, params: Optional[Dict] = None):
        """Fetch all rows at once. Use when result set size is known and manageable."""
        async with self.session() as session:
            result = await session.execute(text(query), params)
            return [dict(row._mapping) for row in result.all()]

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def execute(self, query: str, params: Optional[Dict] = None) -> Any:
        """Execute a write operation"""
        async with self.session() as session:
            result = await session.execute(text(query), params or {})
            await session.commit()
            return result

    @with_timeout(DEFAULT_TRANSACTION_TIMEOUT)
    async def execute_many(self, query: str, data: List[Dict[str, Any]]) -> None:
        """Execute multiple write operations in a transaction"""
        async with self.session() as session:
            async with session.begin():
                try:
                    await session.execute(text(query), data)
                except SQLAlchemyError as e:
                    logger.error(f"Database error in execute_many: {str(e)}")
                    raise

    @with_timeout(DEFAULT_TRANSACTION_TIMEOUT)
    async def execute_all(self, queries: List[str], params: Optional[List[Dict]] = None) -> None:
        """Execute multiple different queries in a single transaction."""
        async with self.session() as session:
            async with session.begin():
                try:
                    for i, query in enumerate(queries):
                        query_params = params[i] if params and i < len(params) else None
                        await session.execute(text(query), query_params or {})
                except SQLAlchemyError as e:
                    logger.error(f"Database error in execute_all: {e}")
                    raise

    @with_timeout(DEFAULT_CONNECTION_TIMEOUT)
    async def reset_pool(self):
        """Reset connection pool with timeout"""
        if self._engine:
            await self._engine.dispose()
        await self._initialize_engine()

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def acquire_lock(self, lock_id: int) -> bool:
        """Acquire advisory lock with timeout"""
        async with self.transaction() as session:
            result = await session.execute(
                text("SELECT pg_try_advisory_lock(:lock_id)"),
                {"lock_id": lock_id}
            )
            return (await result.scalar()) is True

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def release_lock(self, lock_id: int) -> bool:
        """Release advisory lock with timeout (WRITE)"""
        async with self.transaction() as session:
            result = await session.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"),
                {"lock_id": lock_id}
            )
            return (await result.scalar()) is True

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def _test_connection(self, session):
        """Test database connection"""
        await session.execute(text("SELECT 1"))

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        async with self.session() as session:
            result = await session.execute(
                text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = :table_name
                    )
                """),
                {"table_name": table_name},
            )
            return (await result.scalar()) is True

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def _monitor_health(self):
        """Monitor database health with timeout"""
        while True:
            try:
                async with self._operation_lock:
                    if self._active_operations > self._max_connections * 0.8:
                        logger.warning(f"High connection usage: {self._active_operations}/{self._max_connections}")
                
                current_time = time.time()
                async with self._session_lock:
                    for session in self._active_sessions:
                        if hasattr(session, 'start_time') and \
                           current_time - session.start_time > self._operation_timeouts['query']:
                            logger.warning(f"Long-running session detected: {current_time - session.start_time}s")
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(30)

    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def execute_with_retry(self, query: str, params: Optional[Dict] = None, max_retries: int = 3):
        """Execute query with retry for transient failures"""
        for attempt in range(max_retries):
            try:
                return await self.execute(query, params)
            except SQLAlchemyError as e:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)

    async def _acquire_transaction_lock(self):
        """Acquire transaction lock to prevent deadlocks"""
        await self._transaction_lock.acquire()

    async def _release_transaction_lock(self):
        """Release transaction lock"""
        self._transaction_lock.release()
