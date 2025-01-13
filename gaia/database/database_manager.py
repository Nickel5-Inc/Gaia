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

    # Default timeouts
    DEFAULT_QUERY_TIMEOUT = 30  # 30 seconds
    DEFAULT_TRANSACTION_TIMEOUT = 180  # 3 minutes
    DEFAULT_CONNECTION_TIMEOUT = 10  # 10 seconds

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

            # PostgreSQL async URL
            self.db_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
            self.initialized = True

            # Create engine with connection pooling and timeouts
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

            # Connection health tracking
            self.last_successful_connection = 0
            self.consecutive_failures = 0
            self.MAX_CONSECUTIVE_FAILURES = 3

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

    async def get_connection(self):
        """Get a database session with retry logic and timeouts."""
        max_retries = 3
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                session = AsyncSession(self._engine)
                await asyncio.wait_for(
                    session.execute(text("SELECT 1")),
                    timeout=30
                )
                return session
                    
            except asyncio.TimeoutError:
                logger.error(f"Connection attempt {attempt + 1} timed out")
                if attempt == max_retries - 1:
                    raise
                
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to get connection after {max_retries} attempts: {e}")
                    raise
                
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
            
            delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
            logger.info(f"Retrying connection in {delay:.1f} seconds")
            await asyncio.sleep(delay)
            
            await self.reset_pool()

    @asynccontextmanager
    async def get_session(self):
        """Get a database session with timeout handling."""
        try:
            if not self._engine:
                await self._ensure_pool()

            session = self._session_factory()
            try:
                # Use wait_for instead of timeout for Python 3.10 compatibility
                await asyncio.wait_for(
                    self._test_connection(session), 
                    timeout=self.DEFAULT_CONNECTION_TIMEOUT
                )
                yield session
            except asyncio.TimeoutError:
                logger.error("Database connection timeout")
                await self.reset_pool()
                raise
            except Exception as e:
                logger.error(f"Error getting database session: {e}")
                raise
            finally:
                await session.close()
        except Exception as e:
            logger.error(f"Error in get_session: {e}")
            raise

    async def _test_connection(self, session):
        """Test the database connection."""
        try:
            await session.execute(text("SELECT 1"))
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            raise

    async def execute(self, query, params=None):
        """Execute a database query with timeout handling."""
        try:
            async with self.get_session() as session:
                try:
                    # Use wait_for instead of timeout
                    result = await asyncio.wait_for(
                        session.execute(text(query), params),
                        timeout=self.DEFAULT_QUERY_TIMEOUT
                    )
                    await session.commit()
                    return result
                except asyncio.TimeoutError:
                    logger.error("Query execution timeout")
                    await session.rollback()
                    raise
                except Exception as e:
                    logger.error(f"Query execution error: {e}")
                    await session.rollback()
                    raise
        except Exception as e:
            logger.error(f"Error in execute: {e}")
            raise

    async def fetch_one(self, query, params=None):
        """Fetch a single row with timeout handling."""
        try:
            async with self.get_session() as session:
                try:
                    # Use wait_for instead of timeout
                    result = await asyncio.wait_for(
                        session.execute(text(query), params),
                        timeout=self.DEFAULT_QUERY_TIMEOUT
                    )
                    row = result.first()
                    return dict(row) if row else None
                except asyncio.TimeoutError:
                    logger.error("Query fetch timeout")
                    raise
                except Exception as e:
                    logger.error(f"Query fetch error: {e}")
                    raise
        except Exception as e:
            logger.error(f"Error in fetch_one: {e}")
            raise

    async def fetch_many(self, query, params=None):
        """Fetch multiple rows with timeout handling."""
        try:
            async with self.get_session() as session:
                try:
                    # Use wait_for instead of timeout
                    result = await asyncio.wait_for(
                        session.execute(text(query), params),
                        timeout=self.DEFAULT_QUERY_TIMEOUT
                    )
                    rows = result.fetchall()
                    return [dict(row) for row in rows]
                except asyncio.TimeoutError:
                    logger.error("Query fetch timeout")
                    raise
                except Exception as e:
                    logger.error(f"Query fetch error: {e}")
                    raise
        except Exception as e:
            logger.error(f"Error in fetch_many: {e}")
            raise

    async def reset_connection_pool(self):
        """Reset the database connection pool."""
        try:
            logger.info("Resetting database connection pool")
            if self._engine:
                await self._engine.dispose()
            
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
            
            # Test the new connection
            async with self.get_session() as session:
                await session.execute(text("SELECT 1"))
            
            logger.info("Successfully reset database connection pool")
            self.consecutive_failures = 0
            return True
        except Exception as e:
            logger.error(f"Failed to reset connection pool: {e}")
            return False

    def with_timeout(timeout: Optional[float] = None):
        """Decorator that adds timeout to database operations."""
        def decorator(func):
            @wraps(func)
            async def wrapper(self, *args, **kwargs):
                try:
                    return await asyncio.wait_for(
                        func(self, *args, **kwargs),
                        timeout=timeout or self.DEFAULT_QUERY_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    logger.error(f"Database operation timed out: {func.__name__}")
                    raise DatabaseTimeout(f"Operation {func.__name__} timed out")
            return wrapper
        return decorator

    async def acquire_lock(self, lock_id: int) -> bool:
        """
        Acquire a PostgreSQL advisory lock.
        
        Args:
            lock_id (int): The ID of the lock to acquire
            
        Returns:
            bool: True if lock was acquired, False otherwise
        """
        async with self.get_session() as session:
            result = await session.execute(
                text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": lock_id}
            )
            return (await result.scalar()) is True

    async def release_lock(self, lock_id: int) -> bool:
        """
        Release a PostgreSQL advisory lock.
        
        Args:
            lock_id (int): The ID of the lock to release
            
        Returns:
            bool: True if lock was released, False otherwise
        """
        async with self.get_session() as session:
            result = await session.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": lock_id}
            )
            return (await result.scalar()) is True

    def with_session(func):
        """Decorator that provides a database session to the wrapped function."""
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            async with self.get_session() as session:
                return await func(self, session, *args, **kwargs)
        return wrapper

    def with_transaction(func):
        """Decorator that wraps the function in a database transaction."""
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            async with self.get_session() as session:
                try:
                    # Start transaction if one isn't already active
                    if not session.in_transaction():
                        async with session.begin():
                            return await func(self, session, *args, **kwargs)
                    else:
                        # If transaction is already active, just execute the function
                        return await func(self, session, *args, **kwargs)
                except Exception as e:
                    logger.error(f"Transaction error in {func.__name__}: {str(e)}")
                    if session.in_transaction():
                        await session.rollback()
                    raise
        return wrapper

    @with_session
    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def execute(self, session, query: str, params: dict = None) -> Any:
        """
        Execute a single SQL query.
        
        Args:
            session: The database session
            query (str): The SQL query to execute
            params (dict, optional): Query parameters
            
        Returns:
            Any: Query result
        """
        try:
            if not session.in_transaction():
                async with session.begin():
                    result = await session.execute(text(query), params or {})
            else:
                result = await session.execute(text(query), params or {})
            return result
        except SQLAlchemyError as e:
            logger.error(f"Database error in execute: {str(e)}")
            if session.in_transaction():
                await session.rollback()
            raise

    @with_transaction
    @with_timeout(DEFAULT_TRANSACTION_TIMEOUT)
    async def execute_many(self, session, query: str, data: List[Dict[str, Any]]) -> None:
        """
        Execute the same query with multiple sets of parameters.
        
        Args:
            session: The database session
            query (str): The SQL query to execute
            data (List[Dict[str, Any]]): List of parameter sets
        """
        try:
            await session.execute(text(query), data)
        except SQLAlchemyError as e:
            logger.error(f"Database error in execute_many: {str(e)}")
            raise

    @with_session
    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_one(self, session, query: str, params: dict = None) -> Optional[Dict]:
        """
        Fetch a single row from the database.
        
        Args:
            session: The database session
            query (str): The SQL query to execute
            params (dict, optional): Query parameters
            
        Returns:
            Optional[Dict]: Single row result or None
        """
        try:
            result = await session.execute(text(query), params or {})
            row = result.first()
            return dict(row._mapping) if row else None
        except SQLAlchemyError as e:
            logger.error(f"Database error in fetch_one: {str(e)}")
            raise

    @with_session
    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def fetch_many(self, session, query: str, params: dict = None) -> List[Dict]:
        """
        Fetch multiple rows from the database.
        
        Args:
            session: The database session
            query (str): The SQL query to execute
            params (dict, optional): Query parameters
            
        Returns:
            List[Dict]: List of row results
        """
        try:
            result = await session.execute(text(query), params or {})
            return [dict(row._mapping) for row in result]
        except SQLAlchemyError as e:
            logger.error(f"Database error in fetch_many: {str(e)}")
            raise

    async def close(self):
        """Close the database engine."""
        if self._engine:
            await self._engine.dispose()

    @with_session
    @with_timeout(DEFAULT_QUERY_TIMEOUT)
    async def table_exists(self, session, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            session: The database session
            table_name (str): Name of the table to check
            
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
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
        except SQLAlchemyError as e:
            logger.error(f"Database error in table_exists: {str(e)}")
            raise
