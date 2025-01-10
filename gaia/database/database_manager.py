import asyncio
from abc import ABC, abstractmethod
from typing import (
    Optional, Any, List, Dict, TypeVar, Callable, Coroutine,
    AsyncGenerator, AsyncContextManager, cast
)
from contextlib import asynccontextmanager
from functools import wraps
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.result import ScalarResult

T = TypeVar('T')

class BaseDatabaseManager(ABC):
    """
    Abstract base class for PostgreSQL database management with SQLAlchemy async support.
    Implements singleton pattern to ensure only one instance exists per node type.
    """

    _instances = {}
    _lock = asyncio.Lock()
    _engine = None
    _session_factory = None

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

            # PostgreSQL async URL
            self.db_url = (
                f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
            )
            self.initialized = True

            # Create engine with connection pooling
            self._engine = create_async_engine(
                self.db_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                echo=False,  # Set to True for SQL query logging
            )

            # Initialize session factory
            self._session_factory = async_sessionmaker(
                bind=self._engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Async context manager for getting a database session.
        
        Yields:
            AsyncSession: Database session
        """
        if not self._session_factory:
            raise RuntimeError("Session factory not initialized")
            
        async with self._session_factory() as session:
            try:
                yield session
            except Exception as e:
                await session.rollback()
                raise e

    async def acquire_lock(self, lock_id: int) -> bool:
        """Acquire a PostgreSQL advisory lock."""
        async with self.get_session() as session:
            result = await session.execute(
                text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": lock_id}
            )
            row = result.first()
            return bool(row[0]) if row else False

    async def release_lock(self, lock_id: int) -> bool:
        """Release a PostgreSQL advisory lock."""
        async with self.get_session() as session:
            result = await session.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": lock_id}
            )
            row = result.first()
            return bool(row[0]) if row else False

    @staticmethod
    def with_session(func: Callable[..., Coroutine[Any, Any, T]]) -> Callable[..., Coroutine[Any, Any, T]]:
        """Decorator that provides a database session to the wrapped function."""

        @wraps(func)
        async def wrapper(self: 'BaseDatabaseManager', *args, **kwargs) -> T:
            async with self.get_session() as session:
                return await func(self, session, *args, **kwargs)

        return wrapper

    @staticmethod
    def with_transaction(func: Callable[..., Coroutine[Any, Any, T]]) -> Callable[..., Coroutine[Any, Any, T]]:
        """Decorator that wraps the function in a database transaction."""

        @wraps(func)
        async def wrapper(self: 'BaseDatabaseManager', *args, **kwargs) -> T:
            async with self.get_session() as session:
                async with session.begin():
                    return await func(self, session, *args, **kwargs)

        return wrapper

    @with_session
    async def execute(self, session: AsyncSession, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a single SQL query."""
        result = await session.execute(text(query), params or {})
        await session.commit()
        return result

    @with_transaction
    async def execute_many(
        self, session: AsyncSession, query: str, data: List[Dict[str, Any]]
    ) -> None:
        """Execute the same query with multiple sets of parameters."""
        await session.execute(text(query), data)

    @with_session
    async def fetch_one(
        self, session: AsyncSession, query: str, params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single row from the database."""
        result = await session.execute(text(query), params or {})
        row = result.first()
        return dict(row._mapping) if row else None

    @with_session
    async def fetch_many(
        self, session: AsyncSession, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch multiple rows from the database."""
        result = await session.execute(text(query), params or {})
        return [dict(row._mapping) for row in result]

    async def close(self) -> None:
        """Close the database engine."""
        if self._engine:
            await self._engine.dispose()

    @with_session
    async def table_exists(self, session: AsyncSession, table_name: str) -> bool:
        """Check if a table exists in the database."""
        result = await session.execute(
            text(
                """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = :table_name
            )
        """
            ),
            {"table_name": table_name},
        )
        row = result.first()
        return bool(row[0]) if row else False
