import asyncio
import asyncpg
from abc import ABC, abstractmethod
from typing import Optional, Any, List, Dict
from contextlib import asynccontextmanager
from functools import wraps

class BaseDatabaseManager(ABC):
    """
    Abstract base class for PostgreSQL database management with connection pooling and async support.
    Implements singleton pattern to ensure only one instance exists per node type.
    
    Attributes:
        _instances: Dictionary of singleton instances per node type
        _lock: Asyncio lock for thread-safe operations
        _pool: Connection pool for database connections
    """
    _instances = {}
    _lock = asyncio.Lock()
    _pool: Optional[asyncpg.Pool] = None
    
    def __new__(cls, node_type: str, *args, **kwargs):
        """
        Singleton pattern implementation per node type.
        
        Args:
            node_type (str): Type of node ('validator' or 'miner')
            
        Returns:
            BaseDatabaseManager: The single instance for the specified node type
        """
        if node_type not in cls._instances:
            cls._instances[node_type] = super().__new__(cls)
        return cls._instances[node_type]

    def __init__(self, node_type: str, host: str = 'localhost', 
                 port: int = 5432, user: str = 'postgres', 
                 password: str = 'postgres', min_size: int = 5, 
                 max_size: int = 20):
        """
        Initialize the database manager with connection parameters.
        Only executes once per node type due to singleton pattern.
        
        Args:
            node_type (str): Type of node ('validator' or 'miner')
            host (str, optional): Database host. Defaults to 'localhost'
            port (int, optional): Database port. Defaults to 5432
            user (str, optional): Database user. Defaults to 'postgres'
            password (str, optional): Database password. Defaults to 'postgres'
            min_size (int, optional): Minimum pool size. Defaults to 5
            max_size (int, optional): Maximum pool size. Defaults to 20
        """
        if not hasattr(self, 'initialized'):
            self.node_type = node_type
            self.db_config = {
                'database': f"{node_type}_db",  # Database name based on node type
                'user': user,
                'password': password,
                'host': host,
                'port': port,
                'min_size': min_size,
                'max_size': max_size
            }
            self.initialized = True

    @asynccontextmanager
    async def get_connection(self):
        """
        Async context manager for getting a database connection from the pool.
        Creates the pool if it doesn't exist.
        
        Yields:
            asyncpg.Connection: Database connection from the pool
            
        Usage:
            async with db.get_connection() as conn:
                await conn.execute(query)
        """
        if not self._pool:
            self._pool = await asyncpg.create_pool(**self.db_config)
        async with self._pool.acquire() as connection:
            yield connection

    def with_connection(func):
        """
        Decorator that provides a database connection to the wrapped function.
        Automatically manages connection lifecycle.
        
        Args:
            func: Async function that needs a database connection
            
        Returns:
            wrapper: Decorated function
            
        Usage:
            @with_connection
            async def my_query(self, conn, *args, **kwargs):
                await conn.execute(query)
        """
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            async with self.get_connection() as conn:
                return await func(self, conn, *args, **kwargs)
        return wrapper

    def with_transaction(func):
        """
        Decorator that wraps the function in a database transaction.
        Automatically rolls back on error.
        
        Args:
            func: Async function that needs transaction management
            
        Returns:
            wrapper: Decorated function
            
        Usage:
            @with_transaction
            async def my_transaction(self, conn, *args, **kwargs):
                await conn.execute(query1)
                await conn.execute(query2)
        """
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            async with self.get_connection() as conn:
                async with conn.transaction():
                    return await func(self, conn, *args, **kwargs)
        return wrapper

    @with_connection
    async def execute(self, conn, query: str, *args) -> str:
        """
        Execute a single SQL query.
        
        Args:
            conn: Database connection (provided by decorator)
            query (str): SQL query to execute
            *args: Query parameters
            
        Returns:
            str: Query status
        """
        return await conn.execute(query, *args)

    @with_transaction
    async def execute_many(self, conn, query: str, data: List[Dict[str, Any]]) -> None:
        """
        Execute the same query with multiple sets of parameters.
        
        Args:
            conn: Database connection (provided by decorator)
            query (str): SQL query to execute
            data (List[Dict[str, Any]]): List of parameter dictionaries
        """
        await conn.executemany(query, data)

    @with_connection
    async def fetch_one(self, conn, query: str, *args) -> Optional[asyncpg.Record]:
        """
        Fetch a single row from the database.
        
        Args:
            conn: Database connection (provided by decorator)
            query (str): SQL query to execute
            *args: Query parameters
            
        Returns:
            Optional[asyncpg.Record]: Single record or None if not found
        """
        return await conn.fetchrow(query, *args)

    @with_connection
    async def fetch_many(self, conn, query: str, *args) -> List[asyncpg.Record]:
        """
        Fetch multiple rows from the database.
        
        Args:
            conn: Database connection (provided by decorator)
            query (str): SQL query to execute
            *args: Query parameters
            
        Returns:
            List[asyncpg.Record]: List of records
        """
        return await conn.fetch(query, *args)

    async def acquire_lock(self, lock_name: str, timeout: float = 10.0) -> bool:
        """
        Acquire a named PostgreSQL advisory lock.
        
        Args:
            lock_name (str): Unique name for the lock
            timeout (float, optional): Maximum time to wait for lock. Defaults to 10.0
            
        Returns:
            bool: True if lock was acquired, False if timeout
        """
        async with self.get_connection() as conn:
            result = await conn.fetchval(
                "SELECT pg_try_advisory_lock(hashtext($1))", lock_name
            )
            return result

    async def release_lock(self, lock_name: str) -> None:
        """
        Release a previously acquired advisory lock.
        
        Args:
            lock_name (str): Name of the lock to release
        """
        async with self.get_connection() as conn:
            await conn.execute(
                "SELECT pg_advisory_unlock(hashtext($1))", lock_name
            )

    @abstractmethod
    async def initialize_database(self):
        """
        Abstract method to initialize the database schema.
        Must be implemented by concrete classes.
        """
        pass

    async def close(self):
        """
        Close the database connection pool.
        Should be called when shutting down the application.
        """
        if self._pool:
            await self._pool.close()

    @with_connection
    async def table_exists(self, conn, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            conn: Database connection (provided by decorator)
            table_name (str): Name of the table to check
            
        Returns:
            bool: True if table exists, False otherwise
        """
        result = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = $1
            )
        """, table_name)
        return result

    @with_connection
    async def create_index(self, conn, table_name: str, column_name: str, 
                          index_name: str = None, unique: bool = False):
        """
        Create an index on a table column.
        
        Args:
            conn: Database connection (provided by decorator)
            table_name (str): Name of the table
            column_name (str): Name of the column to index
            index_name (str, optional): Custom index name. Defaults to None
            unique (bool, optional): Whether the index should be unique. Defaults to False
        """
        if not index_name:
            index_name = f"{table_name}_{column_name}_idx"
        unique_str = "UNIQUE" if unique else ""
        await conn.execute(f"""
            CREATE {unique_str} INDEX IF NOT EXISTS {index_name} 
            ON {table_name} ({column_name})
        """)

    async def store_task_data(self, table_name: str, data: Dict[str, Any]) -> int:
        """Store task-specific data in database."""
        columns = ", ".join(data.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(data)))
        
        async with self.get_connection() as conn:
            return await conn.fetchval(f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
                RETURNING id
            """, *data.values())
    
    async def get_task_data(self, table_name: str, conditions: Dict[str, Any]) -> List[Dict]:
        """Retrieve task-specific data from database."""
        where_clause = " AND ".join(f"{k} = ${i+1}" for i, k in enumerate(conditions))
        
        async with self.get_connection() as conn:
            rows = await conn.fetch(f"""
                SELECT *
                FROM {table_name}
                WHERE {where_clause}
            """, *conditions.values())
            
        return [dict(row) for row in rows]

    @abstractmethod
    async def initialize_task_tables(self, task_schemas: Dict[str, Dict[str, Any]]):
        """Initialize task-specific tables based on schemas."""
        pass

    

    
