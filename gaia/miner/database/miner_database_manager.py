from gaia.database.database_manager import BaseDatabaseManager, DatabaseError
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy import text
import json
from pathlib import Path
from fiber.logging_utils import get_logger
from functools import wraps
import time
import os # Ensure os is imported for getenv

logger = get_logger(__name__)

def track_operation(operation_type: str):
    """Decorator to track database operations."""
    def decorator(func):
        @wraps(func)
        async def wrapper(self: 'MinerDatabaseManager', *args, **kwargs):
            try:
                start_time = time.time()
                result = await func(self, *args, **kwargs)
                duration = time.time() - start_time
                
                # Track operation
                self._operation_stats[f'{operation_type}_operations'] += 1
                
                # Track long-running queries
                if duration > self.MINER_QUERY_TIMEOUT / 2:
                    self._operation_stats['long_running_queries'].append({
                        'operation': func.__name__,
                        'duration': duration,
                        'timestamp': start_time
                    })
                    logger.warning(f"Long-running {operation_type} operation detected: {duration:.2f}s")
                
                return result
            except Exception as e:
                logger.error(f"Error in {operation_type} operation: {str(e)}")
                raise
        return wrapper
    return decorator

class MinerDatabaseManager(BaseDatabaseManager):
    """
    Database manager specifically for miner nodes.
    Handles all miner-specific database operations.
    Implements singleton pattern to ensure only one database connection pool exists.
    """

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls, node_type="miner")
        return cls._instance

    def __init__(
        self,
        database: str = "miner_db",
        host: str = "localhost", # Default for TCP
        port: int = 5432,      # Default for TCP
        user: str = "postgres",
        password: str = "postgres",
        # No default for socket_path, should come from env if socket is used
    ):
        """Initialize the miner database manager (only once)."""
        if not hasattr(self, '_initialized'):
            # Determine connection type: check specific MINER_DB_CONNECTION_TYPE first, then global DB_CONNECTION_TYPE
            connection_type = os.getenv("MINER_DB_CONNECTION_TYPE", os.getenv("DB_CONNECTION_TYPE", "tcp")).lower()

            db_name_resolved = os.getenv("MINER_DB_NAME", database)
            db_user_resolved = os.getenv("MINER_DB_USER", user)
            db_password_resolved = os.getenv("MINER_DB_PASSWORD", password)
            
            db_host_resolved: Optional[str]
            db_port_resolved: Optional[int]

            if connection_type == "socket":
                # For socket connections, host is the socket directory path, port is typically None or ignored.
                # MINER_DB_HOST can be repurposed as the socket directory path for socket connections.
                db_host_resolved = os.getenv("MINER_DB_SOCKET_PATH", os.getenv("MINER_DB_HOST", "/var/run/postgresql")) # Default socket path
                db_port_resolved = None # Port is not used for socket connections with SQLAlchemy like this
                logger.info(f"MinerDatabaseManager configured for UNIX socket connection.")
                logger.info(f"MinerDatabaseManager connecting with: socket_path='{db_host_resolved}', db='{db_name_resolved}', user='{db_user_resolved}'")
            else: # Default to TCP
                db_host_resolved = os.getenv("MINER_DB_HOST", host)
                db_port_resolved = int(os.getenv("MINER_DB_PORT", str(port)))
                logger.info(f"MinerDatabaseManager configured for TCP/IP connection.")
                logger.info(f"MinerDatabaseManager connecting with: host='{db_host_resolved}', port={db_port_resolved}, db='{db_name_resolved}', user='{db_user_resolved}'")

            if db_password_resolved and db_password_resolved != password:
                logger.info("MinerDatabaseManager using a custom password from environment.")
            elif not db_password_resolved and password:
                logger.info("MinerDatabaseManager using default password as MINER_DB_PASSWORD env var is not set.")

            super().__init__(
                node_type="miner",
                database=db_name_resolved,
                host=db_host_resolved, # This will be socket path if connection_type is socket
                port=db_port_resolved, # This will be None if connection_type is socket
                user=db_user_resolved,
                password=db_password_resolved,
                connection_type=connection_type, # Pass connection_type to base class
                enable_monitoring=False # Disable monitoring for Miner
            )
            # Operation statistics for monitoring
            # self._operation_stats = {  # This was overwriting the base class's more complete dictionary
            #     'ddl_operations': 0,
            #     'read_operations': 0,
            #     'write_operations': 0,
            #     'long_running_queries': []
            # } 
            # If miner-specific stats are needed, they should be added to the existing self._operation_stats
            # For now, inheriting the base class's stats dictionary resolves the KeyError.
            
            # Custom timeouts for miner operations
            self.MINER_QUERY_TIMEOUT = 60  # 1 minute
            self.MINER_TRANSACTION_TIMEOUT = 300  # 5 minutes
            
            self._engine = None
            self._engine_initialized = False
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

    @track_operation('ddl')
    async def initialize_database(self):
        """Ensure basic database connectivity. Schema is managed by Alembic."""
        try:
            # Basic connectivity test. Actual schema is managed by Alembic.
            await self.execute("SELECT 1") 
            logger.info("Successfully connected to miner database. Schema should be managed by Alembic.")
        except Exception as e:
            logger.error(f"Error during basic database check for miner: {e}")
            raise DatabaseError(f"Failed basic database check for miner: {str(e)}")



    async def close_all_connections(self):
        """Close all database connections gracefully."""
        try:
            logger.info("Closing all database connections...")
            await self.close()  # Using the base class close method
            
            # Clear operation stats
            # self._operation_stats = {  # This was overwriting the base class's more complete dictionary
            #     'ddl_operations': 0,
            #     'read_operations': 0,
            #     'write_operations': 0,
            #     'long_running_queries': []
            # } 
            # If miner-specific stats are needed, they should be added to the existing self._operation_stats
            # For now, inheriting the base class's stats dictionary resolves the KeyError.
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")
            raise DatabaseError(f"Failed to close database connections: {str(e)}")
