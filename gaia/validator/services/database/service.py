# Thread-safe DatabaseService using SQLAlchemy connection pooling
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone, timedelta
import json
from contextlib import asynccontextmanager

# Import the actual database manager (not wrapper)
from ...database.validator_database_manager import ValidatorDatabaseManager


class DatabaseService:
    """
    Thread-safe database service using SQLAlchemy's built-in connection pooling.
    
    Leverages SQLAlchemy's async engine and connection pool for efficient concurrent operations.
    
    """
    
    _instance: Optional['DatabaseService'] = None
    _lock = asyncio.Lock()
    
    def __new__(cls, *args, **kwargs):
        """Singleton pattern for shared database service."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, database_config: Dict[str, Any] = None, 
                 system_running_event: Optional[asyncio.Event] = None,
                 pool_size: int = 20, max_overflow: int = 30, pool_timeout: int = 30):
        # Prevent re-initialization of singleton
        if hasattr(self, '_initialized'):
            return
            
        self.logger = logging.getLogger(f"{__name__}.DatabaseService")
        
        # SQLAlchemy connection pool configuration (informational - actual config is in ValidatorDatabaseManager)
        self.pool_config = {
            'pool_size': pool_size,          # Number of persistent connections
            'max_overflow': max_overflow,     # Additional connections when pool exhausted  
            'pool_timeout': pool_timeout,     # Timeout for getting connection from pool
            'pool_recycle': 3600,            # Recycle connections after 1 hour
            'pool_pre_ping': True            # Validate connections before use
        }
        
        # Note: ValidatorDatabaseManager uses its own pool config (MAX_CONNECTIONS, etc.)
        
        # Initialize the underlying database manager 
        # Note: ValidatorDatabaseManager already has its own connection pool configuration
        config = database_config or {}
        
        self.db_manager = ValidatorDatabaseManager(
            database=config.get('database', 'validator_db'),
            host=config.get('host', 'localhost'),
            port=config.get('port', 5432),
            user=config.get('user', 'postgres'),
            password=config.get('password', 'postgres'),
            system_running_event=system_running_event
        )
        
        # Simple operation statistics (no complex job tracking needed)
        self.stats = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'avg_response_time': 0.0,
            'active_connections': 0,
            'configured_pool_size': pool_size,    # What we requested 
            'configured_max_overflow': max_overflow,  # What we requested
            'actual_pool_size': 0,               # What ValidatorDatabaseManager actually uses
            'actual_max_overflow': 0             # What ValidatorDatabaseManager actually uses
        }
        
        self._initialized = True
        self.logger.info(f"DatabaseService initialized with SQLAlchemy pool: size={pool_size}, max_overflow={max_overflow}")
    
    async def initialize(self) -> bool:
        """Initialize database connections and schema."""
        try:
            await self.db_manager.initialize_database()
            self.logger.info("✅ DatabaseService initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"❌ DatabaseService initialization failed: {e}")
            return False
    
    def _update_stats(self, execution_time: float, success: bool):
        """Update operation statistics."""
        self.stats['total_operations'] += 1
        
        if success:
            self.stats['successful_operations'] += 1
        else:
            self.stats['failed_operations'] += 1
        
        # Update average response time
        total_ops = self.stats['total_operations']
        current_avg = self.stats['avg_response_time']
        self.stats['avg_response_time'] = (
            (current_avg * (total_ops - 1) + execution_time) / total_ops
        )
    
    def _get_pool_status(self) -> Dict[str, Any]:
        """Get connection pool status from ValidatorDatabaseManager's SQLAlchemy engine."""
        try:
            # Check if ValidatorDatabaseManager has an initialized engine
            if hasattr(self.db_manager, '_engine') and self.db_manager._engine is not None:
                engine = self.db_manager._engine
                if hasattr(engine, 'pool'):
                    pool = engine.pool
                    return {
                        'pool_size': pool.size(),
                        'checked_in': pool.checkedin(), 
                        'checked_out': pool.checkedout(),
                        'overflow': pool.overflow(),
                        'invalid': pool.invalid()
                    }
        except Exception as e:
            self.logger.debug(f"Could not get pool status from engine: {e}")
        
        # Fallback to default values
        return {
            'pool_size': 20,  # ValidatorDatabaseManager.MAX_CONNECTIONS default
            'checked_in': 0,
            'checked_out': 0,
            'overflow': 0,
            'invalid': 0
        }
    
    async def close(self):
        """Close database connections."""
        try:
            await self.db_manager.close_all_connections()
            self.logger.info("DatabaseService closed")
        except Exception as e:
            self.logger.error(f"Error closing DatabaseService: {e}")
    
    # === Core Database Operations ===
    
    async def fetch_all(self, query: str, params: Dict = None) -> List[Dict]:
        """Execute query and return all results."""
        start_time = time.time()
        try:
            result = await self.db_manager.fetch_all(query, params or {})
            result_list = [dict(row) for row in result]
            
            execution_time = time.time() - start_time
            self._update_stats(execution_time, success=True)
            return result_list
            
        except Exception as e:
            execution_time = time.time() - start_time
            self._update_stats(execution_time, success=False)
            self.logger.error(f"Error in fetch_all: {e}")
            raise
    
    async def fetch_one(self, query: str, params: Dict = None) -> Optional[Dict]:
        """Execute query and return one result."""
        start_time = time.time()
        try:
            result = await self.db_manager.fetch_one(query, params or {})
            result_dict = dict(result) if result else None
            
            execution_time = time.time() - start_time
            self._update_stats(execution_time, success=True)
            return result_dict
            
        except Exception as e:
            execution_time = time.time() - start_time
            self._update_stats(execution_time, success=False)
            self.logger.error(f"Error in fetch_one: {e}")
            raise
    
    async def execute(self, query: str, params: Dict = None) -> Any:
        """Execute statement (INSERT/UPDATE/DELETE)."""
        start_time = time.time()
        try:
            result = await self.db_manager.execute(query, params or {})
            
            execution_time = time.time() - start_time
            self._update_stats(execution_time, success=True)
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            self._update_stats(execution_time, success=False)
            self.logger.error(f"Error in execute: {e}")
            raise
    
    @asynccontextmanager
    async def transaction(self):
        """Database transaction context manager."""
        async with self.db_manager.session("database_service_transaction") as session:
            try:
                yield session
                # Commit handled by session context manager
            except Exception:
                # Rollback handled by session context manager
                raise
    
    # === Miner Management ===
    
    async def update_miner_info(self, index: int, hotkey: str, coldkey: str, **kwargs):
        """Update miner information."""
        return await self.db_manager.update_miner_info(
            index=index, hotkey=hotkey, coldkey=coldkey, **kwargs
        )
    
    async def batch_update_miners(self, miners_data: List[Dict[str, Any]]):
        """Batch update multiple miners."""
        return await self.db_manager.batch_update_miners(miners_data)
    
    async def get_miner_info(self, index: int) -> Optional[Dict]:
        """Get miner information by index."""
        return await self.db_manager.get_miner_info(index)
    
    async def get_all_active_miners(self) -> List[Dict]:
        """Get all active miners."""
        return await self.db_manager.get_all_active_miners()
    
    # === Scoring Operations ===
    
    async def get_recent_scores(self, task_type: str) -> List[float]:
        """Get recent scores for a task type."""
        return await self.db_manager.get_recent_scores(task_type)
    
    async def remove_miner_from_score_tables(self, uids: List[int], task_names: List[str], 
                                           filter_start_time: Optional[datetime] = None,
                                           filter_end_time: Optional[datetime] = None):
        """Remove miners from score tables."""
        return await self.db_manager.remove_miner_from_score_tables(
            uids, task_names, filter_start_time, filter_end_time
        )
    
    # === Baseline Predictions ===
    
    async def store_baseline_prediction(self, task_name: str, task_id: str, 
                                      timestamp: datetime, prediction: Any,
                                      region_id: Optional[str] = None) -> bool:
        """Store baseline model prediction."""
        return await self.db_manager.store_baseline_prediction(
            task_name, task_id, timestamp, prediction, region_id
        )
    
    async def get_baseline_prediction(self, task_name: str, task_id: str,
                                    region_id: Optional[str] = None) -> Optional[Dict]:
        """Get baseline prediction."""
        return await self.db_manager.get_baseline_prediction(task_name, task_id, region_id)
    
    # === Health and Stats ===
    
    async def get_health(self) -> Dict[str, Any]:
        """Get database health status."""
        try:
            # Test basic connectivity
            await self.fetch_one("SELECT 1")
            
            # Get operation stats if available
            stats = {}
            if hasattr(self.db_manager, 'get_operation_stats'):
                stats = await self.db_manager.get_operation_stats()
            
            # Get pool status and service stats
            pool_status = self._get_pool_status()
            service_stats = self.get_service_stats()
            
            return {
                "healthy": True,
                "service": "database",
                "stats": stats,
                "service_stats": service_stats,
                "pool_status": pool_status
            }
        except Exception as e:
            return {
                "healthy": False, 
                "service": "database", 
                "error": str(e),
                "service_stats": self.get_service_stats(),
                "pool_status": self._get_pool_status()
            }
    
    def get_service_stats(self) -> Dict[str, Any]:
        """Get DatabaseService-specific statistics."""
        # Update actual pool stats from current pool status
        pool_status = self._get_pool_status()
        self.stats['actual_pool_size'] = pool_status['pool_size']
        
        return {
            "total_operations": self.stats['total_operations'],
            "successful_operations": self.stats['successful_operations'],
            "failed_operations": self.stats['failed_operations'],
            "avg_response_time": self.stats['avg_response_time'],
            "success_rate": (self.stats['successful_operations'] / max(1, self.stats['total_operations'])) * 100,
            "configured_pool_size": self.stats['configured_pool_size'],
            "configured_max_overflow": self.stats['configured_max_overflow'],
            "actual_pool_size": self.stats['actual_pool_size'],
            "actual_max_overflow": self.stats['actual_max_overflow']
        }
    
    def get_pool_status(self) -> Dict[str, Any]:
        """Get detailed connection pool status for monitoring."""
        pool_status = self._get_pool_status()
        
        # Calculate pool utilization
        total_connections = pool_status['checked_out'] + pool_status['checked_in']
        pool_utilization = (pool_status['checked_out'] / max(1, pool_status['pool_size'])) * 100
        
        return {
            **pool_status,
            "total_connections": total_connections,
            "pool_utilization_percent": pool_utilization,
            "has_overflow": pool_status['overflow'] > 0,
            "has_invalid": pool_status['invalid'] > 0,
            "configuration": {
                "requested_pool_size": self.pool_config['pool_size'],
                "requested_max_overflow": self.pool_config['max_overflow'],
                "requested_pool_timeout": self.pool_config['pool_timeout'],
                "actual_pool_size": pool_status['pool_size'],
                "pool_recycle": 300,  # ValidatorDatabaseManager default
                "pool_pre_ping": True  # ValidatorDatabaseManager default
            }
        }
    
    async def get_operation_stats(self) -> Dict[str, Any]:
        """Get database operation statistics."""
        if hasattr(self.db_manager, 'get_operation_stats'):
            return await self.db_manager.get_operation_stats()
        return {}
    
    # === Utility Methods ===
    
    def is_storage_locked(self) -> bool:
        """Check if database storage is locked."""
        return getattr(self.db_manager, '_storage_locked', False)
    
    def lock_storage(self):
        """Lock database storage (prevents writes)."""
        if hasattr(self.db_manager, '_storage_locked'):
            self.db_manager._storage_locked = True
            self.logger.warning("Database storage locked")
    
    def unlock_storage(self):
        """Unlock database storage."""
        if hasattr(self.db_manager, '_storage_locked'):
            self.db_manager._storage_locked = False
            self.logger.info("Database storage unlocked") 