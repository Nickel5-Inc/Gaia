"""Cache manager for database operations using Redis."""

import json
from typing import Any, Optional, Dict
from datetime import datetime
import aioredis
from prefect import task
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class CacheManager:
    """Redis-based cache manager with Prefect integration."""
    
    _instance = None
    _initialized = False
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        max_connections: int = 10,
        **kwargs
    ):
        """Initialize Redis connection pool."""
        if self._initialized:
            return
            
        self.redis_url = f"redis://{':' + password + '@' if password else ''}{host}:{port}/{db}"
        self.pool = None
        self.max_connections = max_connections
        self._initialized = True
    
    async def initialize(self):
        """Initialize Redis connection pool."""
        if self.pool is None:
            try:
                self.pool = aioredis.ConnectionPool.from_url(
                    self.redis_url,
                    max_connections=self.max_connections,
                    decode_responses=True
                )
                self.redis = aioredis.Redis(connection_pool=self.pool)
                logger.info("Redis connection pool initialized")
            except Exception as e:
                logger.error(f"Error initializing Redis connection pool: {e}")
                raise
    
    async def cleanup(self):
        """Cleanup Redis connections."""
        if self.pool:
            await self.pool.disconnect()
            logger.info("Redis connections cleaned up")
    
    @task(retries=2, retry_delay_seconds=1)
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        try:
            value = await self.redis.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Error getting from cache: {e}")
            return None
    
    @task(retries=2, retry_delay_seconds=1)
    async def set(self, key: str, value: Any, ttl: int = 300) -> bool:
        """Set value in cache with TTL."""
        try:
            serialized = json.dumps(value)
            await self.redis.setex(key, ttl, serialized)
            return True
        except Exception as e:
            logger.error(f"Error setting in cache: {e}")
            return False
    
    @task(retries=2, retry_delay_seconds=1)
    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        try:
            await self.redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Error deleting from cache: {e}")
            return False
    
    @task(retries=2, retry_delay_seconds=1)
    async def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching pattern."""
        try:
            keys = await self.redis.keys(pattern)
            if keys:
                return await self.redis.delete(*keys)
            return 0
        except Exception as e:
            logger.error(f"Error deleting pattern from cache: {e}")
            return 0
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        try:
            info = await self.redis.info()
            return {
                "used_memory": info.get("used_memory_human"),
                "connected_clients": info.get("connected_clients"),
                "total_connections_received": info.get("total_connections_received"),
                "total_commands_processed": info.get("total_commands_processed"),
                "keyspace_hits": info.get("keyspace_hits"),
                "keyspace_misses": info.get("keyspace_misses"),
                "uptime_in_seconds": info.get("uptime_in_seconds")
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {}
    
    async def health_check(self) -> bool:
        """Check if cache is healthy."""
        try:
            return await self.redis.ping()
        except Exception as e:
            logger.error(f"Cache health check failed: {e}")
            return False 