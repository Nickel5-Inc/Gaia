"""Redis setup and initialization script."""

import asyncio
from typing import Dict, Any
import aioredis
from prefect import task
from .redis_config import get_redis_config, get_redis_url
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

async def init_redis() -> aioredis.Redis:
    """Initialize Redis connection with configuration."""
    config = get_redis_config()
    redis_url = get_redis_url()
    
    try:
        # Create connection pool
        pool = aioredis.ConnectionPool.from_url(
            redis_url,
            max_connections=config["max_connections"],
            socket_timeout=config["socket_timeout"],
            socket_connect_timeout=config["socket_connect_timeout"],
            retry_on_timeout=config["retry_on_timeout"],
            health_check_interval=config["health_check_interval"],
            decode_responses=True
        )
        
        # Create Redis client
        redis = aioredis.Redis(connection_pool=pool)
        
        # Test connection
        await redis.ping()
        logger.info("Redis connection established successfully")
        
        return redis
    except Exception as e:
        logger.error(f"Failed to initialize Redis: {e}")
        raise

@task(retries=3, retry_delay_seconds=5)
async def verify_redis_connection() -> Dict[str, Any]:
    """Verify Redis connection and get server info."""
    redis = await init_redis()
    try:
        # Run basic health checks
        is_connected = await redis.ping()
        info = await redis.info()
        config = await redis.config_get('*')
        
        status = {
            "connected": is_connected,
            "version": info.get("redis_version"),
            "used_memory": info.get("used_memory_human"),
            "connected_clients": info.get("connected_clients"),
            "uptime_days": info.get("uptime_in_days"),
            "max_memory": config.get("maxmemory-policy", "unknown"),
            "persistence": bool(info.get("rdb_last_save_time")),
        }
        
        logger.info(f"Redis status: {status}")
        return status
        
    except Exception as e:
        logger.error(f"Redis verification failed: {e}")
        raise
    finally:
        await redis.close()

@task
async def setup_redis_persistence():
    """Configure Redis persistence settings."""
    redis = await init_redis()
    try:
        # Enable RDB persistence
        await redis.config_set('save', '900 1 300 10 60 10000')
        await redis.config_set('stop-writes-on-bgsave-error', 'yes')
        
        # Set memory policies
        await redis.config_set('maxmemory-policy', 'allkeys-lru')
        await redis.config_set('maxmemory', '1gb')  # Adjust based on your needs
        
        logger.info("Redis persistence configured")
    except Exception as e:
        logger.error(f"Failed to configure Redis persistence: {e}")
        raise
    finally:
        await redis.close()

async def cleanup_redis():
    """Cleanup Redis connections."""
    redis = await init_redis()
    try:
        await redis.close()
        logger.info("Redis connections cleaned up")
    except Exception as e:
        logger.error(f"Error cleaning up Redis connections: {e}")

if __name__ == "__main__":
    async def main():
        """Main setup function."""
        try:
            # Initialize Redis
            await verify_redis_connection()
            
            # Configure persistence
            await setup_redis_persistence()
            
            logger.info("Redis setup completed successfully")
        except Exception as e:
            logger.error(f"Redis setup failed: {e}")
            raise
        finally:
            await cleanup_redis()
    
    # Run setup
    asyncio.run(main()) 