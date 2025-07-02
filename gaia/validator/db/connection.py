import asyncpg
import logging
from typing import Optional
from gaia.validator.utils.config import settings

logger = logging.getLogger(__name__)

# Global connection pool instance
_POOL: Optional[asyncpg.Pool] = None


async def get_db_pool() -> asyncpg.Pool:
    """
    Returns a singleton instance of the asyncpg connection pool.
    This ensures the IO-Engine uses a single, shared pool for all database operations.
    """
    global _POOL
    if _POOL is None:
        logger.info("Initializing database connection pool")
        
        # Construct database URL from settings
        db_url = f"postgresql://{settings.DATABASE.USER}:{settings.DATABASE.PASSWORD}@{settings.DATABASE.HOST}:{settings.DATABASE.PORT}/{settings.DATABASE.NAME}"
        
        try:
            _POOL = await asyncpg.create_pool(
                dsn=db_url,
                min_size=settings.DATABASE.POOL_MIN_SIZE,
                max_size=settings.DATABASE.POOL_MAX_SIZE,
                command_timeout=60,
                server_settings={
                    'jit': 'off',  # Disable JIT for better performance in short queries
                }
            )
            logger.info(f"Database pool created: {settings.DATABASE.POOL_MIN_SIZE}-{settings.DATABASE.POOL_MAX_SIZE} connections")
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise
    
    return _POOL


async def close_db_pool():
    """Close the database connection pool."""
    global _POOL
    if _POOL:
        logger.info("Closing database connection pool")
        await _POOL.close()
        _POOL = None


async def check_db_health() -> bool:
    """Check if the database connection is healthy."""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False