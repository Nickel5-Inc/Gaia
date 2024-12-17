import os
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
from dotenv import load_dotenv
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "validator_db")
}

def get_db_url() -> str:
    """Get database URL from configuration."""
    return (
        f"postgresql+asyncpg://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Get database session."""
    from .database_manager import DatabaseManager
    
    db = DatabaseManager(
        database=DB_CONFIG["database"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )
    
    async with db.get_connection() as session:
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            await session.close() 