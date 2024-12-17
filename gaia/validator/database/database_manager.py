from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from sqlalchemy import select, update, delete, text, event
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import AsyncAdaptedQueuePool
from prefect import task, get_run_logger
from contextlib import asynccontextmanager
from .models import Node, Score, TaskState, ProcessQueue, Base
from .migrations import run_migrations, get_migration_history
from .cache_manager import CacheManager
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class DatabaseManager:
    """Base database manager using SQLAlchemy with Prefect integration."""
    
    def __init__(
        self,
        database: str = "validator_db",
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "postgres",
        pool_size: int = 20,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 1800,
        echo: bool = False,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_password: Optional[str] = None
    ):
        """Initialize database connection with connection pooling."""
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        
        # Connection pool settings
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        
        # Initialize SQLAlchemy engine with optimized settings
        self.engine = create_async_engine(
            f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}",
            poolclass=AsyncAdaptedQueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=True,  # Enable connection health checks
            echo=echo,
            execution_options={
                "isolation_level": "READ_COMMITTED",  # Default isolation level
                "statement_timeout": 30000,  # 30 seconds timeout
                "pool_timeout": pool_timeout
            }
        )

        # Configure session factory with optimized settings
        self.async_session = sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            future=True
        )

        # Initialize cache manager
        self.cache = CacheManager(
            host=redis_host,
            port=redis_port,
            password=redis_password
        )

        # Set up engine event listeners
        self._setup_engine_events()

    def _setup_engine_events(self):
        """Set up event listeners for connection pool monitoring."""
        @event.listens_for(self.engine.sync_engine, "checkout")
        def receive_checkout(dbapi_connection, connection_record, connection_proxy):
            logger.debug("Connection checked out from pool")

        @event.listens_for(self.engine.sync_engine, "checkin")
        def receive_checkin(dbapi_connection, connection_record):
            logger.debug("Connection returned to pool")

        @event.listens_for(self.engine.sync_engine, "connect")
        def receive_connect(dbapi_connection, connection_record):
            logger.info("New database connection established")

    @asynccontextmanager
    async def get_connection(self):
        """Provide a database session/connection with automatic cleanup."""
        session = self.async_session()
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            await session.close()

    async def get_pool_status(self) -> Dict[str, Any]:
        """Get current status of the connection pool."""
        pool = self.engine.sync_engine.pool
        return {
            "size": pool.size(),
            "checkedin": pool.checkedin(),
            "checkedout": pool.checkedout(),
            "overflow": pool.overflow(),
            "checkedout_overflow": pool.overflow_checkedout()
        }

    async def health_check(self) -> Dict[str, bool]:
        """Perform health check on both database and cache."""
        db_healthy = False
        cache_healthy = False
        
        try:
            async with self.get_connection() as session:
                await session.execute(text("SELECT 1"))
                db_healthy = True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            
        try:
            cache_healthy = await self.cache.health_check()
        except Exception as e:
            logger.error(f"Cache health check failed: {e}")
            
        return {
            "database": db_healthy,
            "cache": cache_healthy,
            "overall": db_healthy and cache_healthy
        }

    @task(retries=3, retry_delay_seconds=5)
    async def initialize_database(self):
        """Initialize database tables and run any pending migrations."""
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            async with self.get_connection() as session:
                # Run any pending migrations
                await run_migrations(session)
                
                # Log migration history
                history = await get_migration_history(session)
                if history:
                    logger.info("Migration history:")
                    for migration in history:
                        logger.info(f"Version {migration['version']} applied at {migration['applied_at']}: {migration['description']}")
            
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    async def cleanup(self):
        """Cleanup database and cache connections."""
        try:
            await self.engine.dispose()
            await self.cache.cleanup()
            logger.info("Database and cache connections cleaned up")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            raise

    async def _get_from_cache(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        return await self.cache.get(key)

    async def _set_in_cache(self, key: str, value: Any, ttl: int = 300) -> None:
        """Set value in cache with TTL."""
        await self.cache.set(key, value, ttl)

    async def invalidate_cache(self, pattern: str = "*") -> None:
        """Invalidate cache entries matching pattern."""
        await self.cache.delete_pattern(pattern)

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return await self.cache.get_stats()

    async def _bulk_insert(self, session: AsyncSession, model: Any, items: List[Dict[str, Any]]) -> None:
        """Optimized bulk insert operation."""
        if not items:
            return
            
        try:
            await session.execute(
                model.__table__.insert(),
                items
            )
            await session.commit()
        except SQLAlchemyError as e:
            await session.rollback()
            logger.error(f"Error in bulk insert: {e}")
            raise

    async def _bulk_update(self, session: AsyncSession, model: Any, items: List[Dict[str, Any]], key_field: str) -> None:
        """Optimized bulk update operation."""
        if not items:
            return
            
        try:
            # Create a temporary table for the updates
            temp_table = f"temp_{model.__tablename__}_updates"
            await session.execute(f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM {model.__tablename__} LIMIT 0")
            
            # Bulk insert into temp table
            await session.execute(
                text(f"INSERT INTO {temp_table} SELECT * FROM :items"),
                {"items": items}
            )
            
            # Perform the update
            await session.execute(
                text(f"""
                    UPDATE {model.__tablename__} m
                    SET {', '.join(f'{k} = t.{k}' for k in items[0].keys() if k != key_field)}
                    FROM {temp_table} t
                    WHERE m.{key_field} = t.{key_field}
                """)
            )
            
            # Clean up
            await session.execute(f"DROP TABLE {temp_table}")
            await session.commit()
            
        except SQLAlchemyError as e:
            await session.rollback()
            logger.error(f"Error in bulk update: {e}")
            raise

    @task(retries=3, retry_delay_seconds=5)
    async def update_miner_info(
        self,
        index: int,
        hotkey: str,
        coldkey: str,
        ip: str,
        ip_type: str,
        port: int,
        stake: float,
        trust: float,
        vtrust: float,
        incentive: float,
        protocol: str,
    ) -> None:
        """Update miner information in the database."""
        async with self.get_connection() as session:
            try:
                stmt = update(Node).where(Node.uid == index).values(
                    hotkey=hotkey,
                    coldkey=coldkey,
                    ip=ip,
                    ip_type=ip_type,
                    port=port,
                    stake=stake,
                    trust=trust,
                    vtrust=vtrust,
                    incentive=incentive,
                    protocol=protocol,
                    last_updated=datetime.utcnow()
                )
                await session.execute(stmt)
                await session.commit()
            except SQLAlchemyError as e:
                await session.rollback()
                logger.error(f"Error updating miner info: {e}")
                raise

    @task(retries=3, retry_delay_seconds=5)
    async def get_miner_info(self, index: int) -> Optional[Dict[str, Any]]:
        """Get miner information from the database with caching."""
        cache_key = f"miner_info:{index}"
        
        # Try to get from cache first
        cached_result = await self._get_from_cache(cache_key)
        if cached_result:
            return cached_result
            
        async with self.get_connection() as session:
            try:
                # Use select with specific columns instead of full table scan
                stmt = select(
                    Node.hotkey,
                    Node.coldkey,
                    Node.ip,
                    Node.ip_type,
                    Node.port,
                    Node.stake,
                    Node.trust,
                    Node.vtrust,
                    Node.incentive,
                    Node.protocol
                ).where(Node.uid == index)
                
                result = await session.execute(stmt)
                row = result.first()
                
                if row:
                    miner_info = {
                        "hotkey": row.hotkey,
                        "coldkey": row.coldkey,
                        "ip": row.ip,
                        "ip_type": row.ip_type,
                        "port": row.port,
                        "stake": row.stake,
                        "trust": row.trust,
                        "vtrust": row.vtrust,
                        "incentive": row.incentive,
                        "protocol": row.protocol
                    }
                    # Cache the result
                    await self._set_in_cache(cache_key, miner_info, ttl=300)  # Cache for 5 minutes
                    return miner_info
                return None
            except SQLAlchemyError as e:
                logger.error(f"Error getting miner info: {e}")
                raise

    @task(retries=3, retry_delay_seconds=5)
    async def store_score(
        self,
        task_name: str,
        task_id: str,
        score: List[float],
        status: str = 'pending'
    ) -> None:
        """Store task scores in the database."""
        async with self.get_connection() as session:
            try:
                score_entry = Score(
                    task_name=task_name,
                    task_id=task_id,
                    score=score,
                    status=status
                )
                session.add(score_entry)
                await session.commit()
            except SQLAlchemyError as e:
                await session.rollback()
                logger.error(f"Error storing score: {e}")
                raise

    @task(retries=3, retry_delay_seconds=5)
    async def get_recent_scores(
        self,
        task_name: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent scores for a specific task with caching."""
        cache_key = f"recent_scores:{task_name}:{limit}"
        
        # Try to get from cache first
        cached_result = await self._get_from_cache(cache_key)
        if cached_result:
            return cached_result
            
        async with self.get_connection() as session:
            try:
                # Optimize query with specific columns and index
                stmt = (
                    select(
                        Score.task_id,
                        Score.score,
                        Score.status,
                        Score.created_at
                    )
                    .where(Score.task_name == task_name)
                    .order_by(Score.created_at.desc())
                    .limit(limit)
                )
                
                result = await session.execute(stmt)
                scores = result.fetchall()
                
                formatted_scores = [
                    {
                        "task_id": score.task_id,
                        "score": score.score,
                        "status": score.status,
                        "created_at": score.created_at
                    }
                    for score in scores
                ]
                
                # Cache the result
                await self._set_in_cache(cache_key, formatted_scores, ttl=60)  # Cache for 1 minute
                return formatted_scores
                
            except SQLAlchemyError as e:
                logger.error(f"Error getting recent scores: {e}")
                raise

    async def get_all_active_miners(self) -> List[Dict[str, Any]]:
        """Get information for all miners with non-null hotkeys with caching."""
        cache_key = "active_miners"
        
        # Try to get from cache first
        cached_result = await self._get_from_cache(cache_key)
        if cached_result:
            return cached_result
            
        async with self.get_connection() as session:
            try:
                # Optimize query with specific columns and index
                stmt = (
                    select(
                        Node.uid,
                        Node.hotkey,
                        Node.coldkey,
                        Node.ip,
                        Node.ip_type,
                        Node.port,
                        Node.stake,
                        Node.trust,
                        Node.vtrust,
                        Node.incentive,
                        Node.protocol,
                        Node.last_updated
                    )
                    .where(Node.hotkey.isnot(None))
                    .order_by(Node.uid)
                )
                
                result = await session.execute(stmt)
                nodes = result.fetchall()
                
                formatted_nodes = [
                    {
                        "uid": node.uid,
                        "hotkey": node.hotkey,
                        "coldkey": node.coldkey,
                        "ip": node.ip,
                        "ip_type": node.ip_type,
                        "port": node.port,
                        "stake": node.stake,
                        "trust": node.trust,
                        "vtrust": node.vtrust,
                        "incentive": node.incentive,
                        "protocol": node.protocol,
                        "last_updated": node.last_updated
                    }
                    for node in nodes
                ]
                
                # Cache the result
                await self._set_in_cache(cache_key, formatted_nodes, ttl=60)  # Cache for 1 minute
                return formatted_nodes
                
            except SQLAlchemyError as e:
                logger.error(f"Error getting active miners: {e}")
                raise 