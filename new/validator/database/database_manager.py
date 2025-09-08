from __future__ import annotations

import os
from typing import Optional

from sqlalchemy.ext.asyncio import (AsyncEngine, AsyncSession,
                                    async_sessionmaker, create_async_engine)


class DatabaseManager:
    def __init__(self, *, url_env: str = "DATABASE_URL") -> None:
        url = os.environ.get(url_env)
        if not url:
            raise RuntimeError(f"Database URL not found in env var {url_env}")
        self._engine: AsyncEngine = create_async_engine(url, pool_pre_ping=True)
        self._session_maker: async_sessionmaker[AsyncSession] = async_sessionmaker(self._engine, expire_on_commit=False)

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    def session(self) -> AsyncSession:
        return self._session_maker()


