from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple

from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)


class DeltaCache:
    """
    Tracks last-seen update tokens per logical feed to compute deltas every window.
    For simplicity we track updated_at or similar high-water marks per table.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        # high-water marks by key -> value (e.g., table name -> timestamp)
        self._hwm: Dict[str, Any] = {}
        # last flush time
        self._last_flush_epoch: float = 0.0

    async def get_hwm(self, key: str) -> Any:
        async with self._lock:
            return self._hwm.get(key)

    async def set_hwm(self, key: str, value: Any) -> None:
        async with self._lock:
            self._hwm[key] = value

    async def mark_flushed(self) -> None:
        async with self._lock:
            self._last_flush_epoch = time.time()

    async def time_since_flush(self) -> float:
        async with self._lock:
            if self._last_flush_epoch == 0.0:
                return 1e9
            return time.time() - self._last_flush_epoch


