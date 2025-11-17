import multiprocessing as mp
import os
from typing import Optional

from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)


def start_websync_subprocess() -> Optional[mp.Process]:
    if os.getenv("WEBSYNC_ON", "false").lower() != "true":
        logger.info("WebSync: disabled by WEBSYNC_ON env var")
        return None

    try:
        from gaia.websync.worker_main import main as websync_main
    except Exception as e:
        logger.error(f"WebSync: cannot import worker_main: {e}")
        return None

    p = mp.Process(name="websync-worker", target=websync_main, daemon=True)
    p.start()
    logger.info(f"WebSync: spawned subprocess pid={p.pid}")
    return p


