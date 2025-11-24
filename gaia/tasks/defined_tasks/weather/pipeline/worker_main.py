from __future__ import annotations

import asyncio
import time
import os
import signal
import multiprocessing as mp
from typing import Optional
import logging
import sys
import re

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager, DatabaseError
from gaia.tasks.defined_tasks.weather.pipeline.workers import process_one, process_one_utility
from gaia.utils.custom_logger import get_logger
from gaia.utils.gcsfs_safe_close import apply_gcsfs_threadsafe_close_patch

# Use custom logger for consistency with other modules
logger = get_logger(__name__)


def _patch_gcsfs_close_session() -> None:
    apply_gcsfs_threadsafe_close_patch()


def _prefix() -> str:
    pname = mp.current_process().name if mp.current_process() else "weather-w?"
    return f"[{pname}]"


def _get_rss_mb() -> float:
    """Return current process RSS in MB (fallbacks avoid external deps)."""
    try:
        import psutil  # type: ignore

        return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except Exception:
        try:
            # Linux: read VmRSS from /proc/self/status
            with open("/proc/self/status", "r") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        parts = line.split()
                        if len(parts) >= 2:
                            # value in kB
                            return float(parts[1]) / 1024.0
        except Exception:
            pass
        try:
            # Fallback to ru_maxrss (max, not current, but better than nothing)
            import resource  # type: ignore

            rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # On Linux ru_maxrss is in kilobytes
            return float(rss_kb) / 1024.0
        except Exception:
            return -1.0


async def worker_loop(db: ValidatorDatabaseManager, idle_sleep: float = 5.0, memory_limit_mb: float = 0.0) -> None:
    last_idle_log = 0.0
    tag = _prefix()
    
    # Create a WeatherTask instance for this worker
    # This provides the necessary validator context for miner communication
    try:
        from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
        from fiber.chain import chain_utils
        import os
        
        # Load the validator keypair from wallet
        wallet_name = os.getenv("WALLET_NAME", "default")
        hotkey_name = os.getenv("HOTKEY_NAME", "default")
        
        try:
            keypair = chain_utils.load_hotkey_keypair(wallet_name, hotkey_name)
            logger.info(f"loaded keypair for wallet={wallet_name}, hotkey={hotkey_name}")
        except Exception as e:
            logger.warning(f"could not load keypair: {e}, will try to proceed without it")
            keypair = None
        
        # Create the task with the keypair; honor explicit test-mode env only
        tm_env = os.getenv("WEATHER_TASK_TEST_MODE", "").strip().lower()
        is_test_mode = tm_env in ("1", "true", "yes", "on")
        task = WeatherTask(db_manager=db, node_type="validator", test_mode=is_test_mode, keypair=keypair)
        validator = task
        logger.info("initialized WeatherTask for miner communication with keypair")
    except Exception as e:
        logger.warning(f"could not initialize WeatherTask: {e}, proceeding without validator context")
        validator = None
    
    # Force-reload workers module to avoid stale pre-fork imports; rebind handlers
    try:
        import importlib
        import gaia.tasks.defined_tasks.weather.pipeline.workers as _workers_mod
        _workers_mod = importlib.reload(_workers_mod)
        globals()['process_one'] = getattr(_workers_mod, 'process_one')
        globals()['process_one_utility'] = getattr(_workers_mod, 'process_one_utility')
        logger.debug("reloaded workers module and rebound handlers")
    except Exception:
        pass

    while True:
        try:
            # If this is the UTILITY WORKER, use utility loop; else normal loop
            use_utility = False
            try:
                import multiprocessing as _mp
                _pname = _mp.current_process().name if _mp.current_process() else ""
                use_utility = (_pname.strip().upper() == "UTILITY WORKER")
            except Exception:
                use_utility = False

            processed = await (process_one_utility(db, validator=validator) if use_utility else process_one(db, validator=validator))
            # Memory guard: check if we should restart after completing current job
            if memory_limit_mb and memory_limit_mb > 0:
                rss_mb = _get_rss_mb()
                if rss_mb >= 0 and rss_mb > memory_limit_mb:
                    if processed:
                        logger.warning(
                            f"memory threshold exceeded: rss={rss_mb:.1f}MB > limit={memory_limit_mb:.1f}MB — exiting after completing job for restart"
                        )
                    else:
                        logger.warning(
                            f"memory threshold exceeded: rss={rss_mb:.1f}MB > limit={memory_limit_mb:.1f}MB — exiting for restart"
                        )
                    break
            if not processed:
                now = time.time()
                # Occasional idle heartbeat
                if now - last_idle_log > 60:
                    rss_mb = _get_rss_mb()
                    mem_str = f" | rss={rss_mb:.1f}MB" if rss_mb >= 0 else ""
                    logger.info(f"idle: no jobs to claim currently{mem_str}")
                    last_idle_log = now
                await asyncio.sleep(idle_sleep)
        except asyncio.CancelledError:
            break
        except Exception:
            # brief backoff on unexpected errors
            await asyncio.sleep(1.0)


async def _install_worker_prefix_filters(tag: str) -> None:
    import logging

    # Don't set up custom logging - let the global custom logger system handle it
    # The global system will properly format worker logs as [WORKER X/Y]
    # Setting up additional handlers here causes conflicts and double bracketing
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Suppress duplicate logs from ONLY heavy background modules to prevent double logging
    # Keep important worker processing logs visible for debugging
    for module in ['gfs_api', 'era5_api', 'async_processing', 'memory_management']:
        logger = logging.getLogger(module)
        logger.setLevel(logging.WARNING)  # Only show warnings and errors from these modules in workers
    
    # Keep these modules at INFO level in workers for operational visibility:
    # - weather_task: Important for job processing logs
    # - weather_scoring_mechanism: Critical for scoring progress
    # - hardening_integration: Important for error recovery


async def main() -> None:
    db = ValidatorDatabaseManager()
    tag = _prefix()
    _patch_gcsfs_close_session()
    await _install_worker_prefix_filters(tag)
    # Retry DB init while Postgres is restarting or paused for maintenance
    max_tries = int(os.getenv("WEATHER_WORKER_DB_INIT_RETRIES", "30"))
    backoff = 1.0
    for attempt in range(1, max_tries + 1):
        try:
            await db.initialize_database()
            break
        except Exception as e:
            msg = str(e)
            if "shutting down" in msg or "CannotConnectNow" in msg or "Failed to initialize database engine" in msg:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 10.0)
                continue
            raise
    try:
        idle = float(os.getenv("WEATHER_WORKER_IDLE_SLEEP", "5"))
        mem_limit = float(os.getenv("WEATHER_WORKER_RSS_LIMIT_MB", "3072"))
        logger.info(f"started (idle_sleep={idle}s, rss_limit={mem_limit}MB)")
        await worker_loop(db, idle_sleep=idle, memory_limit_mb=mem_limit)
    finally:
        # Give any scheduled close tasks a chance to run
        try:
            await asyncio.sleep(0)
        except Exception:
            pass
        await db.close_all_connections()


async def utility_worker_loop(db: ValidatorDatabaseManager, idle_sleep: float = 5.0, memory_limit_mb: float = 0.0) -> None:
    last_idle_log = 0.0
    tag = _prefix()
    validator = None
    try:
        from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
        import os
        tm_env = os.getenv("WEATHER_TASK_TEST_MODE", "").strip().lower()
        is_test_mode = tm_env in ("1", "true", "yes", "on")
        validator = WeatherTask(db_manager=db, node_type="validator", test_mode=is_test_mode, keypair=None)
    except Exception:
        validator = None
    while True:
        try:
            processed = await process_one_utility(db, validator=validator)
            if memory_limit_mb and memory_limit_mb > 0:
                rss_mb = _get_rss_mb()
                if rss_mb >= 0 and rss_mb > memory_limit_mb:
                    if processed:
                        logger.warning(
                            f"memory threshold exceeded: rss={rss_mb:.1f}MB > limit={memory_limit_mb:.1f}MB — exiting after completing job for restart"
                        )
                    else:
                        logger.warning(
                            f"memory threshold exceeded: rss={rss_mb:.1f}MB > limit={memory_limit_mb:.1f}MB — exiting for restart"
                        )
                    break
            if not processed:
                now = time.time()
                if now - last_idle_log > 60:
                    rss_mb = _get_rss_mb()
                    mem_str = f" | rss={rss_mb:.1f}MB" if rss_mb >= 0 else ""
                    logger.info(f"idle: no jobs to claim currently{mem_str}")
                    last_idle_log = now
                await asyncio.sleep(idle_sleep)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(1.0)


async def utility_main() -> None:
    db = ValidatorDatabaseManager()
    tag = _prefix()
    _patch_gcsfs_close_session()
    await _install_worker_prefix_filters(tag)
    # Force-reload workers module to avoid stale pre-fork imports; rebind handlers
    try:
        import importlib
        import gaia.tasks.defined_tasks.weather.pipeline.workers as _workers_mod
        _workers_mod = importlib.reload(_workers_mod)
        globals()['process_one'] = getattr(_workers_mod, 'process_one')
        globals()['process_one_utility'] = getattr(_workers_mod, 'process_one_utility')
        logger.debug("reloaded workers module and rebound handlers (utility)")
    except Exception:
        pass
    # Retry DB init while Postgres is restarting or paused for maintenance
    max_tries = int(os.getenv("WEATHER_WORKER_DB_INIT_RETRIES", "30"))
    backoff = 1.0
    for attempt in range(1, max_tries + 1):
        try:
            await db.initialize_database()
            break
        except Exception as e:
            msg = str(e)
            if "shutting down" in msg or "CannotConnectNow" in msg or "Failed to initialize database engine" in msg:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 10.0)
                continue
            raise
    try:
        idle = float(os.getenv("WEATHER_WORKER_IDLE_SLEEP", "5"))
        mem_limit = float(os.getenv("WEATHER_WORKER_RSS_LIMIT_MB", "3072"))
        logger.info(f"started (UTILITY) (idle_sleep={idle}s, rss_limit={mem_limit}MB)")
        await utility_worker_loop(db, idle_sleep=idle, memory_limit_mb=mem_limit)
    finally:
        try:
            await asyncio.sleep(0)
        except Exception:
            pass
        await db.close_all_connections()


if __name__ == "__main__":
    # Simple signal-friendly runner
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    task: Optional[asyncio.Task] = None
    try:
        task = loop.create_task(main())

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: task.cancel() if task else None)

        loop.run_until_complete(task)
    finally:
        loop.close()


