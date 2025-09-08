"""
Memory management utilities for weather scoring workers.
Provides per-miner cleanup and global LRU cache limits for handling many miners efficiently.
"""

import functools
import gc
import os
import sys
import warnings
from typing import Any, Dict, Optional

from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)

# Global LRU cache size limits for scoring workers
DEFAULT_LRU_MAXSIZE = int(os.getenv("GAIA_SCORING_LRU_MAXSIZE", "128"))
AGGRESSIVE_LRU_MAXSIZE = int(os.getenv("GAIA_SCORING_AGGRESSIVE_LRU_MAXSIZE", "32"))


class PerMinerMemoryManager:
    """
    Manages memory cleanup between individual miner scoring operations.
    Designed to prevent memory accumulation when scoring many miners.
    """

    def __init__(self, worker_name: str = "ScoringWorker"):
        self.worker_name = worker_name
        self.cleanup_count = 0

    def cleanup_between_miners(
        self, miner_hotkey: str, run_id: int, force_aggressive: bool = False
    ) -> Dict[str, Any]:
        """
        Perform targeted memory cleanup between individual miner scoring operations.

        Args:
            miner_hotkey: Identifier for the miner just scored
            run_id: Run ID for logging context
            force_aggressive: Whether to force aggressive cleanup

        Returns:
            Dictionary with cleanup statistics
        """
        self.cleanup_count += 1

        logger.debug(
            f"[{self.worker_name}] Run {run_id}: Starting per-miner cleanup after {miner_hotkey}"
        )

        stats = {
            "miner_hotkey": miner_hotkey,
            "lru_caches_cleared": 0,
            "cache_objects_cleared": 0,
            "gc_objects_collected": 0,
            "modules_processed": 0,
        }

        try:
            # 1. Clear LRU caches and cache objects in high-impact modules
            stats.update(self._clear_targeted_caches(force_aggressive))

            # 2. Perform garbage collection (1-2 passes for per-miner cleanup)
            gc_passes = 2 if force_aggressive else 1
            for pass_num in range(gc_passes):
                collected = gc.collect()
                stats["gc_objects_collected"] += collected
                if collected == 0:
                    break

            # 3. Clear Python's type cache periodically
            if self.cleanup_count % 10 == 0:  # Every 10th miner
                if hasattr(sys, "_clear_type_cache"):
                    sys._clear_type_cache()
                    logger.debug(
                        f"[{self.worker_name}] Run {run_id}: Cleared Python type cache"
                    )

            logger.debug(
                f"[{self.worker_name}] Run {run_id}: Per-miner cleanup for {miner_hotkey}: "
                f"{stats['lru_caches_cleared']} LRU caches, {stats['cache_objects_cleared']} cache objects, "
                f"{stats['gc_objects_collected']} GC objects"
            )

        except Exception as cleanup_err:
            logger.debug(
                f"[{self.worker_name}] Run {run_id}: Error in per-miner cleanup for {miner_hotkey}: {cleanup_err}"
            )

        return stats

    def _clear_targeted_caches(self, aggressive: bool = False) -> Dict[str, int]:
        """Clear caches in targeted modules that are likely to accumulate during scoring."""

        lru_caches_cleared = 0
        cache_objects_cleared = 0
        modules_processed = 0

        # High-priority modules for cache clearing during scoring
        priority_modules = [
            "xarray",
            "numpy",
            "scipy",
            "pandas",
            "netCDF4",
            "zarr",
            "dask",
            "xskillscore",
            "matplotlib",
            "numba",
            "sklearn",
            "gaia.tasks.defined_tasks.weather",
        ]

        # Additional modules for aggressive cleanup
        if aggressive:
            priority_modules.extend(
                [
                    "gaia.validator",
                    "gaia.miner",
                    "torch",
                    "tensorflow",
                    "cfgrib",
                    "metpy",
                    "cartopy",
                    "pint",
                ]
            )

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")

            for module_pattern in priority_modules:
                for module_name in list(sys.modules.keys()):
                    if module_pattern in module_name:
                        module = sys.modules.get(module_name)
                        if module and hasattr(module, "__dict__"):
                            modules_processed += 1

                            for attr_name in list(module.__dict__.keys()):
                                try:
                                    attr = getattr(module, attr_name)

                                    # Clear LRU cache decorators
                                    if hasattr(attr, "cache_clear") and callable(
                                        attr.cache_clear
                                    ):
                                        attr.cache_clear()
                                        lru_caches_cleared += 1

                                    # Clear cache containers/objects
                                    elif any(
                                        cache_pattern in attr_name.lower()
                                        for cache_pattern in [
                                            "cache",
                                            "_cached",
                                            "_memo",
                                            "_buffer",
                                            "registry",
                                        ]
                                    ):
                                        if hasattr(attr, "clear") and callable(
                                            attr.clear
                                        ):
                                            attr.clear()
                                            cache_objects_cleared += 1
                                        elif isinstance(attr, (dict, list, set)):
                                            attr.clear()
                                            cache_objects_cleared += 1

                                except Exception:
                                    continue

        return {
            "lru_caches_cleared": lru_caches_cleared,
            "cache_objects_cleared": cache_objects_cleared,
            "modules_processed": modules_processed,
        }


def apply_global_lru_limits():
    """
    Apply global LRU cache size limits to common scientific computing functions.
    This prevents unlimited cache growth during scoring of many miners.
    """

    logger.info(f"Applying global LRU cache limits (maxsize={DEFAULT_LRU_MAXSIZE})")

    try:
        # Apply limits to commonly cached functions in scientific libraries

        # NumPy functions
        if "numpy" in sys.modules:
            numpy = sys.modules["numpy"]
            _apply_lru_limit_to_module_functions(numpy, "numpy")

        # XArray functions
        if "xarray" in sys.modules:
            xarray = sys.modules["xarray"]
            _apply_lru_limit_to_module_functions(xarray, "xarray")

        # Scipy functions
        if "scipy" in sys.modules:
            scipy = sys.modules["scipy"]
            _apply_lru_limit_to_module_functions(scipy, "scipy")

        # Pandas functions
        if "pandas" in sys.modules:
            pandas = sys.modules["pandas"]
            _apply_lru_limit_to_module_functions(pandas, "pandas")

        # Weather-specific modules
        weather_modules = [
            mod
            for mod in sys.modules.keys()
            if "gaia.tasks.defined_tasks.weather" in mod
        ]
        for mod_name in weather_modules:
            module = sys.modules[mod_name]
            _apply_lru_limit_to_module_functions(module, mod_name)

        logger.info("Global LRU cache limits applied successfully")

    except Exception as e:
        logger.warning(f"Error applying global LRU limits: {e}")


def _apply_lru_limit_to_module_functions(module, module_name: str):
    """Apply LRU cache limits to functions in a module that don't already have limits."""

    if not hasattr(module, "__dict__"):
        return

    limited_count = 0

    for attr_name in dir(module):
        try:
            attr = getattr(module, attr_name)

            # Check if this is a function with an LRU cache that has no size limit
            if (
                hasattr(attr, "cache_info")
                and hasattr(attr, "cache_clear")
                and callable(attr.cache_info)
            ):

                cache_info = attr.cache_info()

                # If maxsize is None (unlimited), apply a limit
                if cache_info.maxsize is None:
                    # Create a new limited version of the function
                    if hasattr(attr, "__wrapped__"):
                        original_func = attr.__wrapped__
                        limited_func = functools.lru_cache(maxsize=DEFAULT_LRU_MAXSIZE)(
                            original_func
                        )
                        setattr(module, attr_name, limited_func)
                        limited_count += 1

        except Exception:
            continue

    if limited_count > 0:
        logger.debug(
            f"Applied LRU limits to {limited_count} functions in {module_name}"
        )


def create_memory_aware_lru_cache(maxsize: Optional[int] = None):
    """
    Create an LRU cache decorator with memory-aware size limits.
    Automatically reduces cache size in high-memory scenarios.
    """

    if maxsize is None:
        maxsize = DEFAULT_LRU_MAXSIZE

    # Check available memory and adjust cache size
    try:
        import psutil

        memory_percent = psutil.virtual_memory().percent

        if memory_percent > 85:  # High memory usage
            maxsize = min(maxsize, AGGRESSIVE_LRU_MAXSIZE)
            logger.debug(
                f"High memory usage ({memory_percent}%), reducing LRU cache size to {maxsize}"
            )

    except ImportError:
        # psutil not available, use default
        pass
    except Exception as e:
        logger.debug(f"Error checking memory usage for cache sizing: {e}")

    return functools.lru_cache(maxsize=maxsize)


# Convenience decorator for weather scoring functions
memory_aware_cache = create_memory_aware_lru_cache()
