import gc
import logging
import sys
from datetime import datetime, timezone, timedelta
import os
import time
import threading
import concurrent.futures
import glob
import signal
import tracemalloc
import memray
import asyncio
import traceback
import math

from gaia.database.database_manager import DatabaseTimeout
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None 
    PSUTIL_AVAILABLE = False
    print("psutil not found, memory logging will be skipped.")

import ssl
import random
from typing import Any, Optional, List, Dict, Set
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import httpx
from fiber.chain import chain_utils, interface
from fiber.chain import weights as w
from fiber.chain.fetch_nodes import get_nodes_for_netuid
from fiber.chain.chain_utils import query_substrate
from fiber.logging_utils import get_logger
from fiber.encrypted.validator import client as vali_client, handshake
from fiber.chain.metagraph import Metagraph
from fiber.chain.interface import get_substrate
from substrateinterface import SubstrateInterface
from argparse import ArgumentParser
import pandas as pd
import json
import base64
import numpy as np
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

logger = get_logger(__name__)


class MemoryManager:
    """Manages memory monitoring, cleanup, and optimization for the validator."""
    
    def __init__(self, validator_instance):
        self.validator = validator_instance
        self.tracemalloc_snapshot1 = getattr(validator_instance, 'tracemalloc_snapshot1', None)
        self._shutdown_event = validator_instance._shutdown_event
        self._background_tasks = validator_instance._background_tasks
        self.substrate_manager = validator_instance.substrate_manager

    async def memory_snapshot_taker(self):
        """Periodically takes memory snapshots and logs differences."""
        logger.info("Starting memory snapshot taker task...")
        
        snapshot_interval_seconds = 300 # 5 minutes
        logger.info(f"Memory snapshots will be taken every {snapshot_interval_seconds} seconds.")

        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(snapshot_interval_seconds)
                if self._shutdown_event.is_set():
                    break

                logger.info("--- Taking Tracemalloc Snapshot ---")
                current_snapshot = tracemalloc.take_snapshot()
                
                logger.info("Top 10 current memory allocations (by line number):")
                for stat in current_snapshot.statistics('lineno')[:10]:
                    logger.info(f"  {stat}")
                    # Uncomment for full traceback of top allocations if needed
                    # logger.info(f"    Traceback for allocation at {stat.traceback[0]}:")
                    # for line in stat.traceback.format():
                    #    logger.info(f"      {line}")

                if self.tracemalloc_snapshot1:
                    logger.info("Comparing to previous snapshot...")
                    top_stats = current_snapshot.compare_to(self.tracemalloc_snapshot1, 'lineno')
                    logger.info("Top 10 memory differences since last snapshot:")
                    for stat in top_stats[:10]:
                        logger.info(f"  {stat}")
                        # Uncomment for full traceback of significant differences
                        # logger.info(f"    Traceback for diff at {stat.traceback[0]}:")
                        # for line in stat.traceback.format():
                        #    logger.info(f"      {line}")
                
                self.tracemalloc_snapshot1 = current_snapshot
                logger.info("--- Tracemalloc Snapshot Processed ---")

            except asyncio.CancelledError:
                logger.info("Memory snapshot taker task cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in memory_snapshot_taker: {e}", exc_info=True)
                await asyncio.sleep(60) # Wait a bit before retrying if an error occurs

    async def periodic_substrate_cleanup(self):
        """Periodically force substrate connection cleanup to prevent memory leaks."""
        while True:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes (more frequent cleanup)
                
                # Use managed substrate connection for periodic cleanup
                logger.debug("Periodic cleanup using managed substrate connection")
                try:
                    # Get fresh managed connection
                    self.validator.substrate = self.substrate_manager.get_fresh_connection()
                    logger.debug("🔄 Periodic cleanup - using managed fresh substrate connection")
                    
                    # Force garbage collection
                    import gc
                    collected = gc.collect()
                    logger.debug(f"Periodic substrate cleanup - GC collected {collected} objects")
                    
                except Exception as e:
                    logger.debug(f"Error during periodic substrate cleanup: {e}")
                
            except asyncio.CancelledError:
                logger.info("Periodic substrate cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in periodic substrate cleanup: {e}")
                await asyncio.sleep(60)  # Shorter retry on error

    async def aggressive_memory_cleanup(self):
        """Enhanced aggressive memory cleanup with pressure monitoring."""
        while True:
            try:
                await asyncio.sleep(120)  # Run every 2 minutes
                
                # Get memory before cleanup
                if PSUTIL_AVAILABLE:
                    process = psutil.Process()
                    memory_before = process.memory_info().rss / (1024 * 1024)
                    
                    # Determine cleanup intensity based on memory pressure
                    cleanup_level = "light"
                    if memory_before > 8000:  # > 8GB
                        cleanup_level = "aggressive"
                    elif memory_before > 6000:  # > 6GB
                        cleanup_level = "moderate"
                        
                    # Perform cleanup based on pressure level
                    collected = 0
                    comp_memory_freed = 0
                    
                    if cleanup_level == "aggressive":
                        logger.warning(f"HIGH MEMORY PRESSURE: {memory_before:.1f}MB - performing aggressive cleanup")
                        
                        # Force comprehensive cleanup first
                        if hasattr(self.validator, 'last_metagraph_sync'):
                            comp_memory_freed = self._comprehensive_memory_cleanup("pressure_aggressive")
                        
                        # Clean up HTTP connections more aggressively
                        try:
                            if hasattr(self.validator, 'miner_client') and self.validator.miner_client and not self.validator.miner_client.is_closed:
                                await self.validator._cleanup_idle_connections()
                        except Exception:
                            pass
                            
                        # Force multiple GC passes
                        import gc
                        for _ in range(3):
                            collected += gc.collect()
                            
                        # Clean up background tasks that may be accumulating
                        try:
                            task_count = len(self._background_tasks)
                            if task_count > 50:  # Too many background tasks
                                logger.warning(f"High background task count: {task_count} - cleaning up completed tasks")
                                completed_tasks = [t for t in self._background_tasks if t.done()]
                                for task in completed_tasks:
                                    self._background_tasks.discard(task)
                                logger.info(f"Removed {len(completed_tasks)} completed background tasks")
                        except Exception:
                            pass
                            
                    elif cleanup_level == "moderate":
                        logger.info(f"MODERATE MEMORY PRESSURE: {memory_before:.1f}MB - performing moderate cleanup")
                        
                        # Standard comprehensive cleanup
                        if hasattr(self.validator, 'last_metagraph_sync'):
                            comp_memory_freed = self._comprehensive_memory_cleanup("pressure_moderate")
                        
                        # Standard GC
                        import gc
                        collected = gc.collect()
                        
                    else:  # light cleanup
                        # Just basic GC and cache clearing
                        import gc
                        collected = gc.collect()
                        
                        # Clear basic module caches
                        try:
                            if hasattr(self.validator, 'miner_client') and self.validator.miner_client and not self.validator.miner_client.is_closed:
                                await self.validator._cleanup_idle_connections()
                        except Exception:
                            pass
                    
                    # Get memory after cleanup
                    memory_after = process.memory_info().rss / (1024 * 1024)
                    memory_freed = memory_before - memory_after
                    
                    # Log results based on effectiveness
                    if cleanup_level == "aggressive" or memory_freed > 50:
                        logger.info(f"Memory cleanup ({cleanup_level}): {memory_before:.1f}MB → {memory_after:.1f}MB "
                                  f"(freed {memory_freed:.1f}MB, GC collected {collected} objects)")
                    elif collected > 50 or comp_memory_freed > 10:
                        logger.info(f"Memory cleanup ({cleanup_level}): GC collected {collected} objects, "
                                  f"comprehensive freed {comp_memory_freed:.1f}MB")
                    else:
                        logger.debug(f"Memory cleanup ({cleanup_level}): minimal cleanup needed")
                        
                    # Emergency action if memory is still very high
                    if memory_after > 10000:  # > 10GB after cleanup
                        logger.error(f"🚨 CRITICAL: Memory still very high after cleanup: {memory_after:.1f}MB")
                        # Could trigger more drastic measures here
                        
            except asyncio.CancelledError:
                logger.info("Aggressive memory cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in aggressive memory cleanup: {e}")
                await asyncio.sleep(30)  # Shorter retry on error

    def _comprehensive_memory_cleanup(self, context: str = "general"):
        """Enhanced comprehensive memory cleanup with aggressive module cache clearing."""
        
        if not PSUTIL_AVAILABLE:
            logger.debug("psutil not available for memory cleanup")
            return 0
        
        try:
            process = psutil.Process()
            memory_before = process.memory_info().rss / (1024 * 1024)
        except Exception:
            memory_before = 0
        
        try:
            # 1. Clear exception tracebacks (major memory holder)
            import sys
            sys.last_traceback = None
            sys.last_type = None 
            sys.last_value = None
            
            # 2. AGGRESSIVE MODULE CACHE CLEANUP - This is the major fix
            try:
                import sys
                modules_cleared = 0
                cache_objects_cleared = 0
                
                # Target problematic modules that accumulate caches (substrate/scalecodec are major offenders)
                target_modules = [
                    'substrate', 'scalecodec', 'scale_info', 'metadata', 'fiber', 'substrateinterface',
                    'xarray', 'dask', 'numpy', 'pandas', 'fsspec', 'zarr', 
                    'netcdf4', 'h5py', 'scipy', 'era5', 'gcsfs', 'cloudpickle',
                    'numcodecs', 'blosc', 'lz4', 'snappy', 'zstd'
                ]
                
                for module_name in list(sys.modules.keys()):
                    if any(pattern in module_name.lower() for pattern in target_modules):
                        module = sys.modules.get(module_name)
                        if hasattr(module, '__dict__'):
                            module_cleared = False
                            
                            # Clear all cache-like attributes
                            for attr_name in list(module.__dict__.keys()):
                                if any(cache_pattern in attr_name.lower() for cache_pattern in 
                                       ['cache', 'registry', '_cached', '__pycache__', '_instance_cache', 
                                        '_memo', '_lru', '_registry', '_store', '_buffer']):
                                    try:
                                        cache_obj = getattr(module, attr_name)
                                        if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                            cache_obj.clear()
                                            cache_objects_cleared += 1
                                            module_cleared = True
                                        elif isinstance(cache_obj, dict):
                                            cache_obj.clear()
                                            cache_objects_cleared += 1
                                            module_cleared = True
                                        elif isinstance(cache_obj, list):
                                            cache_obj.clear()
                                            cache_objects_cleared += 1
                                            module_cleared = True
                                        elif isinstance(cache_obj, set):
                                            cache_obj.clear()
                                            cache_objects_cleared += 1
                                            module_cleared = True
                                    except Exception:
                                        pass
                            
                            if module_cleared:
                                modules_cleared += 1
                
                if modules_cleared > 0:
                    logger.info(f"Module cache cleanup: cleared {cache_objects_cleared} cache objects from {modules_cleared} modules")
                    
            except Exception as e:
                logger.debug(f"Error during module cache cleanup: {e}")
            
            # 3. Clear specific library caches that are known problematic
            try:
                # Clear NumPy caches
                import numpy as np
                if hasattr(np, '_get_ndarray_cache'):
                    np._get_ndarray_cache().clear()
                if hasattr(np, 'core') and hasattr(np.core, 'multiarray'):
                    if hasattr(np.core.multiarray, '_reconstruct'):
                        # Clear numpy reconstruction cache
                        pass
                        
                # Clear xarray caches
                try:
                    import xarray as xr
                    if hasattr(xr.backends, 'plugins'):
                        if hasattr(xr.backends.plugins, 'clear'):
                            xr.backends.plugins.clear()
                        elif isinstance(xr.backends.plugins, dict):
                            xr.backends.plugins.clear()
                    if hasattr(xr, 'core') and hasattr(xr.core, 'formatting'):
                        if hasattr(xr.core.formatting, '_KNOWN_TYPE_REPRS'):
                            xr.core.formatting._KNOWN_TYPE_REPRS.clear()
                except ImportError:
                    pass
                    
                # Clear fsspec/gcsfs caches
                try:
                    import fsspec
                    if hasattr(fsspec, 'config') and hasattr(fsspec.config, 'conf'):
                        fsspec.config.conf.clear()
                    if hasattr(fsspec, 'filesystem') and hasattr(fsspec.filesystem, '_cache'):
                        fsspec.filesystem._cache.clear()
                    if hasattr(fsspec, 'registry') and hasattr(fsspec.registry, 'registry'):
                        # Don't clear registry as it's needed for functionality
                        pass
                except ImportError:
                    pass
                    
                # Clear netCDF4/HDF5 caches
                try:
                    import netCDF4
                    if hasattr(netCDF4, '_default_fillvals'):
                        netCDF4._default_fillvals.clear()
                    if hasattr(netCDF4, '_netCDF4'):
                        # Clear any internal caches
                        pass
                except ImportError:
                    pass
                    
            except Exception as e:
                logger.debug(f"Error clearing specific library caches: {e}")
            
            # 4. Clear HTTP client caches (only if they exist)
            try:
                if hasattr(self.validator, 'miner_client') and self.validator.miner_client and not self.validator.miner_client.is_closed:
                    if hasattr(self.validator.miner_client, '_transport'):
                        transport = self.validator.miner_client._transport
                        if hasattr(transport, '_pool') and hasattr(transport._pool, 'clear'):
                            transport._pool.clear()
                            
                if hasattr(self.validator, 'api_client') and self.validator.api_client and not self.validator.api_client.is_closed:
                    if hasattr(self.validator.api_client, '_transport'):
                        transport = self.validator.api_client._transport
                        if hasattr(transport, '_pool') and hasattr(transport._pool, 'clear'):
                            transport._pool.clear()
            except Exception as e:
                logger.debug(f"Error clearing HTTP client caches: {e}")
            
            # 5. Clear database connection pool caches (only if fully initialized)
            try:
                if hasattr(self.validator, 'database_manager') and getattr(self.validator, 'database_manager', None):
                    # Force database manager to clear any cached connections/results
                    if hasattr(self.validator.database_manager, '_engine') and self.validator.database_manager._engine:
                        engine = self.validator.database_manager._engine
                        if hasattr(engine, 'pool'):
                            pool = engine.pool
                            if hasattr(pool, 'invalidate'):
                                # Invalidate stale connections but don't clear active ones
                                pass
            except Exception as e:
                logger.debug(f"Error clearing database caches: {e}")
            
            # 6. Force multiple garbage collection passes with different strategies
            import gc
            collected_total = 0
            
            # First pass: standard collection
            collected = gc.collect()
            collected_total += collected
            
            # Second pass: collect generation 0 (youngest objects)
            collected = gc.collect(0)
            collected_total += collected
            
            # Third pass: collect all generations
            for generation in range(3):
                collected = gc.collect(generation)
                collected_total += collected
            
            # Clear Python's internal type cache
            try:
                if hasattr(sys, '_clear_type_cache'):
                    sys._clear_type_cache()
            except Exception:
                pass
            
            try:
                process = psutil.Process()
                memory_after = process.memory_info().rss / (1024 * 1024)
                memory_freed = memory_before - memory_after
                
                if memory_freed > 10:  # Only log significant memory freeing
                    logger.info(f"Comprehensive cleanup ({context}): freed {memory_freed:.1f}MB, GC collected {collected_total} objects")
                
                return memory_freed
            except Exception:
                return 0
                
        except Exception as e:
            logger.debug(f"Error in comprehensive memory cleanup: {e}")
            return 0

    def _aggressive_substrate_cleanup(self, context: str = "substrate_cleanup"):
        """Aggressive cleanup of substrate/scalecodec module caches."""
        try:
            import sys
            import gc
            
            substrate_modules_cleared = 0
            cache_objects_cleared = 0
            
            # Target substrate-related modules specifically
            substrate_patterns = [
                'substrate', 'scalecodec', 'scale_info', 'metadata', 
                'substrateinterface', 'scale_codec', 'polkadot'
            ]
            
            for module_name in list(sys.modules.keys()):
                if any(pattern in module_name.lower() for pattern in substrate_patterns):
                    module = sys.modules.get(module_name)
                    if hasattr(module, '__dict__'):
                        module_cleared = False
                        
                        # Clear all cache-like attributes in substrate modules
                        for attr_name in list(module.__dict__.keys()):
                            if any(cache_pattern in attr_name.lower() for cache_pattern in 
                                   ['cache', 'registry', '_cached', '_memo', '_lru', '_store', 
                                    '_type_registry', '_metadata', '_runtime', '_version']):
                                try:
                                    cache_obj = getattr(module, attr_name)
                                    if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                        cache_obj.clear()
                                        cache_objects_cleared += 1
                                        module_cleared = True
                                    elif isinstance(cache_obj, (dict, list, set)):
                                        cache_obj.clear()
                                        cache_objects_cleared += 1
                                        module_cleared = True
                                except Exception:
                                    pass
                        
                        if module_cleared:
                            substrate_modules_cleared += 1
            
            # Force garbage collection after substrate cleanup
            collected = gc.collect()
            
            if substrate_modules_cleared > 0:
                logger.info(f"Substrate cleanup ({context}): cleared {cache_objects_cleared} cache objects "
                          f"from {substrate_modules_cleared} substrate modules, GC collected {collected}")
                return cache_objects_cleared
            else:
                logger.debug(f"Substrate cleanup ({context}): no substrate caches found to clear")
                return 0
                
        except Exception as e:
            logger.debug(f"Error during aggressive substrate cleanup: {e}")
            return 0 