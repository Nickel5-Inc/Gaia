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


class BackgroundWorkerManager:
    """Manages background workers and task health monitoring for the validator."""
    
    def __init__(self, validator_instance):
        self.validator = validator_instance
        self.args = validator_instance.args
        self.task_health = validator_instance.task_health
        self.watchdog_timeout = validator_instance.watchdog_timeout
        self.db_check_interval = validator_instance.db_check_interval
        self.metagraph_sync_interval = validator_instance.metagraph_sync_interval
        self.max_consecutive_errors = validator_instance.max_consecutive_errors
        self.watchdog_running = validator_instance.watchdog_running
        self._shutdown_event = validator_instance._shutdown_event
        self._background_tasks = validator_instance._background_tasks
        self._task_cleanup_lock = validator_instance._task_cleanup_lock
        self.memory_monitor_enabled = validator_instance.memory_monitor_enabled
        self.pm2_restart_enabled = validator_instance.pm2_restart_enabled
        self.memory_warning_threshold_mb = validator_instance.memory_warning_threshold_mb
        self.memory_emergency_threshold_mb = validator_instance.memory_emergency_threshold_mb
        self.memory_critical_threshold_mb = validator_instance.memory_critical_threshold_mb
        self.last_memory_log_time = validator_instance.last_memory_log_time
        self.memory_log_interval = validator_instance.memory_log_interval
        self.last_emergency_gc_time = validator_instance.last_emergency_gc_time
        self.emergency_gc_cooldown = validator_instance.emergency_gc_cooldown
        self.tracemalloc_snapshot1 = getattr(validator_instance, 'tracemalloc_snapshot1', None)

    async def start_watchdog(self):
        """Start the watchdog in a separate thread."""
        if not self.watchdog_running:
            self.watchdog_running = True
            logger.info("Started watchdog")
            
            # Log memory monitoring configuration
            if self.memory_monitor_enabled:
                try:
                    import psutil
                    system_memory = psutil.virtual_memory()
                    logger.info(f"🔍 Validator memory monitoring enabled:")
                    logger.info(f"  System memory: {system_memory.total / (1024**3):.1f} GB total, {system_memory.available / (1024**3):.1f} GB available")
                    logger.info(f"  Memory thresholds: Warning={self.memory_warning_threshold_mb}MB, Emergency={self.memory_emergency_threshold_mb}MB, Critical={self.memory_critical_threshold_mb}MB")
                    logger.info(f"  PM2 restart enabled: {self.pm2_restart_enabled}")
                    if self.pm2_restart_enabled:
                        logger.info("  🔄 ULTRA-AGGRESSIVE memory-based restarts are ENABLED")
                        logger.info(f"     - Emergency restart after 60s sustained pressure (>{self.memory_emergency_threshold_mb}MB)")
                        logger.info(f"     - Critical restart after 1s + GC attempt (>{self.memory_critical_threshold_mb}MB)")
                        logger.info(f"     - Weight setting only critical for 30s max")
                        logger.info(f"     - All other operations can be interrupted for memory safety")
                        # Check multiple possible PM2 environment variables
                        pm2_id = (os.getenv('pm_id') or 
                                 os.getenv('NODE_APP_INSTANCE') or 
                                 os.getenv('PM2_INSTANCE_ID') or 
                                 'not detected')
                        logger.info(f"  PM2 instance ID: {pm2_id}")
                        
                        # Also try to get the process name dynamically
                        try:
                            process_name = await self._get_pm2_process_name()
                            if process_name:
                                logger.info(f"  PM2 process name: {process_name}")
                            else:
                                logger.info(f"  PM2 process name: not detected")
                        except Exception as e:
                            logger.debug(f"Error getting PM2 process name during startup: {e}")
                except ImportError:
                    logger.warning("psutil not available - memory monitoring will be disabled")
                    self.memory_monitor_enabled = False
            else:
                logger.info("Validator memory monitoring disabled by configuration")
            
            asyncio.create_task(self._watchdog_loop())

    async def _watchdog_loop(self):
        """Run the watchdog monitoring in the main event loop."""
        while self.watchdog_running:
            try:
                # Add timeout to prevent long execution
                try:
                    await asyncio.wait_for(
                        self._watchdog_check(),
                        timeout=30  # 30 second timeout
                    )
                except asyncio.TimeoutError:
                    logger.error("Watchdog check timed out after 30 seconds")
                except Exception as e:
                    logger.error(f"Error in watchdog loop: {e}")
                    logger.error(traceback.format_exc())
            except Exception as outer_e:
                logger.error(f"Outer error in watchdog: {outer_e}")
                logger.error(traceback.format_exc())
            await asyncio.sleep(60)  # Check every minute

    async def stop_watchdog(self):
        """Stop the watchdog."""
        if self.watchdog_running:
            self.watchdog_running = False
            logger.info("Stopped watchdog")

    async def _watchdog_check(self):
        """Perform a single watchdog check iteration."""
        try:
            current_time = time.time()
            
            # Memory monitoring check (first priority)
            if self.memory_monitor_enabled:
                try:
                    await asyncio.wait_for(
                        self._check_memory_usage(current_time),
                        timeout=5  # 5 second timeout for memory check
                    )
                except asyncio.TimeoutError:
                    logger.error("Memory usage check timed out")
                except Exception as e:
                    logger.error(f"Error checking memory usage: {e}")
            
            # Update resource usage for all active tasks
            try:
                await asyncio.wait_for(
                    self._check_resource_usage(current_time),
                    timeout=10  # 10 second timeout
                )
            except asyncio.TimeoutError:
                logger.error("Resource usage check timed out")
            except Exception as e:
                logger.error(f"Error checking resource usage: {e}")

            # Check task health with timeout
            try:
                await asyncio.wait_for(
                    self._check_task_health(current_time),
                    timeout=10  # 10 second timeout
                )
            except asyncio.TimeoutError:
                logger.error("Task health check timed out")
            except Exception as e:
                logger.error(f"Error checking task health: {e}")
            
            # Check metagraph sync health with timeout
            if current_time - self.validator.last_metagraph_sync > self.metagraph_sync_interval:
                try:
                    await asyncio.wait_for(
                        self.validator._sync_metagraph(),
                        timeout=10  # 10 second timeout
                    )
                except asyncio.TimeoutError:
                    logger.error("Metagraph sync timed out")
                except Exception as e:
                    logger.error(f"Metagraph sync failed: {e}")

        except Exception as e:
            logger.error(f"Error in watchdog check: {e}")
            logger.error(traceback.format_exc())

    # ... existing code ... 