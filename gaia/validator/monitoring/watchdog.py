import time
import asyncio
from typing import Dict, Optional, List
from datetime import datetime, timezone

from fiber.logging_utils import get_logger
from fiber.chain.metagraph import Metagraph
from fiber.chain import interface
from substrateinterface import SubstrateInterface
from gaia.validator.utils.task_decorator import task_step
from gaia.validator.core.constants import (
    WATCHDOG_TIMEOUT,
    WATCHDOG_SLEEP,
    STATUS_SLEEP,
    NETWORK_OP_TIMEOUT
)

logger = get_logger(__name__)

# Constants
WATCHDOG_CHECK_INTERVAL = 60  # Check components every 60 seconds

class ComponentHealth:
    """Tracks health status of a validator component."""
    def __init__(self, name: str, timeout: int):
        self.name = name
        self.timeout = timeout
        self.last_success = time.time()
        self.last_check = time.time()
        self.consecutive_failures = 0
        self.total_failures = 0
        self.is_healthy = True

    def update_success(self):
        """Record a successful operation."""
        self.last_success = time.time()
        self.last_check = time.time()
        self.consecutive_failures = 0
        self.is_healthy = True

    def update_failure(self):
        """Record a failed operation."""
        self.last_check = time.time()
        self.consecutive_failures += 1
        self.total_failures += 1
        if self.consecutive_failures >= 3:
            self.is_healthy = False

    def needs_check(self) -> bool:
        """Check if component needs health verification."""
        return time.time() - self.last_check > self.timeout

    def get_status(self) -> Dict:
        """Get current health status."""
        return {
            "name": self.name,
            "is_healthy": self.is_healthy,
            "last_success": datetime.fromtimestamp(self.last_success, tz=timezone.utc),
            "consecutive_failures": self.consecutive_failures,
            "total_failures": self.total_failures,
            "time_since_last_success": time.time() - self.last_success
        }

class Watchdog:
    """
    Monitors validator component health and handles recovery.
    
    This class handles:
    1. Component health tracking
    2. Automatic recovery attempts
    3. Health metrics collection
    4. Critical component monitoring
    5. Failure reporting
    """
    
    def __init__(self, database_manager, network_manager, status_monitor):
        self.database_manager = database_manager
        self.network_manager = network_manager  # Initialize network_manager
        self.status_monitor = status_monitor    # Initialize status_monitor
        self._running = False
        self.unhealthy_components = []

    async def start(self):
        """Start the watchdog."""
        self._running = True
        await self.start_monitoring()

    async def stop(self):
        """Stop the watchdog."""
        self._running = False

    async def start_monitoring(self):
        """Start monitoring components."""
        while self._running:
            try:
                await self.check_components()
                await asyncio.sleep(WATCHDOG_CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Error in watchdog monitoring: {e}")
                await asyncio.sleep(10)

    async def check_components(self):
        """Check health of all components."""
        try:
            self.unhealthy_components = []
            
            # Check database
            if not await self.check_database():
                self.unhealthy_components.append("database")
            
            # Check network
            if not await self.check_network():
                self.unhealthy_components.append("network")
            
            # Check status monitor
            if not await self.check_status_monitor():
                self.unhealthy_components.append("status_monitor")
            
            if self.unhealthy_components:
                logger.warning(f"Unhealthy components: {self.unhealthy_components}")
            
            return len(self.unhealthy_components) == 0
        except Exception as e:
            logger.error(f"Error checking components: {e}")
            return False

    async def check_database(self):
        """Check database health."""
        try:
            return await self.database_manager.check_connection()
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    async def check_network(self):
        """Check network health."""
        try:
            return self.network_manager.is_connected if self.network_manager else False
        except Exception as e:
            logger.error(f"Network health check failed: {e}")
            return False

    async def check_status_monitor(self):
        """Check status monitor health."""
        try:
            return await self.status_monitor.check_health() if self.status_monitor else False
        except Exception as e:
            logger.error(f"Status monitor health check failed: {e}")
            return False

    def check_health(self):
        """Check overall system health."""
        return len(self.unhealthy_components) == 0 