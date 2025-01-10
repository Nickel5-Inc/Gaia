import time
import asyncio
from typing import Dict, Optional, List
from datetime import datetime, timezone
from collections import deque
import psutil

from fiber.logging_utils import get_logger
from gaia.validator.utils.task_decorator import task_step
from gaia.validator.core.constants import (
    METRICS_COLLECTION_INTERVAL,
    METRICS_HISTORY_SIZE,
    METRICS_COLLECTION_TIMEOUT
)

logger = get_logger(__name__)

class MetricHistory:
    """Tracks historical values for a specific metric."""
    def __init__(self, name: str, max_size: int = METRICS_HISTORY_SIZE):
        self.name = name
        self.values = deque(maxlen=max_size)
        self.timestamps = deque(maxlen=max_size)
        
    def add_value(self, value: float):
        """Add a new value to the history."""
        self.values.append(value)
        self.timestamps.append(time.time())
        
    def get_average(self, window: Optional[int] = None) -> Optional[float]:
        """Get average value over specified window."""
        if not self.values:
            return None
            
        if window is None or window >= len(self.values):
            return sum(self.values) / len(self.values)
            
        recent_values = list(self.values)[-window:]
        return sum(recent_values) / len(recent_values)
        
    def get_trend(self, window: int = 10) -> Optional[float]:
        """Calculate trend over specified window."""
        if len(self.values) < 2:
            return None
            
        recent_values = list(self.values)[-window:]
        if len(recent_values) < 2:
            return None
            
        # Simple linear regression
        x = range(len(recent_values))
        y = recent_values
        n = len(x)
        
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(xi * yi for xi, yi in zip(x, y))
        sum_xx = sum(xi * xi for xi in x)
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
        return slope

class MetricsCollector:
    """
    Collects and tracks detailed performance metrics.
    
    This class handles:
    1. Performance metric collection
    2. Historical metric tracking
    3. Trend analysis
    4. Threshold monitoring
    5. Performance reporting
    """
    
    def __init__(
        self,
        database_manager,
        watchdog,
        status_monitor,
        custom_metrics: Optional[List[str]] = None
    ):
        self.database_manager = database_manager
        self.watchdog = watchdog
        self.status_monitor = status_monitor
        self._running = False
        
        # Initialize metric tracking
        self.metrics = {
            # System metrics
            "cpu_usage": MetricHistory("CPU Usage"),
            "memory_usage": MetricHistory("Memory Usage"),
            "disk_usage": MetricHistory("Disk Usage"),
            
            # Database metrics
            "db_queries": MetricHistory("Database Queries"),
            "db_latency": MetricHistory("Database Latency"),
            
            # Network metrics
            "network_latency": MetricHistory("Network Latency"),
            "network_errors": MetricHistory("Network Errors"),
            
            # Task metrics
            "task_success_rate": MetricHistory("Task Success Rate"),
            "task_duration": MetricHistory("Task Duration"),
            
            # Weight setting metrics
            "weight_set_duration": MetricHistory("Weight Set Duration"),
            "weight_set_failures": MetricHistory("Weight Set Failures")
        }
        
        # Add custom metrics if provided
        if custom_metrics:
            for metric in custom_metrics:
                if metric not in self.metrics:
                    self.metrics[metric] = MetricHistory(metric)
                    
        self.last_collection = time.time()
        self.collection_errors = 0

    async def start(self):
        """Start the metrics collector."""
        self._running = True
        await self.start_collection()

    async def stop(self):
        """Stop the metrics collector."""
        self._running = False

    async def start_collection(self):
        """Start collecting metrics."""
        while self._running:
            try:
                await self.collect_metrics()
                await asyncio.sleep(METRICS_COLLECTION_INTERVAL)
            except Exception as e:
                logger.error(f"Error in metrics collection: {e}")
                await asyncio.sleep(10)

    async def collect_metrics(self):
        """Collect and store metrics."""
        try:
            metrics = {
                "timestamp": datetime.now(timezone.utc),
                "system_metrics": await self._collect_system_metrics(),
                "network_metrics": await self._collect_network_metrics(),
                "task_metrics": await self._collect_task_metrics()
            }
            
            await self._store_metrics(metrics)
            return True
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            return False

    async def _collect_system_metrics(self):
        """Collect system metrics."""
        try:
            return {
                "cpu_percent": psutil.cpu_percent(),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_percent": psutil.disk_usage("/").percent
            }
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
            return {}

    async def _collect_network_metrics(self):
        """Collect network metrics."""
        try:
            return {
                "active_nodes": len(self.status_monitor.metagraph.nodes) if self.status_monitor.metagraph else 0,
                "connection_status": self.watchdog.check_health() if self.watchdog else False
            }
        except Exception as e:
            logger.error(f"Error collecting network metrics: {e}")
            return {}

    async def _collect_task_metrics(self):
        """Collect task-specific metrics."""
        try:
            return {
                "pending_tasks": await self._get_pending_task_count(),
                "completed_tasks": await self._get_completed_task_count()
            }
        except Exception as e:
            logger.error(f"Error collecting task metrics: {e}")
            return {}

    async def _get_db_query_count(self) -> int:
        """Get count of database queries in last interval."""
        try:
            # TODO: Implement actual query counting
            return 0
        except Exception:
            return 0

    def update_task_metrics(self, task_name: str, duration: float, success: bool):
        """Update metrics for a completed task."""
        try:
            self.metrics["task_duration"].add_value(duration)
            self.metrics["task_success_rate"].add_value(1.0 if success else 0.0)
            
            if task_name == "weight_setting":
                self.metrics["weight_set_duration"].add_value(duration)
                if not success:
                    current = self.metrics["weight_set_failures"].get_average(1) or 0
                    self.metrics["weight_set_failures"].add_value(current + 1)
                    
        except Exception as e:
            logger.error(f"Error updating task metrics: {e}")

    def get_metric_summary(self) -> Dict:
        """Get summary of all current metrics."""
        return {
            name: {
                "current": history.get_average(1),
                "average": history.get_average(),
                "trend": history.get_trend()
            }
            for name, history in self.metrics.items()
        }

    def _should_log_summary(self, metrics: Dict) -> bool:
        """Determine if metrics summary should be logged."""
        if self.collection_errors > 0:
            return True
            
        # Check for significant changes in key metrics
        for name, value in metrics.items():
            if name in self.metrics:
                avg = self.metrics[name].get_average()
                if avg is not None:
                    if abs(value - avg) / avg > 0.2:  # 20% change
                        return True
                        
        return False

    def _log_metrics_summary(self, current_metrics: Dict):
        """Log summary of current metrics."""
        try:
            summary = []
            for name, value in current_metrics.items():
                if name in self.metrics:
                    avg = self.metrics[name].get_average()
                    trend = self.metrics[name].get_trend()
                    
                    if avg is not None and trend is not None:
                        change = ((value - avg) / avg) * 100 if avg != 0 else 0
                        trend_symbol = "↑" if trend > 0 else "↓" if trend < 0 else "→"
                        
                        summary.append(
                            f"{name}: {value:.2f} "
                            f"(avg: {avg:.2f}, change: {change:+.1f}% {trend_symbol})"
                        )
                        
            if summary:
                logger.info(
                    "Metrics Summary:\n  - " + "\n  - ".join(summary)
                )
                
        except Exception as e:
            logger.error(f"Error logging metrics summary: {e}")

    async def _store_metrics(self, metrics: Dict):
        """Store collected metrics."""
        try:
            # TODO: Implement actual metrics storage
            pass
        except Exception as e:
            logger.error(f"Error storing metrics: {e}")

    async def _get_pending_task_count(self) -> int:
        """Get count of pending tasks."""
        try:
            # TODO: Implement actual pending task counting
            return 0
        except Exception:
            return 0

    async def _get_completed_task_count(self) -> int:
        """Get count of completed tasks."""
        try:
            # TODO: Implement actual completed task counting
            return 0
        except Exception:
            return 0 