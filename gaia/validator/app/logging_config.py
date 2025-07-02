"""
Centralized logging configuration for Gaia Validator v4.0.

This module provides structured logging using structlog with consistent
formatting, context management, and performance optimizations.
"""

import structlog
import logging
import logging.handlers
import sys
import os
from pathlib import Path
from typing import Optional, Dict, Any

from structlog.contextvars import bind_contextvars, clear_contextvars


def setup_logging(
    log_level: str = "INFO",
    log_format: str = "json",
    log_file: Optional[str] = None,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> None:
    """
    Set up structured logging for the Gaia Validator.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Output format ("json" or "console")
        log_file: Optional log file path. If None, logs to stdout only.
        max_file_size: Maximum size of each log file before rotation
        backup_count: Number of backup files to keep
    """
    # Clear any existing configuration
    logging.getLogger().handlers.clear()
    structlog.reset_defaults()
    clear_contextvars()
    
    # Configure standard library logging
    log_level_obj = getattr(logging, log_level.upper())
    
    # Create formatters
    if log_format == "json":
        processor_chain = [
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.CallsiteParameterAdder(
                parameters=[structlog.processors.CallsiteParameter.FILENAME,
                           structlog.processors.CallsiteParameter.FUNC_NAME,
                           structlog.processors.CallsiteParameter.LINENO]
            ),
            structlog.processors.JSONRenderer()
        ]
    else:
        processor_chain = [
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=True),
            structlog.dev.ConsoleRenderer(colors=True)
        ]
    
    # Configure handlers
    handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level_obj)
    handlers.append(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_file_size,
            backupCount=backup_count
        )
        file_handler.setLevel(log_level_obj)
        handlers.append(file_handler)
    
    # Configure standard library logging
    logging.basicConfig(
        level=log_level_obj,
        handlers=handlers,
        format="%(message)s"
    )
    
    # Configure structlog
    structlog.configure(
        processors=processor_chain,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = __name__) -> structlog.BoundLogger:
    """
    Get a structured logger instance.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)


def bind_context(**kwargs) -> None:
    """
    Bind context variables to all subsequent log messages in this execution context.
    
    Args:
        **kwargs: Key-value pairs to bind to logging context
    """
    bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear all bound context variables."""
    clear_contextvars()


class LoggingContext:
    """
    Context manager for scoped logging context.
    
    Example:
        with LoggingContext(job_id="123", miner_uid=42):
            log.info("Processing job")  # Will include job_id and miner_uid
    """
    
    def __init__(self, **kwargs):
        self.context = kwargs
        self.previous_context = None
    
    def __enter__(self):
        # Save current context (if any)
        try:
            from structlog.contextvars import get_contextvars
            self.previous_context = get_contextvars()
        except:
            self.previous_context = {}
        
        # Bind new context
        bind_contextvars(**self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clear current context
        clear_contextvars()
        
        # Restore previous context
        if self.previous_context:
            bind_contextvars(**self.previous_context)


class PerformanceLogger:
    """
    Specialized logger for performance metrics and monitoring.
    """
    
    def __init__(self, name: str = "performance"):
        self.log = get_logger(name)
    
    def log_timing(
        self, 
        operation: str, 
        duration_ms: float, 
        **context
    ) -> None:
        """Log operation timing."""
        self.log.info(
            "operation_timing",
            operation=operation,
            duration_ms=duration_ms,
            **context
        )
    
    def log_memory_usage(
        self, 
        process_type: str, 
        rss_mb: float, 
        vms_mb: float,
        **context
    ) -> None:
        """Log memory usage."""
        self.log.info(
            "memory_usage",
            process_type=process_type,
            rss_mb=rss_mb,
            vms_mb=vms_mb,
            **context
        )
    
    def log_queue_metrics(
        self, 
        queue_name: str, 
        depth: int, 
        max_size: int,
        **context
    ) -> None:
        """Log queue depth metrics."""
        self.log.info(
            "queue_metrics",
            queue_name=queue_name,
            depth=depth,
            max_size=max_size,
            utilization_pct=(depth / max_size * 100) if max_size > 0 else 0,
            **context
        )
    
    def log_compute_job(
        self,
        job_id: str,
        task_name: str,
        success: bool,
        duration_ms: float,
        worker_pid: int,
        **context
    ) -> None:
        """Log compute job completion."""
        self.log.info(
            "compute_job_completed",
            job_id=job_id,
            task_name=task_name,
            success=success,
            duration_ms=duration_ms,
            worker_pid=worker_pid,
            **context
        )


class SecurityLogger:
    """
    Specialized logger for security events and audit trails.
    """
    
    def __init__(self, name: str = "security"):
        self.log = get_logger(name)
    
    def log_authentication(
        self, 
        event: str, 
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        success: bool = True,
        **context
    ) -> None:
        """Log authentication events."""
        self.log.info(
            "authentication_event",
            event=event,
            user_id=user_id,
            ip_address=ip_address,
            success=success,
            **context
        )
    
    def log_access_attempt(
        self,
        resource: str,
        action: str,
        user_id: Optional[str] = None,
        allowed: bool = True,
        **context
    ) -> None:
        """Log resource access attempts."""
        self.log.info(
            "access_attempt",
            resource=resource,
            action=action,
            user_id=user_id,
            allowed=allowed,
            **context
        )
    
    def log_data_access(
        self,
        data_type: str,
        operation: str,
        miner_hotkey: Optional[str] = None,
        **context
    ) -> None:
        """Log sensitive data access."""
        self.log.info(
            "data_access",
            data_type=data_type,
            operation=operation,
            miner_hotkey=miner_hotkey,
            **context
        )


# Pre-configured logger instances
performance_log = PerformanceLogger()
security_log = SecurityLogger()

# Default application logger
log = get_logger("gaia.validator")


def configure_from_environment() -> None:
    """
    Configure logging from environment variables.
    
    Environment variables:
        LOG_LEVEL: Logging level (default: INFO)
        LOG_FORMAT: Output format - json or console (default: json)
        LOG_FILE: Log file path (optional)
        LOG_MAX_SIZE: Maximum log file size in bytes (default: 10MB)
        LOG_BACKUP_COUNT: Number of backup files (default: 5)
    """
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_format = os.getenv("LOG_FORMAT", "json")
    log_file = os.getenv("LOG_FILE")
    max_size = int(os.getenv("LOG_MAX_SIZE", 10 * 1024 * 1024))
    backup_count = int(os.getenv("LOG_BACKUP_COUNT", 5))
    
    setup_logging(
        log_level=log_level,
        log_format=log_format,
        log_file=log_file,
        max_file_size=max_size,
        backup_count=backup_count
    )


# Example usage and patterns
def example_usage():
    """
    Example usage patterns for the logging system.
    """
    # Basic logging
    log.info("Application starting", version="4.0", config_loaded=True)
    
    # Scoped context
    with LoggingContext(run_id=123, process="supervisor"):
        log.info("Creating weather forecast run")
        
        with LoggingContext(miner_uid=42, miner_hotkey="abc123"):
            log.info("Processing miner response")
    
    # Performance logging
    performance_log.log_timing("gfs_hash_computation", 850.5, cache_hit=False)
    performance_log.log_memory_usage("compute_worker", 256.7, 512.4, worker_id=1)
    
    # Security logging
    security_log.log_authentication("login_attempt", user_id="validator_1", success=True)
    security_log.log_data_access("weather_forecast", "read", miner_hotkey="abc123")
    
    # Bound context
    bind_context(session_id="sess_456", request_id="req_789")
    log.info("Processing request")  # Will include session_id and request_id
    clear_context()


if __name__ == "__main__":
    # Demo configuration
    setup_logging(log_level="DEBUG", log_format="console")
    example_usage()