"""
Custom Loguru-based Logger for Gaia
Replaces fiber.logging_utils.get_logger with enhanced functionality:
- Worker-specific colors and prefixes
- Method/import chain/code line tracking
- Better timestamp formatting
- Fun colors and improved readability
"""

import sys
import os
import multiprocessing as mp
from typing import Optional, Dict, Any
from loguru import logger
import inspect
from pathlib import Path
import logging

# Early monkey-patch to silence fiber logging noise
try:
    # Try to import and patch fiber's get_logger before it's used
    import fiber.logging_utils
    
    # Store original function
    _original_fiber_get_logger = fiber.logging_utils.get_logger
    
    def _silent_fiber_get_logger(name: str):
        """Replacement fiber get_logger that returns silent loggers for noisy modules."""
        noisy_modules = [
            "chain_utils", "interface", "weights", "fetch_nodes", "signatures",
            "utils", "client", "handshake", "metagraph"
        ]
        
        if any(noisy_module in name for noisy_module in noisy_modules):
            # Return a logger that only logs warnings and above
            silent_logger = _original_fiber_get_logger(name)
            silent_logger.setLevel(logging.WARNING)
            return silent_logger
        
        # Return normal logger for other modules
        return _original_fiber_get_logger(name)
    
    # Monkey patch fiber's get_logger
    fiber.logging_utils.get_logger = _silent_fiber_get_logger
    
except ImportError:
    # fiber not available yet, that's fine
    pass
except Exception:
    # Any other error, ignore and continue
    pass


# ANSI color codes for workers (avoid loguru tag conflicts)
WORKER_COLORS = [
    "\033[36m",    # Cyan - Worker 1
    "\033[32m",    # Green - Worker 2  
    "\033[33m",    # Yellow - Worker 3
    "\033[35m",    # Magenta - Worker 4
    "\033[34m",    # Blue - Worker 5
    "\033[31m",    # Red - Worker 6
    "\033[37m",    # White - Worker 7
    "\033[96m",    # Light Cyan - Worker 8
    "\033[92m",    # Light Green - Worker 9
    "\033[93m",    # Light Yellow - Worker 10+
]

# ANSI color codes for levels
LEVEL_COLORS = {
    "TRACE": "\033[2m",      # Dim
    "DEBUG": "\033[34m",     # Blue
    "INFO": "\033[32m",      # Green
    "SUCCESS": "\033[1m\033[32m",  # Bold Green
    "WARNING": "\033[33m",   # Yellow
    "ERROR": "\033[31m",     # Red
    "CRITICAL": "\033[1m\033[31m", # Bold Red
}

# Reset code
RESET = "\033[0m"

# Global registry to track worker colors
_worker_color_registry: Dict[str, str] = {}
_next_worker_color_index = 0


def _get_worker_info() -> tuple[Optional[str], Optional[str]]:
    """Get current worker process name and assigned color."""
    global _worker_color_registry, _next_worker_color_index
    
    try:
        current_process = mp.current_process()
        if current_process:
            process_name = current_process.name
            
            if process_name == "MainProcess":
                return "MAIN", "\033[1m"  # Main gets bold ANSI code
            
            # Parse worker names like "worker-3/9" or "weather-w3/9" (legacy)
            if ("-w" in process_name or "worker-" in process_name) and "/" in process_name:
                # Extract worker number and total from names like "worker-3/9" or "weather-w3/9"
                try:
                    if "worker-" in process_name:
                        parts = process_name.split("worker-")
                        if len(parts) == 2:
                            worker_part = parts[1]  # "3/9"
                            worker_display = f"WORKER {worker_part}"  # "WORKER 3/9"
                    else:
                        # Legacy weather-w format
                        parts = process_name.split("-w")
                        if len(parts) == 2:
                            worker_part = parts[1]  # "3/9"
                            worker_display = f"WORKER {worker_part}"  # "WORKER 3/9"
                    
                    # Deterministic color assignment based on worker number (for both formats)
                    if 'worker_part' in locals():
                        worker_num_str = worker_part.split("/")[0]  # "3"
                        worker_num = int(worker_num_str) - 1  # 0-based index
                        color_index = worker_num % len(WORKER_COLORS)
                        worker_color = WORKER_COLORS[color_index]
                        
                        return worker_display, worker_color
                except Exception:
                    pass
            
            # Fallback for other process names
            if process_name not in _worker_color_registry:
                color_index = _next_worker_color_index % len(WORKER_COLORS)
                _worker_color_registry[process_name] = WORKER_COLORS[color_index]
                _next_worker_color_index += 1
            
            worker_color = _worker_color_registry[process_name]
            return process_name, worker_color
    except Exception:
        pass
    
    return None, None


def _get_caller_info(record: Dict[str, Any]) -> str:
    """Get caller method/function and file info from loguru record."""
    try:
        # Use loguru's built-in file/function/line info
        filename = record["file"].path
        func_name = record["function"] 
        line_no = record["line"]
        
            # Get relative path for cleaner display and escape problematic characters
        try:
            rel_path = Path(filename).relative_to(Path.cwd())
            file_display = str(rel_path)
        except ValueError:
            file_display = Path(filename).name
        
        # Escape problematic characters in file path that could be interpreted as color tags
        file_display = file_display.replace('<', '&lt;').replace('>', '&gt;')
        func_name = str(func_name).replace('<', '&lt;').replace('>', '&gt;')
        
        # Format: module:function:line
        return f"{file_display}:{func_name}:{line_no}"
        
    except Exception:
        return "unknown:unknown:0"


def _format_log_record(record: Dict[str, Any]) -> str:
    """Custom formatter with ANSI colors that work reliably."""
    # Get worker info
    worker_name, worker_color = _get_worker_info()
    
    # Get caller info (without escaping < > since we're not using loguru tags)
    caller_info = _get_caller_info(record)
    
    # Base timestamp and level
    timestamp = record["time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    level = record["level"].name
    
    # Apply colors using ANSI codes
    if worker_name and worker_color:
        worker_prefix = f"{worker_color}[{worker_name}]{RESET}"
    elif worker_name:
        worker_prefix = f"[{worker_name}]"
    else:
        worker_prefix = f"\033[1m[MAIN]{RESET}"  # Bold for main
    
    # Color the level
    level_color = LEVEL_COLORS.get(level, "")
    if level_color:
        level_colored = f"{level_color}{level:<8}{RESET}"
    else:
        level_colored = f"{level:<8}"
    
    # Dim the timestamp and caller info
    dim_color = "\033[2m"
    timestamp_colored = f"{dim_color}{timestamp}{RESET}"
    caller_colored = f"{dim_color}{caller_info}{RESET}"
    
    # Clean message (only escape loguru-specific characters)
    message = str(record['message']).replace('{', '{{').replace('}', '}}')
    
    # Build colorful log line
    log_line = f"{worker_prefix} {timestamp_colored} | {level_colored} | {caller_colored} - {message}\n"
    
    return log_line


class CustomLogger:
    """
    Custom logger wrapper that provides the same interface as fiber's get_logger
    but with enhanced functionality using loguru.
    """
    
    def __init__(self, name: str):
        self.name = name
        self._logger = logger
        
        # Configure loguru if not already configured
        if not hasattr(logger, '_gaia_configured'):
            self._configure_loguru()
            logger._gaia_configured = True
    
    def _configure_loguru(self):
        """Configure loguru with our custom format and settings."""
        # Remove default handler
        logger.remove()
        
        # Get log level from environment (default to INFO to reduce noise)
        log_level = os.getenv("LOGGING_LEVEL", "INFO").upper()
        
        # Filter function to silence noisy logs
        def filter_noise(record):
            """Filter out noisy logs while keeping useful ones."""
            message = record["message"]
            module = record["name"]
            level = record["level"].name
            
            # Silence specific fiber noise patterns
            if any(pattern in message for pattern in [
                "Logging mode is DEBUG",
                "Logging mode is INFO", 
                "Logging mode is WARNING",
                "Logging mode is ERROR",
            ]):
                return False
            
            # Silence verbose database session logs (only at DEBUG level)
            if level == "DEBUG" and any(db_noise in message for db_noise in [
                "Session new_",
                "Transaction committed",
                "New session closed",
                "Released. Total time:",
                "Active sessions:",
                "ready. Active sessions:",
                "Factory init:",
                "In yield:",
                "Release code:",
            ]):
                return False
            
            # Silence fiber internal module setup logs
            if any(noisy_module in module for noisy_module in [
                "chain_utils",
                "interface", 
                "weights",
                "fetch_nodes",
                "signatures",
                "utils",
                "client",
                "handshake",
                "metagraph",
            ]) and any(noise in message for noise in [
                "get_logger",
                "Logging mode",
                "Logger initialized",
            ]):
                return False
                
            return True
        
        # PM2-FRIENDLY LOGGING: Split logs by severity for proper PM2 color coding
        # Add handler for INFO and below to stdout (normal color in PM2)
        def stdout_filter(record):
            """Filter for stdout: INFO, DEBUG, SUCCESS, TRACE levels (normal color in PM2)."""
            level_no = record["level"].no
            return level_no < 30 and filter_noise(record)  # Less than WARNING
        
        logger.add(
            sys.stdout,
            format=_format_log_record,
            level="TRACE",
            colorize=True,  # Enable colorization for our manual color tags
            filter=stdout_filter,
        )
        
        # Add handler for WARNING and above to stderr (red color in PM2 for actual issues)
        def stderr_filter(record):
            """Filter for stderr: WARNING, ERROR, CRITICAL levels (red color in PM2)."""
            level_no = record["level"].no
            return level_no >= 30 and filter_noise(record)  # WARNING and above
        
        logger.add(
            sys.stderr,
            format=_format_log_record,
            level="WARNING",
            colorize=True,  # Enable colorization for our manual color tags
            backtrace=True,
            diagnose=True,
            catch=True,
            filter=stderr_filter,
        )
        
        # Add file handler for persistent logging
        log_file = os.getenv("LOG_FILE", "logs/gaia.log")
        if log_file:
            try:
                # Ensure log directory exists
                log_dir = os.path.dirname(log_file)
                os.makedirs(log_dir, exist_ok=True)
                
                # Create initial log file if it doesn't exist
                if not os.path.exists(log_file):
                    Path(log_file).touch()
                
                logger.add(
                    log_file,
                    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} - {message}",
                    level="DEBUG",  # File logging captures more detail
                    rotation="100 MB",
                    retention="7 days",
                    compression="gz",
                    catch=True,  # Catch exceptions during logging to prevent crashes
                    enqueue=True,  # Use async logging to prevent blocking
                )
            except Exception as e:
                # Fallback to console-only logging if file logging fails
                pass
        
        # Intercept standard Python logging to filter fiber noise
        self._setup_standard_logging_intercept()
        
        # Also try to intercept fiber's own logging
        self._setup_fiber_logging_intercept()
        
        # Mark logger as configured to avoid duplicate setup
        logger._gaia_configured = True
    
    def _setup_standard_logging_intercept(self):
        """Setup interception of standard Python logging to filter fiber noise."""
        class FiberNoiseFilter(logging.Filter):
            """Filter to silence noisy fiber internal logs."""
            
            def filter(self, record):
                # Silence specific fiber noise patterns
                if hasattr(record, 'getMessage'):
                    message = record.getMessage()
                    if any(pattern in message for pattern in [
                        "Logging mode is DEBUG",
                        "Logging mode is INFO",
                        "Logging mode is WARNING", 
                        "Logging mode is ERROR",
                        "Logger initialized",
                    ]):
                        return False
                
                # Silence noisy fiber modules
                if any(noisy_module in record.name for noisy_module in [
                    "chain_utils",
                    "interface",
                    "weights", 
                    "fetch_nodes",
                    "signatures",
                ]):
                    if hasattr(record, 'getMessage'):
                        message = record.getMessage()
                        if any(noise in message for noise in [
                            "Logging mode",
                            "get_logger",
                        ]):
                            return False
                
                return True
        
        # Apply filter to root logger to catch all standard logging
        root_logger = logging.getLogger()
        root_logger.addFilter(FiberNoiseFilter())
    
    def _setup_fiber_logging_intercept(self):
        """Try to silence fiber's internal logging by setting levels."""
        try:
            # Set specific fiber loggers to WARNING level to reduce noise
            noisy_loggers = [
                "chain_utils",
                "interface", 
                "weights",
                "fetch_nodes",
                "signatures",
                "utils",
                "client",
                "handshake",
                "metagraph",
            ]
            
            for logger_name in noisy_loggers:
                fiber_logger = logging.getLogger(logger_name)
                fiber_logger.setLevel(logging.WARNING)  # Only show warnings and above
                
        except Exception as e:
            # Ignore if we can't intercept fiber logging
            pass
    
    # Implement the same interface as fiber's logger with proper caller detection
    def debug(self, message: str, *args, **kwargs):
        self._logger.opt(depth=1).debug(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs):
        self._logger.opt(depth=1).info(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs):
        self._logger.opt(depth=1).warning(message, *args, **kwargs)
    
    def warn(self, message: str, *args, **kwargs):
        # Alias for warning to match standard logging
        self._logger.opt(depth=1).warning(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs):
        self._logger.opt(depth=1).error(message, *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs):
        self._logger.opt(depth=1).critical(message, *args, **kwargs)
    
    def exception(self, message: str, *args, **kwargs):
        # Log with exception traceback
        self._logger.opt(depth=1).exception(message, *args, **kwargs)
    
    def success(self, message: str, *args, **kwargs):
        # Loguru's special success level
        self._logger.opt(depth=1).success(message, *args, **kwargs)
    
    def trace(self, message: str, *args, **kwargs):
        # Loguru's trace level (more verbose than debug)
        self._logger.opt(depth=1).trace(message, *args, **kwargs)
    
    # Properties to match standard logger interface
    @property
    def level(self):
        return self._logger.level
    
    def setLevel(self, level):
        """Set logging level (for compatibility)."""
        # Note: loguru handles levels differently, but we can update the handler
        pass
    
    def addHandler(self, handler):
        """Add handler (for compatibility with existing code)."""
        # loguru handles this differently, but we'll ignore for now
        pass
    
    def log(self, level, message, *args, **kwargs):
        """Log method for compatibility with standard logging interface."""
        # Convert numeric levels to loguru levels with proper depth
        if isinstance(level, int):
            if level >= 50:
                self._logger.opt(depth=1).critical(message, *args, **kwargs)
            elif level >= 40:
                self._logger.opt(depth=1).error(message, *args, **kwargs)
            elif level >= 30:
                self._logger.opt(depth=1).warning(message, *args, **kwargs)
            elif level >= 20:
                self._logger.opt(depth=1).info(message, *args, **kwargs)
            else:
                self._logger.opt(depth=1).debug(message, *args, **kwargs)
        else:
            # String level
            level_str = str(level).upper()
            if level_str == "CRITICAL":
                self._logger.opt(depth=1).critical(message, *args, **kwargs)
            elif level_str == "ERROR":
                self._logger.opt(depth=1).error(message, *args, **kwargs)
            elif level_str == "WARNING":
                self._logger.opt(depth=1).warning(message, *args, **kwargs)
            elif level_str == "INFO":
                self._logger.opt(depth=1).info(message, *args, **kwargs)
            else:
                self._logger.opt(depth=1).debug(message, *args, **kwargs)


# Cache to store logger instances (like fiber does)
_logger_cache: Dict[str, CustomLogger] = {}

# Silent logger for noisy fiber modules
class SilentLogger:
    """Silent logger that suppresses noisy fiber internal logs."""
    
    def debug(self, *args, **kwargs): pass
    def info(self, *args, **kwargs): pass  
    def warning(self, *args, **kwargs): pass
    def warn(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass
    def critical(self, *args, **kwargs): pass
    def exception(self, *args, **kwargs): pass
    def success(self, *args, **kwargs): pass
    def trace(self, *args, **kwargs): pass
    def setLevel(self, *args, **kwargs): pass
    def addHandler(self, *args, **kwargs): pass
    def log(self, *args, **kwargs): pass
    
    @property
    def level(self): return "WARNING"


def get_logger(name: str):
    """
    Drop-in replacement for fiber.logging_utils.get_logger
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        CustomLogger instance with enhanced functionality, or SilentLogger for noisy modules
    """
    # Return silent logger for noisy fiber modules
    noisy_modules = [
        "chain_utils",
        "interface", 
        "weights",
        "fetch_nodes", 
        "signatures",
        "utils",
        "client",
        "handshake",
        "metagraph",
    ]
    
    # Check if this is a noisy fiber module
    if any(noisy_module in name for noisy_module in noisy_modules):
        return SilentLogger()
    
    # Return full logger for everything else
    if name not in _logger_cache:
        _logger_cache[name] = CustomLogger(name)
    
    return _logger_cache[name]


# Convenience function for testing
def test_logger():
    """Test the custom logger with different scenarios."""
    test_log = get_logger("test_module")
    
    test_log.debug("This is a debug message")
    test_log.info("This is an info message") 
    test_log.success("This is a success message")
    test_log.warning("This is a warning message")
    test_log.error("This is an error message")
    test_log.critical("This is a critical message")
    
    try:
        raise ValueError("Test exception")
    except Exception:
        test_log.exception("This is an exception with traceback")


if __name__ == "__main__":
    test_logger()
