import time
import gc
import threading
import subprocess
import json
import sys
import os
import asyncio
from typing import Optional, Any, Dict, List
from contextlib import contextmanager
from fiber.chain.interface import get_substrate
from substrateinterface import SubstrateInterface
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class ProcessIsolatedSubstrate:
    """
    A process-isolated wrapper around SubstrateInterface that prevents ABC memory leaks.
    
    This class provides the exact same interface as SubstrateInterface but runs all operations
    in separate processes. When each process terminates, all ABC objects are automatically
    destroyed, preventing memory accumulation.
    
    This is a drop-in replacement - existing code works unchanged.
    """
    
    def __init__(self, subtensor_network: str, chain_endpoint: str = None):
        self.subtensor_network = subtensor_network
        self.chain_endpoint = chain_endpoint or ""
        self._operation_count = 0
        
        logger.debug(f"ProcessIsolatedSubstrate created for {subtensor_network}@{chain_endpoint}")
    
    @property
    def url(self) -> str:
        """URL property for compatibility with existing code."""
        if self.chain_endpoint and self.chain_endpoint.strip():
            return self.chain_endpoint.strip()
        
        # Construct default URL based on network
        if self.subtensor_network == "finney":
            return "wss://entrypoint-finney.opentensor.ai:443"
        elif self.subtensor_network == "test":
            return "wss://test.finney.opentensor.ai:443"
        elif self.subtensor_network == "local":
            return "ws://127.0.0.1:9944"
        else:
            return f"wss://{self.subtensor_network}.opentensor.ai:443"
    
    def _run_substrate_operation(self, operation_type: str, *args, timeout: float = 15.0) -> Any:
        """
        Run any substrate operation in an isolated process.
        
        Args:
            operation_type: Either 'query' or 'rpc_request'
            *args: Arguments for the operation
            timeout: Timeout in seconds
        """
        import inspect
        
        self._operation_count += 1
        
        # Get calling context for better logging
        frame = inspect.currentframe()
        caller_info = "unknown"
        try:
            # Go up the stack to find the actual caller (skip wrapper methods)
            caller_frame = frame.f_back.f_back if frame.f_back else None
            if caller_frame:
                caller_info = f"{caller_frame.f_code.co_filename.split('/')[-1]}:{caller_frame.f_code.co_name}:{caller_frame.f_lineno}"
        except:
            pass
        finally:
            del frame  # Prevent reference cycles
        
        # Create operation description
        if operation_type == "query" and len(args) >= 2:
            op_desc = f"query({args[0]}.{args[1]})"
            if len(args) > 2 and args[2]:  # params
                op_desc += f" with params"
            if len(args) > 3 and args[3]:  # block_hash
                op_desc += f" at block"
        elif operation_type == "rpc_request" and len(args) >= 1:
            op_desc = f"rpc({args[0]})"
        else:
            op_desc = f"{operation_type}"
        
        logger.debug(f"Process-isolated {op_desc} #{self._operation_count} (timeout: {timeout}s) called from {caller_info}")
        
        try:
            start_time = time.time()
            operation_desc = op_desc  # Store for completion logging
            
            # Create subprocess script that runs the operation
            if operation_type == "query":
                module, storage_function = args[0], args[1]
                params = args[2] if len(args) > 2 else []
                block_hash = args[3] if len(args) > 3 else None
                script_content = f'''
import sys
import signal
import json
import warnings
warnings.filterwarnings('ignore')

def timeout_handler(signum, frame):
    print(json.dumps(dict(error="Query timeout")))
    sys.exit(1)

try:
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm({max(int(timeout)-2, 5)})
    
    from fiber.chain.interface import get_substrate
    
    if "{self.chain_endpoint}" and "{self.chain_endpoint}".strip():
        substrate = get_substrate(subtensor_network="{self.subtensor_network}", subtensor_address="{self.chain_endpoint}")
    else:
        substrate = get_substrate(subtensor_network="{self.subtensor_network}")
    
    params = {repr(params)}
    block_hash = {repr(block_hash)}
    
    # Call substrate.query with appropriate parameters
    if params and block_hash is not None:
        result = substrate.query("{module}", "{storage_function}", params, block_hash)
    elif params:
        result = substrate.query("{module}", "{storage_function}", params)
    elif block_hash is not None:
        result = substrate.query("{module}", "{storage_function}", None, block_hash)
    else:
        result = substrate.query("{module}", "{storage_function}")
    
    # Extract value if it's a substrate result object
    if hasattr(result, 'value'):
        value = result.value
    else:
        value = result
    
    # Create a simple wrapper object that mimics substrate result structure for compatibility
    class SubstrateResultWrapper:
        def __init__(self, val):
            self.value = val
            self._value = val  # Some code might expect _value
            
        def __str__(self):
            return str(self.value)
            
        def __repr__(self):
            return repr(self.value)
            
        def __int__(self):
            return int(self.value) if hasattr(self.value, '__int__') else self.value
            
        def __index__(self):
            # Required for using SubstrateResult as list/array index
            if hasattr(self.value, '__index__'):
                return self.value.__index__()
            elif hasattr(self.value, '__int__'):
                return int(self.value)
            else:
                return int(self.value)
            
        def __len__(self):
            return len(self.value) if hasattr(self.value, '__len__') else 1
            
        def __getitem__(self, key):
            return self.value[key] if hasattr(self.value, '__getitem__') else self.value
            
        def __iter__(self):
            return iter(self.value) if hasattr(self.value, '__iter__') else iter([self.value])
        
    wrapped_result = SubstrateResultWrapper(value)
    
    substrate.close()
    signal.alarm(0)
    
    print("RESULT_START")
    print(json.dumps(dict(value=value, wrapped=True), default=str))
    print("RESULT_END")
    
except Exception as e:
    signal.alarm(0)
    print("RESULT_START")
    print(json.dumps(dict(error=str(e))))
    print("RESULT_END")
'''
            
            elif operation_type == "rpc_request":
                method = args[0]
                params = args[1] if len(args) > 1 else []
                script_content = f'''
import sys
import signal
import json
import warnings
warnings.filterwarnings('ignore')

def timeout_handler(signum, frame):
    print(json.dumps(dict(error="RPC timeout")))
    sys.exit(1)

try:
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm({max(int(timeout)-2, 5)})
    
    import time
    start_time = time.time()
    
    from fiber.chain.interface import get_substrate
    
    print(f"DEBUG: Connecting to substrate...", file=sys.stderr)
    connection_start = time.time()
    
    if "{self.chain_endpoint}" and "{self.chain_endpoint}".strip():
        substrate = get_substrate(subtensor_network="{self.subtensor_network}", subtensor_address="{self.chain_endpoint}")
    else:
        substrate = get_substrate(subtensor_network="{self.subtensor_network}")
    
    connection_time = time.time() - connection_start
    print(f"DEBUG: Connected in " + str(round(connection_time, 2)) + "s, making RPC request...", file=sys.stderr)
    
    params = {repr(params)}
    request_start = time.time()
    if params:
        result = substrate.rpc_request("{method}", params)
    else:
        result = substrate.rpc_request("{method}", [])
    
    request_time = time.time() - request_start
    print(f"DEBUG: RPC request completed in " + str(round(request_time, 2)) + "s", file=sys.stderr)
    
    substrate.close()
    signal.alarm(0)
    
    total_time = time.time() - start_time
    print(f"DEBUG: Total operation time: " + str(round(total_time, 2)) + "s", file=sys.stderr)
    
    print("RESULT_START")
    print(json.dumps(result, default=str))
    print("RESULT_END")
    
except Exception as e:
    signal.alarm(0)
    print("RESULT_START")
    print(json.dumps(dict(error=str(e))))
    print("RESULT_END")
'''
            else:
                raise ValueError(f"Unknown operation type: {operation_type}")
            
            # Run the script in a subprocess
            result = subprocess.run(
                [sys.executable, "-c", script_content],
                capture_output=True,
                text=True,
                timeout=timeout,
                env={**dict(os.environ), 'PYTHONUNBUFFERED': '1'}
            )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                # Log any debug output from subprocess
                if result.stderr and result.stderr.strip():
                    for debug_line in result.stderr.strip().split('\n'):
                        if debug_line.startswith('DEBUG:'):
                            logger.debug(f"Subprocess {operation_desc}: {debug_line}")
                
                # Extract result between markers
                stdout = result.stdout
                start_marker = "RESULT_START\n"
                end_marker = "\nRESULT_END"
                
                start_idx = stdout.find(start_marker)
                end_idx = stdout.find(end_marker)
                
                if start_idx >= 0 and end_idx >= 0:
                    json_content = stdout[start_idx + len(start_marker):end_idx].strip()
                else:
                    json_content = stdout.strip()
                
                if not json_content:
                    raise Exception("Empty response from subprocess")
                
                data = json.loads(json_content)
                if isinstance(data, dict) and "error" in data:
                    raise Exception(f"Substrate {operation_type} failed: {data['error']}")
                
                # Handle wrapped results from subprocess
                if isinstance(data, dict) and "wrapped" in data and data["wrapped"]:
                    # Create a substrate-compatible result object
                    class SubstrateResult:
                        def __init__(self, val):
                            self.value = val
                            self._value = val
                            
                        def __str__(self):
                            return str(self.value)
                            
                        def __repr__(self):
                            return repr(self.value)
                            
                        def __int__(self):
                            return int(self.value) if hasattr(self.value, '__int__') else self.value
                            
                        def __index__(self):
                            # Required for using SubstrateResult as list/array index
                            if hasattr(self.value, '__index__'):
                                return self.value.__index__()
                            elif hasattr(self.value, '__int__'):
                                return int(self.value)
                            else:
                                return int(self.value)
                            
                        def __len__(self):
                            return len(self.value) if hasattr(self.value, '__len__') else 1
                            
                        def __getitem__(self, key):
                            return self.value[key] if hasattr(self.value, '__getitem__') else self.value
                            
                        def __iter__(self):
                            return iter(self.value) if hasattr(self.value, '__iter__') else iter([self.value])
                        
                        # Comparison operators - delegate to underlying value
                        def __eq__(self, other):
                            return self.value == other
                            
                        def __ne__(self, other):
                            return self.value != other
                            
                        def __lt__(self, other):
                            return self.value < other
                            
                        def __le__(self, other):
                            return self.value <= other
                            
                        def __gt__(self, other):
                            return self.value > other
                            
                        def __ge__(self, other):
                            return self.value >= other
                        
                        # Arithmetic operators for completeness
                        def __add__(self, other):
                            return self.value + other
                            
                        def __sub__(self, other):
                            return self.value - other
                            
                        def __mul__(self, other):
                            return self.value * other
                            
                        def __truediv__(self, other):
                            return self.value / other
                            
                        def __floordiv__(self, other):
                            return self.value // other
                            
                        def __mod__(self, other):
                            return self.value % other
                            
                        def __hash__(self):
                            return hash(self.value)
                    
                    result_obj = SubstrateResult(data["value"])
                    logger.debug(f"Process-isolated {operation_desc} #{self._operation_count} completed in {execution_time:.2f}s → wrapped result")
                    return result_obj
                else:
                    logger.debug(f"Process-isolated {operation_desc} #{self._operation_count} completed in {execution_time:.2f}s → raw result")
                    return data
            else:
                # Log stderr for failed subprocess to help with debugging
                stderr_info = f": {result.stderr}" if result.stderr else ""
                raise Exception(f"Subprocess failed (code {result.returncode}){stderr_info}")
                
        except subprocess.TimeoutExpired:
            execution_time = time.time() - start_time if 'start_time' in locals() else 0
            logger.error(f"Process-isolated {operation_desc} #{self._operation_count} TIMED OUT after {execution_time:.2f}s")
            logger.error(f"Network appears to be unresponsive. Consider checking substrate network status.")
            raise Exception(f"Substrate {operation_type} timeout after {timeout}s")
        except Exception as e:
            execution_time = time.time() - start_time if 'start_time' in locals() else 0
            logger.error(f"Process-isolated {operation_desc} #{self._operation_count} failed after {execution_time:.2f}s: {e}")
            raise
    
    def query(self, module: str, storage_function: str, params: List = None, block_hash: str = None, timeout: float = 90.0) -> Any:
        """Query the substrate in an isolated process."""
        return self._run_substrate_operation("query", module, storage_function, params or [], block_hash, timeout=timeout)
    
    def rpc_request(self, method: str, params: List = None, timeout: float = 90.0) -> Any:
        """Make an RPC request in an isolated process."""
        return self._run_substrate_operation("rpc_request", method, params or [], timeout=timeout)
    
    def get_block(self, block_hash: str = None) -> Dict[str, Any]:
        """
        Get block information using process isolation.
        This provides compatibility with the original substrate interface.
        """
        try:
            if block_hash:
                result = self.rpc_request("chain_getBlock", [block_hash])
            else:
                result = self.rpc_request("chain_getBlock", [])
            
            # Handle different response structures more robustly
            if isinstance(result, dict):
                if "block" in result:
                    return result["block"]
                elif "header" in result:
                    # Already in the right format
                    return result
                else:
                    # Return full result if structure is unexpected
                    logger.debug(f"Unexpected get_block response structure: {list(result.keys())}")
                    return result
            else:
                logger.warning(f"get_block returned non-dict result: {type(result)}")
                return result
        except Exception as e:
            logger.error(f"Error in process-isolated get_block: {e}")
            raise
    
    def close(self):
        """Close method for compatibility (no-op since each operation uses its own process)."""
        logger.debug(f"ProcessIsolatedSubstrate close() called (no-op - {self._operation_count} operations completed)")
    
    def __getattr__(self, name: str):
        """Handle any other attributes that might be accessed."""
        if name in ['address', 'endpoint']:
            return self.url
        elif name in ['chain', 'network']:
            return self.subtensor_network
        else:
            # For unknown attributes, return None or raise AttributeError
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")


class SubstrateManager:
    """
    Substrate connection manager that prevents memory leaks with always-fresh connections.
    
    Key principles:
    1. ALWAYS create fresh connections (dispose old, create new) to prevent memory leaks
    2. Manager instance stays stable - only recreated when network changes
    3. Thorough cleanup of scalecodec caches when disposing connections  
    4. Thread-safe operations
    5. Handles endpoint changes gracefully without manager recreation
    6. OPTIONAL: Use process isolation for complete ABC memory leak prevention
    
    This manager prioritizes memory leak prevention over connection reuse.
    """
    
    def __init__(self, subtensor_network: str, chain_endpoint: str, use_process_isolation: bool = False):
        self.subtensor_network = subtensor_network
        self.chain_endpoint = chain_endpoint
        self.use_process_isolation = use_process_isolation
        self._current_connection: Optional[SubstrateInterface] = None
        self._connection_created_at: Optional[float] = None
        self._lock = threading.Lock()
        self._connection_count = 0
        
        isolation_mode = "process-isolated" if use_process_isolation else "regular"
        logger.info(f"SubstrateManager initialized for {subtensor_network}@{chain_endpoint} ({isolation_mode})")
    
    def get_fresh_connection(self) -> SubstrateInterface:
        """
        Get a fresh substrate connection. Always disposes old connection and creates new one.
        This prevents connection stacking and memory accumulation.
        
        If process_isolation is enabled, returns a ProcessIsolatedSubstrate wrapper instead.
        """
        with self._lock:
            # ALWAYS dispose existing connection first - no reuse, always fresh
            if self._current_connection is not None:
                logger.debug("🧹 Disposing old connection to create fresh one")
                if not self.use_process_isolation:
                    self._dispose_connection_thoroughly(self._current_connection)
                self._current_connection = None
            
            # Create fresh connection
            try:
                self._connection_count += 1
                logger.debug(f"Creating fresh substrate connection #{self._connection_count}")
                
                if self.use_process_isolation:
                    # Return process-isolated wrapper - no actual connection created here
                    self._current_connection = ProcessIsolatedSubstrate(
                        subtensor_network=self.subtensor_network,
                        chain_endpoint=self.chain_endpoint
                    )
                    logger.debug(f"🛡️ Fresh process-isolated substrate connection #{self._connection_count} created")
                else:
                    # Create regular connection
                    if self.chain_endpoint and self.chain_endpoint.strip():
                        self._current_connection = get_substrate(
                            subtensor_network=self.subtensor_network,
                            subtensor_address=self.chain_endpoint
                        )
                    else:
                        # Use default endpoint when chain_endpoint is empty/None
                        self._current_connection = get_substrate(
                            subtensor_network=self.subtensor_network
                        )
                    logger.debug(f"✅ Fresh regular substrate connection #{self._connection_count} created")
                
                self._connection_created_at = time.time()
                return self._current_connection
                
            except Exception as e:
                logger.error(f"❌ Failed to create fresh substrate connection: {e}")
                raise
    

    
    def _dispose_connection_thoroughly(self, connection: SubstrateInterface):
        """
        Thoroughly dispose of a substrate connection with full cleanup.
        This is the key to preventing scalecodec memory leaks.
        """
        try:
            logger.debug("🧹 Starting thorough substrate connection disposal...")
            
            # 1. Close websocket first (if exists)
            try:
                if hasattr(connection, 'websocket') and connection.websocket:
                    connection.websocket.close()
                    logger.debug("   ✓ Websocket closed")
            except Exception as e:
                logger.debug(f"   ⚠ Error closing websocket: {e}")
            
            # 2. Clear connection-level caches
            try:
                cache_attrs = ['metadata_cache', '_request_cache', 'runtime_configuration', 
                              '_metadata', '_runtime_info', '_type_registry']
                for attr in cache_attrs:
                    if hasattr(connection, attr):
                        cache_obj = getattr(connection, attr)
                        if cache_obj is not None:
                            if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                cache_obj.clear()
                            elif isinstance(cache_obj, dict):
                                cache_obj.clear()
                            setattr(connection, attr, None)
                logger.debug("   ✓ Connection caches cleared")
            except Exception as e:
                logger.debug(f"   ⚠ Error clearing connection caches: {e}")
            
            # 3. Close main connection
            try:
                connection.close()
                logger.debug("   ✓ Main connection closed")
            except Exception as e:
                logger.debug(f"   ⚠ Error closing main connection: {e}")
            
            # 4. Scalecodec module cleanup
            try:
                self._clear_scalecodec_caches()
                logger.debug("   ✓ Scalecodec caches cleared")
            except Exception as e:
                logger.debug(f"   ⚠ Error clearing scalecodec caches: {e}")
            
            # 5. Force garbage collection
            try:
                collected = gc.collect()
                logger.debug(f"   ✓ Garbage collection freed {collected} objects")
            except Exception as e:
                logger.debug(f"   ⚠ Error during garbage collection: {e}")
            
            logger.debug("🧹 Thorough substrate disposal completed")
            
        except Exception as e:
            logger.debug(f"❌ Error during thorough disposal: {e}")
    
    def _clear_scalecodec_caches(self):
        """
        Clear scalecodec module caches thoroughly.
        This is critical for preventing memory accumulation.
        """
        import sys
        
        cleared_count = 0
        scalecodec_patterns = [
            'scalecodec', 'substrate', 'scale_info', 'metadata', 
            'substrateinterface', 'scale_codec'
        ]
        
        for module_name in list(sys.modules.keys()):
            if any(pattern in module_name.lower() for pattern in scalecodec_patterns):
                module = sys.modules.get(module_name)
                if module and hasattr(module, '__dict__'):
                    # Clear all cache-like attributes
                    for attr_name in list(module.__dict__.keys()):
                        if any(cache_word in attr_name.lower() for cache_word in 
                               ['cache', 'registry', '_cached', '_memo', '_lru', '_store', 
                                '_type_registry', '_metadata', '_runtime', '_version']):
                            try:
                                cache_obj = getattr(module, attr_name)
                                if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                    cache_obj.clear()
                                    cleared_count += 1
                                elif isinstance(cache_obj, (dict, list, set)):
                                    cache_obj.clear()
                                    cleared_count += 1
                            except Exception:
                                pass
        
        if cleared_count > 0:
            logger.debug(f"   Cleared {cleared_count} scalecodec cache objects")
    
    def force_cleanup(self):
        """
        Force cleanup of current connection and caches.
        Useful for emergency memory cleanup.
        """
        with self._lock:
            if self._current_connection is not None:
                logger.info("🚨 Force cleanup of substrate connection")
                self._dispose_connection_thoroughly(self._current_connection)
                self._current_connection = None
            else:
                # Even if no connection, clear scalecodec caches
                logger.debug("🧹 Force cleanup of scalecodec caches (no active connection)")
                self._clear_scalecodec_caches()
                gc.collect()
    
    @contextmanager
    def fresh_connection(self):
        """
        Context manager for automatic connection cleanup.
        
        Usage:
            with manager.fresh_connection() as substrate:
                # Use substrate connection
                result = substrate.query(...)
            # Connection automatically disposed here
        """
        connection = None
        try:
            connection = self.get_fresh_connection()
            yield connection
        finally:
            # Always cleanup, even on exceptions
            if connection is not None:
                with self._lock:
                    if self._current_connection == connection:
                        self._dispose_connection_thoroughly(connection)
                        self._current_connection = None
    
    def get_stats(self) -> dict:
        """Get manager statistics."""
        with self._lock:
            connection_age = None
            
            if self._current_connection is not None and self._connection_created_at is not None:
                connection_age = time.time() - self._connection_created_at
            
            return {
                "connection_count": self._connection_count,
                "has_active_connection": self._current_connection is not None,
                "connection_age_seconds": connection_age,
                "network": self.subtensor_network,
                "endpoint": self.chain_endpoint
            }
    
    def shutdown(self):
        """Complete shutdown with cleanup."""
        logger.info("🛑 Shutting down SubstrateManager...")
        with self._lock:
            if self._current_connection is not None:
                self._dispose_connection_thoroughly(self._current_connection)
                self._current_connection = None
        logger.info("✅ SubstrateManager shutdown complete")


# Global manager instance (singleton per process)
_global_manager: Optional[SubstrateManager] = None
_global_lock = threading.Lock()


def get_substrate_manager(subtensor_network: str, chain_endpoint: str, use_process_isolation: bool = False) -> SubstrateManager:
    """
    Get the global substrate manager (singleton).
    Creates one only if it doesn't exist or if the network actually changed.
    The manager handles different endpoints gracefully without recreation.
    
    Args:
        subtensor_network: Network name (finney, test, local, etc.)
        chain_endpoint: Chain endpoint URL 
        use_process_isolation: If True, use process isolation to prevent ABC memory leaks
    """
    global _global_manager
    
    with _global_lock:
        # Only create new manager if none exists, network changed, or isolation mode changed
        needs_new_manager = (
            _global_manager is None or 
            _global_manager.subtensor_network != subtensor_network or
            _global_manager.use_process_isolation != use_process_isolation
        )
        
        if needs_new_manager:
            # Cleanup old manager if exists
            if _global_manager is not None:
                old_isolation = "process-isolated" if _global_manager.use_process_isolation else "regular"
                new_isolation = "process-isolated" if use_process_isolation else "regular"
                logger.info(f"Manager change: {_global_manager.subtensor_network}({old_isolation}) → {subtensor_network}({new_isolation}), recreating manager")
                _global_manager.shutdown()
            
            # Create new manager - normalize endpoint
            normalized_endpoint = chain_endpoint.strip() if chain_endpoint else ""
            _global_manager = SubstrateManager(subtensor_network, normalized_endpoint, use_process_isolation)
            isolation_mode = "process-isolated" if use_process_isolation else "regular"
            logger.info(f"🔄 Created new global SubstrateManager for {subtensor_network} ({isolation_mode})")
        else:
            # Just update endpoint if it's different, but don't recreate manager
            normalized_endpoint = chain_endpoint.strip() if chain_endpoint else ""
            if _global_manager.chain_endpoint != normalized_endpoint:
                logger.debug(f"Updating endpoint: {_global_manager.chain_endpoint} → {normalized_endpoint}")
                _global_manager.chain_endpoint = normalized_endpoint
                # Force disposal of current connection since endpoint changed
                _global_manager.force_cleanup()
        
        return _global_manager


def cleanup_global_substrate_manager():
    """Cleanup the global substrate manager."""
    global _global_manager
    
    with _global_lock:
        if _global_manager is not None:
            _global_manager.shutdown()
            _global_manager = None
            logger.info("🧹 Global substrate manager cleaned up")


def get_fresh_substrate_connection(subtensor_network: str, chain_endpoint: str, use_process_isolation: bool = False) -> SubstrateInterface:
    """
    Convenience function to get a fresh substrate connection.
    Always creates a new connection and disposes of any previous one.
    
    Args:
        subtensor_network: Network name (finney, test, local, etc.)
        chain_endpoint: Chain endpoint URL
        use_process_isolation: If True, return a process-isolated wrapper to prevent ABC memory leaks
    """
    manager = get_substrate_manager(subtensor_network, chain_endpoint, use_process_isolation)
    return manager.get_fresh_connection()


def get_process_isolated_substrate(subtensor_network: str, chain_endpoint: str) -> SubstrateInterface:
    """
    Convenience function to get a process-isolated substrate connection.
    This completely prevents ABC memory leaks by running operations in separate processes.
    
    This is a drop-in replacement for regular substrate connections.
    """
    return get_fresh_substrate_connection(subtensor_network, chain_endpoint, use_process_isolation=True)


def force_substrate_cleanup(subtensor_network: str = None, chain_endpoint: str = None):
    """
    Force cleanup of substrate connections and caches.
    Useful for emergency memory management.
    
    Args:
        subtensor_network: Network name (optional, uses current global manager if None)
        chain_endpoint: Chain endpoint (optional, uses current global manager if None)
    """
    global _global_manager
    
    if subtensor_network and chain_endpoint:
        manager = get_substrate_manager(subtensor_network, chain_endpoint)
        manager.force_cleanup()
    elif _global_manager:
        _global_manager.force_cleanup()
    else:
        logger.debug("No substrate manager to cleanup")


def get_substrate_manager_stats(subtensor_network: str = None, chain_endpoint: str = None) -> dict:
    """
    Get substrate manager statistics without creating new connections.
    
    Args:
        subtensor_network: Network name (optional, uses current global manager if None)
        chain_endpoint: Chain endpoint (optional, uses current global manager if None)
    """
    global _global_manager
    
    with _global_lock:
        if _global_manager is None:
            return {"status": "no_manager", "has_manager": False}
        
        try:
            stats = _global_manager.get_stats()
            stats["has_manager"] = True
            stats["status"] = "active"
            stats["use_process_isolation"] = _global_manager.use_process_isolation
            return stats
        except Exception as e:
            return {"status": "error", "has_manager": True, "error": str(e)} 