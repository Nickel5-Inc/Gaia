# ExecutionService for running code in separate threads/processes with job queuing
import asyncio
import logging
import traceback
import time
import functools
import gc
import psutil
import weakref
import uuid
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, TimeoutError, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Set
from dataclasses import dataclass, field
from enum import Enum
import multiprocessing
import threading
from contextlib import asynccontextmanager, contextmanager
import os
import signal

from ..database.service import DatabaseService


class ExecutionMode(Enum):
    """Execution modes for running code."""
    THREAD = "thread"       # For I/O bound tasks
    PROCESS = "process"     # For CPU bound tasks  
    ASYNC = "async"         # For async functions in current event loop


class JobPriority(Enum):
    """Priority levels for execution jobs."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass
class ExecutionJob:
    """Job definition for execution service."""
    job_id: str
    func: Callable
    args: Tuple = ()
    kwargs: Optional[Dict] = None
    execution_mode: ExecutionMode = ExecutionMode.THREAD
    timeout: int = 300
    inject_database: bool = True
    worker_name: Optional[str] = None
    priority: JobPriority = JobPriority.NORMAL
    future: Optional[asyncio.Future] = None
    created_at: float = field(default_factory=time.time)
    
    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}
        if self.future is None:
            self.future = asyncio.Future()
    
    def __lt__(self, other):
        """For priority queue sorting."""
        return self.priority.value < other.priority.value


@dataclass
class ExecutionResult:
    """Result of code execution."""
    success: bool
    result: Any = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    execution_time: float = 0.0
    execution_mode: Optional[str] = None
    worker_id: Optional[str] = None
    memory_used_mb: float = 0.0
    peak_memory_mb: float = 0.0
    cleanup_performed: bool = False


@dataclass 
class ExecutionContext:
    """Context tracking for individual executions."""
    worker_id: str
    start_time: float
    initial_memory_mb: float
    database_connections: Set[Any]
    file_handles: Set[Any]
    temp_resources: List[Any]
    thread_locals: Dict[str, Any]
    
    def __post_init__(self):
        self.database_connections = set()
        self.file_handles = set()
        self.temp_resources = []
        self.thread_locals = {}
    

class ExecutionService:
    """
    General-purpose execution service for running code in separate threads/processes with job queuing.
    
    Provides a simple API for executing any callable with proper exception handling,
    timeout management, database connection injection, and job queuing.
    """
    
    def __init__(self, database_service: Optional[DatabaseService] = None,
                 max_thread_workers: int = 4, max_process_workers: int = 2,
                 memory_limit_mb: int = 1024, cleanup_interval: int = 300,
                 max_queue_size: int = 1000, job_processors: int = 3):
        self.logger = logging.getLogger(f"{__name__}.ExecutionService")
        
        # Database service for connection injection
        self.database_service = database_service
        
        # Resource limits
        self.memory_limit_mb = memory_limit_mb
        self.cleanup_interval = cleanup_interval
        
        # Job queue and processing
        self.job_queue = asyncio.PriorityQueue(maxsize=max_queue_size)
        self.job_processors = job_processors
        self.processor_tasks = []
        self.active_jobs: Dict[str, ExecutionJob] = {}
        self.job_lock = asyncio.Lock()
        self.running = False
        
        # Thread pool for I/O bound tasks
        self.max_thread_workers = max_thread_workers
        self.thread_executor = ThreadPoolExecutor(
            max_workers=max_thread_workers,
            thread_name_prefix="gaia_execution_thread"
        )
        
        # Process pool for CPU bound tasks
        self.max_process_workers = max_process_workers
        self.process_executor = ProcessPoolExecutor(
            max_workers=max_process_workers
        )
        
        # Execution context tracking
        self.active_contexts: Dict[str, ExecutionContext] = {}
        self.context_lock = asyncio.Lock()
        
        # Resource monitoring
        self.process = psutil.Process()
        self.peak_memory_mb = 0.0
        self.last_cleanup = time.time()
        
        # Cleanup tracking
        self.cleanup_callbacks: List[Callable] = []
        
        # Execution statistics
        self.stats = {
            'total_jobs': 0,
            'queued_jobs': 0,
            'completed_jobs': 0,
            'failed_jobs': 0,
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'thread_executions': 0,
            'process_executions': 0,
            'async_executions': 0,
            'avg_execution_time': 0.0,
            'active_workers': 0,
            'active_processors': 0,
            'total_memory_mb': 0.0,
            'peak_memory_mb': 0.0,
            'cleanup_runs': 0,
            'resources_cleaned': 0
        }
        
        # Start background cleanup task
        self._cleanup_task = None
        self._shutdown_event = asyncio.Event()
        
        self.logger.info(f"ExecutionService initialized: {max_thread_workers} thread workers, {max_process_workers} process workers, {job_processors} job processors, {memory_limit_mb}MB limit, queue size {max_queue_size}")
    
    async def start_processors(self):
        """Start job processor tasks."""
        if self.running:
            self.logger.warning("ExecutionService processors already running")
            return
        
        self.running = True
        
        # Start job processor tasks
        for i in range(self.job_processors):
            processor_task = asyncio.create_task(self._job_processor(f"exec_processor_{i}"))
            self.processor_tasks.append(processor_task)
        
        self.logger.info(f"Started {self.job_processors} execution job processors")
    
    async def _job_processor(self, processor_name: str):
        """Background processor that executes jobs from the queue."""
        self.stats['active_processors'] += 1
        
        try:
            while self.running and not self._shutdown_event.is_set():
                try:
                    # Get job from priority queue with timeout
                    _, job = await asyncio.wait_for(self.job_queue.get(), timeout=1.0)
                    
                    self.logger.debug(f"[{processor_name}] Processing job {job.job_id}")
                    await self._process_execution_job(job, processor_name)
                    
                    # Mark job as done
                    self.job_queue.task_done()
                    
                except asyncio.TimeoutError:
                    # No jobs available, continue
                    continue
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(f"[{processor_name}] Error processing job: {e}")
                    continue
                    
        finally:
            self.stats['active_processors'] = max(0, self.stats['active_processors'] - 1)
            self.logger.debug(f"[{processor_name}] Job processor stopped")
    
    async def _process_execution_job(self, job: ExecutionJob, processor_name: str):
        """Process a single execution job."""
        start_time = time.time()
        
        try:
            async with self.job_lock:
                self.active_jobs[job.job_id] = job
            
            # Execute the job
            result = await self._execute_job_directly(job, processor_name)
            
            # Set successful result
            if not job.future.done():
                job.future.set_result(result)
            
            execution_time = time.time() - start_time
            self.stats['completed_jobs'] += 1
            
            self.logger.debug(f"[{processor_name}] Job {job.job_id} completed in {execution_time:.3f}s")
            
        except Exception as e:
            # Set error result
            error_result = ExecutionResult(
                success=False,
                error=str(e),
                error_type=type(e).__name__,
                execution_time=time.time() - start_time,
                execution_mode=job.execution_mode.value,
                worker_id=job.job_id
            )
            
            if not job.future.done():
                job.future.set_result(error_result)
            
            execution_time = time.time() - start_time
            self.stats['failed_jobs'] += 1
            
            self.logger.error(f"[{processor_name}] Job {job.job_id} failed after {execution_time:.3f}s: {e}")
            
        finally:
            # Clean up
            async with self.job_lock:
                self.active_jobs.pop(job.job_id, None)
    
    async def start_background_cleanup(self):
        """Start background cleanup task."""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._background_cleanup_loop())
            self.logger.info("Background cleanup task started")
    
    async def _background_cleanup_loop(self):
        """Background task for periodic resource cleanup."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._periodic_cleanup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in background cleanup: {e}")
    
    async def _periodic_cleanup(self):
        """Perform periodic resource cleanup."""
        try:
            current_memory = self._get_memory_usage_mb()
            self.stats['total_memory_mb'] = current_memory
            
            if current_memory > self.peak_memory_mb:
                self.peak_memory_mb = current_memory
                self.stats['peak_memory_mb'] = self.peak_memory_mb
            
            # Force cleanup if memory usage is high
            if current_memory > self.memory_limit_mb:
                self.logger.warning(f"Memory usage {current_memory:.1f}MB exceeds limit {self.memory_limit_mb}MB - forcing cleanup")
                await self._force_cleanup()
            
            # Clean up completed contexts
            await self._cleanup_completed_contexts()
            
            # Run garbage collection
            gc.collect()
            
            self.stats['cleanup_runs'] += 1
            self.last_cleanup = time.time()
            
        except Exception as e:
            self.logger.error(f"Error in periodic cleanup: {e}")
    
    def _get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB."""
        try:
            memory_info = self.process.memory_info()
            return memory_info.rss / 1024 / 1024  # Convert to MB
        except Exception:
            return 0.0
    
    async def _force_cleanup(self):
        """Force cleanup of resources when memory limit exceeded."""
        self.logger.warning("Forcing resource cleanup due to memory pressure")
        
        # Run all cleanup callbacks
        for callback in self.cleanup_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback()
                else:
                    callback()
            except Exception as e:
                self.logger.error(f"Error in cleanup callback: {e}")
        
        # Force garbage collection
        gc.collect()
        
        # Clear completed contexts
        await self._cleanup_completed_contexts()
        
        self.stats['resources_cleaned'] += 1
    
    async def _cleanup_completed_contexts(self):
        """Clean up execution contexts that are no longer active."""
        async with self.context_lock:
            completed_contexts = []
            for worker_id, context in self.active_contexts.items():
                # Check if context is old or should be cleaned up
                age = time.time() - context.start_time
                if age > 3600:  # 1 hour timeout for contexts
                    completed_contexts.append(worker_id)
            
            for worker_id in completed_contexts:
                await self._cleanup_execution_context(worker_id)
    
    async def _create_execution_context(self, worker_id: str) -> ExecutionContext:
        """Create and track execution context."""
        current_memory = self._get_memory_usage_mb()
        
        context = ExecutionContext(
            worker_id=worker_id,
            start_time=time.time(),
            initial_memory_mb=current_memory,
            database_connections=set(),
            file_handles=set(),
            temp_resources=[],
            thread_locals={}
        )
        
        async with self.context_lock:
            self.active_contexts[worker_id] = context
        
        return context
    
    async def _cleanup_execution_context(self, worker_id: str):
        """Clean up specific execution context."""
        async with self.context_lock:
            context = self.active_contexts.pop(worker_id, None)
            
        if context:
            try:
                # Clean up database connections
                for conn in context.database_connections:
                    try:
                        if hasattr(conn, 'close'):
                            if asyncio.iscoroutinefunction(conn.close):
                                await conn.close()
                            else:
                                conn.close()
                    except Exception as e:
                        self.logger.debug(f"Error closing database connection: {e}")
                
                # Clean up file handles
                for handle in context.file_handles:
                    try:
                        if hasattr(handle, 'close'):
                            handle.close()
                    except Exception as e:
                        self.logger.debug(f"Error closing file handle: {e}")
                
                # Clean up temporary resources
                for resource in context.temp_resources:
                    try:
                        if hasattr(resource, 'cleanup'):
                            resource.cleanup()
                        elif hasattr(resource, 'close'):
                            resource.close()
                    except Exception as e:
                        self.logger.debug(f"Error cleaning up resource: {e}")
                
                self.logger.debug(f"Cleaned up execution context for {worker_id}")
                
            except Exception as e:
                self.logger.error(f"Error cleaning up context {worker_id}: {e}")
    
    @contextmanager
    def _resource_tracker(self, worker_id: str):
        """Context manager for tracking execution resources."""
        context = None
        try:
            # This will be set in the async context
            yield
        except Exception as e:
            self.logger.error(f"Exception in resource tracker for {worker_id}: {e}")
            raise
        finally:
            # Cleanup is handled by the async cleanup methods
            pass
    
    async def submit_job(self, 
                     func: Callable, 
                     args: Tuple = (), 
                     kwargs: Optional[Dict] = None,
                     execution_mode: Union[ExecutionMode, str] = ExecutionMode.THREAD,
                     timeout: int = 300,
                     inject_database: bool = True,
                     worker_name: Optional[str] = None,
                     priority: Union[JobPriority, str] = JobPriority.NORMAL) -> ExecutionResult:
        """
        Submit a job to the execution queue and wait for result.
        
        Args:
            func: Function to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            execution_mode: How to execute the function (thread/process/async)
            timeout: Timeout in seconds
            inject_database: Whether to inject database service as 'database' kwarg
            worker_name: Optional name for this worker (for logging)
            priority: Priority level for the job
            
        Returns:
            ExecutionResult with success status, result, and execution details
        """
        if not self.running:
            # Fallback to direct execution if processors not running
            self.logger.warning("Job processors not running, executing directly")
            return await self.run(func, args, kwargs, execution_mode, timeout, inject_database, worker_name)
        
        # Convert string parameters to enums if needed
        if isinstance(execution_mode, str):
            execution_mode = ExecutionMode(execution_mode.lower())
        if isinstance(priority, str):
            priority = JobPriority[priority.upper()]
        
        # Create job
        job = ExecutionJob(
            job_id=str(uuid.uuid4()),
            func=func,
            args=args,
            kwargs=kwargs,
            execution_mode=execution_mode,
            timeout=timeout,
            inject_database=inject_database,
            worker_name=worker_name,
            priority=priority
        )
        
        # Submit to priority queue
        await self.job_queue.put((priority.value, job))
        self.stats['total_jobs'] += 1
        self.stats['queued_jobs'] = self.job_queue.qsize()
        
        self.logger.debug(f"Submitted job {job.job_id} with priority {priority.name}")
        
        # Wait for result
        try:
            result = await asyncio.wait_for(job.future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self.logger.error(f"Job {job.job_id} timed out after {timeout}s")
            return ExecutionResult(
                success=False,
                error=f"Job timed out after {timeout}s",
                error_type="TimeoutError",
                execution_time=timeout,
                worker_id=job.job_id
            )
    
    async def _execute_job_directly(self, job: ExecutionJob, processor_name: str) -> ExecutionResult:
        """Execute a job directly (used by job processors)."""
        return await self.run(
            job.func,
            job.args,
            job.kwargs,
            job.execution_mode,
            job.timeout,
            job.inject_database,
            job.worker_name or f"{processor_name}_{job.job_id}"
        )
    
    async def run(self, 
                  func: Callable, 
                  args: Tuple = (), 
                  kwargs: Optional[Dict] = None,
                  execution_mode: Union[ExecutionMode, str] = ExecutionMode.THREAD,
                  timeout: int = 300,
                  inject_database: bool = True,
                  worker_name: Optional[str] = None) -> ExecutionResult:
        """
        Execute a function in the specified execution mode.
        
        Args:
            func: Function to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            execution_mode: How to execute the function (thread/process/async)
            timeout: Timeout in seconds
            inject_database: Whether to inject database service as 'database' kwarg
            worker_name: Optional name for this worker (for logging)
            
        Returns:
            ExecutionResult with success status, result, and execution details
        """
        start_time = time.perf_counter()
        kwargs = kwargs or {}
        
        # Convert string to enum if needed
        if isinstance(execution_mode, str):
            execution_mode = ExecutionMode(execution_mode.lower())
        
        # Inject database service if requested
        if inject_database and self.database_service and 'database' not in kwargs:
            kwargs['database'] = self.database_service
        
        # Generate worker ID
        worker_id = worker_name or f"{execution_mode.value}_{int(time.time())}"
        
        # Create execution context for resource tracking
        context = await self._create_execution_context(worker_id)
        
        self.logger.info(f"[{worker_id}] Executing {func.__name__} in {execution_mode.value} mode")
        self.stats['total_executions'] += 1
        self.stats['active_workers'] += 1
        
        try:
            # Execute based on mode
            if execution_mode == ExecutionMode.ASYNC:
                result = await self._execute_async(func, args, kwargs, timeout, worker_id)
            elif execution_mode == ExecutionMode.THREAD:
                result = await self._execute_in_thread(func, args, kwargs, timeout, worker_id)
            elif execution_mode == ExecutionMode.PROCESS:
                result = await self._execute_in_process(func, args, kwargs, timeout, worker_id)
            else:
                raise ValueError(f"Unknown execution mode: {execution_mode}")
            
            execution_time = time.perf_counter() - start_time
            
            # Calculate memory usage
            current_memory = self._get_memory_usage_mb()
            memory_used = max(0, current_memory - context.initial_memory_mb)
            
            self.stats['successful_executions'] += 1
            self.stats[f'{execution_mode.value}_executions'] += 1
            self._update_avg_execution_time(execution_time)
            
            self.logger.info(f"[{worker_id}] Execution successful in {execution_time:.2f}s, memory: {memory_used:.1f}MB")
            
            # Cleanup execution context
            await self._cleanup_execution_context(worker_id)
            
            return ExecutionResult(
                success=True,
                result=result,
                execution_time=execution_time,
                execution_mode=execution_mode.value,
                worker_id=worker_id,
                memory_used_mb=memory_used,
                peak_memory_mb=current_memory,
                cleanup_performed=True
            )
            
        except Exception as e:
            execution_time = time.perf_counter() - start_time
            error_type = type(e).__name__
            error_msg = str(e)
            
            # Calculate memory usage even on failure
            current_memory = self._get_memory_usage_mb()
            memory_used = max(0, current_memory - context.initial_memory_mb)
            
            self.stats['failed_executions'] += 1
            self._update_avg_execution_time(execution_time)
            
            self.logger.error(f"[{worker_id}] Execution failed after {execution_time:.2f}s, memory: {memory_used:.1f}MB: {error_type}: {error_msg}")
            self.logger.error(f"[{worker_id}] Traceback: {traceback.format_exc()}")
            
            # Force cleanup on failure
            await self._cleanup_execution_context(worker_id)
            
            return ExecutionResult(
                success=False,
                error=error_msg,
                error_type=error_type,
                execution_time=execution_time,
                execution_mode=execution_mode.value,
                worker_id=worker_id,
                memory_used_mb=memory_used,
                peak_memory_mb=current_memory,
                cleanup_performed=True
            )
        finally:
            self.stats['active_workers'] = max(0, self.stats['active_workers'] - 1)
            
            # Emergency cleanup if context still exists
            if worker_id in self.active_contexts:
                try:
                    await self._cleanup_execution_context(worker_id)
                except Exception as cleanup_error:
                    self.logger.error(f"[{worker_id}] Emergency cleanup failed: {cleanup_error}")
    
    async def _execute_async(self, func: Callable, args: Tuple, kwargs: Dict, 
                           timeout: int, worker_id: str) -> Any:
        """Execute async function in current event loop."""
        if not asyncio.iscoroutinefunction(func):
            raise ValueError(f"Function {func.__name__} is not async but execution_mode is 'async'")
        
        return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
    
    async def _execute_in_thread(self, func: Callable, args: Tuple, kwargs: Dict,
                                timeout: int, worker_id: str) -> Any:
        """Execute function in thread pool."""
        loop = asyncio.get_event_loop()
        
        # Wrap function to handle database connection in thread
        wrapped_func = self._wrap_function_for_thread(func, worker_id)
        
        future = self.thread_executor.submit(wrapped_func, *args, **kwargs)
        
        try:
            return await asyncio.wait_for(
                asyncio.wrap_future(future),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            future.cancel()
            raise TimeoutError(f"Thread execution timed out after {timeout}s")
    
    async def _execute_in_process(self, func: Callable, args: Tuple, kwargs: Dict,
                                 timeout: int, worker_id: str) -> Any:
        """Execute function in process pool."""
        # For process execution, we can't inject the database service directly
        # The function will need to create its own database connection if needed
        if 'database' in kwargs:
            self.logger.warning(f"[{worker_id}] Removing database service from kwargs for process execution")
            kwargs = {k: v for k, v in kwargs.items() if k != 'database'}
        
        # Wrap function for process execution
        wrapped_func = self._wrap_function_for_process(func, worker_id)
        
        future = self.process_executor.submit(wrapped_func, *args, **kwargs)
        
        try:
            return await asyncio.wait_for(
                asyncio.wrap_future(future),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            future.cancel()
            raise TimeoutError(f"Process execution timed out after {timeout}s")
    
    def _wrap_function_for_thread(self, func: Callable, worker_id: str) -> Callable:
        """Wrap function with thread-specific error handling."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            thread_name = threading.current_thread().name
            self.logger.debug(f"[{worker_id}] Executing in thread: {thread_name}")
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.logger.error(f"[{worker_id}] Thread execution error in {thread_name}: {e}")
                raise
        
        return wrapper
    
    def _wrap_function_for_process(self, func: Callable, worker_id: str) -> Callable:
        """Wrap function with process-specific error handling."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            process_name = multiprocessing.current_process().name
            # Note: Logger might not work properly in child processes
            # Consider using multiprocessing.get_logger() for process pool logging
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # In child process, we can't use self.logger
                print(f"[{worker_id}] Process execution error in {process_name}: {e}")
                raise
        
        return wrapper
    
    def _update_avg_execution_time(self, execution_time: float):
        """Update average execution time statistics."""
        total_executions = self.stats['total_executions']
        if total_executions > 0:
            current_avg = self.stats['avg_execution_time']
            self.stats['avg_execution_time'] = (
                (current_avg * (total_executions - 1) + execution_time) / total_executions
            )
    
    async def get_health(self) -> Dict[str, Any]:
        """Get execution service health and statistics."""
        current_memory = self._get_memory_usage_mb()
        memory_pressure = current_memory > self.memory_limit_mb * 0.8  # 80% threshold
        
        # Update real-time stats
        self.stats['total_memory_mb'] = current_memory
        self.stats['active_contexts'] = len(self.active_contexts)
        
        return {
            "healthy": not memory_pressure and not self._shutdown_event.is_set(),
            "service": "execution",
            "stats": self.stats.copy(),
            "memory": {
                "current_mb": current_memory,
                "limit_mb": self.memory_limit_mb,
                "peak_mb": self.peak_memory_mb,
                "pressure": memory_pressure
            },
            "executors": {
                "thread_alive": not self.thread_executor._shutdown,
                "process_alive": not self.process_executor._shutdown,
                "active_contexts": len(self.active_contexts)
            },
            "cleanup": {
                "last_cleanup": self.last_cleanup,
                "cleanup_interval": self.cleanup_interval,
                "background_task_running": self._cleanup_task is not None and not self._cleanup_task.done()
            }
        }
    
    async def close(self):
        """Shutdown all executors gracefully with complete resource cleanup."""
        self.logger.info("Shutting down ExecutionService...")
        
        # Signal shutdown to background tasks and processors
        self.running = False
        self._shutdown_event.set()
        
        # Cancel all job processors
        for processor in self.processor_tasks:
            processor.cancel()
        
        # Wait for processors to finish
        if self.processor_tasks:
            await asyncio.gather(*self.processor_tasks, return_exceptions=True)
            self.logger.info("Job processors stopped")
        
        # Cancel and wait for cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.error(f"Error during cleanup task shutdown: {e}")
        
        # Clean up all active execution contexts
        self.logger.info(f"Cleaning up {len(self.active_contexts)} active execution contexts...")
        active_worker_ids = list(self.active_contexts.keys())
        for worker_id in active_worker_ids:
            try:
                await self._cleanup_execution_context(worker_id)
            except Exception as e:
                self.logger.error(f"Error cleaning up context {worker_id}: {e}")
        
        # Run final resource cleanup
        try:
            await self._force_cleanup()
        except Exception as e:
            self.logger.error(f"Error in final cleanup: {e}")
        
        # Shutdown thread executor
        try:
            self.thread_executor.shutdown(wait=False)  # Don't wait indefinitely
            self.logger.info("Thread executor shutdown initiated")
        except Exception as e:
            self.logger.error(f"Error shutting down thread executor: {e}")
        
        # Shutdown process executor  
        try:
            self.process_executor.shutdown(wait=False)  # Don't wait indefinitely
            self.logger.info("Process executor shutdown initiated")
        except Exception as e:
            self.logger.error(f"Error shutting down process executor: {e}")
        
        # Force garbage collection
        gc.collect()
        
        final_memory = self._get_memory_usage_mb()
        self.logger.info(f"ExecutionService shutdown complete. Final memory usage: {final_memory:.1f}MB")
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get detailed queue status for monitoring."""
        oldest_job = None
        if self.active_jobs:
            oldest_job_time = min(job.created_at for job in self.active_jobs.values())
            oldest_job = time.time() - oldest_job_time
        
        return {
            "queue_size": self.job_queue.qsize() if hasattr(self.job_queue, 'qsize') else 0,
            "max_queue_size": self.job_queue.maxsize if hasattr(self.job_queue, 'maxsize') else 0,
            "active_jobs": len(self.active_jobs),
            "processors_running": self.stats['active_processors'],
            "max_processors": self.job_processors,
            "oldest_job_age_seconds": oldest_job,
            "is_queue_full": self.job_queue.full() if hasattr(self.job_queue, 'full') else False,
            "running": self.running
        }
    
    # === Convenience Methods ===
    
    async def submit_high_priority(self, func: Callable, *args, **kwargs) -> ExecutionResult:
        """Submit a high priority job."""
        return await self.submit_job(func, args, kwargs.get('kwargs', {}), 
                                   kwargs.get('execution_mode', ExecutionMode.THREAD),
                                   kwargs.get('timeout', 300), 
                                   kwargs.get('inject_database', True),
                                   kwargs.get('worker_name'),
                                   JobPriority.HIGH)
    
    async def submit_background(self, func: Callable, *args, **kwargs) -> ExecutionResult:
        """Submit a background priority job."""
        return await self.submit_job(func, args, kwargs.get('kwargs', {}), 
                                   kwargs.get('execution_mode', ExecutionMode.THREAD),
                                   kwargs.get('timeout', 300), 
                                   kwargs.get('inject_database', True),
                                   kwargs.get('worker_name'),
                                   JobPriority.BACKGROUND)
    
    async def submit_cpu_intensive(self, func: Callable, *args, timeout: int = 600, **kwargs) -> ExecutionResult:
        """Submit a CPU intensive job (process mode)."""
        return await self.submit_job(func, args, kwargs.get('kwargs', {}), 
                                   ExecutionMode.PROCESS,
                                   timeout, 
                                   False,  # No database injection for processes
                                   kwargs.get('worker_name'),
                                   kwargs.get('priority', JobPriority.NORMAL))
    
    def add_cleanup_callback(self, callback: Callable):
        """Add a cleanup callback to be called during resource cleanup."""
        self.cleanup_callbacks.append(callback)
    
    def remove_cleanup_callback(self, callback: Callable):
        """Remove a cleanup callback."""
        if callback in self.cleanup_callbacks:
            self.cleanup_callbacks.remove(callback)


# === Helper Functions for Common Use Cases ===

async def run_in_background(execution_service: ExecutionService,
                           func: Callable, 
                           *args, 
                           execution_mode: str = "thread",
                           **kwargs) -> ExecutionResult:
    """Convenience function for running tasks in background."""
    return await execution_service.run(
        func, 
        args=args, 
        execution_mode=execution_mode,
        **kwargs
    )


async def run_cpu_intensive(execution_service: ExecutionService,
                           func: Callable,
                           *args,
                           timeout: int = 600,
                           **kwargs) -> ExecutionResult:
    """Convenience function for CPU intensive tasks."""
    return await execution_service.run(
        func,
        args=args,
        execution_mode=ExecutionMode.PROCESS,
        timeout=timeout,
        inject_database=False,  # Processes need their own DB connections
        **kwargs
    ) 