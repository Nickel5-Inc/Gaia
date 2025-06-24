# NetworkService - Async job manager for all network operations
import asyncio
import aiohttp
import aiofiles
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable, Union, Set
from urllib.parse import urlparse
import json
import traceback

# Import existing business logic for integration
from ...core.network.miners import MinerCommunication
from ...core.http_client import HTTPClientManager


class NetworkJobPriority(IntEnum):
    """Priority levels for network jobs (lower number = higher priority)."""
    CRITICAL = 0    # Chain operations, urgent miner queries
    HIGH = 1        # Normal miner queries, sync operations  
    NORMAL = 2      # Data validation, health checks
    LOW = 3         # File downloads, bulk operations
    BACKGROUND = 4  # Large file transfers, optional operations


class NetworkJobType(Enum):
    """Types of network operations."""
    QUERY_MINERS = "query_miners"
    SYNC_CHAIN = "sync_chain"
    SYNC_METAGRAPH = "sync_metagraph"
    DOWNLOAD_FILE = "download_file"
    UPLOAD_FILE = "upload_file"
    HTTP_REQUEST = "http_request"
    VALIDATE_ENDPOINT = "validate_endpoint"
    BATCH_OPERATION = "batch_operation"


@dataclass
class NetworkJob:
    """Network job definition."""
    job_id: str
    job_type: NetworkJobType
    priority: NetworkJobPriority
    payload: Dict[str, Any]
    timeout: int = 30
    retries: int = 3
    created_at: float = field(default_factory=time.time)
    callback: Optional[Callable] = None
    
    def __post_init__(self):
        if not self.job_id:
            self.job_id = str(uuid.uuid4())


@dataclass  
class NetworkJobResult:
    """Result of network job execution."""
    job_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    execution_time: float = 0.0
    attempts: int = 1
    completed_at: float = field(default_factory=time.time)


class NetworkClientPool:
    """Manages specialized HTTP clients for different operation types."""
    
    def __init__(self):
        self.clients: Dict[str, aiohttp.ClientSession] = {}
        self.logger = logging.getLogger(f"{__name__}.NetworkClientPool")
    
    async def get_client(self, client_type: str) -> aiohttp.ClientSession:
        """Get or create HTTP client for specific operation type."""
        if client_type not in self.clients:
            timeout = aiohttp.ClientTimeout(total=300, connect=30)
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=20,
                keepalive_timeout=60,
                enable_cleanup_closed=True
            )
            
            self.clients[client_type] = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={'User-Agent': f'GaiaValidator-{client_type}/2.0'}
            )
            
            self.logger.info(f"Created {client_type} HTTP client")
        
        return self.clients[client_type]
    
    async def close_all(self):
        """Close all HTTP clients."""
        for client_type, client in self.clients.items():
            try:
                await client.close()
                self.logger.info(f"Closed {client_type} HTTP client")
            except Exception as e:
                self.logger.error(f"Error closing {client_type} client: {e}")
        
        self.clients.clear()


class NetworkService:
    """
    Async job manager for all network operations.
    
    Manages network traffic through priority queues, specialized HTTP clients,
    and background task coordination. Handles miner queries, chain sync,
    file downloads, and all other network operations.
    """
    
    def __init__(self, miner_client=None, metagraph=None, max_concurrent_jobs: int = 10):
        self.logger = logging.getLogger(f"{__name__}.NetworkService")
        
        # Legacy integration for existing business logic
        self.miner_communication = MinerCommunication(miner_client, metagraph)
        self.http_manager = HTTPClientManager()
        
        # HTTP client pool for different operation types
        self.client_pool = NetworkClientPool()
        
        # Job queues by priority
        self.job_queues: Dict[NetworkJobPriority, asyncio.Queue] = {
            priority: asyncio.Queue() for priority in NetworkJobPriority
        }
        
        # Active job tracking
        self.active_jobs: Dict[str, NetworkJob] = {}
        self.job_results: Dict[str, NetworkJobResult] = {}
        self.job_futures: Dict[str, asyncio.Future] = {}
        
        # Concurrency control
        self.max_concurrent_jobs = max_concurrent_jobs
        self.semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self.active_job_count = 0
        
        # Background task management
        self.background_tasks: Set[asyncio.Task] = set()
        self.shutdown_event = asyncio.Event()
        
        # Thread pool for file operations
        self.file_executor = ThreadPoolExecutor(
            max_workers=4, 
            thread_name_prefix="gaia_network_file"
        )
        
        # Statistics
        self.stats = {
            'total_jobs': 0,
            'successful_jobs': 0,
            'failed_jobs': 0,
            'active_jobs': 0,
            'queued_jobs': 0,
            'avg_execution_time': 0.0,
            'jobs_by_type': {job_type.value: 0 for job_type in NetworkJobType},
            'jobs_by_priority': {priority.name: 0 for priority in NetworkJobPriority}
        }
        
        # Main job processor task
        self._processor_task = None
        self._background_processor_task = None
        
        self.logger.info(f"NetworkService initialized with {max_concurrent_jobs} max concurrent jobs")
    
    async def start(self):
        """Start the network service job processors."""
        if self._processor_task is None:
            self._processor_task = asyncio.create_task(self._job_processor())
            self._background_processor_task = asyncio.create_task(self._background_processor())
            self.logger.info("Network service job processors started")
    
    async def _job_processor(self):
        """Main job processor that handles priority queues."""
        while not self.shutdown_event.is_set():
            try:
                # Check queues in priority order
                job_found = False
                
                for priority in sorted(NetworkJobPriority):
                    queue = self.job_queues[priority]
                    
                    if not queue.empty():
                        try:
                            job = queue.get_nowait()
                            asyncio.create_task(self._execute_job(job))
                            job_found = True
                            break
                        except asyncio.QueueEmpty:
                            continue
                
                if not job_found:
                    # No jobs available, wait a bit
                    await asyncio.sleep(0.1)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in job processor: {e}")
                await asyncio.sleep(1)
    
    async def _background_processor(self):
        """Background processor for low-priority operations."""
        while not self.shutdown_event.is_set():
            try:
                # Process background and low priority jobs
                for priority in [NetworkJobPriority.LOW, NetworkJobPriority.BACKGROUND]:
                    queue = self.job_queues[priority]
                    
                    while not queue.empty() and self.active_job_count < self.max_concurrent_jobs:
                        try:
                            job = queue.get_nowait()
                            asyncio.create_task(self._execute_job(job))
                        except asyncio.QueueEmpty:
                            break
                
                await asyncio.sleep(1)  # Background processor runs slower
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in background processor: {e}")
                await asyncio.sleep(5)
    
    async def submit_job(self, job_type: NetworkJobType, payload: Dict[str, Any],
                        priority: NetworkJobPriority = NetworkJobPriority.NORMAL,
                        timeout: int = 30, retries: int = 3,
                        callback: Optional[Callable] = None) -> str:
        """Submit a network job and return job ID."""
        
        job = NetworkJob(
            job_id=str(uuid.uuid4()),
            job_type=job_type,
            priority=priority,
            payload=payload,
            timeout=timeout,
            retries=retries,
            callback=callback
        )
        
        # Create future for result tracking
        future = asyncio.Future()
        self.job_futures[job.job_id] = future
        
        # Add to appropriate priority queue
        await self.job_queues[priority].put(job)
        
        # Update stats
        self.stats['total_jobs'] += 1
        self.stats['jobs_by_type'][job_type.value] += 1
        self.stats['jobs_by_priority'][priority.name] += 1
        self._update_queue_stats()
        
        self.logger.debug(f"Submitted {job_type.value} job {job.job_id} with priority {priority.name}")
        return job.job_id
    
    async def submit_job_and_wait(self, job_type: NetworkJobType, payload: Dict[str, Any],
                                 priority: NetworkJobPriority = NetworkJobPriority.NORMAL,
                                 timeout: int = 30, retries: int = 3) -> NetworkJobResult:
        """Submit job and wait for completion."""
        job_id = await self.submit_job(job_type, payload, priority, timeout, retries)
        return await self.wait_for_job(job_id)
    
    async def wait_for_job(self, job_id: str, timeout: Optional[int] = None) -> NetworkJobResult:
        """Wait for a specific job to complete."""
        if job_id not in self.job_futures:
            raise ValueError(f"Job {job_id} not found")
        
        try:
            await asyncio.wait_for(self.job_futures[job_id], timeout=timeout)
            return self.job_results[job_id]
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout waiting for job {job_id}")
            raise
        finally:
            # Cleanup
            self.job_futures.pop(job_id, None)
            self.job_results.pop(job_id, None)
    
    async def _execute_job(self, job: NetworkJob):
        """Execute a network job with retry logic and error handling."""
        async with self.semaphore:  # Limit concurrent jobs
            self.active_jobs[job.job_id] = job
            self.active_job_count += 1
            self.stats['active_jobs'] = self.active_job_count
            
            start_time = time.time()
            attempts = 0
            last_error = None
            
            try:
                for attempt in range(job.retries + 1):
                    attempts = attempt + 1
                    try:
                        self.logger.debug(f"Executing {job.job_type.value} job {job.job_id}, attempt {attempts}")
                        
                        # Route job to appropriate handler
                        if job.job_type == NetworkJobType.QUERY_MINERS:
                            result = await self._handle_query_miners(job)
                        elif job.job_type == NetworkJobType.SYNC_CHAIN:
                            result = await self._handle_sync_chain(job)
                        elif job.job_type == NetworkJobType.SYNC_METAGRAPH:
                            result = await self._handle_sync_metagraph(job)
                        elif job.job_type == NetworkJobType.DOWNLOAD_FILE:
                            result = await self._handle_download_file(job)
                        elif job.job_type == NetworkJobType.UPLOAD_FILE:
                            result = await self._handle_upload_file(job)
                        elif job.job_type == NetworkJobType.HTTP_REQUEST:
                            result = await self._handle_http_request(job)
                        elif job.job_type == NetworkJobType.VALIDATE_ENDPOINT:
                            result = await self._handle_validate_endpoint(job)
                        elif job.job_type == NetworkJobType.BATCH_OPERATION:
                            result = await self._handle_batch_operation(job)
                        else:
                            raise ValueError(f"Unknown job type: {job.job_type}")
                        
                        # Success
                        execution_time = time.time() - start_time
                        job_result = NetworkJobResult(
                            job_id=job.job_id,
                            success=True,
                            result=result,
                            execution_time=execution_time,
                            attempts=attempts
                        )
                        
                        self.stats['successful_jobs'] += 1
                        self._update_avg_execution_time(execution_time)
                        
                        self.logger.info(f"Job {job.job_id} completed successfully in {execution_time:.2f}s")
                        break
                        
                    except Exception as e:
                        last_error = e
                        error_msg = str(e)
                        self.logger.warning(f"Job {job.job_id} attempt {attempts} failed: {error_msg}")
                        
                        if attempt < job.retries:
                            # Wait before retry (exponential backoff)
                            wait_time = min(2 ** attempt, 30)
                            await asyncio.sleep(wait_time)
                        else:
                            # All retries exhausted
                            execution_time = time.time() - start_time
                            job_result = NetworkJobResult(
                                job_id=job.job_id,
                                success=False,
                                error=error_msg,
                                error_type=type(e).__name__,
                                execution_time=execution_time,
                                attempts=attempts
                            )
                            
                            self.stats['failed_jobs'] += 1
                            self._update_avg_execution_time(execution_time)
                            
                            self.logger.error(f"Job {job.job_id} failed after {attempts} attempts: {error_msg}")
                
            finally:
                # Store result and complete future
                self.job_results[job_result.job_id] = job_result
                
                if job.job_id in self.job_futures:
                    self.job_futures[job.job_id].set_result(job_result)
                
                # Call callback if provided
                if job.callback:
                    try:
                        if asyncio.iscoroutinefunction(job.callback):
                            await job.callback(job_result)
                        else:
                            job.callback(job_result)
                    except Exception as e:
                        self.logger.error(f"Error in job callback: {e}")
                
                # Cleanup
                self.active_jobs.pop(job.job_id, None)
                self.active_job_count = max(0, self.active_job_count - 1)
                self.stats['active_jobs'] = self.active_job_count
                self._update_queue_stats()
    
    def _update_queue_stats(self):
        """Update queue statistics."""
        total_queued = sum(queue.qsize() for queue in self.job_queues.values())
        self.stats['queued_jobs'] = total_queued
    
    def _update_avg_execution_time(self, execution_time: float):
        """Update average execution time."""
        total_completed = self.stats['successful_jobs'] + self.stats['failed_jobs']
        if total_completed > 0:
            current_avg = self.stats['avg_execution_time']
            self.stats['avg_execution_time'] = (
                (current_avg * (total_completed - 1) + execution_time) / total_completed
            )
    
    # === Job Handlers ===
    
    async def _handle_query_miners(self, job: NetworkJob) -> Any:
        """Handle miner query operations."""
        payload = job.payload.get('payload', {})
        endpoint = job.payload.get('endpoint', '')
        hotkeys = job.payload.get('hotkeys')
        
        # Use existing miner communication logic but through service
        return await self.miner_communication.query_miners(payload, endpoint, hotkeys)
    
    async def _handle_sync_chain(self, job: NetworkJob) -> Any:
        """Handle blockchain sync operations."""
        # Implementation depends on specific chain operations needed
        operation = job.payload.get('operation', 'sync')
        
        if operation == 'sync':
            # Implement chain sync logic
            pass
        elif operation == 'submit_weights':
            # Implement weight submission
            pass
        
        return {"status": "completed", "operation": operation}
    
    async def _handle_sync_metagraph(self, job: NetworkJob) -> Any:
        """Handle metagraph sync operations."""
        return await self.miner_communication.sync_metagraph()
    
    async def _handle_download_file(self, job: NetworkJob) -> Any:
        """Handle file download operations."""
        url = job.payload['url']
        dest_path = Path(job.payload['dest_path'])
        
        client = await self.client_pool.get_client('data_client')
        
        async with client.get(url) as response:
            response.raise_for_status()
            
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            async with aiofiles.open(dest_path, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)
        
        return {"file_path": str(dest_path), "size": dest_path.stat().st_size}
    
    async def _handle_upload_file(self, job: NetworkJob) -> Any:
        """Handle file upload operations."""
        # Implementation for file uploads
        pass
    
    async def _handle_http_request(self, job: NetworkJob) -> Any:
        """Handle generic HTTP requests."""
        method = job.payload.get('method', 'GET')
        url = job.payload['url']
        data = job.payload.get('data')
        headers = job.payload.get('headers', {})
        
        client = await self.client_pool.get_client('general_client')
        
        async with client.request(method, url, json=data, headers=headers) as response:
            response.raise_for_status()
            
            if response.content_type == 'application/json':
                return await response.json()
            else:
                return await response.text()
    
    async def _handle_validate_endpoint(self, job: NetworkJob) -> Any:
        """Handle endpoint validation."""
        # Implementation for endpoint health checks
        pass
    
    async def _handle_batch_operation(self, job: NetworkJob) -> Any:
        """Handle batch operations."""
        # Implementation for batch job processing
        pass
    
    # === Public API Methods ===
    
    async def query_miners(self, payload: Dict, endpoint: str, 
                          hotkeys: Optional[List[str]] = None,
                          priority: NetworkJobPriority = NetworkJobPriority.HIGH) -> Dict:
        """Query miners with high priority."""
        job_payload = {
            'payload': payload,
            'endpoint': endpoint,
            'hotkeys': hotkeys
        }
        
        result = await self.submit_job_and_wait(
            NetworkJobType.QUERY_MINERS,
            job_payload,
            priority=priority,
            timeout=60
        )
        
        if result.success:
            return result.result
        else:
            raise Exception(f"Miner query failed: {result.error}")
    
    async def sync_metagraph(self, priority: NetworkJobPriority = NetworkJobPriority.HIGH) -> bool:
        """Sync metagraph with high priority."""
        result = await self.submit_job_and_wait(
            NetworkJobType.SYNC_METAGRAPH,
            {},
            priority=priority,
            timeout=30
        )
        
        return result.success
    
    async def download_file_async(self, url: str, dest_path: str,
                                 priority: NetworkJobPriority = NetworkJobPriority.BACKGROUND) -> str:
        """Download file asynchronously in background."""
        job_id = await self.submit_job(
            NetworkJobType.DOWNLOAD_FILE,
            {'url': url, 'dest_path': dest_path},
            priority=priority,
            timeout=600  # 10 minutes for large files
        )
        return job_id
    
    async def get_health(self) -> Dict[str, Any]:
        """Get network service health and statistics."""
        return {
            "healthy": not self.shutdown_event.is_set(),
            "service": "network",
            "stats": self.stats.copy(),
            "queue_sizes": {
                priority.name: queue.qsize() 
                for priority, queue in self.job_queues.items()
            },
            "active_jobs": len(self.active_jobs),
            "client_pool_size": len(self.client_pool.clients)
        }
    
    async def close(self):
        """Shutdown network service gracefully."""
        self.logger.info("Shutting down NetworkService...")
        
        # Signal shutdown
        self.shutdown_event.set()
        
        # Cancel processor tasks
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        
        if self._background_processor_task:
            self._background_processor_task.cancel()
            try:
                await self._background_processor_task
            except asyncio.CancelledError:
                pass
        
        # Cancel background tasks
        for task in self.background_tasks:
            task.cancel()
        
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        # Close HTTP clients
        await self.client_pool.close_all()
        
        # Shutdown file executor
        self.file_executor.shutdown(wait=False)
        
        self.logger.info("NetworkService shutdown complete") 