"""
Miner Communications Core Module

Handles miner communication, handshakes, HTTP client management,
and connection pooling for validator-miner interactions.
"""

import os
import ssl
import time
import base64
import asyncio
import traceback
from typing import Dict, Optional, List, Any
import httpx
import psutil

from fiber.logging_utils import get_logger
from fiber.encrypted.validator import client as vali_client, handshake

logger = get_logger(__name__)

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None 
    PSUTIL_AVAILABLE = False


class MinerCommunicator:
    """
    Handles all miner communication including handshakes, HTTP clients,
    and connection management.
    """
    
    def __init__(self, args):
        """Initialize the miner communicator."""
        self.args = args
        self.miner_client: Optional[httpx.AsyncClient] = None
        self.api_client: Optional[httpx.AsyncClient] = None
        self.last_metagraph_sync = time.time()
        self.metagraph_sync_interval = 300  # 5 minutes
        
        # Initialize HTTP clients
        self._setup_http_clients()
        
    def _setup_http_clients(self):
        """Set up HTTP clients for miner and API communication."""
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Client for miner communication with SSL verification disabled
        self.miner_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=5.0),
            follow_redirects=True,
            verify=False,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=50,
                keepalive_expiry=300,  # 5 minutes
            ),
            transport=httpx.AsyncHTTPTransport(
                retries=2,
                verify=False,
            ),
        )
        
        # Client for API communication with SSL verification enabled
        self.api_client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            ),
            transport=httpx.AsyncHTTPTransport(retries=3),
        )

    async def perform_handshake_with_retry(
        self,
        httpx_client: httpx.AsyncClient,
        server_address: str,
        keypair: Any,
        miner_hotkey_ss58_address: str,
        max_retries: int = 2,
        base_timeout: float = 15.0
    ) -> tuple[str, str]:
        """
        Custom handshake function with retry logic and progressive timeouts.
        """
        last_exception = None
        
        for attempt in range(max_retries + 1):
            # Progressive timeout: increase timeout with each retry
            current_timeout = base_timeout * (1.5 ** attempt)
            
            try:
                logger.debug(f"Handshake attempt {attempt + 1}/{max_retries + 1} with timeout {current_timeout:.1f}s")
                
                # Get public key with current timeout
                public_key_encryption_key = await asyncio.wait_for(
                    handshake.get_public_encryption_key(
                        httpx_client, 
                        server_address, 
                        timeout=int(current_timeout)
                    ),
                    timeout=current_timeout
                )
                
                # Generate symmetric key
                symmetric_key: bytes = os.urandom(32)
                symmetric_key_uuid: str = os.urandom(32).hex()
                
                # Send symmetric key with current timeout
                success = await asyncio.wait_for(
                    handshake.send_symmetric_key_to_server(
                        httpx_client,
                        server_address,
                        keypair,
                        public_key_encryption_key,
                        symmetric_key,
                        symmetric_key_uuid,
                        miner_hotkey_ss58_address,
                        timeout=int(current_timeout),
                    ),
                    timeout=current_timeout
                )
                
                if success:
                    symmetric_key_str = base64.b64encode(symmetric_key).decode()
                    return symmetric_key_str, symmetric_key_uuid
                else:
                    raise Exception("Handshake failed: server returned unsuccessful status")
                    
            except (asyncio.TimeoutError, httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
                last_exception = e
                if attempt < max_retries:
                    wait_time = 1.0 * (attempt + 1)
                    logger.warning(f"Handshake timeout on attempt {attempt + 1}, retrying in {wait_time:.1f}s...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Handshake failed after {max_retries + 1} attempts due to timeout")
                    break
            except Exception as e:
                last_exception = e
                if attempt < max_retries:
                    wait_time = 1.0 * (attempt + 1)
                    logger.warning(f"Handshake error on attempt {attempt + 1}: {type(e).__name__} - {e}, retrying in {wait_time:.1f}s...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Handshake failed after {max_retries + 1} attempts due to error: {type(e).__name__} - {e}")
                    break
        
        # If we get here, all attempts failed
        raise last_exception or Exception("Handshake failed after all retry attempts")

    async def query_miners(
        self, 
        payload: Dict, 
        endpoint: str, 
        hotkeys: Optional[List[str]] = None,
        metagraph = None,
        keypair = None
    ) -> Dict:
        """Query miners with the given payload in parallel with batch retry logic."""
        try:
            logger.info(f"Querying miners for endpoint {endpoint} with payload size: {len(str(payload))} bytes. Specified hotkeys: {hotkeys if hotkeys else 'All/Default'}")
            
            responses = {}
            
            current_time = time.time()
            if metagraph is None:
                logger.error("Metagraph not provided to query_miners")
                return {}
                
            if not metagraph.nodes:
                logger.error("Metagraph has no nodes. Cannot query miners.")
                return {}

            nodes_to_consider = metagraph.nodes
            miners_to_query = {}

            if hotkeys:
                logger.info(f"Targeting specific hotkeys: {hotkeys}")
                for hk in hotkeys:
                    if hk in nodes_to_consider:
                        miners_to_query[hk] = nodes_to_consider[hk]
                    else:
                        logger.warning(f"Specified hotkey {hk} not found in current metagraph. Skipping.")
                if not miners_to_query:
                    logger.warning(f"No specified hotkeys found in metagraph. Querying will be empty for endpoint: {endpoint}")
                    return {}
            else:
                miners_to_query = nodes_to_consider
                if self.args.test and len(miners_to_query) > 10:
                    selected_hotkeys_for_test = list(miners_to_query.keys())[-10:]
                    miners_to_query = {k: miners_to_query[k] for k in selected_hotkeys_for_test}
                    logger.info(f"Test mode: Selected the last {len(miners_to_query)} miners to query for endpoint: {endpoint}")

            if not miners_to_query:
                logger.warning(f"No miners to query for endpoint {endpoint} after filtering. Hotkeys: {hotkeys}")
                return {}

            # Ensure miner client is available
            if not self.miner_client or self.miner_client.is_closed:
                logger.warning("Miner client not available or closed, creating new one")
                self._setup_http_clients()

            # Use chunked processing to reduce database contention and memory spikes
            chunk_size = 50
            chunk_concurrency = 15
            chunks = []
            
            # Split miners into chunks
            miners_list = list(miners_to_query.items())
            for i in range(0, len(miners_list), chunk_size):
                chunk = dict(miners_list[i:i + chunk_size])
                chunks.append(chunk)
            
            logger.info(f"Processing {len(miners_to_query)} miners in {len(chunks)} chunks of {chunk_size} (concurrency: {chunk_concurrency} per chunk)")

            # Configuration for immediate retries
            max_retries_per_miner = 2
            base_timeout = 15.0
            
            async def query_single_miner_with_retries(miner_hotkey: str, node, semaphore: asyncio.Semaphore) -> Optional[Dict]:
                """Query a single miner with immediate retries on failure."""
                base_url = f"https://{node.ip}:{node.port}"
                process = psutil.Process() if PSUTIL_AVAILABLE else None
                
                async with semaphore:
                    for attempt in range(max_retries_per_miner):
                        attempt_timeout = base_timeout + (attempt * 5.0)
                        
                        try:
                            logger.debug(f"Miner {miner_hotkey} attempt {attempt + 1}/{max_retries_per_miner}")
                            
                            # Perform handshake
                            handshake_start_time = time.time()
                            try:
                                symmetric_key_str, symmetric_key_uuid = await self.perform_handshake_with_retry(
                                    self.miner_client,
                                    base_url,
                                    keypair,
                                    miner_hotkey,
                                    max_retries=1,
                                    base_timeout=attempt_timeout
                                )
                                    
                            except Exception as hs_err:
                                logger.debug(f"Handshake failed for miner {miner_hotkey} attempt {attempt + 1}: {type(hs_err).__name__}")
                                if attempt < max_retries_per_miner - 1:
                                    await asyncio.sleep(0.5 * (attempt + 1))
                                    continue
                                return {"hotkey": miner_hotkey, "status": "failed", "reason": "Handshake Error", "details": f"{type(hs_err).__name__}"}

                            handshake_duration = time.time() - handshake_start_time
                            logger.debug(f"Handshake with {miner_hotkey} completed in {handshake_duration:.2f}s (attempt {attempt + 1})")

                            if process:
                                logger.debug(f"Memory after handshake ({miner_hotkey}): {process.memory_info().rss / (1024*1024):.2f} MB")
                            
                            from cryptography.fernet import Fernet
                            fernet = Fernet(symmetric_key_str)
                            
                            # Make the actual request
                            try:
                                logger.debug(f"Making request to {miner_hotkey} (attempt {attempt + 1})")
                                request_start_time = time.time()
                                
                                resp = await asyncio.wait_for(
                                    vali_client.make_non_streamed_post(
                                        httpx_client=self.miner_client,
                                        server_address=base_url,
                                        fernet=fernet,
                                        keypair=keypair,
                                        symmetric_key_uuid=symmetric_key_uuid,
                                        validator_ss58_address=keypair.ss58_address,
                                        miner_ss58_address=miner_hotkey,
                                        payload=payload,
                                        endpoint=endpoint,
                                    ),
                                    timeout=240.0
                                )
                                request_duration = time.time() - request_start_time
                                
                                # Immediate cleanup of cryptographic objects
                                try:
                                    if hasattr(fernet, '__dict__'):
                                        fernet.__dict__.clear()
                                    del fernet
                                    del symmetric_key_str
                                    del symmetric_key_uuid
                                except Exception:
                                    pass
                                
                                if resp and resp.status_code < 400:
                                    response_data = {
                                        "status": "success",
                                        "text": resp.text,
                                        "status_code": resp.status_code,
                                        "hotkey": miner_hotkey,
                                        "port": node.port,
                                        "ip": node.ip,
                                        "duration": request_duration,
                                        "content_length": len(resp.content) if resp.content else 0,
                                        "attempts_used": attempt + 1
                                    }
                                    logger.info(f"SUCCESS: {miner_hotkey} responded in {request_duration:.2f}s (attempt {attempt + 1})")
                                    return response_data
                                else:
                                    logger.debug(f"Bad response from {miner_hotkey} attempt {attempt + 1}: status {resp.status_code if resp else 'None'}")
                                    if attempt < max_retries_per_miner - 1:
                                        await asyncio.sleep(0.5 * (attempt + 1))
                                        continue
                                    return {"hotkey": miner_hotkey, "status": "failed", "reason": "Bad Response", "details": f"Status code {resp.status_code if resp else 'N/A'}"}

                            except asyncio.TimeoutError:
                                if attempt < max_retries_per_miner - 1:
                                    await asyncio.sleep(0.5 * (attempt + 1))
                                    continue
                                return {"hotkey": miner_hotkey, "status": "failed", "reason": "Request Timeout", "details": "Timeout during non-streamed POST"}
                            except Exception as request_error:
                                logger.debug(f"Request error for {miner_hotkey} attempt {attempt + 1}: {type(request_error).__name__}")
                                if attempt < max_retries_per_miner - 1:
                                    await asyncio.sleep(0.5 * (attempt + 1))
                                    continue
                                return {"hotkey": miner_hotkey, "status": "failed", "reason": "Request Error", "details": f"{type(request_error).__name__}"}
                                
                        except Exception as outer_error:
                            logger.debug(f"Outer error for {miner_hotkey} attempt {attempt + 1}: {type(outer_error).__name__} - {outer_error}")
                            if attempt < max_retries_per_miner - 1:
                                await asyncio.sleep(0.5 * (attempt + 1))
                                continue
                            return {"hotkey": miner_hotkey, "status": "failed", "reason": "Outer Error", "details": f"{type(outer_error).__name__}"}
                    
                    return {"hotkey": miner_hotkey, "status": "failed", "reason": "All Attempts Failed", "details": "Fell through retry loop"}

            # Process miners in chunks
            logger.info(f"Starting chunked processing of {len(chunks)} chunks...")
            start_time = time.time()
            all_results = []
            
            for chunk_idx, chunk_miners in enumerate(chunks):
                chunk_start_time = time.time()
                logger.info(f"Processing chunk {chunk_idx + 1}/{len(chunks)} with {len(chunk_miners)} miners...")
                
                # Create semaphore for this chunk
                chunk_semaphore = asyncio.Semaphore(chunk_concurrency)
                
                # Create tasks for this chunk only
                chunk_tasks = []
                for hotkey, node in chunk_miners.items():
                    if node.ip and node.port:
                        task = asyncio.create_task(
                            query_single_miner_with_retries(hotkey, node, chunk_semaphore),
                            name=f"query_chunk{chunk_idx}_{hotkey[:8]}"
                        )
                        chunk_tasks.append((hotkey, task))
                    else:
                        logger.warning(f"Skipping miner {hotkey} - missing IP or port")
                        all_results.append({"hotkey": hotkey, "status": "failed", "reason": "Missing IP/Port", "details": "No IP or port available"})

                # Process this chunk's responses
                chunk_results = []
                
                # Create a mapping for this chunk
                task_to_hotkey = {task: hotkey for hotkey, task in chunk_tasks}
                just_tasks = [task for _, task in chunk_tasks]
                
                if just_tasks:
                    for completed_task in asyncio.as_completed(just_tasks):
                        try:
                            result = await completed_task
                            chunk_results.append(result)
                            all_results.append(result)
                        except Exception as e:
                            task_hotkey = task_to_hotkey.get(completed_task, "unknown")
                            error_result = {"hotkey": task_hotkey, "status": "failed", "reason": "Task Exception", "details": str(e)}
                            chunk_results.append(error_result)
                            all_results.append(error_result)
                            logger.debug(f"Exception processing response in chunk {chunk_idx + 1}: {e}")

                chunk_time = time.time() - chunk_start_time
                chunk_successful = sum(1 for r in chunk_results if r and r.get('status') == 'success')
                chunk_failed = len(chunk_results) - chunk_successful
                logger.info(f"Chunk {chunk_idx + 1}/{len(chunks)} completed: {chunk_successful} successful, {chunk_failed} failed in {chunk_time:.2f}s")
                
                # Memory cleanup between chunks
                try:
                    del chunk_tasks
                    del task_to_hotkey
                    del just_tasks
                    del chunk_results
                    del chunk_semaphore
                    
                    import gc
                    collected = gc.collect()
                    if collected > 20:
                        logger.debug(f"GC collected {collected} objects after chunk {chunk_idx + 1}")
                except Exception as cleanup_err:
                    logger.debug(f"Error during chunk cleanup: {cleanup_err}")
                
                # Small delay between chunks
                if chunk_idx < len(chunks) - 1:
                    await asyncio.sleep(0.5)

            total_time = time.time() - start_time
            
            # Process results
            successful_results = [r for r in all_results if r and r.get('status') == 'success']
            failed_results = [r for r in all_results if not r or r.get('status') == 'failed']
            
            responses = {res['hotkey']: res for res in successful_results}
            
            total_queries = len(all_results)
            success_count = len(successful_results)
            failed_count = len(failed_results)
            success_rate = success_count / total_queries * 100 if total_queries > 0 else 0

            logger.info(f"Miner query summary: {success_count}/{total_queries} successful ({success_rate:.1f}%) in {total_time:.2f}s")
            
            # Clean up connections after query batch completes
            await self.cleanup_idle_connections()
            
            return responses

        except Exception as e:
            logger.error(f"Error querying miners: {e}")
            logger.error(traceback.format_exc())
            return {}

    async def cleanup_idle_connections(self):
        """Clean up idle connections in the HTTP client pool."""
        try:
            if self.miner_client and not self.miner_client.is_closed:
                # Force close idle connections by accessing the transport pool
                if hasattr(self.miner_client, '_transport') and hasattr(self.miner_client._transport, '_pool'):
                    pool = self.miner_client._transport._pool
                    if hasattr(pool, '_connections'):
                        connections = pool._connections
                        closed_count = 0
                        
                        # Handle both dict and list cases
                        if hasattr(connections, 'items'):  # Dict-like
                            connection_items = list(connections.items())
                            for key, conn in connection_items:
                                if (hasattr(conn, 'is_idle') and conn.is_idle() and 
                                    hasattr(conn, '_idle_time') and 
                                    getattr(conn, '_idle_time', 0) > 60):  # 60 seconds idle
                                    try:
                                        await conn.aclose()
                                        del connections[key]
                                        closed_count += 1
                                        logger.debug(f"Closed stale idle connection to {key}")
                                    except Exception as e:
                                        logger.debug(f"Error closing idle connection: {e}")
                        else:  # List-like
                            for conn in list(connections):
                                if (hasattr(conn, 'is_idle') and conn.is_idle() and 
                                    hasattr(conn, '_idle_time') and 
                                    getattr(conn, '_idle_time', 0) > 60):  # 60 seconds idle
                                    try:
                                        await conn.aclose()
                                        connections.remove(conn)
                                        closed_count += 1
                                        logger.debug(f"Closed stale idle connection")
                                    except Exception as e:
                                        logger.debug(f"Error closing idle connection: {e}")
                    
                        if closed_count > 0:
                            logger.info(f"Cleaned up {closed_count} stale idle connections")
                        
        except Exception as e:
            logger.debug(f"Error during connection cleanup: {e}")

    async def monitor_client_health(self):
        """Monitor HTTP client connection pool health."""
        while True:
            try:
                if self.miner_client and hasattr(self.miner_client, '_transport'):
                    transport = self.miner_client._transport
                    if hasattr(transport, '_pool'):
                        pool = transport._pool
                        if hasattr(pool, '_connections'):
                            connections = pool._connections
                            total_connections = len(connections)
                            
                            # Count different connection states
                            keepalive_connections = 0
                            idle_connections = 0
                            active_connections = 0
                            unique_hosts = set()
                            
                            # Handle both list and dict cases for _connections
                            if hasattr(connections, 'values'):  # Dict-like
                                connection_items = connections.values()
                            else:  # List-like
                                connection_items = connections
                            
                            for conn in connection_items:
                                # Count keepalive connections
                                if hasattr(conn, '_keepalive_expiry') and conn._keepalive_expiry:
                                    keepalive_connections += 1
                                
                                # Count idle connections
                                if hasattr(conn, 'is_idle') and callable(conn.is_idle):
                                    try:
                                        if conn.is_idle():
                                            idle_connections += 1
                                        else:
                                            active_connections += 1
                                    except Exception:
                                        pass
                                
                                # Track unique hosts
                                if hasattr(conn, '_origin') and conn._origin:
                                    unique_hosts.add(str(conn._origin))
                            
                            # Get pool limits
                            pool_limit = "unknown"
                            keepalive_limit = "unknown"
                            if hasattr(self.miner_client, '_limits'):
                                if hasattr(self.miner_client._limits, 'max_connections'):
                                    pool_limit = self.miner_client._limits.max_connections
                                if hasattr(self.miner_client._limits, 'max_keepalive_connections'):
                                    keepalive_limit = self.miner_client._limits.max_keepalive_connections
                            
                            # Log detailed information
                            log_level = logger.info if total_connections > 75 else logger.debug
                            log_level(f"HTTP Client Pool Health - "
                                    f"Total: {total_connections}/{pool_limit}, "
                                    f"Keepalive: {keepalive_connections}/{keepalive_limit}, "
                                    f"Idle: {idle_connections}, Active: {active_connections}, "
                                    f"Unique hosts: {len(unique_hosts)}")
                            
                            # Trigger cleanup if excessive idle connections
                            if idle_connections > 30:
                                logger.info(f"High idle connection count ({idle_connections}), triggering cleanup")
                                try:
                                    await self.cleanup_idle_connections()
                                except Exception as e:
                                    logger.warning(f"Error during automatic connection cleanup: {e}")
                            
                await asyncio.sleep(300)  # Check every 5 minutes
            except Exception as e:
                logger.debug(f"Error monitoring client health: {e}")
                await asyncio.sleep(300)

    async def cleanup(self):
        """Clean up HTTP clients and connections."""
        try:
            if self.miner_client and not self.miner_client.is_closed:
                try:
                    await self.cleanup_idle_connections()
                except Exception:
                    pass
                await self.miner_client.aclose()
                logger.info("Closed miner HTTP client")
            
            if self.api_client and not self.api_client.is_closed:
                await self.api_client.aclose()
                logger.info("Closed API HTTP client")
                
        except Exception as e:
            logger.error(f"Error during miner communicator cleanup: {e}")