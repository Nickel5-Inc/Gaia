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

from gaia.database.database_manager import DatabaseTimeout
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None 
    PSUTIL_AVAILABLE = False
    print("psutil not found, memory logging will be skipped.")

import ssl
import traceback
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
import math
import numpy as np
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

logger = get_logger(__name__)


async def perform_handshake_with_retry(
    httpx_client: httpx.AsyncClient,
    server_address: str,
    keypair: Any,
    miner_hotkey_ss58_address: str,
    max_retries: int = 2,
    base_timeout: float = 15.0
) -> tuple[str, str]:
    """
    Custom handshake function with retry logic and progressive timeouts.
    
    Args:
        httpx_client: The HTTP client to use
        server_address: Miner server address
        keypair: Validator keypair
        miner_hotkey_ss58_address: Miner's hotkey address
        max_retries: Maximum number of retry attempts
        base_timeout: Base timeout for handshake operations
    
    Returns:
        Tuple of (symmetric_key_str, symmetric_key_uuid)
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
                wait_time = 1.0 * (attempt + 1)  # Progressive backoff
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


class HTTPClientManager:
    """Manages HTTP client operations for validator-miner communication."""
    
    def __init__(self):
        """Initialize HTTP client manager independently."""
        self.miner_client = None
        self.api_client = None
        self._last_memory = 0
        logger.info("HTTP Client Manager initialized")
    
    async def setup_clients(self):
        """Setup HTTP clients for miner and API communication."""
        try:
            logger.info("Setting up HTTP clients...")
            
            # Create SSL context that doesn't verify certificates
            import ssl
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            # Setup miner client
            self.miner_client = httpx.AsyncClient(
                timeout=httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=5.0),
                follow_redirects=True,
                verify=False,
                limits=httpx.Limits(
                    max_connections=100,
                    max_keepalive_connections=50,
                    keepalive_expiry=300,
                ),
                transport=httpx.AsyncHTTPTransport(
                    retries=2,
                    verify=False,
                ),
            )
            
            # Setup API client
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
            
            logger.info("HTTP clients setup completed successfully")
            
        except Exception as e:
            logger.error(f"Error setting up HTTP clients: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup HTTP client resources."""
        try:
            if self.miner_client and not self.miner_client.is_closed:
                await self._cleanup_idle_connections()
                await self.miner_client.aclose()
                logger.info("Closed miner HTTP client")
            
            if self.api_client and not self.api_client.is_closed:
                await self.api_client.aclose()
                logger.info("Closed API HTTP client")
                
        except Exception as e:
            logger.error(f"Error during HTTP client cleanup: {e}")

    async def query_miners(self, payload: Dict, endpoint: str, neuron_manager, hotkeys: Optional[List[str]] = None) -> Dict:
        """Query miners with the given payload in parallel with batch retry logic."""
        try:
            logger.info(f"Querying miners for endpoint {endpoint} with payload size: {len(str(payload))} bytes. Specified hotkeys: {hotkeys if hotkeys else 'All/Default'}")
            if "data" in payload and "combined_data" in payload["data"]:
                logger.debug(f"TIFF data size before serialization: {len(payload['data']['combined_data'])} bytes")
                if isinstance(payload["data"]["combined_data"], bytes):
                    logger.debug(f"TIFF header before serialization: {payload['data']['combined_data'][:4]}")

            responses = {}
            
            # Log payload memory footprint early
            try:
                import sys
                payload_size_mb = sys.getsizeof(payload) / (1024 * 1024)
                if payload_size_mb > 50:  # Log large payloads early
                    logger.warning(f"Large payload detected: {payload_size_mb:.1f}MB - will reuse same reference for all miners")
            except Exception:
                pass
            
            current_time = time.time()
            if neuron_manager.metagraph is None or current_time - neuron_manager.last_metagraph_sync > neuron_manager.metagraph_sync_interval:
                logger.info(f"Metagraph not initialized or sync interval ({neuron_manager.metagraph_sync_interval}s) exceeded. Syncing metagraph before querying miners. Last sync: {current_time - neuron_manager.last_metagraph_sync if neuron_manager.metagraph else 'Never'}s ago.")
                try:
                    await asyncio.wait_for(neuron_manager._sync_metagraph(), timeout=60.0) 
                except asyncio.TimeoutError:
                    logger.error("Metagraph sync timed out within query_miners. Proceeding with potentially stale metagraph.")
                except Exception as e_sync:
                    logger.error(f"Error during metagraph sync in query_miners: {e_sync}. Proceeding with potentially stale metagraph.")
            else:
                logger.debug(f"Metagraph recently synced. Skipping sync for this query_miners call. Last sync: {current_time - neuron_manager.last_metagraph_sync:.2f}s ago.")

            if not neuron_manager.metagraph or not neuron_manager.metagraph.nodes:
                logger.error("Metagraph not available or no nodes in metagraph after sync attempt. Cannot query miners.")
                return {}

            nodes_to_consider = neuron_manager.metagraph.nodes
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
                if neuron_manager.args.test and len(miners_to_query) > 10:
                    selected_hotkeys_for_test = list(miners_to_query.keys())[-10:]
                    miners_to_query = {k: miners_to_query[k] for k in selected_hotkeys_for_test}
                    logger.info(f"Test mode: Selected the last {len(miners_to_query)} miners to query for endpoint: {endpoint} (no specific hotkeys provided).")
                elif not neuron_manager.args.test:
                    logger.info(f"Querying all {len(miners_to_query)} available miners for endpoint: {endpoint} (no specific hotkeys provided).")

            if not miners_to_query:
                logger.warning(f"No miners to query for endpoint {endpoint} after filtering. Hotkeys: {hotkeys}")
                return {}

            # Ensure miner client is available
            if not self.miner_client or self.miner_client.is_closed:
                logger.warning("Miner client not available or closed, recreating it")
                # Properly close old client if it exists
                if self.miner_client and not self.miner_client.is_closed:
                    try:
                        await self.miner_client.aclose()
                        logger.debug("Closed old miner client before creating new one")
                    except Exception as e:
                        logger.debug(f"Error closing old miner client: {e}")
                
                # Create SSL context that doesn't verify certificates
                import ssl
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                self.miner_client = httpx.AsyncClient(
                    timeout=httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=5.0),
                    follow_redirects=True,
                    verify=False,
                    limits=httpx.Limits(
                        max_connections=100,  # Restore higher limit for 200+ miners
                        max_keepalive_connections=50,  # Allow more keepalive for efficiency
                        keepalive_expiry=300,  # 5 minutes - good balance for regular queries
                    ),
                    transport=httpx.AsyncHTTPTransport(
                        retries=2,  # Reduced from 3
                        verify=False,  # Explicitly set verify=False on transport
                    ),
                )

            # Use chunked processing to reduce database contention and memory spikes
            chunk_size = 50  # Process miners in chunks of 50
            chunk_concurrency = 15  # Lower concurrency per chunk to reduce DB pressure
            chunks = []
            
            # Split miners into chunks
            miners_list = list(miners_to_query.items())
            for i in range(0, len(miners_list), chunk_size):
                chunk = dict(miners_list[i:i + chunk_size])
                chunks.append(chunk)
            
            logger.info(f"Processing {len(miners_to_query)} miners in {len(chunks)} chunks of {chunk_size} (concurrency: {chunk_concurrency} per chunk)")

            # Configuration for immediate retries
            max_retries_per_miner = 2  # Total of 2 attempts (1 initial + 1 retry)
            base_timeout = 15.0
            
            async def query_single_miner_with_retries(miner_hotkey: str, node, semaphore: asyncio.Semaphore) -> Optional[Dict]:
                """Query a single miner with immediate retries on failure."""
                base_url = f"https://{node.ip}:{node.port}"
                process = psutil.Process() if PSUTIL_AVAILABLE else None
                
                async with semaphore: # Acquire semaphore before starting attempts for a miner
                    for attempt in range(max_retries_per_miner):
                        attempt_timeout = base_timeout + (attempt * 5.0)  # Progressive timeout
                        
                        try:
                            logger.debug(f"Miner {miner_hotkey} attempt {attempt + 1}/{max_retries_per_miner}")
                            
                            # Perform handshake
                            handshake_start_time = time.time()
                            try:
                                # Get public key
                                public_key_encryption_key = await asyncio.wait_for(
                                    handshake.get_public_encryption_key(
                                        self.miner_client, 
                                        base_url, 
                                        timeout=int(attempt_timeout)
                                    ),
                                    timeout=attempt_timeout
                                )
                                
                                # Generate symmetric key
                                symmetric_key: bytes = os.urandom(32)
                                symmetric_key_uuid: str = os.urandom(32).hex()
                                
                                # Send symmetric key
                                success = await asyncio.wait_for(
                                    handshake.send_symmetric_key_to_server(
                                        self.miner_client,
                                        base_url,
                                        neuron_manager.keypair,
                                        public_key_encryption_key,
                                        symmetric_key,
                                        symmetric_key_uuid,
                                        miner_hotkey,
                                        timeout=int(attempt_timeout),
                                    ),
                                    timeout=attempt_timeout
                                )
                                
                                if not success:
                                    raise Exception("Handshake failed: server returned unsuccessful status")
                                    
                                symmetric_key_str = base64.b64encode(symmetric_key).decode()
                                    
                            except Exception as hs_err:
                                logger.debug(f"Handshake failed for miner {miner_hotkey} attempt {attempt + 1}: {type(hs_err).__name__}")
                                if attempt < max_retries_per_miner - 1:
                                    await asyncio.sleep(0.5 * (attempt + 1))  # Brief delay before retry
                                    continue
                                return {"hotkey": miner_hotkey, "status": "failed", "reason": "Handshake Error", "details": f"{type(hs_err).__name__}"}

                            handshake_duration = time.time() - handshake_start_time
                            logger.debug(f"Handshake with {miner_hotkey} completed in {handshake_duration:.2f}s (attempt {attempt + 1})")

                            if process:
                                logger.debug(f"Memory after handshake ({miner_hotkey}): {process.memory_info().rss / (1024*1024):.2f} MB")
                            
                            fernet = Fernet(symmetric_key_str)
                            
                            # Make the actual request - REUSE THE SAME PAYLOAD REFERENCE
                            try:
                                logger.debug(f"Making request to {miner_hotkey} (attempt {attempt + 1})")
                                request_start_time = time.time()
                                
                                resp = await asyncio.wait_for(
                                    vali_client.make_non_streamed_post(
                                        httpx_client=self.miner_client,
                                        server_address=base_url,
                                        fernet=fernet,
                                        keypair=neuron_manager.keypair,
                                        symmetric_key_uuid=symmetric_key_uuid,
                                        validator_ss58_address=neuron_manager.keypair.ss58_address,
                                        miner_ss58_address=miner_hotkey,
                                        payload=payload,  # REUSE SAME PAYLOAD REFERENCE - NO COPYING!
                                        endpoint=endpoint,
                                    ),
                                    timeout=240.0  # Keep longer timeout for actual request
                                )
                                request_duration = time.time() - request_start_time
                                
                                # Immediate cleanup of cryptographic objects and large variables
                                try:
                                    # Clear Fernet cipher and symmetric key data
                                    if hasattr(fernet, '__dict__'):
                                        fernet.__dict__.clear()
                                    del fernet
                                    
                                    # Clear symmetric key variables
                                    symmetric_key = None
                                    del symmetric_key_str
                                    del symmetric_key_uuid
                                    
                                    # Clear request response data if it's large
                                    if resp and hasattr(resp, 'content') and len(resp.content) > 1024*1024:  # > 1MB
                                        logger.debug(f"Clearing large response content for {miner_hotkey}: {len(resp.content)} bytes")
                                        
                                except Exception as cleanup_err:
                                    logger.debug(f"Non-critical cleanup error for {miner_hotkey}: {cleanup_err}")
                                
                                if process:
                                    mem_after = process.memory_info().rss / (1024*1024)
                                    logger.debug(f"Memory after request ({miner_hotkey}): {mem_after:.2f} MB")
                                
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
                                    return response_data # Success, return immediately
                                else:
                                    logger.debug(f"Bad response from {miner_hotkey} attempt {attempt + 1}: status {resp.status_code if resp else 'None'}")
                                    if attempt < max_retries_per_miner - 1:
                                        await asyncio.sleep(0.5 * (attempt + 1))
                                        continue # Go to next attempt for this miner
                                    return {"hotkey": miner_hotkey, "status": "failed", "reason": "Bad Response", "details": f"Status code {resp.status_code if resp else 'N/A'}"}

                            except asyncio.TimeoutError:
                                # This is a specific type of request error, so we can categorize it
                                if attempt < max_retries_per_miner - 1:
                                    await asyncio.sleep(0.5 * (attempt + 1))
                                    continue
                                return {"hotkey": miner_hotkey, "status": "failed", "reason": "Request Timeout", "details": "Timeout during non-streamed POST"}
                            except Exception as request_error:
                                # Enhanced cleanup on error
                                try:
                                    if 'fernet' in locals() and fernet:
                                        if hasattr(fernet, '__dict__'):
                                            fernet.__dict__.clear()
                                        del fernet
                                    if 'symmetric_key_str' in locals():
                                        del symmetric_key_str
                                    if 'symmetric_key_uuid' in locals():
                                        del symmetric_key_uuid
                                    if 'symmetric_key' in locals():
                                        symmetric_key = None
                                except Exception:
                                    pass
                                    
                                logger.debug(f"Request error for {miner_hotkey} attempt {attempt + 1}: {type(request_error).__name__}")
                                if attempt < max_retries_per_miner - 1:
                                    await asyncio.sleep(0.5 * (attempt + 1))
                                    continue # Go to next attempt for this miner
                                return {"hotkey": miner_hotkey, "status": "failed", "reason": "Request Error", "details": f"{type(request_error).__name__}"}
                                
                        except Exception as outer_error: # Catch errors within the attempt loop but outside handshake/request
                            logger.debug(f"Outer error for {miner_hotkey} attempt {attempt + 1}: {type(outer_error).__name__} - {outer_error}")
                            if attempt < max_retries_per_miner - 1:
                                await asyncio.sleep(0.5 * (attempt + 1))
                                continue # Go to next attempt for this miner
                            return {"hotkey": miner_hotkey, "status": "failed", "reason": "Outer Error", "details": f"{type(outer_error).__name__}"}
                    
                    # If loop finishes, all attempts for this miner failed (should be caught by returns above but as a fallback)
                    logger.debug(f"All {max_retries_per_miner} attempts failed for {miner_hotkey} (fallback).")
                    return {"hotkey": miner_hotkey, "status": "failed", "reason": "All Attempts Failed", "details": "Fell through retry loop"}
                # Semaphore is automatically released when async with block exits

            # Process miners in chunks to reduce database contention and memory usage
            logger.info(f"Starting chunked processing of {len(chunks)} chunks...")
            start_time = time.time()
            successful_responses = {}  # Final results only
            
            # Log memory before launching queries
            self._log_memory_usage("query_miners_start")
            
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

                # Process this chunk's responses
                completed_count = 0
                
                # Create a mapping for this chunk
                task_to_hotkey = {task: hotkey for hotkey, task in chunk_tasks}
                just_tasks = [task for _, task in chunk_tasks]
                
                if just_tasks:  # Only process if we have tasks
                    for completed_task in asyncio.as_completed(just_tasks):
                        completed_count += 1
                        try:
                            result = await completed_task
                            if result and result.get('status') == 'success':
                                # Only keep successful results in final collection
                                successful_responses[result['hotkey']] = result
                        except Exception as e:
                            task_hotkey = task_to_hotkey.get(completed_task, "unknown")
                            logger.debug(f"Exception processing response in chunk {chunk_idx + 1}: {e}")

                chunk_time = time.time() - chunk_start_time
                chunk_successful = len([r for r in successful_responses.values() if r.get('hotkey') in chunk_miners])
                chunk_failed = len(chunk_miners) - chunk_successful
                logger.info(f"Chunk {chunk_idx + 1}/{len(chunks)} completed: {chunk_successful} successful, {chunk_failed} failed in {chunk_time:.2f}s")
                
                # AGGRESSIVE CLEANUP BETWEEN CHUNKS
                try:
                    del chunk_tasks
                    del task_to_hotkey
                    del just_tasks
                    del chunk_semaphore
                    
                    # Force garbage collection between chunks to manage memory
                    if chunk_idx % 3 == 0:  # Every 3 chunks
                        import gc
                        collected = gc.collect()
                        if collected > 20:
                            logger.debug(f"GC collected {collected} objects after chunk {chunk_idx + 1}")
                except Exception as cleanup_err:
                    logger.debug(f"Error during chunk cleanup: {cleanup_err}")
                
                # Small delay between chunks to allow database recovery
                if chunk_idx < len(chunks) - 1:  # Don't delay after the last chunk
                    await asyncio.sleep(0.5)  # 500ms delay between chunks

            total_time = time.time() - start_time
            
            # Final summary logging
            total_queries = sum(len(chunk) for chunk in chunks)
            success_count = len(successful_responses)
            failed_count = total_queries - success_count
            success_rate = success_count / total_queries * 100 if total_queries > 0 else 0
            avg_rate = total_queries / total_time if total_time > 0 else 0

            logger.info(f"Query Summary - Total: {total_queries}, Success: {success_count} ({success_rate:.1f}%), "
                       f"Failed: {failed_count}, Time: {total_time:.2f}s, Rate: {avg_rate:.1f} queries/sec")
            
            # FINAL AGGRESSIVE CLEANUP
            try:
                # Clear all chunk references
                del chunks
                del miners_list
                del miners_to_query  # Clear the large metagraph subset
                
                # Force final garbage collection
                import gc
                collected = gc.collect()
                if collected > 100:  # Log significant cleanup
                    logger.info(f"Final cleanup: GC collected {collected} objects after miner queries")
                        
            except Exception as cleanup_error:
                logger.warning(f"Error during final query cleanup: {cleanup_error}")
            
            # Log memory after cleanup to verify effectiveness
            self._log_memory_usage("query_miners_end")
            
            # Clean up connections after query batch completes
            await self._cleanup_idle_connections()
            
            return successful_responses

        except Exception as e:
            logger.error(f"Error querying miners: {e}")
            logger.error(traceback.format_exc())
            return {}

    async def _cleanup_idle_connections(self):
        """Clean up idle connections in the HTTP client pool, but only stale ones."""
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
                                # Only close connections that have been idle for a while
                                # This preserves recent connections for potential reuse
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
                        else:
                            logger.debug("No stale connections found to clean up")
                        
        except Exception as e:
            logger.debug(f"Error during connection cleanup: {e}")
    
    async def monitor_client_health(self):
        """Monitor HTTP client health and connection status."""
        try:
            # Check miner client health
            if self.miner_client:
                if self.miner_client.is_closed:
                    logger.warning("Miner client is closed unexpectedly")
                else:
                    logger.debug("Miner client is healthy")
            else:
                logger.warning("Miner client is not initialized")
            
            # Check API client health
            if self.api_client:
                if self.api_client.is_closed:
                    logger.warning("API client is closed unexpectedly")
                else:
                    logger.debug("API client is healthy")
            else:
                logger.warning("API client is not initialized")
                
        except Exception as e:
            logger.error(f"Error monitoring client health: {e}")

    def _log_memory_usage(self, context: str, threshold_mb: float = 100.0):
        """Enhanced memory logging with detailed breakdown and automatic cleanup."""
        if not PSUTIL_AVAILABLE:
            return
            
        try:
            process = psutil.Process()
            current_memory = process.memory_info().rss / (1024 * 1024)
            
            # Calculate memory change
            if not hasattr(self, '_last_memory'):
                self._last_memory = current_memory
                memory_change = 0
            else:
                memory_change = current_memory - self._last_memory
                self._last_memory = current_memory
            
            # Enhanced logging for significant changes
            if abs(memory_change) > threshold_mb or context in ['calc_weights_start', 'calc_weights_after_cleanup']:
                # Get additional memory details
                memory_info = process.memory_info()
                try:
                    # Try to get memory percentage
                    memory_percent = process.memory_percent()
                    
                    # Get thread and file handle counts
                    num_threads = process.num_threads()
                    open_files = len(process.open_files())
                    
                    # Check if we have tracemalloc data
                    tracemalloc_info = ""
                    if tracemalloc.is_tracing():
                        try:
                            current, peak = tracemalloc.get_traced_memory()
                            tracemalloc_info = f", Traced: {current/(1024*1024):.1f}MB (peak: {peak/(1024*1024):.1f}MB)"
                        except Exception:
                            pass
                    
                    logger.info(
                        f"Memory usage [{context}]: {current_memory:.1f}MB "
                        f"({'+' if memory_change > 0 else ''}{memory_change:.1f}MB), "
                        f"RSS: {memory_info.rss/(1024*1024):.1f}MB, "
                        f"VMS: {memory_info.vms/(1024*1024):.1f}MB, "
                        f"Percent: {memory_percent:.1f}%, "
                        f"Threads: {num_threads}, "
                        f"Files: {open_files}"
                        f"{tracemalloc_info}"
                    )
                    
                    # Trigger comprehensive cleanup for large memory increases (but not during startup)
                    if (memory_change > 200 and 
                        hasattr(self.validator, 'last_metagraph_sync')):  # Only after validator is fully running
                        logger.warning(f"Large memory increase detected ({memory_change:.1f}MB), forcing comprehensive cleanup")
                        memory_freed = self.validator._comprehensive_memory_cleanup(f"emergency_{context}")
                        
                        # Check memory again after comprehensive cleanup
                        new_memory = process.memory_info().rss / (1024 * 1024)
                        total_savings = current_memory - new_memory
                        logger.info(f"Emergency comprehensive cleanup freed {memory_freed:.1f}MB (total reduction: {total_savings:.1f}MB)")
                    elif memory_change > 200:
                        logger.warning(f"Large memory increase during startup ({memory_change:.1f}MB) - cleanup deferred until validator is fully running")
                    
                except Exception as e:
                    logger.debug(f"Error getting detailed memory info: {e}")
                    logger.info(f"Memory usage [{context}]: {current_memory:.1f}MB ({'+' if memory_change > 0 else ''}{memory_change:.1f}MB)")
            else:
                logger.debug(f"Memory usage [{context}]: {current_memory:.1f}MB ({'+' if memory_change > 0 else ''}{memory_change:.1f}MB)")
        except Exception as e:
            logger.debug(f"Error logging memory usage: {e}")

    async def monitor_client_health(self):
        """Monitor HTTP client connection pool health."""
        while not self._shutdown_event.is_set():
            try:
                if hasattr(self.validator, 'miner_client') and hasattr(self.validator.miner_client, '_transport'):
                    transport = self.validator.miner_client._transport
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
                            if hasattr(connections, 'values'):  # It's a dict-like object
                                connection_items = connections.values()
                            else:  # It's a list-like object
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
                                elif hasattr(conn, '_socket') and hasattr(conn._socket, 'getpeername'):
                                    try:
                                        peer = conn._socket.getpeername()
                                        unique_hosts.add(f"{peer[0]}:{peer[1]}")
                                    except Exception:
                                        pass
                            
                            # Get pool limit - check multiple possible locations
                            pool_limit = "unknown"
                            keepalive_limit = "unknown"
                            if hasattr(self.validator.miner_client, '_limits'):
                                if hasattr(self.validator.miner_client._limits, 'max_connections'):
                                    pool_limit = self.validator.miner_client._limits.max_connections
                                if hasattr(self.validator.miner_client._limits, 'max_keepalive_connections'):
                                    keepalive_limit = self.validator.miner_client._limits.max_keepalive_connections
                            
                            # Log detailed information - use INFO level when high connection counts detected
                            log_level = logger.info if total_connections > 75 else logger.debug
                            log_level(f"HTTP Client Pool Health - "
                                    f"Total: {total_connections}/{pool_limit}, "
                                    f"Keepalive: {keepalive_connections}/{keepalive_limit}, "
                                    f"Idle: {idle_connections}, Active: {active_connections}, "
                                    f"Unique hosts: {len(unique_hosts)}")
                            
                            # If we have excessive connections, log the unique hosts
                            if total_connections > 80 and unique_hosts:
                                logger.info(f"Connection pool has {total_connections} connections to {len(unique_hosts)} unique hosts")
                                if len(unique_hosts) <= 15:  # Only log if manageable number
                                    logger.debug(f"Connected hosts: {list(unique_hosts)[:15]}")
                            
                            # Only trigger cleanup if we have excessive idle connections (not just any idle)
                            if idle_connections > 30:
                                logger.info(f"High idle connection count ({idle_connections}), triggering cleanup")
                                try:
                                    await self._cleanup_idle_connections()
                                except Exception as e:
                                    logger.warning(f"Error during automatic connection cleanup: {e}")
                            
                await asyncio.sleep(300)  # Check every 5 minutes
            except Exception as e:
                logger.debug(f"Error monitoring client health: {e}")
                await asyncio.sleep(300) 