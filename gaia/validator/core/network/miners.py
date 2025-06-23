"""
Miner Network Communication
===========================

Clean functions for communicating with miners including querying,
handshakes, and connection management.
"""

import asyncio
import base64
import os
import time
from typing import Dict, List, Optional, Any
import httpx
from fiber.logging_utils import get_logger
from fiber.encrypted.validator import client as vali_client, handshake

from ..config.constants import NETWORK_CONSTANTS

logger = get_logger(__name__)


async def query_miners(
    payload: Dict[str, Any], 
    endpoint: str, 
    miner_client: httpx.AsyncClient,
    metagraph: Any,
    keypair: Any,
    hotkeys: Optional[List[str]] = None,
    test_mode: bool = False
) -> Dict[str, Any]:
    """
    Query miners with the given payload in parallel with clean retry logic.
    
    Args:
        payload: Data to send to miners
        endpoint: API endpoint to query
        miner_client: HTTP client for miner communication
        metagraph: Network metagraph
        keypair: Validator keypair for signing
        hotkeys: Specific hotkeys to query (optional)
        test_mode: Whether running in test mode
        
    Returns:
        Dict of successful miner responses
    """
    logger.info(f"🔍 Querying miners for endpoint {endpoint}")
    
    # Determine miners to query
    miners_to_query = _determine_miners_to_query(metagraph, hotkeys, test_mode)
    if not miners_to_query:
        logger.warning("No miners to query")
        return {}
    
    logger.info(f"📡 Querying {len(miners_to_query)} miners")
    
    # Process miners in chunks
    chunk_size = NETWORK_CONSTANTS['miner_query']['chunk_size']
    chunk_concurrency = NETWORK_CONSTANTS['miner_query']['chunk_concurrency']
    chunk_delay = NETWORK_CONSTANTS['miner_query']['chunk_delay']
    
    chunks = _create_miner_chunks(miners_to_query, chunk_size)
    logger.info(f"📊 Processing {len(chunks)} chunks of {chunk_size} miners each")
    
    successful_responses = {}
    
    for chunk_idx, chunk_miners in enumerate(chunks):
        logger.info(f"🔄 Processing chunk {chunk_idx + 1}/{len(chunks)}")
        
        # Create semaphore for this chunk
        chunk_semaphore = asyncio.Semaphore(chunk_concurrency)
        
        # Create tasks for this chunk
        chunk_tasks = []
        for hotkey, node in chunk_miners.items():
            if node.ip and node.port:
                task = asyncio.create_task(
                    _query_single_miner_with_retries(
                        hotkey, node, payload, endpoint, 
                        miner_client, keypair, chunk_semaphore
                    )
                )
                chunk_tasks.append(task)
        
        # Process chunk responses
        if chunk_tasks:
            results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, dict) and result.get('status') == 'success':
                    successful_responses[result['hotkey']] = result
        
        # Delay between chunks
        if chunk_idx < len(chunks) - 1:
            await asyncio.sleep(chunk_delay)
    
    success_count = len(successful_responses)
    total_queries = sum(len(chunk) for chunk in chunks)
    success_rate = success_count / total_queries * 100 if total_queries > 0 else 0
    
    logger.info(f"✅ Query complete: {success_count}/{total_queries} successful ({success_rate:.1f}%)")
    
    return successful_responses


async def cleanup_idle_connections(miner_client: httpx.AsyncClient) -> None:
    """Clean up idle connections in the HTTP client pool."""
    try:
        if not miner_client or miner_client.is_closed:
            return
            
        # Access the transport pool
        if hasattr(miner_client, '_transport') and hasattr(miner_client._transport, '_pool'):
            pool = miner_client._transport._pool
            if hasattr(pool, '_connections'):
                connections = pool._connections
                closed_count = 0
                
                # Handle both dict and list cases
                if hasattr(connections, 'items'):  # Dict-like
                    connection_items = list(connections.items())
                    for key, conn in connection_items:
                        if _should_close_connection(conn):
                            try:
                                await conn.aclose()
                                del connections[key]
                                closed_count += 1
                            except Exception:
                                pass
                else:  # List-like
                    for conn in list(connections):
                        if _should_close_connection(conn):
                            try:
                                await conn.aclose()
                                connections.remove(conn)
                                closed_count += 1
                            except Exception:
                                pass
                
                if closed_count > 0:
                    logger.info(f"🧹 Cleaned up {closed_count} idle connections")
                    
    except Exception as e:
        logger.debug(f"Error during connection cleanup: {e}")


def _determine_miners_to_query(metagraph: Any, hotkeys: Optional[List[str]], test_mode: bool) -> Dict[str, Any]:
    """Determine which miners to query based on parameters."""
    if not metagraph or not metagraph.nodes:
        return {}
    
    nodes_to_consider = metagraph.nodes
    
    if hotkeys:
        # Query specific hotkeys
        miners_to_query = {}
        for hk in hotkeys:
            if hk in nodes_to_consider:
                miners_to_query[hk] = nodes_to_consider[hk]
        return miners_to_query
    
    # Query all or test subset
    miners_to_query = nodes_to_consider
    if test_mode and len(miners_to_query) > 10:
        selected_hotkeys = list(miners_to_query.keys())[-10:]
        miners_to_query = {k: miners_to_query[k] for k in selected_hotkeys}
        logger.info(f"🧪 Test mode: limited to {len(miners_to_query)} miners")
    
    return miners_to_query


def _create_miner_chunks(miners_to_query: Dict[str, Any], chunk_size: int) -> List[Dict[str, Any]]:
    """Split miners into chunks for processing."""
    chunks = []
    miners_list = list(miners_to_query.items())
    
    for i in range(0, len(miners_list), chunk_size):
        chunk = dict(miners_list[i:i + chunk_size])
        chunks.append(chunk)
    
    return chunks


async def _query_single_miner_with_retries(
    miner_hotkey: str,
    node: Any,
    payload: Dict[str, Any],
    endpoint: str,
    miner_client: httpx.AsyncClient,
    keypair: Any,
    semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """Query a single miner with retry logic."""
    base_url = f"https://{node.ip}:{node.port}"
    max_retries = NETWORK_CONSTANTS['retry_config']['max_retries']
    base_timeout = NETWORK_CONSTANTS['retry_config']['base_timeout']
    
    async with semaphore:
        for attempt in range(max_retries):
            attempt_timeout = base_timeout + (attempt * 5.0)
            
            try:
                # Perform handshake
                symmetric_key_str, symmetric_key_uuid = await _perform_handshake(
                    miner_client, base_url, keypair, miner_hotkey, attempt_timeout
                )
                
                # Make the request
                response = await _make_encrypted_request(
                    miner_client, base_url, symmetric_key_str, symmetric_key_uuid,
                    keypair, miner_hotkey, payload, endpoint
                )
                
                if response and response.status_code < 400:
                    return {
                        "status": "success",
                        "text": response.text,
                        "status_code": response.status_code,
                        "hotkey": miner_hotkey,
                        "port": node.port,
                        "ip": node.ip,
                        "attempts_used": attempt + 1
                    }
                
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                
                return {
                    "hotkey": miner_hotkey,
                    "status": "failed", 
                    "reason": type(e).__name__,
                    "details": str(e)
                }
        
        return {
            "hotkey": miner_hotkey,
            "status": "failed",
            "reason": "All attempts failed",
            "details": f"Failed after {max_retries} attempts"
        }


async def _perform_handshake(
    miner_client: httpx.AsyncClient,
    base_url: str,
    keypair: Any,
    miner_hotkey: str,
    timeout: float
) -> tuple[str, str]:
    """Perform encrypted handshake with miner."""
    # Get public key
    public_key = await asyncio.wait_for(
        handshake.get_public_encryption_key(
            miner_client, base_url, timeout=int(timeout)
        ),
        timeout=timeout
    )
    
    # Generate symmetric key
    symmetric_key = os.urandom(32)
    symmetric_key_uuid = os.urandom(32).hex()
    
    # Send symmetric key
    success = await asyncio.wait_for(
        handshake.send_symmetric_key_to_server(
            miner_client, base_url, keypair, public_key,
            symmetric_key, symmetric_key_uuid, miner_hotkey,
            timeout=int(timeout)
        ),
        timeout=timeout
    )
    
    if not success:
        raise Exception("Handshake failed")
    
    return base64.b64encode(symmetric_key).decode(), symmetric_key_uuid


async def _make_encrypted_request(
    miner_client: httpx.AsyncClient,
    base_url: str,
    symmetric_key_str: str,
    symmetric_key_uuid: str,
    keypair: Any,
    miner_hotkey: str,
    payload: Dict[str, Any],
    endpoint: str
) -> httpx.Response:
    """Make encrypted request to miner."""
    from cryptography.fernet import Fernet
    
    fernet = Fernet(symmetric_key_str)
    
    try:
        response = await asyncio.wait_for(
            vali_client.make_non_streamed_post(
                httpx_client=miner_client,
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
        return response
    finally:
        # Clean up cryptographic objects
        try:
            if hasattr(fernet, '__dict__'):
                fernet.__dict__.clear()
            del fernet
        except Exception:
            pass


def _should_close_connection(conn: Any) -> bool:
    """Determine if a connection should be closed."""
    # Only close connections idle for more than 60 seconds
    if (hasattr(conn, 'is_idle') and callable(conn.is_idle) and conn.is_idle() and 
        hasattr(conn, '_idle_time') and 
        getattr(conn, '_idle_time', 0) > 60):
        return True
    return False 