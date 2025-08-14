"""
Worker-friendly miner communication utilities with proper fiber handshake.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import base64
import time
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import httpx
from fiber import Keypair
from fiber.encrypted.validator import handshake
from fiber.encrypted.validator import client as vali_client
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)

# Cache duration for symmetric keys (24 hours by default)
SYMMETRIC_KEY_CACHE_HOURS = 24

async def _get_cached_symmetric_key(db, miner_uid: int) -> Optional[Dict[str, Any]]:
    """
    Get cached symmetric key for a miner if it exists and hasn't expired.
    
    Returns:
        Dict with 'key' (base64 encoded), 'uuid', and 'age_minutes' if found, None otherwise
    """
    from datetime import datetime, timezone, timedelta
    
    try:
        query = """
            SELECT fiber_symmetric_key, fiber_symmetric_key_uuid, fiber_key_cached_at
            FROM node_table
            WHERE uid = :uid
            AND fiber_symmetric_key IS NOT NULL
            AND fiber_key_expires_at > NOW()
        """
        result = await db.fetch_one(query, {"uid": miner_uid})
        
        if result and result["fiber_symmetric_key"]:
            age = datetime.now(timezone.utc) - result["fiber_key_cached_at"]
            return {
                "key": result["fiber_symmetric_key"],
                "uuid": result["fiber_symmetric_key_uuid"],
                "age_minutes": age.total_seconds() / 60
            }
    except Exception as e:
        logger.warning(f"Error checking cached symmetric key: {e}")
    
    return None

async def _cache_symmetric_key(db, miner_uid: int, symmetric_key: bytes, symmetric_key_uuid: str):
    """
    Cache a symmetric key for future use.
    """
    from datetime import datetime, timezone, timedelta
    import base64
    
    try:
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=SYMMETRIC_KEY_CACHE_HOURS)
        key_b64 = base64.b64encode(symmetric_key).decode()
        
        query = """
            UPDATE node_table
            SET fiber_symmetric_key = :key,
                fiber_symmetric_key_uuid = :uuid,
                fiber_key_cached_at = :cached_at,
                fiber_key_expires_at = :expires_at
            WHERE uid = :uid
        """
        await db.execute(query, {
            "uid": miner_uid,
            "key": key_b64,
            "uuid": symmetric_key_uuid,
            "cached_at": now,
            "expires_at": expires_at
        })
    except Exception as e:
        logger.warning(f"Error caching symmetric key: {e}")

async def _invalidate_cached_key(db, miner_uid: int):
    """
    Invalidate a cached symmetric key (e.g., after a failed request).
    """
    try:
        query = """
            UPDATE node_table
            SET fiber_symmetric_key = NULL,
                fiber_symmetric_key_uuid = NULL,
                fiber_key_cached_at = NULL,
                fiber_key_expires_at = NULL
            WHERE uid = :uid
        """
        await db.execute(query, {"uid": miner_uid})
        logger.info(f"Invalidated cached key for miner UID {miner_uid}")
    except Exception as e:
        logger.warning(f"Error invalidating cached key: {e}")

async def _get_miner_info(db, miner_hotkey: str) -> Optional[Dict[str, Any]]:
    """
    Get miner information from node_table.
    """
    try:
        query = """
            SELECT uid, ip, port
            FROM node_table
            WHERE hotkey = :hotkey
        """
        result = await db.fetch_one(query, {"hotkey": miner_hotkey})
        if result:
            return dict(result)
    except Exception as e:
        logger.error(f"Error fetching miner info: {e}")
    return None


async def query_single_miner(
    validator: Any,
    miner_hotkey: str,
    endpoint: str,
    payload: Dict[str, Any],
    timeout: float = 30.0,
    db_manager: Any = None,
) -> Optional[Dict[str, Any]]:
    """
    Query a single miner with proper fiber handshake and detailed logging.
    
    Args:
        validator: Validator instance with keypair
        miner_hotkey: Miner's SS58 hotkey
        endpoint: API endpoint (e.g., "/weather-initiate-fetch")
        payload: Request payload
        timeout: Request timeout in seconds
        max_retries: Maximum retry attempts
        db_manager: Database manager instance
        
    Returns:
        Response dict with status, data, and metadata, or None if failed
    """
    start_time = time.time()
    
    # Get miner details
    db = db_manager if db_manager is not None else getattr(validator, 'db_manager', None)
    if db is None:
        logger.error("No database manager available")
        return None
        
    miner_info = await _get_miner_info(db, miner_hotkey)
    if not miner_info:
        logger.warning(f"Miner {miner_hotkey[:8]} not found in node_table")
        return None
        
    miner_uid = miner_info["uid"]
    miner_ip = miner_info["ip"]
    miner_port = miner_info["port"]
    
    if not miner_ip or not miner_port:
        logger.warning(f"Miner {miner_hotkey[:8]} (UID {miner_uid}) has no IP/port")
        return None
        
    # Use HTTPS for fiber handshake
    server_address = f"https://{miner_ip}:{miner_port}"
    
    logger.info(
        f"Starting fiber handshake with miner {miner_hotkey[:8]} (UID {miner_uid}) at {server_address}"
        f"\n  Endpoint: {endpoint}"
        f"\n  Payload keys: {list(payload.keys())}"
    )
    
    # Get validator keypair
    validator_keypair = validator.keypair
    if not validator_keypair:
        logger.error("No validator keypair available")
        return None
    
    # Check for cached symmetric key first
    cached_key_data = await _get_cached_symmetric_key(db, miner_uid)
    symmetric_key = None
    symmetric_key_uuid = None
    
    if cached_key_data:
        logger.info(f"✅ Using cached symmetric key for {miner_hotkey[:8]} (cached {cached_key_data['age_minutes']:.1f} min ago)")
        symmetric_key = base64.b64decode(cached_key_data['key'])
        symmetric_key_uuid = cached_key_data['uuid']
    
    # Single attempt - retries are handled by the job system
    try:
        # Create HTTP client for this request
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            verify=False  # Disable SSL verification for self-signed certs
        ) as client:
            try:
                if not symmetric_key:
                    # Need to perform handshake
                    logger.info(f"🔑 No cached key, performing new handshake with {miner_hotkey[:8]}")
                    
                    # Step 1: Get public encryption key from miner
                    logger.debug(f"🔑 Getting public key from {miner_hotkey[:8]}")
                    public_key_encryption_key = await handshake.get_public_encryption_key(
                        client,
                        server_address,
                        timeout=int(timeout),
                    )
                    
                    # Step 2: Generate symmetric key
                    symmetric_key: bytes = os.urandom(32)
                    symmetric_key_uuid = os.urandom(32).hex()
                    
                    # Step 3: Send symmetric key to server
                    logger.debug(f"🔐 Sending symmetric key to {miner_hotkey[:8]}")
                    success = await handshake.send_symmetric_key_to_server(
                        client,
                        server_address,
                        validator_keypair,
                        public_key_encryption_key,
                        symmetric_key,
                        symmetric_key_uuid,
                        miner_hotkey,
                        timeout=int(timeout),
                    )
                    
                    if not success:
                        raise Exception("Handshake failed: server returned unsuccessful status")
                    
                    # Cache the symmetric key for future use
                    await _cache_symmetric_key(db, miner_uid, symmetric_key, symmetric_key_uuid)
                    logger.info(f"💾 Cached symmetric key for {miner_hotkey[:8]} for future requests")
                    
                    symmetric_key_str = base64.b64encode(symmetric_key).decode()
                    fernet = Fernet(symmetric_key_str)
                    
                    logger.debug(f"✅ Handshake complete with {miner_hotkey[:8]}, making request")
                    
                    # Step 4: Make the actual request with encryption
                    request_start = time.time()
                    
                    response = await vali_client.make_non_streamed_post(
                        httpx_client=client,
                        server_address=server_address,
                        fernet=fernet,
                        keypair=validator_keypair,
                        symmetric_key_uuid=symmetric_key_uuid,
                        validator_ss58_address=validator_keypair.ss58_address,
                        miner_ss58_address=miner_hotkey,
                        payload=payload,
                        endpoint=endpoint,
                    )
                    
                    request_time = time.time() - request_start
                    
                    # Parse response
                    if response and hasattr(response, 'content'):
                        try:
                            data = json.loads(response.content)
                        except json.JSONDecodeError:
                            data = {"raw": response.content.decode('utf-8', errors='ignore')}
                    else:
                        data = {}
                    
                    logger.info(
                        f"✅ Success from {miner_hotkey[:8]} (UID {miner_uid})"
                        f"\n  Time: {request_time:.2f}s"
                        f"\n  Response keys: {list(data.keys()) if isinstance(data, dict) else 'non-dict'}"
                    )
                    
                    return {
                        "success": True,
                        "data": data,
                        "miner_uid": miner_uid,
                        "response_time": request_time,
                        "response_time_ms": int(request_time * 1000),
                    }
                    
            except httpx.ConnectError as e:
                logger.warning(f"Connection failed to {miner_hotkey[:8]} at {server_address}: {e}")
                return {
                    "success": False,
                    "error": f"Connection failed: {e}",
                    "miner_uid": miner_uid,
                }
                
            except httpx.TimeoutException as e:
                logger.warning(f"Timeout querying {miner_hotkey[:8]}: {e}")
                return {
                    "success": False,
                    "error": f"Timeout: {e}",
                    "miner_uid": miner_uid,
                }
                
            except Exception as e:
                logger.error(
                    f"Unexpected error querying {miner_hotkey[:8]}: {e}",
                    exc_info=True
                )
                return {
                    "success": False,
                    "error": f"Unexpected error: {e}",
                    "miner_uid": miner_uid,
                }
                    
    except Exception as e:
        logger.error(f"Failed to create HTTP client: {e}")
        return {
            "success": False,
            "error": f"Client error: {e}",
            "miner_uid": miner_uid,
        }





async def query_miner_for_weather(
    validator: Any,
    miner_hotkey: str,
    forecast_start_time: datetime,
    previous_step_time: datetime,
    validator_hotkey: str,
    db_manager: Any = None,
) -> Optional[Dict[str, Any]]:
    """
    Query a miner to initiate weather forecast with detailed logging.
    
    This is a worker-friendly wrapper around the fiber handshake process.
    """
    # Wrap the data in the expected format for WeatherInitiateFetchRequest
    payload = {
        "data": {
            "forecast_start_time": forecast_start_time.isoformat(),
            "previous_step_time": previous_step_time.isoformat(),
            "validator_hotkey": validator_hotkey,
        }
    }
    
    logger.info(
        f"Initiating weather fetch for {miner_hotkey[:8]}"
        f"\n  Forecast start: {forecast_start_time}"
        f"\n  Previous step: {previous_step_time}"
    )
    
    result = await query_single_miner(
        validator=validator,
        miner_hotkey=miner_hotkey,
        endpoint="/weather-initiate-fetch",
        payload=payload,
        timeout=30.0,
        db_manager=db_manager,
    )
    
    if result and result.get("success"):
        data = result.get("data", {})
        logger.info(
            f"✓ Weather fetch accepted by {miner_hotkey[:8]}"
            f"\n  Job ID: {data.get('job_id')}"
            f"\n  Status: {data.get('status')}"
            f"\n  Response time: {result.get('response_time', 0):.2f}s"
        )
    else:
        error = result.get("error", "Unknown error") if result else "No response"
        logger.warning(
            f"✗ Weather fetch failed for {miner_hotkey[:8]}: {error}"
        )
    
    return result


async def poll_miner_job_status(
    validator: Any,
    miner_hotkey: str,
    job_id: str,
    db_manager: Any = None,
) -> Optional[Dict[str, Any]]:
    """
    Poll a miner for job status with detailed logging.
    """
    # Wrap the data in the expected format for WeatherGetInputStatusRequest
    payload = {
        "data": {
            "job_id": job_id,
        }
    }
    
    logger.info(f"Polling job status for {miner_hotkey[:8]}, job_id: {job_id}")
    
    result = await query_single_miner(
        validator=validator,
        miner_hotkey=miner_hotkey,
        endpoint="/weather-get-input-status",
        payload=payload,
        timeout=15.0,
        db_manager=db_manager,
    )
    
    if result and result.get("success"):
        data = result.get("data", {})
        logger.info(
            f"Job status from {miner_hotkey[:8]}: {data.get('status')}"
            f"\n  Progress: {data.get('progress', 'N/A')}"
            f"\n  Response time: {result.get('response_time', 0):.2f}s"
        )
    else:
        error = result.get("error", "Unknown error") if result else "No response"
        logger.warning(
            f"Failed to get status from {miner_hotkey[:8]}: {error}"
        )
    
    return result
