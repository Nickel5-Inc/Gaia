"""
Worker-friendly miner communication utilities with fiber handshake.
"""
from __future__ import annotations

import json
import logging
import time
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import httpx
from fiber import Keypair
from fiber.chain import signatures

logger = logging.getLogger(__name__)


async def query_single_miner(
    validator: Any,
    miner_hotkey: str,
    endpoint: str,
    payload: Dict[str, Any],
    timeout: float = 30.0,
    max_retries: int = 2,
) -> Optional[Dict[str, Any]]:
    """
    Query a single miner with fiber handshake and detailed logging.
    
    Args:
        validator: Validator instance with keypair and httpx_client
        miner_hotkey: Miner's SS58 hotkey
        endpoint: API endpoint (e.g., "/weather-initiate-fetch")
        payload: Request payload
        timeout: Request timeout in seconds
        max_retries: Maximum retry attempts
        
    Returns:
        Response dict with status, data, and metadata, or None if failed
    """
    start_time = time.time()
    
    # Get miner details
    miner_info = await _get_miner_info(validator.db_manager, miner_hotkey)
    if not miner_info:
        logger.warning(f"Miner {miner_hotkey[:8]} not found in node_table")
        return None
        
    miner_uid = miner_info["uid"]
    miner_ip = miner_info["ip"]
    miner_port = miner_info["port"]
    
    if not miner_ip or not miner_port:
        logger.warning(f"Miner {miner_hotkey[:8]} (UID {miner_uid}) has no IP/port")
        return None
        
    server_address = f"http://{miner_ip}:{miner_port}"
    full_url = f"{server_address}{endpoint}"
    
    logger.info(
        f"Querying miner {miner_hotkey[:8]} (UID {miner_uid}) at {server_address}"
        f"\n  Endpoint: {endpoint}"
        f"\n  Payload keys: {list(payload.keys())}"
    )
    
    # Prepare fiber handshake
    validator_keypair = validator.keypair
    
    for attempt in range(max_retries + 1):
        try:
            # Create nonce and signature for fiber handshake
            nonce = str(int(time.time() * 1000))
            
            # Serialize payload
            payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
            
            # Create signature
            message = f"{nonce}.{miner_hotkey}.{validator_keypair.ss58_address}.{payload_bytes.hex()}"
            signature = signatures.sign(validator_keypair, message)
            
            # Prepare headers
            headers = {
                "Content-Type": "application/json",
                "X-Nonce": nonce,
                "X-Signature": signature.hex(),
                "X-Validator-Hotkey": validator_keypair.ss58_address,
                "X-Miner-Hotkey": miner_hotkey,
            }
            
            # Make request
            logger.debug(
                f"Attempt {attempt + 1}/{max_retries + 1} to {miner_hotkey[:8]}"
                f"\n  URL: {full_url}"
                f"\n  Headers: {list(headers.keys())}"
            )
            
            request_start = time.time()
            
            async with validator.httpx_client as client:
                response = await client.post(
                    full_url,
                    json=payload,
                    headers=headers,
                    timeout=httpx.Timeout(timeout),
                )
            
            request_time = time.time() - request_start
            
            logger.info(
                f"Response from {miner_hotkey[:8]} (UID {miner_uid})"
                f"\n  Status: {response.status_code}"
                f"\n  Time: {request_time:.2f}s"
                f"\n  Headers: {dict(response.headers)}"
            )
            
            # Parse response
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    total_time = time.time() - start_time
                    
                    logger.info(
                        f"✓ Success from {miner_hotkey[:8]} (UID {miner_uid})"
                        f"\n  Total time: {total_time:.2f}s"
                        f"\n  Response keys: {list(response_data.keys()) if isinstance(response_data, dict) else type(response_data)}"
                    )
                    
                    return {
                        "success": True,
                        "data": response_data,
                        "status_code": response.status_code,
                        "response_time": request_time,
                        "total_time": total_time,
                        "miner_uid": miner_uid,
                        "miner_hotkey": miner_hotkey,
                        "attempt": attempt + 1,
                    }
                    
                except json.JSONDecodeError as e:
                    logger.error(
                        f"JSON decode error from {miner_hotkey[:8]}: {e}"
                        f"\n  Response text: {response.text[:500]}"
                    )
                    return {
                        "success": False,
                        "error": f"JSON decode error: {e}",
                        "status_code": response.status_code,
                        "response_text": response.text[:500],
                        "response_time": request_time,
                        "miner_uid": miner_uid,
                        "miner_hotkey": miner_hotkey,
                    }
                    
            elif response.status_code == 502:
                logger.warning(f"502 Bad Gateway from {miner_hotkey[:8]} - miner likely down")
                return {
                    "success": False,
                    "error": "502 Bad Gateway - miner likely down",
                    "status_code": response.status_code,
                    "response_time": request_time,
                    "miner_uid": miner_uid,
                    "miner_hotkey": miner_hotkey,
                }
                
            elif response.status_code in [401, 403]:
                logger.warning(
                    f"Authentication failed for {miner_hotkey[:8]}: {response.status_code}"
                    f"\n  Response: {response.text[:200]}"
                )
                return {
                    "success": False,
                    "error": f"Authentication failed: {response.status_code}",
                    "status_code": response.status_code,
                    "response_text": response.text[:200],
                    "response_time": request_time,
                    "miner_uid": miner_uid,
                    "miner_hotkey": miner_hotkey,
                }
                
            else:
                logger.warning(
                    f"Unexpected status {response.status_code} from {miner_hotkey[:8]}"
                    f"\n  Response: {response.text[:200]}"
                )
                
                # Retry on 5xx errors
                if response.status_code >= 500 and attempt < max_retries:
                    logger.info(f"Retrying {miner_hotkey[:8]} after {response.status_code}...")
                    continue
                    
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}",
                    "status_code": response.status_code,
                    "response_text": response.text[:200],
                    "response_time": request_time,
                    "miner_uid": miner_uid,
                    "miner_hotkey": miner_hotkey,
                }
                
        except httpx.TimeoutException:
            logger.warning(
                f"Timeout querying {miner_hotkey[:8]} (attempt {attempt + 1}/{max_retries + 1})"
            )
            if attempt < max_retries:
                continue
            return {
                "success": False,
                "error": "Request timeout",
                "miner_uid": miner_uid,
                "miner_hotkey": miner_hotkey,
                "attempt": attempt + 1,
            }
            
        except httpx.ConnectError as e:
            logger.warning(
                f"Connection failed to {miner_hotkey[:8]} at {server_address}: {e}"
            )
            return {
                "success": False,
                "error": f"Connection failed: {e}",
                "miner_uid": miner_uid,
                "miner_hotkey": miner_hotkey,
            }
            
        except Exception as e:
            logger.error(
                f"Unexpected error querying {miner_hotkey[:8]}: {e}"
                f"\n{traceback.format_exc()}"
            )
            if attempt < max_retries:
                continue
            return {
                "success": False,
                "error": f"Unexpected error: {e}",
                "miner_uid": miner_uid,
                "miner_hotkey": miner_hotkey,
                "traceback": traceback.format_exc(),
            }
    
    # All retries exhausted
    return {
        "success": False,
        "error": "All retry attempts failed",
        "miner_uid": miner_uid,
        "miner_hotkey": miner_hotkey,
        "attempts": max_retries + 1,
    }


async def _get_miner_info(db_manager, miner_hotkey: str) -> Optional[Dict]:
    """Get miner info from node_table."""
    result = await db_manager.fetch_one(
        """
        SELECT uid, hotkey, ip, port, protocol
        FROM node_table
        WHERE hotkey = :hotkey
        """,
        {"hotkey": miner_hotkey}
    )
    return dict(result) if result else None


async def query_miner_for_weather(
    validator: Any,
    miner_hotkey: str,
    forecast_start_time: datetime,
    previous_step_time: datetime,
    validator_hotkey: str,
) -> Optional[Dict[str, Any]]:
    """
    Query a miner to initiate weather forecast with detailed logging.
    
    This is a worker-friendly wrapper around the fiber handshake process.
    """
    payload = {
        "forecast_start_time": forecast_start_time.isoformat(),
        "previous_step_time": previous_step_time.isoformat(),
        "validator_hotkey": validator_hotkey,
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
        max_retries=2,
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
) -> Optional[Dict[str, Any]]:
    """
    Poll a miner for job status with detailed logging.
    """
    payload = {
        "job_id": job_id,
    }
    
    logger.info(f"Polling job status for {miner_hotkey[:8]}, job_id: {job_id}")
    
    result = await query_single_miner(
        validator=validator,
        miner_hotkey=miner_hotkey,
        endpoint="/weather-get-input-status",
        payload=payload,
        timeout=15.0,
        max_retries=1,
    )
    
    if result and result.get("success"):
        data = result.get("data", {})
        logger.info(
            f"Job status from {miner_hotkey[:8]}: {data.get('status')}"
            f"\n  Progress: {data.get('progress', 'N/A')}"
        )
    else:
        error = result.get("error", "Unknown error") if result else "No response"
        logger.warning(f"Failed to poll {miner_hotkey[:8]}: {error}")
    
    return result
