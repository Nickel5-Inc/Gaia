from __future__ import annotations

from typing import Optional, Dict, Any
import asyncio
import base64

import httpx

from fiber.encrypted.validator import client as vali_client, handshake
from cryptography.fernet import Fernet

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager


async def _handshake_and_request(
    *,
    httpx_client: httpx.AsyncClient,
    base_url: str,
    keypair,
    miner_hotkey: str,
    endpoint: str,
    payload: Dict[str, Any],
    timeout: float,
) -> Dict[str, Any]:
    public_key_encryption_key = await handshake.get_public_encryption_key(
        httpx_client, base_url, timeout=int(timeout)
    )
    symmetric_key: bytes = __import__("os").urandom(32)
    symmetric_key_uuid = __import__("os").urandom(32).hex()

    ok = await handshake.send_symmetric_key_to_server(
        httpx_client,
        base_url,
        keypair,
        public_key_encryption_key,
        symmetric_key,
        symmetric_key_uuid,
        miner_hotkey,
        timeout=int(timeout),
    )
    if not ok:
        return {"status": "failed", "reason": "Handshake Error"}

    fernet = Fernet(base64.b64encode(symmetric_key))
    resp = await vali_client.make_non_streamed_post(
        httpx_client=httpx_client,
        server_address=base_url,
        fernet=fernet,
        keypair=keypair,
        symmetric_key_uuid=symmetric_key_uuid,
        validator_ss58_address=keypair.ss58_address,
        miner_ss58_address=miner_hotkey,
        payload=payload,
        endpoint=endpoint,
    )
    return {"status_code": getattr(resp, "status_code", None), "text": getattr(resp, "text", None)}


async def query_single_miner(
    task_or_validator: Any,
    miner_hotkey: str,
    *,
    endpoint: str,
    payload: Dict[str, Any],
    timeout: float = 45.0,
) -> Optional[Dict[str, Any]]:
    """Handshake+request to exactly one miner using the same Fiber/Bittensor path as validator.query_miners.

    If a validator instance is present, reuse its httpx client and keypair. Otherwise, construct a temporary client using node_table ip/port.
    """
    # Prefer validator instance
    validator = getattr(task_or_validator, "validator", None)
    if validator is None and getattr(task_or_validator, "keypair", None) is not None:
        # task_or_validator is likely the validator already
        validator = task_or_validator

    if validator is not None and getattr(validator, "miner_client", None) and getattr(validator, "keypair", None):
        node = validator.metagraph.nodes.get(miner_hotkey) if getattr(validator, "metagraph", None) else None
        if not node:
            return None
        base_url = f"https://{node.ip}:{node.port}"
        return await _handshake_and_request(
            httpx_client=validator.miner_client,
            base_url=base_url,
            keypair=validator.keypair,
            miner_hotkey=miner_hotkey,
            endpoint=endpoint,
            payload=payload,
            timeout=timeout,
        )

    # Fallback: direct ip/port lookup
    db = getattr(task_or_validator, "db_manager", None)
    if db is None:
        db = task_or_validator if isinstance(task_or_validator, ValidatorDatabaseManager) else None
    if db is None:
        return None

    try:
        row = await db.fetch_one(
            "SELECT ip, port FROM node_table WHERE hotkey = :hk", {"hk": miner_hotkey}
        )
    except Exception:
        row = None
    if not row or not row.get("ip") or not row.get("port"):
        return None
    ip_val = row.get("ip")
    port_val = row.get("port")
    base_url = f"https://{ip_val}:{port_val}"
    async with httpx.AsyncClient(verify=False, timeout=timeout) as client:
        # No access to validator keypair here; cannot do fiber handshake → return None
        return None


# Convenience wrappers
async def initiate_fetch(
    task_or_validator: Any,
    miner_hotkey: str,
    *,
    forecast_start_time,
    previous_step_time,
    validator_hotkey: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    payload = {
        "nonce": __import__("uuid").uuid4().hex,
        "data": {
            "forecast_start_time": forecast_start_time,
            "previous_step_time": previous_step_time,
            "validator_hotkey": validator_hotkey,
        },
    }
    return await query_single_miner(
        task_or_validator, miner_hotkey, endpoint="/weather-initiate-fetch", payload=payload
    )


async def kerchunk_request(
    task_or_validator: Any, miner_hotkey: str, *, job_id: str
) -> Optional[Dict[str, Any]]:
    payload = {"nonce": __import__("uuid").uuid4().hex, "data": {"job_id": job_id}}
    return await query_single_miner(
        task_or_validator, miner_hotkey, endpoint="/weather-kerchunk-request", payload=payload
    )


async def get_input_status(
    task_or_validator: Any, miner_hotkey: str, *, job_id: str
) -> Optional[Dict[str, Any]]:
    payload = {"nonce": __import__("uuid").uuid4().hex, "data": {"job_id": job_id}}
    return await query_single_miner(
        task_or_validator, miner_hotkey, endpoint="/weather-get-input-status", payload=payload
    )


async def start_inference(
    task_or_validator: Any, miner_hotkey: str, *, job_id: str
) -> Optional[Dict[str, Any]]:
    payload = {"nonce": __import__("uuid").uuid4().hex, "data": {"job_id": job_id}}
    return await query_single_miner(
        task_or_validator, miner_hotkey, endpoint="/weather-start-inference", payload=payload
    )


