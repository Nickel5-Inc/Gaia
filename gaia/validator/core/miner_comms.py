import asyncio
import base64
import os
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

import httpx
import numpy as np
import psutil
from cryptography.fernet import Fernet
from fiber.encrypted.validator import client as vali_client
from fiber.encrypted.validator import handshake
from fiber.logging_utils import get_logger

PSUTIL_AVAILABLE = psutil is not None
logger = get_logger(__name__)


async def perform_handshake_with_retry(
    httpx_client: httpx.AsyncClient,
    server_address: str,
    keypair: Any,
    miner_hotkey_ss58_address: str,
    max_retries: int = 2,
    base_timeout: float = 15.0,
) -> tuple[str, str]:
    """
    Custom handshake function with retry logic and progressive timeouts.
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        current_timeout = base_timeout * (1.5 ** attempt)
        try:
            logger.debug(f"Handshake attempt {attempt + 1}/{max_retries + 1} with timeout {current_timeout:.1f}s")
            public_key_encryption_key = await asyncio.wait_for(
                handshake.get_public_encryption_key(
                    httpx_client, server_address, timeout=int(current_timeout)
                ),
                timeout=current_timeout,
            )
            symmetric_key: bytes = os.urandom(32)
            symmetric_key_uuid: str = os.urandom(32).hex()
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
                timeout=current_timeout,
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
    raise last_exception or Exception("Handshake failed after all retry attempts")


class MinerComms:
    def __init__(self, validator):
        self.validator = validator
        self.miner_client = validator.miner_client

    async def query_miners(self, payload: Dict, endpoint: str, hotkeys: Optional[List[str]] = None) -> Dict:
        """Query miners with the given payload in parallel with batch retry logic."""
        try:
            logger.info(f"Querying miners for endpoint {endpoint} with payload size: {len(str(payload))} bytes. Specified hotkeys: {hotkeys if hotkeys else 'All/Default'}")
            
            await self.validator.neuron.sync_metagraph(self.validator.metagraph_sync_interval)
            self.validator.metagraph = self.validator.neuron.metagraph
            self.validator.last_metagraph_sync = self.validator.neuron.last_metagraph_sync

            if not self.validator.metagraph or not self.validator.metagraph.nodes:
                logger.error("Metagraph not available. Cannot query miners.")
                return {}

            nodes_to_consider = self.validator.metagraph.nodes
            miners_to_query = self._get_miners_to_query(nodes_to_consider, hotkeys, endpoint)

            if not miners_to_query:
                logger.warning(f"No miners to query for endpoint {endpoint} after filtering. Hotkeys: {hotkeys}")
                return {}

            all_results = await self._run_query_tasks(miners_to_query, payload, endpoint)
            
            self._log_query_summary(all_results, endpoint)
            
            await self._post_query_cleanup(payload)
            
            successful_results = [r for r in all_results if r and r.get('status') == 'success']
            return {res['hotkey']: res for res in successful_results}

        except Exception as e:
            logger.error(f"Error querying miners: {e}", exc_info=True)
            return {}

    def _get_miners_to_query(self, nodes_to_consider, hotkeys, endpoint):
        miners_to_query = {}
        if hotkeys:
            logger.info(f"Targeting specific hotkeys: {hotkeys}")
            for hk in hotkeys:
                if hk in nodes_to_consider:
                    miners_to_query[hk] = nodes_to_consider[hk]
                else:
                    logger.warning(f"Specified hotkey {hk} not found in current metagraph. Skipping.")
        else:
            miners_to_query = nodes_to_consider
            if self.validator.args.test and len(miners_to_query) > 10:
                selected_hotkeys_for_test = list(miners_to_query.keys())[-10:]
                miners_to_query = {k: miners_to_query[k] for k in selected_hotkeys_for_test}
                logger.info(f"Test mode: Selected last {len(miners_to_query)} miners for {endpoint}.")
            elif not self.validator.args.test:
                logger.info(f"Querying all {len(miners_to_query)} available miners for {endpoint}.")
        return miners_to_query

    async def _run_query_tasks(self, miners_to_query, payload, endpoint):
        chunk_size = 50
        chunks = [dict(list(miners_to_query.items())[i:i + chunk_size]) for i in range(0, len(miners_to_query), chunk_size)]
        logger.info(f"Processing {len(miners_to_query)} miners in {len(chunks)} chunks of {chunk_size}")

        all_results = []
        for idx, chunk in enumerate(chunks):
            logger.info(f"Processing chunk {idx + 1}/{len(chunks)} with {len(chunk)} miners...")
            chunk_results = await self._process_chunk(chunk, payload, endpoint)
            all_results.extend(chunk_results)
            if idx < len(chunks) - 1:
                await asyncio.sleep(0.5)
        return all_results

    async def _process_chunk(self, chunk_miners, payload, endpoint):
        chunk_concurrency = 15
        semaphore = asyncio.Semaphore(chunk_concurrency)
        tasks = []
        for hotkey, node in chunk_miners.items():
            if node.ip and node.port:
                task = asyncio.create_task(
                    self._query_single_miner_with_retries(hotkey, node, payload, endpoint, semaphore),
                    name=f"query_{hotkey[:8]}"
                )
                tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        processed_results = []
        for res in results:
            if isinstance(res, Exception):
                logger.debug(f"Exception processing response in chunk: {res}")
                # Potentially find hotkey from task name if needed for error logging
                processed_results.append({"status": "failed", "reason": "Task Exception", "details": str(res)})
            else:
                processed_results.append(res)
        return processed_results

    async def _query_single_miner_with_retries(self, miner_hotkey, node, payload, endpoint, semaphore):
        max_retries = 2
        base_timeout = 15.0
        async with semaphore:
            for attempt in range(max_retries):
                try:
                    handshake_timeout = base_timeout + (attempt * 5.0)
                    symmetric_key_str, symmetric_key_uuid = await self._perform_handshake(miner_hotkey, node.ip, node.port, handshake_timeout)
                    
                    if not symmetric_key_str:
                        if attempt < max_retries - 1: continue
                        return {"hotkey": miner_hotkey, "status": "failed", "reason": "Handshake Failed"}

                    return await self._make_request(miner_hotkey, node, payload, endpoint, symmetric_key_str, symmetric_key_uuid, attempt)
                except Exception as e:
                    logger.debug(f"Error in query attempt {attempt+1} for {miner_hotkey}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    return {"hotkey": miner_hotkey, "status": "failed", "reason": "Outer Error", "details": f"{type(e).__name__}"}
        return {"hotkey": miner_hotkey, "status": "failed", "reason": "All Attempts Failed"}

    async def _perform_handshake(self, miner_hotkey, ip, port, timeout):
        base_url = f"https://{ip}:{port}"
        try:
            return await perform_handshake_with_retry(
                self.miner_client,
                base_url,
                self.validator.keypair,
                miner_hotkey,
                max_retries=1, # Retries handled in outer loop
                base_timeout=timeout
            )
        except Exception as hs_err:
            logger.debug(f"Handshake failed for {miner_hotkey}: {type(hs_err).__name__}")
            return None, None

    async def _make_request(self, miner_hotkey, node, payload, endpoint, key, uuid, attempt):
        base_url = f"https://{node.ip}:{node.port}"
        fernet = Fernet(key)
        start_time = time.time()
        try:
            resp = await asyncio.wait_for(
                vali_client.make_non_streamed_post(
                    httpx_client=self.miner_client,
                    server_address=base_url,
                    fernet=fernet,
                    keypair=self.validator.keypair,
                    symmetric_key_uuid=uuid,
                    validator_ss58_address=self.validator.keypair.ss58_address,
                    miner_ss58_address=miner_hotkey,
                    payload=payload,
                    endpoint=endpoint,
                ),
                timeout=240.0
            )
            duration = time.time() - start_time
            if resp and resp.status_code < 400:
                logger.info(f"SUCCESS: {miner_hotkey} responded in {duration:.2f}s (attempt {attempt + 1})")
                return {
                    "status": "success", "text": resp.text, "status_code": resp.status_code,
                    "hotkey": miner_hotkey, "port": node.port, "ip": node.ip, "duration": duration,
                    "content_length": len(resp.content) if resp.content else 0, "attempts_used": attempt + 1
                }
            return {"hotkey": miner_hotkey, "status": "failed", "reason": "Bad Response", "details": f"Status code {resp.status_code if resp else 'N/A'}"}
        except asyncio.TimeoutError:
            return {"hotkey": miner_hotkey, "status": "failed", "reason": "Request Timeout", "details": "Timeout during POST"}
        except Exception as e:
            return {"hotkey": miner_hotkey, "status": "failed", "reason": "Request Error", "details": f"{type(e).__name__}"}

    def _log_query_summary(self, all_results, endpoint):
        successful = [r for r in all_results if r and r.get('status') == 'success']
        failed = [r for r in all_results if not r or r.get('status') == 'failed']
        total = len(all_results)
        success_rate = len(successful) / total * 100 if total > 0 else 0

        summary = f"\n--- Miner Query Summary (Endpoint: {endpoint}) ---\n"
        summary += f"  - Total: {total}, Successful: {len(successful)} ({success_rate:.1f}%), Failed: {len(failed)}\n"
        
        if successful:
            durations = [r['duration'] for r in successful]
            summary += f"  - Timing (Success): Avg={np.mean(durations):.2f}s, Med={np.median(durations):.2f}s, 95th={np.percentile(durations, 95):.2f}s\n"

        if failed:
            failures_by_reason = defaultdict(list)
            for f in failed:
                failures_by_reason[f.get('reason', 'Unknown')].append(f)
            summary += "  - Failures:\n"
            for reason, items in sorted(failures_by_reason.items(), key=lambda item: len(item[1]), reverse=True):
                summary += f"    - {reason} ({len(items)}): {[f.get('hotkey', 'N/A')[:12] for f in items[:3]]}...\n"
        summary += "--- End of Summary ---\n"
        logger.info(summary)
    
    async def _post_query_cleanup(self, payload):
        try:
            del payload
            import gc
            collected = gc.collect()
            if collected > 100:
                logger.info(f"Garbage collected {collected} objects after miner queries")
            await self._cleanup_idle_connections()
        except Exception as e:
            logger.warning(f"Error during post-query cleanup: {e}")

    async def _cleanup_idle_connections(self):
        """Clean up idle connections in the HTTP client pool."""
        if hasattr(self.miner_client, '_transport') and hasattr(self.miner_client._transport, '_pool'):
            pool = self.miner_client._transport._pool
            if hasattr(pool, 'close_idle_connections'):
                pool.close_idle_connections()
                logger.debug("Closed idle connections in miner client pool.") 