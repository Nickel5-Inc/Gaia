"""
Weather Miner Client

Implements the three-phase miner communication protocol for weather validation.
This replaces the scattered HTTP communication logic from the legacy system.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

import httpx
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class WeatherMinerClient:
    """
    Client for communicating with weather miners using the three-phase protocol.
    
    Phase 1: Initiate Fetch - Request miners to fetch GFS data and compute hashes
    Phase 2: Get Input Status - Poll miners for hash computation completion  
    Phase 3: Trigger Inference - Request miners to start forecast generation
    """
    
    def __init__(self, io_engine):
        """
        Initialize the weather miner client.
        
        Args:
            io_engine: IOEngine instance for configuration and utilities
        """
        self.io_engine = io_engine
        self.config = io_engine.config
        self.timeout_config = {
            "initiate_fetch_timeout": 30.0,
            "status_poll_timeout": 15.0,
            "inference_trigger_timeout": 30.0,
            "max_retries": 3,
            "retry_delay": 2.0
        }
        
        # HTTP client configuration
        self.http_client_config = {
            "timeout": httpx.Timeout(30.0),
            "limits": httpx.Limits(max_connections=50, max_keepalive_connections=10),
            "follow_redirects": True
        }
        
        logger.info("WeatherMinerClient initialized")

    async def initiate_fetch_from_miners(
        self,
        validator,
        gfs_t0_run_time: datetime,
        gfs_t_minus_6_run_time: datetime,
        max_concurrent_requests: int = 20
    ) -> Dict[str, Dict[str, Any]]:
        """
        Phase 1: Initiate fetch requests to all available miners.
        
        Sends initiate_fetch requests to miners asking them to fetch GFS data
        and compute input hashes for the specified forecast run.
        
        Args:
            validator: Validator instance with miner information
            gfs_t0_run_time: GFS T0 initialization time
            gfs_t_minus_6_run_time: GFS T-6 time for analysis
            max_concurrent_requests: Maximum concurrent HTTP requests
            
        Returns:
            Dict mapping miner hotkeys to their responses
        """
        logger.info(f"Phase 1: Initiating fetch requests for GFS T0: {gfs_t0_run_time}")
        
        # Get active miners from validator
        active_miners = await self._get_active_miners(validator)
        if not active_miners:
            logger.warning("No active miners found for fetch initiation")
            return {}
        
        logger.info(f"Sending initiate fetch requests to {len(active_miners)} miners")
        
        # Prepare request payload
        request_payload = {
            "gfs_t0_run_time": gfs_t0_run_time.isoformat(),
            "gfs_t_minus_6_run_time": gfs_t_minus_6_run_time.isoformat(),
            "validator_hotkey": validator.keypair.ss58_address,
            "request_timestamp": datetime.utcnow().isoformat()
        }
        
        # Create semaphore for concurrent request limiting
        semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Create tasks for all miners
        tasks = []
        for miner_info in active_miners:
            task = asyncio.create_task(
                self._initiate_fetch_single_miner(
                    semaphore, miner_info, request_payload, validator
                ),
                name=f"initiate_fetch_{miner_info['hotkey'][:8]}"
            )
            tasks.append((miner_info['hotkey'], task))
        
        # Execute all requests concurrently
        responses = {}
        completed_tasks = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
        
        # Process results
        for i, (miner_hotkey, _) in enumerate(tasks):
            result = completed_tasks[i]
            
            if isinstance(result, Exception):
                logger.error(f"Initiate fetch failed for miner {miner_hotkey[:8]}: {result}")
                responses[miner_hotkey] = {
                    "status": "fetch_rejected",
                    "error": str(result),
                    "job_id": None
                }
            else:
                responses[miner_hotkey] = result
        
        successful_count = sum(1 for r in responses.values() if r.get("status") == "fetch_accepted")
        logger.info(f"Phase 1 completed: {successful_count}/{len(active_miners)} miners accepted fetch requests")
        
        return responses

    async def get_input_status_from_miners(
        self,
        validator,
        miner_jobs: List[Dict[str, Any]],
        max_concurrent_requests: int = 20
    ) -> Dict[str, Dict[str, Any]]:
        """
        Phase 2: Poll miners for input status and hash computation completion.
        
        Checks if miners have completed GFS data fetching and hash computation.
        
        Args:
            validator: Validator instance
            miner_jobs: List of miner job records with hotkeys and job IDs
            max_concurrent_requests: Maximum concurrent HTTP requests
            
        Returns:
            Dict mapping miner hotkeys to their status responses
        """
        logger.info(f"Phase 2: Polling input status for {len(miner_jobs)} miner jobs")
        
        if not miner_jobs:
            logger.warning("No miner jobs provided for status polling")
            return {}
        
        # Create semaphore for concurrent request limiting
        semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Create tasks for all miner jobs
        tasks = []
        for job_info in miner_jobs:
            miner_hotkey = job_info["miner_hotkey"]
            job_id = job_info["job_id"]
            
            task = asyncio.create_task(
                self._get_input_status_single_miner(
                    semaphore, miner_hotkey, job_id, validator
                ),
                name=f"status_poll_{miner_hotkey[:8]}"
            )
            tasks.append((miner_hotkey, task))
        
        # Execute all requests concurrently
        responses = {}
        completed_tasks = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
        
        # Process results
        for i, (miner_hotkey, _) in enumerate(tasks):
            result = completed_tasks[i]
            
            if isinstance(result, Exception):
                logger.error(f"Status poll failed for miner {miner_hotkey[:8]}: {result}")
                responses[miner_hotkey] = {
                    "status": "fetch_error",
                    "error": str(result),
                    "input_data_hash": None
                }
            else:
                responses[miner_hotkey] = result
        
        ready_count = sum(1 for r in responses.values() 
                         if r.get("status") == "input_hashed_awaiting_validation")
        logger.info(f"Phase 2 completed: {ready_count}/{len(miner_jobs)} miners ready for validation")
        
        return responses

    async def trigger_inference_from_miners(
        self,
        validator,
        verified_miners: List[Dict[str, Any]],
        max_concurrent_requests: int = 20
    ) -> Dict[str, Dict[str, Any]]:
        """
        Phase 3: Trigger inference on verified miners.
        
        Sends inference trigger requests to miners that have passed hash verification.
        
        Args:
            validator: Validator instance
            verified_miners: List of verified miner records
            max_concurrent_requests: Maximum concurrent HTTP requests
            
        Returns:
            Dict mapping miner hotkeys to their inference responses
        """
        logger.info(f"Phase 3: Triggering inference for {len(verified_miners)} verified miners")
        
        if not verified_miners:
            logger.warning("No verified miners provided for inference triggering")
            return {}
        
        # Create semaphore for concurrent request limiting
        semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Create tasks for all verified miners
        tasks = []
        for miner_info in verified_miners:
            miner_hotkey = miner_info["miner_hotkey"]
            job_id = miner_info["job_id"]
            
            task = asyncio.create_task(
                self._trigger_inference_single_miner(
                    semaphore, miner_hotkey, job_id, validator
                ),
                name=f"trigger_inference_{miner_hotkey[:8]}"
            )
            tasks.append((miner_hotkey, task))
        
        # Execute all requests concurrently
        responses = {}
        completed_tasks = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
        
        # Process results
        for i, (miner_hotkey, _) in enumerate(tasks):
            result = completed_tasks[i]
            
            if isinstance(result, Exception):
                logger.error(f"Inference trigger failed for miner {miner_hotkey[:8]}: {result}")
                responses[miner_hotkey] = {
                    "status": "inference_failed",
                    "error": str(result)
                }
            else:
                responses[miner_hotkey] = result
        
        started_count = sum(1 for r in responses.values() 
                           if r.get("status") == "inference_started")
        logger.info(f"Phase 3 completed: {started_count}/{len(verified_miners)} miners started inference")
        
        return responses

    # ========== Single Miner Communication Methods ==========

    async def _initiate_fetch_single_miner(
        self,
        semaphore: asyncio.Semaphore,
        miner_info: Dict[str, Any],
        request_payload: Dict[str, Any],
        validator
    ) -> Dict[str, Any]:
        """Send initiate fetch request to a single miner."""
        async with semaphore:
            miner_hotkey = miner_info["hotkey"]
            miner_url = self._build_miner_url(miner_info, "initiate_fetch")
            
            logger.debug(f"Sending initiate fetch to miner {miner_hotkey[:8]} at {miner_url}")
            
            try:
                async with httpx.AsyncClient(**self.http_client_config) as client:
                    # Add authentication headers
                    headers = await self._get_auth_headers(validator, miner_hotkey)
                    
                    response = await client.post(
                        miner_url,
                        json=request_payload,
                        headers=headers,
                        timeout=self.timeout_config["initiate_fetch_timeout"]
                    )
                    
                    if response.status_code == 200:
                        response_data = response.json()
                        logger.debug(f"Miner {miner_hotkey[:8]} initiate fetch response: {response_data}")
                        
                        return {
                            "status": response_data.get("status", "fetch_rejected"),
                            "job_id": response_data.get("job_id"),
                            "message": response_data.get("message"),
                            "miner_hotkey": miner_hotkey
                        }
                    else:
                        logger.warning(f"Miner {miner_hotkey[:8]} returned status {response.status_code}")
                        return {
                            "status": "fetch_rejected",
                            "error": f"HTTP {response.status_code}: {response.text}",
                            "job_id": None,
                            "miner_hotkey": miner_hotkey
                        }
                        
            except httpx.TimeoutException:
                logger.warning(f"Timeout initiating fetch for miner {miner_hotkey[:8]}")
                return {
                    "status": "fetch_rejected",
                    "error": "Request timeout",
                    "job_id": None,
                    "miner_hotkey": miner_hotkey
                }
            except Exception as e:
                logger.error(f"Error initiating fetch for miner {miner_hotkey[:8]}: {e}")
                return {
                    "status": "fetch_rejected",
                    "error": str(e),
                    "job_id": None,
                    "miner_hotkey": miner_hotkey
                }

    async def _get_input_status_single_miner(
        self,
        semaphore: asyncio.Semaphore,
        miner_hotkey: str,
        job_id: str,
        validator
    ) -> Dict[str, Any]:
        """Get input status from a single miner."""
        async with semaphore:
            # Get miner info for URL building
            miner_info = await self._get_miner_info(validator, miner_hotkey)
            if not miner_info:
                return {
                    "status": "fetch_error",
                    "error": "Miner not found",
                    "input_data_hash": None,
                    "miner_hotkey": miner_hotkey
                }
            
            miner_url = self._build_miner_url(miner_info, f"get_input_status/{job_id}")
            
            logger.debug(f"Polling input status for miner {miner_hotkey[:8]} job {job_id}")
            
            try:
                async with httpx.AsyncClient(**self.http_client_config) as client:
                    # Add authentication headers
                    headers = await self._get_auth_headers(validator, miner_hotkey)
                    
                    response = await client.get(
                        miner_url,
                        headers=headers,
                        timeout=self.timeout_config["status_poll_timeout"]
                    )
                    
                    if response.status_code == 200:
                        response_data = response.json()
                        logger.debug(f"Miner {miner_hotkey[:8]} status: {response_data.get('status')}")
                        
                        return {
                            "status": response_data.get("status", "fetch_error"),
                            "input_data_hash": response_data.get("input_data_hash"),
                            "message": response_data.get("message"),
                            "job_id": job_id,
                            "miner_hotkey": miner_hotkey
                        }
                    else:
                        logger.warning(f"Miner {miner_hotkey[:8]} status poll returned {response.status_code}")
                        return {
                            "status": "fetch_error",
                            "error": f"HTTP {response.status_code}: {response.text}",
                            "input_data_hash": None,
                            "miner_hotkey": miner_hotkey
                        }
                        
            except httpx.TimeoutException:
                logger.warning(f"Timeout polling status for miner {miner_hotkey[:8]}")
                return {
                    "status": "fetch_error",
                    "error": "Status poll timeout",
                    "input_data_hash": None,
                    "miner_hotkey": miner_hotkey
                }
            except Exception as e:
                logger.error(f"Error polling status for miner {miner_hotkey[:8]}: {e}")
                return {
                    "status": "fetch_error",
                    "error": str(e),
                    "input_data_hash": None,
                    "miner_hotkey": miner_hotkey
                }

    async def _trigger_inference_single_miner(
        self,
        semaphore: asyncio.Semaphore,
        miner_hotkey: str,
        job_id: str,
        validator
    ) -> Dict[str, Any]:
        """Trigger inference on a single miner."""
        async with semaphore:
            # Get miner info for URL building
            miner_info = await self._get_miner_info(validator, miner_hotkey)
            if not miner_info:
                return {
                    "status": "inference_failed",
                    "error": "Miner not found",
                    "miner_hotkey": miner_hotkey
                }
            
            miner_url = self._build_miner_url(miner_info, f"start_inference/{job_id}")
            
            logger.debug(f"Triggering inference for miner {miner_hotkey[:8]} job {job_id}")
            
            try:
                async with httpx.AsyncClient(**self.http_client_config) as client:
                    # Add authentication headers
                    headers = await self._get_auth_headers(validator, miner_hotkey)
                    
                    response = await client.post(
                        miner_url,
                        headers=headers,
                        timeout=self.timeout_config["inference_trigger_timeout"]
                    )
                    
                    if response.status_code == 200:
                        response_data = response.json()
                        logger.debug(f"Miner {miner_hotkey[:8]} inference trigger: {response_data}")
                        
                        return {
                            "status": response_data.get("status", "inference_failed"),
                            "message": response_data.get("message"),
                            "job_id": job_id,
                            "miner_hotkey": miner_hotkey
                        }
                    else:
                        logger.warning(f"Miner {miner_hotkey[:8]} inference trigger returned {response.status_code}")
                        return {
                            "status": "inference_failed",
                            "error": f"HTTP {response.status_code}: {response.text}",
                            "miner_hotkey": miner_hotkey
                        }
                        
            except httpx.TimeoutException:
                logger.warning(f"Timeout triggering inference for miner {miner_hotkey[:8]}")
                return {
                    "status": "inference_failed",
                    "error": "Inference trigger timeout",
                    "miner_hotkey": miner_hotkey
                }
            except Exception as e:
                logger.error(f"Error triggering inference for miner {miner_hotkey[:8]}: {e}")
                return {
                    "status": "inference_failed",
                    "error": str(e),
                    "miner_hotkey": miner_hotkey
                }

    # ========== Helper Methods ==========

    async def _get_active_miners(self, validator) -> List[Dict[str, Any]]:
        """Get list of active miners from the validator."""
        try:
            # This would typically query the substrate network for active miners
            # For now, return a mock list for testing
            # In production, this would be: return await validator.get_active_miners()
            
            # Mock active miners for testing
            mock_miners = [
                {
                    "hotkey": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
                    "ip": "192.168.1.100",
                    "port": 8091,
                    "protocol": 4,  # HTTPS
                    "uid": 1
                },
                {
                    "hotkey": "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
                    "ip": "192.168.1.101", 
                    "port": 8091,
                    "protocol": 4,  # HTTPS
                    "uid": 2
                }
            ]
            
            logger.info(f"Retrieved {len(mock_miners)} active miners (mock data)")
            return mock_miners
            
        except Exception as e:
            logger.error(f"Error getting active miners: {e}")
            return []

    async def _get_miner_info(self, validator, miner_hotkey: str) -> Optional[Dict[str, Any]]:
        """Get miner information by hotkey."""
        try:
            active_miners = await self._get_active_miners(validator)
            for miner in active_miners:
                if miner["hotkey"] == miner_hotkey:
                    return miner
            return None
        except Exception as e:
            logger.error(f"Error getting miner info for {miner_hotkey[:8]}: {e}")
            return None

    def _build_miner_url(self, miner_info: Dict[str, Any], endpoint: str) -> str:
        """Build the complete URL for a miner endpoint."""
        protocol = "https" if miner_info.get("protocol") == 4 else "http"
        ip = miner_info["ip"]
        port = miner_info["port"]
        
        # Convert IP if it's in integer format
        if isinstance(ip, int):
            import ipaddress
            ip = str(ipaddress.ip_address(ip))
        
        base_url = f"{protocol}://{ip}:{port}"
        full_url = f"{base_url}/weather/{endpoint}"
        
        return full_url

    async def _get_auth_headers(self, validator, miner_hotkey: str) -> Dict[str, str]:
        """Get authentication headers for miner communication."""
        try:
            # This would typically create signed headers for authentication
            # For now, return basic headers
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "GaiaValidator/4.0",
                "X-Validator-Hotkey": validator.keypair.ss58_address,
                "X-Request-ID": f"req_{int(datetime.utcnow().timestamp())}"
            }
            
            # In production, this would include cryptographic signatures:
            # headers["Authorization"] = await self._create_auth_signature(validator, miner_hotkey)
            
            return headers
            
        except Exception as e:
            logger.error(f"Error creating auth headers: {e}")
            return {"Content-Type": "application/json"}

    async def _create_auth_signature(self, validator, miner_hotkey: str) -> str:
        """Create cryptographic signature for authentication."""
        # This would implement the actual signature creation
        # using the validator's keypair and current timestamp
        # For now, return a placeholder
        return "Bearer mock_signature"

    # ========== Batch Operations ==========

    async def poll_miner_status_with_retry(
        self,
        validator,
        miner_jobs: List[Dict[str, Any]],
        max_wait_minutes: int = 10,
        poll_interval_seconds: int = 30
    ) -> Dict[str, Dict[str, Any]]:
        """
        Poll miner status with retry logic until all miners are ready or timeout.
        
        Args:
            validator: Validator instance
            miner_jobs: List of miner job records
            max_wait_minutes: Maximum wait time in minutes
            poll_interval_seconds: Interval between polls in seconds
            
        Returns:
            Dict mapping miner hotkeys to their final status
        """
        logger.info(f"Polling miner status with retry for {len(miner_jobs)} jobs")
        logger.info(f"Max wait: {max_wait_minutes} minutes, Poll interval: {poll_interval_seconds} seconds")
        
        start_time = datetime.utcnow()
        max_wait_time = timedelta(minutes=max_wait_minutes)
        
        pending_jobs = miner_jobs.copy()
        final_responses = {}
        
        while pending_jobs and (datetime.utcnow() - start_time) < max_wait_time:
            logger.info(f"Polling status for {len(pending_jobs)} pending jobs...")
            
            # Poll current pending jobs
            responses = await self.get_input_status_from_miners(validator, pending_jobs)
            
            # Process responses and update pending list
            still_pending = []
            for job in pending_jobs:
                miner_hotkey = job["miner_hotkey"]
                response = responses.get(miner_hotkey, {})
                status = response.get("status")
                
                if status == "input_hashed_awaiting_validation":
                    # Miner is ready
                    final_responses[miner_hotkey] = response
                    logger.info(f"Miner {miner_hotkey[:8]} is ready for validation")
                elif status in ["fetch_error", "input_fetch_error"]:
                    # Miner failed permanently
                    final_responses[miner_hotkey] = response
                    logger.warning(f"Miner {miner_hotkey[:8]} failed permanently: {status}")
                else:
                    # Miner still processing, keep polling
                    still_pending.append(job)
                    logger.debug(f"Miner {miner_hotkey[:8]} still processing: {status}")
            
            pending_jobs = still_pending
            
            if pending_jobs:
                logger.info(f"Waiting {poll_interval_seconds} seconds before next poll...")
                await asyncio.sleep(poll_interval_seconds)
        
        # Handle any remaining pending jobs as timeouts
        for job in pending_jobs:
            miner_hotkey = job["miner_hotkey"]
            final_responses[miner_hotkey] = {
                "status": "fetch_error",
                "error": "Polling timeout",
                "input_data_hash": None,
                "miner_hotkey": miner_hotkey
            }
            logger.warning(f"Miner {miner_hotkey[:8]} timed out during status polling")
        
        ready_count = sum(1 for r in final_responses.values() 
                         if r.get("status") == "input_hashed_awaiting_validation")
        total_time = datetime.utcnow() - start_time
        
        logger.info(f"Status polling completed: {ready_count}/{len(miner_jobs)} miners ready")
        logger.info(f"Total polling time: {total_time.total_seconds():.1f} seconds")
        
        return final_responses