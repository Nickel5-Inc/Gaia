"""
Weather Miner Communication Client.

This module handles all HTTP communication between the validator and miners
for weather forecasting tasks. It integrates with the IO-Engine's HTTP client
and provides typed request/response handling.
"""

import asyncio
import logging
import uuid
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone

from gaia.tasks.defined_tasks.weather.schemas.weather_inputs import (
    WeatherInitiateFetchData,
    WeatherGetInputStatusData,
    WeatherStartInferenceData
)
from gaia.validator.utils.config import settings

logger = logging.getLogger(__name__)


class WeatherMinerClient:
    """
    Client for communicating with miners for weather forecast tasks.
    
    This class handles the three-phase weather protocol:
    1. Initiate fetch (request miners to fetch GFS data)
    2. Get input status (check if miners have computed hashes)
    3. Start inference (trigger forecast generation)
    """
    
    def __init__(self, io_engine):
        """
        Initialize the weather miner client.
        
        Args:
            io_engine: The IOEngine instance with HTTP client and validator access
        """
        self.io_engine = io_engine
        self.http_client = io_engine.http_client
        self.config = io_engine.config
        
    async def initiate_fetch_from_miners(
        self, 
        validator,
        gfs_t0_run_time: datetime,
        gfs_t_minus_6_run_time: datetime
    ) -> Dict[str, Any]:
        """
        Send initiate fetch requests to all active miners.
        
        Args:
            validator: Validator instance for accessing metagraph
            gfs_t0_run_time: GFS T0 initialization time
            gfs_t_minus_6_run_time: GFS T-6 time
            
        Returns:
            Dictionary mapping miner hotkeys to their responses
        """
        logger.info("Sending initiate fetch requests to miners")
        
        # Prepare the payload
        payload_data = WeatherInitiateFetchData(
            forecast_start_time=gfs_t0_run_time,
            previous_step_time=gfs_t_minus_6_run_time
        )
        
        payload = {
            "nonce": str(uuid.uuid4()),
            "data": payload_data.model_dump(mode="json")
        }
        
        # Send requests to all miners
        responses = await validator.query_miners(
            payload=payload,
            endpoint="/weather-initiate-fetch"
        )
        
        logger.info(f"Received {len(responses)} responses from initiate fetch requests")
        return responses
    
    async def get_input_status_from_miners(
        self,
        validator,
        miner_jobs: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Poll miners for their input status (hash computation).
        
        Args:
            validator: Validator instance for accessing metagraph
            miner_jobs: List of miner job records with miner_hotkey and job_id
            
        Returns:
            Dictionary mapping miner hotkeys to their status responses
        """
        logger.info(f"Polling {len(miner_jobs)} miners for input status")
        
        # Create polling tasks for each miner
        polling_tasks = []
        for job in miner_jobs:
            task = self._poll_single_miner_status(
                validator,
                job["miner_hotkey"],
                job["job_id"]
            )
            polling_tasks.append(task)
        
        # Execute all polling tasks concurrently
        poll_results = await asyncio.gather(*polling_tasks, return_exceptions=True)
        
        # Process results
        status_responses = {}
        for i, result in enumerate(poll_results):
            job = miner_jobs[i]
            miner_hotkey = job["miner_hotkey"]
            
            if isinstance(result, Exception):
                logger.error(f"Error polling miner {miner_hotkey[:8]}: {result}")
                status_responses[miner_hotkey] = {
                    "status": "validator_poll_error",
                    "message": str(result)
                }
            else:
                status_responses[miner_hotkey] = result
        
        logger.info(f"Collected status from {len(status_responses)} miners")
        return status_responses
    
    async def trigger_inference_on_miners(
        self,
        validator,
        verified_miners: List[Dict[str, Any]]
    ) -> Dict[str, bool]:
        """
        Trigger inference on miners with verified input hashes.
        
        Args:
            validator: Validator instance for accessing metagraph
            verified_miners: List of miner records with miner_hotkey and job_id
            
        Returns:
            Dictionary mapping miner hotkeys to success status
        """
        logger.info(f"Triggering inference on {len(verified_miners)} verified miners")
        
        # Create trigger tasks for each verified miner
        trigger_tasks = []
        for miner in verified_miners:
            task = self._trigger_single_miner_inference(
                validator,
                miner["miner_hotkey"],
                miner["job_id"]
            )
            trigger_tasks.append(task)
        
        # Execute all trigger tasks concurrently
        trigger_results = await asyncio.gather(*trigger_tasks, return_exceptions=True)
        
        # Process results
        inference_results = {}
        for i, result in enumerate(trigger_results):
            miner = verified_miners[i]
            miner_hotkey = miner["miner_hotkey"]
            
            if isinstance(result, Exception):
                logger.error(f"Error triggering inference on {miner_hotkey[:8]}: {result}")
                inference_results[miner_hotkey] = False
            else:
                inference_results[miner_hotkey] = result
        
        successful_triggers = sum(1 for success in inference_results.values() if success)
        logger.info(f"Successfully triggered inference on {successful_triggers}/{len(verified_miners)} miners")
        
        return inference_results
    
    async def _poll_single_miner_status(
        self,
        validator,
        miner_hotkey: str,
        job_id: str
    ) -> Dict[str, Any]:
        """
        Poll a single miner for input status.
        
        Args:
            validator: Validator instance
            miner_hotkey: Miner's hotkey
            job_id: Job ID to check status for
            
        Returns:
            Miner's status response
        """
        try:
            # Check if miner is available in metagraph
            node = validator.metagraph.nodes.get(miner_hotkey)
            if not node or not node.ip or not node.port:
                logger.warning(f"Miner {miner_hotkey[:8]} not found in metagraph or missing IP/Port")
                return {
                    "status": "validator_poll_error",
                    "message": "Miner not found in metagraph"
                }
            
            # Prepare status request payload
            status_payload_data = WeatherGetInputStatusData(job_id=job_id)
            status_payload = {
                "nonce": str(uuid.uuid4()),
                "data": status_payload_data.model_dump()
            }
            
            # Send request to specific miner
            all_responses = await validator.query_miners(
                payload=status_payload,
                endpoint="/weather-get-input-status",
                hotkeys=[miner_hotkey]
            )
            
            # Process response
            status_response = all_responses.get(miner_hotkey)
            if status_response:
                parsed_response = self._parse_miner_response(status_response)
                logger.debug(f"Status from {miner_hotkey[:8]}: {parsed_response.get('status', 'unknown')}")
                return parsed_response
            else:
                logger.warning(f"No response from miner {miner_hotkey[:8]} for status request")
                return {
                    "status": "validator_poll_failed",
                    "message": "No response from miner"
                }
                
        except Exception as e:
            logger.error(f"Error polling miner {miner_hotkey[:8]} for status: {e}")
            return {
                "status": "validator_poll_error",
                "message": str(e)
            }
    
    async def _trigger_single_miner_inference(
        self,
        validator,
        miner_hotkey: str,
        job_id: str
    ) -> bool:
        """
        Trigger inference on a single miner.
        
        Args:
            validator: Validator instance
            miner_hotkey: Miner's hotkey
            job_id: Job ID to trigger inference for
            
        Returns:
            True if inference was successfully triggered, False otherwise
        """
        try:
            # Prepare inference trigger payload
            trigger_payload_data = WeatherStartInferenceData(job_id=job_id)
            trigger_payload = {
                "nonce": str(uuid.uuid4()),
                "data": trigger_payload_data.model_dump()
            }
            
            # Send trigger request to specific miner
            all_responses = await validator.query_miners(
                payload=trigger_payload,
                endpoint="/weather-start-inference",
                hotkeys=[miner_hotkey]
            )
            
            # Process response
            trigger_response = all_responses.get(miner_hotkey)
            if trigger_response:
                parsed_response = self._parse_miner_response(trigger_response)
                
                # Check if inference was successfully started
                if parsed_response.get("status") == "inference_started":
                    logger.info(f"Successfully triggered inference on {miner_hotkey[:8]} (Job: {job_id})")
                    return True
                else:
                    logger.warning(f"Failed to trigger inference on {miner_hotkey[:8]}: {parsed_response}")
                    return False
            else:
                logger.warning(f"No response from miner {miner_hotkey[:8]} for inference trigger")
                return False
                
        except Exception as e:
            logger.error(f"Error triggering inference on {miner_hotkey[:8]}: {e}")
            return False
    
    async def request_forecast_data(
        self,
        validator,
        miner_hotkey: str,
        job_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Request forecast data (zarr store URL and access token) from a miner.
        
        Args:
            validator: Validator instance
            miner_hotkey: Miner's hotkey
            job_id: Job ID to request forecast data for
            
        Returns:
            Forecast data response with zarr_store_url, access_token, verification_hash
        """
        try:
            logger.info(f"Requesting forecast data from {miner_hotkey[:8]} for job {job_id}")
            
            # Prepare forecast data request payload
            # Note: The actual payload structure would depend on the miner's API
            # This is a placeholder that matches the existing pattern
            forecast_payload = {
                "nonce": str(uuid.uuid4()),
                "data": {"job_id": job_id}
            }
            
            # Send request to specific miner
            all_responses = await validator.query_miners(
                payload=forecast_payload,
                endpoint="/weather-kerchunk-request",  # Existing endpoint
                hotkeys=[miner_hotkey]
            )
            
            # Process response
            forecast_response = all_responses.get(miner_hotkey)
            if forecast_response:
                parsed_response = self._parse_miner_response(forecast_response)
                
                # Validate that required fields are present
                required_fields = ["zarr_store_url", "verification_hash"]
                if all(field in parsed_response for field in required_fields):
                    logger.info(f"Successfully received forecast data from {miner_hotkey[:8]}")
                    return parsed_response
                else:
                    logger.warning(f"Incomplete forecast data from {miner_hotkey[:8]}: {parsed_response}")
                    return None
            else:
                logger.warning(f"No forecast data response from miner {miner_hotkey[:8]}")
                return None
                
        except Exception as e:
            logger.error(f"Error requesting forecast data from {miner_hotkey[:8]}: {e}")
            return None
    
    def _parse_miner_response(self, response: Any) -> Dict[str, Any]:
        """
        Parse miner response, handling both direct dict and text-wrapped responses.
        
        Args:
            response: Raw response from miner
            
        Returns:
            Parsed response dictionary
        """
        try:
            # Handle direct dict responses
            if isinstance(response, dict):
                # If response has a 'text' field, try to parse it as JSON
                if "text" in response:
                    import json
                    try:
                        return json.loads(response["text"])
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"Failed to parse response text as JSON: {e}")
                        return {
                            "status": "parse_error",
                            "message": f"JSON parse error: {e}"
                        }
                else:
                    # Direct dict response
                    return response
            
            # Handle string responses
            elif isinstance(response, str):
                import json
                try:
                    return json.loads(response)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse string response as JSON: {e}")
                    return {
                        "status": "parse_error", 
                        "message": f"JSON parse error: {e}"
                    }
            
            # Handle other types
            else:
                logger.warning(f"Unexpected response type: {type(response)}")
                return {
                    "status": "parse_error",
                    "message": f"Unexpected response type: {type(response)}"
                }
                
        except Exception as e:
            logger.error(f"Error parsing miner response: {e}")
            return {
                "status": "parse_error",
                "message": str(e)
            }