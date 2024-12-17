from gaia.tasks.base.task import Task
from datetime import datetime, timedelta, timezone
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
import importlib.util
import os
from gaia.tasks.base.components.metadata import Metadata
from gaia.tasks.defined_tasks.soilmoisture.soil_miner_preprocessing import (
    SoilMinerPreprocessing,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_scoring_mechanism import (
    SoilScoringMechanism,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_inputs import (
    SoilMoistureInputs,
    SoilMoisturePayload,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_outputs import (
    SoilMoistureOutputs,
    SoilMoisturePrediction,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_metadata import SoilMoistureMetadata
from gaia.tasks.defined_tasks.soilmoisture.utils.model_manager import ModelManager
from pydantic import Field
from fiber.logging_utils import get_logger
from uuid import uuid4
from gaia.validator.database.database_manager import ValidatorDatabaseManager
from sqlalchemy import text
from gaia.models.soil_moisture_basemodel import SoilModel
import traceback
import base64
import json
import asyncio
import tempfile
import math
import glob
from collections import defaultdict
from prefect import flow, get_run_logger, task

logger = get_logger(__name__)

if os.environ.get("NODE_TYPE") == "validator":
    from gaia.tasks.defined_tasks.soilmoisture.soil_validator_preprocessing import (
        SoilValidatorPreprocessing,
    )
else:
    SoilValidatorPreprocessing = None


class CircuitBreakerState:
    """Track circuit breaker state with automatic recovery."""
    def __init__(self, failure_threshold=3, recovery_timeout=300):
        self.failure_count = 0
        self.is_open = False
        self.last_failure_time = None
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

    def record_failure(self):
        """Record a failure and update circuit state."""
        self.failure_count += 1
        self.last_failure_time = datetime.now(timezone.utc)
        if self.failure_count >= self.failure_threshold:
            self.is_open = True

    def record_success(self):
        """Record a success and update circuit state."""
        self.failure_count = 0
        self.is_open = False
        self.last_failure_time = None

    def can_try_request(self) -> bool:
        """Check if a request can be attempted."""
        if not self.is_open:
            return True
        
        if self.last_failure_time is None:
            return True
            
        time_since_failure = (datetime.now(timezone.utc) - self.last_failure_time).total_seconds()
        if time_since_failure >= self.recovery_timeout:
            self.is_open = False
            self.failure_count = 0
            return True
            
        return False


class OperationError(Exception):
    """Base class for operation-specific errors."""
    def __init__(self, message: str, operation: str, is_recoverable: bool = True):
        self.message = message
        self.operation = operation
        self.is_recoverable = is_recoverable
        super().__init__(self.message)


class DataFetchError(OperationError):
    """Error during data fetching operations."""
    def __init__(self, message: str, is_recoverable: bool = True):
        super().__init__(message, "data_fetch", is_recoverable)


class MinerQueryError(OperationError):
    """Error during miner query operations."""
    def __init__(self, message: str, is_recoverable: bool = True):
        super().__init__(message, "miner_query", is_recoverable)


class ScoringError(OperationError):
    """Error during scoring operations."""
    def __init__(self, message: str, is_recoverable: bool = True):
        super().__init__(message, "scoring", is_recoverable)


class TaskStateManager:
    """Manages task state persistence and recovery."""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        
    async def save_task_state(self, task_id: str, state: dict):
        """Save task state to database."""
        try:
            # Convert sets to lists for JSON serialization
            state_copy = state.copy()
            state_copy['processed_regions'] = list(state['processed_regions'])
            state_copy['failed_regions'] = list(state['failed_regions'])
            
            await self.db_manager.execute(
                """
                INSERT INTO task_states (task_id, state, created_at, updated_at)
                VALUES (:task_id, :state, NOW(), NOW())
                ON CONFLICT (task_id) 
                DO UPDATE SET state = :state, updated_at = NOW()
                """,
                {
                    "task_id": task_id,
                    "state": json.dumps(state_copy)
                }
            )
        except Exception as e:
            logger.error(f"Error saving task state: {str(e)}")
            
    async def get_task_state(self, task_id: str) -> Optional[dict]:
        """Retrieve task state from database."""
        try:
            result = await self.db_manager.fetch_one(
                "SELECT state FROM task_states WHERE task_id = :task_id",
                {"task_id": task_id}
            )
            if result and result['state']:
                state = json.loads(result['state'])
                # Convert lists back to sets
                state['processed_regions'] = set(state['processed_regions'])
                state['failed_regions'] = set(state['failed_regions'])
                return state
        except Exception as e:
            logger.error(f"Error retrieving task state: {str(e)}")
        return None
        
    async def cleanup_old_states(self, days_old: int = 7):
        """Clean up task states older than specified days."""
        try:
            await self.db_manager.execute(
                """
                DELETE FROM task_states 
                WHERE updated_at < NOW() - INTERVAL ':days days'
                """,
                {"days": days_old}
            )
        except Exception as e:
            logger.error(f"Error cleaning up old task states: {str(e)}")


class SoilMoistureTask(Task):
    """Task for soil moisture prediction using satellite and weather data."""

    prediction_horizon: timedelta = Field(
        default_factory=lambda: timedelta(hours=6),
        description="Prediction horizon for the task",
    )
    scoring_delay: timedelta = Field(
        default_factory=lambda: timedelta(days=3),
        description="Delay before scoring due to SMAP data latency",
    )

    validator_preprocessing: Optional["SoilValidatorPreprocessing"] = None # type: ignore
    miner_preprocessing: Optional["SoilMinerPreprocessing"] = None
    model_manager: Optional[ModelManager] = None
    db_manager: Any = Field(default=None)
    node_type: str = Field(default="miner")
    test_mode: bool = Field(default=False)
    use_raw_preprocessing: bool = Field(default=False)
    state_manager: Optional["TaskStateManager"] = None

    def __init__(self, db_manager=None, node_type=None, test_mode=False, **data):
        super().__init__(
            name="SoilMoistureTask",
            description="Soil moisture prediction task",
            task_type="atomic",
            metadata=SoilMoistureMetadata(),
            inputs=SoilMoistureInputs(),
            outputs=SoilMoistureOutputs(),
            scoring_mechanism=SoilScoringMechanism(db_manager=db_manager),
        )

        self.db_manager = db_manager
        self.node_type = node_type
        self.test_mode = test_mode
        self.state_manager = TaskStateManager(db_manager) if db_manager else None

        if node_type == "validator":
            self.validator_preprocessing = SoilValidatorPreprocessing()
        else:
            self.miner_preprocessing = SoilMinerPreprocessing(task=self)
            self.model_manager = ModelManager()
            
            try:
                self.model_manager.initialize_model()
                self.use_raw_preprocessing = self.model_manager.get_model_type() == "custom"
                logger.info(f"Initialized {self.model_manager.get_model_type()} soil model")
            except Exception as e:
                logger.error(f"Error initializing model: {str(e)}")
                logger.error(traceback.format_exc())
                raise RuntimeError(f"Failed to initialize model: {str(e)}")

            logger.info("Initialized miner components for SoilMoistureTask")

        self._prepared_regions = {}
        
        # Initialize circuit breakers for critical services
        self.circuit_breakers = {
            'smap_api': CircuitBreakerState(),
            'miner_query': CircuitBreakerState(failure_threshold=5),
            'database': CircuitBreakerState(recovery_timeout=600),
            'sentinel_api': CircuitBreakerState(),
            'ifs_api': CircuitBreakerState()
        }

    async def _check_circuit_state(self, service_name: str) -> Dict[str, Any]:
        """Check if circuit breaker is closed for a service."""
        if service_name not in self.circuit_breakers:
            return {"is_closed": True, "failure_count": 0, "next_attempt": datetime.now(timezone.utc)}
            
        breaker = self.circuit_breakers[service_name]
        if not breaker.can_try_request():
            next_attempt = breaker.last_failure_time + timedelta(seconds=breaker.recovery_timeout)
            return {
                "is_closed": False,
                "failure_count": breaker.failure_count,
                "next_attempt": next_attempt
            }
            
        return {
            "is_closed": True,
            "failure_count": breaker.failure_count,
            "next_attempt": datetime.now(timezone.utc)
        }

    async def _record_failure(self, service_name: str):
        """Record a service failure and update circuit breaker state."""
        if service_name in self.circuit_breakers:
            breaker = self.circuit_breakers[service_name]
            breaker.record_failure()
            
            logger.warning(
                f"Service {service_name} failed. Failure count: {breaker.failure_count}. "
                f"Circuit breaker {'open' if breaker.is_open else 'closed'}."
            )

    async def _close_circuit(self, service_name: str):
        """Reset circuit breaker state after successful operation."""
        if service_name in self.circuit_breakers:
            breaker = self.circuit_breakers[service_name]
            breaker.record_success()
            logger.info(f"Circuit breaker closed for service: {service_name}")

    def get_next_preparation_time(self, current_time: datetime) -> datetime:
        """Get the next preparation window start time."""
        windows = self.get_validator_windows()
        current_mins = current_time.hour * 60 + current_time.minute

        for start_hr, start_min, _, _ in windows:
            window_start_mins = start_hr * 60 + start_min
            if window_start_mins > current_mins:
                return current_time.replace(
                    hour=start_hr, minute=start_min, second=0, microsecond=0
                )

        tomorrow = current_time + timedelta(days=1)
        first_window = windows[0]
        return tomorrow.replace(
            hour=first_window[0], minute=first_window[1], second=0, microsecond=0
        )

    async def validator_execute(self, validator):
        """Execute validator workflow."""
        if not hasattr(self, "db_manager") or self.db_manager is None:
            self.db_manager = validator.db_manager

        while True:
            try:
                current_time = datetime.now(timezone.utc)

                if current_time.minute % 1 == 0:
                    await self.validator_score()

                if self.test_mode:
                    logger.info("Running in test mode - bypassing window checks")
                    target_smap_time = self.get_smap_time_for_validator(current_time)
                    ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)

                    await self.validator_preprocessing.get_daily_regions(
                        target_time=target_smap_time,
                        ifs_forecast_time=ifs_forecast_time,
                    )

                    regions = await self.db_manager.fetch_many(
                        """
                        SELECT * FROM soil_moisture_regions 
                        WHERE status = 'pending'
                        AND target_time = :target_time
                        """,
                        {"target_time": target_smap_time},
                    )

                    if regions:
                        for region in regions:
                            try:
                                logger.info(f"Processing region {region['id']}")

                                if "combined_data" not in region:
                                    logger.error(
                                        f"Region {region['id']} missing combined_data field"
                                    )
                                    continue

                                if not region["combined_data"]:
                                    logger.error(
                                        f"Region {region['id']} has null combined_data"
                                    )
                                    continue

                                combined_data = region["combined_data"]
                                if not isinstance(combined_data, bytes):
                                    logger.error(
                                        f"Region {region['id']} has invalid data type: {type(combined_data)}"
                                    )
                                    continue

                                if not (
                                    combined_data.startswith(b"II\x2A\x00")
                                    or combined_data.startswith(b"MM\x00\x2A")
                                ):
                                    logger.error(
                                        f"Region {region['id']} has invalid TIFF header"
                                    )
                                    logger.error(
                                        f"First 16 bytes: {combined_data[:16].hex()}"
                                    )
                                    continue

                                logger.info(
                                    f"Region {region['id']} TIFF size: {len(combined_data) / (1024 * 1024):.2f} MB"
                                )
                                logger.info(
                                    f"Region {region['id']} TIFF header: {combined_data[:4]}"
                                )
                                logger.info(
                                    f"Region {region['id']} TIFF header hex: {combined_data[:16].hex()}"
                                )
                                encoded_data = base64.b64encode(combined_data)
                                logger.info(
                                    f"Base64 first 16 chars: {encoded_data[:16]}"
                                )

                                task_data = {
                                    "region_id": region["id"],
                                    "combined_data": encoded_data.decode("ascii"),
                                    "sentinel_bounds": region["sentinel_bounds"],
                                    "sentinel_crs": region["sentinel_crs"],
                                    "target_time": target_smap_time.isoformat(),
                                }

                                payload = {"nonce": str(uuid4()), "data": task_data}

                                logger.info(
                                    f"Sending region {region['id']} to miners..."
                                )
                                responses = await validator.query_miners(
                                    payload=payload, endpoint="/soilmoisture-request"
                                )

                                if responses:
                                    metadata = {
                                        "region_id": region["id"],
                                        "target_time": target_smap_time,
                                        "data_collection_time": current_time,
                                        "ifs_forecast_time": ifs_forecast_time,
                                    }
                                    await self.add_task_to_queue(responses, metadata)

                                    await self.db_manager.execute(
                                        """
                                        UPDATE soil_moisture_regions 
                                        SET status = 'sent_to_miners'
                                        WHERE id = :region_id
                                        """,
                                        {"region_id": region["id"]},
                                    )

                            except Exception as e:
                                logger.error(f"Error preparing region: {str(e)}")
                                continue

                    logger.info("Test mode execution complete. Disabling test mode.")
                    self.test_mode = False
                    continue

                windows = self.get_validator_windows()
                current_window = next(
                    (w for w in windows if self.is_in_window(current_time, w)), None
                )

                if not current_window:
                    logger.info(
                        f"Not in any preparation or execution window: {current_time}"
                    )
                    logger.info(
                        f"Next soil task time: {self.get_next_preparation_time(current_time)}"
                    )
                    logger.info(f"Sleeping for 60 seconds")
                    await asyncio.sleep(60)
                    continue

                is_prep = current_window[1] == 30  # If minutes = 30, it's a prep window

                target_smap_time = self.get_smap_time_for_validator(current_time)
                ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)

                if is_prep:
                    await self.validator_preprocessing.get_daily_regions(
                        target_time=target_smap_time,
                        ifs_forecast_time=ifs_forecast_time,
                    )
                else:
                    # Execution window logic
                    regions = await self.db_manager.fetch_many(
                        """
                        SELECT * FROM soil_moisture_regions 
                        WHERE status = 'pending'
                        AND target_time = :target_time
                        """,
                        {"target_time": target_smap_time},
                    )

                    if regions:
                        for region in regions:
                            try:
                                logger.info(f"Processing region {region['id']}")

                                if "combined_data" not in region:
                                    logger.error(
                                        f"Region {region['id']} missing combined_data field"
                                    )
                                    continue

                                if not region["combined_data"]:
                                    logger.error(
                                        f"Region {region['id']} has null combined_data"
                                    )
                                    continue

                                # Validate TIFF data retrieved from database
                                combined_data = region["combined_data"]
                                if not isinstance(combined_data, bytes):
                                    logger.error(
                                        f"Region {region['id']} has invalid data type: {type(combined_data)}"
                                    )
                                    continue

                                if not (
                                    combined_data.startswith(b"II\x2A\x00")
                                    or combined_data.startswith(b"MM\x00\x2A")
                                ):
                                    logger.error(
                                        f"Region {region['id']} has invalid TIFF header"
                                    )
                                    logger.error(
                                        f"First 16 bytes: {combined_data[:16].hex()}"
                                    )
                                    continue

                                logger.info(
                                    f"Region {region['id']} TIFF size: {len(combined_data) / (1024 * 1024):.2f} MB"
                                )
                                logger.info(
                                    f"Region {region['id']} TIFF header: {combined_data[:4]}"
                                )
                                logger.info(
                                    f"Region {region['id']} TIFF header hex: {combined_data[:16].hex()}"
                                )
                                encoded_data = base64.b64encode(combined_data)
                                logger.info(
                                    f"Base64 first 16 chars: {encoded_data[:16]}"
                                )

                                task_data = {
                                    "region_id": region["id"],
                                    "combined_data": encoded_data.decode("ascii"),
                                    "sentinel_bounds": region["sentinel_bounds"],
                                    "sentinel_crs": region["sentinel_crs"],
                                    "target_time": target_smap_time.isoformat(),
                                }

                                payload = {
                                    "nonce": str(uuid4()),
                                    "data": {
                                        "region_id": task_data["region_id"],
                                        "combined_data": task_data["combined_data"],
                                        "sentinel_bounds": task_data["sentinel_bounds"],
                                        "sentinel_crs": task_data["sentinel_crs"],
                                        "target_time": task_data["target_time"],
                                    },
                                }

                                logger.info(
                                    f"Sending payload to miners with region_id: {task_data['region_id']}"
                                )
                                try:
                                    responses = await validator.query_miners(
                                        payload=payload,
                                        endpoint="/soilmoisture-request",
                                    )

                                    if not responses:
                                        logger.error(
                                            "No responses received from miners"
                                        )
                                        return

                                    logger.info(
                                        f"Received {len(responses)} responses from miners"
                                    )
                                    first_hotkey = (
                                        next(iter(responses)) if responses else None
                                    )
                                    logger.debug(
                                        f"First response structure: {responses[first_hotkey].keys() if first_hotkey else 'No responses'}"
                                    )

                                except Exception as e:
                                    logger.error(f"Error querying miners: {str(e)}")
                                    logger.error(traceback.format_exc())
                                    return

                                if responses:
                                    metadata = {
                                        "region_id": region["id"],
                                        "target_time": target_smap_time,
                                        "data_collection_time": current_time,
                                        "ifs_forecast_time": ifs_forecast_time,
                                    }
                                    await self.add_task_to_queue(responses, metadata)

                                    await self.db_manager.execute(
                                        """
                                        UPDATE soil_moisture_regions 
                                        SET status = 'sent_to_miners'
                                        WHERE id = :region_id
                                        """,
                                        {"region_id": region["id"]},
                                    )

                            except Exception as e:
                                logger.error(f"Error preparing region: {str(e)}")
                                logger.error(traceback.format_exc())
                                continue

                # Get all prep windows
                prep_windows = [w for w in self.get_validator_windows() if w[1] == 0]
                in_any_prep = any(
                    self.is_in_window(current_time, w) for w in prep_windows
                )

                if not in_any_prep:
                    next_prep_time = self.get_next_preparation_time(current_time)
                    sleep_seconds = (
                        next_prep_time - datetime.now(timezone.utc)
                    ).total_seconds()
                    if sleep_seconds > 0:
                        logger.info(
                            f"Sleeping until next soil task window: {next_prep_time}"
                        )
                        await asyncio.sleep(sleep_seconds)

            except Exception as e:
                logger.error(f"Error in validator_execute: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(60)

    async def get_todays_regions(self, target_time: datetime) -> List[Dict]:
        """Get regions already selected for today."""
        try:
            async with self.db_manager.get_connection() as conn:
                regions = await conn.fetch(
                    """
                    SELECT * FROM soil_moisture_regions
                    WHERE region_date = $1::date
                    AND status = 'pending'
                """,
                    target_time.date(),
                )
                return regions
        except Exception as e:
            logger.error(f"Error getting today's regions: {str(e)}")
            return []

    async def miner_execute(self, data: Dict[str, Any], miner) -> Dict[str, Any]:
        """Execute miner workflow."""
        try:
            processed_data = await self.miner_preprocessing.process_miner_data(
                data["data"]
            )
            
            if hasattr(self.model, "run_inference"):
                predictions = self.model.run_inference(processed_data) # Custom model inference
            else:
                predictions = self.run_model_inference(processed_data) # Base model inference

            try:
                # Visualization disabled for now
                # import matplotlib.pyplot as plt
                # import numpy as np

                # fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
                # surface_plot = ax1.imshow(predictions["surface"], cmap='viridis')
                # ax1.set_title('Surface Soil Moisture')
                # plt.colorbar(surface_plot, ax=ax1, label='Moisture Content')
                # rootzone_plot = ax2.imshow(predictions["rootzone"], cmap='viridis')
                # ax2.set_title('Root Zone Soil Moisture')
                # plt.colorbar(rootzone_plot, ax=ax2, label='Moisture Content')

                # plt.tight_layout()
                # plt.savefig('soil_moisture_predictions.png')
                # plt.close()

                # logger.info("Saved prediction visualization to soil_moisture_predictions.png")
                logger.info(
                    f"Surface moisture stats - Min: {predictions['surface'].min():.3f}, "
                    f"Max: {predictions['surface'].max():.3f}, "
                    f"Mean: {predictions['surface'].mean():.3f}"
                )
                logger.info(
                    f"Root zone moisture stats - Min: {predictions['rootzone'].min():.3f}, "
                    f"Max: {predictions['rootzone'].max():.3f}, "
                    f"Mean: {predictions['rootzone'].mean():.3f}"
                )

            except Exception as e:
                logger.error(f"Error creating visualization: {str(e)}")
            target_time = data["data"]["target_time"]
            if isinstance(target_time, datetime):
                pass
            elif isinstance(target_time, str):
                target_time = datetime.fromisoformat(target_time)
            else:
                logger.error(f"Unexpected target_time type: {type(target_time)}")
                raise ValueError(f"Unexpected target_time format: {target_time}")

            prediction_time = self.get_next_preparation_time(target_time)

            return {
                "surface_sm": predictions["surface"].tolist(),
                "rootzone_sm": predictions["rootzone"].tolist(),
                "uncertainty_surface": None,
                "uncertainty_rootzone": None,
                "miner_hotkey": miner.keypair.ss58_address,
                "sentinel_bounds": data["data"]["sentinel_bounds"],
                "sentinel_crs": data["data"]["sentinel_crs"],
                "target_time": prediction_time.isoformat(),
            }

        except Exception as e:
            logger.error(f"Error in miner execution: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def add_task_to_queue(
        self, responses: Dict[str, Any], metadata: Dict[str, Any]
    ):
        try:
            if not self.db_manager:
                raise RuntimeError("Database manager not initialized")

            region_update = """
            UPDATE soil_moisture_regions 
            SET status = 'sent_to_miners' 
            WHERE id = :region_id
            """
            await self.db_manager.execute(region_update, {"region_id": metadata["region_id"]})

            for miner_hotkey, response_data in responses.items():
                try:
                    # Get miner UID from hotkey
                    query = "SELECT uid FROM node_table WHERE hotkey = :miner_hotkey"
                    result = await self.db_manager.fetch_one(
                        query, {"miner_hotkey": miner_hotkey}
                    )

                    if not result:
                        logger.warning(f"No UID found for hotkey {miner_hotkey}")
                        continue

                    miner_uid = str(result["uid"])

                    if isinstance(response_data, dict) and "text" in response_data:
                        try:
                            response_data = json.loads(response_data["text"])
                        except json.JSONDecodeError as e:
                            logger.error(
                                f"Failed to parse response text for {miner_hotkey}: {e}"
                            )
                            continue

                    prediction_data = {
                        "region_id": metadata["region_id"],
                        "miner_uid": miner_uid,  # Now a string
                        "miner_hotkey": miner_hotkey,
                        "target_time": metadata["target_time"],
                        "surface_sm": response_data.get("surface_sm", []),
                        "rootzone_sm": response_data.get("rootzone_sm", []),
                        "uncertainty_surface": response_data.get(
                            "uncertainty_surface"
                        ),
                        "uncertainty_rootzone": response_data.get(
                            "uncertainty_rootzone"
                        ),
                        "sentinel_bounds": response_data.get(
                            "sentinel_bounds", metadata.get("sentinel_bounds")
                        ),
                        "sentinel_crs": response_data.get(
                            "sentinel_crs", metadata.get("sentinel_crs")
                        ),
                        "status": "sent_to_miner",
                    }

                    await self.db_manager.execute(
                        """
                        INSERT INTO soil_moisture_predictions 
                        (region_id, miner_uid, miner_hotkey, target_time, surface_sm, rootzone_sm, 
                        uncertainty_surface, uncertainty_rootzone, sentinel_bounds, 
                        sentinel_crs, status)
                        VALUES 
                        (:region_id, :miner_uid, :miner_hotkey, :target_time, 
                        :surface_sm, :rootzone_sm,
                        :uncertainty_surface, :uncertainty_rootzone, :sentinel_bounds,
                        :sentinel_crs, :status)
                        """,
                        prediction_data,
                    )

                    logger.info(
                        f"Stored predictions from miner {miner_hotkey} (UID: {miner_uid}) for region {metadata['region_id']}"
                    )

                except Exception as e:
                    logger.error(
                        f"Error processing response from miner {miner_hotkey}: {str(e)}"
                    )
                    logger.error(traceback.format_exc())
                    continue

        except Exception as e:
            logger.error(f"Error storing predictions: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def get_pending_tasks(self):
        """Get tasks that are ready for scoring and haven't been scored yet."""
        scoring_time = datetime.now(timezone.utc) - self.scoring_delay
        #scoring_time = scoring_time.replace(hour=19, minute=30, second=0, microsecond=0) this is for testing
        
        try:
            debug_result = await self.db_manager.fetch_many(
                """
                SELECT p.status, COUNT(*) as count, MIN(r.target_time) as earliest, MAX(r.target_time) as latest
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                GROUP BY p.status
                """
            )
            logger.debug("Current prediction status counts:")
            for row in debug_result:
                logger.debug(f"Status: {row['status']}, Count: {row['count']}, Time Range: {row['earliest']} to {row['latest']}")

            result = await self.db_manager.fetch_many(
                """
                SELECT 
                    r.*,
                    json_agg(json_build_object(
                        'miner_id', p.miner_uid,
                        'miner_hotkey', p.miner_hotkey,
                        'surface_sm', p.surface_sm,
                        'rootzone_sm', p.rootzone_sm,
                        'uncertainty_surface', p.uncertainty_surface,
                        'uncertainty_rootzone', p.uncertainty_rootzone
                    )) as predictions
                FROM soil_moisture_regions r
                JOIN soil_moisture_predictions p ON p.region_id = r.id
                WHERE r.target_time <= :scoring_time
                AND p.status = 'sent_to_miner'
                GROUP BY r.id, r.target_time, r.sentinel_bounds, r.sentinel_crs, r.status
                ORDER BY r.target_time ASC
                """,
                {"scoring_time": scoring_time}
            )
            logger.debug(f"Found {len(result) if result else 0} pending tasks")
            return result

        except Exception as e:
            logger.error(f"Error fetching pending tasks: {str(e)}")
            return []

    @task(
        name="validator_score_task",
        retries=3,
        retry_delay_seconds=lambda x: 60 * (2 ** x),
        retry_jitter_factor=0.2,
        tags=["scoring"],
        description="Score pending predictions and update task history"
    )
    async def validator_score(self, result=None):
        """Score task results by timestamp to minimize SMAP data downloads."""
        try:
            pending_tasks = await self.get_pending_tasks()
            if not pending_tasks:
                return {"status": "no_pending_tasks"}

            scoring_results = {}
            tasks_by_time = {}
            for task in pending_tasks:
                target_time = task["target_time"]
                if target_time not in tasks_by_time:
                    tasks_by_time[target_time] = []
                tasks_by_time[target_time].append(task)

            for target_time, tasks in tasks_by_time.items():
                logger.info(f"Processing {len(tasks)} predictions for timestamp {target_time}")
                scored_tasks = []

                for task in tasks:
                    try:
                        scores = {}
                        for prediction in task["predictions"]:
                            pred_data = {
                                "bounds": task["sentinel_bounds"],
                                "crs": task["sentinel_crs"],
                                "predictions": prediction,
                                "target_time": target_time,
                                "region": {"id": task["id"]},
                                "miner_id": prediction["miner_id"],
                                "miner_hotkey": prediction["miner_hotkey"]
                            }
                            
                            score = await self.scoring_mechanism.score(pred_data)
                            if score:
                                scores = score
                                task["score"] = score
                                scored_tasks.append(task)

                    except Exception as e:
                        logger.error(f"Error scoring task {task['id']}: {str(e)}")
                        continue

                if scored_tasks:
                    current_time = datetime.now(timezone.utc)
                    try:
                        # Create task group for scoring operations
                        history_futures = []
                        score_row_futures = []
                        cleanup_futures = []

                        # Submit history tasks first
                        for task in scored_tasks:
                            future = await self.move_task_to_history_task.submit(
                                region=task,
                                predictions=task["predictions"],
                                ground_truth=task["score"].get("ground_truth"),
                                scores=task["score"]
                            )
                            history_futures.append(future)

                        # Wait for history tasks to complete
                        await asyncio.gather(*history_futures)

                        # Submit score row task
                        score_row_future = await self.build_score_row_task.submit(
                            target_time=target_time,
                            recent_tasks=scored_tasks
                        )
                        score_row_futures.append(score_row_future)

                        # Wait for score row task to complete
                        score_rows = await asyncio.gather(*score_row_futures)
                        score_rows = score_rows[0]  # Unpack single result
                        
                        if score_rows:
                            query = """
                            INSERT INTO score_table (task_name, task_id, score, status)
                            VALUES (:task_name, :task_id, :score, :status)
                            """
                            for row in score_rows:
                                await self.db_manager.execute(query, row)
                            logger.info(f"Stored global scores for timestamp {target_time}")

                        # Submit cleanup tasks only after history and scoring are complete
                        for task in scored_tasks:
                            cleanup_future = await self.cleanup_predictions_task.submit(
                                bounds=task["sentinel_bounds"],
                                target_time=task["target_time"],
                                miner_uid=task["predictions"][0]["miner_id"]
                            )
                            cleanup_futures.append(cleanup_future)

                        # Wait for all cleanup tasks
                        await asyncio.gather(*cleanup_futures)

                        # Finally, cleanup SMAP files
                        await self.cleanup_smap_files_task.submit()

                    except Exception as e:
                        logger.error(f"Error processing scored tasks: {str(e)}")
                        logger.error(traceback.format_exc())
                        continue

            return {"status": "success", "results": scoring_results}

        except Exception as e:
            logger.error(f"Error in validator_score: {str(e)}")
            return {"status": "error", "message": str(e)}

    @task(
        name="move_task_to_history_task",
        retries=2,
        retry_delay_seconds=30,
        tags=["database"],
        description="Move scored task to history table"
    )
    async def move_task_to_history_task(
        self,
        region: Dict,
        predictions: Dict,
        ground_truth: Dict,
        scores: Dict
    ):
        """Move scored task to history with proper error handling."""
        conn = None
        try:
            logger.info(f"Moving task to history for region {region['id']}")
            logger.info(f"Final scores for region {region['id']}:")
            logger.info(f"Surface RMSE: {scores['metrics'].get('surface_rmse'):.4f}")
            logger.info(f"Surface SSIM: {scores['metrics'].get('surface_ssim', 0):.4f}")
            logger.info(f"Rootzone RMSE: {scores['metrics'].get('rootzone_rmse'):.4f}")
            logger.info(f"Rootzone SSIM: {scores['metrics'].get('rootzone_ssim', 0):.4f}")
            logger.info(f"Total Score: {scores.get('total_score', 0):.4f}")

            conn = await self.db_manager.get_connection()
            async with conn as session:
                async with session.begin():
                    for prediction in predictions:
                        miner_id = prediction["miner_id"]
                        params = {
                            "region_id": region["id"],
                            "miner_uid": miner_id,
                            "miner_hotkey": prediction.get("miner_hotkey", ""),
                            "target_time": region["target_time"],
                            "surface_sm_pred": prediction["surface_sm"],
                            "rootzone_sm_pred": prediction["rootzone_sm"],
                            "surface_sm_truth": ground_truth["surface_sm"] if ground_truth else None,
                            "rootzone_sm_truth": ground_truth["rootzone_sm"] if ground_truth else None,
                            "surface_rmse": scores["metrics"].get("surface_rmse"),
                            "rootzone_rmse": scores["metrics"].get("rootzone_rmse"),
                            "surface_structure_score": scores["metrics"].get("surface_ssim", 0),
                            "rootzone_structure_score": scores["metrics"].get("rootzone_ssim", 0),
                        }

                        await session.execute(
                            text("""
                                INSERT INTO soil_moisture_history 
                                (region_id, miner_uid, miner_hotkey, target_time, 
                                 surface_sm_pred, rootzone_sm_pred,
                                 surface_sm_truth, rootzone_sm_truth,
                                 surface_rmse, rootzone_rmse,
                                 surface_structure_score, rootzone_structure_score)
                                VALUES 
                                (:region_id, :miner_uid, :miner_hotkey, :target_time,
                                 :surface_sm_pred, :rootzone_sm_pred,
                                 :surface_sm_truth, :rootzone_sm_truth,
                                 :surface_rmse, :rootzone_rmse,
                                 :surface_structure_score, :rootzone_structure_score)
                            """),
                            params
                        )
                        logger.info(f"Stored scores for miner {miner_id} in region {region['id']}")

                        await session.execute(
                            text("""
                                UPDATE soil_moisture_predictions 
                                SET status = 'scored'
                                WHERE region_id = :region_id AND miner_uid = :miner_uid
                            """),
                            params
                        )

            # Trigger cleanup after successful history move
            await self.cleanup_predictions_task.submit(
                bounds=region["sentinel_bounds"],
                target_time=region["target_time"],
                miner_uid=miner_id
            )
            
        except Exception as e:
            logger.error(f"Failed to move task to history: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if conn:
                await conn.close()

    @task(
        name="build_score_row_task",
        retries=2,
        retry_delay_seconds=30,
        tags=["scoring"],
        description="Build score row for global scoring mechanism"
    )
    async def build_score_row_task(self, target_time, recent_tasks=None):
        """Build score row for global scoring mechanism with proper error handling."""
        try:
            current_time = datetime.now(timezone.utc)
            scores = [float("nan")] * 256
            
            miner_query = "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL"
            miner_mappings = await self.db_manager.fetch_many(miner_query)
            hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}
            logger.info(f"Found {len(hotkey_to_uid)} miner mappings")

            if recent_tasks:
                logger.info(f"Processing {len(recent_tasks)} recent tasks")
                miner_scores = {}
                
                for task in recent_tasks:
                    task_score = task.get("score", {})
                    for prediction in task.get("predictions", []):
                        miner_id = prediction.get("miner_id")
                        if miner_id not in miner_scores:
                            miner_scores[miner_id] = []
                        if isinstance(task_score.get("total_score"), (int, float)):
                            miner_scores[miner_id].append(float(task_score["total_score"]))

                for miner_id, scores_list in miner_scores.items():
                    if scores_list:
                        scores[int(miner_id)] = sum(scores_list) / len(scores_list)
                        logger.info(f"Final average score for miner {miner_id}: {scores[int(miner_id)]}")

                score_row = {
                    "task_name": "soil_moisture",
                    "task_id": str(current_time.timestamp()),
                    "score": scores,
                    "status": "completed"
                }
                
                return [score_row]

        except Exception as e:
            logger.error(f"Error building score row: {e}")
            logger.error(traceback.format_exc())
            return []

    @task(
        name="cleanup_predictions_task",
        retries=2,
        retry_delay_seconds=30,
        tags=["cleanup"],
        description="Clean up processed predictions"
    )
    async def cleanup_predictions_task(self, bounds, target_time=None, miner_uid=None):
        """Clean up predictions after processing with proper error handling."""
        try:
            conn = await self.db_manager.get_connection()
            try:
                async with conn.begin():
                    result = await conn.execute(
                        text("""
                        DELETE FROM soil_moisture_predictions p
                        USING soil_moisture_regions r
                        WHERE p.region_id = r.id 
                        AND r.sentinel_bounds = :bounds
                        AND r.target_time = :target_time
                        AND p.miner_uid = :miner_uid
                        AND p.status = 'scored'
                        RETURNING p.id
                        """),
                        {
                            "bounds": bounds,
                            "target_time": target_time,
                            "miner_uid": miner_uid
                        }
                    )
                    
                    deleted_count = result.rowcount
                    logger.info(
                        f"Cleaned up {deleted_count} predictions for bounds {bounds}"
                        f"{f', time {target_time}' if target_time else ''}"
                        f"{f', miner {miner_uid}' if miner_uid else ''}"
                    )
                    
            finally:
                await conn.close()

        except Exception as e:
            logger.error(f"Failed to cleanup predictions: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @task(
        name="cleanup_smap_files_task",
        retries=2,
        retry_delay_seconds=30,
        tags=["cleanup"],
        description="Clean up SMAP files after processing"
    )
    async def cleanup_smap_files_task(self):
        """Clean up SMAP files after processing."""
        try:
            smap_cache_dir = "smap_cache"
            if os.path.exists(smap_cache_dir):
                for file in os.listdir(smap_cache_dir):
                    if file.endswith(".h5"):
                        file_path = os.path.join(smap_cache_dir, file)
                        try:
                            os.remove(file_path)
                            logger.info(f"Cleaned up SMAP file {file}")
                        except Exception as e:
                            logger.error(f"Error cleaning up file {file}: {str(e)}")
                            
            tmp_files = glob.glob('/tmp/tmp*.h5')
            for file in tmp_files:
                try:
                    os.remove(file)
                    logger.info(f"Cleaned up temporary file in /tmp: {file}")
                except Exception as e:
                    logger.error(f"Error cleaning up tmp file {file}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error in cleanup_smap_files: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def run_model_inference(self, processed_data):
        """Run model inference on processed data."""
        if not self.model_manager:
            raise RuntimeError(
                "Model manager not initialized. Are you running on a miner node?"
            )

        model = self.model_manager.get_model()
        if not model:
            raise RuntimeError("No model available for inference")

        if self.use_raw_preprocessing:
            # Custom model handles its own preprocessing
            return model.run_inference(processed_data)
        else:
            # Use base model preprocessing
            return self.miner_preprocessing.predict_smap(processed_data, model)

    def get_ifs_time_for_smap(self, smap_time: datetime) -> datetime:
        """Get corresponding IFS forecast time for SMAP target time."""
        smap_to_ifs = {
            1: 0,  # 01:30 uses 00:00 forecast
            7: 6,  # 07:30 uses 06:00 forecast
            13: 12,  # 13:30 uses 12:00 forecast
            19: 18,  # 19:30 uses 18:00 forecast
        }

        ifs_hour = smap_to_ifs.get(smap_time.hour)
        if ifs_hour is None:
            raise ValueError(f"Invalid SMAP time: {smap_time.hour}:30")

        return smap_time.replace(hour=ifs_hour, minute=0, second=0, microsecond=0)

    def get_smap_time_for_validator(self, current_time: datetime) -> datetime:
        """Get SMAP time based on validator execution time."""
        if self.test_mode:
            smap_hours = [1, 7, 13, 19]
            current_hour = current_time.hour
            closest_hour = min(smap_hours, key=lambda x: abs(x - current_hour))
            return current_time.replace(
                hour=7, minute=30, second=0, microsecond=0
            )

        validator_to_smap = {
            1: 1,    # 1:30 prep → 1:30 SMAP
            2: 1,    # 2:00 execution → 1:30 SMAP
            9: 7,    # 9:30 prep → 7:30 SMAP
            10: 7,   # 10:00 execution → 7:30 SMAP
            13: 13,  # 13:30 prep → 13:30 SMAP
            14: 13,  # 14:00 execution → 13:30 SMAP
            19: 19,  # 19:30 prep → 19:30 SMAP
            20: 19,  # 20:00 execution → 19:30 SMAP
        }
        smap_hour = validator_to_smap.get(current_time.hour)
        if smap_hour is None:
            raise ValueError(f"No SMAP time mapping for validator hour {current_time.hour}")

        return current_time.replace(hour=smap_hour, minute=30, second=0, microsecond=0)

    def get_validator_windows(self) -> List[Tuple[int, int, int, int]]:
        """Get all validator windows (hour_start, min_start, hour_end, min_end)."""
        return [
            (1, 30, 2, 0),  # Prep window for 1:30 SMAP time
            (2, 0, 2, 30),  # Execution window for 1:30 SMAP time
            (9, 30, 10, 0),  # Prep window for 7:30 SMAP time
            (10, 0, 10, 30),  # Execution window for 7:30 SMAP time
            (13, 30, 14, 0),  # Prep window for 13:30 SMAP time
            (14, 0, 14, 30),  # Execution window for 13:30 SMAP time
            (19, 30, 20, 0),  # Prep window for 19:30 SMAP time
            (20, 0, 20, 30),  # Execution window for 19:30 SMAP time
        ]

    def is_in_window(
        self, current_time: datetime, window: Tuple[int, int, int, int]
    ) -> bool:
        """Check if current time is within a specific window."""
        start_hr, start_min, end_hr, end_min = window
        current_mins = current_time.hour * 60 + current_time.minute
        window_start_mins = start_hr * 60 + start_min
        window_end_mins = end_hr * 60 + end_min
        return window_start_mins <= current_mins < window_end_mins

    def miner_preprocess(self, preprocessing=None, inputs=None):
        """Preprocess data for model input."""
        pass

    async def create_flow(self, validator):
        """Create a Prefect flow for this task."""
        task_name = self.name.lower().replace(" ", "_")
        
        @flow(
            name=f"{task_name}_flow",
            description=f"Flow for {self.name}",
            version=validator.args.version,
            retries=3,
            retry_delay_seconds=60,
            timeout_seconds=3600
        )
        async def task_flow():
            flow_logger = get_run_logger()
            flow_logger.info(f"Starting {self.name} flow")
            
            # Initialize flow metrics
            flow_metrics = {
                'processing_time': {},
                'success_rate': {},
                'error_counts': {},
                'prediction_stats': {}
            }
            
            # Initialize flow state
            flow_state = {
                'current_time': datetime.now(timezone.utc),
                'validator': validator,
                'flow_id': str(uuid4()),
                'status': 'running',
                'error_count': 0,
                'last_successful_step': None,
                'recovery_attempts': 0,
                'metrics': flow_metrics
            }
            
            try:
                # Execute the validator task
                await self.validator_execute_task.submit(validator=validator)
                flow_state['status'] = 'completed'
                
            except Exception as e:
                flow_state['status'] = 'failed'
                flow_state['error_count'] += 1
                flow_metrics['error_counts'][str(e)] = flow_metrics['error_counts'].get(str(e), 0) + 1
                
                flow_logger.error(f"Flow failed: {str(e)}")
                flow_logger.error(traceback.format_exc())
                raise
                
            return flow_state
            
        return task_flow

    @task(
        name="validator_execute_task",
        retries=3,
        retry_delay_seconds=lambda x: 60 * (2 ** x),
        retry_jitter_factor=0.2,
        tags=["validator"],
        description="Execute validator workflow for soil moisture predictions"
    )
    async def validator_execute_task(self, validator):
        """Execute validator workflow with proper error handling and state recovery."""
        flow_logger = get_run_logger()
        flow_logger.info("Starting validator execution task")
        
        # Generate unique task ID for this execution
        task_id = f"soil_moisture_task_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{str(uuid4())[:8]}"
        
        # Try to recover existing state
        task_state = None
        if self.state_manager:
            task_state = await self.state_manager.get_task_state(task_id)
        
        # Initialize new state if none exists
        if not task_state:
            task_state = {
                'task_id': task_id,
                'current_step': None,
                'last_successful_step': None,
                'retry_count': defaultdict(int),
                'errors': defaultdict(list),
                'processed_regions': set(),
                'failed_regions': set(),
                'start_time': datetime.now(timezone.utc).isoformat()
            }
        
        try:
            current_time = datetime.now(timezone.utc)
            task_state['current_step'] = 'initialization'
            
            # Persist initial state
            if self.state_manager:
                await self.state_manager.save_task_state(task_id, task_state)
            
            # Check if we need to score any pending predictions
            if current_time.minute % 1 == 0:
                task_state['current_step'] = 'scoring'
                try:
                    await self.validator_score()
                    task_state['last_successful_step'] = 'scoring'
                    if self.state_manager:
                        await self.state_manager.save_task_state(task_id, task_state)
                except Exception as e:
                    task_state['errors']['scoring'].append(str(e))
                    task_state['retry_count']['scoring'] += 1
                    flow_logger.error(f"Error in scoring step: {str(e)}")
                    if self.state_manager:
                        await self.state_manager.save_task_state(task_id, task_state)
                    # Continue execution as scoring errors are non-fatal

            # Handle test mode execution
            if self.test_mode:
                flow_logger.info("Running in test mode - bypassing window checks")
                target_smap_time = self.get_smap_time_for_validator(current_time)
                ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)
                
                task_state['current_step'] = 'test_mode_execution'
                if self.state_manager:
                    await self.state_manager.save_task_state(task_id, task_state)
                
                # Get daily regions with circuit breaker and state tracking
                circuit_state = await self._check_circuit_state("sentinel_api")
                if not circuit_state['is_closed']:
                    task_state['errors']['sentinel_api'].append("Circuit breaker open")
                    if self.state_manager:
                        await self.state_manager.save_task_state(task_id, task_state)
                    raise DataFetchError(
                        f"Circuit breaker open for sentinel_api until {circuit_state['next_attempt']}",
                        is_recoverable=False
                    )
                
                try:
                    # Check if we already have regions from a previous attempt
                    regions = None
                    if task_state.get('cached_regions'):
                        regions = task_state['cached_regions']
                        flow_logger.info("Recovered regions from previous state")
                    else:
                        regions = await self.get_daily_regions(
                            target_time=target_smap_time,
                            ifs_forecast_time=ifs_forecast_time
                        )
                        task_state['cached_regions'] = regions
                        
                    await self._close_circuit("sentinel_api")
                    task_state['last_successful_step'] = 'get_daily_regions'
                    if self.state_manager:
                        await self.state_manager.save_task_state(task_id, task_state)
                        
                except Exception as e:
                    await self._record_failure("sentinel_api")
                    task_state['errors']['get_daily_regions'].append(str(e))
                    task_state['retry_count']['get_daily_regions'] += 1
                    if self.state_manager:
                        await self.state_manager.save_task_state(task_id, task_state)
                    raise DataFetchError(f"Error getting daily regions: {str(e)}")

                if regions:
                    for region in regions:
                        # Skip already processed regions
                        if region['id'] in task_state['processed_regions']:
                            flow_logger.info(f"Skipping already processed region {region['id']}")
                            continue
                            
                        # Skip failed regions that exceeded retry limit
                        if region['id'] in task_state['failed_regions'] and task_state['retry_count'].get(f"region_{region['id']}", 0) >= 3:
                            flow_logger.info(f"Skipping failed region {region['id']} (exceeded retry limit)")
                            continue
                            
                        task_state['current_step'] = f"processing_region_{region['id']}"
                        if self.state_manager:
                            await self.state_manager.save_task_state(task_id, task_state)
                            
                        try:
                            result = await self.process_region(
                                region=region,
                                target_time=target_smap_time,
                                ifs_forecast_time=ifs_forecast_time,
                                current_time=current_time,
                                validator=validator
                            )
                            if result:
                                task_state['processed_regions'].add(region['id'])
                                task_state['last_successful_step'] = f"processed_region_{region['id']}"
                                if self.state_manager:
                                    await self.state_manager.save_task_state(task_id, task_state)
                        except Exception as e:
                            task_state['failed_regions'].add(region['id'])
                            task_state['errors'][f"region_{region['id']}"].append(str(e))
                            task_state['retry_count'][f"region_{region['id']}"] += 1
                            flow_logger.error(f"Error processing region {region['id']}: {str(e)}")
                            if self.state_manager:
                                await self.state_manager.save_task_state(task_id, task_state)
                            continue
                            
                flow_logger.info("Test mode execution complete. Disabling test mode.")
                self.test_mode = False
                task_state['current_step'] = 'completed'
                if self.state_manager:
                    await self.state_manager.save_task_state(task_id, task_state)
                return task_state

            # Normal execution mode
            task_state['current_step'] = 'normal_execution'
            if self.state_manager:
                await self.state_manager.save_task_state(task_id, task_state)
                
            windows = self.get_validator_windows()
            current_window = next(
                (w for w in windows if self.is_in_window(current_time, w)), None
            )

            if not current_window:
                flow_logger.info(f"Not in any preparation or execution window: {current_time}")
                flow_logger.info(f"Next soil task time: {self.get_next_preparation_time(current_time)}")
                task_state['last_successful_step'] = 'window_check'
                if self.state_manager:
                    await self.state_manager.save_task_state(task_id, task_state)
                return task_state

            is_prep = current_window[1] == 30  # If minutes = 30, it's a prep window
            target_smap_time = self.get_smap_time_for_validator(current_time)
            ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)

            if is_prep:
                task_state['current_step'] = 'preparation_window'
                if self.state_manager:
                    await self.state_manager.save_task_state(task_id, task_state)
                    
                # Preparation window with circuit breaker and state tracking
                circuit_state = await self._check_circuit_state("sentinel_api")
                if not circuit_state['is_closed']:
                    task_state['errors']['sentinel_api'].append("Circuit breaker open")
                    if self.state_manager:
                        await self.state_manager.save_task_state(task_id, task_state)
                    raise DataFetchError(
                        f"Circuit breaker open for sentinel_api until {circuit_state['next_attempt']}",
                        is_recoverable=False
                    )
                
                try:
                    # Check if we already have regions from a previous attempt
                    regions = None
                    if task_state.get('cached_regions'):
                        regions = task_state['cached_regions']
                        flow_logger.info("Recovered regions from previous state")
                    else:
                        regions = await self.get_daily_regions(
                            target_time=target_smap_time,
                            ifs_forecast_time=ifs_forecast_time
                        )
                        task_state['cached_regions'] = regions
                        
                    await self._close_circuit("sentinel_api")
                    task_state['last_successful_step'] = 'preparation_complete'
                    if self.state_manager:
                        await self.state_manager.save_task_state(task_id, task_state)
                except Exception as e:
                    await self._record_failure("sentinel_api")
                    task_state['errors']['preparation'].append(str(e))
                    task_state['retry_count']['preparation'] += 1
                    if self.state_manager:
                        await self.state_manager.save_task_state(task_id, task_state)
                    raise DataFetchError(f"Error in preparation window: {str(e)}")
                    
            else:
                task_state['current_step'] = 'execution_window'
                if self.state_manager:
                    await self.state_manager.save_task_state(task_id, task_state)
                    
                # Execution window with state tracking
                try:
                    regions = await self.db_manager.fetch_many(
                        """
                        SELECT * FROM soil_moisture_regions 
                        WHERE status = 'pending'
                        AND target_time = :target_time
                        """,
                        {"target_time": target_smap_time},
                    )
                    
                    if regions:
                        for region in regions:
                            # Skip already processed regions
                            if region['id'] in task_state['processed_regions']:
                                flow_logger.info(f"Skipping already processed region {region['id']}")
                                continue
                                
                            # Skip failed regions that exceeded retry limit
                            if region['id'] in task_state['failed_regions'] and task_state['retry_count'].get(f"region_{region['id']}", 0) >= 3:
                                flow_logger.info(f"Skipping failed region {region['id']} (exceeded retry limit)")
                                continue
                                
                            task_state['current_step'] = f"processing_region_{region['id']}"
                            if self.state_manager:
                                await self.state_manager.save_task_state(task_id, task_state)
                                
                            try:
                                result = await self.process_region(
                                    region=region,
                                    target_time=target_smap_time,
                                    ifs_forecast_time=ifs_forecast_time,
                                    current_time=current_time,
                                    validator=validator
                                )
                                if result:
                                    task_state['processed_regions'].add(region['id'])
                                    task_state['last_successful_step'] = f"processed_region_{region['id']}"
                                    if self.state_manager:
                                        await self.state_manager.save_task_state(task_id, task_state)
                            except Exception as e:
                                task_state['failed_regions'].add(region['id'])
                                task_state['errors'][f"region_{region['id']}"].append(str(e))
                                task_state['retry_count'][f"region_{region['id']}"] += 1
                                flow_logger.error(f"Error processing region {region['id']}: {str(e)}")
                                if self.state_manager:
                                    await self.state_manager.save_task_state(task_id, task_state)
                                continue
                                
                except Exception as e:
                    task_state['errors']['execution'].append(str(e))
                    if self.state_manager:
                        await self.state_manager.save_task_state(task_id, task_state)
                    raise MinerQueryError(f"Error in execution window: {str(e)}")

            # Log final task state
            flow_logger.info(f"Task completed with state: {task_state}")
            task_state['current_step'] = 'completed'
            if self.state_manager:
                await self.state_manager.save_task_state(task_id, task_state)
            return task_state

        except Exception as e:
            task_state['errors']['fatal'].append(str(e))
            task_state['current_step'] = 'failed'
            if self.state_manager:
                await self.state_manager.save_task_state(task_id, task_state)
            flow_logger.error(f"Fatal error in validator execution: {str(e)}")
            flow_logger.error(traceback.format_exc())
            raise

    @task(
        name="get_daily_regions_task",
        retries=3,
        retry_delay_seconds=lambda x: 60 * (2 ** x),
        retry_jitter_factor=0.2,
        tags=["external_service"],
        description="Fetch and prepare daily regions for soil moisture prediction"
    )
    async def get_daily_regions(
        self,
        target_time: datetime,
        ifs_forecast_time: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch and prepare daily regions for soil moisture prediction."""
        circuit_state = await self._check_circuit_state("sentinel_api")
        if not circuit_state['is_closed']:
            raise DataFetchError(
                f"Circuit breaker open for sentinel_api until {circuit_state['next_attempt']}",
                is_recoverable=False
            )
        
        try:
            logger.info(f"Getting daily regions for target time: {target_time}")
            regions = await self.validator_preprocessing.get_daily_regions(
                target_time=target_time,
                ifs_forecast_time=ifs_forecast_time
            )
            
            await self._close_circuit("sentinel_api")
            return regions
            
        except Exception as e:
            await self._record_failure("sentinel_api")
            raise DataFetchError(f"Error getting daily regions: {str(e)}")

    @task(
        name="process_region_task",
        retries=3,
        retry_delay_seconds=lambda x: 30 * (2 ** x),
        retry_jitter_factor=0.1,
        tags=["data_processing"],
        description="Process a single region and prepare it for miners"
    )
    async def process_region(
        self,
        region: Dict[str, Any],
        target_time: datetime,
        ifs_forecast_time: datetime,
        current_time: datetime,
        validator: Any
    ) -> Optional[Dict[str, Any]]:
        """Process a single region and prepare it for miners."""
        try:
            if "combined_data" not in region:
                logger.error(f"Region {region['id']} missing combined_data field")
                return None

            if not region["combined_data"]:
                logger.error(f"Region {region['id']} has null combined_data")
                return None

            combined_data = region["combined_data"]
            if not isinstance(combined_data, bytes):
                logger.error(f"Region {region['id']} has invalid data type: {type(combined_data)}")
                return None

            if not (
                combined_data.startswith(b"II\x2A\x00")
                or combined_data.startswith(b"MM\x00\x2A")
            ):
                logger.error(f"Region {region['id']} has invalid TIFF header")
                logger.error(f"First 16 bytes: {combined_data[:16].hex()}")
                return None

            logger.info(f"Region {region['id']} TIFF size: {len(combined_data) / (1024 * 1024):.2f} MB")
            logger.info(f"Region {region['id']} TIFF header: {combined_data[:4]}")
            logger.info(f"Region {region['id']} TIFF header hex: {combined_data[:16].hex()}")
            
            encoded_data = base64.b64encode(combined_data)
            logger.info(f"Base64 first 16 chars: {encoded_data[:16]}")

            task_data = {
                "region_id": region["id"],
                "combined_data": encoded_data.decode("ascii"),
                "sentinel_bounds": region["sentinel_bounds"],
                "sentinel_crs": region["sentinel_crs"],
                "target_time": target_time.isoformat(),
            }

            payload = {"nonce": str(uuid4()), "data": task_data}

            logger.info(f"Sending region {region['id']} to miners...")
            responses = await validator.query_miners(
                payload=payload, endpoint="/soilmoisture-request"
            )

            if responses:
                metadata = {
                    "region_id": region["id"],
                    "target_time": target_time,
                    "data_collection_time": current_time,
                    "ifs_forecast_time": ifs_forecast_time,
                }
                await self.add_task_to_queue.submit(responses, metadata)

                await self.db_manager.execute(
                    """
                    UPDATE soil_moisture_regions 
                    SET status = 'sent_to_miners'
                    WHERE id = :region_id
                    """,
                    {"region_id": region["id"]},
                )
                
                return {
                    "region_id": region["id"],
                    "responses": responses,
                    "metadata": metadata
                }

        except Exception as e:
            logger.error(f"Error processing region: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    @task(
        name="add_task_to_queue_task",
        retries=3,
        retry_delay_seconds=60,
        tags=["database"],
        description="Add processed miner responses to the task queue"
    )
    async def add_task_to_queue(
        self,
        responses: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> None:
        """Add processed miner responses to the task queue."""
        try:
            if not self.db_manager:
                raise RuntimeError("Database manager not initialized")

            region_update = """
            UPDATE soil_moisture_regions 
            SET status = 'sent_to_miners' 
            WHERE id = :region_id
            """
            await self.db_manager.execute(region_update, {"region_id": metadata["region_id"]})

            for miner_hotkey, response_data in responses.items():
                try:
                    # Get miner UID from hotkey
                    query = "SELECT uid FROM node_table WHERE hotkey = :miner_hotkey"
                    result = await self.db_manager.fetch_one(
                        query, {"miner_hotkey": miner_hotkey}
                    )

                    if not result:
                        logger.warning(f"No UID found for hotkey {miner_hotkey}")
                        continue

                    miner_uid = str(result["uid"])

                    if isinstance(response_data, dict) and "text" in response_data:
                        try:
                            response_data = json.loads(response_data["text"])
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse response text for {miner_hotkey}: {e}")
                            continue

                    prediction_data = {
                        "region_id": metadata["region_id"],
                        "miner_uid": miner_uid,
                        "miner_hotkey": miner_hotkey,
                        "target_time": metadata["target_time"],
                        "surface_sm": response_data.get("surface_sm", []),
                        "rootzone_sm": response_data.get("rootzone_sm", []),
                        "uncertainty_surface": response_data.get("uncertainty_surface"),
                        "uncertainty_rootzone": response_data.get("uncertainty_rootzone"),
                        "sentinel_bounds": response_data.get("sentinel_bounds", metadata.get("sentinel_bounds")),
                        "sentinel_crs": response_data.get("sentinel_crs", metadata.get("sentinel_crs")),
                        "status": "sent_to_miner",
                    }

                    await self.db_manager.execute(
                        """
                        INSERT INTO soil_moisture_predictions 
                        (region_id, miner_uid, miner_hotkey, target_time, surface_sm, rootzone_sm, 
                        uncertainty_surface, uncertainty_rootzone, sentinel_bounds, 
                        sentinel_crs, status)
                        VALUES 
                        (:region_id, :miner_uid, :miner_hotkey, :target_time, 
                        :surface_sm, :rootzone_sm,
                        :uncertainty_surface, :uncertainty_rootzone, :sentinel_bounds,
                        :sentinel_crs, :status)
                        """,
                        prediction_data,
                    )

                except Exception as e:
                    logger.error(f"Error processing response from {miner_hotkey}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error in add_task_to_queue: {e}")
            logger.error(traceback.format_exc())
            raise
