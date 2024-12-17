import os
import asyncio
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Union, Type
import numpy as np
import pandas as pd
from uuid import uuid4
import json
from pydantic import Field
import os
import importlib.util
from fiber.logging_utils import get_logger
from gaia.validator.database.database_manager import ValidatorDatabaseManager
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.tasks.base.task import Task
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_metadata import GeomagneticMetadata
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_inputs import GeomagneticInputs
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_outputs import GeomagneticOutputs
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_scoring_mechanism import GeomagneticScoringMechanism
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import GeomagneticPreprocessing
from gaia.models.geomag_basemodel import GeoMagBaseModel
from gaia.tasks.defined_tasks.geomagnetic.utils.process_geomag_data import get_latest_geomag_data
from prefect import flow, get_run_logger, task
from gaia.tasks.defined_tasks.geomagnetic.validation import (
    validate_geomag_data,
    validate_historical_data,
    validate_miner_query,
    validate_miner_response,
    validate_scored_prediction,
    validate_batch_predictions,
    validate_batch_scores
)
import torch


logger = get_logger(__name__)


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


class GeomagneticModelManager:
    """Manages model loading, validation, and state persistence for geomagnetic models."""
    
    def __init__(self, cache_dir: Optional[str] = None):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.cache_dir = cache_dir or "tasks/defined_tasks/geomagnetic/"
        self.model: Optional[Any] = None
        self.model_type: str = "base"
        
    def validate_custom_model(self, model_class: Type) -> bool:
        """Validate that custom model implements required interface."""
        try:
            required_methods = ['run_inference']
            
            for method in required_methods:
                if not hasattr(model_class, method):
                    logger.error(f"Custom model missing required method: {method}")
                    return False
                    
            # Validate initialization
            model_instance = model_class()
            if not hasattr(model_instance, '_load_model'):
                logger.warning("Custom model missing recommended _load_model method")
                
            return True
            
        except Exception as e:
            logger.error(f"Error validating custom model: {str(e)}")
            return False
            
    def load_base_model(self) -> Optional[GeoMagBaseModel]:
        """Load the base GeoMagBaseModel."""
        try:
            model = GeoMagBaseModel()
            model.to(self.device)
            model.eval()
            
            param_count = sum(p.numel() for p in model.parameters())
            logger.info(f"Base model loaded successfully with {param_count:,} parameters")
            
            return model
            
        except Exception as e:
            logger.error(f"Error loading base model: {str(e)}")
            logger.error(traceback.format_exc())
            return None
            
    def load_custom_model(self) -> Optional[Any]:
        """Load custom model if available."""
        try:
            custom_model_path = "gaia/models/custom_models/custom_geomagnetic_model.py"
            
            if not os.path.exists(custom_model_path):
                logger.info("No custom model found")
                return None
                
            spec = importlib.util.spec_from_file_location(
                "custom_geomagnetic_model", 
                custom_model_path
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            if not hasattr(module, 'CustomGeomagneticModel'):
                logger.error("Custom model file missing CustomGeomagneticModel class")
                return None
                
            model_class = getattr(module, 'CustomGeomagneticModel')
            
            if not self.validate_custom_model(model_class):
                logger.error("Custom model validation failed")
                return None
                
            model = model_class()
            logger.info("Custom model loaded successfully")
            return model
            
        except Exception as e:
            logger.error(f"Error loading custom model: {str(e)}")
            logger.error(traceback.format_exc())
            return None
            
    def initialize_model(self) -> None:
        """Initialize either custom or base model."""
        self.model = self.load_custom_model()
        
        if self.model is not None:
            self.model_type = "custom"
            logger.info("Using custom geomagnetic model")
        else:
            self.model = self.load_base_model()
            if self.model is not None:
                self.model_type = "base"
                logger.info("Using base geomagnetic model")
            else:
                raise RuntimeError("Failed to load either custom or base model")
                
    def get_model(self) -> Any:
        """Get the current model instance."""
        if self.model is None:
            self.initialize_model()
        return self.model
        
    def get_model_type(self) -> str:
        """Get the type of currently loaded model."""
        return self.model_type


class GeomagneticTask(Task):
    """
    A task class for processing and analyzing geomagnetic data, with
    execution methods for both miner and validator workflows.

    This task involves:
        - Querying miners for predictions
        - Adding predictions to a queue for scoring
        - Fetching ground truth data
        - Scoring predictions
        - Moving scored tasks to history
    """

    # Declare Pydantic fields
    db_manager: ValidatorDatabaseManager = Field(
        default_factory=ValidatorDatabaseManager,
        description="Database manager for the task",
    )
    miner_preprocessing: GeomagneticPreprocessing = Field(
        default_factory=GeomagneticPreprocessing,
        description="Preprocessing component for miner",
    )
    model_manager: Optional[GeomagneticModelManager] = None
    model: Optional[Any] = None

    # Set task-specific schedule
    default_schedule = "0 * * * *"  # Run at the start of every hour

    def __init__(self, db_manager=None, **data):
        super().__init__(
            name="GeomagneticTask",
            description="Geomagnetic prediction task",
            task_type="atomic",
            metadata=GeomagneticMetadata(),
            inputs=GeomagneticInputs(),
            outputs=GeomagneticOutputs(),
            scoring_mechanism=GeomagneticScoringMechanism(),
            **data,
        )
        if db_manager:
            self.db_manager = db_manager

        # Initialize model manager and load model
        try:
            self.model_manager = GeomagneticModelManager()
            self.model_manager.initialize_model()
            self.model = self.model_manager.get_model()
            logger.info(f"Initialized {self.model_manager.get_model_type()} geomagnetic model")
        except Exception as e:
            logger.error(f"Error initializing model: {str(e)}")
            logger.error(traceback.format_exc())
            raise RuntimeError(f"Failed to initialize model: {str(e)}")

        # Initialize circuit breakers for critical services
        self.circuit_breakers = {
            'geomag_api': CircuitBreakerState(),
            'miner_query': CircuitBreakerState(failure_threshold=5),
            'database': CircuitBreakerState(recovery_timeout=600)
        }

        # Log whether the fallback model is being used
        if self.model.is_fallback:
            logger.warning("Using fallback GeoMag model for predictions.")
        else:
            logger.info("Using Hugging Face GeoMag model for predictions.")

    @task(
        name="fetch_geomag_data",
        retries=3,
        retry_delay_seconds=lambda x: 60 * (2 ** x),
        retry_jitter_factor=0.2,
        tags=["external_service"],
        description="Fetch latest geomagnetic data with historical records"
    )
    async def fetch_geomag_data(self):
        """Fetch latest geomagnetic data with historical records."""
        circuit_state = await self._check_circuit_state("geomag_api")
        if not circuit_state['is_closed']:
            raise DataFetchError(
                f"Circuit breaker open for geomag_api until {circuit_state['next_attempt']}",
                is_recoverable=False
            )
        
        try:
            logger.info("Fetching latest geomagnetic data...")
            timestamp, dst_value, historical_data = await get_latest_geomag_data(
                include_historical=True
            )
            
            if timestamp == "N/A" or dst_value == "N/A":
                raise DataFetchError("Invalid data received from geomag API")
            
            # Validate data before proceeding
            try:
                validated_data = validate_geomag_data({
                    'timestamp': timestamp,
                    'dst_value': dst_value,
                    'historical_data': historical_data
                })
            except ValueError as ve:
                raise DataFetchError(f"Data validation failed: {str(ve)}")
            
            await self._close_circuit("geomag_api")
            
            logger.info(
                f"Fetched latest geomagnetic data: timestamp={validated_data.timestamp}, value={validated_data.dst_value}"
            )
            if validated_data.historical_data is not None:
                logger.info(
                    f"Fetched historical data for the current month: {len(validated_data.historical_data)} records"
                )
            else:
                logger.warning("No historical data available for the current month.")
            
            return {
                'timestamp': validated_data.timestamp,
                'dst_value': validated_data.dst_value,
                'historical_data': validated_data.historical_data
            }
        except DataFetchError:
            raise
        except Exception as e:
            await self._record_failure("geomag_api")
            raise DataFetchError(f"Error fetching geomagnetic data: {str(e)}")

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

    @task(
        name="query_miners",
        retries=5,
        retry_delay_seconds=lambda x: 30 * (2 ** x),
        retry_jitter_factor=0.1,
        tags=["miner_service"],
        description="Query miners with current data and process responses"
    )
    async def query_miners(
        self,
        timestamp: datetime,
        dst_value: float,
        historical_data: pd.DataFrame,
        current_hour_start: datetime,
        validator: Any
    ) -> Dict[str, Any]:
        """Query miners with current data and process responses."""
        circuit_state = await self._check_circuit_state("miner_query")
        if not circuit_state['is_closed']:
            raise MinerQueryError(
                f"Circuit breaker open for miner_query until {circuit_state['next_attempt']}",
                is_recoverable=False
            )
        
        try:
            # Construct and validate payload
            payload_template = {
                "nonce": str(uuid4()),
                "data": {
                    "name": "Geomagnetic Data",
                    "timestamp": timestamp.isoformat(),
                    "value": dst_value,
                    "historical_values": [
                        {"timestamp": row["timestamp"].isoformat(), "Dst": row["Dst"]}
                        for _, row in (historical_data.iterrows() if historical_data is not None else [])
                    ],
                },
            }
            
            try:
                # Validate query payload
                validated_query = validate_miner_query(payload_template)
            except ValueError as ve:
                raise MinerQueryError(f"Invalid query payload: {str(ve)}")
            
            logger.info(f"Querying miners for geomagnetic predictions")
            
            # Validate validator connection
            if not validator or not hasattr(validator, 'query_miners'):
                raise MinerQueryError(
                    "Invalid validator instance provided",
                    is_recoverable=False
                )
            
            responses = await validator.query_miners(validated_query.dict(), "/geomagnetic-request")
            logger.info(f"Collected responses from miners: {len(responses)}")

            if not responses:
                logger.warning("No responses received from miners")
                return {}

            # Process and validate responses
            try:
                processed_responses = await self.process_miner_responses.submit(
                    responses=responses,
                    current_hour_start=current_hour_start,
                    validator=validator
                )
                
                # Validate processed responses
                validated_responses = validate_batch_predictions(processed_responses)
                logger.info(f"Validated {len(validated_responses)} miner responses")
                
                await self._close_circuit("miner_query")
                return [response.dict() for response in validated_responses]
                
            except ValueError as ve:
                raise MinerQueryError(f"Error processing miner responses: {str(ve)}")
                
        except MinerQueryError:
            raise
        except Exception as e:
            await self._record_failure("miner_query")
            raise MinerQueryError(f"Error querying miners: {str(e)}")

    async def process_responses(self, responses, context):
        """Process and validate miner responses."""
        processed_responses = []
        current_hour_start = context['current_time'].replace(
            minute=0, second=0, microsecond=0
        )

        for hotkey, response_data in responses.items():
            try:
                # Parse response
                if isinstance(response_data, dict) and "text" in response_data:
                    response = json.loads(response_data["text"])
                else:
                    continue

                # Get miner UID
                query = "SELECT uid FROM node_table WHERE hotkey = :miner_hotkey"
                result = await self.db_manager.fetch_one(
                    query, {"miner_hotkey": hotkey}
                )
                if not result:
                    continue

                miner_uid = result["uid"]
                predicted_value = float(response.get("predicted_values"))

                # Add to queue
                await self.add_prediction_to_queue(
                    miner_uid=str(miner_uid),
                    miner_hotkey=hotkey,
                    predicted_value=predicted_value,
                    query_time=current_hour_start,
                    status="pending"
                )

                processed_responses.append({
                    'miner_uid': miner_uid,
                    'miner_hotkey': hotkey,
                    'predicted_value': predicted_value,
                    'query_time': current_hour_start
                })

            except Exception as e:
                logger.error(f"Error processing response from {hotkey}: {e}")
                continue

        return processed_responses

    async def score_responses(self, responses, context):
        """Score the processed responses."""
        # Fetch ground truth
        ground_truth_value = await self.fetch_ground_truth()
        if ground_truth_value is None:
            logger.warning("Ground truth data not available. Skipping scoring.")
            return []

        # Get validator from context
        validator = context['validator']
        current_time = context['current_time']

        scored_responses = []
        for response in responses:
            try:
                score = self.scoring_mechanism.calculate_score(
                    response['predicted_value'],
                    ground_truth_value
                )
                
                # Move to history
                await self.move_task_to_history(
                    response,
                    ground_truth_value,
                    score,
                    current_time
                )
                
                response['score'] = score
                scored_responses.append(response)
                
            except Exception as e:
                logger.error(f"Error scoring response: {e}")
                continue

        return scored_responses

    async def update_scores(self, scores, context):
        """Update the scoring system with new scores."""
        current_hour = context['current_time'].replace(
            minute=0, second=0, microsecond=0
        )
        await self.build_score_row(current_hour, scores)

    async def validator_prepare_subtasks(self):
        """No subtask preparation needed for atomic task."""
        return None

    async def validator_execute(self, validator):
        """Execute the geomagnetic task."""
        try:
            # Step 1: Align to the top of the next hour
            current_time = datetime.now(timezone.utc)
            next_hour = current_time.replace(
                minute=0, second=0, microsecond=0
            ) + timedelta(hours=1)

            logger.info("Starting GeomagneticTask execution...")

            # Step 2: Fetch Latest Geomagnetic Data
            timestamp, dst_value, historical_data = await self._fetch_geomag_data()

            # Step 3: Query Miners
            current_hour_start = next_hour - timedelta(hours=1)
            await self._query_miners(
                validator, timestamp, dst_value, historical_data, current_hour_start
            )

            # Step 4: Process Scores
            await self._process_scores(validator, current_hour_start, next_hour)

            return {
                'timestamp': timestamp,
                'dst_value': dst_value,
                'current_hour_start': current_hour_start,
                'next_hour': next_hour
            }

        except Exception as e:
            logger.error(f"Error in validator_execute: {e}")
            logger.error(traceback.format_exc())
            raise

    async def validator_score(self, result=None, context=None):
        """Score task results."""
        if result is None or context is None:
            return
            
        try:
            # Fetch Ground Truth
            ground_truth_value = await self.fetch_ground_truth()
            if ground_truth_value is None:
                logger.warning("Ground truth data not available. Skipping scoring.")
                return

            # Score Predictions and Archive Results
            last_hour_start = result['current_hour_start']
            last_hour_end = result['next_hour']
            current_time = datetime.now(timezone.utc)

            logger.info(
                f"Fetching predictions between {last_hour_start} and {last_hour_end}"
            )
            tasks = await self.get_tasks_for_hour(last_hour_start, last_hour_end, context['validator'])
            await self.score_tasks(tasks, ground_truth_value, current_time)

        except Exception as e:
            logger.error(f"Error in validator_score: {e}")
            logger.error(traceback.format_exc())
            raise

    def miner_preprocess(self, raw_data):
        """
        Preprocess raw geomagnetic data on the miner's side.

        Args:
            raw_data (dict): Raw data received by the miner.
        Returns:
            dict: Preprocessed data ready for prediction.
        """
        try:
            processed_data = {
                "timestamp": raw_data["timestamp"],
                "value": raw_data["value"] / 100.0,  # Normalize values
            }
            return processed_data
        except Exception as e:
            logger.error(f"Error in miner_preprocess: {e}")
            return None

    def validator_score(self, prediction, ground_truth):
        """
        Score a miner's prediction against the ground truth.

        Args:
            prediction (float): The predicted value.
            ground_truth (float): The actual ground truth value.
        Returns:
            float: A score indicating the accuracy of the prediction.
        """
        try:
            score = abs(prediction - ground_truth)
            return score
        except Exception as e:
            print(f"Error in validator_score: {e}")
            return float("inf")

    ############################################################
    # Validator execution method
    ############################################################

    async def _fetch_geomag_data(self):
        """Fetch latest geomagnetic data."""
        logger.info("Fetching latest geomagnetic data...")
        timestamp, dst_value, historical_data = await get_latest_geomag_data(
            include_historical=True
        )
        logger.info(
            f"Fetched latest geomagnetic data: timestamp={timestamp}, value={dst_value}"
        )
        if historical_data is not None:
            logger.info(
                f"Fetched historical data for the current month: {len(historical_data)} records"
            )
        else:
            logger.warning("No historical data available for the current month.")
        return timestamp, dst_value, historical_data

    async def _query_miners(
        self, validator, timestamp, dst_value, historical_data, current_hour_start
    ):
        """Query miners with current data and process responses."""
        # Construct Payload for Miners
        nonce = str(uuid4())

        # Convert historical data to serializable format
        historical_records = []
        if historical_data is not None:
            for _, row in historical_data.iterrows():
                historical_records.append(
                    {"timestamp": row["timestamp"].isoformat(), "Dst": row["Dst"]}
                )

        payload_template = {
            "nonce": nonce,
            "data": {
                "name": "Geomagnetic Data",
                "timestamp": timestamp.isoformat(),
                "value": dst_value,
                "historical_values": historical_records,
            },
        }
        endpoint = "/geomagnetic-request"

        logger.info(f"Querying miners for geomagnetic predictions")
        responses = await validator.query_miners(payload_template, endpoint)
        logger.info(f"Collected responses from miners: {len(responses)}")

        await self.process_miner_responses(responses, current_hour_start, validator)
        logger.info(f"Added {len(responses)} predictions to the database")

    async def _process_scores(self, validator, current_hour_start, next_hour):
        """Process and archive scores for the previous hour."""
        # Fetch Ground Truth
        ground_truth_value = await self.fetch_ground_truth()
        if ground_truth_value is None:
            logger.warning("Ground truth data not available. Skipping scoring.")
            return

        # Score Predictions and Archive Results
        last_hour_start = current_hour_start
        last_hour_end = next_hour.replace(minute=0, second=0, microsecond=0)
        current_time = datetime.now(timezone.utc)  # Add this line

        logger.info(
            f"Fetching predictions between {last_hour_start} and {last_hour_end}"
        )
        tasks = await self.get_tasks_for_hour(last_hour_start, last_hour_end, validator)
        await self.score_tasks(tasks, ground_truth_value, current_time)

    async def get_tasks_for_hour(self, start_time, end_time, validator=None):
        """
        Fetches tasks submitted within a specific UTC time range from the database.
        Only returns the most recent task per miner.

        Args:
            start_time (datetime): Start of the time range (inclusive).
            end_time (datetime): End of the time range (exclusive).
            validator (optional): Validator instance containing metagraph.

        Returns:
            list: List of task dictionaries containing task details.
        """
        try:
            # Query the database for most recent tasks per miner in the time range
            query = """
                WITH RankedTasks AS (
                    SELECT 
                        id,
                        miner_uid,
                        miner_hotkey,
                        predicted_value,
                        query_time,
                        ROW_NUMBER() OVER (
                            PARTITION BY miner_uid 
                            ORDER BY query_time DESC
                        ) as rn
                    FROM geomagnetic_predictions
                    WHERE query_time >= :start_time 
                    AND query_time < :end_time 
                    AND status = 'pending'
                )
                SELECT 
                    id,
                    miner_uid,
                    miner_hotkey,
                    predicted_value,
                    query_time
                FROM RankedTasks
                WHERE rn = 1;
            """
            # Convert timestamps to UTC if they aren't already
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)

            logger.info(f"Querying tasks with:")
            logger.info(f"  start_time: {start_time} (tzinfo: {start_time.tzinfo})")
            logger.info(f"  end_time: {end_time} (tzinfo: {end_time.tzinfo})")

            params = {"start_time": start_time, "end_time": end_time}

            # Log the query parameters for debugging
            logger.info(f"Querying with start_time: {start_time}, end_time: {end_time}")

            results = await self.db_manager.fetch_many(query, params)

            # Log raw results for debugging
            logger.info(f"Raw query results: {results}")

            # Convert results to a list of task dictionaries
            tasks = []
            for row in results:
                task = {
                    "id": row["id"],
                    "miner_uid": row["miner_uid"],
                    "miner_hotkey": row["miner_hotkey"],
                    "predicted_values": row["predicted_value"],
                    "query_time": row["query_time"],
                }
                tasks.append(task)

            logger.info(
                f"Fetched {len(tasks)} tasks between {start_time} and {end_time}"
            )

            # task validation - ensure that miner_hotkey is in the metagraph if validator is provided
            if validator:
                tasks = [
                    task
                    for task in tasks
                    if task["miner_hotkey"] in validator.metagraph.nodes
                ]

            return tasks

        except Exception as e:
            logger.error(f"Error fetching tasks for hour: {e}")
            logger.error(f"{traceback.format_exc()}")
            return []

    async def fetch_ground_truth(self):
        """
        Fetches the ground truth DST value for the current UTC hour.

        Returns:
            int: The real-time DST value, or None if fetching fails.
        """
        try:
            # Get the current UTC time
            current_time = datetime.now(timezone.utc)
            logger.info(f"Fetching ground truth for UTC hour: {current_time.hour}")

            # Fetch the most recent geomagnetic data
            timestamp, dst_value = await get_latest_geomag_data(
                include_historical=False
            )

            if timestamp == "N/A" or dst_value == "N/A":
                logger.warning("No ground truth data available for the current hour.")
                return None

            logger.info(f"Ground truth value for hour {current_time.hour}: {dst_value}")
            return dst_value

        except Exception as e:
            logger.error(f"Error fetching ground truth: {e}")
            logger.error(f"{traceback.format_exc()}")
            return None

    async def move_task_to_history(
        self, task: dict, ground_truth_value: float, score: float, score_time: datetime
    ):
        """
        Archives a completed task in the history table.

        Args:
            task (dict): Task details including predicted_values and query_time
            ground_truth_value (float): The actual observed value
            score (float): The calculated score
            score_time (datetime): When the task was scored
        """
        try:
            # Insert into history table
            query = """
                INSERT INTO geomagnetic_history 
                (miner_uid, miner_hotkey, query_time, predicted_value, ground_truth_value, score, scored_at)
                VALUES (:miner_uid, :miner_hotkey, :query_time, :predicted_value, :ground_truth_value, :score, :scored_at)
            """

            params = {
                "miner_uid": task["miner_uid"],
                "miner_hotkey": task["miner_hotkey"],
                "query_time": task["query_time"],
                "predicted_value": task["predicted_values"],
                "ground_truth_value": ground_truth_value,
                "score": score,
                "scored_at": score_time,
            }

            await self.db_manager.execute(query, params)
            logger.info(f"Archived task to history: {task['id']}")

            # Remove from predictions table
            delete_query = """
                DELETE FROM geomagnetic_predictions 
                WHERE id = :task_id
            """
            await self.db_manager.execute(delete_query, {"task_id": task["id"]})
            logger.info(f"Removed task from predictions: {task['id']}")

        except Exception as e:
            logger.error(f"Error moving task to history: {e}")
            logger.error(traceback.format_exc())
            raise

    ############################################################
    # Miner execution method
    ############################################################

    def run_model_inference(self, processed_data):
        """
        Run the GeoMag model inference.

        Args:
            processed_data (pd.DataFrame): Preprocessed input data for the model.

        Returns:
            float: Predicted value.
        """
        try:
            # Perform prediction using the model
            prediction = self.model.predict(processed_data)

            # Handle NaN or infinite values
            if np.isnan(prediction) or np.isinf(prediction):
                logger.warning("Model returned NaN/Inf, using fallback value")
                return float(
                    processed_data["value"].iloc[-1]
                )  # Use input value as fallback

            return float(prediction)  # Ensure we return a Python float

        except Exception as e:
            logger.error(f"Error during model inference: {e}")
            return float(
                processed_data["value"].iloc[-1]
            )  # Return input value as fallback

    def miner_execute(self, data, miner):
        """
        Executes the miner workflow:
        - Preprocesses the received data along with historical data.
        - Dynamically determines whether to use a custom or base model for inference.
        - Returns formatted predictions.

        Args:
            data: Raw input data received from the request.
            miner: Miner instance executing the task.

        Returns:
            dict: Prediction results formatted as per requirements.
        """
        try:
            # Process input data
            processed_data = self.miner_preprocess(data["data"])
            if processed_data is None:
                raise ValueError("Failed to preprocess input data")

            # Run model inference with proper error handling
            try:
                if self.model_manager.get_model_type() == "custom":
                    logger.info("Using custom geomagnetic model for inference")
                    predictions = self.model.run_inference(processed_data)
                else:
                    logger.info("Using base geomagnetic model for inference")
                    raw_prediction = self.run_model_inference(processed_data)
                    predictions = {
                        "predicted_value": float(raw_prediction),
                        "prediction_time": data["data"]["timestamp"]
                    }

                # Validate predictions
                if not isinstance(predictions.get("predicted_value"), (int, float)):
                    raise ValueError("Model did not return a valid numeric prediction")

                # Format response
                return {
                    "predicted_values": float(predictions.get("predicted_value", 0.0)),
                    "timestamp": predictions.get("prediction_time", data["data"]["timestamp"]),
                    "miner_hotkey": miner.keypair.ss58_address,
                }

            except Exception as e:
                logger.error(f"Model inference failed: {str(e)}")
                logger.error(traceback.format_exc())
                raise

        except Exception as e:
            logger.error(f"Error in miner execution: {str(e)}")
            logger.error(traceback.format_exc())
            # Return safe fallback response
            current_time = datetime.now(timezone.utc)
            return {
                "predicted_values": "N/A",
                "timestamp": current_time.isoformat(),
                "miner_hotkey": miner.keypair.ss58_address,
            }

    def query_miners(self):
        """
        Simulates querying miners and collecting predictions.

        Returns:
            dict: Simulated predictions and metadata.
        """
        try:
            # Simulate prediction values
            predictions = np.random.randint(-100, 100, size=256)  # Example predictions
            return {"predictions": predictions}
        except Exception as e:
            print(f"Error querying miners: {str(e)}")
            return None

    def add_task_to_queue(self, predictions, query_time):
        """
        Adds a new task to the task queue.

        Args:
            predictions (np.ndarray or None): Array of predictions from miners.
            query_time (datetime): The time the task was added.
        """
        try:
            # Use MinerDatabaseManager to insert task into the database
            db_manager = MinerDatabaseManager()
            task_name = "geomagnetic_prediction"
            miner_id = (
                "example_miner_id"  # Replace with the actual miner ID if available
            )

            # Validate predictions
            if predictions is None:
                logger.warning("Received None predictions, skipping queue addition")
                return

            # Convert predictions to a dictionary or JSON-like structure
            if isinstance(predictions, np.ndarray):
                predicted_value = {"predictions": predictions.tolist()}
            else:
                predicted_value = {"predictions": predictions}

            # Add to the queue
            asyncio.run(
                db_manager.add_to_queue(
                    task_name=task_name,
                    miner_id=miner_id,
                    predicted_value=predicted_value,
                    query_time=query_time,
                )
            )
            logger.info(f"Task added to queue: {task_name} at {query_time}")

        except Exception as e:
            logger.error(f"Error adding task to queue: {e}")

    async def add_prediction_to_queue(
        self,
        miner_uid: str,
        miner_hotkey: str,
        predicted_value: float,
        query_time: datetime,
        status: str = "pending",
    ) -> None:
        """
        Add a prediction to the geomagnetic_predictions table.

        Args:
            miner_id (str): ID of the miner submitting the prediction
            predicted_value (float): The predicted DST value
            query_time (datetime): Timestamp for when the prediction was made
            status (str, optional): Current status of the prediction. Defaults to "pending"
        """
        try:
            # Initialize the database manager
            db_manager = ValidatorDatabaseManager()

            # Construct the query based on schema.json
            query = """
                INSERT INTO geomagnetic_predictions 
                (id, miner_uid, miner_hotkey, predicted_value, query_time, status)
                VALUES (:id, :miner_uid, :miner_hotkey, :predicted_value, :query_time, :status)
            """

            # Prepare parameters
            params = {
                "id": str(uuid4()),
                "miner_uid": miner_uid,
                "miner_hotkey": miner_hotkey,
                "predicted_value": float(predicted_value),  # Ensure float type
                "query_time": query_time,
                "status": status,
            }

            logger.info(f"Adding prediction to queue with params: {params}")

            # Execute the query
            await db_manager.execute(query, params)
            logger.info(f"Added prediction from miner {miner_uid} to queue")

        except Exception as e:
            logger.error(f"Error adding prediction to queue: {e}")
            logger.error(f"{traceback.format_exc()}")
            raise

    @task(
        name="process_miner_responses",
        retries=3,
        retry_delay_seconds=60,
        description="Process and validate miner responses"
    )
    async def process_miner_responses(
        self,
        responses: Dict[str, Any],
        current_hour_start: datetime,
        validator: Any
    ) -> List[Dict[str, Any]]:
        """Process and validate miner responses."""
        logger.info(
            f"Processing responses with current_hour_start: {current_hour_start} (tzinfo: {current_hour_start.tzinfo})"
        )

        processed_responses = []
        for hotkey, response_data in responses.items():
            try:
                logger.info(f"Processing response for hotkey {hotkey}: {response_data}")

                # Handle response object with 'text' field
                if isinstance(response_data, dict) and "text" in response_data:
                    try:
                        response = json.loads(response_data["text"])
                        logger.debug(f"Successfully parsed response: {response}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse response text for {hotkey}: {e}")
                        continue
                else:
                    logger.warning(
                        f"Invalid response format for {hotkey}: {response_data}"
                    )
                    continue

                # Extract values from response
                predicted_value = float(
                    response.get("predicted_values")
                )  # Ensure numeric
                miner_hotkey = hotkey  # Use the key from responses dict

                # Get miner UID from hotkey
                miner_uid = None
                query = "SELECT uid FROM node_table WHERE hotkey = :miner_hotkey"
                result = await self.db_manager.fetch_one(
                    query, {"miner_hotkey": miner_hotkey}
                )
                if result:
                    miner_uid = result["uid"]
                    logger.info(
                        f"Found miner UID {miner_uid} for hotkey {miner_hotkey}"
                    )
                else:
                    logger.warning(f"No UID found for hotkey {miner_hotkey}")
                    continue

                # Validate response
                if predicted_value is None:
                    logger.warning(f"Missing predicted value in response: {response}")
                    continue

                # Add to queue with proper timestamp handling
                logger.info(
                    f"Adding prediction to queue for {miner_hotkey} with value {predicted_value}"
                )
                await self.add_prediction_to_queue.submit(
                    miner_uid=str(miner_uid),
                    miner_hotkey=miner_hotkey,
                    predicted_value=predicted_value,
                    query_time=current_hour_start,
                    status="pending"
                )

                processed_responses.append({
                    'miner_uid': miner_uid,
                    'miner_hotkey': miner_hotkey,
                    'predicted_value': predicted_value,
                    'query_time': current_hour_start
                })

            except Exception as e:
                logger.error(f"Error processing miner response for {hotkey}: {e}")
                logger.error(f"{traceback.format_exc()}")
                continue

        return processed_responses

    @task(
        name="score_tasks",
        retries=3,
        retry_delay_seconds=60,
        description="Score tasks and archive results"
    )
    async def score_tasks(
        self,
        tasks: List[Dict[str, Any]],
        ground_truth_value: float,
        current_time: datetime
    ) -> List[Dict[str, Any]]:
        """Score tasks and archive results."""
        if not tasks:
            logger.info("No tasks to score")
            return []
        
        try:
            # Validate ground truth
            validated_ground_truth = await self.validate_ground_truth.submit(ground_truth_value)
            if validated_ground_truth is None:
                raise ScoringError("Invalid ground truth value", is_recoverable=False)
            
            scored_tasks = []
            failed_tasks = []
            
            for task in tasks:
                try:
                    predicted_value = task["predicted_values"]
                    score = self.scoring_mechanism.calculate_score(
                        predicted_value,
                        validated_ground_truth
                    )
                    
                    # Validate scored prediction
                    try:
                        validated_score = validate_scored_prediction({
                            'prediction': {
                                'miner_uid': task['miner_uid'],
                                'miner_hotkey': task['miner_hotkey'],
                                'predicted_value': predicted_value,
                                'query_time': task['query_time']
                            },
                            'ground_truth': validated_ground_truth,
                            'score': score,
                            'score_time': current_time
                        })
                    except ValueError as ve:
                        logger.error(f"Score validation failed for task {task['id']}: {str(ve)}")
                        failed_tasks.append(task)
                        continue
                    
                    # Move to history
                    try:
                        await self.move_task_to_history.submit(
                            task=task,
                            ground_truth_value=validated_score.ground_truth,
                            score=validated_score.score,
                            score_time=validated_score.score_time
                        )
                    except Exception as e:
                        logger.error(f"Failed to archive task {task['id']}: {str(e)}")
                        failed_tasks.append(task)
                        continue
                    
                    task["score"] = validated_score.score
                    scored_tasks.append(task)
                    logger.info(
                        f"Task scored and archived: task_id={task['id']}, score={validated_score.score}"
                    )
                    
                except Exception as e:
                    logger.error(f"Error scoring task {task['id']}: {str(e)}")
                    failed_tasks.append(task)
                    
            if failed_tasks:
                # Queue failed tasks for retry
                await self.queue_failed_tasks.submit(failed_tasks)
                
            return scored_tasks
            
        except ScoringError:
            raise
        except Exception as e:
            raise ScoringError(f"Error in scoring process: {str(e)}")

    @task(
        name="build_score_row",
        retries=3,
        retry_delay_seconds=60,
        description="Build and store score row for the current hour"
    )
    async def build_score_row(
        self,
        current_hour: Union[int, datetime],
        recent_tasks: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Build and store score row for the current hour."""
        try:
            # Convert current_hour to datetime if it's an integer
            if isinstance(current_hour, int):
                current_time = datetime.now(timezone.utc)
                current_datetime = current_time.replace(
                    hour=current_hour, minute=0, second=0, microsecond=0
                )
                previous_datetime = current_datetime - timedelta(hours=1)
            else:
                current_datetime = current_hour
                previous_datetime = current_datetime - timedelta(hours=1)

            # Initialize scores array with NaN values
            scores = np.full(24, np.nan)

            # Step 1: Get scores from recent tasks if provided
            if recent_tasks:
                for task in recent_tasks:
                    if "score" in task:
                        scores[current_hour] = task["score"]

            # Step 2: Set up time window (3 days)
            current_time = datetime.now(timezone.utc)
            history_window = current_time - timedelta(days=3)

            # Delete existing score rows for the period
            await self.db_manager.execute(
                """
                DELETE FROM geomagnetic_scores
                WHERE timestamp >= :history_window
                """,
                {"history_window": history_window},
            )

            # Step 3: Get historical scores
            historical_scores = await self.db_manager.fetch_all(
                """
                SELECT timestamp, score
                FROM geomagnetic_history
                WHERE timestamp >= :history_window
                ORDER BY timestamp DESC
                """,
                {"history_window": history_window},
            )

            # Step 4: Process historical scores
            for record in historical_scores:
                hour = record["timestamp"].hour
                scores[hour] = record["score"]

            # Step 5: Calculate statistics
            valid_scores = scores[~np.isnan(scores)]
            if len(valid_scores) > 0:
                mean_score = float(np.mean(valid_scores))
                std_score = float(np.std(valid_scores))
            else:
                mean_score = np.nan
                std_score = np.nan

            # Step 6: Create score row
            score_row = {
                "timestamp": current_datetime,
                "scores": scores.tolist(),
                "mean_score": mean_score,
                "std_score": std_score,
            }

            # Step 7: Store in database
            await self.db_manager.execute(
                """
                INSERT INTO geomagnetic_scores (timestamp, scores, mean_score, std_score)
                VALUES (:timestamp, :scores, :mean_score, :std_score)
                """,
                score_row,
            )

            logger.info(f"Successfully built and stored score row for {current_datetime}")
            return score_row

        except Exception as e:
            logger.error(f"Error building score row: {e}")
            logger.error(traceback.format_exc())
            raise

    async def recalculate_recent_scores(self, uids: list):
        """
        Recalculate recent scores for the given UIDs across the 3-day scoring window
        and update all affected score_table rows.

        Args:
            uids (list): List of UIDs to recalculate scores for.
        """
        try:
            # Step 1: Delete predictions for the given UIDs
            delete_query = """
            DELETE FROM geomagnetic_predictions
            WHERE miner_uid = ANY(:uids)
            """
            await self.db_manager.execute(delete_query, {"uids": uids})
            logger.info(f"Deleted predictions for UIDs: {uids}")

            # Step 2: Set up time window (3 days)
            current_time = datetime.now(timezone.utc)
            history_window = current_time - timedelta(days=3)

            # Delete existing score rows for the period
            delete_scores_query = """
            DELETE FROM score_table 
            WHERE task_name = 'geomagnetic'
            AND task_id::float >= :start_timestamp
            AND task_id::float <= :end_timestamp
            """
            await self.db_manager.execute(
                delete_scores_query,
                {
                    "start_timestamp": history_window.timestamp(),
                    "end_timestamp": current_time.timestamp(),
                },
            )
            logger.info(
                f"Deleted score rows for period {history_window} to {current_time}"
            )

            # Get historical data
            history_query = """
            SELECT 
                miner_hotkey,
                score,
                query_time
            FROM geomagnetic_history
            WHERE query_time >= :history_window
            ORDER BY query_time ASC
            """
            history_results = await self.db_manager.fetch_many(
                history_query, {"history_window": history_window}
            )

            # Get mapping of hotkeys to UIDs
            query = """
            SELECT uid, hotkey FROM node_table 
            WHERE hotkey IS NOT NULL
            """
            miner_mappings = await self.db_manager.fetch_many(query)
            hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}

            # Group records by hour
            hourly_records = {}
            for record in history_results:
                hour_key = record["query_time"].replace(
                    minute=0, second=0, microsecond=0
                )
                if hour_key not in hourly_records:
                    hourly_records[hour_key] = []
                hourly_records[hour_key].append(record)

            # Process each hour
            for hour, records in hourly_records.items():
                scores = [float("nan")] * 256

                # Calculate scores for this hour
                for record in records:
                    miner_hotkey = record["miner_hotkey"]
                    if miner_hotkey in hotkey_to_uid:
                        uid = hotkey_to_uid[miner_hotkey]
                        scores[uid] = record["score"]

                # Create and insert score row for this hour
                score_row = {
                    "task_name": "geomagnetic",
                    "task_id": str(hour.timestamp()),
                    "score": scores,
                    "status": "completed",
                }

                insert_query = """
                INSERT INTO score_table (task_name, task_id, score, status)
                VALUES (:task_name, :task_id, :score, :status)
                """
                await self.db_manager.execute(insert_query, score_row)
                logger.info(f"Recalculated and inserted score row for hour {hour}")

            logger.info(
                f"Completed recalculation of scores for UIDs: {uids} over 3-day window"
            )

        except Exception as e:
            logger.error(f"Error recalculating recent scores: {e}")
            logger.error(traceback.format_exc())

    @task(
        name="validate_miner_response",
        retries=3,
        retry_delay_seconds=60,
        description="Validate and process a single miner response"
    )
    async def validate_miner_response(
        self,
        response_data: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Validate and process a single miner response."""
        try:
            if isinstance(response_data, dict) and "text" in response_data:
                response = json.loads(response_data["text"])
                predicted_value = float(response.get("predicted_values"))
                miner_hotkey = response.get("miner_hotkey")

                # Get miner UID
                query = "SELECT uid FROM node_table WHERE hotkey = :miner_hotkey"
                result = await self.db_manager.fetch_one(
                    query, {"miner_hotkey": miner_hotkey}
                )
                
                if result and predicted_value is not None:
                    return {
                        'miner_uid': result["uid"],
                        'miner_hotkey': miner_hotkey,
                        'predicted_value': predicted_value
                    }
            return None
        except Exception as e:
            logger.error(f"Error validating miner response: {e}")
            return None

    @task(
        name="validate_ground_truth",
        retries=3,
        retry_delay_seconds=60,
        description="Validate ground truth value"
    )
    async def validate_ground_truth(self, value: float) -> Optional[float]:
        """
        Validate ground truth DST value.
        
        Args:
            value (float): Ground truth DST value
            
        Returns:
            Optional[float]: Validated value or None if invalid
        """
        try:
            if value is None or not isinstance(value, (int, float)):
                logger.error(f"Invalid ground truth type: {type(value)}")
                return None
                
            # DST values typically range from -500 to 500 nT
            if not -500 <= value <= 500:
                logger.warning(f"Ground truth value {value} outside typical range [-500, 500]")
                
            return float(value)
        except Exception as e:
            logger.error(f"Error validating ground truth: {e}")
            return None

    @task(
        name="validate_prediction",
        retries=3,
        retry_delay_seconds=60,
        description="Validate prediction value"
    )
    async def validate_prediction(self, value: float) -> Optional[float]:
        """
        Validate predicted DST value.
        
        Args:
            value (float): Predicted DST value
            
        Returns:
            Optional[float]: Validated value or None if invalid
        """
        try:
            if value is None or not isinstance(value, (int, float)):
                logger.error(f"Invalid prediction type: {type(value)}")
                return None
                
            # DST values typically range from -500 to 500 nT
            if not -500 <= value <= 500:
                logger.warning(f"Predicted value {value} outside typical range [-500, 500]")
                
            return float(value)
        except Exception as e:
            logger.error(f"Error validating prediction: {e}")
            return None

    @task(
        name="normalize_score",
        retries=3,
        retry_delay_seconds=60,
        description="Normalize score to [0, 1] range"
    )
    async def normalize_score(self, score: float) -> float:
        """
        Normalize score to [0, 1] range using exponential decay.
        
        Args:
            score (float): Raw score (absolute difference)
            
        Returns:
            float: Normalized score between 0 and 1
        """
        try:
            # Use exponential decay for normalization
            # Score of 0 -> 1.0
            # Score of 50 -> 0.37
            # Score of 100 -> 0.14
            return float(np.exp(-abs(score) / 50.0))
        except Exception as e:
            logger.error(f"Error normalizing score: {e}")
            return 0.0

    @task(
        name="retry_failed_predictions",
        retries=3,
        retry_delay_seconds=60,
        description="Retry processing of failed predictions"
    )
    async def retry_failed_predictions(self, max_retries: int = 3) -> List[Dict[str, Any]]:
        """
        Retry processing of failed predictions with exponential backoff.
        
        Args:
            max_retries (int): Maximum number of retry attempts
            
        Returns:
            List[Dict[str, Any]]: List of successfully recovered predictions
        """
        try:
            # Get failed predictions that haven't exceeded retry limit
            failed_predictions = await self.db_manager.fetch_all(
                """
                SELECT * FROM geomagnetic_predictions 
                WHERE status = 'retry'
                AND retry_count < :max_retries
                AND last_retry < CURRENT_TIMESTAMP - INTERVAL '5 minutes'
                ORDER BY last_retry ASC
                """,
                {"max_retries": max_retries}
            )
            
            if not failed_predictions:
                logger.info("No failed predictions to retry")
                return []
            
            logger.info(f"Found {len(failed_predictions)} failed predictions to retry")
            
            recovered_predictions = []
            for pred in failed_predictions:
                try:
                    # Calculate backoff time
                    retry_count = pred["retry_count"]
                    backoff_seconds = min(300 * (2 ** retry_count), 3600)  # Max 1 hour
                    
                    # Check if enough time has passed since last retry
                    time_since_retry = (datetime.now(timezone.utc) - pred["last_retry"]).total_seconds()
                    if time_since_retry < backoff_seconds:
                        continue
                    
                    # Validate prediction data
                    validated_value = await self.validate_prediction.submit(
                        pred.get("predicted_value")
                    )
                    
                    if validated_value is None:
                        await self._mark_prediction_failed(
                            pred["id"],
                            "Invalid prediction value"
                        )
                        continue
                    
                    # Update prediction status
                    await self.db_manager.execute(
                        """
                        UPDATE geomagnetic_predictions 
                        SET status = 'pending',
                            predicted_value = :value,
                            retry_count = retry_count + 1,
                            last_retry = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """,
                        {"id": pred["id"], "value": validated_value}
                    )
                    
                    recovered_predictions.append({
                        "id": pred["id"],
                        "miner_uid": pred["miner_uid"],
                        "predicted_value": validated_value,
                        "query_time": pred["query_time"]
                    })
                    
                    logger.info(f"Successfully recovered prediction {pred['id']}")
                    
                except Exception as e:
                    logger.error(f"Error retrying prediction {pred['id']}: {str(e)}")
                    if retry_count >= max_retries - 1:
                        await self._mark_prediction_failed(
                            pred["id"],
                            f"Max retries exceeded: {str(e)}"
                        )
                    continue
            
            logger.info(f"Successfully recovered {len(recovered_predictions)} predictions")
            return recovered_predictions
            
        except Exception as e:
            logger.error(f"Error in retry_failed_predictions: {str(e)}")
            return []

    async def _mark_prediction_failed(self, prediction_id: str, reason: str):
        """Mark a prediction as permanently failed."""
        try:
            await self.db_manager.execute(
                """
                UPDATE geomagnetic_predictions 
                SET status = 'failed',
                    failure_reason = :reason,
                    last_updated = CURRENT_TIMESTAMP
                WHERE id = :id
                """,
                {"id": prediction_id, "reason": reason}
            )
            logger.warning(f"Marked prediction {prediction_id} as failed: {reason}")
        except Exception as e:
            logger.error(f"Error marking prediction {prediction_id} as failed: {str(e)}")

    @task(
        name="cleanup_stale_predictions",
        retries=2,
        retry_delay_seconds=30,
        description="Clean up stale predictions"
    )
    async def cleanup_stale_predictions(self, max_age_hours: int = 24):
        """
        Clean up stale predictions that are too old to be useful.
        
        Args:
            max_age_hours (int): Maximum age in hours before a prediction is considered stale
        """
        try:
            # Archive old failed predictions
            await self.db_manager.execute(
                """
                INSERT INTO geomagnetic_history 
                (miner_uid, miner_hotkey, query_time, predicted_value, status, failure_reason)
                SELECT 
                    miner_uid,
                    miner_hotkey,
                    query_time,
                    predicted_value,
                    status,
                    failure_reason
                FROM geomagnetic_predictions
                WHERE query_time < CURRENT_TIMESTAMP - INTERVAL ':hours hours'
                AND status IN ('failed', 'retry')
                """,
                {"hours": max_age_hours}
            )
            
            # Delete archived predictions
            deleted = await self.db_manager.execute(
                """
                DELETE FROM geomagnetic_predictions
                WHERE query_time < CURRENT_TIMESTAMP - INTERVAL ':hours hours'
                AND status IN ('failed', 'retry')
                RETURNING id
                """,
                {"hours": max_age_hours}
            )
            
            if deleted:
                logger.info(f"Cleaned up {len(deleted)} stale predictions")
            
        except Exception as e:
            logger.error(f"Error cleaning up stale predictions: {str(e)}")

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
                start_time = datetime.now(timezone.utc)
                
                # Step 1: Fetch Data and Start Recovery in Parallel
                flow_logger.info("Step 1: Parallel data fetch and recovery")
                data_future = self.fetch_geomag_data.submit()
                
                # Run retry and cleanup tasks in parallel
                retry_future = self.retry_failed_predictions.submit(max_retries=3)
                cleanup_future = self.cleanup_stale_predictions.submit(max_age_hours=24)
                
                recovery_window_start = flow_state['current_time'] - timedelta(hours=3)
                recovery_future = self.recover_failed_predictions.submit(
                    start_time=recovery_window_start,
                    end_time=flow_state['current_time'],
                    validator=validator
                )
                
                # Wait for data fetch result first as it's needed for next steps
                data = await data_future
                if not data:
                    raise ValueError("Failed to fetch geomagnetic data")
                
                flow_state['last_successful_step'] = 'fetch_data'
                flow_metrics['processing_time']['fetch_data'] = (datetime.now(timezone.utc) - start_time).total_seconds()
                
                # Continue with existing flow logic...
                
                # Wait for recovery operations to complete
                retry_results = await retry_future
                await cleanup_future  # No return value needed
                recovered_predictions = await recovery_future
                
                flow_metrics['prediction_stats'].update({
                    'recovered_count': len(recovered_predictions),
                    'retried_count': len(retry_results)
                })
                
                # Continue with rest of flow...
                
            except Exception as e:
                flow_state['status'] = 'failed'
                flow_state['error_count'] += 1
                flow_metrics['success_rate'] = 0.0
                flow_metrics['error_counts'][str(e)] = flow_metrics['error_counts'].get(str(e), 0) + 1
                
                flow_logger.error(f"Flow failed at step {flow_state['last_successful_step']}: {str(e)}")
                flow_logger.error(traceback.format_exc())
                
                # Store flow state and metrics for debugging
                await self.db_manager.execute(
                    """
                    INSERT INTO flow_state_log (
                        flow_id, 
                        task_name, 
                        status, 
                        last_successful_step,
                        error_count,
                        error_message,
                        metrics,
                        timestamp
                    ) VALUES (
                        :flow_id,
                        :task_name,
                        :status,
                        :last_step,
                        :error_count,
                        :error_msg,
                        :metrics,
                        CURRENT_TIMESTAMP
                    )
                    """,
                    {
                        "flow_id": flow_state['flow_id'],
                        "task_name": task_name,
                        "status": flow_state['status'],
                        "last_step": flow_state['last_successful_step'],
                        "error_count": flow_state['error_count'],
                        "error_msg": str(e),
                        "metrics": json.dumps(flow_metrics)
                    }
                )
                raise
            
            return flow_state
            
        return task_flow

    @task(
        retries=3,
        retry_delay_seconds=60,
        cache_expiration=timedelta(minutes=5)
    )
    async def add_prediction_to_queue(
        self,
        miner_uid: str,
        miner_hotkey: str,
        predicted_value: float,
        query_time: datetime,
        status: str = "pending",
    ) -> None:
        """Add a prediction to the queue as a Prefect task."""
        async with self.db_manager.get_connection() as session:
            try:
                query = """
                    INSERT INTO geomagnetic_predictions 
                    (id, miner_uid, miner_hotkey, predicted_value, query_time, status)
                    VALUES (:id, :miner_uid, :miner_hotkey, :predicted_value, :query_time, :status)
                """

                params = {
                    "id": str(uuid4()),
                    "miner_uid": miner_uid,
                    "miner_hotkey": miner_hotkey,
                    "predicted_value": float(predicted_value),
                    "query_time": query_time,
                    "status": status,
                }

                await session.execute(query, params)
                await session.commit()
                logger.info(f"Added prediction from miner {miner_uid} to queue")

            except Exception as e:
                await session.rollback()
                logger.error(f"Error adding prediction to queue: {e}")
                logger.error(traceback.format_exc())
                raise

    @task(
        retries=3,
        retry_delay_seconds=60,
        cache_expiration=timedelta(minutes=5)
    )
    async def fetch_ground_truth(self):
        """Fetch ground truth as a Prefect task."""
        return await super().fetch_ground_truth()

    @task(
        name="get_pending_predictions",
        retries=3,
        retry_delay_seconds=60,
        description="Get pending predictions for a time range"
    )
    async def get_pending_predictions(
        self,
        start_time: datetime,
        end_time: datetime,
        context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get pending predictions for the specified time range."""
        return await self.get_tasks_for_hour(start_time, end_time, context['validator'])

    @task(
        name="archive_scores",
        retries=3,
        retry_delay_seconds=60,
        description="Archive scored predictions to history"
    )
    async def archive_scores(
        self,
        scored_predictions: List[Dict[str, Any]],
        context: Dict[str, Any]
    ) -> None:
        """Archive scored predictions to history."""
        for prediction in scored_predictions:
            await self.move_task_to_history.submit(
                task=prediction,
                ground_truth_value=prediction['ground_truth_value'],
                score=prediction['score'],
                score_time=context['current_time']
            )

    @task(
        name="recover_failed_predictions",
        retries=3,
        retry_delay_seconds=60,
        description="Recover and reprocess failed predictions"
    )
    async def recover_failed_predictions(
        self,
        start_time: datetime,
        end_time: datetime,
        validator: Any
    ) -> List[Dict[str, Any]]:
        """
        Recover and reprocess failed predictions within a time window.
        
        Args:
            start_time (datetime): Start of time window
            end_time (datetime): End of time window
            validator (Any): Validator instance
            
        Returns:
            List[Dict[str, Any]]: List of recovered predictions
        """
        try:
            # Query failed predictions
            failed_predictions = await self.db_manager.fetch_all(
                """
                SELECT * FROM geomagnetic_predictions 
                WHERE query_time BETWEEN :start_time AND :end_time
                AND (status = 'failed' OR status = 'error')
                """,
                {"start_time": start_time, "end_time": end_time}
            )
            
            if not failed_predictions:
                logger.info("No failed predictions found in the specified time window")
                return []
                
            logger.info(f"Found {len(failed_predictions)} failed predictions to recover")
            
            recovered_predictions = []
            for pred in failed_predictions:
                try:
                    # Validate prediction data
                    validated_value = await self.validate_prediction.submit(
                        pred.get("predicted_value")
                    )
                    
                    if validated_value is None:
                        continue
                        
                    # Update prediction status
                    await self.db_manager.execute(
                        """
                        UPDATE geomagnetic_predictions 
                        SET status = 'pending', 
                            predicted_value = :value,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """,
                        {"id": pred["id"], "value": validated_value}
                    )
                    
                    recovered_predictions.append({
                        "id": pred["id"],
                        "miner_uid": pred["miner_uid"],
                        "predicted_value": validated_value,
                        "query_time": pred["query_time"]
                    })
                    
                except Exception as e:
                    logger.error(f"Error recovering prediction {pred['id']}: {str(e)}")
                    continue
            
            logger.info(f"Successfully recovered {len(recovered_predictions)} predictions")
            return recovered_predictions
            
        except Exception as e:
            logger.error(f"Error in recover_failed_predictions task: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    @task(
        name="queue_failed_tasks",
        retries=2,
        retry_delay_seconds=30,
        description="Queue failed tasks for retry"
    )
    async def queue_failed_tasks(self, failed_tasks: List[Dict[str, Any]]):
        """Queue failed tasks for retry processing."""
        try:
            for task in failed_tasks:
                # Update task status to retry
                await self.db_manager.execute(
                    """
                    UPDATE geomagnetic_predictions
                    SET status = 'retry',
                        retry_count = COALESCE(retry_count, 0) + 1,
                        last_retry = CURRENT_TIMESTAMP
                    WHERE id = :task_id
                    """,
                    {"task_id": task["id"]}
                )
            logger.info(f"Queued {len(failed_tasks)} tasks for retry")
        except Exception as e:
            logger.error(f"Error queueing failed tasks: {str(e)}")

