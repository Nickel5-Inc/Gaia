import traceback
from typing import Any, Dict, Optional, Union, List
import uuid
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.tasks.base.task import Task
from gaia.tasks.base.deterministic_job_id import DeterministicJobID
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_metadata import (
    GeomagneticMetadata,
)
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_inputs import GeomagneticInputs
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import (
    GeomagneticPreprocessing,
)
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_scoring_mechanism import (
    GeomagneticScoringMechanism,
)
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_outputs import GeomagneticOutputs
from gaia.tasks.defined_tasks.geomagnetic.utils.process_geomag_data import (
    get_latest_geomag_data,
    get_geomag_data_for_hour,
)
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.models.geomag_basemodel import GeoMagBaseModel
import torch
import datetime
import numpy as np
import pandas as pd
import asyncio
from uuid import uuid4
from fiber.logging_utils import get_logger
import json
from pydantic import Field
import os
import importlib.util
from sqlalchemy.sql import text

logger = get_logger(__name__)


class GeomagneticTask(Task):
    """
    A task class for processing and analyzing geomagnetic data, with
    execution methods for both miner and validator workflows.

    🔒 SECURITY ENHANCEMENTS IMPLEMENTED:
    
    1. TEMPORAL SEPARATION ENFORCEMENT AT SCORING TIME:
       - Ground truth fetching requires 90 minutes delay (30 minutes in test mode)
       - Intelligent retry logic waits for proper data stability before scoring
       - Prevents premature scoring when ground truth isn't stable yet
    
    2. SECURE SCORING FLOW:
       - _fetch_geomag_data(): Uses latest data for real-time miner queries (good!)
       - _query_miners(): Sends current data for realistic prediction tasks
       - _fetch_ground_truth_for_time(): Enhanced temporal separation controls for scoring
       - _check_ground_truth_availability(): Enforces security requirements before scoring
       - _intelligent_retry_pending_tasks(): Waits for proper timing before retry attempts
    
    3. VULNERABILITY MITIGATIONS:
       - Eliminates premature ground truth access during prediction windows
       - Ensures adequate time for Kyoto data stability before scoring  
       - Adds comprehensive security logging and validation
       - Implements sanity checks for data integrity

    This task involves:
        - Querying miners for predictions using current data (real-time prediction)
        - Adding predictions to a queue for scoring
        - Waiting for proper temporal separation before scoring
        - Fetching time-specific ground truth with security controls
        - Scoring predictions only when ground truth is stable
        - Moving scored tasks to history

    Attributes:
        name (str): The name of the task, set as "GeomagneticTask".
        description (str): A description of the task's purpose.
        task_type (str): Specifies the type of task (e.g., "atomic").
        metadata (GeomagneticMetadata): Metadata associated with the task.
        inputs (GeomagneticInputs): Handles data loading and validation.
        preprocessing (GeomagneticPreprocessing): Processes raw data.
        scoring_mechanism (GeomagneticScoringMechanism): Computes scores.
        outputs (GeomagneticOutputs): Manages output formatting and saving.

    Example:
        task = GeomagneticTask()
        task.miner_execute()
        task.validator_execute()
    """

    # Declare Pydantic fields
    db_manager: Union[ValidatorDatabaseManager, MinerDatabaseManager] = Field(
        default_factory=ValidatorDatabaseManager,
        description="Database manager for the task",
    )
    miner_preprocessing: GeomagneticPreprocessing = Field(
        default_factory=GeomagneticPreprocessing,
        description="Preprocessing component for miner",
    )
    model: Optional[GeoMagBaseModel] = Field(
        default=None, description="The geomagnetic prediction model"
    )
    node_type: str = Field(
        default="validator",
        description="Type of node running the task (validator or miner)"
    )
    test_mode: bool = Field(
        default=False,
        description="Whether to run in test mode (immediate execution, limited scope)"
    )
    validator: Any = Field(
        default=None, 
        description="Reference to the validator instance"
    )
    # New fields for improved retry logic
    pending_retry_worker_running: bool = Field(
        default=False,
        description="Whether the pending retry worker is running"
    )
    pending_retry_worker_task: Optional[Any] = Field(
        default=None,
        description="Reference to the pending retry worker task"
    )


    def __init__(self, node_type: str, db_manager, test_mode: bool = False, **data):
        """Initialize the task."""
        super().__init__(
            name="GeomagneticTask",
            description="Geomagnetic prediction task",
            task_type="atomic",
            metadata=GeomagneticMetadata(),
            inputs=GeomagneticInputs(),
            outputs=GeomagneticOutputs(),
            db_manager=db_manager,
            scoring_mechanism=GeomagneticScoringMechanism(db_manager=db_manager),
            **data,
        )
        
        self.node_type = node_type
        self.test_mode = test_mode
        self.model = None
        
        if self.node_type == "miner":
            try:
                logger.info("Running as miner - loading model...")
                # Try to load custom model first
                custom_model_path = "gaia/models/custom_models/custom_geomagnetic_model.py"
                if os.path.exists(custom_model_path):
                    spec = importlib.util.spec_from_file_location(
                        "custom_geomagnetic_model", custom_model_path
                    )
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    self.model = module.CustomGeomagneticModel()
                    logger.info("Successfully loaded custom geomagnetic model")
                else:
                    # Fall back to base model
                    from gaia.models.geomag_basemodel import GeoMagBaseModel
                    self.model = GeoMagBaseModel()
                    logger.info("No custom model found, using base model")
            except Exception as e:
                logger.error(f"Error loading model: {e}")
                logger.error(traceback.format_exc())
                raise
        else:
            logger.info("Running as validator - skipping model loading")

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

    def validator_prepare_subtasks(self, data):
        """
        Prepare subtasks for validation.

        Args:
            data (dict): Data received by the validator.
        Returns:
            list: List of subtasks to process.
        """
        try:
            subtasks = [
                {"timestamp": data["timestamp"], "value": value}
                for value in data["values"]
            ]
            return subtasks
        except Exception as e:
            logger.error(f"Error in validator_prepare_subtasks: {e}")
            return []

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

    async def validator_execute(self, validator):
        """
        Executes the validator workflow:
        - Aligns execution to start at the top of each UTC hour.
        - At hour N:
            - Query miners for new predictions
            - Score predictions collected during hour N-1
        - Runs in a continuous loop.
        - Starts a background worker for intelligent retry of pending tasks.
        """
        self.validator = validator
        
        # Start the background worker for intelligent retry of pending tasks
        await self._start_pending_retry_worker(validator)
        
        # Track the last executed hour to prevent double execution
        last_executed_hour = None
        
        try:
            while True:
                try:
                    await validator.update_task_status('geomagnetic', 'active')
                    
                    # Step 1: Flexible execution window T:02-T:15 for operational resilience
                    current_time = datetime.datetime.now(datetime.timezone.utc)
                    if not self.test_mode:
                        # Define execution window: T:02 to T:15 (13-minute window)
                        current_hour = current_time.replace(minute=0, second=0, microsecond=0)
                        window_start = current_hour + datetime.timedelta(minutes=2)   # T:02
                        window_end = current_hour + datetime.timedelta(minutes=15)    # T:15
                        
                        # Check if we've already executed for this hour
                        if last_executed_hour == current_hour:
                            # Already executed for this hour - wait for next hour's window
                            next_hour = current_hour + datetime.timedelta(hours=1)
                            next_window_start = next_hour + datetime.timedelta(minutes=2)
                            sleep_duration = (next_window_start - current_time).total_seconds()
                            logger.info(f"🔒 EXECUTION COMPLETE: Already executed for hour {current_hour.strftime('%H:%M')}, waiting for next window at {next_window_start.strftime('%H:%M')} (in {sleep_duration:.0f} seconds)")
                            await validator.update_task_status('geomagnetic', 'idle')
                            await asyncio.sleep(sleep_duration)
                            query_hour = next_hour
                        
                        # Check if we're in the current hour's execution window
                        elif window_start <= current_time <= window_end:
                            # We're in the execution window - proceed immediately
                            query_hour = current_hour
                            logger.info(f"🔒 FLEXIBLE TIMING: Executing within window at {current_time.strftime('%H:%M')} (T:02-T:15 window)")
                            logger.info(f"🔒 Using {query_hour.strftime('%H:%M')} data to query for {(query_hour + datetime.timedelta(hours=1)).strftime('%H:%M')} predictions")
                        
                        elif current_time < window_start:
                            # Too early - wait until T:02
                            sleep_duration = (window_start - current_time).total_seconds()
                            logger.info(f"🔒 SECURE TIMING: Waiting for execution window to open at {window_start.strftime('%H:%M')} (in {sleep_duration:.0f} seconds)")
                            logger.info(f"🔒 Execution window: T:02-T:15 allows flexibility for async delays")
                            await validator.update_task_status('geomagnetic', 'idle')
                            await asyncio.sleep(sleep_duration)
                            query_hour = current_hour
                        
                        else:
                            # Past T:15 - move to next hour's window
                            next_hour = current_hour + datetime.timedelta(hours=1)
                            next_window_start = next_hour + datetime.timedelta(minutes=2)
                            sleep_duration = (next_window_start - current_time).total_seconds()
                            logger.info(f"⏰ MISSED WINDOW: Past T:15, waiting for next execution window at {next_window_start.strftime('%H:%M')} (in {sleep_duration:.0f} seconds)")
                            await validator.update_task_status('geomagnetic', 'idle')
                            await asyncio.sleep(sleep_duration)
                            query_hour = next_hour
                    else:
                        query_hour = current_time.replace(minute=0, second=0, microsecond=0)
                        logger.info("Test mode: Running immediately, will sleep for 5 minutes after completion")

                    logger.info(f"🔒 Starting GeomagneticTask execution at T:02 for hour {query_hour}")

                    # Step 2: Fetch Geomagnetic Data for the current query hour
                    await validator.update_task_status('geomagnetic', 'processing', 'data_fetch')
                    # Wait up to 30 minutes for correct hour data (or 5 minutes in test mode)
                    max_wait = 5 if self.test_mode else 30
                    
                    timestamp, dst_value, historical_data = await self._fetch_geomag_data(
                        target_hour=query_hour, max_wait_minutes=max_wait
                    )
                    
                    # Verify we got valid data for the correct hour
                    if timestamp == "N/A" or dst_value == "N/A":
                        logger.warning(f"❌ Could not obtain geomagnetic data for target hour {query_hour}. Skipping this cycle.")
                        await validator.update_task_status('geomagnetic', 'idle', 'data_unavailable_for_target_hour')
                        # Sleep before next attempt - don't busy loop
                        await asyncio.sleep(300)  # 5 minutes
                        continue
                    
                    # Double-check we got data for the exact target hour
                    if timestamp.replace(minute=0, second=0, microsecond=0) != query_hour:
                        logger.error(f"❌ Expected data for {query_hour} but got {timestamp}. This should not happen with the new logic.")
                        await validator.update_task_status('geomagnetic', 'error', 'incorrect_hour_data')
                        continue
                    
                    logger.info(f"✅ Successfully obtained geomagnetic data for target hour {query_hour}: {dst_value}")
                    
                    # Step 3: Query Miners for predictions  
                    await validator.update_task_status('geomagnetic', 'processing', 'miner_query')
                    # Use 1-hour prediction window from data timestamp
                    target_prediction_hour = timestamp + datetime.timedelta(hours=1)
                    logger.info(f"Querying miners to predict {target_prediction_hour} using data from {timestamp}")
                    logger.info(f"🔍 Prediction window: {timestamp} → {target_prediction_hour} (1 hour ahead)")
                    await self._query_miners(
                        validator, timestamp, dst_value, historical_data, query_hour, target_prediction_hour
                    )
                    logger.info(f"Collected predictions for {target_prediction_hour}")

                    # Step 4: Score predictions with SECURE ground truth fetching
                    score_target_hour = query_hour  # Score predictions that targeted this hour
                    scoring_query_hour = query_hour - datetime.timedelta(hours=1)  # Made 1 hour ago
                    await validator.update_task_status('geomagnetic', 'processing', 'scoring')
                    logger.info(f"🔒 SECURITY: Scoring predictions that targeted {score_target_hour}")
                    logger.info(f"🔒 SECURITY: These predictions were made at {scoring_query_hour} using aged data")
                    logger.info(f"🔒 SECURITY: Ground truth fetching will enforce minimum {90 if not self.test_mode else 30}-minute delay")
                    await self._process_scores(validator, scoring_query_hour, score_target_hour)
                    
                    # Mark this hour as executed to prevent re-execution
                    last_executed_hour = query_hour
                    logger.info(f"🔒 EXECUTION CYCLE COMPLETE: Marked hour {query_hour.strftime('%H:%M')} as executed")
                    
                    await validator.update_task_status('geomagnetic', 'idle')

                    # In test mode, sleep for 5 minutes before next iteration
                    if self.test_mode:
                        logger.info("Test mode: Sleeping for 5 minutes before next execution")
                        await asyncio.sleep(300)
                        last_executed_hour = None  # Reset in test mode to allow re-execution
                    else:
                        # Every 6 hours, run a more comprehensive cleanup of old pending tasks
                        if query_hour.hour % 6 == 0:
                            logger.info("Running comprehensive cleanup of old pending tasks")
                            await self._comprehensive_pending_cleanup(validator)
                        
                        # Continue loop immediately - will wait for next hour due to last_executed_hour check

                except Exception as e:
                    logger.error(f"Unexpected error in validator_execute loop: {e}")
                    logger.error(traceback.format_exc())
                    await validator.update_task_status('geomagnetic', 'error')
                    await asyncio.sleep(3600)
        finally:
            # Clean up the background worker when exiting
            await self._stop_pending_retry_worker()

    async def _fetch_geomag_data(self, target_hour=None, max_wait_minutes=30):
        """
        Fetch geomagnetic data for miner queries, for the specific target hour only.
        
        🔒 SECURITY: Using current hour data for miner queries ensures proper alignment
        between query execution time and data timestamp. Security enforcement happens 
        at scoring time with proper temporal separation.
        
        Args:
            target_hour (datetime, optional): Specific hour to fetch data for
            max_wait_minutes (int): Maximum minutes to wait for target hour data
        """
        if target_hour is not None:
            logger.info(f"Fetching geomagnetic data for target hour: {target_hour}")
            timestamp, dst_value, historical_data = await get_geomag_data_for_hour(
                target_hour, include_historical=True, max_wait_minutes=max_wait_minutes
            )
        else:
            logger.info("Fetching latest geomagnetic data for miner queries...")
            timestamp, dst_value, historical_data = await get_latest_geomag_data(
                include_historical=True
            )
        
        logger.info(
            f"Fetched geomagnetic data: timestamp={timestamp}, value={dst_value}"
        )
        if historical_data is not None:
            logger.info(
                f"Fetched historical data for the current month: {len(historical_data)} records"
            )
        else:
            logger.warning("No historical data available for the current month.")
        return timestamp, dst_value, historical_data

    async def _query_miners(
        self, validator, timestamp, dst_value, historical_data, current_hour_start, target_prediction_hour=None
    ):
        """
        Query miners with current geomagnetic data for real-time predictions.
        
        🔒 SECURITY: Using current data for miner queries is correct - miners need
        up-to-date information for realistic prediction tasks. Security is enforced
        at scoring time with proper temporal separation.
        
        Args:
            validator: Validator instance
            timestamp: Timestamp of the current geomagnetic data being provided
            dst_value: Current DST value being provided to miners  
            historical_data: Historical data context
            current_hour_start: The hour the query is being made (for task_id)
            target_prediction_hour: The hour miners should predict (defaults to current_hour_start + 1)
        """
        if timestamp == "N/A" or dst_value == "N/A":
            logger.warning("Invalid geomagnetic data. Skipping miner queries.")
            return

        # Default target prediction hour to 1 hour after data timestamp (not execution hour)
        if target_prediction_hour is None:
            target_prediction_hour = timestamp + datetime.timedelta(hours=1)
        
        task_id = str(current_hour_start.timestamp())
        logger.info(f"Using current data from {timestamp} to ask miners to predict {target_prediction_hour}")
        logger.info(f"Running DST basemodel for scoring with task_id: {task_id} (timestamp: {current_hour_start})")
        
        validator.basemodel_evaluator.test_mode = self.test_mode
        
        await validator.basemodel_evaluator.predict_geo_and_store(
            historical_data,
            task_id
        )
        # Construct Payload for Miners
        nonce = str(uuid4())

        def _prepare_historical_records_sync(hist_data):
            """Synchronous helper to prepare historical records from DataFrame."""
            records = []
            if hist_data is not None:
                for _, rec_row in hist_data.iterrows(): # Changed variable name to avoid conflict
                    records.append(
                        {"timestamp": rec_row["timestamp"].isoformat(), "Dst": rec_row["Dst"]}
                    )
            return records

        # Convert historical data to serializable format
        if historical_data is not None:
            loop = asyncio.get_event_loop()
            historical_records = await loop.run_in_executor(None, _prepare_historical_records_sync, historical_data)
        else:
            historical_records = []

        payload_template = {
            "nonce": nonce,
            "data": {
                "name": "Geomagnetic Data",
                "timestamp": timestamp.isoformat(),
                "value": dst_value,
                "historical_values": historical_records,
                "prediction_target_hour": target_prediction_hour.isoformat(),
                "instruction": f"Predict DST value for {target_prediction_hour.strftime('%Y-%m-%d %H:%M')} UTC using provided data up to {timestamp.strftime('%Y-%m-%d %H:%M')} UTC"
            },
        }
        endpoint = "/geomagnetic-request"

        logger.info(f"Querying miners for geomagnetic predictions")
        responses = await validator.query_miners(payload_template, endpoint)
        logger.info(f"Collected responses from miners: {len(responses)}")

        await self.process_miner_responses(responses, current_hour_start, validator)
        logger.info(f"Added {len(responses)} predictions to the database")

    async def _process_scores(self, validator, query_hour, target_hour=None):
        """
        Process task scores for a given hour.
        
        Args:
            validator: Validator instance
            query_hour: When the predictions were made (query time)
            target_hour: What hour the predictions were targeting (defaults to query_hour + 1)
        """
        try:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            
            # Default target hour to next hour if not specified
            if target_hour is None:
                target_hour = query_hour + datetime.timedelta(hours=1)
            
            logger.info(
                f"🔒 SECURE SCORING: Processing predictions made at {query_hour.isoformat()} that targeted {target_hour.isoformat()}"
            )

            if self.test_mode:
                start_time = current_time - datetime.timedelta(minutes=10)
                end_time = current_time
                logger.info(f"TEST MODE: Using recent time window for immediate scoring: {start_time} to {end_time}")
            else:
                start_time = query_hour
                end_time = query_hour + datetime.timedelta(hours=1)

            tasks = await self.get_tasks_for_hour(
                start_time,
                end_time,
                validator=validator
            )

            if not tasks:
                logger.info(f"No tasks found for query hour {query_hour.isoformat()}")
                return [], current_time

            # If all tasks are already scored, return early
            all_processed = all(task.get("status") == "scored" for task in tasks)
            if all_processed:
                logger.info(f"All tasks already scored for query hour {query_hour.isoformat()}")
                return [], current_time

            # Check if ground truth is available for the TARGET hour (not query hour)
            if not await self._check_ground_truth_availability(target_hour):
                logger.info(f"Ground truth not yet available for target hour {target_hour.isoformat()}, tasks will remain pending")
                return [], current_time

            # Get ground truth data for the TARGET hour that predictions were aiming for
            ground_truth_value = await self._fetch_ground_truth_for_time(target_hour, max_attempts=3)
            if ground_truth_value is None:
                logger.warning(f"Could not fetch ground truth value for target hour {target_hour.isoformat()}, tasks will remain pending")
                return [], current_time

            logger.info(f"Ground truth value: {ground_truth_value}")
            
            scored_tasks = []
            for task in tasks:
                if task.get("status") == "scored":
                    logger.info(f"Task {task['id']} already scored, skipping")
                    continue

                try:
                    # Use validator's basemodel_evaluator to get the baseline score for comparison
                    baseline_score = None
                    if validator and hasattr(validator, 'basemodel_evaluator'):
                        try:
                            task_timestamp = task.get("timestamp", task.get("query_time"))
                            if task_timestamp:
                                if isinstance(task_timestamp, datetime.datetime):
                                    if task_timestamp.tzinfo is not None:
                                        task_timestamp_utc = task_timestamp.astimezone(datetime.timezone.utc)
                                    else:
                                        task_timestamp_utc = task_timestamp.replace(tzinfo=datetime.timezone.utc)
                                        
                                    task_id = str(task_timestamp_utc.timestamp())
                                else:
                                    task_id = str(task_timestamp)
                                logger.debug(f"Looking up baseline prediction with task_id: {task_id}")
                                validator.basemodel_evaluator.test_mode = self.test_mode
                                
                                baseline_score = await validator.basemodel_evaluator.score_geo_baseline(
                                    task_id=task_id,
                                    ground_truth=ground_truth_value
                                )
                                if baseline_score is not None:
                                    logger.info(f"Retrieved baseline score for task_id {task_id}: {baseline_score:.4f}")
                                else:
                                    logger.info(f"No baseline score available for task_id {task_id}")
                            else:
                                logger.warning(f"Task has no timestamp, cannot retrieve baseline prediction")
                        except Exception as e:
                            logger.error(f"Error retrieving baseline score: {e}")
                            logger.error(traceback.format_exc())
                    
                    # Calculate score between prediction and ground truth
                    score = self.scoring_mechanism.calculate_score(
                        task["predicted_values"], ground_truth_value
                    )

                    if baseline_score is not None:
                        logger.info(f"Geomagnetic Task - Miner: {task_id} - Miner score: {score:.4f}, Baseline: {baseline_score:.4f}, Diff: {score - baseline_score:.4f}")

                        base_epsilon = 0.005
                        theoretical_max = 0.99
                        
                        if baseline_score > theoretical_max - 0.10:
                            epsilon = 0.002
                            logger.debug(f"Using reduced epsilon: {epsilon:.4f} (baseline near theoretical max)")
                        else:
                            epsilon = base_epsilon
                        
                        if score <= baseline_score + epsilon:
                            if score < baseline_score:
                                logger.info(f"Score zeroed - Below baseline: {score:.4f} < {baseline_score:.4f}")
                            else:
                                logger.info(f"Score zeroed - Insufficient improvement: {score:.4f} vs baseline {baseline_score:.4f} (needed > {baseline_score + epsilon:.4f})")
                            score = 0
                        else:
                            logger.info(f"Score valid - Exceeds baseline by {score - baseline_score:.4f}")
                    else:
                        logger.info(f"No baseline comparison available - using raw score: {score:.4f}")
                    
                    # Mark task as scored in DB
                    await self.move_task_to_history(
                        task, ground_truth_value, score, current_time
                    )

                    # Add score to task dict for building score row later
                    task["score"] = score
                    scored_tasks.append(task)
                    logger.info(
                        f"Task {task['id']} scored and archived. Score: {score}"
                    )
                except Exception as e:
                    logger.error(f"Error processing task {task.get('id', 'unknown')}: {e}")
                    logger.error(traceback.format_exc())

            # Add score row to score_table if we have scored tasks
            if scored_tasks:
                logger.info(f"Building score row for {len(scored_tasks)} scored tasks targeting {target_hour}")
                await self.build_score_row(target_hour, scored_tasks)
            else:
                logger.info("No tasks were scored, skipping score row creation")

            # Try to retry older pending tasks now that we have ground truth
            await self._retry_pending_tasks(validator, current_time)

            return scored_tasks, current_time

        except Exception as e:
            logger.error(f"Error in process_scores: {e}")
            logger.error(traceback.format_exc())
            return [], current_time

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
            # Convert timestamps to UTC if they aren't already
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=datetime.timezone.utc)
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=datetime.timezone.utc)

            logger.info(f"Querying tasks with:")
            logger.info(f"  start_time: {start_time} (tzinfo: {start_time.tzinfo})")
            logger.info(f"  end_time: {end_time} (tzinfo: {end_time.tzinfo})")

            results = await self.db_manager.fetch_all(
                """
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
                WHERE rn = 1
                """,
                {
                    "start_time": start_time,
                    "end_time": end_time
                }
            )
            
            tasks = []
            for row in results:
                task = {
                    "id": row["id"],
                    "miner_uid": row["miner_uid"],
                    "miner_hotkey": row["miner_hotkey"],
                    "predicted_values": row["predicted_value"],
                    "query_time": row["query_time"],
                    "timestamp": row["query_time"],
                }
                tasks.append(task)

            logger.info(f"Fetched {len(tasks)} tasks between {start_time} and {end_time}")

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
        # Use the retry version for consistency
        return await self._fetch_ground_truth_with_retry()

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
            await self.db_manager.execute(
                """
                INSERT INTO geomagnetic_history 
                (miner_uid, miner_hotkey, query_time, predicted_value, ground_truth_value, score, scored_at)
                VALUES (:miner_uid, :miner_hotkey, :query_time, :predicted_value, :ground_truth_value, :score, :scored_at)
                """,
                {
                    "miner_uid": task["miner_uid"],
                    "miner_hotkey": task["miner_hotkey"],
                    "query_time": task["query_time"],
                    "predicted_value": task["predicted_values"],
                    "ground_truth_value": ground_truth_value,
                    "score": score,
                    "scored_at": score_time,
                }
            )
            logger.info(f"Archived task to history: {task['id']}")

            await self.db_manager.execute(
                """
                DELETE FROM geomagnetic_predictions 
                WHERE id = :task_id
                """,
                {"task_id": task["id"]}
            )
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
            # Extract and validate data from the request payload
            if data and data.get("data"):
                # Process current data
                input_data = pd.DataFrame(
                    {
                        "timestamp": [pd.to_datetime(data["data"]["timestamp"], utc=True)],
                        "value": [float(data["data"]["value"])],
                    }
                )

                # Check and process historical data if available
                if data["data"].get("historical_values"):
                    historical_df = pd.DataFrame(data["data"]["historical_values"])
                    historical_df = historical_df.rename(columns={"Dst": "value"})  # Rename Dst to value
                    historical_df["timestamp"] = pd.to_datetime(historical_df["timestamp"], utc=True)
                    historical_df = historical_df[["timestamp", "value"]]  # Ensure correct columns
                    combined_df = pd.concat([historical_df, input_data], ignore_index=True)
                else:
                    combined_df = input_data

                # Preprocess combined data
                processed_data = self.miner_preprocessing.process_miner_data(combined_df)
            else:
                logger.error("No data provided in request")
                return None

            # Run model inference: Check for custom model first
            if hasattr(self.model, "run_inference"):
                logger.info("Using custom geomagnetic model for inference.")
                predictions = self.model.run_inference(processed_data)
            else:
                logger.info("Using base geomagnetic model for inference.")
                raw_prediction = self.run_model_inference(processed_data)
                predictions = {
                    "predicted_value": float(raw_prediction),
                    "prediction_time": data["data"]["timestamp"]
                }

            # Format response as per MINER.md requirements
            return {
                "predicted_values": float(predictions.get("predicted_value", 0.0)),
                "timestamp": predictions.get("prediction_time", data["data"]["timestamp"]),
                "miner_hotkey": miner.keypair.ss58_address,
            }

        except Exception as e:
            logger.error(f"Error in miner execution: {str(e)}")
            logger.error(traceback.format_exc())
            # Return float('nan') instead of "N/A"
            current_time = datetime.datetime.now(datetime.timezone.utc)
            return {
                "predicted_values": float('nan'),  # Changed from "N/A" to float('nan')
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

    async def add_task_to_queue(self, predictions, query_time):
        """
        Adds a new task to the task queue.

        Args:
            predictions (np.ndarray or None): Array of predictions from miners.
            query_time (datetime): The time the task was added.
        """
        try:
            # Validate predictions
            if predictions is None:
                logger.warning("Received None predictions, skipping queue addition")
                return

            # Convert predictions to a dictionary or JSON-like structure
            if isinstance(predictions, np.ndarray):
                predicted_value = {"predictions": predictions.tolist()}
            else:
                predicted_value = {"predictions": predictions}

            # Add to the queue using execute
            await self.db_manager.execute(
                """
                INSERT INTO task_queue (task_name, miner_id, predicted_value, query_time)
                VALUES (:task_name, :miner_id, :predicted_value, :query_time)
                """,
                {
                    "task_name": "geomagnetic_prediction",
                    "miner_id": "example_miner_id",  # Replace with actual miner ID
                    "predicted_value": json.dumps(predicted_value),
                    "query_time": query_time
                }
            )
            logger.info(f"Task added to queue: geomagnetic_prediction at {query_time}")

        except Exception as e:
            logger.error(f"Error adding task to queue: {e}")
            raise

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
            # Prepare parameters
            # NOTE: Using random UUID for prediction record ID (database primary key)
            # Task IDs elsewhere use deterministic timestamp-based IDs
            params = {
                "id": str(uuid.uuid4()),
                "miner_uid": miner_uid,
                "miner_hotkey": miner_hotkey,
                "predicted_value": float(predicted_value),  # Ensure float type
                "query_time": query_time,
                "status": status,
            }

            logger.info(f"Adding prediction to queue with params: {params}")

            # Execute the query
            await self.db_manager.execute(
                """
                INSERT INTO geomagnetic_predictions 
                (id, miner_uid, miner_hotkey, predicted_value, query_time, status)
                VALUES (:id, :miner_uid, :miner_hotkey, :predicted_value, :query_time, :status)
                """,
                params
            )
            logger.info(f"Added prediction from miner {miner_uid} to queue")

        except Exception as e:
            logger.error(f"Error adding prediction to queue: {e}")
            logger.error(f"{traceback.format_exc()}")
            raise

    def extract_prediction(self, response):
        """Recursively extract prediction from response, handling various formats."""
        if isinstance(response, dict):
            # Direct access to predicted values
            if "predicted_values" in response:
                return response["predicted_values"]
            if "predicted_value" in response:
                return response["predicted_value"]
            # If response has text field that might be JSON
            if "text" in response:
                try:
                    parsed = json.loads(response["text"])
                    return self.extract_prediction(parsed)
                except json.JSONDecodeError:
                    return None
        return None

    def _extract_prediction_sync(self, response): # Synchronous wrapper
        """Synchronous version of extract_prediction for executor."""
        return self.extract_prediction(response)

    async def process_miner_responses(
        self,
        responses: Dict[str, Any],
        current_hour_start: datetime.datetime,
        validator,
    ) -> None:
        """Process responses from miners and add to queue."""
        try:
            if not responses:
                logger.warning("No responses received from miners")
                return

            for hotkey, response in responses.items():
                try:
                    logger.info(f"Raw response from miner {hotkey}: {response}")
                    
                    loop = asyncio.get_event_loop() # Get event loop
                    predicted_value = await loop.run_in_executor(None, self._extract_prediction_sync, response)

                    if predicted_value is None:
                        logger.error(f"No valid prediction found in response from {hotkey}")
                        continue

                    try:
                        predicted_value = float(predicted_value)
                    except (TypeError, ValueError) as e:
                        logger.error(f"Invalid prediction from {hotkey}: {predicted_value}")
                        continue

                    logger.info("=" * 50)
                    logger.info(f"Received prediction from miner:")
                    logger.info(f"Miner Hotkey: {hotkey}")
                    logger.info(f"Predicted Value: {predicted_value}")
                    logger.info(f"Timestamp: {current_hour_start}")
                    logger.info("=" * 50)

                    result = await self.db_manager.fetch_one(
                        """
                        SELECT uid FROM node_table 
                        WHERE hotkey = :miner_hotkey
                        """,
                        {"miner_hotkey": hotkey}
                    )
                    
                    if not result:
                        logger.warning(f"No UID found for hotkey {hotkey}")
                        continue
                    miner_uid = str(result["uid"])
                    logger.info(f"Found miner UID {miner_uid} for hotkey {hotkey}")

                    logger.info(f"Adding prediction to queue for {hotkey} with value {predicted_value}")
                    await self.add_prediction_to_queue(
                        miner_uid=miner_uid,
                        miner_hotkey=hotkey,
                        predicted_value=predicted_value,
                        query_time=current_hour_start,
                        status="pending",
                    )

                except Exception as e:
                    logger.error(f"Error processing response from {hotkey}: {e}")
                    logger.error(traceback.format_exc())
                    continue

        except Exception as e:
            logger.error(f"Error processing miner responses: {e}")
            logger.error(traceback.format_exc())

    async def score_tasks(self, tasks, ground_truth_value, current_time):
        if tasks:
            scored_tasks = []
            for task in tasks:
                try:
                    predicted_value = task["predicted_values"]
                    score = self.scoring_mechanism.calculate_score(
                        predicted_value, ground_truth_value
                    )
                    await self.move_task_to_history(
                        task, ground_truth_value, score, current_time
                    )
                    task["score"] = score  # Add score to task dict
                    scored_tasks.append(task)
                    logger.info(
                        f"Task scored and archived: task_id={task['id']}, score={score}"
                    )
                except Exception as e:
                    logger.error(f"Error processing task {task['id']}: {e}")
                    logger.error(traceback.format_exc())

            current_hour = datetime.datetime.now(datetime.timezone.utc).hour
            await self.build_score_row(current_hour, scored_tasks)
        else:
            logger.info("No predictions to score for the last hour.")

    async def build_score_row(self, current_hour, recent_tasks=None):
        """
        Build a score row from recent tasks and historical data.
        The task_id should be the verification time (when we can score the predictions).

        Args:
            current_hour (datetime): Current hour timestamp (when predictions were made)
            recent_tasks (list, optional): List of recently scored tasks

        Returns:
            dict: Dictionary containing task_name, task_id, and scores array
        """
        try:
            # Convert current_hour to datetime if it's an integer
            if isinstance(current_hour, int):
                current_time = datetime.datetime.now(datetime.timezone.utc)
                prediction_time = current_time.replace(
                    hour=current_hour, minute=0, second=0, microsecond=0
                )
            else:
                prediction_time = current_hour

            # Verification time is 1 hour after prediction time
            verification_time = prediction_time + datetime.timedelta(hours=1)

            # Initialize scores array with NaN values
            scores = [float("nan")] * 256

            # Get mapping of hotkeys to UIDs from node_table
            query = """
            SELECT uid, hotkey FROM node_table 
            """
            miner_mappings = await self.db_manager.fetch_all(query)
            hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}

            # Check historical table for any tasks in this time period
            historical_query = """
            SELECT miner_uid, miner_hotkey, score
            FROM geomagnetic_history
            WHERE query_time = :prediction_time
            """
            historical_tasks = await self.db_manager.fetch_all(
                historical_query,
                {"prediction_time": prediction_time},
            )

            # Process historical tasks
            for task in historical_tasks:
                historical_miner_uid = task["miner_uid"]
                historical_miner_hotkey = task["miner_hotkey"]

                if historical_miner_uid is None or historical_miner_hotkey is None:
                    logger.warning(f"Skipping historical task due to missing UID/Hotkey: {task}")
                    continue

                is_valid_in_metagraph = False
                if self.validator and hasattr(self.validator, 'metagraph') and self.validator.metagraph is not None:
                    # Check if the historical hotkey exists in the current metagraph's nodes dictionary
                    if historical_miner_hotkey in self.validator.metagraph.nodes:
                        logger.info(f"Historical hotkey {historical_miner_hotkey} found in metagraph")
                        # Retrieve the Node object from the metagraph
                        node_in_metagraph = self.validator.metagraph.nodes[historical_miner_hotkey]
                        # Compare the historical UID with the UID from the metagraph node
                        if hasattr(node_in_metagraph, 'node_id') and str(node_in_metagraph.node_id) == str(historical_miner_uid):
                            is_valid_in_metagraph = True
                        else:
                            metagraph_uid_str = getattr(node_in_metagraph, 'node_id', '[UID not found]')
                            logger.warning(f"Metagraph UID mismatch for {historical_miner_hotkey} (Historical Task): Prediction UID {historical_miner_uid}, Metagraph Node UID {metagraph_uid_str}. Skipping score.")
                    else:
                        logger.warning(f"Miner hotkey {historical_miner_hotkey} (Historical Task) not found in current metagraph. Prediction UID {historical_miner_uid}. Skipping score.")
                else:
                    logger.warning("Validator or metagraph not available for validation (historical_tasks). Skipping.")

                if not is_valid_in_metagraph:
                    continue
                
                # If valid, use historical_miner_uid to place the score.
                try:
                    scores[int(historical_miner_uid)] = task["score"]
                except (ValueError, IndexError, TypeError) as e:
                    logger.error(f"Failed to assign score for historical task, UID: {historical_miner_uid}, Score: {task.get('score')}, Error: {e}")


            # Process recent tasks (overwrite historical scores if exists)
            if recent_tasks:
                for task in recent_tasks:
                    recent_miner_uid = task["miner_uid"]
                    recent_miner_hotkey = task["miner_hotkey"]

                    if recent_miner_uid is None or recent_miner_hotkey is None:
                        logger.warning(f"Skipping recent task due to missing UID/Hotkey: {task}")
                        continue

                    is_valid_in_metagraph = False
                    if self.validator and hasattr(self.validator, 'metagraph') and self.validator.metagraph is not None:
                        # Check if the recent hotkey exists in the current metagraph's nodes dictionary
                        if recent_miner_hotkey in self.validator.metagraph.nodes:
                            logger.info(f"Recent hotkey {recent_miner_hotkey} found in metagraph")
                            # Retrieve the Node object from the metagraph
                            node_in_metagraph = self.validator.metagraph.nodes[recent_miner_hotkey]
                            # Compare the recent UID with the UID from the metagraph node
                            if hasattr(node_in_metagraph, 'node_id') and str(node_in_metagraph.node_id) == str(recent_miner_uid):
                                is_valid_in_metagraph = True
                            else:
                                metagraph_uid_str = getattr(node_in_metagraph, 'node_id', '[UID not found]')
                                logger.warning(f"Metagraph UID mismatch for {recent_miner_hotkey} (Recent Task): Prediction UID {recent_miner_uid}, Metagraph Node UID {metagraph_uid_str}. Skipping score.")
                        else:
                            logger.warning(f"Miner hotkey {recent_miner_hotkey} (Recent Task) not found in current metagraph. Prediction UID {recent_miner_uid}. Skipping score.")
                    else:
                        logger.warning("Validator or metagraph not available for validation (recent_tasks). Skipping.")

                    if not is_valid_in_metagraph:
                        continue

                    # If valid, use recent_miner_uid to place the score.
                    try:
                        scores[int(recent_miner_uid)] = task.get("score", float("nan"))
                    except (ValueError, IndexError, TypeError) as e:
                        logger.error(f"Failed to assign score for recent task, UID: {recent_miner_uid}, Score: {task.get('score')}, Error: {e}")

            # Create score row using verification time as task_id
            score_row = {
                "task_name": "geomagnetic",
                "task_id": str(verification_time.timestamp()),
                "score": scores,
                "status": "completed",
            }

            # WRITE operation - use execute for upserting score (handles duplicates)
            await self.db_manager.execute(
                """
                INSERT INTO score_table (task_name, task_id, score, status)
                VALUES (:task_name, :task_id, :score, :status)
                ON CONFLICT (task_name, task_id) DO UPDATE SET
                    score = EXCLUDED.score,
                    status = EXCLUDED.status
                """,
                score_row
            )

            logger.info(
                f"Built score row for predictions at {prediction_time} (verification time {verification_time}) with {len([s for s in scores if not np.isnan(s)])} scores"
            )
            return score_row

        except Exception as e:
            logger.error(f"Error building score row: {e}")
            logger.error(traceback.format_exc())
            return None

    async def recalculate_recent_scores(self, uids: List[int]) -> None:
        """
        Recalculate scores for specified miners over the last 3 days.
        
        Args:
            uids (List[int]): List of miner UIDs to recalculate scores for
            
        Raises:
            ValueError: If UIDs are invalid
            DatabaseError: If there is an error accessing the database
        """
        try:
            # Validate UIDs
            if not all(isinstance(uid, int) and 0 <= uid < 256 for uid in uids):
                raise ValueError("All UIDs must be integers between 0 and 255")

            current_time = datetime.datetime.now(datetime.timezone.utc)
            history_window = current_time - datetime.timedelta(days=3)
            logger.info(f"Recalculating scores for UIDs {uids} from {history_window} to {current_time}")

            # Convert UIDs to strings for database query
            str_uids = [str(uid) for uid in uids]

            # Delete existing predictions
            await self.db_manager.execute(
                """
                DELETE FROM geomagnetic_predictions
                WHERE miner_uid = ANY(:uids)
                """,
                {"uids": str_uids}
            )
            logger.info(f"Successfully deleted predictions for UIDs: {uids}")

            # Delete affected score rows
            await self.db_manager.execute(
                """
                DELETE FROM score_table 
                WHERE task_name = 'geomagnetic'
                AND task_id::float >= :start_timestamp
                AND task_id::float <= :end_timestamp
                """,
                {
                    "start_timestamp": history_window.timestamp(),
                    "end_timestamp": current_time.timestamp(),
                }
            )
            logger.info(f"Successfully deleted scores for time window")

            # Get history data
            history_results = await self.db_manager.fetch_all(
                """
                SELECT 
                    miner_hotkey,
                    miner_uid,
                    query_time,
                    score
                FROM geomagnetic_history
                WHERE query_time >= :history_window
                AND query_time <= :current_time
                AND miner_uid = ANY(:uids)
                ORDER BY query_time ASC
                LIMIT 10000
                """,
                {
                    "history_window": history_window,
                    "current_time": current_time,
                    "uids": str_uids
                }
            )

            if not history_results:
                logger.warning(f"No historical data found for UIDs {uids} in window {history_window} to {current_time}")
                return

            # Log if we fetched a large dataset
            if len(history_results) > 1000:
                logger.warning(f"Large geomagnetic history dataset: {len(history_results)} records for UIDs {uids}")

            # Get current miner mappings
            miner_mappings = await self.db_manager.fetch_all(
                """
                SELECT uid, hotkey 
                FROM node_table 
                WHERE hotkey IS NOT NULL
                """
            )
            hotkey_to_uid: Dict[str, int] = {row["hotkey"]: row["uid"] for row in miner_mappings}

            # Group records by hour with memory-efficient processing
            hourly_records: Dict[datetime.datetime, List[Dict[str, Any]]] = {}
            processed_count = 0
            
            try:
                for record in history_results:
                    hour_key = record["query_time"].replace(
                        minute=0, second=0, microsecond=0
                    )
                    if hour_key not in hourly_records:
                        hourly_records[hour_key] = []
                    hourly_records[hour_key].append(record)
                    processed_count += 1
                    
                    # Yield control periodically for large datasets
                    if processed_count % 1000 == 0:
                        await asyncio.sleep(0)

                # AGGRESSIVE cleanup of large datasets
                del history_results
                del miner_mappings
                
                # Force comprehensive cleanup for large datasets  
                if processed_count > 1000:
                    import gc
                    collected = gc.collect()
                    logger.info(f"Geomagnetic history processing: GC collected {collected} objects after processing {processed_count} records")
                
                # Additional cleanup: clear any large intermediate data structures
                try:
                    # Clear hourly_records dict to release memory from grouped records
                    if processed_count > 500:  # Only for moderate+ datasets
                        logger.info(f"Clearing hourly_records dict with {len(hourly_records)} hour keys")
                        hourly_records.clear()
                except Exception as hourly_cleanup_err:
                    logger.debug(f"Error during hourly records cleanup: {hourly_cleanup_err}")

            except Exception as processing_error:
                logger.error(f"Error processing historical records: {processing_error}")
                # Clean up on error
                try:
                    del history_results
                    del miner_mappings
                except:
                    pass
                raise

            # Process each hour
            for hour, records in hourly_records.items():
                try:
                    scores: List[float] = [float("nan")] * 256

                    # Calculate scores for this hour
                    for record in records:
                        try:
                            miner_hotkey = record["miner_hotkey"]
                            if miner_hotkey in hotkey_to_uid:
                                uid = hotkey_to_uid[miner_hotkey]
                                if record["score"] is not None:
                                    scores[uid] = float(record["score"])
                        except (ValueError, TypeError) as e:
                            logger.error(f"Error processing record for miner {record.get('miner_hotkey')}: {e}")
                            continue

                    # Insert score row
                    score_row = {
                        "task_name": "geomagnetic",
                        "task_id": str(hour.timestamp()),
                        "score": scores,
                        "status": "completed",
                    }

                    await self.db_manager.execute(
                        """
                        INSERT INTO score_table (task_name, task_id, score, status)
                        VALUES (:task_name, :task_id, :score, :status)
                        ON CONFLICT (task_name, task_id) DO UPDATE SET
                            score = EXCLUDED.score,
                            status = EXCLUDED.status
                        """,
                        score_row
                    )
                    logger.info(f"Recalculated and inserted score row for hour {hour}")

                except Exception as e:
                    logger.error(f"Error processing hour {hour}: {e}")
                    logger.error(traceback.format_exc())
                    continue

            logger.info(f"Completed recalculation of scores for UIDs: {uids} over 3-day window")

        except ValueError as e:
            logger.error(f"Invalid UIDs in recalculate_recent_scores: {e}")
            raise
        except Exception as e:
            logger.error(f"Error in recalculate_recent_scores: {e}")
            logger.error(traceback.format_exc())
            raise  # Re-raise to trigger error handling in deregistration loop

    async def cleanup_resources(self):
        """Clean up any resources used by the task during recovery."""
        try:
            # Stop the background retry worker
            await self._stop_pending_retry_worker()
            
            await self.db_manager.execute(
                """
                UPDATE geomagnetic_predictions 
                SET status = 'pending'
                WHERE status = 'processing'
                """
            )
            logger.info("Reset in-progress prediction statuses")
            
            await self.db_manager.execute(
                """
                DELETE FROM score_table 
                WHERE task_name = 'geomagnetic' 
                AND status = 'processing'
                """
            )
            logger.info("Cleaned up incomplete scoring operations")
            logger.info("Completed geomagnetic task cleanup")
            
        except Exception as e:
            logger.error(f"Error during geomagnetic task cleanup: {e}")
            logger.error(traceback.format_exc())
            raise

    async def _fetch_ground_truth_with_retry(self):
        """
        Fetches the ground truth DST value for the current UTC hour with retry logic.

        Returns:
            int: The real-time DST value, or None if fetching fails after retries.
        """
        try:
            # Get the current UTC time
            current_time = datetime.datetime.now(datetime.timezone.utc)
            
            if self.test_mode:
                logger.info(f"TEST MODE: Fetching ground truth for current UTC hour: {current_time.hour}")
            else:
                logger.info(f"Fetching ground truth for UTC hour: {current_time.hour}")

            # Fetch the most recent geomagnetic data with retry logic
            for attempt in range(1, 4):
                try:
                    result = await get_latest_geomag_data(include_historical=False)
                    
                    # Handle the result based on expected format (should be 2 values)
                    if len(result) == 2:
                        timestamp, dst_value = result
                    else:
                        logger.error(f"Unexpected number of values returned from get_latest_geomag_data: {len(result)} (expected 2)")
                        logger.error(f"Result: {result}")
                        continue
                    
                    if timestamp == "N/A" or dst_value == "N/A":
                        logger.warning("No ground truth data available for the current hour.")
                        continue

                    logger.info(f"Ground truth value for hour {current_time.hour}: {dst_value}")
                    return dst_value

                except Exception as e:
                    logger.error(f"Error fetching ground truth, attempt {attempt}: {e}")
                    logger.error(f"{traceback.format_exc()}")
                    await asyncio.sleep(attempt * 10)  # Wait between attempts

            logger.warning("All ground truth fetching attempts failed. No ground truth value returned.")
            return None

        except Exception as e:
            logger.error(f"Error fetching ground truth: {e}")
            logger.error(f"{traceback.format_exc()}")
            return None

    async def _retry_pending_tasks(self, validator, current_time):
        """
        Retries scoring for older pending tasks from the last 24 hours.
        Groups tasks by hour and attempts to score them properly.

        Args:
            validator: Validator instance
            current_time: Current datetime
        """
        try:
            # Look for pending tasks from the last 24 hours
            start_time = current_time - datetime.timedelta(hours=24)
            
            logger.info(f"Checking for pending tasks from {start_time} to {current_time}")
            
            # Get all pending tasks from the last 24 hours
            pending_tasks_query = await self.db_manager.fetch_all(
                """
                SELECT 
                    id,
                    miner_uid,
                    miner_hotkey,
                    predicted_value,
                    query_time
                FROM geomagnetic_predictions
                WHERE query_time >= :start_time 
                AND query_time < :end_time 
                AND status = 'pending'
                ORDER BY query_time ASC
                """,
                {
                    "start_time": start_time,
                    "end_time": current_time
                }
            )

            if not pending_tasks_query:
                logger.debug("No pending tasks found for retry in the last 24 hours")
                return

            logger.info(f"Found {len(pending_tasks_query)} pending tasks to retry")

            # Convert to task format and group by hour
            hourly_pending_tasks = {}
            for row in pending_tasks_query:
                task = {
                    "id": row["id"],
                    "miner_uid": row["miner_uid"],
                    "miner_hotkey": row["miner_hotkey"],
                    "predicted_values": row["predicted_value"],
                    "query_time": row["query_time"],
                    "timestamp": row["query_time"],
                }
                
                # Group by hour
                hour_key = row["query_time"].replace(minute=0, second=0, microsecond=0)
                if hour_key not in hourly_pending_tasks:
                    hourly_pending_tasks[hour_key] = []
                hourly_pending_tasks[hour_key].append(task)

            # Process each hour's pending tasks
            scored_any_tasks = False
            for hour_key, tasks in hourly_pending_tasks.items():
                try:
                    logger.info(f"Attempting to score {len(tasks)} pending tasks from hour {hour_key}")
                    
                    # 🔒 SECURITY: Check ground truth availability before attempting to fetch
                    if not await self._check_ground_truth_availability(hour_key):
                        logger.debug(f"🔒 SECURITY: Ground truth not yet available for retry tasks from hour {hour_key}, will retry later")
                        continue
                    
                    # SECURITY FIX: Use time-specific ground truth fetching instead of latest data
                    ground_truth_value = await self._fetch_ground_truth_for_time(hour_key, max_attempts=2)
                    if ground_truth_value is None:
                        logger.warning(f"Could not fetch time-specific ground truth for pending tasks from hour {hour_key}, will retry later")
                        continue

                    logger.info(f"✅ SECURITY: Successfully got time-specific ground truth value {ground_truth_value} for pending tasks from hour {hour_key}")

                    # Score all tasks for this hour
                    scored_tasks = []
                    for task in tasks:
                        try:
                            # Check if this task should be retried
                            if not await self._should_retry_task(task):
                                logger.debug(f"Skipping task {task['id']} - retry conditions not met")
                                continue
                            
                            # Update retry count
                            await self._update_retry_count(task["id"])
                            
                            # Validate task is still in metagraph if validator is available
                            if validator and hasattr(validator, 'metagraph') and validator.metagraph is not None:
                                if task["miner_hotkey"] not in validator.metagraph.nodes:
                                    logger.warning(f"Miner {task['miner_hotkey']} no longer in metagraph, deleting task {task['id']}")
                                    try:
                                        # Delete the task from the database since miner is no longer valid
                                        await self.db_manager.execute(
                                            "DELETE FROM geomagnetic_predictions WHERE id = :task_id",
                                            {"task_id": task["id"]}
                                        )
                                        logger.info(f"Successfully deleted orphaned task {task['id']} for removed miner {task['miner_hotkey']}")
                                    except Exception as e:
                                        logger.error(f"Failed to delete orphaned task {task['id']}: {e}")
                                    continue

                            # Use validator's basemodel_evaluator to get the baseline score for comparison
                            baseline_score = None
                            if validator and hasattr(validator, 'basemodel_evaluator'):
                                try:
                                    task_timestamp = task.get("timestamp", task.get("query_time"))
                                    if task_timestamp:
                                        if isinstance(task_timestamp, datetime.datetime):
                                            if task_timestamp.tzinfo is not None:
                                                task_timestamp_utc = task_timestamp.astimezone(datetime.timezone.utc)
                                            else:
                                                task_timestamp_utc = task_timestamp.replace(tzinfo=datetime.timezone.utc)
                                                
                                            task_id = str(task_timestamp_utc.timestamp())
                                        else:
                                            task_id = str(task_timestamp)
                                        
                                        validator.basemodel_evaluator.test_mode = self.test_mode
                                        baseline_score = await validator.basemodel_evaluator.score_geo_baseline(
                                            task_id=task_id,
                                            ground_truth=ground_truth_value
                                        )
                                        if baseline_score is not None:
                                            logger.info(f"Retrieved baseline score for retry task_id {task_id}: {baseline_score:.4f}")
                                except Exception as e:
                                    logger.warning(f"Error retrieving baseline score for retry task: {e}")

                            # Calculate score
                            score = self.scoring_mechanism.calculate_score(
                                task["predicted_values"], ground_truth_value
                            )

                            # Apply baseline comparison if available
                            if baseline_score is not None:
                                base_epsilon = 0.005
                                theoretical_max = 0.99
                                
                                if baseline_score > theoretical_max - 0.10:
                                    epsilon = 0.002
                                else:
                                    epsilon = base_epsilon
                                
                                if score <= baseline_score + epsilon:
                                    logger.info(f"Retry task score zeroed - insufficient improvement: {score:.4f} vs baseline {baseline_score:.4f}")
                                    score = 0
                                else:
                                    logger.info(f"Retry task score valid - exceeds baseline by {score - baseline_score:.4f}")

                            # Move task to history
                            await self.move_task_to_history(
                                task, ground_truth_value, score, current_time
                            )

                            # Add score to task for building score row
                            task["score"] = score
                            scored_tasks.append(task)
                            logger.info(f"Successfully retried and scored task {task['id']} with score {score}")

                        except Exception as e:
                            logger.error(f"Error processing retry task {task['id']}: {e}")
                            continue

                    # Build score row for this hour if we scored any tasks
                    if scored_tasks:
                        await self.build_score_row(hour_key, scored_tasks)
                        logger.info(f"Built score row for {len(scored_tasks)} retried tasks from hour {hour_key}")
                        scored_any_tasks = True

                except Exception as e:
                    logger.error(f"Error processing pending tasks for hour {hour_key}: {e}")
                    logger.error(traceback.format_exc())
                    continue

            if scored_any_tasks:
                logger.info("Successfully scored some pending tasks from previous hours")
            else:
                logger.info("No pending tasks could be scored in this retry attempt")

        except Exception as e:
            logger.error(f"Error in _retry_pending_tasks: {e}")
            logger.error(traceback.format_exc())

    async def _comprehensive_pending_cleanup(self, validator):
        """
        Performs a more comprehensive cleanup of old pending tasks.

        Args:
            validator: Validator instance
        """
        try:
            # Get all pending tasks from the database
            pending_tasks_query = await self.db_manager.fetch_all(
                """
                SELECT 
                    id,
                    miner_uid,
                    miner_hotkey,
                    predicted_value,
                    query_time
                FROM geomagnetic_predictions
                WHERE status = 'pending'
                """,
            )

            if not pending_tasks_query:
                logger.info("No pending tasks found for comprehensive cleanup")
                return

            logger.info(f"Found {len(pending_tasks_query)} pending tasks to cleanup")
            
            # Track cleanup statistics
            skipped_too_young = 0
            deleted_orphaned = 0
            scored_ready = 0

            # Process each pending task
            for row in pending_tasks_query:
                task = {
                    "id": row["id"],
                    "miner_uid": row["miner_uid"],
                    "miner_hotkey": row["miner_hotkey"],
                    "predicted_values": row["predicted_value"],
                    "query_time": row["query_time"],
                    "timestamp": row["query_time"],
                }
                
                try:
                    # Validate task is still in metagraph if validator is available
                    if validator and hasattr(validator, 'metagraph') and validator.metagraph is not None:
                        if task["miner_hotkey"] not in validator.metagraph.nodes:
                            logger.warning(f"Miner {task['miner_hotkey']} no longer in metagraph, deleting task {task['id']}")
                            try:
                                # Delete the task from the database since miner is no longer valid
                                await self.db_manager.execute(
                                    "DELETE FROM geomagnetic_predictions WHERE id = :task_id",
                                    {"task_id": task["id"]}
                                )
                                logger.info(f"Successfully deleted orphaned task {task['id']} for removed miner {task['miner_hotkey']}")
                                deleted_orphaned += 1
                            except Exception as e:
                                logger.error(f"Failed to delete orphaned task {task['id']}: {e}")
                            continue

                    # Try to get actual ground truth for this task's time period
                    task_hour = task["query_time"].replace(minute=0, second=0, microsecond=0)
                    
                    # 🔒 SECURITY: Check ground truth availability before attempting to fetch
                    if not await self._check_ground_truth_availability(task_hour):
                        # If ground truth isn't available due to security timing, SKIP this task
                        # It should remain pending until the 90-minute window passes
                        logger.info(f"🔒 SECURITY: Ground truth not yet available for cleanup task {task['id']} from {task_hour}, keeping task pending until security window passes")
                        skipped_too_young += 1
                        continue  # Skip this task - don't score it prematurely
                    else:
                        # Security check passed, attempt to fetch ground truth
                        ground_truth_value = await self._fetch_ground_truth_for_time(task_hour, max_attempts=1)
                        
                        if ground_truth_value is None:
                            # If ground truth isn't available due to data issues, assign a default low score
                            logger.warning(f"No ground truth data available for cleanup task {task['id']} from {task_hour}, assigning default score of 0")
                            ground_truth_value = 0.0  # Default fallback for data unavailability
                            score = 0.0  # Default score when no ground truth data available
                        else:
                            # Use validator's basemodel_evaluator to get the baseline score for comparison
                            baseline_score = None
                            if validator and hasattr(validator, 'basemodel_evaluator'):
                                try:
                                    task_timestamp = task.get("timestamp", task.get("query_time"))
                                    if task_timestamp:
                                        if isinstance(task_timestamp, datetime.datetime):
                                            if task_timestamp.tzinfo is not None:
                                                task_timestamp_utc = task_timestamp.astimezone(datetime.timezone.utc)
                                            else:
                                                task_timestamp_utc = task_timestamp.replace(tzinfo=datetime.timezone.utc)
                                                
                                            task_id = str(task_timestamp_utc.timestamp())
                                        else:
                                            task_id = str(task_timestamp)
                                        
                                        validator.basemodel_evaluator.test_mode = self.test_mode
                                        baseline_score = await validator.basemodel_evaluator.score_geo_baseline(
                                            task_id=task_id,
                                            ground_truth=ground_truth_value  # FIXED: Use actual ground truth, not prediction
                                        )
                                        if baseline_score is not None:
                                            logger.info(f"Retrieved baseline score for cleanup task_id {task_id}: {baseline_score:.4f}")
                                except Exception as e:
                                    logger.warning(f"Error retrieving baseline score for cleanup task: {e}")

                            # Calculate score using actual ground truth
                            score = self.scoring_mechanism.calculate_score(
                                task["predicted_values"], ground_truth_value  # FIXED: Use actual ground truth
                            )

                            # Apply baseline comparison if available (same logic as other scoring methods)
                            if baseline_score is not None:
                                base_epsilon = 0.005
                                theoretical_max = 0.99
                                
                                if baseline_score > theoretical_max - 0.10:
                                    epsilon = 0.002
                                else:
                                    epsilon = base_epsilon
                                
                                if score <= baseline_score + epsilon:
                                    logger.debug(f"Cleanup task score zeroed - insufficient improvement: {score:.4f} vs baseline {baseline_score:.4f}")
                                    score = 0
                                else:
                                    logger.debug(f"Cleanup task score valid - exceeds baseline by {score - baseline_score:.4f}")

                    # Move task to history with proper ground truth
                    await self.move_task_to_history(
                        task, ground_truth_value, score, datetime.datetime.now(datetime.timezone.utc)
                    )
                    scored_ready += 1

                except Exception as e:
                    logger.error(f"Error processing cleanup task {task['id']}: {e}")
                    continue

            # Log cleanup summary
            logger.info(f"Completed comprehensive cleanup: {scored_ready} tasks scored, {skipped_too_young} tasks kept pending (too young), {deleted_orphaned} orphaned tasks deleted")
            
            if skipped_too_young > 0:
                logger.info(f"🔒 SECURITY: {skipped_too_young} tasks kept pending until 90-minute security window passes")
            if scored_ready > 0:
                logger.info(f"✅ Successfully scored {scored_ready} tasks with available ground truth")
            if deleted_orphaned > 0:
                logger.info(f"🧹 Cleaned up {deleted_orphaned} orphaned tasks from miners no longer in metagraph")

        except Exception as e:
            logger.error(f"Error in _comprehensive_pending_cleanup: {e}")
            logger.error(traceback.format_exc())

    async def _check_ground_truth_availability(self, target_time: datetime.datetime) -> bool:
        """
        Enhanced security check for ground truth data availability with temporal separation enforcement.
        
        🔒 SECURITY: This function enforces the same temporal separation requirements
        as _fetch_ground_truth_for_time to prevent premature ground truth access.
        
        Args:
            target_time: The datetime to check ground truth availability for
            
        Returns:
            bool: True if ground truth data meets security requirements and appears available, False otherwise
        """
        try:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            target_hour = target_time.replace(minute=0, second=0, microsecond=0)
            time_since_target = current_time - target_hour
            
            # 🔒 SECURITY: Apply same temporal separation requirements as fetch function
            if not self.test_mode:
                # Production: Require at least 90 minutes for data stability and separation
                min_delay_seconds = 90 * 60  # 90 minutes
                security_message = "90 minutes for production data stability"
            else:
                # Test mode: Require at least 30 minutes for basic separation
                min_delay_seconds = 30 * 60  # 30 minutes  
                security_message = "30 minutes for test mode"
            
            # 🔒 SECURITY: Enforce minimum delay requirement
            if time_since_target.total_seconds() < min_delay_seconds:
                logger.debug(f"🔒 SECURITY: Ground truth not available for {target_hour} - only {time_since_target.total_seconds()/60:.1f} minutes have passed, need {min_delay_seconds/60:.0f} minutes ({security_message})")
                return False
            
            # 🔒 SECURITY: Prevent checking for future times
            if time_since_target.total_seconds() < 0:
                logger.debug(f"🔒 SECURITY: Cannot check ground truth availability for future time {target_hour}")
                return False
            
            # 🔒 SECURITY: Limit maximum lookback to prevent excessive historical queries
            max_lookback_hours = 168  # 7 days
            if time_since_target.total_seconds() > max_lookback_hours * 3600:
                logger.debug(f"🔒 SECURITY: Target hour {target_hour} is too old ({time_since_target.total_seconds()/3600:.1f} hours ago, max {max_lookback_hours} hours)")
                return False
            
            # If more than 6 hours have passed, we should definitely have stable data
            if time_since_target.total_seconds() > 21600:  # 6 hours
                logger.debug(f"🔒 SECURITY: Ground truth should definitely be available for {target_hour} (data is {time_since_target.total_seconds()/3600:.1f} hours old)")
                return True
            
            # For times between our minimum delay and 6 hours ago, do a quick check
            # We'll try a single attempt to fetch historical data without retries
            try:
                result = await get_latest_geomag_data(include_historical=True)
                
                if len(result) == 3:
                    latest_timestamp, latest_dst_value, historical_data = result
                    if latest_timestamp != "N/A" and latest_dst_value != "N/A" and historical_data is not None:
                        # Check if historical data contains our target hour
                        target_data = historical_data[historical_data["timestamp"] == target_hour]
                        if not target_data.empty:
                            logger.debug(f"✅ SECURITY: Ground truth data found for {target_hour} in historical data")
                            return True
                        else:
                            logger.debug(f"🔒 SECURITY: Target hour {target_hour} not found in historical data range: {historical_data['timestamp'].min()} to {historical_data['timestamp'].max()}")
                            return False
                
                logger.debug(f"🔒 SECURITY: Ground truth check returned N/A values for {target_hour}")
                return False
                
            except Exception as e:
                logger.debug(f"🔒 SECURITY: Error during ground truth availability check for {target_hour}: {e}")
                # If we can't check and meet minimum delay, assume it might be available
                return time_since_target.total_seconds() >= min_delay_seconds
                
        except Exception as e:
            logger.error(f"Error in _check_ground_truth_availability: {e}")
            # Default to False for security - don't assume availability if we can't verify
            return False

    async def _fetch_ground_truth_for_time(self, target_time: datetime.datetime, max_attempts: int = 3):
        """
        Fetches ground truth data for a specific time period with enhanced security controls.
        
        🔒 SECURITY: This function enforces strict temporal separation and ensures
        ground truth data is only fetched when sufficient time has passed for
        data stability and proper separation from miner query data.
        
        Args:
            target_time: The datetime to fetch ground truth for (must be specific hour)
            max_attempts: Maximum number of retry attempts
            
        Returns:
            float: The ground truth DST value for the target hour, or None if fetching fails
        """
        try:
            # Align target_time to the start of the hour for precise matching
            target_hour = target_time.replace(minute=0, second=0, microsecond=0)
            current_time = datetime.datetime.now(datetime.timezone.utc)
            time_diff = current_time - target_hour
            
            # 🔒 SECURITY: Enhanced temporal separation requirements
            if not self.test_mode:
                # Production: Require at least 90 minutes for data stability and separation
                min_delay_seconds = 90 * 60  # 90 minutes
                security_message = "90 minutes for production data stability"
            else:
                # Test mode: Require at least 30 minutes for basic separation
                min_delay_seconds = 30 * 60  # 30 minutes  
                security_message = "30 minutes for test mode"
            
            if time_diff.total_seconds() < min_delay_seconds:
                logger.warning(f"🔒 SECURITY: Refusing to fetch ground truth for {target_hour}")
                logger.warning(f"🔒 SECURITY: Only {time_diff.total_seconds()/60:.1f} minutes have passed, need {min_delay_seconds/60:.0f} minutes ({security_message})")
                logger.warning(f"🔒 SECURITY: This prevents miners from accessing ground truth data during prediction windows")
                return None
            
            # 🔒 SECURITY: Prevent fetching ground truth for target times that are too far in the future
            if time_diff.total_seconds() < 0:
                logger.error(f"🔒 SECURITY: Cannot fetch ground truth for future time {target_hour} (current time: {current_time})")
                return None
            
            # 🔒 SECURITY: Limit maximum lookback to prevent excessive historical queries
            max_lookback_hours = 168  # 7 days
            if time_diff.total_seconds() > max_lookback_hours * 3600:
                logger.warning(f"🔒 SECURITY: Target hour {target_hour} is too old ({time_diff.total_seconds()/3600:.1f} hours ago, max {max_lookback_hours} hours)")
                return None
            
            logger.info(f"🔒 SECURITY: Fetching ground truth for {target_hour} (safe: {time_diff.total_seconds()/3600:.1f} hours ago, {time_diff.total_seconds()/60:.0f} minutes)")

            # Fetch historical data to find the specific time period
            for attempt in range(1, max_attempts + 1):
                try:
                    # Get historical data for the current month
                    result = await get_latest_geomag_data(include_historical=True)
                    
                    # Handle the result based on expected format (should be 3 values when include_historical=True)
                    if len(result) == 3:
                        latest_timestamp, latest_dst_value, historical_data = result
                    else:
                        logger.warning(f"Unexpected number of values returned from get_latest_geomag_data: {len(result)} (expected 3 with historical)")
                        logger.warning(f"Result: {result}")
                        if attempt < max_attempts:
                            await asyncio.sleep(attempt * 5)
                        continue
                    
                    if latest_timestamp == "N/A" or latest_dst_value == "N/A" or historical_data is None:
                        logger.warning(f"No historical geomagnetic data available (attempt {attempt}/{max_attempts})")
                        if attempt < max_attempts:
                            await asyncio.sleep(attempt * 5)
                        continue

                    # 🔒 SECURITY: Verify historical data meets our temporal requirements
                    historical_data_max_time = historical_data["timestamp"].max()
                    if historical_data_max_time < target_hour:
                        logger.warning(f"🔒 SECURITY: Historical data ends at {historical_data_max_time}, cannot provide ground truth for {target_hour}")
                        if attempt < max_attempts:
                            await asyncio.sleep(attempt * 10)
                        continue

                    # Filter historical data for the specific target hour
                    target_data = historical_data[historical_data["timestamp"] == target_hour]
                    
                    if target_data.empty:
                        logger.warning(f"No ground truth data found for specific hour {target_hour} (attempt {attempt}/{max_attempts})")
                        logger.info(f"Available data range: {historical_data['timestamp'].min()} to {historical_data['timestamp'].max()}")
                        if attempt < max_attempts:
                            await asyncio.sleep(attempt * 5)
                        continue

                    # Extract the DST value for the target hour
                    ground_truth_value = float(target_data.iloc[0]["Dst"])
                    
                    # 🔒 SECURITY: Final validation of ground truth value
                    if abs(ground_truth_value) > 1000:  # Sanity check for reasonable DST values
                        logger.warning(f"🔒 SECURITY: Ground truth value {ground_truth_value} seems unreasonable for DST, skipping")
                        if attempt < max_attempts:
                            await asyncio.sleep(attempt * 5)
                        continue
                    
                    logger.info(f"✅ SECURITY: Successfully fetched time-specific ground truth for {target_hour}: {ground_truth_value}")
                    logger.info(f"✅ SECURITY: Ground truth is {time_diff.total_seconds()/3600:.1f} hours old, ensuring proper temporal separation")
                    return ground_truth_value

                except Exception as e:
                    logger.warning(f"Error fetching time-specific ground truth for {target_hour}, attempt {attempt}/{max_attempts}: {e}")
                    if attempt < max_attempts:
                        await asyncio.sleep(attempt * 5)

            logger.warning(f"❌ SECURITY: All attempts failed to fetch ground truth for specific hour {target_hour}")
            logger.warning(f"❌ SECURITY: This ensures no compromised scoring occurs")
            return None

        except Exception as e:
            logger.error(f"Error fetching time-specific ground truth for {target_time}: {e}")
            logger.error(f"{traceback.format_exc()}")
            return None

    async def _start_pending_retry_worker(self, validator):
        """
        Starts a background worker that checks for pending tasks every 10 minutes
        and attempts to score them if ground truth becomes available.
        """
        if self.pending_retry_worker_running:
            logger.info("Pending retry worker is already running")
            return
        
        self.pending_retry_worker_running = True
        self.pending_retry_worker_task = asyncio.create_task(self._pending_retry_worker_loop(validator))
        logger.info("Started pending retry worker (checks every 10 minutes)")

    async def _stop_pending_retry_worker(self):
        """Stops the background pending retry worker."""
        if not self.pending_retry_worker_running:
            return
        
        self.pending_retry_worker_running = False
        if self.pending_retry_worker_task:
            self.pending_retry_worker_task.cancel()
            try:
                await self.pending_retry_worker_task
            except asyncio.CancelledError:
                pass
            self.pending_retry_worker_task = None
        logger.info("Stopped pending retry worker")

    async def _pending_retry_worker_loop(self, validator):
        """
        Background worker loop that checks for pending tasks every 10 minutes
        and attempts to score them if ground truth becomes available.
        """
        # Register this worker for global memory cleanup coordination
        try:
            from gaia.utils.global_memory_manager import register_thread_cleanup
            
            def cleanup_geomag_caches():
                # Clear any caches that accumulate during geomagnetic retry processing
                import gc
                collected = gc.collect()
                logger.debug(f"[GeomagRetryWorker] Performed cleanup, collected {collected} objects")
            
            register_thread_cleanup("geomagnetic_retry_worker", cleanup_geomag_caches)
            logger.debug("[GeomagRetryWorker] Registered for global memory cleanup")
        except Exception as e:
            logger.debug(f"[GeomagRetryWorker] Failed to register cleanup: {e}")
        
        retry_interval = 600 if not self.test_mode else 60  # 10 minutes in production, 1 minute in test mode
        
        while self.pending_retry_worker_running:
            try:
                await asyncio.sleep(retry_interval)
                
                if not self.pending_retry_worker_running:
                    break
                
                logger.info("Pending retry worker: Checking for tasks that can now be scored...")
                await self._intelligent_retry_pending_tasks(validator)
                
            except asyncio.CancelledError:
                logger.info("Pending retry worker cancelled")
                break
            except Exception as e:
                logger.error(f"Error in pending retry worker: {e}")
                logger.error(traceback.format_exc())
                # Continue running even if there's an error
                await asyncio.sleep(60)  # Wait 1 minute before retrying

    async def _intelligent_retry_pending_tasks(self, validator):
        """
        Intelligently retry pending tasks by first checking if ground truth is available
        before attempting to fetch it. Groups tasks by hour and processes efficiently.
        """
        try:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            
            # Look for pending tasks from the last 72 hours (extended window)
            start_time = current_time - datetime.timedelta(hours=72)
            
            logger.debug(f"Checking for pending tasks from {start_time} to {current_time}")
            
            # Get all pending tasks, grouped by hour
            pending_tasks_query = await self.db_manager.fetch_all(
                """
                SELECT 
                    id,
                    miner_uid,
                    miner_hotkey,
                    predicted_value,
                    query_time,
                    DATE_TRUNC('hour', query_time) as hour_bucket
                FROM geomagnetic_predictions
                WHERE query_time >= :start_time 
                AND query_time < :end_time 
                AND status = 'pending'
                ORDER BY query_time ASC
                """,
                {
                    "start_time": start_time,
                    "end_time": current_time
                }
            )

            if not pending_tasks_query:
                logger.debug("No pending tasks found for intelligent retry")
                return

            logger.info(f"Found {len(pending_tasks_query)} pending tasks to check for retry")

            # Group tasks by hour
            hourly_pending_tasks = {}
            for row in pending_tasks_query:
                hour_key = row["hour_bucket"]
                if hour_key not in hourly_pending_tasks:
                    hourly_pending_tasks[hour_key] = []
                
                task = {
                    "id": row["id"],
                    "miner_uid": row["miner_uid"],
                    "miner_hotkey": row["miner_hotkey"],
                    "predicted_values": row["predicted_value"],
                    "query_time": row["query_time"],
                    "timestamp": row["query_time"],
                }
                hourly_pending_tasks[hour_key].append(task)

            # Process each hour's pending tasks
            scored_any_tasks = False
            for hour_key, tasks in hourly_pending_tasks.items():
                try:
                    # First, check if ground truth is likely available for this hour
                    if not await self._check_ground_truth_availability(hour_key):
                        logger.debug(f"Ground truth not yet available for hour {hour_key}, skipping {len(tasks)} tasks")
                        continue
                    
                    logger.info(f"Ground truth appears available for hour {hour_key}, attempting to score {len(tasks)} tasks")
                    
                    # Try to fetch ground truth for this time period
                    ground_truth_value = await self._fetch_ground_truth_for_time(hour_key, max_attempts=2)
                    if ground_truth_value is None:
                        logger.warning(f"Could not fetch ground truth for hour {hour_key} despite availability check")
                        continue

                    logger.info(f"Successfully got ground truth value {ground_truth_value} for hour {hour_key}")

                    # Score all tasks for this hour
                    scored_tasks = []
                    for task in tasks:
                        try:
                            # Validate task is still in metagraph if validator is available
                            if validator and hasattr(validator, 'metagraph') and validator.metagraph is not None:
                                if task["miner_hotkey"] not in validator.metagraph.nodes:
                                    logger.warning(f"Miner {task['miner_hotkey']} no longer in metagraph, deleting task {task['id']}")
                                    try:
                                        # Delete the task from the database since miner is no longer valid
                                        await self.db_manager.execute(
                                            "DELETE FROM geomagnetic_predictions WHERE id = :task_id",
                                            {"task_id": task["id"]}
                                        )
                                        logger.info(f"Successfully deleted orphaned task {task['id']} for removed miner {task['miner_hotkey']}")
                                    except Exception as e:
                                        logger.error(f"Failed to delete orphaned task {task['id']}: {e}")
                                    continue

                            # Get baseline score for comparison
                            baseline_score = None
                            if validator and hasattr(validator, 'basemodel_evaluator'):
                                try:
                                    task_timestamp = task.get("timestamp", task.get("query_time"))
                                    if task_timestamp:
                                        if isinstance(task_timestamp, datetime.datetime):
                                            if task_timestamp.tzinfo is not None:
                                                task_timestamp_utc = task_timestamp.astimezone(datetime.timezone.utc)
                                            else:
                                                task_timestamp_utc = task_timestamp.replace(tzinfo=datetime.timezone.utc)
                                                
                                            task_id = str(task_timestamp_utc.timestamp())
                                        else:
                                            task_id = str(task_timestamp)
                                        
                                        validator.basemodel_evaluator.test_mode = self.test_mode
                                        baseline_score = await validator.basemodel_evaluator.score_geo_baseline(
                                            task_id=task_id,
                                            ground_truth=ground_truth_value
                                        )
                                        if baseline_score is not None:
                                            logger.debug(f"Retrieved baseline score for intelligent retry task_id {task_id}: {baseline_score:.4f}")
                                except Exception as e:
                                    logger.debug(f"Error retrieving baseline score for intelligent retry task: {e}")

                            # Calculate score
                            score = self.scoring_mechanism.calculate_score(
                                task["predicted_values"], ground_truth_value
                            )

                            # Apply baseline comparison if available
                            if baseline_score is not None:
                                base_epsilon = 0.005
                                theoretical_max = 0.99
                                
                                if baseline_score > theoretical_max - 0.10:
                                    epsilon = 0.002
                                else:
                                    epsilon = base_epsilon
                                
                                if score <= baseline_score + epsilon:
                                    logger.debug(f"Intelligent retry task score zeroed - insufficient improvement: {score:.4f} vs baseline {baseline_score:.4f}")
                                    score = 0
                                else:
                                    logger.debug(f"Intelligent retry task score valid - exceeds baseline by {score - baseline_score:.4f}")

                            # Move task to history
                            await self.move_task_to_history(
                                task, ground_truth_value, score, current_time
                            )

                            # Add score to task for building score row
                            task["score"] = score
                            scored_tasks.append(task)
                            logger.info(f"Intelligent retry: Successfully scored task {task['id']} with score {score}")

                        except Exception as e:
                            logger.error(f"Error processing intelligent retry task {task['id']}: {e}")
                            continue

                    # Build score row for this hour if we scored any tasks
                    if scored_tasks:
                        await self.build_score_row(hour_key, scored_tasks)
                        logger.info(f"Built score row for {len(scored_tasks)} intelligently retried tasks from hour {hour_key}")
                        scored_any_tasks = True

                except Exception as e:
                    logger.error(f"Error processing intelligent retry for hour {hour_key}: {e}")
                    logger.error(traceback.format_exc())
                    continue

            if scored_any_tasks:
                logger.info("Intelligent retry: Successfully scored some pending tasks")
            else:
                logger.debug("Intelligent retry: No pending tasks could be scored in this attempt")

        except Exception as e:
            logger.error(f"Error in _intelligent_retry_pending_tasks: {e}")
            logger.error(traceback.format_exc())

    async def _update_retry_count(self, task_id: int):
        """
        Updates the retry count for a task to track how many times we've attempted to score it.
        This helps prevent infinite retries of problematic tasks.
        """
        try:
            await self.db_manager.execute(
                """
                UPDATE geomagnetic_predictions 
                SET retry_count = COALESCE(retry_count, 0) + 1,
                    last_retry_attempt = NOW()
                WHERE id = :task_id
                """,
                {"task_id": task_id}
            )
        except Exception as e:
            logger.warning(f"Failed to update retry count for task {task_id}: {e}")

    async def _should_retry_task(self, task: dict, max_retries: int = 10) -> bool:
        """
        Determines if a task should be retried based on its age and retry count.
        
        Args:
            task: Task dictionary containing task information
            max_retries: Maximum number of retry attempts allowed
            
        Returns:
            bool: True if the task should be retried, False otherwise
        """
        try:
            # Get current retry count from database
            result = await self.db_manager.fetch_one(
                "SELECT COALESCE(retry_count, 0) as retry_count, last_retry_attempt FROM geomagnetic_predictions WHERE id = :task_id",
                {"task_id": task["id"]}
            )
            
            if not result:
                return True  # If we can't find the task, allow retry
            
            retry_count = result["retry_count"] or 0
            last_retry = result["last_retry_attempt"]
            
            # Don't retry if we've exceeded max attempts
            if retry_count >= max_retries:
                logger.debug(f"Task {task['id']} has exceeded max retries ({retry_count}/{max_retries})")
                return False
            
            # Don't retry if we just attempted recently (within last 30 minutes)
            if last_retry:
                current_time = datetime.datetime.now(datetime.timezone.utc)
                if isinstance(last_retry, datetime.datetime):
                    if last_retry.tzinfo is None:
                        last_retry = last_retry.replace(tzinfo=datetime.timezone.utc)
                    time_since_retry = current_time - last_retry
                    if time_since_retry.total_seconds() < 1800:  # 30 minutes
                        logger.debug(f"Task {task['id']} was retried recently ({time_since_retry.total_seconds()/60:.1f} minutes ago)")
                        return False
            
            # Check task age - don't retry tasks older than 7 days
            task_time = task.get("query_time") or task.get("timestamp")
            if task_time:
                if isinstance(task_time, datetime.datetime):
                    if task_time.tzinfo is None:
                        task_time = task_time.replace(tzinfo=datetime.timezone.utc)
                    current_time = datetime.datetime.now(datetime.timezone.utc)
                    task_age = current_time - task_time
                    if task_age.total_seconds() > 604800:  # 7 days
                        logger.debug(f"Task {task['id']} is too old ({task_age.total_seconds()/86400:.1f} days)")
                        return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Error checking if task {task['id']} should be retried: {e}")
            return True  # Default to allowing retry if we can't determine


