# GeomagneticScheduler - Service-oriented refactor of GeomagneticTask
import asyncio
import logging
import datetime
import traceback
from typing import Dict, Any, List, Optional
from uuid import uuid4

# Import the new services
from ..services import DatabaseService, NetworkService, ExecutionService
from ..services.network.service import NetworkJobType, NetworkJobPriority

# Import existing business logic
from ...tasks.defined_tasks.geomagnetic.geomagnetic_metadata import GeomagneticMetadata
from ...tasks.defined_tasks.geomagnetic.geomagnetic_scoring_mechanism import GeomagneticScoringMechanism
from ...tasks.defined_tasks.geomagnetic.utils.process_geomag_data import get_latest_geomag_data

logger = logging.getLogger(__name__)


class GeomagneticScheduler:
    """
    Service-oriented geomagnetic task scheduler.
    
    Coordinates geomagnetic data fetching, miner querying, and scoring
    across DatabaseService, NetworkService, and ExecutionService.
    
    This replaces the monolithic GeomagneticTask.validator_execute() with
    a clean scheduler that delegates work to services.
    """
    
    def __init__(self, database_service: DatabaseService, 
                 network_service: NetworkService, 
                 execution_service: ExecutionService,
                 validator=None, test_mode: bool = False):
        
        self.database = database_service
        self.network = network_service
        self.execution = execution_service
        self.validator = validator
        self.test_mode = test_mode
        
        # Business logic components
        self.metadata = GeomagneticMetadata()
        self.scoring_mechanism = GeomagneticScoringMechanism(db_manager=database_service.db_manager)
        
        # Scheduler state
        self.running = False
        self.background_tasks = set()
        
        self.logger = logging.getLogger(f"{__name__}.GeomagneticScheduler")
        self.logger.info(f"GeomagneticScheduler initialized (test_mode: {test_mode})")
    
    async def start(self):
        """Start the geomagnetic scheduler."""
        if self.running:
            self.logger.warning("GeomagneticScheduler already running")
            return
        
        self.running = True
        
        # Start background retry worker
        retry_task = asyncio.create_task(self._background_retry_worker())
        self.background_tasks.add(retry_task)
        
        self.logger.info("GeomagneticScheduler started")
    
    async def stop(self):
        """Stop the geomagnetic scheduler gracefully."""
        self.running = False
        
        # Cancel all background tasks
        for task in self.background_tasks:
            task.cancel()
        
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        self.background_tasks.clear()
        self.logger.info("GeomagneticScheduler stopped")
    
    async def run_hourly_cycle(self):
        """
        Run one complete geomagnetic cycle:
        1. Fetch latest geomagnetic data
        2. Query miners for predictions  
        3. Score predictions from previous hour
        """
        try:
            await self._update_task_status('active')
            
            # Step 1: Align to next hour (or immediate in test mode)
            current_time = datetime.datetime.now(datetime.timezone.utc)
            
            if not self.test_mode:
                next_hour = current_time.replace(
                    minute=0, second=0, microsecond=0
                ) + datetime.timedelta(hours=1)
                
                sleep_duration = (next_hour - current_time).total_seconds()
                self.logger.info(f"Waiting until next hour: {next_hour} (in {sleep_duration}s)")
                
                await self._update_task_status('idle')
                await asyncio.sleep(sleep_duration)
            else:
                next_hour = current_time
                self.logger.info("Test mode: Running immediately")
            
            self.logger.info("Starting geomagnetic cycle...")
            
            # Step 2: Fetch geomagnetic data (via ExecutionService for I/O)
            await self._update_task_status('processing', 'data_fetch')
            geomag_data = await self._fetch_geomagnetic_data()
            
            if not geomag_data:
                self.logger.warning("No geomagnetic data available, skipping cycle")
                return
            
            # Step 3: Query miners (via NetworkService with high priority)
            await self._update_task_status('processing', 'miner_query')
            await self._query_miners_for_predictions(geomag_data, next_hour)
            
            # Step 4: Score previous hour's predictions (via ExecutionService)
            previous_hour = next_hour - datetime.timedelta(hours=1)
            await self._update_task_status('processing', 'scoring')
            await self._score_previous_hour_predictions(previous_hour)
            
            await self._update_task_status('idle')
            self.logger.info("Geomagnetic cycle completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in geomagnetic cycle: {e}")
            self.logger.error(traceback.format_exc())
            await self._update_task_status('error')
            raise
    
    async def _fetch_geomagnetic_data(self) -> Optional[Dict[str, Any]]:
        """
        Fetch latest geomagnetic data using ExecutionService.
        
        This runs the I/O-bound data fetching in a thread pool
        to avoid blocking the main event loop.
        """
        try:
            # Submit data fetching job to ExecutionService
            result = await self.execution.run(
                get_latest_geomag_data,
                kwargs={'include_historical': True},
                execution_mode="thread",  # I/O bound operation
                timeout=60,
                worker_name="geomag_data_fetch"
            )
            
            if not result.success:
                self.logger.error(f"Failed to fetch geomagnetic data: {result.error}")
                return None
            
            # Unpack the result
            data = result.result
            if len(data) == 3:
                timestamp, dst_value, historical_data = data
            else:
                self.logger.error(f"Unexpected geomagnetic data format: {data}")
                return None
            
            if timestamp == "N/A" or dst_value == "N/A":
                self.logger.warning("Invalid geomagnetic data received")
                return None
            
            self.logger.info(f"Fetched geomagnetic data: timestamp={timestamp}, value={dst_value}")
            if historical_data is not None:
                self.logger.info(f"Historical data: {len(historical_data)} records")
            
            return {
                'timestamp': timestamp,
                'dst_value': dst_value,
                'historical_data': historical_data
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching geomagnetic data: {e}")
            return None
    
    async def _query_miners_for_predictions(self, geomag_data: Dict[str, Any], hour_start: datetime.datetime):
        """
        Query miners for predictions using NetworkService.
        
        This uses the NetworkService job system with high priority
        to ensure miner queries are processed quickly.
        """
        try:
            timestamp = geomag_data['timestamp']
            dst_value = geomag_data['dst_value']
            historical_data = geomag_data['historical_data']
            
            # Generate task ID for this hour
            task_id = str(hour_start.timestamp())
            
            # Run baseline model prediction (via ExecutionService)
            if self.validator and hasattr(self.validator, 'basemodel_evaluator'):
                self.logger.info(f"Running baseline model prediction for task_id: {task_id}")
                
                baseline_result = await self.execution.run(
                    self.validator.basemodel_evaluator.predict_geo_and_store,
                    args=(historical_data, task_id),
                    execution_mode="thread",
                    timeout=120,
                    worker_name="baseline_prediction"
                )
                
                if baseline_result.success:
                    self.logger.info("Baseline prediction completed successfully")
                else:
                    self.logger.error(f"Baseline prediction failed: {baseline_result.error}")
            
            # Prepare historical records for miners
            historical_records = []
            if historical_data is not None:
                # Use ExecutionService for data processing
                processing_result = await self.execution.run(
                    self._prepare_historical_records,
                    args=(historical_data,),
                    execution_mode="thread",
                    timeout=30,
                    worker_name="data_preparation"
                )
                
                if processing_result.success:
                    historical_records = processing_result.result
                else:
                    self.logger.error(f"Failed to prepare historical records: {processing_result.error}")
            
            # Construct payload for miners
            nonce = str(uuid4())
            payload = {
                "nonce": nonce,
                "data": {
                    "name": "Geomagnetic Data",
                    "timestamp": timestamp.isoformat(),
                    "value": dst_value,
                    "historical_values": historical_records,
                },
            }
            
            # Submit miner query via NetworkService with HIGH priority
            self.logger.info("Submitting miner query to NetworkService")
            query_result = await self.network.query_miners(
                payload=payload,
                endpoint="/geomagnetic-request",
                priority=NetworkJobPriority.HIGH
            )
            
            self.logger.info(f"Received {len(query_result)} miner responses")
            
            # Process responses and store in database
            await self._store_miner_predictions(query_result, hour_start)
            
        except Exception as e:
            self.logger.error(f"Error querying miners: {e}")
            self.logger.error(traceback.format_exc())
    
    def _prepare_historical_records(self, historical_data) -> List[Dict[str, Any]]:
        """
        Prepare historical records for miner payload.
        
        This runs in a thread via ExecutionService to avoid blocking.
        """
        records = []
        if historical_data is not None:
            for _, row in historical_data.iterrows():
                records.append({
                    "timestamp": row["timestamp"].isoformat(),
                    "Dst": row["Dst"]
                })
        return records
    
    async def _store_miner_predictions(self, responses: Dict[str, Any], hour_start: datetime.datetime):
        """
        Store miner predictions in database using DatabaseService.
        """
        try:
            if not responses:
                self.logger.warning("No miner responses to store")
                return
            
            stored_count = 0
            for hotkey, response in responses.items():
                try:
                    # Extract prediction value
                    predicted_value = self._extract_prediction(response)
                    if predicted_value is None:
                        self.logger.error(f"No valid prediction from miner {hotkey}")
                        continue
                    
                    # Get miner UID from database
                    miner_result = await self.database.fetch_one(
                        "SELECT uid FROM node_table WHERE hotkey = :hotkey",
                        {"hotkey": hotkey}
                    )
                    
                    if not miner_result:
                        self.logger.warning(f"No UID found for hotkey {hotkey}")
                        continue
                    
                    miner_uid = str(miner_result["uid"])
                    
                    # Store prediction
                    await self.database.execute(
                        """
                        INSERT INTO geomagnetic_predictions 
                        (id, miner_uid, miner_hotkey, predicted_value, query_time, status)
                        VALUES (:id, :miner_uid, :miner_hotkey, :predicted_value, :query_time, :status)
                        """,
                        {
                            "id": str(uuid4()),
                            "miner_uid": miner_uid,
                            "miner_hotkey": hotkey,
                            "predicted_value": float(predicted_value),
                            "query_time": hour_start,
                            "status": "pending"
                        }
                    )
                    
                    stored_count += 1
                    self.logger.info(f"Stored prediction from miner {hotkey}: {predicted_value}")
                    
                except Exception as e:
                    self.logger.error(f"Error storing prediction from {hotkey}: {e}")
                    continue
            
            self.logger.info(f"Stored {stored_count} miner predictions")
            
        except Exception as e:
            self.logger.error(f"Error storing miner predictions: {e}")
            self.logger.error(traceback.format_exc())
    
    def _extract_prediction(self, response) -> Optional[float]:
        """Extract prediction value from miner response."""
        try:
            if isinstance(response, dict):
                if "predicted_values" in response:
                    return float(response["predicted_values"])
                if "predicted_value" in response:
                    return float(response["predicted_value"])
            return None
        except (ValueError, TypeError):
            return None
    
    async def _score_previous_hour_predictions(self, target_hour: datetime.datetime):
        """
        Score predictions from previous hour using ExecutionService.
        
        This runs the scoring logic in the execution service to handle
        both I/O (database queries) and computation (scoring algorithms).
        """
        try:
            # Submit scoring job to ExecutionService
            scoring_result = await self.execution.run(
                self._score_hour_predictions,
                args=(target_hour,),
                execution_mode="thread",  # Can handle both I/O and computation
                timeout=300,  # 5 minutes for scoring
                worker_name=f"score_predictions_{target_hour.hour}"
            )
            
            if scoring_result.success:
                scored_count = scoring_result.result
                self.logger.info(f"Successfully scored {scored_count} predictions for hour {target_hour}")
            else:
                self.logger.error(f"Scoring failed: {scoring_result.error}")
                
        except Exception as e:
            self.logger.error(f"Error in scoring predictions: {e}")
            self.logger.error(traceback.format_exc())
    
    async def _score_hour_predictions(self, target_hour: datetime.datetime, database=None) -> int:
        """
        Score predictions for a specific hour.
        
        This method runs inside ExecutionService and gets database injected.
        """
        try:
            if database is None:
                database = self.database
            
            # Time window for predictions
            if self.test_mode:
                current_time = datetime.datetime.now(datetime.timezone.utc)
                start_time = current_time - datetime.timedelta(minutes=10)
                end_time = current_time
            else:
                start_time = target_hour
                end_time = target_hour + datetime.timedelta(hours=1)
            
            # Get pending tasks for this hour
            tasks = await database.fetch_all(
                """
                SELECT 
                    id, miner_uid, miner_hotkey, predicted_value, query_time
                FROM geomagnetic_predictions
                WHERE query_time >= :start_time 
                AND query_time < :end_time 
                AND status = 'pending'
                ORDER BY query_time DESC
                """,
                {"start_time": start_time, "end_time": end_time}
            )
            
            if not tasks:
                self.logger.info(f"No pending tasks found for hour {target_hour}")
                return 0
            
            # Check if ground truth is available
            if not await self._check_ground_truth_availability(target_hour):
                self.logger.info(f"Ground truth not available yet for hour {target_hour}")
                return 0
            
            # Fetch ground truth
            ground_truth = await self._fetch_ground_truth_value()
            if ground_truth is None:
                self.logger.warning(f"Could not fetch ground truth for hour {target_hour}")
                return 0
            
            self.logger.info(f"Scoring {len(tasks)} tasks with ground truth: {ground_truth}")
            
            # Score each task
            scored_tasks = []
            current_time = datetime.datetime.now(datetime.timezone.utc)
            
            for task in tasks:
                try:
                    # Validate miner is still in metagraph
                    if not self._validate_miner_in_metagraph(task["miner_hotkey"]):
                        # Delete invalid task
                        await database.execute(
                            "DELETE FROM geomagnetic_predictions WHERE id = :task_id",
                            {"task_id": task["id"]}
                        )
                        continue
                    
                    # Calculate score
                    predicted_value = task["predicted_value"]
                    score = self.scoring_mechanism.calculate_score(predicted_value, ground_truth)
                    
                    # Get baseline score for comparison if available
                    baseline_score = await self._get_baseline_score(task, ground_truth)
                    
                    # Apply baseline comparison
                    if baseline_score is not None:
                        epsilon = 0.005
                        if score <= baseline_score + epsilon:
                            self.logger.info(f"Score zeroed for task {task['id']}: {score:.4f} vs baseline {baseline_score:.4f}")
                            score = 0
                        else:
                            self.logger.info(f"Score valid for task {task['id']}: {score:.4f} exceeds baseline by {score - baseline_score:.4f}")
                    
                    # Archive task to history
                    await database.execute(
                        """
                        INSERT INTO geomagnetic_history 
                        (miner_uid, miner_hotkey, query_time, predicted_value, ground_truth_value, score, scored_at)
                        VALUES (:miner_uid, :miner_hotkey, :query_time, :predicted_value, :ground_truth, :score, :scored_at)
                        """,
                        {
                            "miner_uid": task["miner_uid"],
                            "miner_hotkey": task["miner_hotkey"],
                            "query_time": task["query_time"],
                            "predicted_value": predicted_value,
                            "ground_truth": ground_truth,
                            "score": score,
                            "scored_at": current_time
                        }
                    )
                    
                    # Remove from predictions table
                    await database.execute(
                        "DELETE FROM geomagnetic_predictions WHERE id = :task_id",
                        {"task_id": task["id"]}
                    )
                    
                    # Add to scored tasks for score row
                    task["score"] = score
                    scored_tasks.append(task)
                    
                    self.logger.info(f"Scored task {task['id']}: {score:.4f}")
                    
                except Exception as e:
                    self.logger.error(f"Error scoring task {task['id']}: {e}")
                    continue
            
            # Build score row if we have scored tasks
            if scored_tasks:
                await self._build_score_row(target_hour, scored_tasks, database)
            
            return len(scored_tasks)
            
        except Exception as e:
            self.logger.error(f"Error in _score_hour_predictions: {e}")
            self.logger.error(traceback.format_exc())
            return 0
    
    async def _check_ground_truth_availability(self, target_time: datetime.datetime) -> bool:
        """Check if ground truth data is likely available."""
        try:
            if self.test_mode:
                return True
            
            current_time = datetime.datetime.now(datetime.timezone.utc)
            time_since_target = current_time - target_time
            
            # Need at least 1 hour for geomagnetic data to be processed
            return time_since_target.total_seconds() > 3600
            
        except Exception:
            return True  # Default to trying
    
    async def _fetch_ground_truth_value(self) -> Optional[float]:
        """Fetch the current ground truth value."""
        try:
            result = await get_latest_geomag_data(include_historical=False)
            
            if len(result) == 2:
                timestamp, dst_value = result
                if timestamp != "N/A" and dst_value != "N/A":
                    return float(dst_value)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching ground truth: {e}")
            return None
    
    def _validate_miner_in_metagraph(self, miner_hotkey: str) -> bool:
        """Validate that miner is still in the metagraph."""
        if not self.validator or not hasattr(self.validator, 'metagraph'):
            return True  # Can't validate, assume valid
        
        return miner_hotkey in self.validator.metagraph.nodes
    
    async def _get_baseline_score(self, task: Dict[str, Any], ground_truth: float) -> Optional[float]:
        """Get baseline score for comparison."""
        try:
            if not self.validator or not hasattr(self.validator, 'basemodel_evaluator'):
                return None
            
            task_timestamp = task["query_time"]
            if isinstance(task_timestamp, datetime.datetime):
                if task_timestamp.tzinfo is None:
                    task_timestamp = task_timestamp.replace(tzinfo=datetime.timezone.utc)
                task_id = str(task_timestamp.timestamp())
            else:
                task_id = str(task_timestamp)
            
            self.validator.basemodel_evaluator.test_mode = self.test_mode
            baseline_score = await self.validator.basemodel_evaluator.score_geo_baseline(
                task_id=task_id,
                ground_truth=ground_truth
            )
            
            return baseline_score
            
        except Exception as e:
            self.logger.debug(f"Error getting baseline score: {e}")
            return None
    
    async def _build_score_row(self, target_hour: datetime.datetime, scored_tasks: List[Dict], database):
        """Build and store score row for the hour."""
        try:
            # Initialize scores array
            scores = [float("nan")] * 256
            
            # Fill in scores from tasks
            for task in scored_tasks:
                try:
                    miner_uid = int(task["miner_uid"])
                    if 0 <= miner_uid < 256:
                        scores[miner_uid] = task["score"]
                except (ValueError, TypeError):
                    continue
            
            # Verification time is 1 hour after prediction time
            verification_time = target_hour + datetime.timedelta(hours=1)
            
            # Store score row
            await database.execute(
                """
                INSERT INTO score_table (task_name, task_id, score, status)
                VALUES (:task_name, :task_id, :score, :status)
                ON CONFLICT (task_name, task_id) DO UPDATE SET
                    score = EXCLUDED.score,
                    status = EXCLUDED.status
                """,
                {
                    "task_name": "geomagnetic",
                    "task_id": str(verification_time.timestamp()),
                    "score": scores,
                    "status": "completed"
                }
            )
            
            non_nan_scores = len([s for s in scores if not str(s) == 'nan'])
            self.logger.info(f"Built score row for hour {target_hour} with {non_nan_scores} scores")
            
        except Exception as e:
            self.logger.error(f"Error building score row: {e}")
            self.logger.error(traceback.format_exc())
    
    async def _background_retry_worker(self):
        """Background worker to retry pending tasks."""
        retry_interval = 60 if self.test_mode else 600  # 1 min test, 10 min production
        
        while self.running:
            try:
                await asyncio.sleep(retry_interval)
                
                if not self.running:
                    break
                
                self.logger.debug("Background retry worker: Checking pending tasks...")
                
                # Submit retry job to ExecutionService
                retry_result = await self.execution.run(
                    self._retry_pending_tasks,
                    execution_mode="thread",
                    timeout=300,
                    worker_name="retry_pending_tasks"
                )
                
                if retry_result.success:
                    retried_count = retry_result.result
                    if retried_count > 0:
                        self.logger.info(f"Background retry processed {retried_count} pending tasks")
                else:
                    self.logger.error(f"Background retry failed: {retry_result.error}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in background retry worker: {e}")
                await asyncio.sleep(60)
    
    async def _retry_pending_tasks(self, database=None) -> int:
        """Retry pending tasks that can now be scored."""
        try:
            if database is None:
                database = self.database
            
            current_time = datetime.datetime.now(datetime.timezone.utc)
            start_time = current_time - datetime.timedelta(hours=72)  # 3 days
            
            # Get pending tasks grouped by hour
            pending_tasks = await database.fetch_all(
                """
                SELECT 
                    id, miner_uid, miner_hotkey, predicted_value, query_time,
                    DATE_TRUNC('hour', query_time) as hour_bucket
                FROM geomagnetic_predictions
                WHERE query_time >= :start_time 
                AND status = 'pending'
                ORDER BY query_time ASC
                """,
                {"start_time": start_time}
            )
            
            if not pending_tasks:
                return 0
            
            # Group by hour and process
            hourly_tasks = {}
            for task in pending_tasks:
                hour = task["hour_bucket"]
                if hour not in hourly_tasks:
                    hourly_tasks[hour] = []
                hourly_tasks[hour].append(task)
            
            total_processed = 0
            for hour, tasks in hourly_tasks.items():
                try:
                    # Check if ground truth is available
                    if not await self._check_ground_truth_availability(hour):
                        continue
                    
                    # Try to score this hour
                    processed = await self._score_hour_predictions(hour, database)
                    total_processed += processed
                    
                except Exception as e:
                    self.logger.error(f"Error retrying tasks for hour {hour}: {e}")
                    continue
            
            return total_processed
            
        except Exception as e:
            self.logger.error(f"Error in _retry_pending_tasks: {e}")
            return 0
    
    async def _update_task_status(self, status: str, substatus: str = None):
        """Update task status in validator."""
        try:
            if self.validator and hasattr(self.validator, 'update_task_status'):
                await self.validator.update_task_status('geomagnetic', status, substatus)
        except Exception as e:
            self.logger.debug(f"Could not update task status: {e}")


# Example usage function
async def example_geomagnetic_scheduler():
    """Example showing how to use the GeomagneticScheduler."""
    
    # Initialize services
    database_service = DatabaseService()
    await database_service.initialize()
    
    network_service = NetworkService()
    await network_service.start()
    
    execution_service = ExecutionService(
        database_service=database_service,
        max_thread_workers=4,
        max_process_workers=2
    )
    await execution_service.start_background_cleanup()
    
    # Initialize scheduler
    scheduler = GeomagneticScheduler(
        database_service=database_service,
        network_service=network_service,
        execution_service=execution_service,
        test_mode=True  # For quick testing
    )
    
    try:
        await scheduler.start()
        
        # Run one cycle
        await scheduler.run_hourly_cycle()
        
        # In production, this would run in a loop:
        # while True:
        #     await scheduler.run_hourly_cycle()
        #     if not scheduler.test_mode:
        #         await asyncio.sleep(3600)  # Wait 1 hour
        
    finally:
        await scheduler.stop()
        await execution_service.close()
        await network_service.close()
        await database_service.close()


if __name__ == "__main__":
    asyncio.run(example_geomagnetic_scheduler()) 