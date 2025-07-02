"""
Geomagnetic Task Core Implementation

Clean task interface with delegation to specialized modules.
Extracted from the monolithic geomagnetic_task.py (2,845 lines) for better modularity.
"""

import asyncio
import datetime
import importlib.util
import os
import traceback
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

from fiber.logging_utils import get_logger
from pydantic import Field

from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.models.geomag_basemodel import GeoMagBaseModel
from gaia.tasks.base.task import Task
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_inputs import GeomagneticInputs
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_metadata import GeomagneticMetadata
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_outputs import GeomagneticOutputs
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import GeomagneticPreprocessing
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_scoring_mechanism import GeomagneticScoringMechanism
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager

from .config import GeomagneticConfig, load_geomagnetic_config
from ..processing.validator_workflow import GeomagneticValidatorWorkflow
from ..processing.miner_workflow import GeomagneticMinerWorkflow
from ..processing.scoring_workflow import GeomagneticScoringWorkflow
from ..processing.data_management import GeomagneticDataManager

logger = get_logger(__name__)


class GeomagneticTask(Task):
    """
    Geomagnetic Prediction Task
    
    Clean interface that delegates to specialized workflow modules.
    Replaces the monolithic 2,845-line implementation with modular architecture.
    
    🔒 SECURITY ENHANCEMENTS IMPLEMENTED:
    1. TEMPORAL SEPARATION ENFORCEMENT AT SCORING TIME
    2. SECURE SCORING FLOW WITH PROPER DELAYS
    3. VULNERABILITY MITIGATIONS FOR PREMATURE GROUND TRUTH ACCESS
    
    This task involves:
    - Querying miners for predictions using current data (real-time prediction)
    - Adding predictions to a queue for scoring
    - Waiting for proper temporal separation before scoring
    - Fetching time-specific ground truth with security controls
    - Scoring predictions only when ground truth is stable
    - Moving scored tasks to history
    """

    # Configuration
    config: GeomagneticConfig = Field(default_factory=load_geomagnetic_config)
    
    # Database and components
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
    
    # Node configuration
    node_type: str = Field(
        default="validator",
        description="Type of node running the task (validator or miner)",
    )
    test_mode: bool = Field(
        default=False,
        description="Whether to run in test mode (immediate execution, limited scope)",
    )
    validator: Any = Field(
        default=None, description="Reference to the validator instance"
    )
    
    # Workflow delegates
    validator_workflow: Optional[GeomagneticValidatorWorkflow] = None
    miner_workflow: Optional[GeomagneticMinerWorkflow] = None
    scoring_workflow: Optional[GeomagneticScoringWorkflow] = None
    data_manager: Optional[GeomagneticDataManager] = None
    
    # Retry worker configuration
    pending_retry_worker_running: bool = Field(
        default=False, description="Whether the pending retry worker is running"
    )
    pending_retry_worker_task: Optional[Any] = Field(
        default=None, description="Reference to the pending retry worker task"
    )

    def __init__(self, node_type: str, db_manager, test_mode: bool = False, **data):
        """Initialize the geomagnetic task with configuration and components."""
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

        # Load configuration
        self.config = load_geomagnetic_config()
        if node_type:
            self.config.node_type = node_type
        if test_mode:
            self.config.test_mode = test_mode

        # Store references
        self.node_type = self.config.node_type
        self.test_mode = self.config.test_mode
        self.model = None

        # Initialize components based on node type
        self._initialize_components()

        # Initialize workflow delegates
        self._initialize_workflows()

    def _initialize_components(self) -> None:
        """Initialize node-specific components."""
        if self.node_type == "miner":
            self._initialize_miner_model()
        else:
            logger.info("Running as validator - skipping model loading")

    def _initialize_miner_model(self) -> None:
        """Initialize the geomagnetic model for miner nodes."""
        try:
            logger.info("Running as miner - loading model...")
            
            # Try to load custom model first
            custom_model_path = self.config.custom_model_path
            if os.path.exists(custom_model_path):
                self._load_custom_model(custom_model_path)
            else:
                self._load_base_model()
                
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            logger.error(traceback.format_exc())
            raise

    def _load_custom_model(self, model_path: str) -> None:
        """Load custom geomagnetic model."""
        try:
            spec = importlib.util.spec_from_file_location("custom_geomagnetic_model", model_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.model = module.CustomGeomagneticModel()
            logger.info("Successfully loaded custom geomagnetic model")
        except Exception as e:
            logger.error(f"Failed to load custom model from {model_path}: {e}")
            self._load_base_model()

    def _load_base_model(self) -> None:
        """Load base geomagnetic model."""
        self.model = GeoMagBaseModel()
        logger.info("No custom model found, using base model")

    def _initialize_workflows(self) -> None:
        """Initialize workflow delegate modules."""
        self.data_manager = GeomagneticDataManager(self.config, self.db_manager)
        
        if self.node_type == "validator":
            self.validator_workflow = GeomagneticValidatorWorkflow(
                self.config, self.db_manager, self.data_manager
            )
            self.scoring_workflow = GeomagneticScoringWorkflow(
                self.config, self.db_manager, self.scoring_mechanism
            )
        else:
            self.miner_workflow = GeomagneticMinerWorkflow(
                self.config, self.model, self.miner_preprocessing
            )

    # === MAIN EXECUTION METHODS ===

    async def validator_execute(self, validator) -> None:
        """Execute validator workflow - delegates to GeomagneticValidatorWorkflow."""
        self.validator = validator
        
        if not self.validator_workflow:
            raise RuntimeError("Validator workflow not initialized")
        
        # Initialize workflows with validator reference
        self.validator_workflow.set_validator(validator)
        if self.scoring_workflow:
            self.scoring_workflow.set_validator(validator)

        # Log startup
        logger.info("🔒 Starting geomagnetic validator workflow with security enhancements...")

        # Delegate to validator workflow
        await self.validator_workflow.run_main_loop()

    def miner_execute(self, data, miner) -> Dict[str, Any]:
        """Execute miner workflow - delegates to GeomagneticMinerWorkflow."""
        if not self.miner_workflow:
            raise RuntimeError("Miner workflow not initialized")
        
        return self.miner_workflow.process_request(data, miner)

    # === LEGACY COMPATIBILITY METHODS ===

    def miner_preprocess(self, raw_data):
        """
        Preprocess raw geomagnetic data on the miner's side.
        Compatibility method for existing interfaces.
        """
        if self.miner_workflow:
            return self.miner_workflow.preprocess_data(raw_data)
        
        # Fallback to simple preprocessing
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
        Compatibility method for existing interfaces.
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
        Compatibility method for existing interfaces.
        """
        if self.scoring_workflow:
            return self.scoring_workflow.calculate_score(prediction, ground_truth)
        
        # Fallback to simple scoring
        try:
            score = abs(prediction - ground_truth)
            return score
        except Exception as e:
            logger.error(f"Error in validator_score: {e}")
            return float("inf")

    def run_model_inference(self, processed_data):
        """
        Run model inference compatibility method.
        """
        if self.miner_workflow:
            return self.miner_workflow.run_inference(processed_data)
        return None

    def extract_prediction(self, response):
        """
        Extract prediction from response compatibility method.
        """
        if self.miner_workflow:
            return self.miner_workflow.extract_prediction(response)
        return None

    # === DATA MANAGEMENT DELEGATION ===

    async def add_task_to_queue(self, predictions, query_time):
        """Add task to queue - delegates to data manager."""
        if not self.data_manager:
            raise RuntimeError("Data manager not initialized")
        await self.data_manager.add_task_to_queue(predictions, query_time)

    async def add_prediction_to_queue(
        self, miner_uid: str, miner_hotkey: str, predicted_value: float, 
        query_time: datetime.datetime, status: str = "pending"
    ) -> None:
        """Add prediction to queue - delegates to data manager."""
        if not self.data_manager:
            raise RuntimeError("Data manager not initialized")
        await self.data_manager.add_prediction_to_queue(
            miner_uid, miner_hotkey, predicted_value, query_time, status
        )

    async def get_tasks_for_hour(self, start_time, end_time, validator=None):
        """Get tasks for hour - delegates to data manager."""
        if not self.data_manager:
            raise RuntimeError("Data manager not initialized")
        return await self.data_manager.get_tasks_for_hour(start_time, end_time, validator)

    async def move_task_to_history(
        self, task: dict, ground_truth_value: float, score: float, score_time: datetime.datetime
    ):
        """Move task to history - delegates to data manager."""
        if not self.data_manager:
            raise RuntimeError("Data manager not initialized")
        await self.data_manager.move_task_to_history(task, ground_truth_value, score, score_time)

    async def cleanup_resources(self):
        """Cleanup resources - delegates to data manager."""
        if not self.data_manager:
            raise RuntimeError("Data manager not initialized")
        await self.data_manager.cleanup_resources()

    # === SCORING DELEGATION ===

    async def build_score_row(self, current_hour, recent_tasks=None):
        """Build score row - delegates to scoring workflow."""
        if not self.scoring_workflow:
            raise RuntimeError("Scoring workflow not initialized")
        return await self.scoring_workflow.build_score_row(current_hour, recent_tasks)

    async def recalculate_recent_scores(self, uids: List[int]) -> None:
        """Recalculate recent scores - delegates to scoring workflow."""
        if not self.scoring_workflow:
            raise RuntimeError("Scoring workflow not initialized")
        await self.scoring_workflow.recalculate_recent_scores(uids)