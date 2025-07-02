"""
Soil Moisture Task Core Implementation

Clean task interface with delegation to specialized modules.
Extracted from the monolithic soil_task.py (3,195 lines) for better modularity.
"""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fiber.logging_utils import get_logger
from pydantic import Field

from gaia.models.soil_moisture_basemodel import SoilModel
from gaia.tasks.base.task import Task
from gaia.tasks.defined_tasks.soilmoisture.soil_inputs import SoilMoistureInputs
from gaia.tasks.defined_tasks.soilmoisture.soil_metadata import SoilMoistureMetadata
from gaia.tasks.defined_tasks.soilmoisture.soil_miner_preprocessing import SoilMinerPreprocessing
from gaia.tasks.defined_tasks.soilmoisture.soil_outputs import SoilMoistureOutputs
from gaia.tasks.defined_tasks.soilmoisture.soil_scoring_mechanism import SoilScoringMechanism

from .config import SoilMoistureConfig, load_soil_moisture_config
from ..processing.validator_workflow import SoilValidatorWorkflow
from ..processing.miner_workflow import SoilMinerWorkflow
from ..processing.scoring_workflow import SoilScoringWorkflow
from ..processing.data_management import SoilDataManager

logger = get_logger(__name__)

# Configure async debugging
os.environ["PYTHONASYNCIODEBUG"] = "1"

# Import validator preprocessing only on validator nodes
if os.environ.get("NODE_TYPE") == "validator":
    from gaia.tasks.defined_tasks.soilmoisture.soil_validator_preprocessing import (
        SoilValidatorPreprocessing,
    )
else:
    SoilValidatorPreprocessing = None


class SoilMoistureTask(Task):
    """
    Soil Moisture Prediction Task
    
    Clean interface that delegates to specialized workflow modules.
    Replaces the monolithic 3,195-line implementation with modular architecture.
    """

    # Configuration
    config: SoilMoistureConfig = Field(default_factory=load_soil_moisture_config)
    
    # Legacy fields for compatibility
    prediction_horizon: timedelta = Field(
        default_factory=lambda: timedelta(hours=6),
        description="Prediction horizon for the task",
    )
    scoring_delay: timedelta = Field(
        default_factory=lambda: timedelta(days=3),
        description="Delay before scoring due to SMAP data latency",
    )

    # Components
    validator_preprocessing: Optional["SoilValidatorPreprocessing"] = None
    miner_preprocessing: Optional["SoilMinerPreprocessing"] = None
    model: Optional[SoilModel] = None
    
    # Workflow delegates
    validator_workflow: Optional[SoilValidatorWorkflow] = None
    miner_workflow: Optional[SoilMinerWorkflow] = None
    scoring_workflow: Optional[SoilScoringWorkflow] = None
    data_manager: Optional[SoilDataManager] = None
    
    # Database and node configuration
    db_manager: Any = Field(default=None)
    node_type: str = Field(default="miner")
    test_mode: bool = Field(default=False)
    use_raw_preprocessing: bool = Field(default=False)
    validator: Any = Field(default=None, description="Reference to the validator instance")
    use_threaded_scoring: bool = Field(
        default=False, description="Enable threaded scoring for performance improvement"
    )

    def __init__(self, db_manager=None, node_type=None, test_mode=False, **data):
        """Initialize the soil moisture task with configuration and components."""
        super().__init__(
            name="SoilMoistureTask",
            description="Soil moisture prediction task",
            task_type="atomic",
            metadata=SoilMoistureMetadata(),
            inputs=SoilMoistureInputs(),
            outputs=SoilMoistureOutputs(),
            scoring_mechanism=SoilScoringMechanism(
                db_manager=db_manager, 
                baseline_rmse=50, 
                alpha=10, 
                beta=0.1, 
                task=None
            ),
        )

        # Load configuration
        self.config = load_soil_moisture_config()
        if node_type:
            self.config.node_type = node_type
        if test_mode:
            self.config.test_mode = test_mode

        # Set legacy fields from config
        self.prediction_horizon = self.config.prediction_horizon
        self.scoring_delay = self.config.scoring_delay
        self.use_threaded_scoring = self.config.use_threaded_scoring

        # Store references
        self.db_manager = db_manager
        self.node_type = self.config.node_type
        self.test_mode = self.config.test_mode
        self.scoring_mechanism.task = self

        # Log configuration
        if self.use_threaded_scoring:
            logger.info("🚀 Soil threaded scoring enabled - improved performance expected")

        # Initialize components based on node type
        self._initialize_components()

        # Initialize workflow delegates
        self._initialize_workflows()

        # Initialize prepared regions cache
        self._prepared_regions = {}

    def _initialize_components(self) -> None:
        """Initialize node-specific components."""
        if self.node_type == "validator":
            self.validator_preprocessing = SoilValidatorPreprocessing()
        else:
            self.miner_preprocessing = SoilMinerPreprocessing(task=self)
            self._initialize_model()

    def _initialize_model(self) -> None:
        """Initialize the soil moisture model."""
        custom_model_path = self.config.custom_model_path
        if os.path.exists(custom_model_path):
            self._load_custom_model(custom_model_path)
        else:
            self._load_base_model()

    def _load_custom_model(self, model_path: str) -> None:
        """Load custom soil moisture model."""
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("custom_soil_model", model_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.model = module.CustomSoilModel()
            self.use_raw_preprocessing = True
            logger.info("Initialized custom soil model")
        except Exception as e:
            logger.error(f"Failed to load custom model from {model_path}: {e}")
            self._load_base_model()

    def _load_base_model(self) -> None:
        """Load base soil moisture model."""
        self.model = self.miner_preprocessing.model
        self.use_raw_preprocessing = False
        logger.info("Initialized base soil model")

    def _initialize_workflows(self) -> None:
        """Initialize workflow delegate modules."""
        self.data_manager = SoilDataManager(self.config, self.db_manager)
        
        if self.node_type == "validator":
            self.validator_workflow = SoilValidatorWorkflow(
                self.config, self.db_manager, self.validator_preprocessing
            )
            self.scoring_workflow = SoilScoringWorkflow(
                self.config, self.db_manager, self.scoring_mechanism
            )
        else:
            self.miner_workflow = SoilMinerWorkflow(
                self.config, self.model, self.miner_preprocessing
            )

    # === MAIN EXECUTION METHODS ===

    async def validator_execute(self, validator) -> None:
        """Execute validator workflow - delegates to SoilValidatorWorkflow."""
        if not hasattr(self, "db_manager") or self.db_manager is None:
            self.db_manager = validator.db_manager
        self.validator = validator

        # Initialize workflows with validator reference
        self.validator_workflow.set_validator(validator)
        self.scoring_workflow.set_validator(validator)

        # Log startup
        logger.info("🚀 Starting soil moisture validator workflow...")

        # Run startup retry check
        await self._startup_retry_check()

        # Main validator loop
        await self.validator_workflow.run_main_loop()

    async def miner_execute(self, data: Dict[str, Any], miner) -> Dict[str, Any]:
        """Execute miner workflow - delegates to SoilMinerWorkflow."""
        if not self.miner_workflow:
            raise RuntimeError("Miner workflow not initialized")
        
        return await self.miner_workflow.process_request(data, miner)

    async def validator_score(self, result=None) -> None:
        """Execute scoring workflow - delegates to SoilScoringWorkflow."""
        if not self.scoring_workflow:
            raise RuntimeError("Scoring workflow not initialized")
        
        await self.scoring_workflow.run_scoring_cycle(result)

    # === UTILITY METHODS ===

    def get_next_preparation_time(self, current_time: datetime) -> datetime:
        """Get the next preparation window start time."""
        windows = self.config.validator_windows
        current_mins = current_time.hour * 60 + current_time.minute

        for start_hr, start_min, _, _ in windows:
            window_start_mins = start_hr * 60 + start_min
            if window_start_mins > current_mins:
                return current_time.replace(
                    hour=start_hr, minute=start_min, second=0, microsecond=0
                )

        # Next day, first window
        tomorrow = current_time + timedelta(days=1)
        first_window = windows[0]
        return tomorrow.replace(
            hour=first_window[0], minute=first_window[1], second=0, microsecond=0
        )

    def get_ifs_time_for_smap(self, smap_time: datetime) -> datetime:
        """Get IFS forecast time for given SMAP time."""
        return smap_time - timedelta(hours=6)

    def get_smap_time_for_validator(self, current_time: datetime) -> datetime:
        """Get SMAP target time for validator processing."""
        # Round down to nearest 6-hour interval
        hour = (current_time.hour // 6) * 6
        return current_time.replace(hour=hour, minute=0, second=0, microsecond=0)

    def get_validator_windows(self) -> List[tuple]:
        """Get validator processing windows."""
        return self.config.validator_windows

    def is_in_window(self, current_time: datetime, window: tuple) -> bool:
        """Check if current time is within a validator processing window."""
        start_hr, start_min, end_hr, end_min = window
        current_mins = current_time.hour * 60 + current_time.minute
        start_mins = start_hr * 60 + start_min
        end_mins = end_hr * 60 + end_min
        return start_mins <= current_mins <= end_mins

    # === LEGACY COMPATIBILITY METHODS ===

    async def get_todays_regions(self, target_time: datetime) -> List[Dict]:
        """Get today's regions - delegates to data manager."""
        if not self.data_manager:
            raise RuntimeError("Data manager not initialized")
        return await self.data_manager.get_todays_regions(target_time)

    async def add_task_to_queue(self, responses: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Add task to queue - delegates to data manager."""
        if not self.data_manager:
            raise RuntimeError("Data manager not initialized")
        await self.data_manager.add_task_to_queue(responses, metadata)

    async def get_pending_tasks(self) -> List[Dict]:
        """Get pending tasks - delegates to data manager."""
        if not self.data_manager:
            raise RuntimeError("Data manager not initialized")
        return await self.data_manager.get_pending_tasks()

    async def cleanup_predictions(self, bounds, target_time=None, miner_uid=None) -> None:
        """Cleanup predictions - delegates to data manager."""
        if not self.data_manager:
            raise RuntimeError("Data manager not initialized")
        await self.data_manager.cleanup_predictions(bounds, target_time, miner_uid)

    async def cleanup_resources(self) -> None:
        """Cleanup resources - delegates to data manager."""
        if not self.data_manager:
            raise RuntimeError("Data manager not initialized")
        await self.data_manager.cleanup_resources()

    async def _startup_retry_check(self) -> None:
        """Run startup retry check for pending tasks."""
        logger.info("🚀 Checking for pending tasks on startup...")
        try:
            if self.validator_workflow:
                await self.validator_workflow.startup_retry_check()
        except Exception as e:
            logger.error(f"Error during startup retry check: {e}")

    # === PREPROCESSING COMPATIBILITY ===

    def miner_preprocess(self, preprocessing=None, inputs=None):
        """Miner preprocessing compatibility method."""
        if self.miner_workflow:
            return self.miner_workflow.preprocess_data(preprocessing, inputs)
        return None

    def run_model_inference(self, processed_data):
        """Run model inference compatibility method."""
        if self.miner_workflow:
            return self.miner_workflow.run_inference(processed_data)
        return None