"""Soil Moisture Validator Task.

This module implements the validator task for soil moisture prediction. It handles:
1. Region preparation and data collection
2. Miner response validation and scoring
3. Database management for predictions and scores
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any, ClassVar, Union
import os
import base64
import json
import asyncio
import traceback
from uuid import uuid4

from prefect import flow, task
from pydantic import Field
from prefect.tasks import task_input_hash

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.tasks.base.task import BaseTask
from gaia.tasks.defined_tasks.soilmoisture.soil_metadata import SoilMoistureMetadata
from gaia.tasks.defined_tasks.soilmoisture.protocol import (
    SoilMoistureInputs, SoilMoistureOutputs, SoilMoisturePrediction,
    SoilMoisturePayload, ValidationResult
)
from gaia.tasks.defined_tasks.soilmoisture.validator.soil_scoring_mechanism import SoilScoringMechanism
from gaia.tasks.defined_tasks.soilmoisture.validator.soil_validator_preprocessing import SoilValidatorPreprocessing
from gaia.tasks.defined_tasks.soilmoisture.utils.soil_apis import (
    get_soil_data_parallel_flow
)
from gaia.parallel.core.executor import get_task_runner
from gaia.parallel.config.settings import TaskType
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

os.environ["PYTHONASYNCIODEBUG"] = "1"


class SoilValidatorTask(BaseTask):
    """
    A task class for processing and analyzing soil moisture data, with
    execution methods for both miner and validator workflows.
    """

    # Declare Pydantic fields
    db_manager: Union[ValidatorDatabaseManager, MinerDatabaseManager] = Field(
        default_factory=ValidatorDatabaseManager,
        description="Database manager for the task",
    )
    model: Optional[Any] = Field(
        default=None, description="The soil moisture prediction model"
    )
    node_type: str = Field(
        default="validator",
        description="Type of node running the task (validator or miner)"
    )
    test_mode: bool = Field(
        default=False,
        description="Whether to run in test mode (immediate execution, limited scope)"
    )

    prepare_flow: ClassVar[Any]
    execute_flow: ClassVar[Any]
    score_flow: ClassVar[Any]
    validator_execute: ClassVar[Any]
    validator_score: ClassVar[Any]
    
    get_next_preparation_time: ClassVar[Any]
    get_tasks_for_hour: ClassVar[Any]
    fetch_ground_truth: ClassVar[Any]
    score_tasks: ClassVar[Any]
    _fetch_soil_data: ClassVar[Any]
    _query_miners: ClassVar[Any]
    _process_scores: ClassVar[Any]
    move_task_to_history: ClassVar[Any]
    build_score_row: ClassVar[Any]
    _handle_prep_window: ClassVar[Any]
    _handle_execution_window: ClassVar[Any]
    _process_region: ClassVar[Any]
    _validate_region_data: ClassVar[Any]
    _prepare_task_data: ClassVar[Any]
    _update_region_status: ClassVar[Any]
    _clear_old_regions: ClassVar[Any]

    prediction_horizon: timedelta = Field(
        default_factory=lambda: timedelta(hours=6),
        description="Prediction horizon for the task",
    )
    scoring_delay: timedelta = Field(
        default_factory=lambda: timedelta(days=3),
        description="Delay before scoring due to SMAP data latency",
    )
    validator_preprocessing: Optional[Any] = None

    def __init__(self, db_manager=None, test_mode=False, **data):
        """Initialize the soil moisture validator task.
        
        Args:
            db_manager: Database manager instance
            test_mode: Whether to run in test mode
            **data: Additional task configuration
        """
        scoring_mechanism = SoilScoringMechanism(
            db_manager=db_manager,
            baseline_rmse=50,
            alpha=10,
            beta=0.1,
            task=None
        )
        
        super().__init__(
            name="SoilMoistureValidatorTask",
            description="Soil moisture prediction validation task",
            metadata=SoilMoistureMetadata().model_dump(),
            inputs=SoilMoistureInputs(),
            outputs=SoilMoistureOutputs(),
            scoring_mechanism=scoring_mechanism,
            db_manager=db_manager,
            test_mode=test_mode,
            **data
        )
        
        self.validator_preprocessing = SoilValidatorPreprocessing()
        self._prepared_regions = {}
        scoring_mechanism.task = self





   


