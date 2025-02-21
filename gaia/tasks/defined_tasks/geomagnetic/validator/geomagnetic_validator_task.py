import os
import importlib.util
import traceback
import asyncio
from typing import Optional, Union
from pydantic import Field
import datetime

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.tasks.base.task import BaseTask
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_metadata import GeomagneticMetadata
from gaia.tasks.defined_tasks.geomagnetic.protocol import GeomagneticInputs, GeomagneticOutputs
from gaia.models.geomag_basemodel import GeoMagBaseModel
from fiber.logging_utils import get_logger

from gaia.tasks.defined_tasks.geomagnetic.validator.geomagnetic_flows import prepare_flow, execute_flow, score_flow
from gaia.tasks.defined_tasks.geomagnetic.validator.geomagnetic_prefect_tasks import (
    fetch_geomag_data, query_miners, process_scores, process_miner_responses
)
from gaia.tasks.defined_tasks.geomagnetic.validator.geomagnetic_helpers import (
    prepare_subtasks, calculate_score, add_task_to_queue, cleanup_resources
)

logger = get_logger(__name__)


class GeomagneticValidatorTask(BaseTask):
    """
    A task class for processing and analyzing geomagnetic data, with
    execution methods for both miner and validator workflows.

    This task involves:
        - Querying miners for predictions
        - Adding predictions to a queue for scoring
        - Fetching ground truth data
        - Scoring predictions
        - Moving scored tasks to history

    Attributes:
        name (str): The name of the task, set as "GeomagneticTask".
        description (str): A description of the task's purpose.
        task_type (str): Specifies the type of task (e.g., "atomic").
        metadata (GeomagneticMetadata): Metadata associated with the task.
        inputs (GeomagneticInputs): Handles data loading and validation.
        outputs (GeomagneticOutputs): Manages output formatting and saving.
    """

    # Declare Pydantic fields
    db_manager: Union[ValidatorDatabaseManager, MinerDatabaseManager] = Field(
        default_factory=ValidatorDatabaseManager,
        description="Database manager for the task",
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

    def __init__(self, node_type: str, db_manager, test_mode: bool = False, **data):
        """Initialize the task."""
        # Force test mode if the environment variable TEST_MODE is set
        if os.getenv("TEST_MODE", "false").lower() in ("true", "1"):
            test_mode = True
        
        metadata = GeomagneticMetadata()
        super().__init__(
            name="GeomagneticTask",
            description="Geomagnetic prediction task",
            task_type="atomic",
            metadata=metadata.model_dump(),
            inputs=GeomagneticInputs(),
            outputs=GeomagneticOutputs(),
            db_manager=db_manager,
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
