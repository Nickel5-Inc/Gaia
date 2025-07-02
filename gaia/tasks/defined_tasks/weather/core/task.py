"""
Weather Task Core Interface

Clean, minimal WeatherTask class that delegates functionality to specialized modules.
This replaces the massive 5,738-line monolithic weather_task.py file.
"""

import asyncio
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import torch
import xarray as xr
from fiber.logging_utils import get_logger
from pydantic import ConfigDict, Field

from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.tasks.base.task import Task
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager

from ..schemas.weather_metadata import WeatherMetadata
from ..schemas.weather_outputs import WeatherProgressUpdate
from .config import WeatherConfig, load_weather_config

logger = get_logger(__name__)

# Directory setup
DEFAULT_FORECAST_DIR_BG = Path("./miner_forecasts/")
MINER_FORECAST_DIR_BG = Path(os.getenv("MINER_FORECAST_DIR", DEFAULT_FORECAST_DIR_BG))
VALIDATOR_ENSEMBLE_DIR = Path("./validator_ensembles/")
MINER_FORECAST_DIR_BG.mkdir(parents=True, exist_ok=True)
VALIDATOR_ENSEMBLE_DIR.mkdir(parents=True, exist_ok=True)


class WeatherTask(Task):
    """
    Weather forecast generation and verification task.
    
    This is a clean, minimal interface that delegates functionality
    to specialized modules for maintainability and testability.
    """
    
    db_manager: Union[ValidatorDatabaseManager, MinerDatabaseManager]
    node_type: str = Field(default="validator")
    test_mode: bool = Field(default=False)
    era5_climatology_ds: Optional[xr.Dataset] = Field(default=None, exclude=True)

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    def __init__(
        self,
        db_manager=None,
        node_type=None,
        test_mode=False,
        keypair=None,
        inference_service_url: Optional[str] = None,
        runpod_api_key: Optional[str] = None,
        r2_config: Optional[Dict[str, str]] = None,
        **data,
    ):
        # Load configuration
        self.config = load_weather_config()
        
        # Override with any provided config
        if data:
            for key, value in data.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)

        super_data = {
            "name": "WeatherTask",
            "description": "Weather forecast generation and verification task",
            "task_type": "atomic",
            "metadata": WeatherMetadata(),
            "db_manager": db_manager,
            "node_type": node_type,
            "test_mode": test_mode,
            **data,
        }
        super().__init__(**super_data)

        # Core attributes
        self.keypair = keypair
        self.db_manager = db_manager
        self.node_type = node_type
        self.test_mode = test_mode
        self.r2_config = r2_config
        self.validator = data.get("validator")
        self.era5_climatology_ds = None
        self.inference_service_url = inference_service_url
        self.runpod_api_key = runpod_api_key

        # Initialize semaphores
        self.gpu_semaphore = asyncio.Semaphore(self.config.max_concurrent_inferences)
        era5_scoring_concurrency = int(
            os.getenv("WEATHER_VALIDATOR_ERA5_SCORING_CONCURRENCY", "4")
        )
        self.era5_scoring_semaphore = asyncio.Semaphore(era5_scoring_concurrency)

        # Background worker state
        self._init_worker_state()

        # Test mode tracking
        self.test_mode_run_scored_event = asyncio.Event()
        self.last_test_mode_run_id = None

        # Progress tracking
        self.progress_callbacks: List[callable] = []
        self.current_operations: Dict[str, WeatherProgressUpdate] = {}

        # Initialize node-specific components
        self._init_node_components()

        logger.info(f"WeatherTask initialized as {node_type}")

    def _init_worker_state(self):
        """Initialize background worker state tracking."""
        self.initial_scoring_queue = asyncio.Queue()
        self.initial_scoring_worker_running = False
        self.initial_scoring_workers = []
        self.final_scoring_worker_running = False
        self.final_scoring_workers = []
        self.cleanup_worker_running = False
        self.cleanup_workers = []
        self.r2_cleanup_worker_running = False
        self.r2_cleanup_workers = []
        self.job_status_logger_running = False
        self.job_status_logger_workers = []

    def _init_node_components(self):
        """Initialize node-specific components (miner or validator)."""
        if self.node_type == "miner":
            self._init_miner_components()
        else:
            self._init_validator_components()

    def _init_miner_components(self):
        """Initialize miner-specific components."""
        # File serving configuration
        file_serving_mode = os.getenv("WEATHER_FILE_SERVING_MODE", "local").lower()
        if file_serving_mode not in ["local", "r2_proxy"]:
            logger.warning(f"Invalid WEATHER_FILE_SERVING_MODE: {file_serving_mode}. Defaulting to 'local'.")
            file_serving_mode = "local"
        
        self.config.file_serving_mode = file_serving_mode
        logger.info(f"Weather file serving mode: {file_serving_mode}")

        # Initialize inference components
        self._init_inference_components()

    def _init_inference_components(self):
        """Initialize inference-related components for miners."""
        try:
            inference_type = os.getenv("WEATHER_INFERENCE_TYPE", "local_model").lower()
            self.config.weather_inference_type = inference_type
            
            if inference_type == "http_service":
                if not self.inference_service_url:
                    logger.error("HTTP Inference Service selected but no URL provided")
                    self.config.weather_inference_type = "local_model"
                    inference_type = "local_model"
                else:
                    logger.info(f"Using HTTP Inference Service: {self.inference_service_url}")

            if inference_type == "local_model":
                self._init_local_inference()
            elif inference_type in ["azure_foundry", "http_service"]:
                logger.info(f"Using {inference_type} for inference - local model not loaded")
                self.inference_runner = None
            else:
                logger.warning(f"Invalid inference type: {inference_type}. Using local_model")
                self._init_local_inference()

        except Exception as e:
            logger.error(f"Failed to initialize inference components: {e}")
            self.inference_runner = None

    def _init_local_inference(self):
        """Initialize local inference model."""
        try:
            from ..utils.inference_class import WeatherInferenceRunner
            
            device = "cuda" if torch.cuda.is_available() else "cpu"
            self.inference_runner = WeatherInferenceRunner(device=device, load_local_model=True)
            
            if self.inference_runner.model is not None:
                logger.info(f"Local inference model loaded on {device}")
            else:
                logger.error(f"Local inference model failed to load on {device}")
        except Exception as e:
            logger.error(f"Failed to initialize local inference: {e}")
            self.inference_runner = None

    def _init_validator_components(self):
        """Initialize validator-specific components."""
        logger.info("Initialized validator components for WeatherTask")

    # Core delegation methods - these will delegate to specialized modules

    async def validator_execute(self, validator):
        """Execute validator workflow - delegates to validator.core module."""
        from ..validator.core import execute_validator_workflow
        return await execute_validator_workflow(self, validator)

    async def validator_prepare_subtasks(self):
        """Prepare validator subtasks - delegates to validator.core module."""
        from ..validator.core import prepare_validator_subtasks
        return await prepare_validator_subtasks(self)

    async def validator_score(self, result=None, force_run_id=None):
        """Execute validator scoring - delegates to validator.scoring module."""
        from ..validator.scoring import execute_validator_scoring
        return await execute_validator_scoring(self, result, force_run_id)

    async def miner_execute(self, data: Dict[str, Any], miner) -> Optional[Dict[str, Any]]:
        """Execute miner workflow - delegates to miner.core module."""
        from ..miner.core import execute_miner_workflow
        return await execute_miner_workflow(self, data, miner)

    async def miner_preprocess(self, data: Optional[Dict[str, Any]] = None):
        """Preprocess miner data - delegates to miner.core module."""
        from ..miner.core import preprocess_miner_data
        return await preprocess_miner_data(self, data)

    # HTTP handler methods - delegate to miner.http_handlers module

    async def handle_initiate_fetch(self, request_data):
        """Handle initiate fetch request - delegates to miner.http_handlers module."""
        from ..miner.http_handlers import handle_initiate_fetch_request
        return await handle_initiate_fetch_request(self, request_data)

    async def handle_get_input_status(self, job_id: str):
        """Handle get input status request - delegates to miner.http_handlers module."""
        from ..miner.http_handlers import handle_get_input_status_request
        return await handle_get_input_status_request(self, job_id)

    async def handle_start_inference(self, job_id: str):
        """Handle start inference request - delegates to miner.http_handlers module."""
        from ..miner.http_handlers import handle_start_inference_request
        return await handle_start_inference_request(self, job_id)

    async def handle_kerchunk_request(self, job_id: str):
        """Handle kerchunk request - delegates to miner.http_handlers module."""
        from ..miner.http_handlers import handle_kerchunk_data_request
        return await handle_kerchunk_data_request(self, job_id)

    # Worker management methods - delegate to workers.manager module

    async def start_background_workers(self, **kwargs):
        """Start background workers - delegates to workers.manager module."""
        from ..workers.manager import start_background_workers
        return await start_background_workers(self, **kwargs)

    async def stop_background_workers(self):
        """Stop background workers - delegates to workers.manager module."""
        from ..workers.manager import stop_background_workers
        return await stop_background_workers(self)

    # Progress tracking methods - delegate to common.progress module

    async def register_progress_callback(self, callback: callable):
        """Register progress callback - delegates to common.progress module."""
        from ..common.progress import register_progress_callback
        return await register_progress_callback(self, callback)

    async def get_progress_status(self, job_id: Optional[str] = None):
        """Get progress status - delegates to common.progress module."""
        from ..common.progress import get_progress_status
        return await get_progress_status(self, job_id)

    # Cleanup methods

    async def cleanup_resources(self):
        """Clean up task resources."""
        try:
            await self.stop_background_workers()
            
            if hasattr(self, 'era5_climatology_ds') and self.era5_climatology_ds:
                try:
                    self.era5_climatology_ds.close()
                except:
                    pass
                    
            logger.info("WeatherTask resources cleaned up")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    # Compatibility methods for backward compatibility during transition
    
    def _load_config(self):
        """Backward compatibility method - configuration is now loaded in __init__."""
        return self.config.dict()

    async def build_score_row(self, *args, **kwargs):
        """Backward compatibility method - delegates to validator.scoring module."""
        from ..validator.scoring import build_score_row
        return await build_score_row(self, *args, **kwargs)