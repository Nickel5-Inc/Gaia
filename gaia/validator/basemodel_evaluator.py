import os
import torch
import numpy as np
import traceback
import asyncio
from fiber.logging_utils import get_logger
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any, List, Tuple, Union
import pandas as pd
from gaia.models.geomag_basemodel import GeoMagBaseModel
from gaia.models.soil_moisture_basemodel import SoilModel
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import GeomagneticPreprocessing
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_scoring_mechanism import GeomagneticScoringMechanism
from gaia.tasks.defined_tasks.soilmoisture.soil_scoring_mechanism import SoilScoringMechanism
from gaia.tasks.defined_tasks.soilmoisture.soil_miner_preprocessing import SoilMinerPreprocessing

logger = get_logger(__name__)

class BaseModelEvaluator:
    """
    Handles initialization and execution of baseline models for scoring comparison.
    
    This class is responsible for:
    1. Loading the baseline models for geomagnetic and soil moisture tasks
    2. Running these models on the same input data provided to miners
    3. Providing baseline scores to compare against miner performance
    
    It uses the same preprocessing, inference, and scoring methods as miners.
    """
    
    def __init__(self, db_manager=None, test_mode: bool = False):
        """
        Initialize the BaseModelEvaluator.
        
        Args:
            db_manager: Database manager for storing/retrieving predictions
            test_mode: If True, runs in test mode with limited resources
        """
        self.test_mode = test_mode
        self.db_manager = db_manager
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        self.geo_model = None
        self.soil_model = None
        
        self.geo_model_initialized = False
        self.soil_model_initialized = False
        
        self.geo_preprocessing = GeomagneticPreprocessing()
        self.soil_preprocessing = SoilMinerPreprocessing()
        
        self.geo_scoring = GeomagneticScoringMechanism(db_manager=db_manager) if db_manager else None
        self.soil_scoring = SoilScoringMechanism(db_manager=db_manager) if db_manager else None
        
        logger.info(f"BaseModelEvaluator initialized. Using device: {self.device}")
    
    async def initialize_models(self):
        """Initialize both baseline models asynchronously."""
        try:
            await asyncio.gather(
                self.initialize_geo_model(),
                self.initialize_soil_model()
            )
            logger.info("All baseline models initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize baseline models: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def initialize_geo_model(self):
        """Initialize the geomagnetic baseline model."""
        try:
            logger.info("Initializing geomagnetic baseline model")
            self.geo_model = GeoMagBaseModel()
            self.geo_model_initialized = True
            logger.info("Geomagnetic baseline model initialized")
            
            if hasattr(self.geo_model, 'is_fallback') and self.geo_model.is_fallback:
                logger.warning("Using fallback geomagnetic model (Prophet)")
            
            return True
        except Exception as e:
            logger.error(f"Failed to initialize geomagnetic model: {e}")
            logger.error(traceback.format_exc())
            self.geo_model_initialized = False
            return False
    
    async def initialize_soil_model(self):
        """Initialize the soil moisture baseline model."""
        try:
            logger.info("Initializing soil moisture baseline model")
            self.soil_model = SoilModel()
            self.soil_model = self.soil_model.to(self.device)
            self.soil_model.eval()
            self.soil_model_initialized = True

            logger.info("Soil moisture baseline model initialized")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize soil moisture model: {e}")
            logger.error(traceback.format_exc())
            self.soil_model_initialized = False
            return False
    
    async def predict_geo_and_store(self, data: pd.DataFrame, task_id: str) -> Optional[Dict]:
        """
        Make a geomagnetic prediction using the same method miners use and store it in the database.
        
        Args:
            data: The input data for the geomagnetic prediction (DataFrame with timestamp and value columns)
            task_id: Unique identifier for the task execution
            
        Returns:
            Optional[Dict]: The prediction result or None if prediction fails
        """
        if not self.geo_model_initialized:
            logger.warning("Geomagnetic model not initialized. Attempting to initialize...")
            await self.initialize_geo_model()
            if not self.geo_model_initialized:
                logger.error("Failed to initialize geomagnetic model")
                return None
        
        try:
            processed_data = self.geo_preprocessing.preprocess(data)
            prediction_result = self.geo_preprocessing.predict_next_hour(processed_data, model=self.geo_model)
            
            if not prediction_result:
                logger.error("Failed to get geo prediction result")
                return None
                
            if self.db_manager:
                await self.db_manager.store_baseline_prediction(
                    task_name="geomagnetic",
                    task_id=task_id,
                    timestamp=prediction_result["timestamp"],
                    prediction=prediction_result["predicted_value"]
                )
                logger.info(f"Stored geomagnetic baseline prediction: {prediction_result['predicted_value']}")
            else:
                logger.warning("No database manager available, prediction not stored")
            
            return prediction_result
        except Exception as e:
            logger.error(f"Error in geomagnetic prediction: {e}")
            logger.error(traceback.format_exc())
            return None
    
    async def predict_soil_and_store(self, 
                                    data: Dict[str, Any],
                                    task_id: str,
                                    region_id: str) -> Optional[Dict]:
        """
        Make a soil moisture prediction using the same method miners use and store it in the database.
        
        Args:
            data: The input data for soil moisture prediction, including sentinel, ERA5, and elevation/NDVI data
            task_id: Unique identifier for the task execution
            region_id: Identifier for the specific region
            
        Returns:
            Optional[Dict]: The prediction data or None if prediction fails
        """
        if not self.soil_model_initialized:
            logger.warning("Soil moisture model not initialized. Attempting to initialize...")
            await self.initialize_soil_model()
            if not self.soil_model_initialized:
                logger.error("Failed to initialize soil moisture model")
                return None
        
        try:
            logger.info(f"Running soil moisture baseline prediction for task {task_id}, region {region_id}")
            
            model_inputs = await self.soil_preprocessing.process_miner_data(data)
            prediction = self.soil_preprocessing.predict_smap(model_inputs, self.soil_model)
            
            if not prediction:
                logger.error("Failed to get soil prediction result")
                return None
            
            prediction_data = {
                "surface_sm": prediction["surface_sm"],
                "rootzone_sm": prediction["rootzone_sm"],
                "sentinel_bounds": data.get("sentinel_bounds"),
                "sentinel_crs": data.get("sentinel_crs"),
                "target_time": data.get("target_time", datetime.now(timezone.utc))
            }
            
            if self.db_manager:
                await self.db_manager.store_baseline_prediction(
                    task_name="soil_moisture",
                    task_id=task_id,
                    timestamp=prediction_data["target_time"],
                    prediction=prediction_data,
                    region_id=region_id
                )
                logger.info(f"Stored soil moisture baseline prediction for region {region_id}")
            else:
                logger.warning("No database manager available, prediction not stored")
            
            return prediction_data
        except Exception as e:
            logger.error(f"Error in soil moisture prediction: {e}")
            logger.error(traceback.format_exc())
            return None
    
    async def get_geo_baseline_prediction(self, task_id: str) -> Optional[float]:
        """
        Retrieve a geomagnetic baseline prediction from the database.
        
        Args:
            task_id: ID of the specific task execution
            
        Returns:
            Optional[float]: The prediction or None if not found
        """
        if not self.db_manager:
            logger.warning("No database manager available, cannot retrieve prediction")
            return None
            
        try:
            result = await self.db_manager.get_baseline_prediction(
                task_name="geomagnetic",
                task_id=task_id
            )
            
            if result:
                return result["prediction"]
            
            logger.warning(f"No geomagnetic baseline prediction found for task {task_id}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving geomagnetic baseline prediction: {e}")
            logger.error(traceback.format_exc())
            return None
    
    async def get_soil_baseline_prediction(self, task_id: str, region_id: str) -> Optional[Dict]:
        """
        Retrieve a soil moisture baseline prediction from the database.
        
        Args:
            task_id: ID of the specific task execution
            region_id: Identifier for the specific region
            
        Returns:
            Optional[Dict]: The prediction data or None if not found
        """
        if not self.db_manager:
            logger.warning("No database manager available, cannot retrieve prediction")
            return None
            
        try:
            result = await self.db_manager.get_baseline_prediction(
                task_name="soil_moisture",
                task_id=task_id,
                region_id=region_id
            )
            
            if result:
                return result["prediction"]
            
            logger.warning(f"No soil moisture baseline prediction found for task {task_id}, region {region_id}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving soil moisture baseline prediction: {e}")
            logger.error(traceback.format_exc())
            return None
    
    async def cleanup(self):
        """Release resources held by the models."""
        try:
            self.geo_model = None
            self.soil_model = None
            
            import gc
            gc.collect()
            
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                
            logger.info("BaseModelEvaluator resources cleaned up")
        except Exception as e:
            logger.error(f"Error during BaseModelEvaluator cleanup: {e}")
   