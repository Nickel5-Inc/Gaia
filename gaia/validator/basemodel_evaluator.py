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
            await self.initialize_geo_model()
            if not self.geo_model_initialized:
                logger.error("Failed to initialize geomagnetic model")
                return None
        
        try:
            logger.info(f"GEO BASEMODEL: Processing data for task_id={task_id}")
            
            if data.empty:
                logger.error("GEO BASEMODEL: Empty DataFrame provided")
                return None
                
            processed_df = data.copy()
            
            if 'Dst' in processed_df.columns and 'value' not in processed_df.columns:
                processed_df = processed_df.rename(columns={'Dst': 'value'})
            
            if 'timestamp' not in processed_df.columns or 'value' not in processed_df.columns:
                logger.error(f"GEO BASEMODEL: Missing required columns. Found: {list(processed_df.columns)}")
                return None
            
            processed_data = self.geo_preprocessing.process_miner_data(processed_df)
            
            logger.info(f"GEO BASEMODEL: Processed data shape: {processed_data.shape}")
            logger.info(f"GEO BASEMODEL: Columns: {list(processed_data.columns)}")
            
            logger.info(f"GEO BASEMODEL: Running model prediction")
            
            if hasattr(self.geo_model, "run_inference"):
                # If we have a custom model implementation
                logger.info("GEO BASEMODEL: Using custom geomagnetic model for inference")
                predictions = self.geo_model.run_inference(processed_data)
            else:
                # This is the standard path for the base model
                logger.info("GEO BASEMODEL: Using base geomagnetic model")
                # Use the same run_model_inference logic from GeomagneticTask
                raw_prediction = self.geo_model.predict(processed_data)
                
                # Handle NaN or infinite values
                if np.isnan(raw_prediction) or np.isinf(raw_prediction):
                    logger.warning("GEO BASEMODEL: Model returned NaN/Inf, using fallback value")
                    raw_prediction = float(processed_data["y"].iloc[-1])
                
                # Format prediction result like the miner does
                predictions = {
                    "predicted_value": float(raw_prediction),
                    "timestamp": processed_df["timestamp"].iloc[-1]
                }
            
            logger.info(f"GEO BASEMODEL: Prediction result: {predictions['predicted_value']}")
            
            # Store the prediction in the database
            if self.db_manager:
                success = await self.db_manager.store_baseline_prediction(
                    task_name="geomagnetic",
                    task_id=task_id,
                    timestamp=predictions["timestamp"],
                    prediction=predictions["predicted_value"]
                )
                logger.info(f"GEO BASEMODEL: Storage {'successful' if success else 'failed'}")
            
            return predictions
            
        except Exception as e:
            logger.error(f"GEO BASEMODEL ERROR: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    async def predict_soil_and_store(self, 
                                    data: Dict[str, Any],
                                    task_id: str,
                                    region_id: str) -> Optional[Dict]:
        """
        Make a soil moisture prediction using the same method miners use and store it in the database.
        
        Args:
            data: The input data for soil moisture prediction, including sentinel_ndvi, era5, and elevation data
            task_id: Unique identifier for the task execution
            region_id: Identifier for the specific region
            
        Returns:
            Optional[Dict]: The prediction data or None if prediction fails
        """
        if not self.soil_model_initialized:
            await self.initialize_soil_model()
            if not self.soil_model_initialized:
                logger.error("Failed to initialize soil moisture model")
                return None
        
        try:
            logger.info(f"SOIL BASEMODEL: Processing data for task_id={task_id}, region={region_id}")
            input_keys = list(data.keys())
            logger.info(f"SOIL BASEMODEL: Input data keys: {input_keys}")
            
            for key in ['sentinel_ndvi', 'era5', 'elevation']:
                if key in data and isinstance(data[key], torch.Tensor):
                    logger.info(f"SOIL BASEMODEL: {key} tensor shape: {data[key].shape}")
                    logger.info(f"SOIL BASEMODEL: {key} min: {data[key].min().item():.4f}, max: {data[key].max().item():.4f}")
            
            processed_data = {}
            for key, value in data.items():
                if isinstance(value, torch.Tensor):
                    processed_data[key] = value.to(self.device)
                else:
                    processed_data[key] = value
            
            logger.info("SOIL BASEMODEL: Running model prediction")
            
            with torch.no_grad():
                sentinel = processed_data['sentinel_ndvi'][:2].unsqueeze(0).to(self.device)
                era5 = processed_data['era5'].unsqueeze(0).to(self.device)
                elevation = processed_data['elevation']
                ndvi = processed_data['sentinel_ndvi'][2:3]
                elev_ndvi = torch.cat([elevation, ndvi], dim=0).unsqueeze(0).to(self.device)
                
                logger.info(f"SOIL BASEMODEL: Running soil model with inputs:")
                logger.info(f"SOIL BASEMODEL: - sentinel: {sentinel.shape}")
                logger.info(f"SOIL BASEMODEL: - era5: {era5.shape}")
                logger.info(f"SOIL BASEMODEL: - elev_ndvi: {elev_ndvi.shape}")
                
                raw_output = self.soil_model(
                    sentinel=sentinel,
                    era5=era5,
                    elev_ndvi=elev_ndvi
                )

                predictions = {
                    'surface': raw_output[:, 0].squeeze().cpu().numpy(),
                    'rootzone': raw_output[:, 1].squeeze().cpu().numpy()
                }
            
            if not predictions or not isinstance(predictions, dict) or 'surface' not in predictions or 'rootzone' not in predictions:
                logger.error(f"SOIL BASEMODEL: Invalid prediction output. Keys: {list(predictions.keys()) if predictions else 'None'}")
                return None
            
            logger.info(
                f"SOIL BASEMODEL: Surface stats - Min: {predictions['surface'].min():.3f}, "
                f"Max: {predictions['surface'].max():.3f}, "
                f"Mean: {predictions['surface'].mean():.3f}"
            )
            logger.info(
                f"SOIL BASEMODEL: Root zone stats - Min: {predictions['rootzone'].min():.3f}, "
                f"Max: {predictions['rootzone'].max():.3f}, "
                f"Mean: {predictions['rootzone'].mean():.3f}"
            )
            
            target_time = data.get("target_time", datetime.now(timezone.utc))
            if isinstance(target_time, str):
                target_time = datetime.fromisoformat(target_time)
                
            prediction_data = {
                "surface_sm": predictions["surface"].astype(float),
                "rootzone_sm": predictions["rootzone"].astype(float),
                "sentinel_bounds": data.get("sentinel_bounds"),
                "sentinel_crs": data.get("sentinel_crs"),
                "target_time": target_time
            }
            
            if self.db_manager:
                success = await self.db_manager.store_baseline_prediction(
                    task_name="soil_moisture",
                    task_id=task_id,
                    timestamp=prediction_data["target_time"],
                    prediction=prediction_data,
                    region_id=region_id
                )
                logger.info(f"SOIL BASEMODEL: Storage {'successful' if success else 'failed'}")
            
            return prediction_data
            
        except Exception as e:
            logger.error(f"SOIL BASEMODEL ERROR: {str(e)}")
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
                prediction = result["prediction"]
                
                if isinstance(prediction, dict) and "value" in prediction:
                    return prediction["value"]
                
                if isinstance(prediction, (int, float)):
                    return float(prediction)
                
                logger.warning(f"Unexpected prediction format: {type(prediction)}")
                return None
            
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
   