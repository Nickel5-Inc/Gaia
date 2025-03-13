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
from huggingface_hub import hf_hub_download

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
            self.geo_model = GeoMagBaseModel(random_seed=42)
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
            
            model_dir = os.path.join("gaia", "models", "checkpoints", "soil_moisture")
            os.makedirs(model_dir, exist_ok=True)
            local_path = os.path.join(model_dir, "SoilModel.ckpt")

            if os.path.exists(local_path):
                logger.info(f"Loading soil model from local path: {local_path}")
                self.soil_model = SoilModel.load_from_checkpoint(local_path)
            else:
                logger.info("Local checkpoint not found, downloading from HuggingFace...")
                checkpoint_path = await asyncio.to_thread(
                    hf_hub_download,
                    repo_id="Nickel5HF/soil-moisture-model",
                    filename="SoilModel.ckpt",
                    local_dir=model_dir
                )
                logger.info(f"Loading soil model from HuggingFace: {checkpoint_path}")
                self.soil_model = SoilModel.load_from_checkpoint(checkpoint_path)

            self.soil_model = self.soil_model.to(self.device)
            self.soil_model.eval()
            
            param_count = sum(p.numel() for p in self.soil_model.parameters())
            logger.info(f"Soil model loaded successfully with {param_count:,} parameters")
            logger.info(f"Soil model device: {next(self.soil_model.parameters()).device}")
            
            self.soil_model_initialized = True
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
            
            prediction_timestamp = processed_df["timestamp"].iloc[-1]
            if isinstance(prediction_timestamp, pd.Timestamp):
                prediction_timestamp = prediction_timestamp.to_pydatetime()
                if prediction_timestamp.tzinfo is None:
                    prediction_timestamp = prediction_timestamp.replace(tzinfo=timezone.utc)
                    
            logger.info(f"GEO BASEMODEL: Prediction timestamp: {prediction_timestamp}")
            
            processed_data = self.geo_preprocessing.process_miner_data(processed_df)
            
            logger.info(f"GEO BASEMODEL: Processed data shape: {processed_data.shape}")
            logger.info(f"GEO BASEMODEL: Columns: {list(processed_data.columns)}")
            
            logger.info(f"GEO BASEMODEL: Running model prediction")
            
            if hasattr(self.geo_model, "run_inference"):
                logger.info("GEO BASEMODEL: Using custom geomagnetic model for inference")
                predictions = self.geo_model.run_inference(processed_data)
            else:
                logger.info("GEO BASEMODEL: Using base geomagnetic model")
                raw_prediction = self.geo_model.predict(processed_data)
                
                if np.isnan(raw_prediction) or np.isinf(raw_prediction):
                    logger.warning("GEO BASEMODEL: Model returned NaN/Inf, using fallback value")
                    raw_prediction = float(processed_data["y"].iloc[-1])
                
                predictions = {
                    "predicted_value": float(raw_prediction),
                    "timestamp": prediction_timestamp
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
                                    region_id: Union[str, int]) -> Optional[Dict]:
        """
        Make a soil moisture prediction using the same method miners use and store it in the database.
        
        Args:
            data: The input data for soil moisture prediction, including sentinel_ndvi, era5, and elevation data
            task_id: Unique identifier for the task execution
            region_id: Identifier for the specific region (can be string or int)
            
        Returns:
            Optional[Dict]: The prediction data or None if prediction fails
        """
        if not self.soil_model_initialized:
            await self.initialize_soil_model()
            if not self.soil_model_initialized:
                logger.error("Failed to initialize soil moisture model")
                return None
        
        try:
            region_id_str = str(region_id)
            
            logger.info(f"SOIL BASEMODEL: Processing data for task_id={task_id}, region={region_id_str}")
            input_keys = list(data.keys())
            logger.info(f"SOIL BASEMODEL: Input data keys: {input_keys}")
            
            target_time = data.get("target_time", datetime.now(timezone.utc))
            if isinstance(target_time, str):
                try:
                    target_time = datetime.fromisoformat(target_time)
                except ValueError:
                    try:
                        target_time = datetime.fromtimestamp(float(target_time), tz=timezone.utc)
                    except:
                        logger.warning(f"Could not parse target_time: {target_time}, using current time")
                        target_time = datetime.now(timezone.utc)
            
            if target_time.tzinfo is None:
                target_time = target_time.replace(tzinfo=timezone.utc)
                
            logger.info(f"SOIL BASEMODEL: Target time: {target_time}")
            
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
                    timestamp=target_time,
                    prediction=prediction_data,
                    region_id=region_id_str
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
            task_id: The task ID to retrieve the prediction for
            
        Returns:
            Optional[float]: The prediction value or None if not found
        """
        if not self.db_manager:
            logger.warning("No database manager available, cannot retrieve prediction")
            return None
            
        try:
            if not task_id:
                logger.warning("No task_id provided, cannot retrieve geomagnetic baseline prediction")
                return None
                
            logger.info(f"Retrieving geomagnetic baseline prediction for task_id: {task_id}")
            result = await self.db_manager.get_baseline_prediction(
                task_name="geomagnetic",
                task_id=task_id
            )
            
            if result and 'prediction' in result and 'value' in result['prediction']:
                return float(result['prediction']['value'])
            
            logger.warning(f"No geomagnetic baseline prediction found for task_id {task_id}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving geomagnetic baseline prediction: {e}")
            logger.error(traceback.format_exc())
            return None
    
    async def get_soil_baseline_prediction(self, task_id: str, region_id: Union[str, int]) -> Optional[Dict]:
        """
        Retrieve a soil moisture baseline prediction from the database.
        
        Args:
            task_id: The task ID to retrieve the prediction for
            region_id: Identifier for the specific region (can be string or int)
            
        Returns:
            Optional[Dict]: The prediction data or None if not found
        """
        if not self.db_manager:
            logger.warning("No database manager available, cannot retrieve prediction")
            return None
            
        try:
            region_id_str = str(region_id)
            
            result = await self.db_manager.get_baseline_prediction(
                task_name="soil_moisture",
                task_id=task_id,
                region_id=region_id_str
            )
            
            if result:
                return result["prediction"]
            
            logger.warning(f"No soil moisture baseline prediction found for task_id {task_id}, region {region_id_str}")
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
    
    async def score_geo_baseline(self, task_id: str, ground_truth: float) -> Optional[float]:
        """
        Score a geomagnetic baseline prediction against ground truth.
        
        Args:
            task_id: The task ID to retrieve the baseline prediction for
            ground_truth: The ground truth value to score against
            
        Returns:
            Optional[float]: The score, or None if no prediction was found
        """
        if not self.geo_scoring:
            logger.warning("Geomagnetic scoring mechanism not initialized")
            return None
        
        try:
            if not task_id:
                logger.warning("No task_id provided, cannot score geomagnetic baseline prediction")
                return None
                
            logger.info(f"Scoring geomagnetic baseline prediction for task_id: {task_id}")
            
            baseline_prediction = await self.get_geo_baseline_prediction(task_id)
            if baseline_prediction is None:
                logger.warning(f"No geomagnetic baseline prediction found for task_id {task_id}")
                return None
                
            score = self.geo_scoring.calculate_score(baseline_prediction, ground_truth)
            logger.info(f"Geomagnetic baseline score for task_id {task_id}: {score:.4f}")
            
            return score
        
        except Exception as e:
            logger.error(f"Error scoring geomagnetic baseline: {e}")
            logger.error(traceback.format_exc())
            return None

    async def score_soil_baseline(self, task_id: str, region_id: Union[str, int], ground_truth: Dict, 
                                smap_file_path: Optional[str] = None) -> Optional[float]:
        """
        Score a soil moisture baseline prediction against ground truth.
        
        Args:
            task_id: The task ID to retrieve the baseline prediction for
            region_id: The region ID for the prediction (can be string or int)
            ground_truth: The ground truth data to score against
            smap_file_path: Optional path to an already downloaded SMAP data file
            
        Returns:
            Optional[float]: The score, or None if no prediction was found
        """
        if not self.soil_scoring:
            logger.warning("Soil scoring mechanism not initialized")
            return None
        
        try:
            region_id_str = str(region_id)
            
            baseline_prediction = await self.get_soil_baseline_prediction(task_id, region_id_str)
            if baseline_prediction is None:
                logger.warning(f"No soil baseline prediction found for task_id {task_id}, region {region_id_str}")
                return None
            
            if "surface_sm" in baseline_prediction and "rootzone_sm" in baseline_prediction:
                device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
                surface = torch.tensor(baseline_prediction["surface_sm"]).float()
                rootzone = torch.tensor(baseline_prediction["rootzone_sm"]).float()
                
                if surface.dim() == 2:
                    surface = surface.unsqueeze(0).unsqueeze(0)
                if rootzone.dim() == 2:
                    rootzone = rootzone.unsqueeze(0).unsqueeze(0)
                
                tensor_predictions = torch.cat([surface, rootzone], dim=1).to(device)
                
                logger.info(f"Converted baseline prediction to tensor with shape: {tensor_predictions.shape}")
            else:
                logger.error(f"Baseline prediction missing required fields: {list(baseline_prediction.keys())}")
                return None
            
            target_time = baseline_prediction.get("target_time")
            if isinstance(target_time, str):
                try:
                    target_time = datetime.fromisoformat(target_time)
                except ValueError:
                    try:
                        target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M:%S%z")
                    except ValueError:
                        try:
                            target_time = datetime.fromtimestamp(float(target_time), tz=timezone.utc)
                        except:
                            logger.error(f"Could not parse target_time string: {target_time}")
                            return None
            
            if isinstance(target_time, datetime) and target_time.tzinfo is None:
                target_time = target_time.replace(tzinfo=timezone.utc)
                
            logger.info(f"Using target_time: {target_time} (type: {type(target_time)})")
            
            scoring_input = {
                "miner_id": "baseline",
                "miner_hotkey": "baseline",
                "predictions": tensor_predictions,
                "bounds": baseline_prediction.get("sentinel_bounds"),
                "crs": baseline_prediction.get("sentinel_crs"),
                "target_time": target_time,
                "ground_truth": ground_truth
            }
            
            if smap_file_path:
                scoring_input["smap_file"] = smap_file_path
                logger.info(f"Using provided SMAP file: {smap_file_path}")
            
            metrics = await self.soil_scoring.compute_smap_score_metrics(
                bounds=scoring_input["bounds"],
                crs=scoring_input["crs"],
                model_predictions=scoring_input["predictions"],
                target_date=scoring_input["target_time"],
                miner_id="baseline",
                smap_file_path=smap_file_path if smap_file_path else None
            )
            
            if metrics is None:
                logger.error(f"Failed to compute metrics for baseline prediction, task_id {task_id}, region {region_id_str}")
                return None
                
            logger.info(f"Computed metrics: {metrics}")
            
            score_result = await self.soil_scoring.compute_final_score(metrics)
            
            logger.info(f"Soil baseline score for task_id {task_id}, region {region_id_str}: {score_result:.4f}")
            return score_result
        
        except Exception as e:
            logger.error(f"Error scoring soil baseline: {e}")
            logger.error(traceback.format_exc())
            return None

    async def compare_geo_scores(self, task_id: str, ground_truth: float, miner_scores: List[Dict]) -> List[Dict]:
        """
        Compare miner geomagnetic scores with baseline score.
        
        Args:
            task_id: The task ID to compare
            ground_truth: The ground truth value
            miner_scores: List of miner score dictionaries
            
        Returns:
            List[Dict]: Updated miner scores with baseline comparison
        """
        baseline_score = await self.score_geo_baseline(task_id, ground_truth)
        
        if baseline_score is None:
            logger.warning(f"No baseline score available for comparison, task {task_id}")
            return miner_scores
        
        for score in miner_scores:
            miner_score = score.get("score", 0)
            logger.info(
                f"Score comparison - Miner: {score['miner_hotkey']}, "
                f"Score: {miner_score:.4f}, Baseline: {baseline_score:.4f}, "
                f"Difference: {miner_score - baseline_score:.4f}"
            )
        
        return miner_scores

    async def compare_soil_scores(self, task_id: str, region_id: Union[str, int], ground_truth: Dict, miner_score: Dict) -> Dict:
        """
        Compare miner soil moisture score with baseline score.
        
        Args:
            task_id: The task ID to compare
            region_id: The region ID to compare (can be string or int)
            ground_truth: The ground truth data
            miner_score: Miner score dictionary
            
        Returns:
            Dict: Updated miner score with baseline comparison
        """
        region_id_str = str(region_id)
        baseline_score = await self.score_soil_baseline(task_id, region_id_str, ground_truth)
        
        if baseline_score is None:
            logger.warning(f"No baseline score available for comparison, task {task_id}, region {region_id_str}")
            return miner_score
        
        miner_total = miner_score.get("total_score", 0)
        logger.info(
            f"Score comparison - Miner: {miner_score['miner_hotkey']}, Region: {region_id_str}, "
            f"Score: {miner_total:.4f}, Baseline: {baseline_score:.4f}, "
            f"Difference: {miner_total - baseline_score:.4f}"
        )
        
        return miner_score
   