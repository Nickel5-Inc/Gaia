import os
import torch
import numpy as np
import traceback
import asyncio
import tempfile
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
    
    def _geoprep_process_miner_data_sync(self, processed_df):
        """Synchronous wrapper for geo_preprocessing.process_miner_data."""
        return self.geo_preprocessing.process_miner_data(processed_df)

    def _geomodel_predict_sync(self, processed_data):
        """Synchronous wrapper for geo_model.predict."""
        return self.geo_model.predict(processed_data)

    def _soilmodel_prepare_inputs_sync(self, data_dict):
        """Synchronous wrapper for preparing soil model inputs (moving to device)."""
        processed_d = {}
        for key, value in data_dict.items():
            if isinstance(value, torch.Tensor):
                processed_d[key] = value.to(self.device)
            else:
                processed_d[key] = value
        return processed_d

    def _soilmodel_inference_sync(self, sentinel_tensor, era5_tensor, elev_ndvi_tensor):
        """Synchronous wrapper for soil_model inference."""
        with torch.no_grad():
            return self.soil_model(
                sentinel=sentinel_tensor,
                era5=era5_tensor,
                elev_ndvi=elev_ndvi_tensor
            )

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
            
            loop = asyncio.get_event_loop() # Get event loop
            processed_data = await loop.run_in_executor(None, self._geoprep_process_miner_data_sync, processed_df)
            
            logger.info(f"GEO BASEMODEL: Processed data shape: {processed_data.shape}")
            logger.info(f"GEO BASEMODEL: Columns: {list(processed_data.columns)}")
            
            logger.info(f"GEO BASEMODEL: Running model prediction")
            
            if hasattr(self.geo_model, "run_inference"):
                logger.info("GEO BASEMODEL: Using custom geomagnetic model for inference (direct call)")
                predictions = self.geo_model.run_inference(processed_data)
            else:
                logger.info("GEO BASEMODEL: Using base geomagnetic model")
                raw_prediction = await loop.run_in_executor(None, self._geomodel_predict_sync, processed_data)
                
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
                if prediction_timestamp.tzinfo is not None:
                    prediction_timestamp_utc = prediction_timestamp.astimezone(timezone.utc)
                else:
                    prediction_timestamp_utc = prediction_timestamp.replace(tzinfo=timezone.utc)
                
                success = await self.db_manager.store_baseline_prediction(
                    task_name="geomagnetic",
                    task_id=task_id,
                    timestamp=prediction_timestamp_utc,
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
            
            for key, value in data.items():
                if isinstance(value, torch.Tensor):
                    logger.info(f"SOIL BASEMODEL: {key} tensor shape: {value.shape}")
                    logger.info(f"SOIL BASEMODEL: {key} min: {value.min().item():.4f}, max: {value.max().item():.4f}")
            
            loop = asyncio.get_event_loop() # Get event loop
            processed_data = await loop.run_in_executor(None, self._soilmodel_prepare_inputs_sync, data)
            
            logger.info("SOIL BASEMODEL: Running model prediction")
            
            # Offload inference to executor
            sentinel = processed_data['sentinel_ndvi'][:2].unsqueeze(0).to(self.device) # Keep initial tensor ops on main thread if fast
            era5 = processed_data['era5'].unsqueeze(0).to(self.device)
            elevation = processed_data['elevation']
            ndvi = processed_data['sentinel_ndvi'][2:3]
            elev_ndvi = torch.cat([elevation, ndvi], dim=0).unsqueeze(0).to(self.device)
            
            logger.info(f"SOIL BASEMODEL: Running soil model with inputs:")
            logger.info(f"SOIL BASEMODEL: - sentinel: {sentinel.shape}")
            logger.info(f"SOIL BASEMODEL: - era5: {era5.shape}")
            logger.info(f"SOIL BASEMODEL: - elev_ndvi: {elev_ndvi.shape}")
            
            raw_output = await loop.run_in_executor(None, self._soilmodel_inference_sync, sentinel, era5, elev_ndvi)

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
            result = await self.db_manager.get_baseline_prediction(task_name="geomagnetic", task_id=task_id)

            if result and 'prediction' in result:
                prediction_value = result['prediction']
                if isinstance(prediction_value, (float, int)):
                    logger.info(f"Retrieved geomagnetic baseline prediction for task_id {task_id}: {float(prediction_value)}")
                    return float(prediction_value)
                elif isinstance(prediction_value, dict) and 'value' in prediction_value and isinstance(prediction_value['value'], (float, int)):
                    logger.warning(f"Retrieved geomagnetic baseline prediction for task_id {task_id} in dict format: {prediction_value['value']}. Consider re-saving in simpler format.")
                    return float(prediction_value['value'])
                else:
                    logger.warning(f"Geomagnetic baseline prediction for task_id {task_id} has unexpected payload type or structure: {type(prediction_value)}, value: {prediction_value}")
                    return None
            else:
                logger.warning(f"No geomagnetic baseline prediction found or 'prediction' field missing for task_id {task_id}")
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

    async def _generate_missing_baseline_prediction(self, task_id: str, region_id: str) -> Optional[Dict]:
        """
        Generate a baseline prediction from original region data when one is missing.
        
        Args:
            task_id: The task ID for the prediction
            region_id: The region ID for the prediction
            
        Returns:
            Optional[Dict]: The generated baseline prediction or None if generation fails
        """
        if not self.db_manager:
            logger.warning("No database manager available, cannot generate baseline prediction")
            return None
            
        try:
            # Get the original region data from the database
            region_query = """
                SELECT r.*, r.combined_data, r.target_time, r.sentinel_bounds, r.sentinel_crs
                FROM soil_moisture_regions r
                WHERE r.id = :region_id
            """
            # Convert region_id to integer for database query
            try:
                region_id_int = int(region_id)
            except (ValueError, TypeError):
                logger.error(f"Invalid region_id format: {region_id} (type: {type(region_id)})")
                return None
            
            region_result = await self.db_manager.fetch_one(region_query, {"region_id": region_id_int})
            
            if not region_result:
                logger.error(f"No region data found for region_id {region_id}")
                return None
            
            if not region_result.get("combined_data"):
                logger.error(f"No combined_data found for region_id {region_id}")
                return None
            
            combined_data = region_result["combined_data"]
            target_time = region_result["target_time"]
            
            # Ensure target_time is a datetime object
            if isinstance(target_time, str):
                try:
                    target_time = datetime.fromisoformat(target_time)
                except ValueError:
                    try:
                        target_time = datetime.fromtimestamp(float(target_time), tz=timezone.utc)
                    except:
                        logger.error(f"Could not parse target_time: {target_time}")
                        return None
            
            if target_time.tzinfo is None:
                target_time = target_time.replace(tzinfo=timezone.utc)
            
            logger.info(f"Generating baseline prediction for region {region_id}, target_time {target_time}")
            logger.info(f"Combined data size: {len(combined_data) / (1024 * 1024):.2f} MB")
            
            # Import SoilMoistureInferencePreprocessor
            from gaia.tasks.defined_tasks.soilmoisture.utils.inference_class import SoilMoistureInferencePreprocessor
            
            # Process the combined data to generate model inputs
            model_inputs = None
            temp_file_path = None
            
            try:
                # Offload file writing and preprocessing to an executor
                def _write_and_preprocess_sync(data_bytes):
                    t_file_path = None
                    try:
                        with tempfile.NamedTemporaryFile(suffix='.tiff', delete=False) as temp_f:
                            temp_f.write(data_bytes)
                            t_file_path = temp_f.name
                        
                        s_preprocessor = SoilMoistureInferencePreprocessor()
                        m_inputs = s_preprocessor.preprocess(t_file_path)
                        return m_inputs, t_file_path
                    finally:
                        # Ensure temp file is cleaned up if preprocess fails before returning path
                        if t_file_path and (not 'm_inputs' in locals() or m_inputs is None) and os.path.exists(t_file_path):
                            try:
                                os.unlink(t_file_path)
                            except Exception as e_unlink_inner:
                                logger.error(f"Error cleaning temp file in sync helper: {e_unlink_inner}")

                loop = asyncio.get_event_loop()
                model_inputs, temp_file_path = await loop.run_in_executor(None, _write_and_preprocess_sync, combined_data)
                
                if model_inputs:
                    # Convert numpy arrays to torch tensors
                    for key, value in model_inputs.items():
                        if isinstance(value, np.ndarray):
                            model_inputs[key] = torch.from_numpy(value).float()
                
                    # Add metadata to model inputs
                    model_inputs["sentinel_bounds"] = region_result["sentinel_bounds"]
                    model_inputs["sentinel_crs"] = region_result["sentinel_crs"]
                    model_inputs["target_time"] = target_time
                    
                    logger.info(f"Preprocessed data for baseline generation. Keys: {list(model_inputs.keys())}")
                    
                    # Generate the baseline prediction
                    baseline_prediction = await self.predict_soil_and_store(
                        data=model_inputs,
                        task_id=task_id,
                        region_id=region_id
                    )
                    
                    if baseline_prediction:
                        logger.info(f"Successfully generated and stored baseline prediction for region {region_id}")
                        return baseline_prediction
                    else:
                        logger.error(f"Failed to generate baseline prediction for region {region_id}")
                        return None
                else:
                    logger.error(f"Preprocessing failed for baseline generation, region {region_id}")
                    return None
                    
            except Exception as e:
                logger.error(f"Error preprocessing data for baseline generation: {str(e)}")
                logger.error(traceback.format_exc())
                return None
            finally:
                # Clean up temporary file
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        os.unlink(temp_file_path)
                    except Exception as e:
                        logger.error(f"Error cleaning up temporary file: {str(e)}")
                        
        except Exception as e:
            logger.error(f"Error generating missing baseline prediction: {e}")
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
                
                # Try to generate baseline prediction from original region data
                logger.info(f"Attempting to generate missing baseline prediction for task_id {task_id}, region {region_id_str}")
                baseline_prediction = await self._generate_missing_baseline_prediction(task_id, region_id_str)
                
                if baseline_prediction is None:
                    logger.error(f"Failed to generate baseline prediction for task_id {task_id}, region {region_id_str}")
                    return None
                else:
                    logger.info(f"Successfully generated missing baseline prediction for task_id {task_id}, region {region_id_str}")
            
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
                smap_file_path=smap_file_path if smap_file_path else None,
                test_mode=self.test_mode
            )
            
            if metrics is None:
                logger.error(f"Failed to compute metrics for baseline prediction, task_id {task_id}, region {region_id_str}")
                return None
                
            logger.info(f"Computed metrics: {metrics}")
            
            score_result = self.soil_scoring.compute_final_score(metrics)
            
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
   