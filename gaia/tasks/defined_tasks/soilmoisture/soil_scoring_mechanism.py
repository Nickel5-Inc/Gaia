from datetime import datetime, timezone, timedelta
import tempfile
from gaia.tasks.base.components.scoring_mechanism import ScoringMechanism
from gaia.tasks.base.decorators import task_timer
import numpy as np
from typing import Dict, Optional, Any
from rasterio.coords import BoundingBox
from rasterio.crs import CRS
import torch
import torch.nn.functional as F
from torchmetrics.functional.image import structural_similarity_index_measure as ssim
from gaia.tasks.defined_tasks.soilmoisture.utils.smap_api import (
    construct_smap_url,
    download_smap_data,
    get_smap_data_for_sentinel_bounds,
)
from gaia.tasks.defined_tasks.soilmoisture.utils.evaluation_metrics import calculate_all_metrics
from skimage.metrics import structural_similarity as ssim
from pydantic import Field
from fiber.logging_utils import get_logger
import os
import traceback
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.sql import text
import glob
import asyncio
import psutil
import gc
import logging
import json
import concurrent.futures
from pathlib import Path
import math

logger = get_logger(__name__)

os.makedirs('logs', exist_ok=True)
metrics_logger = logging.getLogger('ExtendedMetricsLogger')
metrics_logger.setLevel(logging.INFO)
metrics_logger.propagate = False

file_handler = logging.FileHandler('logs/extended_metrics.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s|%(levelname)s|%(message)s')
file_handler.setFormatter(formatter)

if not metrics_logger.handlers:
    metrics_logger.addHandler(file_handler)


class SoilScoringMechanism(ScoringMechanism):
    """Scoring mechanism for soil moisture predictions."""

    alpha: float = Field(default=3, description="Sigmoid steepness parameter")
    beta: float = Field(default=0.15, description="Sigmoid midpoint parameter")
    baseline_rmse: float = Field(default=50, description="Baseline RMSE value")
    db_manager: Any = Field(default=None)
    task: Any = Field(default=None, description="Reference to the parent task")
    executor: concurrent.futures.ThreadPoolExecutor = Field(default_factory=concurrent.futures.ThreadPoolExecutor, exclude=True)

    def __init__(self, baseline_rmse: float = 50, alpha: float = 10, beta: float = 0.1, db_manager=None, task=None):
        super().__init__(
            name="SoilMoistureScoringMechanism",
            description="Evaluates soil moisture predictions using RMSE and SSIM",
            normalize_score=True,
            max_score=1.0,
        )
        self.alpha = alpha
        self.beta = beta
        self.baseline_rmse = baseline_rmse
        self.db_manager = db_manager
        self.task = task
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() or 1)

    def sigmoid_rmse(self, rmse: float) -> float:
        """Convert RMSE to score using sigmoid function. (higher is better)"""
        return 1 / (1 + torch.exp(self.alpha * (rmse - self.beta)))

    def _create_prediction_tensor_sync(self, pred_data_dict):
        """Synchronous helper to create prediction tensor."""
        surface_sm = torch.tensor(pred_data_dict["surface_sm"])
        rootzone_sm = torch.tensor(pred_data_dict["rootzone_sm"])
        return torch.stack([surface_sm, rootzone_sm], dim=0).unsqueeze(0)

    def compute_final_score(self, metrics: Dict) -> float:
        """Compute final score combining RMSE and SSIM metrics."""
        try:
            surface_rmse = metrics["validation_metrics"].get("surface_rmse", self.beta)
            rootzone_rmse = metrics["validation_metrics"].get("rootzone_rmse", self.beta)
            surface_ssim = metrics["validation_metrics"].get("surface_ssim", 0)
            rootzone_ssim = metrics["validation_metrics"].get("rootzone_ssim", 0)
            
            # Debug logging to identify NaN sources
            logger.debug(f"Input metrics - Surface RMSE: {surface_rmse}, Rootzone RMSE: {rootzone_rmse}, Surface SSIM: {surface_ssim}, Rootzone SSIM: {rootzone_ssim}")
            
            # Validate input values
            if not isinstance(surface_rmse, (int, float)) or math.isnan(surface_rmse) or math.isinf(surface_rmse):
                logger.error(f"Invalid surface_rmse value: {surface_rmse}")
                return float('nan')
            
            if not isinstance(rootzone_rmse, (int, float)) or math.isnan(rootzone_rmse) or math.isinf(rootzone_rmse):
                logger.error(f"Invalid rootzone_rmse value: {rootzone_rmse}")
                return float('nan')
                
            if not isinstance(surface_ssim, (int, float)) or math.isnan(surface_ssim) or math.isinf(surface_ssim):
                logger.warning(f"Invalid surface_ssim value: {surface_ssim}, setting to 0")
                surface_ssim = 0
                
            if not isinstance(rootzone_ssim, (int, float)) or math.isnan(rootzone_ssim) or math.isinf(rootzone_ssim):
                logger.warning(f"Invalid rootzone_ssim value: {rootzone_ssim}, setting to 0")
                rootzone_ssim = 0
            
            # Compute scores with validation
            surface_score_tensor = self.sigmoid_rmse(torch.tensor(surface_rmse, dtype=torch.float32))
            rootzone_score_tensor = self.sigmoid_rmse(torch.tensor(rootzone_rmse, dtype=torch.float32))
            
            # Extract values and validate
            surface_score = surface_score_tensor.item()
            rootzone_score = rootzone_score_tensor.item()
            
            logger.debug(f"Computed scores - Surface: {surface_score}, Rootzone: {rootzone_score}")
            
            if math.isnan(surface_score) or math.isinf(surface_score):
                logger.error(f"Invalid surface score: {surface_score} from RMSE: {surface_rmse}")
                return float('nan')
                
            if math.isnan(rootzone_score) or math.isinf(rootzone_score):
                logger.error(f"Invalid rootzone score: {rootzone_score} from RMSE: {rootzone_rmse}")
                return float('nan')
            
            final_score = 0.6 * surface_score + 0.4 * rootzone_score
            
            logger.debug(f"Final score calculation: 0.6 * {surface_score} + 0.4 * {rootzone_score} = {final_score}")
            
            if math.isnan(final_score) or math.isinf(final_score):
                logger.error(f"Final score is invalid: {final_score}")
                return float('nan')
            
            return final_score
            
        except Exception as e:
            logger.error(f"Error in compute_final_score: {str(e)}")
            logger.error(f"Metrics: {metrics}")
            return float('nan')

    async def validate_predictions(self, predictions: Dict) -> bool:
        """check predictions before scoring."""
        try:
            pred_data = predictions.get("predictions")
            if pred_data is None:
                logger.error("No predictions found in input")
                return False

            if isinstance(pred_data, dict):
                loop = asyncio.get_event_loop()
                model_predictions = await loop.run_in_executor(
                    self.executor, 
                    self._create_prediction_tensor_sync, 
                    pred_data
                )
            else:
                model_predictions = pred_data

            predictions["predictions"] = model_predictions
            return True

        except Exception as e:
            logger.error(f"Error validating predictions: {str(e)}")
            return False

    async def validate_metrics(self, metrics: Dict) -> bool:
        """Check metrics before final scoring."""
        try:
            validation_metrics = metrics.get("validation_metrics", {})

            has_surface = "surface_rmse" in validation_metrics
            has_rootzone = "rootzone_rmse" in validation_metrics

            if not (has_surface or has_rootzone):
                logger.error("No valid metrics found")
                return False

            if has_surface:
                if validation_metrics["surface_rmse"] < 0:
                    logger.error("Surface RMSE must be positive")
                    return False
                if "surface_ssim" in validation_metrics:
                    if not -1 <= validation_metrics["surface_ssim"] <= 1:
                        logger.error(
                            f"Surface SSIM {validation_metrics['surface_ssim']} outside valid range [-1,1]"
                        )
                        return False

            if has_rootzone:
                if validation_metrics["rootzone_rmse"] < 0:
                    logger.error("Rootzone RMSE must be positive")
                    return False
                if "rootzone_ssim" in validation_metrics:
                    if not -1 <= validation_metrics["rootzone_ssim"] <= 1:
                        logger.error(
                            f"Rootzone SSIM {validation_metrics['rootzone_ssim']} outside valid range [-1,1]"
                        )
                        return False

            return True

        except Exception as e:
            logger.error(f"Error validating metrics: {str(e)}")
            return False

    @task_timer
    async def score(self, predictions: Dict) -> Dict[str, float]:
        """Score predictions against SMAP ground truth."""
        try:
            if not await self.validate_predictions(predictions):
                logger.error("Invalid predictions")
                return None

            pred_info = await self.db_manager.fetch_one(
                """
                SELECT retry_count, next_retry_time, status, target_time
                FROM soil_moisture_predictions 
                WHERE miner_uid = :miner_id
                AND (
                    (status = 'retry_scheduled' AND next_retry_time IS NOT NULL)
                    OR 
                    (status = 'sent_to_miner' AND target_time = :target_time)
                )
                ORDER BY next_retry_time DESC NULLS LAST
                LIMIT 1
                """,
                {
                    "miner_id": predictions["miner_id"],
                    "target_time": predictions["target_time"]
                }
            )
            
            # Note: Retry wait period checking is now handled at the task level in get_pending_tasks()
            # The scoring mechanism should process any task that reaches it, as the task-level logic
            # has already determined it's ready for processing
            if pred_info and pred_info.get("status") == "retry_scheduled":
                logger.info(f"Processing retry for timestamp {predictions['target_time']} - retry time has passed")
            elif pred_info and pred_info.get("status") == "sent_to_miner":
                logger.info(f"Processing new prediction for timestamp {predictions['target_time']}")
            else:
                logger.info(f"Processing prediction for timestamp {predictions['target_time']}")

            # Proceed with scoring - the task-level logic has already validated this task is ready

            metrics = await self.compute_smap_score_metrics(
                bounds=predictions["bounds"],
                crs=predictions["crs"],
                model_predictions=predictions["predictions"],
                target_date=predictions["target_time"],
                miner_id=predictions["miner_id"],
                smap_file_path=predictions.get("smap_file_path"),
                test_mode=predictions.get("test_mode")
            )

            if isinstance(metrics, dict) and metrics.get("status") == "retry_scheduled":
                logger.info(f"SMAP data unavailable, retry scheduled: {metrics.get('message')}")
                return None

            if not metrics:
                return None

            if not await self.validate_metrics(metrics):
                logger.error("Invalid metrics computed")
                return None

            total_score = self.compute_final_score(metrics)

            if not 0 <= total_score <= 1:
                logger.error(f"Final score {total_score} outside valid range [0,1]")
                return None

            logger.info(f"Computed metrics: {metrics['validation_metrics']}")
            logger.info(f"Total score: {total_score:.4f}")

            return {
                "miner_id": predictions.get("miner_id"),
                "miner_hotkey": predictions.get("miner_hotkey"),
                "metrics": metrics["validation_metrics"],
                "total_score": total_score,
                "timestamp": predictions["target_time"],
                "ground_truth": metrics.get("ground_truth"),
            }

        except Exception as e:
            logger.error(f"Error in scoring: {str(e)}")
            return None

    async def compute_smap_score_metrics(
        self,
        bounds: tuple[float, float, float, float],
        crs: float,
        model_predictions: torch.Tensor,
        target_date: datetime,
        miner_id: str,
        smap_file_path: Optional[str] = None,
        test_mode: bool = None
    ) -> dict:
        """
        Compute RMSE and SSIM between model predictions and SMAP data for valid pixels only.
        
        Args:
            bounds: The bounding box of the area
            crs: The coordinate reference system
            model_predictions: The model predictions as a tensor
            target_date: The target date for comparison
            miner_id: ID of the miner
            smap_file_path: Optional path to an already downloaded SMAP data file (for baseline scoring)
            test_mode: Optional flag for test mode behavior
            
        Returns:
            dict: Dictionary containing metrics like RMSE and SSIM, or None if error
        """
        process = psutil.Process() if psutil else None
        mem_before = process.memory_info().rss / (1024 * 1024) if process else 0
        loop = asyncio.get_event_loop() # Get event loop early

        try:
            if self.task and self.task.test_mode:
                target_date = target_date - timedelta(days=7)
                logger.info(f"TEST MODE: Scoring with SMAP data from 7 days prior: {target_date}")

            logger.info(f"Computing SMAP score metrics for miner {miner_id}, target_date {target_date}")
            
            smap_url = construct_smap_url(target_date)
            temp_smap_filename = None # Ensure it's defined for finally block

            if smap_file_path and os.path.exists(smap_file_path):
                logger.info(f"Using provided SMAP file: {smap_file_path}")
                temp_smap_filename = smap_file_path
            else:
                # Use a unique temporary file for download
                cache_dir = Path("smap_cache")
                cache_dir.mkdir(exist_ok=True)
                
                # Check if the file already exists in cache based on URL
                cached_file_name = Path(smap_url).name
                potential_cache_path = cache_dir / cached_file_name

                if await loop.run_in_executor(self.executor, potential_cache_path.exists):
                    logger.info(f"Found SMAP data in cache: {potential_cache_path}")
                    temp_smap_filename = str(potential_cache_path)
                else:
                    # If not in cache, download it
                    unique_suffix = f"_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}_{os.getpid()}"
                    temp_h5_file = tempfile.NamedTemporaryFile(
                        suffix=f"{unique_suffix}.h5", 
                        dir=cache_dir, # download directly to cache dir with .part or unique name
                        delete=False 
                    )
                    temp_smap_filename = temp_h5_file.name
                    temp_h5_file.close() # Close it so download_smap_data can open it

                    logger.info(f"Downloading SMAP data from {smap_url} to {temp_smap_filename}")
                    download_success = await download_smap_data(smap_url, temp_smap_filename)

                    if not download_success:
                        logger.error(f"Failed to download SMAP data from {smap_url}")
                        # Schedule retry for the miner
                        await self.schedule_retry_for_miner(miner_id, target_date, "SMAP download failed")
                        return {"status": "retry_scheduled", "message": "SMAP download failed"}

            if not temp_smap_filename or not os.path.exists(temp_smap_filename):
                 logger.error(f"SMAP data file {temp_smap_filename} not found or not accessible after download/cache check.")
                 await self.schedule_retry_for_miner(miner_id, target_date, "SMAP file not found post-download")
                 return {"status": "retry_scheduled", "message": "SMAP file not found post-download"}

            logger.info(f"Using SMAP file: {temp_smap_filename} for bounds: {bounds}, CRS: {crs}")
            smap_data = await get_smap_data_for_sentinel_bounds(
                temp_smap_filename, bounds, str(crs)
            )

            if not smap_data:
                logger.error(f"Failed to process SMAP data for bounds {bounds}, CRS {crs}")
                await self.schedule_retry_for_miner(miner_id, target_date, "SMAP processing failed")
                return {"status": "retry_scheduled", "message": "SMAP processing failed"}
            
            # Move model_predictions to CPU before metric calculation if it's on GPU
            original_model_predictions_device = model_predictions.device
            model_predictions = model_predictions.cpu()

            # Ensure smap_data is processed correctly
            if not isinstance(smap_data, dict) or not all(k in smap_data for k in ['surface_sm', 'rootzone_sm']):
                logger.error(f"SMAP data is not in the expected dictionary format or missing keys. Got: {type(smap_data)}")
                return None

            # Convert SMAP data to tensors
            smap_surface = torch.from_numpy(smap_data["surface_sm"]).float()
            smap_rootzone = torch.from_numpy(smap_data["rootzone_sm"]).float()
            smap_tensor = torch.stack([smap_surface, smap_rootzone], dim=0).unsqueeze(0)

            # --- Calculate RMSE and SSIM metrics ---
            def _calculate_soil_metrics_sync(preds, truth):
                """Calculate RMSE and SSIM metrics for soil moisture predictions."""
                try:
                    # Convert PyTorch tensors to numpy arrays if needed
                    preds_np = preds.detach().cpu().numpy() if hasattr(preds, 'detach') else preds
                    truth_np = truth.detach().cpu().numpy() if hasattr(truth, 'detach') else truth
                    
                    # Check for NaN or inf values in input data
                    pred_nan_count = np.sum(np.isnan(preds_np))
                    pred_inf_count = np.sum(np.isinf(preds_np))
                    truth_nan_count = np.sum(np.isnan(truth_np))
                    truth_inf_count = np.sum(np.isinf(truth_np))
                    
                    if pred_nan_count > 0 or pred_inf_count > 0:
                        logger.error(f"Predictions contain invalid values: nan_count={pred_nan_count}, inf_count={pred_inf_count}")
                        return None
                    
                    # SMAP ground truth commonly contains NaN values due to:
                    # - Cloud cover, instrument issues, geographic coverage gaps
                    # - This is normal and expected behavior
                    if truth_nan_count > 0:
                        logger.info(f"SMAP ground truth contains {truth_nan_count} NaN values (normal due to satellite data gaps)")
                        
                        # If too many NaN values (>90% of pixels), the region might have insufficient data
                        total_pixels = truth_np.size
                        nan_percentage = (truth_nan_count / total_pixels) * 100
                        
                        if nan_percentage > 90:
                            logger.warning(f"SMAP data has {nan_percentage:.1f}% NaN values - insufficient valid data for scoring")
                            return None
                        
                        logger.info(f"SMAP data quality: {nan_percentage:.1f}% NaN values, proceeding with valid pixels only")
                    
                    if truth_inf_count > 0:
                        logger.error(f"Ground truth contains inf values: inf_count={truth_inf_count}")
                        return None
                    
                    # Ensure we have the right shape: [batch, channels, height, width]
                    if preds_np.ndim == 4 and truth_np.ndim == 4:
                        # Extract surface (channel 0) and rootzone (channel 1) predictions
                        surface_pred = preds_np[0, 0]  # [height, width]
                        rootzone_pred = preds_np[0, 1]  # [height, width]
                        surface_truth = truth_np[0, 0]  # [height, width]
                        rootzone_truth = truth_np[0, 1]  # [height, width]
                    else:
                        logger.error(f"Unexpected tensor shapes: preds {preds_np.shape}, truth {truth_np.shape}")
                        return None
                    
                    # Calculate metrics using only valid (non-NaN) pixels
                    def calculate_rmse_with_valid_pixels(pred, truth):
                        """Calculate RMSE using only pixels where both prediction and truth are valid."""
                        # Create mask for valid pixels (not NaN in either prediction or truth)
                        valid_mask = ~(np.isnan(pred) | np.isnan(truth))
                        
                        if np.sum(valid_mask) == 0:
                            logger.warning("No valid pixels found for RMSE calculation")
                            return np.nan
                        
                        # Calculate MSE only on valid pixels
                        valid_pred = pred[valid_mask]
                        valid_truth = truth[valid_mask]
                        mse = np.mean((valid_pred - valid_truth) ** 2)
                        
                        if np.isnan(mse) or np.isinf(mse) or mse < 0:
                            logger.error(f"Invalid MSE calculated: {mse}")
                            return np.nan
                        
                        rmse = np.sqrt(mse)
                        valid_pixel_count = np.sum(valid_mask)
                        total_pixels = pred.size
                        
                        logger.debug(f"RMSE calculated from {valid_pixel_count}/{total_pixels} valid pixels ({valid_pixel_count/total_pixels*100:.1f}%)")
                        return rmse
                    
                    # Calculate RMSE for surface and rootzone using valid pixels
                    surface_rmse = calculate_rmse_with_valid_pixels(surface_pred, surface_truth)
                    rootzone_rmse = calculate_rmse_with_valid_pixels(rootzone_pred, rootzone_truth)
                    
                    # Validate RMSE values
                    if np.isnan(surface_rmse):
                        logger.error("Failed to calculate valid surface RMSE")
                        return None
                        
                    if np.isnan(rootzone_rmse):
                        logger.error("Failed to calculate valid rootzone RMSE")
                        return None
                    
                    # Calculate SSIM (Structural Similarity Index) with valid pixels
                    def calculate_ssim_with_valid_pixels(pred, truth):
                        """Calculate SSIM using only valid pixels and proper normalization."""
                        try:
                            # Create mask for valid pixels
                            valid_mask = ~(np.isnan(pred) | np.isnan(truth))
                            
                            if np.sum(valid_mask) < 100:  # Need minimum pixels for meaningful SSIM
                                logger.warning(f"Insufficient valid pixels ({np.sum(valid_mask)}) for SSIM calculation")
                                return 0.0
                            
                            # For SSIM, we need to work with the full arrays but handle NaN values
                            # Replace NaN values with mean of valid values for SSIM calculation
                            pred_for_ssim = pred.copy()
                            truth_for_ssim = truth.copy()
                            
                            if np.any(~valid_mask):
                                pred_mean = np.nanmean(pred_for_ssim)
                                truth_mean = np.nanmean(truth_for_ssim)
                                
                                pred_for_ssim[~valid_mask] = pred_mean
                                truth_for_ssim[~valid_mask] = truth_mean
                            
                            # Normalize data to [0, 1] for SSIM calculation
                            def normalize_for_ssim(data):
                                data_min, data_max = np.nanmin(data), np.nanmax(data)
                                if data_max > data_min and not (np.isnan(data_min) or np.isnan(data_max)):
                                    return (data - data_min) / (data_max - data_min)
                                else:
                                    return np.zeros_like(data)
                            
                            pred_norm = normalize_for_ssim(pred_for_ssim)
                            truth_norm = normalize_for_ssim(truth_for_ssim)
                            
                            # Calculate SSIM
                            ssim_value = ssim(truth_norm, pred_norm, data_range=1.0)
                            
                            if np.isnan(ssim_value) or np.isinf(ssim_value):
                                logger.warning(f"Invalid SSIM calculated: {ssim_value}, setting to 0")
                                return 0.0
                            
                            return float(ssim_value)
                            
                        except Exception as e:
                            logger.warning(f"Error calculating SSIM: {e}, setting to 0")
                            return 0.0
                    
                    # Calculate SSIM for surface and rootzone
                    surface_ssim = calculate_ssim_with_valid_pixels(surface_pred, surface_truth)
                    rootzone_ssim = calculate_ssim_with_valid_pixels(rootzone_pred, rootzone_truth)
                    
                    metrics = {
                        "surface_rmse": float(surface_rmse),
                        "rootzone_rmse": float(rootzone_rmse),
                        "surface_ssim": float(surface_ssim),
                        "rootzone_ssim": float(rootzone_ssim)
                    }
                    
                    logger.debug(f"Calculated metrics with valid pixels: {metrics}")
                    return metrics
                    
                except Exception as e:
                    logger.error(f"Error calculating soil metrics: {e}")
                    logger.error(traceback.format_exc())
                    return None
            
            soil_metrics = await loop.run_in_executor(
                self.executor,
                _calculate_soil_metrics_sync,
                model_predictions, # CPU tensor -> will be converted to numpy in sync function
                smap_tensor        # CPU tensor -> will be converted to numpy in sync function
            )
            # --- End offload --

            if not soil_metrics:
                logger.error(f"Failed to compute soil metrics for miner {miner_id} at {target_date}")
                return None
                
            # Move model_predictions back to original device if needed (e.g. if other parts of class expect it there)
            # model_predictions = model_predictions.to(original_model_predictions_device) # Not strictly necessary if only used here

            # Check if data needs to be cleared from cache (only if it was a temporary download, not a direct cache hit)
            # And if it's not the same as a smap_file_path provided externally
            if (smap_file_path is None or temp_smap_filename != smap_file_path) and Path(temp_smap_filename).name.startswith("tmp"):
                # This indicates it was a file created by NamedTemporaryFile and downloaded specifically for this run
                # and not a persistent cache hit or an external file.
                if os.path.exists(temp_smap_filename):
                    try:
                        await loop.run_in_executor(self.executor, os.unlink, temp_smap_filename)
                        logger.info(f"Successfully deleted temporary SMAP file: {temp_smap_filename}")
                    except Exception as e_unlink:
                        logger.error(f"Error deleting temporary SMAP file {temp_smap_filename}: {e_unlink}")
            elif temp_smap_filename and Path(temp_smap_filename).name.startswith("SMAP_L4_SM_gph_"):
                 logger.debug(f"Keeping cached SMAP file: {temp_smap_filename}")


            extended_log_message = {
                "miner_id": miner_id,
                "target_date": target_date.isoformat(),
                "bounds": bounds,
                "crs": str(crs),
                "smap_file_used": temp_smap_filename,
                "metrics_computed": soil_metrics
            }
            metrics_logger.info(json.dumps(extended_log_message))


            return {
                "validation_metrics": soil_metrics, 
                "ground_truth": smap_data
            }

        except FileNotFoundError as e_fnf:
            logger.error(f"SMAP file not found during metric computation for miner {miner_id}, date {target_date}: {e_fnf}")
            await self.schedule_retry_for_miner(miner_id, target_date, f"SMAP file not found: {e_fnf.filename}")
            return {"status": "retry_scheduled", "message": f"SMAP file not found: {e_fnf.filename}"}
        except Exception as e:
            logger.error(f"Error computing SMAP score metrics for miner {miner_id}, date {target_date}: {str(e)}")
            logger.error(traceback.format_exc())
            # If a generic error occurs, also schedule a retry
            await self.schedule_retry_for_miner(miner_id, target_date, "General error during SMAP metrics")
            return {"status": "retry_scheduled", "message": "General error during SMAP metrics"}

        finally:
            if process:
                mem_after = process.memory_info().rss / (1024 * 1024)
                logger.info(f"Memory usage for compute_smap_score_metrics: {(mem_after - mem_before):.2f} MB (Start: {mem_before:.2f}, End: {mem_after:.2f})")
            
            gc.collect() # Explicit garbage collection

    async def cleanup_invalid_prediction(self, bounds, target_time: datetime, miner_id: str):
        try:
            result = await self.db_manager.fetch_one(
                """
                SELECT p.id as pred_id
                FROM soil_moisture_predictions p
                JOIN soil_moisture_regions r ON p.region_id = r.id
                WHERE p.miner_uid = :miner_id 
                AND p.status = 'sent_to_miner'
                AND r.sentinel_bounds = ARRAY[:b1, :b2, :b3, :b4]::float[]
                """,
                {
                    "miner_id": miner_id,
                    "b1": bounds[0],
                    "b2": bounds[1], 
                    "b3": bounds[2],
                    "b4": bounds[3]
                }
            )
            
            if result:
                logger.info(f"Found prediction - ID: {result['pred_id']}")
                await self.db_manager.execute(
                    """
                    DELETE FROM soil_moisture_predictions 
                    WHERE id = :pred_id
                    RETURNING id
                    """,
                    {"pred_id": result['pred_id']}
                )
                logger.info(f"Removed prediction {result['pred_id']}")
                return True
            return False

        except Exception as e:
            logger.error(f"Failed to cleanup invalid prediction: {e}")
            logger.error(traceback.format_exc())
            return False

    def prepare_soil_history_records(self, miner_id: str, miner_hotkey: str, metrics: Dict, target_time: datetime) -> Dict[str, Any]:
        """
        Prepare data for insertion into the soil_moisture_history table.

        Args:
            miner_id (str): Unique identifier for the miner.
            miner_hotkey (str): Hotkey associated with the miner.
            metrics (Dict): Validation metrics including RMSE and SSIM.
            target_time (datetime): Target time of the prediction.

        Returns:
            Dict[str, Any]: Record formatted for soil_moisture_history table.
        """
        try:
            surface_rmse = metrics["validation_metrics"].get("surface_rmse")
            rootzone_rmse = metrics["validation_metrics"].get("rootzone_rmse")
            surface_ssim = metrics["validation_metrics"].get("surface_ssim", 0)
            rootzone_ssim = metrics["validation_metrics"].get("rootzone_ssim", 0)

            if not all(isinstance(x, (int, float)) for x in [surface_rmse, rootzone_rmse]):
                logger.error("RMSE values must be numeric and not None")
                return None

            if not all(-1 <= x <= 1 for x in [surface_ssim, rootzone_ssim]):
                logger.error("SSIM values must be in the range [-1, 1]")
                return None

            record = {
                "miner_id": miner_id,
                "miner_hotkey": miner_hotkey,
                "surface_rmse": surface_rmse,
                "rootzone_rmse": rootzone_rmse,
                "surface_structure_score": surface_ssim,
                "rootzone_structure_score": rootzone_ssim,
                "scored_at": target_time,
            }
            return record

        except Exception as e:
            logger.error(f"Error preparing soil history record: {str(e)}")
            return None

    async def schedule_retry_for_miner(self, miner_id: str, target_date: datetime, error_message: str):
        """
        Schedule a retry for a miner's prediction with smart timing based on error type.
        
        Args:
            miner_id (str): The miner's unique identifier
            target_date (datetime): The target date for the prediction
            error_message (str): Error message describing why retry is needed
        """
        try:
            current_time = datetime.now(timezone.utc)
            
            # Smart retry timing based on error type
            if "SMAP download failed" in error_message or "SMAP file not found" in error_message:
                # Data availability issue - longer delay to allow SMAP data to become available
                next_retry_time = current_time + timedelta(hours=2)
                retry_reason = "SMAP data unavailable"
            elif "SMAP processing failed" in error_message or "_FillValue" in error_message:
                # Processing issue - shorter delay, might be a transient error
                next_retry_time = current_time + timedelta(minutes=15)
                retry_reason = "SMAP processing error"
            elif "General error" in error_message or "Failed to calculate score" in error_message:
                # Scoring/calculation issue - very short delay, likely a transient error  
                next_retry_time = current_time + timedelta(minutes=5)
                retry_reason = "calculation/scoring error"
            else:
                # Unknown error - medium delay
                next_retry_time = current_time + timedelta(minutes=30)
                retry_reason = "unknown error"
            
            update_query = """
                UPDATE soil_moisture_predictions
                SET retry_count = COALESCE(retry_count, 0) + 1,
                    next_retry_time = :next_retry_time,
                    last_error = :error_message,
                    status = 'retry_scheduled'
                WHERE miner_uid = :miner_id
                AND target_time = :target_date
                AND status = 'sent_to_miner'
            """
            
            params = {
                "miner_id": miner_id,
                "target_date": target_date,
                "next_retry_time": next_retry_time,
                "error_message": error_message
            }
            
            result = await self.db_manager.execute(update_query, params)
            logger.info(f"Scheduled retry for miner {miner_id} at {next_retry_time} ({retry_reason}). Error: {error_message}")
            return result
            
        except Exception as e:
            logger.error(f"Error scheduling retry for miner {miner_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return None
