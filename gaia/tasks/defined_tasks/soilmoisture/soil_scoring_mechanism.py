from datetime import datetime, timezone
import tempfile
from gaia.tasks.base.components.scoring_mechanism import ScoringMechanism
from gaia.tasks.base.decorators import task_timer
import numpy as np
from typing import Dict, Optional, Any, List
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

logger = get_logger(__name__)


class SoilScoringMechanism(ScoringMechanism):
    """Scoring mechanism for soil moisture predictions."""

    alpha: float = Field(default=10, description="Sigmoid steepness parameter")
    beta: float = Field(default=0.1, description="Sigmoid midpoint parameter")
    baseline_rmse: float = Field(default=50, description="Baseline RMSE value")
    db_manager: Any = Field(default=None)
    chunk_size: int = Field(default=32, description="Process predictions in chunks")
    memory_threshold: int = Field(default=1024 * 1024 * 1024, description="1GB threshold for memory cleanup")

    def __init__(self, baseline_rmse: float = 50, alpha: float = 10, beta: float = 0.1, db_manager=None):
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
        self.chunk_size = 32  # Process predictions in chunks
        self.memory_threshold = 1024 * 1024 * 1024  # 1GB threshold for memory cleanup

    def sigmoid_rmse(self, rmse: float) -> float:
        """Convert RMSE to score using sigmoid function. (higher is better)"""
        return 1 / (1 + torch.exp(self.alpha * (rmse - self.beta)))

    def compute_final_score(self, metrics: Dict) -> float:
        """Compute final score combining RMSE and SSIM metrics."""
        surface_rmse = metrics["validation_metrics"].get("surface_rmse", self.beta)
        rootzone_rmse = metrics["validation_metrics"].get("rootzone_rmse", self.beta)
        surface_ssim = metrics["validation_metrics"].get("surface_ssim", 0)
        rootzone_ssim = metrics["validation_metrics"].get("rootzone_ssim", 0)
        surface_score = 0.6 * self.sigmoid_rmse(torch.tensor(surface_rmse)) + 0.4 * (
            (surface_ssim + 1) / 2
        )
        rootzone_score = 0.6 * self.sigmoid_rmse(torch.tensor(rootzone_rmse)) + 0.4 * (
            (rootzone_ssim + 1) / 2
        )
        final_score = 0.5 * surface_score + 0.5 * rootzone_score

        return final_score.item()

    async def validate_predictions(self, predictions: Dict) -> bool:
        """check predictions before scoring."""
        try:
            pred_data = predictions.get("predictions")
            if pred_data is None:
                logger.error("No predictions found in input")
                return False

            if isinstance(pred_data, dict):
                surface_sm = torch.tensor(pred_data["surface_sm"])
                rootzone_sm = torch.tensor(pred_data["rootzone_sm"])
                model_predictions = torch.stack(
                    [surface_sm, rootzone_sm], dim=0
                ).unsqueeze(0)
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
            metrics = await self.compute_smap_score_metrics(
                bounds=predictions["bounds"],
                crs=predictions["crs"],
                model_predictions=predictions["predictions"],
                target_date=predictions["target_time"],
                miner_id=predictions["miner_id"]
            )

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
    ) -> dict:
        """
        Compute RMSE and SSIM between model predictions and SMAP data for valid pixels only.
        """
        device = model_predictions.device
        loop = asyncio.get_event_loop()

        left, bottom, right, top = bounds
        sentinel_bounds = BoundingBox(left=left, bottom=bottom, right=right, top=top)
        sentinel_crs = CRS.from_epsg(int(crs))

        smap_url = construct_smap_url(target_date)
        temp_file = None
        temp_path = None
        try:
            temp_file = tempfile.NamedTemporaryFile(suffix=".h5", delete=False)
            temp_path = temp_file.name
            temp_file.close()

            if not download_smap_data(smap_url, temp_file.name):
                return None

            smap_data = get_smap_data_for_sentinel_bounds(
                temp_file.name,
                (
                    sentinel_bounds.left,
                    sentinel_bounds.bottom,
                    sentinel_bounds.right,
                    sentinel_bounds.top,
                ),
                sentinel_crs.to_string(),
            )
            if not smap_data:
                return None

            if model_predictions.size(2) == 0 or model_predictions.size(3) == 0:
                logger.error(f"Empty model predictions detected with shape: {model_predictions.shape}")
                await self.db_manager.execute(
                    """
                    DELETE FROM soil_moisture_predictions 
                    WHERE miner_uid = :miner_id 
                    AND target_time = :target_time
                    """,
                    {
                        "miner_id": miner_id,
                        "target_time": target_date
                    }
                )
                logger.info(f"Deleted invalid prediction for miner {miner_id} at {target_date}")
                return None

            if model_predictions.shape[-2:] != (11, 11):
                logger.error(f"Invalid model prediction shape: {model_predictions.shape}, expected last dimensions to be (11, 11)")
                return None

            surface_sm = torch.from_numpy(smap_data["surface_sm"]).float()
            rootzone_sm = torch.from_numpy(smap_data["rootzone_sm"]).float()

            if surface_sm.dim() == 2:
                surface_sm = surface_sm.unsqueeze(0).unsqueeze(0)
            if rootzone_sm.dim() == 2:
                rootzone_sm = rootzone_sm.unsqueeze(0).unsqueeze(0)

            surface_sm = surface_sm.to(device)
            rootzone_sm = rootzone_sm.to(device)

            logger.info(f"Model predictions shape: {model_predictions.shape}")
            logger.info(f"SMAP data shapes - surface: {smap_data['surface_sm'].shape}, rootzone: {smap_data['rootzone_sm'].shape}")
            logger.info(f"Processed shapes - surface: {surface_sm.shape}, rootzone: {rootzone_sm.shape}, model: {model_predictions.shape}")

            if model_predictions.shape[1] != 2:
                logger.error(f"Model predictions should have 2 channels, got shape: {model_predictions.shape}")
                return None

            # Run interpolation in thread pool
            surface_sm_11x11 = await loop.run_in_executor(None, 
                lambda: F.interpolate(surface_sm, size=(11, 11), mode="bilinear", align_corners=False))
            rootzone_sm_11x11 = await loop.run_in_executor(None,
                lambda: F.interpolate(rootzone_sm, size=(11, 11), mode="bilinear", align_corners=False))

            surface_mask_11x11 = ~torch.isnan(surface_sm_11x11[0, 0])
            rootzone_mask_11x11 = ~torch.isnan(rootzone_sm_11x11[0, 0])

            if not (surface_mask_11x11.any() or rootzone_mask_11x11.any()):
                logger.warning(f"No valid SMAP data found for bounds {bounds}")
                cleanup_success = await self.cleanup_invalid_prediction(bounds, target_date, miner_id)
                if not cleanup_success:
                    logger.error(f"Failed to cleanup invalid prediction for bounds {bounds}")
                return None

            results = {"validation_metrics": {}}
            if surface_mask_11x11.any():
                valid_surface_pred = model_predictions[0, 0][surface_mask_11x11]
                valid_surface_truth = surface_sm_11x11[0, 0][surface_mask_11x11]
                
                # Run RMSE calculation in thread pool
                surface_rmse = await loop.run_in_executor(None, 
                    lambda: torch.sqrt(F.mse_loss(valid_surface_pred, valid_surface_truth)))
                results["validation_metrics"]["surface_rmse"] = surface_rmse.item()

                surface_pred_masked = torch.zeros_like(model_predictions[0:1, 0:1])
                surface_truth_masked = torch.zeros_like(surface_sm_11x11)
                surface_pred_masked[0, 0][surface_mask_11x11] = model_predictions[0, 0][surface_mask_11x11]
                surface_truth_masked[0, 0][surface_mask_11x11] = surface_sm_11x11[0, 0][surface_mask_11x11]

                valid_min = torch.min(valid_surface_pred.min(), valid_surface_truth.min())
                valid_max = torch.max(valid_surface_pred.max(), valid_surface_truth.max())
                data_range = valid_max - valid_min

                if data_range > 0:
                    # Run SSIM calculation in thread pool
                    surface_ssim = await loop.run_in_executor(None, lambda: ssim(
                        surface_pred_masked,
                        surface_truth_masked,
                        data_range=data_range,
                        kernel_size=3,
                    ))
                    results["validation_metrics"]["surface_ssim"] = surface_ssim.item()

            if rootzone_mask_11x11.any():
                valid_rootzone_pred = model_predictions[0, 1][rootzone_mask_11x11]
                valid_rootzone_truth = rootzone_sm_11x11[0, 0][rootzone_mask_11x11]
                
                # Run RMSE calculation in thread pool
                rootzone_rmse = await loop.run_in_executor(None,
                    lambda: torch.sqrt(F.mse_loss(valid_rootzone_pred, valid_rootzone_truth)))
                results["validation_metrics"]["rootzone_rmse"] = rootzone_rmse.item()

                rootzone_pred_masked = torch.zeros_like(model_predictions[0:1, 1:2])
                rootzone_truth_masked = torch.zeros_like(rootzone_sm_11x11)
                rootzone_pred_masked[0, 0][rootzone_mask_11x11] = model_predictions[0, 1][rootzone_mask_11x11]
                rootzone_truth_masked[0, 0][rootzone_mask_11x11] = rootzone_sm_11x11[0, 0][rootzone_mask_11x11]

                valid_min = torch.min(valid_rootzone_pred.min(), valid_rootzone_truth.min())
                valid_max = torch.max(valid_rootzone_pred.max(), valid_rootzone_truth.max())
                data_range = valid_max - valid_min

                if data_range > 0:
                    # Run SSIM calculation in thread pool
                    rootzone_ssim = await loop.run_in_executor(None, lambda: ssim(
                        rootzone_pred_masked,
                        rootzone_truth_masked,
                        data_range=data_range,
                        kernel_size=3,
                    ))
                    results["validation_metrics"]["rootzone_ssim"] = rootzone_ssim.item()

            return results

        except Exception as e:
            logger.error(f"Error processing SMAP data: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None
        finally:
            # Clean up temp file
            if temp_file:
                try:
                    temp_file.close()
                except:
                    pass
            
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                    logger.debug(f"Removed temporary file: {temp_path}")
                except Exception as e:
                    logger.error(f"Failed to remove temporary file {temp_path}: {e}")
            
            # Clean up any other .h5 files in /tmp
            try:
                for f in glob.glob("/tmp/*.h5"):
                    try:
                        os.unlink(f)
                        logger.debug(f"Cleaned up additional temp file: {f}")
                    except Exception as e:
                        logger.error(f"Failed to remove temp file {f}: {e}")
            except Exception as e:
                logger.error(f"Error during temp file cleanup: {e}")

    async def cleanup_invalid_prediction(self, bounds, target_time: datetime, miner_id: str, conn=None):
        """Clean up predictions for a specific miner and timestamp."""
        try:
            if not conn:
                conn = await self.db_manager.get_connection()
            
            async with conn.begin():
                debug_result = await conn.execute(
                    text("""
                    SELECT 
                        p.id as pred_id,
                        p.miner_uid,
                        r.target_time,
                        r.region_date,
                        r.sentinel_bounds
                    FROM soil_moisture_predictions p
                    JOIN soil_moisture_regions r ON p.region_id = r.id
                    WHERE p.miner_uid = :miner_id 
                    AND p.status = 'sent_to_miner'
                    AND r.sentinel_bounds = ARRAY[:b1, :b2, :b3, :b4]::float[]
                    """),
                    {
                        "miner_id": miner_id,
                        "b1": bounds[0],
                        "b2": bounds[1],
                        "b3": bounds[2],
                        "b4": bounds[3]
                    }
                )
                
                row = debug_result.first()
                if row:
                    logger.info(f"Found prediction - ID: {row.pred_id}, Time: {row.target_time}, Date: {row.region_date}")
                    result = await conn.execute(
                        text("""
                        DELETE FROM soil_moisture_predictions p
                        WHERE p.id = :pred_id
                        RETURNING p.id
                        """),
                        {"pred_id": row.pred_id}
                    )
                    
                    deleted = result.first()
                    if deleted:
                        logger.info(f"Removed prediction {deleted.id}")
                        return True
                else:
                    logger.warning(f"No predictions found for miner {miner_id} with bounds {bounds}")
                
                return False

        except Exception as e:
            logger.error(f"Failed to cleanup invalid prediction: {e}")
            logger.error(traceback.format_exc())
            return False
        finally:
            if conn and not isinstance(conn, AsyncSession):
                await conn.close()

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

    async def score_predictions(self, predictions: List[Dict], ground_truth: Dict) -> List[Dict]:
        """Score predictions with memory-efficient processing."""
        try:
            scores = []
            total_predictions = len(predictions)
            logger.info(f"Scoring {total_predictions} predictions")

            # Track initial memory usage
            initial_memory = psutil.Process().memory_info().rss
            logger.debug(f"Initial memory usage: {initial_memory / (1024 * 1024):.2f}MB")

            # Process predictions in chunks
            for i in range(0, total_predictions, self.chunk_size):
                chunk = predictions[i:i + self.chunk_size]
                logger.debug(f"Processing prediction chunk {i//self.chunk_size + 1}/{(total_predictions + self.chunk_size - 1)//self.chunk_size}")
                
                chunk_scores = []
                for prediction in chunk:
                    try:
                        # Score individual prediction
                        score = await self._score_single_prediction(prediction, ground_truth)
                        if score:
                            chunk_scores.append(score)
                    except Exception as e:
                        logger.error(f"Error scoring prediction from miner {prediction.get('miner_hotkey')}: {str(e)}")
                        logger.error(traceback.format_exc())
                        continue

                # Extend scores list with chunk results
                scores.extend(chunk_scores)
                
                # Check memory usage and cleanup if needed
                current_memory = psutil.Process().memory_info().rss
                if current_memory - initial_memory > self.memory_threshold:
                    logger.warning(f"Memory usage exceeded threshold. Forcing cleanup.")
                    gc.collect()
                    await asyncio.sleep(0.1)  # Allow event loop to process other tasks

                logger.debug(f"Current memory usage: {current_memory / (1024 * 1024):.2f}MB")

            return scores

        except Exception as e:
            logger.error(f"Error in score_predictions: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def _score_single_prediction(self, prediction: Dict, ground_truth: Dict) -> Optional[Dict]:
        """Score a single prediction with memory management."""
        try:
            # Validate prediction data
            if not self._validate_prediction(prediction):
                return None

            # Convert data to numpy arrays efficiently
            try:
                surface_sm = np.array(prediction["surface_sm"], dtype=np.float32)
                rootzone_sm = np.array(prediction["rootzone_sm"], dtype=np.float32)
                
                gt_surface = np.array(ground_truth["surface_sm"], dtype=np.float32)
                gt_rootzone = np.array(ground_truth["rootzone_sm"], dtype=np.float32)
            except Exception as e:
                logger.error(f"Error converting data to numpy arrays: {str(e)}")
                return None

            # Calculate scores efficiently
            surface_rmse = self._calculate_rmse(surface_sm, gt_surface)
            rootzone_rmse = self._calculate_rmse(rootzone_sm, gt_rootzone)
            
            # Clear arrays to free memory
            del surface_sm, rootzone_sm, gt_surface, gt_rootzone
            
            # Calculate combined score
            combined_score = (surface_rmse + rootzone_rmse) / 2.0

            return {
                "miner_hotkey": prediction["miner_hotkey"],
                "miner_uid": prediction["miner_uid"],
                "region_id": prediction["region_id"],
                "target_time": prediction["target_time"],
                "surface_rmse": float(surface_rmse),
                "rootzone_rmse": float(rootzone_rmse),
                "combined_score": float(combined_score)
            }

        except Exception as e:
            logger.error(f"Error in _score_single_prediction: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _validate_prediction(self, prediction: Dict) -> bool:
        """Validate prediction data."""
        required_fields = ["surface_sm", "rootzone_sm", "miner_hotkey", "miner_uid", "region_id", "target_time"]
        
        for field in required_fields:
            if field not in prediction:
                logger.error(f"Missing required field: {field}")
                return False
            
            if field in ["surface_sm", "rootzone_sm"]:
                if not isinstance(prediction[field], (list, np.ndarray)):
                    logger.error(f"Invalid data type for {field}: {type(prediction[field])}")
                    return False
                
                if len(prediction[field]) == 0:
                    logger.error(f"Empty data for {field}")
                    return False

        return True

    def _calculate_rmse(self, prediction: np.ndarray, ground_truth: np.ndarray) -> float:
        """Calculate RMSE efficiently."""
        try:
            # Ensure arrays have same shape
            if prediction.shape != ground_truth.shape:
                raise ValueError(f"Shape mismatch: prediction {prediction.shape} != ground_truth {ground_truth.shape}")

            # Calculate RMSE efficiently using numpy operations
            squared_diff = np.square(prediction - ground_truth)
            mean_squared_diff = np.mean(squared_diff)
            rmse = np.sqrt(mean_squared_diff)

            # Clear intermediate arrays
            del squared_diff
            
            return float(rmse)

        except Exception as e:
            logger.error(f"Error calculating RMSE: {str(e)}")
            raise