from datetime import datetime, timezone
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
    task: Any = Field(default=None, description="Reference to the parent task")

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

    def sigmoid_rmse(self, rmse: float) -> float:
        """Convert RMSE to score using sigmoid function. (higher is better)"""
        return 1 / (1 + torch.exp(self.alpha * (rmse - self.beta)))

    def compute_final_score(self, metrics: Dict) -> float:
        """Compute final score combining RMSE and SSIM metrics."""
        surface_rmse = metrics["validation_metrics"].get("surface_rmse", self.beta)
        rootzone_rmse = metrics["validation_metrics"].get("rootzone_rmse", self.beta)
        surface_ssim = metrics["validation_metrics"].get("surface_ssim", 0)
        rootzone_ssim = metrics["validation_metrics"].get("rootzone_ssim", 0)
        surface_score = 0.8 * self.sigmoid_rmse(torch.tensor(surface_rmse)) + 0.2 * (
            (surface_ssim + 1) / 2
        )
        rootzone_score = 0.8 * self.sigmoid_rmse(torch.tensor(rootzone_rmse)) + 0.2 * (
            (rootzone_ssim + 1) / 2
        )
        final_score = 0.6 * surface_score + 0.4 * rootzone_score

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
            
            if pred_info and pred_info.get("status") == "retry_scheduled":
                current_time = datetime.datetime.now(timezone.utc)
                if current_time < pred_info["next_retry_time"]:
                    logger.info(f"Skipping scoring for timestamp {predictions['target_time']} - in retry wait period until {pred_info['next_retry_time']}")
                    return None

            if not pred_info or pred_info.get("status") == "sent_to_miner":
                logger.info(f"Processing new prediction for timestamp {predictions['target_time']}")

            metrics = await self.compute_smap_score_metrics(
                bounds=predictions["bounds"],
                crs=predictions["crs"],
                model_predictions=predictions["predictions"],
                target_date=predictions["target_time"],
                miner_id=predictions["miner_id"],
                smap_file_path=predictions.get("smap_file_path")
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
        smap_file_path: Optional[str] = None
    ) -> dict:
        """
        Compute RMSE and SSIM between model predictions and SMAP data for valid pixels only.
        
        Args:
            bounds: The bounding box of the area
            crs: The coordinate reference system
            model_predictions: The model predictions as a tensor
            target_date: The target date for the prediction
            miner_id: The miner ID
            smap_file_path: Optional path to an already downloaded SMAP file
        """
        device = model_predictions.device
        loop = asyncio.get_event_loop()

        left, bottom, right, top = bounds
        sentinel_bounds = BoundingBox(left=left, bottom=bottom, right=right, top=top)
        sentinel_crs = CRS.from_epsg(int(crs))

        test_mode = getattr(self.task, 'test_mode', False)
        temp_file = None
        temp_path = None
        should_download = True
        
        if smap_file_path and os.path.exists(smap_file_path):
            temp_path = smap_file_path
            should_download = False
            logger.info(f"Using provided SMAP file: {smap_file_path}")
        
        try:
            if should_download:
                smap_url = construct_smap_url(target_date, test_mode=test_mode, verbose=True)
                temp_file = tempfile.NamedTemporaryFile(suffix=".h5", delete=False)
                temp_path = temp_file.name
                temp_file.close()

                if not download_smap_data(smap_url, temp_file.name, verbose=True):
                    return None

            smap_data = get_smap_data_for_sentinel_bounds(
                temp_path,
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
                # WRITE operation - use session for deleting invalid prediction
                query = """
                    DELETE FROM soil_moisture_predictions 
                    WHERE miner_uid = :miner_id 
                    AND target_time = :target_time
                """
                await self.db_manager.execute(
                    query,
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
                        kernel_size=9,
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
                        kernel_size=9,
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