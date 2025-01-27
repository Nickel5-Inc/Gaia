"""Soil Moisture Miner Preprocessing.

This module handles preprocessing of input data for soil moisture prediction.
"""

from prefect import task
from prefect.tasks import task_input_hash
import traceback
from datetime import timedelta
from gaia.tasks.base.components.preprocessing import Preprocessing
from gaia.tasks.defined_tasks.soilmoisture.utils.inference_class import (
    SoilMoistureInferencePreprocessor,
)
from gaia.models.soil_moisture_basemodel import SoilModel
from huggingface_hub import hf_hub_download
import torch
import io
from typing import Dict, Any, Optional, Union
import safetensors.torch
import rasterio
import os
import numpy as np
from fiber.logging_utils import get_logger
import tempfile
import base64

logger = get_logger(__name__)

class SoilMinerPreprocessing(Preprocessing):
    """Handles preprocessing of input data for soil moisture prediction."""

    def __init__(self, task=None):
        """Initialize preprocessing with optional task reference."""
        super().__init__()
        self.task = task
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.preprocessor = SoilMoistureInferencePreprocessor()
        self.model = self._load_model()
        logger.info(f"Initialized preprocessing on device: {self.device}")

    @task(
        name="load_model",
        retries=3,
        retry_delay_seconds=60,
        cache_key_fn=task_input_hash,
        cache_expiration=timedelta(hours=24)
    )
    def _load_model(self) -> SoilModel:
        """Load model weights from local path or HuggingFace with caching."""
        try:
            model_dir = os.path.join("gaia", "models", "checkpoints", "soil_moisture")
            os.makedirs(model_dir, exist_ok=True)
            local_path = os.path.join(model_dir, "SoilModel.ckpt")

            if os.path.exists(local_path):
                logger.info(f"Loading model from local path: {local_path}")
                model = SoilModel.load_from_checkpoint(local_path)
            else:
                logger.info("Local checkpoint not found, downloading from HuggingFace...")
                checkpoint_path = hf_hub_download(
                    repo_id="Nickel5HF/soil-moisture-model",
                    filename="SoilModel.ckpt",
                    local_dir=model_dir,
                )
                logger.info(f"Loading model from HuggingFace: {checkpoint_path}")
                model = SoilModel.load_from_checkpoint(checkpoint_path)

            model.to(self.device)
            model.eval()

            param_count = sum(p.numel() for p in model.parameters())
            logger.info(f"Model loaded successfully with {param_count:,} parameters")
            logger.info(f"Model device: {next(model.parameters()).device}")

            return model

        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            logger.error(traceback.format_exc())
            raise RuntimeError(f"Failed to load model weights: {str(e)}")

    @task(
        name="decode_tiff_data",
        retries=2,
        retry_delay_seconds=30
    )
    def _decode_tiff_data(self, combined_data: Union[str, bytes]) -> bytes:
        """Decode TIFF data from base64 or bytes format."""
        try:
            if isinstance(combined_data, str):
                try:
                    return base64.b64decode(combined_data)
                except Exception as e:
                    logger.error(f"Failed to decode base64: {str(e)}")
                    return combined_data.encode("utf-8")
            return combined_data
        except Exception as e:
            logger.error(f"Error decoding TIFF data: {str(e)}")
            raise ValueError(f"Invalid TIFF data format: {str(e)}")

    @task(
        name="validate_tiff",
        retries=2,
        retry_delay_seconds=30
    )
    def _validate_tiff(self, tiff_bytes: bytes) -> None:
        """Validate TIFF format and header."""
        if not (
            tiff_bytes.startswith(b"II\x2A\x00")
            or tiff_bytes.startswith(b"MM\x00\x2A")
        ):
            logger.error(f"Invalid TIFF header detected")
            logger.error(f"First 16 bytes: {tiff_bytes[:16].hex()}")
            raise ValueError(
                "Invalid TIFF format: File does not start with valid TIFF header"
            )

    @task(
        name="process_miner_data",
        retries=3,
        retry_delay_seconds=60
    )
    async def process_miner_data(self, data: Dict[str, Any]) -> Dict[str, torch.Tensor]:
        """Process combined tiff data for model input."""
        try:
            combined_data = data["combined_data"]
            logger.info(f"Received data type: {type(combined_data)}")
            logger.info(f"Received data: {combined_data[:100]}")

            tiff_bytes = await self._decode_tiff_data.submit(combined_data)
            await self._validate_tiff.submit(tiff_bytes)

            logger.info(f"Decoded data size: {len(tiff_bytes)} bytes")

            temp_file_path = None
            try:
                with tempfile.NamedTemporaryFile(
                    suffix=".tif", delete=False, mode="wb"
                ) as temp_file:
                    temp_file_path = temp_file.name
                    temp_file.write(tiff_bytes)
                    temp_file.flush()
                    os.fsync(temp_file.fileno())

                with rasterio.open(temp_file_path) as dataset:
                    logger.info(f"Successfully opened TIFF with shape: {dataset.shape}")
                    logger.info(f"TIFF metadata: {dataset.profile}")
                    logger.info(f"Band order: {dataset.tags().get('band_order', 'Not found')}")

                    if self.task.use_raw_preprocessing:
                        model_inputs = self.preprocessor.preprocess_raw(temp_file_path)  # Base model
                    else:
                        model_inputs = self.preprocessor.preprocess(temp_file_path)  # Custom model
                        for key in model_inputs:
                            if isinstance(model_inputs[key], torch.Tensor):
                                model_inputs[key] = model_inputs[key].to(self.device)

                    return model_inputs

            finally:
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        os.unlink(temp_file_path)
                    except Exception as e:
                        logger.error(f"Error cleaning up temporary file: {str(e)}")

        except Exception as e:
            logger.error(f"Error processing TIFF data: {str(e)}")
            logger.error(f"Error trace: {traceback.format_exc()}")
            raise RuntimeError(f"Error processing miner data: {str(e)}")

    @task(
        name="predict_smap",
        retries=2,
        retry_delay_seconds=30
    )
    def predict_smap(
        self, model_inputs: Dict[str, torch.Tensor], model: torch.nn.Module
    ) -> Dict[str, np.ndarray]:
        """Run model inference to predict SMAP soil moisture.

        Args:
            model_inputs: Dictionary containing preprocessed tensors
                - sentinel_ndvi: [C, H, W] Sentinel bands + NDVI
                - elevation: [1, H, W] Elevation data
                - era5: [C, H, W] Weather data

        Returns:
            Dictionary containing:
                - surface: [H, W] Surface soil moisture predictions
                - rootzone: [H, W] Root zone soil moisture predictions
        """
        try:
            device = next(model.parameters()).device
            sentinel = model_inputs["sentinel_ndvi"][:2].unsqueeze(0).to(device)
            era5 = model_inputs["era5"].unsqueeze(0).to(device)
            elevation = model_inputs["elevation"]
            ndvi = model_inputs["sentinel_ndvi"][2:3]
            elev_ndvi = torch.cat([elevation, ndvi], dim=0).unsqueeze(0).to(device)

            with torch.no_grad():
                outputs = model(sentinel, era5, elev_ndvi)
                mask = (
                    model_inputs.get("mask", torch.ones_like(outputs[0, 0]))
                    .cpu()
                    .numpy()
                )

                predictions = {
                    "surface": outputs[0, 0].cpu().numpy() * mask,
                    "rootzone": outputs[0, 1].cpu().numpy() * mask,
                }
                logger.info(f"Generated predictions with shapes - surface: {predictions['surface'].shape}, rootzone: {predictions['rootzone'].shape}")
                return predictions

        except Exception as e:
            logger.error(f"Error during model inference: {str(e)}")
            logger.error(
                f"Input shapes - sentinel: {sentinel.shape}, era5: {era5.shape}, elev_ndvi: {elev_ndvi.shape}"
            )
            raise RuntimeError(f"Error during model inference: {str(e)}")
