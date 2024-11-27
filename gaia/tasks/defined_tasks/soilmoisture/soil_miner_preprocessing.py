import traceback
from gaia.tasks.base.components.preprocessing import Preprocessing
from gaia.tasks.defined_tasks.soilmoisture.utils.inference_class import (
    SoilMoistureInferencePreprocessor,
)
from gaia.models.soil_moisture_basemodel import SoilModel
from huggingface_hub import hf_hub_download
import torch
import io
from typing import Dict, Any, Optional
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

    def __init__(self):
        super().__init__()
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.preprocessor = SoilMoistureInferencePreprocessor()
        self.model = None

    def _load_model(self) -> SoilModel:
        """Load model weights from local path or HuggingFace."""
        try:
            if self.model is None:
                self.model = SoilModel()
                local_path = 'tasks/defined_tasks/soilmoisture/model.safetensors'
                
                if os.path.exists(local_path):
                    state_dict = safetensors.torch.load_file(local_path)
                else:
                    logger.info("Local weights not found, downloading from HuggingFace...")
                    weights_path = hf_hub_download(
                        repo_id="Nickel5HF/soil-moisture-model",
                        filename="model.safetensors"
                    )
                    state_dict = safetensors.torch.load_file(weights_path)

                self.model.load_state_dict(state_dict)
                self.model.to(self.device)
                self.model.eval()
            return self.model

        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            raise RuntimeError(f"Failed to load model weights: {str(e)}")

    def process_miner_data(self, data: Dict[str, Any]) -> Dict[str, torch.Tensor]:
        """Process combined tiff data for model input."""
        try:
            combined_data = data['combined_data']
            logger.info(f"####Received data type: {type(combined_data)}")
            logger.info(f"####Received data: {combined_data[:100]}")  # Log first 100 chars
            
            # Always treat as base64 string first
            try:
                tiff_bytes = base64.b64decode(combined_data)
            except Exception as e:
                logger.error(f"Failed to decode base64: {str(e)}")
                # If base64 decode fails, try direct bytes
                tiff_bytes = combined_data if isinstance(combined_data, bytes) else combined_data.encode('utf-8')
                
            logger.info(f"####Decoded data size: {len(tiff_bytes)} bytes")
            logger.info(f"####First 16 bytes hex: {tiff_bytes[:16].hex()}")
            logger.info(f"####First 4 bytes raw: {tiff_bytes[:4]}")
            
            # Validate TIFF header before writing to file
            if not (tiff_bytes.startswith(b'II\x2A\x00') or tiff_bytes.startswith(b'MM\x00\x2A')):
                logger.error(f"Invalid TIFF header detected")
                logger.error(f"First 16 bytes: {tiff_bytes[:16].hex()}")
                raise ValueError("Invalid TIFF format: File does not start with valid TIFF header")
            
            # Create a temporary file to write the TIFF data
            temp_file_path = None
            try:
                with tempfile.NamedTemporaryFile(suffix='.tif', delete=False, mode='wb') as temp_file:
                    temp_file_path = temp_file.name
                    temp_file.write(tiff_bytes)
                    temp_file.flush()
                    os.fsync(temp_file.fileno())
                
                # Now read the file after it's closed
                with open(temp_file_path, 'rb') as check_file:
                    header = check_file.read(4)
                    logger.info(f"Written file header: {header.hex()}")

                # Process with rasterio
                with rasterio.open(temp_file_path) as dataset:
                    logger.info(f"####Successfully opened TIFF with shape: {dataset.shape}")
                    logger.info(f"####TIFF metadata: {dataset.profile}")
                    logger.info(f"####Band order: {dataset.tags().get('band_order', 'Not found')}")
                    
                    # Pass the file path instead of the dataset
                    model_inputs = self.preprocessor.preprocess(temp_file_path)
                    if model_inputs is None:
                        raise ValueError("####Failed to preprocess input data")

                    for key in model_inputs:
                        if isinstance(model_inputs[key], torch.Tensor):
                            model_inputs[key] = model_inputs[key].to(self.device)

                    return model_inputs

            finally:
                # Clean up the temporary file
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        os.unlink(temp_file_path)
                    except Exception as e:
                        logger.error(f"Error cleaning up temporary file: {str(e)}")

        except Exception as e:
            logger.error(f"Error processing TIFF data: {str(e)}")
            logger.error(f"Error trace: {traceback.format_exc()}")
            raise RuntimeError(f"Error processing miner data: {str(e)}")

    def predict_smap(
        self, model_inputs: Dict[str, torch.Tensor]
    ) -> Dict[str, np.ndarray]:
        """Run model inference to predict SMAP soil moisture.

        Args:
            model_inputs: Dictionary containing preprocessed tensors
                - sentinel_ndvi: [B, C, H, W] Sentinel bands + NDVI
                - elevation: [B, 1, H, W] Elevation data
                - era5: [B, C, H, W] Weather data
                - mask: [H, W] Boolean mask

        Returns:
            Dictionary containing:
                - surface: [H, W] Surface soil moisture predictions
                - rootzone: [H, W] Root zone soil moisture predictions
        """
        try:
            with torch.no_grad():
                sentinel = model_inputs["sentinel_ndvi"].unsqueeze(0)
                era5 = model_inputs["era5"].unsqueeze(0)
                elev_ndvi = torch.cat(
                    [model_inputs["elevation"], model_inputs["sentinel_ndvi"][-1:]],
                    dim=0,
                ).unsqueeze(0)

                outputs = self.model(sentinel, era5, elev_ndvi)
                mask = model_inputs["mask"].cpu().numpy()
                predictions = {
                    "surface": outputs["surface_sm"].squeeze().cpu().numpy() * mask,
                    "rootzone": outputs["rootzone_sm"].squeeze().cpu().numpy() * mask,
                }

                return predictions

        except Exception as e:
            raise RuntimeError(f"Error during model inference: {str(e)}")
