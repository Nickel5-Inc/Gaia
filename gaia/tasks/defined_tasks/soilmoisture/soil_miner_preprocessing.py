import traceback
from gaia.tasks.base.components.preprocessing import Preprocessing
from gaia.tasks.defined_tasks.soilmoisture.utils.inference_class import (
    SoilMoistureInferencePreprocessor,
)
from gaia.models.soil_moisture_basemodel import SoilModel
import torch
import io
from typing import Dict, Any, Optional
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
        super().__init__()
        self.task = task
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.preprocessor = SoilMoistureInferencePreprocessor()

    def predict_smap(self, processed_data: Dict[str, Any], model: SoilModel) -> Dict[str, Any]:
        """Run prediction using the provided model."""
        try:
            with torch.no_grad():
                sentinel = torch.from_numpy(processed_data["sentinel_ndvi"][:, :2]).float().to(self.device)
                era5 = torch.from_numpy(processed_data["era5"]).float().to(self.device)
                elev_ndvi = torch.cat([
                    torch.from_numpy(processed_data["elevation"]).float(),
                    torch.from_numpy(processed_data["sentinel_ndvi"][:, 2:3]).float()
                ], dim=1).to(self.device)

                if sentinel.dim() == 3:
                    sentinel = sentinel.unsqueeze(0)
                if era5.dim() == 3:
                    era5 = era5.unsqueeze(0)
                if elev_ndvi.dim() == 3:
                    elev_ndvi = elev_ndvi.unsqueeze(0)

                predictions = model(sentinel, era5, elev_ndvi)

                return {
                    "surface": predictions[0, 0].cpu().numpy(),
                    "rootzone": predictions[0, 1].cpu().numpy()
                }

        except Exception as e:
            logger.error(f"Error in model prediction: {str(e)}")
            logger.error(traceback.format_exc())
            raise RuntimeError(f"Model prediction failed: {str(e)}")

    def process_input(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process input data for model inference."""
        try:
            if not isinstance(data, dict):
                raise ValueError(f"Expected dict input, got {type(data)}")

            required_fields = ["combined_data"]
            for field in required_fields:
                if field not in data:
                    raise ValueError(f"Missing required field: {field}")

            # Decode combined data if it's base64 encoded
            if isinstance(data["combined_data"], str):
                try:
                    combined_data = base64.b64decode(data["combined_data"])
                except Exception as e:
                    raise ValueError(f"Failed to decode base64 data: {str(e)}")
            else:
                combined_data = data["combined_data"]

            # Process the combined data using the preprocessor
            with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as temp_file:
                temp_file.write(combined_data)
                temp_file.flush()
                
                try:
                    processed_data = self.preprocessor.process_combined_file(temp_file.name)
                finally:
                    try:
                        os.unlink(temp_file.name)
                    except:
                        pass

            if not processed_data:
                raise ValueError("Failed to process combined data")

            return processed_data

        except Exception as e:
            logger.error(f"Error processing input: {str(e)}")
            logger.error(traceback.format_exc())
            raise RuntimeError(f"Input processing failed: {str(e)}")

    def validate_output(self, output: Dict[str, Any]) -> bool:
        """Validate model output."""
        try:
            required_fields = ["surface", "rootzone"]
            if not all(field in output for field in required_fields):
                logger.error(f"Missing required output fields: {required_fields}")
                return False

            for field in required_fields:
                if not isinstance(output[field], np.ndarray):
                    logger.error(f"Expected numpy array for {field}, got {type(output[field])}")
                    return False

                if output[field].shape != (11, 11):
                    logger.error(f"Expected shape (11, 11) for {field}, got {output[field].shape}")
                    return False

                if not (0 <= output[field]).all() or not (output[field] <= 1).all():
                    logger.error(f"Values for {field} outside valid range [0, 1]")
                    return False

            return True

        except Exception as e:
            logger.error(f"Error validating output: {str(e)}")
            return False
