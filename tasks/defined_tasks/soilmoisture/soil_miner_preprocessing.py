from tasks.base.components.preprocessing import Preprocessing
from tasks.defined_tasks.soilmoisture.utils.inference_class import SoilMoistureInferencePreprocessor
from tasks.defined_tasks.soilmoisture.soil_model import SoilMoistureModel
from huggingface_hub import hf_hub_download
import torch
import io
from typing import Dict, Any, Optional
import safetensors.torch
import rasterio
import os
import numpy as np

class SoilMinerPreprocessing(Preprocessing):
    """Handles preprocessing of input data for soil moisture prediction."""
    
    def __init__(self):
        super().__init__()
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.preprocessor = SoilMoistureInferencePreprocessor()
        self.model = self._load_model()
        
    def _load_model(self) -> SoilMoistureModel:
        """Load model weights from local path or HuggingFace."""
        try:
            local_path = 'tasks/defined_tasks/soilmoisture/soil_model_weights.safetensors'
            if os.path.exists(local_path):
                state_dict = safetensors.torch.load_file(local_path)
            else:
                print("Local weights not found, downloading from HuggingFace...")
                weights_path = hf_hub_download(
                    repo_id="your-hf-repo/soil-moisture-task",
                    filename="soil_model_weights.safetensors"
                )
                state_dict = safetensors.torch.load_file(weights_path)
            
            model = SoilMoistureModel()
            model.load_state_dict(state_dict)
            model.to(self.device)
            model.eval()
            return model
            
        except Exception as e:
            raise RuntimeError(f"Failed to load model weights: {str(e)}")
    
    def process_miner_data(self, data: Dict[str, Any]) -> Dict[str, torch.Tensor]:
        """Process combined tiff data for model input."""
        try:
            with rasterio.io.MemoryFile(data['combined_data']) as memfile:
                model_inputs = self.preprocessor.preprocess(memfile)
                if model_inputs is None:
                    raise ValueError("Failed to preprocess input data")

                for key in model_inputs:
                    if isinstance(model_inputs[key], torch.Tensor):
                        model_inputs[key] = model_inputs[key].to(self.device)
                    
                return model_inputs
            
        except Exception as e:
            raise RuntimeError(f"Error processing miner data: {str(e)}")

    def predict_smap(self, model_inputs: Dict[str, torch.Tensor]) -> Dict[str, np.ndarray]:
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
                - uncertainty_surface: [H, W] Uncertainty in surface predictions
                - uncertainty_rootzone: [H, W] Uncertainty in rootzone predictions
        """
        try:
            with torch.no_grad():
                sentinel = model_inputs['sentinel_ndvi'].unsqueeze(0)
                era5 = model_inputs['era5'].unsqueeze(0)
                elev_ndvi = torch.cat([
                    model_inputs['elevation'],
                    model_inputs['sentinel_ndvi'][-1:]
                ], dim=0).unsqueeze(0)

                outputs = self.model(sentinel, era5, elev_ndvi)
                mask = model_inputs['mask'].cpu().numpy()
                predictions = {
                    'surface': outputs['surface_sm'].squeeze().cpu().numpy() * mask,
                    'rootzone': outputs['rootzone_sm'].squeeze().cpu().numpy() * mask,
                    'uncertainty_surface': outputs.get('uncertainty_surface', 
                        torch.zeros_like(outputs['surface_sm'])).squeeze().cpu().numpy() * mask,
                    'uncertainty_rootzone': outputs.get('uncertainty_rootzone',
                        torch.zeros_like(outputs['rootzone_sm'])).squeeze().cpu().numpy() * mask
                }
                
                return predictions
                
        except Exception as e:
            raise RuntimeError(f"Error during model inference: {str(e)}")
