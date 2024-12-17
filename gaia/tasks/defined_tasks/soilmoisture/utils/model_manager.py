import os
import importlib.util
from typing import Optional, Type, Dict, Any
import torch
from fiber.logging_utils import get_logger
from gaia.models.soil_moisture_basemodel import SoilModel
from huggingface_hub import hf_hub_download
import traceback

logger = get_logger(__name__)

class ModelManager:
    """Manages model loading, validation, and state persistence for soil moisture models."""
    
    def __init__(self, cache_dir: Optional[str] = None):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.cache_dir = cache_dir or "tasks/defined_tasks/soilmoisture/"
        self.model: Optional[Any] = None
        self.model_type: str = "base"
        
    def validate_custom_model(self, model_class: Type) -> bool:
        """Validate that custom model implements required interface."""
        try:
            required_methods = ['run_inference']
            
            for method in required_methods:
                if not hasattr(model_class, method):
                    logger.error(f"Custom model missing required method: {method}")
                    return False
                    
            # Validate initialization
            model_instance = model_class()
            if not hasattr(model_instance, '_load_model'):
                logger.warning("Custom model missing recommended _load_model method")
                
            return True
            
        except Exception as e:
            logger.error(f"Error validating custom model: {str(e)}")
            return False
            
    def load_base_model(self) -> Optional[SoilModel]:
        """Load the base SoilModel."""
        try:
            local_path = os.path.join(self.cache_dir, "SoilModel.ckpt")
            
            if os.path.exists(local_path):
                logger.info(f"Loading base model from local path: {local_path}")
                model = SoilModel.load_from_checkpoint(local_path)
            else:
                logger.info("Downloading base model from HuggingFace...")
                checkpoint_path = hf_hub_download(
                    repo_id="Nickel5HF/soil-moisture-model",
                    filename="SoilModel.ckpt",
                    local_dir=self.cache_dir
                )
                model = SoilModel.load_from_checkpoint(checkpoint_path)
                
            model.to(self.device)
            model.eval()
            
            param_count = sum(p.numel() for p in model.parameters())
            logger.info(f"Base model loaded successfully with {param_count:,} parameters")
            
            return model
            
        except Exception as e:
            logger.error(f"Error loading base model: {str(e)}")
            logger.error(traceback.format_exc())
            return None
            
    def load_custom_model(self) -> Optional[Any]:
        """Load custom model if available."""
        try:
            custom_model_path = "gaia/models/custom_models/custom_soil_model.py"
            
            if not os.path.exists(custom_model_path):
                logger.info("No custom model found")
                return None
                
            spec = importlib.util.spec_from_file_location(
                "custom_soil_model", 
                custom_model_path
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            if not hasattr(module, 'CustomSoilModel'):
                logger.error("Custom model file missing CustomSoilModel class")
                return None
                
            model_class = getattr(module, 'CustomSoilModel')
            
            if not self.validate_custom_model(model_class):
                logger.error("Custom model validation failed")
                return None
                
            model = model_class()
            logger.info("Custom model loaded successfully")
            return model
            
        except Exception as e:
            logger.error(f"Error loading custom model: {str(e)}")
            logger.error(traceback.format_exc())
            return None
            
    def initialize_model(self) -> None:
        """Initialize either custom or base model."""
        self.model = self.load_custom_model()
        
        if self.model is not None:
            self.model_type = "custom"
            logger.info("Using custom soil moisture model")
        else:
            self.model = self.load_base_model()
            if self.model is not None:
                self.model_type = "base"
                logger.info("Using base soil moisture model")
            else:
                raise RuntimeError("Failed to load either custom or base model")
                
    def get_model(self) -> Any:
        """Get the current model instance."""
        if self.model is None:
            self.initialize_model()
        return self.model
        
    def get_model_type(self) -> str:
        """Get the type of currently loaded model."""
        return self.model_type 