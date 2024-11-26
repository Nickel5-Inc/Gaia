import pandas as pd
from prophet import Prophet
from datetime import datetime, timedelta
from fiber.logging_utils import get_logger
from huggingface_hub import hf_hub_download
import importlib.util
import sys

logger = get_logger(__name__)


class FallbackGeoMagModel:
    """A simple fallback model using Prophet when HuggingFace model isn't available."""
    
    def __init__(self):
        self.model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=True,
            interval_width=0.95
        )
        
    def predict(self, x):
        """
        Make prediction using Prophet model.
        
        Args:
            x: Dictionary containing timestamp and value
            
        Returns:
            float: Predicted DST value for next hour
        """
        try:
            # Convert input to Prophet format
            df = pd.DataFrame({
                'ds': [pd.to_datetime(x['timestamp'])],
                'y': [float(x['value'])]
            })
            
            # Fit model on current data point
            self.model.fit(df)
            
            # Make prediction for next hour
            future = self.model.make_future_dataframe(periods=1, freq='H')
            forecast = self.model.predict(future)
            
            # Return the predicted value
            return float(forecast['yhat'].iloc[-1])
            
        except Exception as e:
            logger.error(f"Error in Prophet prediction: {e}")
            return float(x['value'])  # Fallback to persistence forecast


class GeoMagBaseModel:
    """Wrapper class for geomagnetic prediction models."""
    
    def __init__(self, repo_id="Nickel5HF/geomagmodel", filename="BaseModel.py"):
        self.model = None
        self._is_fallback = True
        
        try:
            # Download the model file from HuggingFace
            logger.info(f"Attempting to download model from repo: {repo_id}, file: {filename}")
            model_path = hf_hub_download(repo_id=repo_id, filename=filename)
            
            # Load the module dynamically
            spec = importlib.util.spec_from_file_location("geomag_model", model_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules["geomag_model"] = module
            spec.loader.exec_module(module)
            
            # Initialize the model from the loaded module
            self.model = module.GeoMagModel()
            self._is_fallback = False
            logger.info("Successfully loaded GeoMagModel from Hugging Face.")
            
        except Exception as e:
            logger.warning(f"Failed to load HuggingFace model: {e}")
            logger.info("Using fallback Prophet model")
            self.model = FallbackGeoMagModel()
    
    @property
    def is_fallback(self) -> bool:
        """Check if using fallback model"""
        return self._is_fallback
    
    def predict(self, data) -> float:
        """Make prediction using either HuggingFace or fallback model."""
        try:
            return self.model.predict(data)
        except Exception as e:
            logger.error(f"Error during prediction: {e}")
            # Return input value as persistence forecast
            return float(data.get('value', 0.0))
