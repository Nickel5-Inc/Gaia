import traceback
import pandas as pd
from prophet import Prophet
from datetime import datetime, timedelta
import pytz
from fiber.logging_utils import get_logger
from huggingface_hub import hf_hub_download
import importlib.util
import sys
import numpy as np
import json
import torch

logger = get_logger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        return super().default(obj)

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
            # Ensure timestamp is timezone-aware
            timestamp = pd.to_datetime(x['timestamp'])
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=pytz.UTC)
            
            # Convert input to Prophet format
            df = pd.DataFrame({
                'ds': [timestamp],
                'y': [float(x['value'])]
            })
            
            # Fit model on current data point
            self.model.fit(df)
            
            # Make prediction for next hour
            future = self.model.make_future_dataframe(periods=1, freq='H')
            future['ds'] = future['ds'].dt.tz_localize(pytz.UTC)
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
            model_instance = module.GeoMagModel()
            # Wrap the forecast method as predict if needed
            if hasattr(model_instance, 'forecast'):
                self.model = model_instance
                self._is_fallback = False
                logger.info("Successfully loaded GeoMagModel from Hugging Face.")
            else:
                raise AttributeError("Model does not have required 'forecast' method")
            
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
            # Ensure data is in the correct format
            if isinstance(data, (torch.Tensor, np.ndarray)):
                data = {
                    'timestamp': datetime.now(pytz.UTC),
                    'value': float(data.item() if hasattr(data, 'item') else data)
                }
            
            # Convert Timestamp to datetime if needed
            if isinstance(data.get('timestamp'), pd.Timestamp):
                data['timestamp'] = data['timestamp'].to_pydatetime()
            
            if self._is_fallback:
                result = self.model.predict(data)
            else:
                if hasattr(self.model, 'train'):
                    logger.info("Retraining model on latest data")
                    self.model.train(data)
                result = self.model.forecast(data)
            
            # Convert result to Python float and handle invalid values
            if hasattr(result, 'item'):
                result = result.item()
            elif isinstance(result, (np.ndarray, np.generic)):
                result = float(result)
            
            # Handle NaN/Inf values
            if np.isnan(result) or np.isinf(result) or not -1000 < float(result) < 1000:
                logger.warning(f"Invalid prediction value: {result}, using input value")
                return float(data.get('value', 0.0))
            
            return float(result)
            
        except Exception as e:
            logger.error(f"Error during prediction: {e}")
            logger.error(f"traceback: {traceback.format_exc()}")
            logger.error(f"Using input value as fallback: {data.get('value', 0.0)}")
            return float(data.get('value', 0.0))
