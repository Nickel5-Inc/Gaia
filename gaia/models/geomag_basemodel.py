import pandas as pd
from prophet import Prophet
from datetime import datetime, timedelta
from fiber.logging_utils import get_logger

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
    
    def __init__(self, repo_id="Nickel5HF/geomagmodel", filename="prophet_model.pkl"):
        self.model = None
        try:
            # Try to load Prophet model from HuggingFace
            logger.info(f"Attempting to load Prophet model from {repo_id}")
            # TODO: Implement HuggingFace model loading
            raise NotImplementedError("HuggingFace model loading not implemented yet")
            
        except Exception as e:
            logger.warning(f"Failed to load HuggingFace model: {e}")
            logger.info("Using fallback Prophet model")
            self.model = FallbackGeoMagModel()
    
    def predict(self, data) -> float:
        """Make prediction using either HuggingFace or fallback model."""
        return self.model.predict(data)
