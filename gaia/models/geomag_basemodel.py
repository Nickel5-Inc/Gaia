import traceback
import pandas as pd
import logging
logging.getLogger("prophet.plot").disabled = True
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
            interval_width=0.95,
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
            # Ensure data is in the correct format
            if isinstance(x, pd.DataFrame):
                # Data is already a DataFrame, just ensure columns are correct
                df = x.copy()
            else:
                # Convert to DataFrame if it's not already
                if isinstance(x, (torch.Tensor, np.ndarray)):
                    x = {
                        "ds": datetime.now(pytz.UTC),
                        "y": float(x.item() if hasattr(x, "item") else x),
                    }
                # Create DataFrame from dict
                df = pd.DataFrame(
                    {
                        "ds": [
                            pd.to_datetime(x["ds"] if "ds" in x else x["timestamp"])
                        ],
                        "y": [float(x["y"] if "y" in x else x["value"])],
                    }
                )

            # Ensure timestamps are timezone-naive
            if df["ds"].dt.tz is not None:
                df["ds"] = df["ds"].dt.tz_localize(None)

            if self._is_fallback:
                # Fit model on current data
                self.model.fit(df)

                # Create future dates properly
                last_date = df["ds"].max()
                future_dates = pd.date_range(
                    start=last_date,
                    periods=2,  # One extra period for prediction
                    freq="H",
                    tz=None,  # Ensure timezone-naive
                )
                future = pd.DataFrame({"ds": future_dates})

                # Make prediction
                forecast = self.model.predict(future)
                result = forecast["yhat"].iloc[-1]
            else:
                if hasattr(self.model, "train"):
                    logger.info("Retraining model on latest data")
                    self.model.train(df)
                result = self.model.forecast(df)

            # Handle NaN/Inf values
            if np.isnan(result) or np.isinf(result) or not -1000 < float(result) < 1000:
                logger.warning(f"Invalid prediction value: {result}, using input value")
                return float(df["y"].iloc[-1])

            return float(result)

        except Exception as e:
            logger.error(f"Error during prediction: {e}")
            logger.error(f"traceback: {traceback.format_exc()}")
            logger.error(
                f"Using input value as fallback: {x.get('y', 0.0) if isinstance(x, dict) else 0.0}"
            )
            return float(
                x.get("y", 0.0)
                if isinstance(x, dict)
                else df["y"].iloc[-1] if "df" in locals() else 0.0
            )


class GeoMagBaseModel:
    """Wrapper class for geomagnetic prediction models."""

    def __init__(self, repo_id="Nickel5HF/geomagmodel", filename="BaseModel.py"):
        self.model = None
        self._is_fallback = True

        try:
            # Download the model file from HuggingFace
            logger.info(
                f"Attempting to download model from repo: {repo_id}, file: {filename}"
            )
            model_path = hf_hub_download(repo_id=repo_id, filename=filename)

            # Load the module dynamically
            spec = importlib.util.spec_from_file_location("geomag_model", model_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules["geomag_model"] = module
            spec.loader.exec_module(module)

            # Initialize the model from the loaded module
            model_instance = module.GeoMagModel()
            # Wrap the forecast method as predict if needed
            if hasattr(model_instance, "forecast"):
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
        """
        Make prediction using either HuggingFace or fallback model.

        Args:
            data (pd.DataFrame): Processed geomagnetic data with historical context.

        Returns:
            float: Predicted DST value.
        """
        try:
            # Ensure data is in the correct format
            if isinstance(data, pd.DataFrame):
                df = data.copy()
                if "ds" not in df.columns:
                    df = df.rename(columns={"timestamp": "ds", "value": "y"})
            else:
                # Convert to DataFrame if it's not already
                if isinstance(data, (torch.Tensor, np.ndarray)):
                    data = {
                        "timestamp": datetime.now(pytz.UTC),
                        "value": float(data.item() if hasattr(data, "item") else data),
                    }

                # Create DataFrame from dict
                df = pd.DataFrame(
                    {
                        "ds": [pd.to_datetime(data.get("timestamp", data.get("ds")))],
                        "y": [float(data.get("value", data.get("y", 0.0)))],
                    }
                )

            # Ensure timestamps are timezone-naive
            if df["ds"].dt.tz is not None:
                df["ds"] = df["ds"].dt.tz_convert("UTC").dt.tz_localize(None)

            if self._is_fallback:
                # Use Prophet model
                self.model.fit(df)
                future_dates = pd.date_range(
                    start=df["ds"].max(),
                    periods=2,  # One extra period for prediction
                    freq="h",
                    tz=None,  # Ensure timezone-naive
                )
                future_df = pd.DataFrame({"ds": future_dates})
                forecast = self.model.predict(future_df)
                result = forecast["yhat"].iloc[-1]
            else:
                # Use HuggingFace model
                if hasattr(self.model, "train"):
                    logger.info("Retraining model on latest data")
                    self.model.train(df)

                # Define the number of periods and frequency
                periods = 1  # Forecasting the next hour
                freq = "h"  # Hourly frequency

                # Call forecast with correct parameters
                forecast = self.model.forecast(periods=periods, freq=freq)

                # Assuming the forecast method returns a DataFrame similar to Prophet's
                result = forecast["yhat"].iloc[-1]

            # Convert result to Python float and handle invalid values
            if hasattr(result, "item"):
                result = result.item()
            elif isinstance(result, (np.ndarray, np.generic)):
                result = float(result)

            # Handle NaN/Inf values
            if np.isnan(result) or np.isinf(result) or not -1000 < float(result) < 1000:
                logger.warning(f"Invalid prediction value: {result}, using input value")
                return float(df["y"].iloc[-1])

            return float(result)

        except Exception as e:
            logger.error(f"Error during prediction: {e}")
            logger.error(f"traceback: {traceback.format_exc()}")
            fallback_value = (
                data.get("value", 0.0)
                if isinstance(data, dict)
                else df["y"].iloc[-1] if "df" in locals() else 0.0
            )
            logger.error(f"Using input value as fallback: {fallback_value}")
            return float(fallback_value)
