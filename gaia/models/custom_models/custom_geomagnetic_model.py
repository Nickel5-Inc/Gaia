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
import time
from gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data import fetch_data
from typing import Dict, Any  # Import Dict và Any từ typing
import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from gaia.tasks.defined_tasks.geomagnetic.utils.process_geomag_data import parse_data, clean_data

logger = get_logger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""

    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        return super().default(obj)

class CustomGeomagneticModel:
    def __init__(self):
        """Initialize your custom geomagnetic model."""
        self.changepoint_prior_scale = 0.5
        self.seasonality_prior_scale = 15.0
        self.seasonality_mode = 'additive'
        self.weekly_seasonality = True
        self.model = None

    async def get_data(self, diff: int = 3):
        try:
            current_time = datetime.now(pytz.UTC)
            month = current_time.month
            year = current_time.year
            hour = current_time.hour
            today = datetime.today()
            all_data = []
            
            for i in range(diff):
                # Trừ tháng cho mỗi vòng lặp
                if i != 0:
                    month -= 1

                # Xử lý nếu tháng nhỏ hơn 1
                if month < 1:
                    month = 12
                    year -= 1
                
                # Kiểm tra nếu ngày hôm nay là ngày đầu tháng và giờ nhỏ hơn 2
                if today.day == 1 and hour < 2:
                    if month == 1:
                        month = 12
                        year -= 1
                    else:
                        month -= 1

                print(f"Đang xử lý: {year}-{month}")

                # Tạo URL và lấy dữ liệu
                url = create_url(year, month)
                raw_data = await fetch_data(url)

                # Parse và clean dữ liệu
                parsed_df = parse_data(raw_data)
                cleaned_df = clean_data(parsed_df)

                # Thêm dữ liệu vào đầu danh sách
                all_data.insert(0, cleaned_df)

        except Exception as e:
            print(f"Lỗi khi xử lý tháng {month}/{year}: {e}")
            print(traceback.format_exc())

        # Kết hợp tất cả dữ liệu thành DataFrame
        final_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
        return final_df

    @staticmethod
    def prepare_for_prophet(df):
        """
        Prepare the DataFrame for Prophet by renaming columns as required.
        
        Args:
            df (pd.DataFrame): The cleaned DataFrame with 'timestamp' and 'Dst' columns.
        
        Returns:
            pd.DataFrame: A DataFrame with 'ds' (timestamp) and 'y' (Dst) columns for Prophet.
        """
        return df.rename(columns={'timestamp': 'ds', 'Dst': 'y'})

    def train(self, df):
        """
        Train the Prophet model on the given DataFrame.
        
        Args:
            df (pd.DataFrame): The DataFrame prepared for Prophet.
        """
        df = self.prepare_for_prophet(df)
        self.model = Prophet(
            interval_width=0.70,
            changepoint_prior_scale=self.changepoint_prior_scale,
            seasonality_prior_scale=self.seasonality_prior_scale,
            seasonality_mode=self.seasonality_mode
        )

        if self.weekly_seasonality:
            self.model.add_seasonality(name='weekly', period=7, fourier_order=5, prior_scale=10)

        self.model.fit(df)

    def forecast(self, periods=1, freq='h'):
        """
        Make a future forecast with the trained model.
        
        Args:
            periods (int): The number of future periods to forecast. Defaults to 1.
            freq (str): The frequency of the forecast ('h' for hours). Defaults to 'h'.
        
        Returns:
            pd.DataFrame: The forecast DataFrame with 'ds', 'yhat', 'yhat_lower', and 'yhat_upper' columns.
        """
        if self.model is None:
            raise ValueError("Model has not been trained. Call train() before forecast().")
        
        future_dates = self.model.make_future_dataframe(periods=periods, freq=freq)
        forecast = self.model.predict(future_dates)
        return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

    def cross_validate(self, initial='36 hours', period='12 hours', horizon='1 hours'):
        """
        Perform cross-validation on the trained Prophet model.
        
        Args:
            initial (str): Initial training period for cross-validation.
            period (str): Frequency of making predictions.
            horizon (str): Forecast horizon for each prediction.
        
        Returns:
            pd.DataFrame: Cross-validation results including metrics.
        """
        if self.model is None:
            raise ValueError("Model has not been trained. Call train() before cross_validate().")
        
        return cross_validation(self.model, initial=initial, period=period, horizon=horizon)

    def run_inference(self, data: pd.DataFrame) -> Dict[str, Any]:
        try:
            logger.info(data)
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

            # Train model
            self.train(df)

            # Forecasting parameters
            periods = 1  # Forecasting the next hour
            freq = "h"  # Hourly frequency

            # Call forecast with correct parameters
            forecast = self.forecast(periods=periods, freq=freq)
            
            # Check forecast output
            if "yhat" not in forecast.columns:
                raise ValueError("Forecast output is missing 'yhat' column.")

            result = forecast["yhat"].iloc[-1]
            predictions = {
                "predicted_value": float(result)
            }

            return predictions
        
        except Exception as e:
            error_message = f"Error in run_inference: {str(e)}"
            traceback_str = traceback.format_exc()
            print(error_message)
            print(traceback_str)
            logger.error(error_message)
            logger.error(traceback_str)
            return {
                "error": error_message,
                "traceback": traceback_str
            }