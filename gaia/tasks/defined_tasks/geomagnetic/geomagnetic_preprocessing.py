from gaia.tasks.base.components.preprocessing import Preprocessing
import pandas as pd
from gaia.models.geomag_basemodel import GeoMagBaseModel
from typing import Dict, Any, Optional, List
import logging
from prefect import task
from uuid import uuid4
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class GeomagneticPreprocessing(Preprocessing):
    """Preprocessing component for geomagnetic data."""

    COLUMN_MAPPINGS = {
        'input': {'timestamp': 'timestamp', 'value': 'Dst'},
        'model': {'timestamp': 'ds', 'value': 'y'}
    }

    @task(
        name="prepare_historical_data",
        retries=3,
        retry_delay_seconds=60,
        description="Prepare historical data for prediction"
    )
    def prepare_historical_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Prepare historical data for prediction.
        
        Args:
            data (Dict[str, Any]): Raw data containing timestamp, dst_value, and optional historical_data
            
        Returns:
            pd.DataFrame: Processed DataFrame with historical context
        """
        # Create DataFrame with current data point
        df = pd.DataFrame({
            'timestamp': [pd.to_datetime(data['timestamp'])],
            'Dst': [float(data['dst_value'])]
        })
        
        # Add historical data if available
        if data.get('historical_data') is not None:
            historical_df = pd.DataFrame(data['historical_data'])
            df = pd.concat([historical_df, df], ignore_index=True)
        
        return self._standardize_dataframe(df)

    @task(
        name="preprocess_geomag_data",
        retries=3,
        retry_delay_seconds=60,
        description="Preprocess geomagnetic data for prediction"
    )
    def preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare data for model input.
        
        Args:
            data (pd.DataFrame): DataFrame with timestamp and Dst columns
            
        Returns:
            pd.DataFrame: Processed data ready for model input
        """
        df = self._standardize_dataframe(data)
        
        # Normalize values for model input
        df['y'] = df['Dst'] / 100.0
        
        # Rename columns for model compatibility
        return df.rename(columns=self.COLUMN_MAPPINGS['model'])

    @task(
        name="prepare_miner_query",
        retries=3,
        retry_delay_seconds=60,
        description="Prepare data for miner query"
    )
    async def prepare_miner_query(
        self,
        data: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Prepare data payload for querying miners.
        
        Args:
            data (Dict[str, Any]): Data containing timestamp, dst_value, and historical_data
            context (Dict[str, Any]): Execution context
            
        Returns:
            Dict[str, Any]: Formatted payload for miner query
        """
        historical_records = self._format_historical_records(data.get('historical_data'))
        
        return {
            "nonce": str(uuid4()),
            "data": {
                "name": "Geomagnetic Data",
                "timestamp": data['timestamp'].isoformat(),
                "value": data['dst_value'],
                "historical_values": historical_records
            }
        }

    def _standardize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize DataFrame format."""
        df = df.copy()
        
        # Ensure timestamp column is datetime with UTC
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
        
        # Ensure value column is named consistently
        if 'value' in df.columns:
            df = df.rename(columns={'value': 'Dst'})
        
        return df.sort_values('timestamp').reset_index(drop=True)

    def _format_historical_records(self, historical_data: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
        """Format historical data for API response."""
        if historical_data is None:
            return []
            
        return [
            {
                "timestamp": row["timestamp"].isoformat(),
                "Dst": row["Dst"]
            }
            for _, row in historical_data.iterrows()
        ]
