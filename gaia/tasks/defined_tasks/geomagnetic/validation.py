"""Data validation models for the geomagnetic task."""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator
import numpy as np
import pandas as pd

class GeomagneticData(BaseModel):
    """Validation model for raw geomagnetic data."""
    timestamp: datetime = Field(..., description="Timestamp of the measurement")
    dst_value: float = Field(..., description="DST value")
    historical_data: Optional[pd.DataFrame] = Field(None, description="Historical DST values")

    @validator('dst_value')
    def validate_dst_value(cls, v):
        """Validate DST value is within reasonable range."""
        if not -500 <= v <= 500:
            raise ValueError(f"DST value {v} outside typical range [-500, 500]")
        return v

    @validator('historical_data')
    def validate_historical_data(cls, v):
        """Validate historical data structure."""
        if v is not None:
            required_columns = {'timestamp', 'Dst'}
            if not all(col in v.columns for col in required_columns):
                raise ValueError(f"Historical data missing required columns: {required_columns}")
        return v

class ProcessedHistoricalData(BaseModel):
    """Validation model for processed historical data."""
    data: pd.DataFrame = Field(..., description="Processed historical data")
    start_time: datetime = Field(..., description="Start time of historical window")
    end_time: datetime = Field(..., description="End time of historical window")
    
    @validator('data')
    def validate_processed_data(cls, v):
        """Validate processed data structure and values."""
        required_columns = {'timestamp', 'value'}
        if not all(col in v.columns for col in required_columns):
            raise ValueError(f"Processed data missing required columns: {required_columns}")
        
        # Check for NaN values
        if v['value'].isna().any():
            raise ValueError("Processed data contains NaN values")
            
        return v

class MinerQuery(BaseModel):
    """Validation model for miner query payload."""
    nonce: str = Field(..., description="Unique identifier for the query")
    data: Dict[str, Any] = Field(..., description="Query data payload")
    
    @validator('data')
    def validate_query_data(cls, v):
        """Validate query data structure."""
        required_fields = {'name', 'timestamp', 'value', 'historical_values'}
        if not all(field in v for field in required_fields):
            raise ValueError(f"Query data missing required fields: {required_fields}")
        return v

class MinerResponse(BaseModel):
    """Validation model for miner responses."""
    miner_uid: str = Field(..., description="Unique identifier for the miner")
    miner_hotkey: str = Field(..., description="Miner's hotkey")
    predicted_value: float = Field(..., description="Predicted DST value")
    query_time: datetime = Field(..., description="Time of the query")
    
    @validator('predicted_value')
    def validate_prediction(cls, v):
        """Validate predicted value."""
        if not -500 <= v <= 500:
            raise ValueError(f"Predicted value {v} outside typical range [-500, 500]")
        return v

class ScoredPrediction(BaseModel):
    """Validation model for scored predictions."""
    prediction: MinerResponse
    ground_truth: float = Field(..., description="Actual DST value")
    score: float = Field(..., description="Calculated score")
    score_time: datetime = Field(..., description="Time when scoring was performed")
    
    @validator('score')
    def validate_score(cls, v):
        """Validate score is normalized correctly."""
        if not 0 <= v <= 1:
            raise ValueError(f"Score {v} outside normalized range [0, 1]")
        return v

def validate_geomag_data(data: Dict[str, Any]) -> GeomagneticData:
    """Validate raw geomagnetic data."""
    return GeomagneticData(**data)

def validate_historical_data(data: Dict[str, Any]) -> ProcessedHistoricalData:
    """Validate processed historical data."""
    return ProcessedHistoricalData(**data)

def validate_miner_query(query: Dict[str, Any]) -> MinerQuery:
    """Validate miner query payload."""
    return MinerQuery(**query)

def validate_miner_response(response: Dict[str, Any]) -> MinerResponse:
    """Validate miner response."""
    return MinerResponse(**response)

def validate_scored_prediction(prediction: Dict[str, Any]) -> ScoredPrediction:
    """Validate scored prediction."""
    return ScoredPrediction(**prediction)

def validate_batch_predictions(predictions: List[Dict[str, Any]]) -> List[MinerResponse]:
    """Validate a batch of predictions."""
    return [validate_miner_response(pred) for pred in predictions]

def validate_batch_scores(scores: List[Dict[str, Any]]) -> List[ScoredPrediction]:
    """Validate a batch of scored predictions."""
    return [validate_scored_prediction(score) for score in scores] 