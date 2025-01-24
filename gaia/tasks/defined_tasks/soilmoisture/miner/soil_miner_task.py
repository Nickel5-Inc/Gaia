"""
Soil Moisture Miner Task

Implements the miner node logic for the soil moisture task.
Responsible for:
1. Data preprocessing and validation
2. Retrieving soil moisture measurements
3. Formatting and returning results
"""

from prefect import flow, task
from typing import Dict, Any, List
import numpy as np
from datetime import datetime
import os
import tempfile
import base64
import logging
import importlib.util

from gaia.tasks.base.task import MinerTask
from ..protocol import SoilRequestData, SoilResponseData, SoilMeasurement
from ..utils.data_sources import fetch_smap_data, fetch_smos_data
from ..utils.preprocessing import preprocess_measurements
from .soil_miner_preprocessing import SoilMinerPreprocessing
from gaia.models.soil_moisture_basemodel import SoilModel

logger = logging.getLogger(__name__)

class SoilMinerTask(MinerTask):
    """
    Miner implementation for soil moisture task.
    Retrieves and processes soil moisture data from multiple sources.
    """
    
    def __init__(self, **kwargs):
        super().__init__(
            name="soil_moisture_miner",
            description="Retrieves and processes soil moisture measurements",
            schema_path="gaia/tasks/defined_tasks/soilmoisture/schema.json",
            **kwargs
        )
        self.preprocessing = SoilMinerPreprocessing(task=self)
        self.model = self._initialize_model()
        self.use_raw_preprocessing = False

    def _initialize_model(self) -> SoilModel:
        """Initialize the soil moisture model"""
        custom_model_path = "gaia/models/custom_models/custom_soil_model.py"
        if os.path.exists(custom_model_path):
            spec = importlib.util.spec_from_file_location(
                "custom_soil_model",
                custom_model_path
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.use_raw_preprocessing = True
            logger.info("Initialized custom soil model")
            return module.CustomSoilModel()
        else:
            logger.info("Initialized base soil model")
            return self.preprocessing.model

    @flow(name="soil_miner_prepare", retries=2)
    async def prepare_flow(self, **kwargs) -> Dict[str, Any]:
        """
        Prepare and validate input data.
        Decodes and preprocesses input data for model.
        """
        # Extract and validate request data
        request_data = kwargs.get('data', {})
        if not request_data:
            raise ValueError("No request data provided")
            
        # Decode combined data
        combined_data = base64.b64decode(request_data['combined_data'])
        
        # Save to temporary file for processing
        with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp:
            tmp.write(combined_data)
            tmp_path = tmp.name
            
        try:
            # Preprocess data
            if self.use_raw_preprocessing:
                processed_data = await self._raw_preprocessing(
                    tmp_path,
                    request_data['sentinel_bounds'],
                    request_data['sentinel_crs']
                )
            else:
                processed_data = await self.preprocessing.process(
                    tmp_path,
                    request_data['sentinel_bounds'],
                    request_data['sentinel_crs']
                )
                
            return {
                'processed_data': processed_data,
                'region_id': request_data['region_id'],
                'target_time': request_data['target_time']
            }
            
        finally:
            # Clean up temporary file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    @flow(name="soil_miner_execute", retries=2)
    async def execute_flow(self, inputs: Dict[str, Any]) -> Any:
        """
        Execute soil moisture prediction.
        Uses model to generate predictions from processed data.
        """
        processed_data = inputs['processed_data']
        region_id = inputs['region_id']
        target_time = inputs['target_time']
        
        try:
            # Generate predictions
            predictions = await self._generate_predictions(processed_data)
            
            # Format response
            response = SoilResponseData(
                request=SoilRequestData(
                    region_id=region_id,
                    target_time=datetime.fromisoformat(target_time)
                ),
                measurements=[
                    SoilMeasurement(
                        value=float(pred['value']),
                        uncertainty=float(pred['uncertainty']),
                        source=pred['source'],
                        quality_flag=int(pred['quality'])
                    )
                    for pred in predictions
                ],
                metadata={
                    'model_version': self.model.version,
                    'preprocessing_version': self.preprocessing.version,
                    'prediction_time': datetime.utcnow().isoformat()
                }
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Error generating predictions: {str(e)}")
            raise

    @task
    async def _raw_preprocessing(
        self,
        data_path: str,
        bounds: Dict,
        crs: str
    ) -> Dict[str, Any]:
        """Raw data preprocessing for custom models"""
        return await self.model.preprocess(data_path, bounds, crs)

    @task
    async def _generate_predictions(
        self,
        processed_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate predictions using the model"""
        predictions = await self.model.predict(processed_data)
        
        # Add uncertainty and quality estimates
        for pred in predictions:
            if 'uncertainty' not in pred:
                pred['uncertainty'] = self._estimate_uncertainty(pred['value'])
            if 'quality' not in pred:
                pred['quality'] = self._estimate_quality(
                    pred['value'],
                    pred['uncertainty']
                )
                
        return predictions

    def _estimate_uncertainty(self, value: float) -> float:
        """Estimate prediction uncertainty"""
        # Simple uncertainty model based on value range
        base_uncertainty = 0.05  # 5% base uncertainty
        range_factor = min(abs(value - 0.5), 0.5) / 0.5  # Higher uncertainty at extremes
        return base_uncertainty * (1 + range_factor)

    def _estimate_quality(self, value: float, uncertainty: float) -> int:
        """Estimate prediction quality flag"""
        if uncertainty > 0.2:
            return 1  # Low quality
        elif uncertainty > 0.1:
            return 2  # Medium quality
        elif 0.0 <= value <= 1.0:
            return 3  # Good quality
        else:
            return 0  # Invalid
