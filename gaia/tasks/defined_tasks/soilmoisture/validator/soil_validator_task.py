"""
Soil Moisture Validator Task

Implements the validator node logic for the soil moisture task.
Responsible for:
1. Preparing validation parameters
2. Validating miner responses
3. Scoring responses based on quality metrics
"""

from prefect import flow, task
from typing import Dict, Any, List
import numpy as np
from datetime import datetime, timedelta, timezone
import asyncio
import base64
from uuid import uuid4
import logging
from sqlalchemy import text

from gaia.tasks.base.task import ValidatorTask
from ..protocol import SoilRequestData, SoilResponseData, ValidationResult
from ..utils.data_quality import check_data_quality, calculate_uncertainty
from ..utils.spatial import validate_coordinates
from ..utils.smap_api import get_smap_data_for_sentinel_bounds
from .soil_validator_preprocessing import SoilValidatorPreprocessing

logger = logging.getLogger(__name__)

class SoilValidatorTask(ValidatorTask):
    """
    Validator implementation for soil moisture task.
    Validates and scores miner responses based on data quality metrics.
    """
    
    def __init__(self, **kwargs):
        super().__init__(
            name="soil_moisture_validator",
            description="Validates and scores soil moisture measurements",
            schema_path="gaia/tasks/defined_tasks/soilmoisture/schema.json",
            **kwargs
        )
        self.preprocessing = SoilValidatorPreprocessing()
        self.prediction_horizon = timedelta(hours=6)
        self.scoring_delay = timedelta(days=3)

    @flow(name="soil_validator_prepare", retries=2)
    async def prepare_flow(self, **kwargs) -> Dict[str, Any]:
        """
        Prepare validation parameters for soil moisture task.
        Generates request parameters and validation criteria.
        """
        current_time = kwargs.get('current_time', datetime.now(timezone.utc))
        target_smap_time = self.get_smap_time_for_validator(current_time)
        ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)
        
        # Get regions that need processing
        regions = await self._get_pending_regions(target_smap_time)
        
        validation_params = {
            'target_time': target_smap_time,
            'ifs_forecast_time': ifs_forecast_time,
            'regions': regions,
            'criteria': {
                'max_uncertainty': 0.1,
                'min_quality_flag': 2,
                'required_sources': ['SMAP', 'SMOS'],
                'max_response_time': 300
            }
        }
        
        return validation_params

    @flow(name="soil_validator_execute", retries=2)
    async def execute_flow(self, inputs: Dict[str, Any]) -> Any:
        """
        Execute validation logic for soil moisture responses.
        Processes regions and collects miner responses.
        """
        regions = inputs['regions']
        target_time = inputs['target_time']
        ifs_forecast_time = inputs['ifs_forecast_time']
        
        results = []
        for region in regions:
            try:
                # Prepare region data
                task_data = await self._prepare_region_data(region, target_time)
                
                # Query miners
                responses = await self._query_miners(task_data)
                
                if responses:
                    metadata = {
                        "region_id": region["id"],
                        "target_time": target_time,
                        "data_collection_time": datetime.now(timezone.utc),
                        "ifs_forecast_time": ifs_forecast_time,
                    }
                    results.append({
                        'region': region,
                        'responses': responses,
                        'metadata': metadata
                    })
                    
                    # Update region status
                    await self._update_region_status(region['id'], 'sent_to_miners')
                    
            except Exception as e:
                logger.error(f"Error processing region {region['id']}: {str(e)}")
                continue
                
        return results

    @flow(name="soil_validator_score", retries=1)
    async def score_flow(self, result: Any) -> float:
        """
        Score miner responses based on validation results.
        Evaluates predictions against actual SMAP data.
        """
        if not result:
            return 0.0
            
        scores = []
        for item in result:
            region = item['region']
            responses = item['responses']
            metadata = item['metadata']
            
            try:
                # Get actual SMAP data for scoring
                actual_data = await get_smap_data_for_sentinel_bounds(
                    region['sentinel_bounds'],
                    metadata['target_time']
                )
                
                # Score each response
                for response in responses:
                    score = await self._score_prediction(
                        response,
                        actual_data,
                        metadata
                    )
                    if score is not None:
                        scores.append(score)
                        
            except Exception as e:
                logger.error(f"Error scoring region {region['id']}: {str(e)}")
                continue
                
        return np.mean(scores) if scores else 0.0

    @task
    async def _get_pending_regions(self, target_time: datetime) -> List[Dict]:
        """Get regions that need processing"""
        query = """
            SELECT * FROM soil_moisture_regions 
            WHERE status = 'pending'
            AND target_time = :target_time
        """
        return await self.db.fetch_all(query, {"target_time": target_time})

    @task
    async def _prepare_region_data(self, region: Dict, target_time: datetime) -> Dict:
        """Prepare region data for miner query"""
        if not region.get("combined_data"):
            raise ValueError(f"Region {region['id']} has no data")
            
        combined_data = region["combined_data"]
        encoded_data = base64.b64encode(combined_data).decode("ascii")
        
        return {
            "region_id": region["id"],
            "combined_data": encoded_data,
            "sentinel_bounds": region["sentinel_bounds"],
            "sentinel_crs": region["sentinel_crs"],
            "target_time": target_time.isoformat(),
        }

    @task
    async def _query_miners(self, task_data: Dict) -> List[Dict]:
        """Query miners with task data"""
        payload = {"nonce": str(uuid4()), "data": task_data}
        return await self.query_miners(payload=payload, endpoint="/soilmoisture-request")

    @task
    async def _update_region_status(self, region_id: int, status: str):
        """Update region status in database"""
        query = """
            UPDATE soil_moisture_regions 
            SET status = :status
            WHERE id = :region_id
        """
        await self.db.execute(query, {"region_id": region_id, "status": status})

    @task
    async def _score_prediction(
        self,
        prediction: Dict,
        actual_data: Dict,
        metadata: Dict
    ) -> float:
        """Score a single prediction"""
        try:
            rmse = np.sqrt(np.mean((prediction['values'] - actual_data['values'])**2))
            return max(0, 1 - (rmse / 0.1))  # Normalize score
        except Exception as e:
            logger.error(f"Error calculating score: {str(e)}")
            return None

    def get_smap_time_for_validator(self, current_time: datetime) -> datetime:
        """Get SMAP data time for validation"""
        # Round down to nearest 6 hours
        hours = (current_time.hour // 6) * 6
        return current_time.replace(hour=hours, minute=0, second=0, microsecond=0)

    def get_ifs_time_for_smap(self, smap_time: datetime) -> datetime:
        """Get IFS forecast time for SMAP time"""
        # Get most recent 00Z or 12Z forecast
        if smap_time.hour < 12:
            return smap_time.replace(hour=0)
        return smap_time.replace(hour=12)
