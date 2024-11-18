from tasks.base.task import Task
from datetime import datetime, timedelta, timezone
import numpy as np
from typing import Dict, List, Optional, Tuple

class SoilMoistureTask(Task):
    """
    Task for soil moisture prediction using satellite and weather data.
    Workflow:
    1. Validator selects random region avoiding urban/water areas
    2. Collects Sentinel-2, IFS weather, and SRTM elevation data
    3. Sends combined data to miners
    4. Miners predict soil moisture 6 hours into future
    5. After 3-day SMAP latency, validator scores predictions
    """
    
    def __init__(self):
        super().__init__(
            name="SoilMoistureTask",
            description="Soil moisture prediction from satellite/weather data",
            task_type="atomic",
            metadata=Metadata(),
            inputs=SoilMoistureInputs(),
            outputs=SoilMoistureOutputs(),
            scoring_mechanism=SoilMoistureScoringMechanism(),
            preprocessing=SoilMoisturePreprocessing()
        )
        self.prediction_horizon = timedelta(hours=6)
        self.scoring_delay = timedelta(days=3)
        self.max_daily_regions = 10
        
    def get_next_valid_time(self, current_time: datetime) -> datetime:
        """Get next valid SMAP measurement time."""
        valid_times = [
            (1, 30), (4, 30), 
            (7, 30), (10, 30),
            (13, 30), (16, 30),
            (19, 30), (22, 30)
        ]
        target_time = current_time + self.prediction_horizon
        current_hour = target_time.hour
        next_time = min(valid_times, key=lambda x: 
            (x[0] - current_hour) % 24 if (x[0] - current_hour) % 24 > 0 
            else float('inf'))
            
        return target_time.replace(
            hour=next_time[0],
            minute=next_time[1],
            second=0,
            microsecond=0
        )

    def validator_execute(self):
        """Execute validator workflow."""
        current_time = datetime.now(timezone.utc)
        target_time = self.get_next_valid_time(current_time)
        regions = self.inputs.get_daily_regions()
        
        for region in regions:
            metadata = {
                'query_time': current_time,
                'target_time': target_time,
                'sentinel_bounds': region['sentinel_bounds'],
                'sentinel_crs': region['sentinel_crs'],
            }
            
            predictions = self.query_miners({
                'combined_data': region['combined_data'],
                'sentinel_bounds': region['sentinel_bounds'],
                'sentinel_crs': region['sentinel_crs'],
                'target_time': target_time
            })
            
            self.add_task_to_queue(predictions, metadata)

    def miner_execute(self, data):
        """Execute miner workflow."""
        try:
            processed_data = self.preprocessing.process_miner_data(data)
            predictions = self.run_model_inference(processed_data)
            
            return {
                'surface_sm': predictions['surface'],
                'rootzone_sm': predictions['rootzone'],
                'uncertainty_surface': predictions.get('uncertainty', None) #TODO: seperate surface and rootzone uncertainty
            }
            
        except Exception as e:
            print(f"Error in miner execution: {str(e)}")
            return None

    def score_predictions(self, predictions: Dict, ground_truth: Dict) -> Dict:
        """
        Score predictions RMSE + something else:
        - RMSE
        - Spatial structure score somehow
        MIGHT CHANGE THIS
        """
        return self.scoring_mechanism.compute_scores(predictions, ground_truth)

    def validator_score(self):
        """Score predictions after SMAP delay."""
        pass

    def miner_preprocess(self, preprocessing=None, inputs=None):
        """Preprocess data for model input."""
        pass

    def query_miners(self, data):
        """Query miners for predictions."""
        pass

    def add_task_to_queue(self, predictions: Dict, metadata: Dict):
        """Add task to queue for later scoring."""
        pass

    def get_pending_tasks(self) -> List[Dict]:
        """Get tasks ready for scoring (past 3-day SMAP latency)."""
        pass

    def move_task_to_history(self, task: Dict, ground_truth: Dict, scores: Dict, score_time: datetime):
        """Move scored task to history."""
        pass