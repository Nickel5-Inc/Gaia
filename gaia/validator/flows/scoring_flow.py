from prefect import task
from typing import Dict, List, Optional
import asyncio
import math
from .base_flow import BaseFlow
from ..work_persistence import WorkPersistenceManager

class ScoringFlow(BaseFlow):
    """Main scoring flow for the validator.
    
    Handles:
    - Score collection from tasks
    - Weight calculation
    - Weight setting on chain
    - Score persistence
    """
    
    def __init__(
        self,
        work_manager: WorkPersistenceManager,
        geomagnetic_task,
        soil_task,
        weight_setter,
        **kwargs
    ):
        super().__init__(
            name="main_scoring_flow",
            description="Main scoring flow for Gaia validator",
            **kwargs
        )
        self.work_manager = work_manager
        self.geomagnetic_task = geomagnetic_task
        self.soil_task = soil_task
        self.weight_setter = weight_setter

    @task(tags=["compute"])
    async def calculate_weights(
        self,
        geomagnetic_scores: List[float],
        soil_scores: List[float]
    ) -> List[float]:
        """Calculate weights using compute resources."""
        async with self.work_manager.task_context("compute", "calculate_weights"):
            weights = [0.0] * 256
            for idx in range(256):
                geomagnetic_score = geomagnetic_scores[idx]
                soil_score = soil_scores[idx]

                if math.isnan(geomagnetic_score) and math.isnan(soil_score):
                    weights[idx] = 0.0
                    self.logger.debug(f"Both scores nan - setting weight to 0")
                elif math.isnan(geomagnetic_score):
                    weights[idx] = 0.5 * soil_score
                    self.logger.debug(
                        f"Geo score nan - using soil score: {weights[idx]}"
                    )
                elif math.isnan(soil_score):
                    geo_normalized = math.exp(-abs(geomagnetic_score) / 10)
                    weights[idx] = 0.5 * geo_normalized
                    self.logger.debug(
                        f"UID {idx}: Soil score nan - normalized geo score: {geo_normalized} -> weight: {weights[idx]}"
                    )
                else:
                    geo_normalized = math.exp(-abs(geomagnetic_score) / 10)
                    weights[idx] = (0.5 * geo_normalized) + (0.5 * soil_score)
                    self.logger.debug(
                        f"UID {idx}: Both scores valid - geo_norm: {geo_normalized}, soil: {soil_score} -> weight: {weights[idx]}"
                    )

            return weights

    @task(retries=3, retry_delay_seconds=5, tags=["database"])
    async def persist_scores(self, scores: Dict, task_type: str):
        """Persist scores to database."""
        async with self.work_manager.task_context("database", "persist_scores"):
            await self.work_manager.db_manager.update_scores(scores, task_type)

    @task(
        retries=5,
        retry_delay_seconds=60,
        tags=["blockchain"],
        timeout_seconds=180
    )
    async def set_weights(self, weights: List[float]) -> bool:
        """Set weights on blockchain with retries."""
        async with self.work_manager.task_context("blockchain", "set_weights"):
            return await self.weight_setter.set_weights(weights)

    async def run(self, **kwargs):
        """Main scoring flow execution."""
        try:
            while True:
                # Get scores concurrently
                geomagnetic_future = self.geomagnetic_task.get_recent_scores()
                soil_future = self.soil_task.get_recent_scores()
                
                geomagnetic_result, soil_result = await asyncio.gather(
                    geomagnetic_future,
                    soil_future
                )

                # Persist scores concurrently
                await asyncio.gather(
                    self.persist_scores(geomagnetic_result, "geomagnetic"),
                    self.persist_scores(soil_result, "soil")
                )

                # Calculate weights
                weights = await self.calculate_weights(
                    geomagnetic_result.get("score", [float("nan")] * 256),
                    soil_result.get("score", [float("nan")] * 256)
                )

                # Set weights if needed
                if weights and any(w != 0.0 for w in weights):
                    await self.set_weights(weights)

                await asyncio.sleep(300)
                
        except Exception as e:
            self.logger.error(f"Error in scoring flow: {e}")
            raise 