"""
Weather Task implementation for the Gaia Validator v4.0 architecture.

This module contains the WeatherTask class that implements the GaiaTask
interface and coordinates with the IO-Engine for scheduled execution.
"""

import logging
from gaia.tasks.defined_tasks.base import GaiaTask
from .validator import lifecycle

logger = logging.getLogger(__name__)


class WeatherTask(GaiaTask):
    """
    Weather forecasting and validation task.
    
    This task coordinates the entire weather validation lifecycle:
    1. Query miners for weather forecasts
    2. Verify input data hashes
    3. Compute validation scores
    4. Update weights and ensemble forecasts
    """

    @property
    def name(self) -> str:
        """A unique, machine-readable name for the task."""
        return "weather"

    @property
    def cron_schedule(self) -> str:
        """APScheduler-compatible cron string for daily execution at 18:05 UTC."""
        # Run every day at 18:05 UTC (5 minutes after the typical GFS run time)
        return "5 18 * * *"

    async def run_scheduled_job(self, io_engine) -> None:
        """
        The main entry point called by the scheduler.
        
        This method orchestrates the entire weather validation lifecycle
        for one scheduled run, including dispatching work to the compute pool
        and updating the database through the IO-Engine.
        
        Args:
            io_engine: The IOEngine instance for database and compute access
        """
        logger.info(f"Starting scheduled weather validation run")
        
        try:
            # Delegate to the lifecycle orchestrator
            await lifecycle.orchestrate_full_weather_run(io_engine)
            logger.info("Weather validation run completed successfully")
            
        except Exception as e:
            logger.error(f"Weather validation run failed: {e}", exc_info=True)
            # The error will be logged, but we don't re-raise to avoid
            # disrupting the scheduler for other tasks