import asyncio
import uvloop
import queue
import uuid
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from gaia.validator.utils.ipc_types import WorkUnit, ResultUnit


logger = logging.getLogger(__name__)


class IOEngine:
    """
    The IO-Engine is the asynchronous brain of the Gaia Validator.
    It handles all network communication, database operations, task scheduling,
    and orchestrates work dispatch to the compute pool.
    """

    def __init__(self, config, work_q, result_q):
        self.config = config
        self.work_q = work_q
        self.result_q = result_q
        self.job_futures = {}  # Maps job_id to asyncio.Future
        self.scheduler = AsyncIOScheduler(timezone=config.SCHEDULER_TIMEZONE)
        self.db_pool = None  # Will be initialized in run()
        self.http_client = None  # Will be initialized in run()
        self.is_running = False

    async def dispatch_and_wait(self, work_unit: WorkUnit) -> ResultUnit:
        """
        Dispatches a job to the compute pool and waits for the result.
        This is the primary interface for sending work to compute workers.
        """
        logger.info(f"Dispatching job {work_unit.job_id} for task {work_unit.task_name}")
        
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.job_futures[work_unit.job_id] = future

        try:
            # Use run_in_executor for the blocking queue.put() call
            await loop.run_in_executor(None, self.work_q.put, work_unit)
            logger.debug(f"Job {work_unit.job_id} queued successfully")
        except Exception as e:
            logger.error(f"Failed to queue job {work_unit.job_id}: {e}")
            future.set_exception(e)
            self.job_futures.pop(work_unit.job_id, None)
            raise

        try:
            # Wait for the result with timeout
            result = await asyncio.wait_for(future, timeout=work_unit.timeout_seconds)
            logger.info(f"Job {work_unit.job_id} completed successfully in {result.execution_time_ms:.2f}ms")
            return result
        except asyncio.TimeoutError:
            logger.error(f"Job {work_unit.job_id} timed out after {work_unit.timeout_seconds}s")
            self.job_futures.pop(work_unit.job_id, None)
            raise
        except Exception as e:
            logger.error(f"Job {work_unit.job_id} failed: {e}")
            raise

    async def dispatch_work(self, task_name: str, payload: dict, timeout_seconds: int = 300) -> ResultUnit:
        """
        Convenience method to create a WorkUnit and dispatch it.
        """
        job_id = str(uuid.uuid4())
        work_unit = WorkUnit(
            job_id=job_id,
            task_name=task_name,
            payload=payload,
            timeout_seconds=timeout_seconds
        )
        return await self.dispatch_and_wait(work_unit)

    async def result_listener(self):
        """
        Background task that continuously listens for results from compute workers
        and resolves the corresponding futures.
        """
        logger.info("Result listener started")
        loop = asyncio.get_running_loop()
        
        while self.is_running:
            try:
                # Use run_in_executor for the blocking queue.get() call with timeout
                result_unit = await loop.run_in_executor(
                    None, 
                    lambda: self.result_q.get(timeout=1)  # 1 second timeout to allow checking is_running
                )
                
                logger.debug(f"Received result for job {result_unit.job_id}")
                
                if future := self.job_futures.pop(result_unit.job_id, None):
                    if result_unit.success:
                        future.set_result(result_unit)
                    else:
                        future.set_exception(Exception(f"Compute Worker Error: {result_unit.error}"))
                else:
                    # Log orphaned result - this shouldn't happen in normal operation
                    logger.warning(f"Received orphaned result for job_id: {result_unit.job_id}")
                    
            except queue.Empty:
                # Normal timeout, continue the loop
                continue
            except Exception as e:
                logger.error(f"Error in result listener: {e}")
                # Continue running even if there's an error

        logger.info("Result listener stopped")

    def setup_scheduled_jobs(self):
        """
        Sets up scheduled jobs for all registered tasks.
        This will be expanded during the logic migration phase.
        """
        logger.info("Setting up scheduled jobs")
        
        # For now, just add a heartbeat job to keep the scheduler active
        self.scheduler.add_job(
            self._heartbeat,
            'interval',
            seconds=300,  # Every 5 minutes
            id='heartbeat',
            name='IO-Engine Heartbeat'
        )
        
        # TODO: During logic migration, register actual tasks here
        # from gaia.tasks.defined_tasks.weather.task import WeatherTask
        # from gaia.tasks.defined_tasks.geomagnetic.task import GeomagneticTask
        # from gaia.tasks.defined_tasks.soilmoisture.task import SoilMoistureTask
        # 
        # tasks = [WeatherTask(), GeomagneticTask(), SoilMoistureTask()]
        # for task in tasks:
        #     self.scheduler.add_job(
        #         task.run_scheduled_job,
        #         'cron',
        #         **self._parse_cron_schedule(task.cron_schedule),
        #         args=[self],
        #         id=f'task_{task.name}',
        #         name=f'Scheduled {task.name} task'
        #     )
        
        logger.info("Scheduled jobs configured")

    def _parse_cron_schedule(self, cron_string: str) -> dict:
        """Helper to parse cron strings into APScheduler format."""
        # Simple parser for basic cron format: minute hour day month day_of_week
        parts = cron_string.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron format: {cron_string}")
        
        minute, hour, day, month, day_of_week = parts
        return {
            'minute': minute,
            'hour': hour,
            'day': day,
            'month': month,
            'day_of_week': day_of_week
        }

    async def _heartbeat(self):
        """Simple heartbeat job to verify scheduler is working."""
        logger.info("IO-Engine heartbeat - scheduler is active")

    async def initialize_connections(self):
        """Initialize database and HTTP connections."""
        logger.info("Initializing connections")
        
        # TODO: Initialize database pool during logic migration
        # from gaia.validator.db.connection import DBPoolManager
        # self.db_pool = await DBPoolManager.get_pool(self.config)
        
        # TODO: Initialize HTTP client during logic migration
        # import httpx
        # self.http_client = httpx.AsyncClient(
        #     timeout=self.config.HTTP_TIMEOUT_SECONDS,
        #     limits=httpx.Limits(max_connections=self.config.HTTP_MAX_CONNECTIONS)
        # )
        
        logger.info("Connections initialized")

    async def cleanup_connections(self):
        """Clean up database and HTTP connections."""
        logger.info("Cleaning up connections")
        
        if self.http_client:
            await self.http_client.aclose()
            self.http_client = None
        
        if self.db_pool:
            await self.db_pool.close()
            self.db_pool = None
        
        logger.info("Connections cleaned up")

    async def run(self):
        """The main entrypoint for the IO-Engine process."""
        logger.info("IO-Engine starting...")
        self.is_running = True
        
        try:
            # Initialize external connections
            await self.initialize_connections()
            
            # Set up scheduled jobs
            self.setup_scheduled_jobs()
            self.scheduler.start()
            logger.info("Scheduler started")
            
            # Start the result listener as a background task
            result_listener_task = asyncio.create_task(
                self.result_listener(), 
                name="result_listener_task"
            )
            
            logger.info("IO-Engine fully initialized and running")
            
            # Keep the engine alive indefinitely
            while self.is_running:
                await asyncio.sleep(60)  # Check every minute
                
        except Exception as e:
            logger.error(f"IO-Engine error: {e}")
            raise
        finally:
            # Cleanup
            logger.info("IO-Engine shutting down...")
            self.is_running = False
            
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
            
            # Wait for result listener to finish
            if 'result_listener_task' in locals():
                await result_listener_task
            
            await self.cleanup_connections()
            logger.info("IO-Engine shutdown complete")


def main(config, work_q, result_q, worker_id=None):  # worker_id is unused here
    """Main entrypoint for the IO-Engine process."""
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("IO-Engine process starting...")
    
    # Install uvloop for better async performance
    uvloop.install()
    
    # Create and run the IO-Engine
    engine = IOEngine(config, work_q, result_q)
    
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logger.info("IO-Engine received interrupt signal")
    except Exception as e:
        logger.error(f"IO-Engine fatal error: {e}")
        raise