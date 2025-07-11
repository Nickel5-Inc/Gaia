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

    def __init__(self, config, work_q, result_q, test_mode=False):
        self.config = config
        self.work_q = work_q
        self.result_q = result_q
        self.test_mode = test_mode
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
        
        if self.test_mode:
            logger.info("🧪 TEST MODE ENABLED - Tasks and services will run with faster processing")
            logger.info("🧪 Core validator services will use accelerated intervals for testing")
        else:
            logger.info("🔧 PRODUCTION MODE - Full validator services will use normal intervals")
        
        # For now, just add a heartbeat job to keep the scheduler active
        self.scheduler.add_job(
            self._heartbeat,
            'interval',
            seconds=300,  # Every 5 minutes
            id='heartbeat',
            name='IO-Engine Heartbeat'
        )
        
        # Register actual tasks
        tasks = []
        
        # Try to register WeatherTask
        try:
            from gaia.tasks.defined_tasks.weather.task import WeatherTask
            weather_task = WeatherTask()
            # Store test mode for weather task to access
            weather_task.test_mode = self.test_mode
            tasks.append(weather_task)
            logger.info(f"Successfully imported WeatherTask (test_mode: {self.test_mode})")
        except ImportError as e:
            logger.warning(f"Could not import WeatherTask: {e}")
        
        # Try to register GeomagneticTask
        try:
            from gaia.tasks.defined_tasks.geomagnetic.core.task import GeomagneticTask
            from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
            # Initialize with proper database manager
            geomagnetic_task = GeomagneticTask(
                node_type="validator",
                db_manager=ValidatorDatabaseManager(),
                test_mode=self.test_mode
            )
            tasks.append(geomagnetic_task)
            logger.info("Successfully imported GeomagneticTask")
        except ImportError as e:
            logger.warning(f"Could not import GeomagneticTask: {e}")
        except Exception as e:
            logger.warning(f"Could not initialize GeomagneticTask: {e}")
        
        # Try to register SoilMoistureTask
        try:
            from gaia.tasks.defined_tasks.soilmoisture.core.task import SoilMoistureTask
            from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
            # Initialize with proper database manager
            soil_task = SoilMoistureTask(
                db_manager=ValidatorDatabaseManager(),
                node_type="validator",
                test_mode=self.test_mode
            )
            tasks.append(soil_task)
            logger.info("Successfully imported SoilMoistureTask")
        except ImportError as e:
            logger.warning(f"Could not import SoilMoistureTask: {e}")
        except Exception as e:
            logger.warning(f"Could not initialize SoilMoistureTask: {e}")
        
        # Register all successfully imported tasks
        for task in tasks:
            try:
                cron_params = self._parse_cron_schedule(task.cron_schedule)
                self.scheduler.add_job(
                    task.run_scheduled_job,
                    'cron',
                    **cron_params,
                    args=[self],
                    id=f'task_{task.name}',
                    name=f'Scheduled {task.name} task',
                    max_instances=1,  # Prevent overlapping runs
                    coalesce=True     # Combine missed runs
                )
                logger.info(f"Registered task: {task.name} with schedule: {task.cron_schedule}")
            except Exception as e:
                logger.error(f"Failed to register task {task.name}: {e}")
        
        if not tasks:
            logger.warning("No tasks were successfully registered - only heartbeat will run")
        
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

    async def _start_core_service_tasks(self):
        """Start core validator service tasks that run continuously."""
        mode_str = "test mode (accelerated)" if self.test_mode else "production mode"
        logger.info(f"Starting core validator service tasks in {mode_str}...")
        
        tasks = []
        
        # Status logger - monitors and logs system status
        tasks.append(asyncio.create_task(
            self._status_logger(), 
            name="status_logger"
        ))
        
        # Database monitor - monitors database health and sync
        tasks.append(asyncio.create_task(
            self._database_monitor(), 
            name="database_monitor"
        ))
        
        # Memory cleanup - periodic memory management
        tasks.append(asyncio.create_task(
            self._memory_cleanup_task(), 
            name="memory_cleanup"
        ))
        
        # Connection health monitor - monitors HTTP client health
        tasks.append(asyncio.create_task(
            self._connection_health_monitor(), 
            name="connection_health"
        ))
        
        # Weight calculation and setting - core scoring functionality
        tasks.append(asyncio.create_task(
            self._weight_management_task(), 
            name="weight_management"
        ))
        
        # Metagraph sync - sync with Bittensor network
        tasks.append(asyncio.create_task(
            self._metagraph_sync_task(), 
            name="metagraph_sync"
        ))
        
        logger.info(f"Started {len(tasks)} core service tasks in {mode_str}")
        return tasks

    async def _monitor_core_service_tasks(self, tasks):
        """Monitor core service tasks and restart if needed."""
        for task in tasks:
            if task.done():
                task_name = task.get_name()
                try:
                    task.result()  # This will raise if the task failed
                    logger.warning(f"Core service task '{task_name}' completed unexpectedly")
                except Exception as e:
                    logger.error(f"Core service task '{task_name}' failed: {e}")
                # TODO: Implement task restart logic if needed

    # Placeholder implementations for core service tasks
    # These would need to be properly implemented with actual validator logic
    
    async def _status_logger(self):
        """Log system status periodically."""
        interval = 60 if self.test_mode else 300  # 1 min in test, 5 min in production
        
        while self.is_running:
            try:
                mode_indicator = "🧪" if self.test_mode else "📊"
                logger.info(f"{mode_indicator} System status: IO-Engine operational")
                # TODO: Add actual status monitoring
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error in status logger: {e}")
                await asyncio.sleep(30 if self.test_mode else 60)

    async def _database_monitor(self):
        """Monitor database health and sync status."""
        interval = 120 if self.test_mode else 600  # 2 min in test, 10 min in production
        
        while self.is_running:
            try:
                mode_indicator = "🧪" if self.test_mode else "💾"
                logger.debug(f"{mode_indicator} Database monitor: Checking health")
                # TODO: Add actual database health checks
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error in database monitor: {e}")
                await asyncio.sleep(60 if self.test_mode else 120)

    async def _memory_cleanup_task(self):
        """Periodic memory cleanup task."""
        interval = 300 if self.test_mode else 1800  # 5 min in test, 30 min in production
        
        while self.is_running:
            try:
                mode_indicator = "🧪" if self.test_mode else "🧹"
                logger.debug(f"{mode_indicator} Memory cleanup: Running periodic cleanup")
                # TODO: Add actual memory cleanup
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error in memory cleanup: {e}")
                await asyncio.sleep(120 if self.test_mode else 300)

    async def _connection_health_monitor(self):
        """Monitor HTTP client health."""
        interval = 180 if self.test_mode else 900  # 3 min in test, 15 min in production
        
        while self.is_running:
            try:
                mode_indicator = "🧪" if self.test_mode else "🌐"
                logger.debug(f"{mode_indicator} Connection health: Monitoring HTTP clients")
                # TODO: Add actual connection health checks
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error in connection health monitor: {e}")
                await asyncio.sleep(90 if self.test_mode else 180)

    async def _weight_management_task(self):
        """Handle weight calculation and setting."""
        interval = 600 if self.test_mode else 3600  # 10 min in test, 1 hour in production
        
        while self.is_running:
            try:
                mode_indicator = "🧪" if self.test_mode else "⚖️"
                logger.info(f"{mode_indicator} Weight management: Placeholder for weight calculations")
                # TODO: Implement actual weight calculation and setting logic
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error in weight management: {e}")
                await asyncio.sleep(300 if self.test_mode else 600)

    async def _metagraph_sync_task(self):
        """Sync with Bittensor metagraph."""
        interval = 300 if self.test_mode else 1200  # 5 min in test, 20 min in production
        
        while self.is_running:
            try:
                mode_indicator = "🧪" if self.test_mode else "🔄"
                logger.info(f"{mode_indicator} Metagraph sync: Placeholder for metagraph synchronization")
                # TODO: Implement actual metagraph sync logic
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error in metagraph sync: {e}")
                await asyncio.sleep(180 if self.test_mode else 300)

    async def initialize_connections(self):
        """Initialize database, HTTP connections, and core validator components."""
        logger.info("Initializing connections and core validator components")
        
        # Initialize database pool
        from gaia.validator.db.connection import get_db_pool
        self.db_pool = await get_db_pool()
        logger.info("Database pool initialized")
        
        # Initialize HTTP clients for miner communication
        import httpx
        self.http_client = httpx.AsyncClient(
            timeout=self.config.HTTP_TIMEOUT_SECONDS,
            limits=httpx.Limits(max_connections=self.config.HTTP_MAX_CONNECTIONS)
        )
        
        # Additional HTTP client for miner communication
        self.miner_client = httpx.AsyncClient(
            timeout=30.0, 
            follow_redirects=True, 
            verify=False
        )
        
        # API client for external services
        self.api_client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            ),
            transport=httpx.AsyncHTTPTransport(retries=3),
        )
        logger.info("HTTP clients initialized")
        
        # Initialize core validator components (adjusted for test mode)
        await self._initialize_core_validator_components()
        
        logger.info("All connections and core components initialized successfully")

    async def _initialize_core_validator_components(self):
        """Initialize core validator components like substrate, metagraph, etc."""
        if self.test_mode:
            logger.info("🧪 Initializing core validator components in TEST MODE...")
        else:
            logger.info("🔧 Initializing core validator components in PRODUCTION MODE...")
        
        try:
            # Initialize substrate connection and validator registration
            # Note: This would need the actual validator instance and substrate setup
            # For now, we'll create placeholders for the missing components
            
            if self.test_mode:
                logger.info("🧪 Test mode: Using lightweight component initialization")
                # TODO: Add test-specific substrate/bittensor initialization
                # self.substrate = self._get_test_substrate_interface()
                # self.metagraph = self._get_test_metagraph()
            else:
                logger.info("🔧 Production mode: Using full component initialization")
                # TODO: Add actual substrate/bittensor initialization
                # self.substrate = self._get_substrate_interface()
                # self.metagraph = self._sync_metagraph()
            
            logger.warning("🚧 Core validator components (substrate/metagraph) not yet implemented in multiprocessing architecture")
            logger.info("✅ Placeholder initialization completed")
            
        except Exception as e:
            logger.error(f"Failed to initialize core validator components: {e}")
            raise

    async def cleanup_connections(self):
        """Clean up database and HTTP connections."""
        logger.info("Cleaning up connections")
        
        # Cleanup HTTP clients
        if hasattr(self, 'http_client') and self.http_client:
            await self.http_client.aclose()
            self.http_client = None
            
        if hasattr(self, 'miner_client') and self.miner_client:
            await self.miner_client.aclose()
            self.miner_client = None
            
        if hasattr(self, 'api_client') and self.api_client:
            await self.api_client.aclose()
            self.api_client = None
        
        if self.db_pool:
            from gaia.validator.db.connection import close_db_pool
            await close_db_pool()
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
            
            # Start core validator service tasks (with test mode adjustments)
            core_service_tasks = await self._start_core_service_tasks()
            
            logger.info("IO-Engine fully initialized and running")
            
            # Keep the engine alive indefinitely
            while self.is_running:
                await asyncio.sleep(60)  # Check every minute
                
                # Monitor core service tasks health
                if core_service_tasks:
                    await self._monitor_core_service_tasks(core_service_tasks)
                
        except Exception as e:
            logger.error(f"IO-Engine error: {e}")
            raise
        finally:
            # Cleanup
            logger.info("IO-Engine shutting down...")
            self.is_running = False
            
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
            
            # Cleanup core service tasks
            if 'core_service_tasks' in locals() and core_service_tasks:
                logger.info("Shutting down core service tasks...")
                for task in core_service_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*core_service_tasks, return_exceptions=True)
                logger.info("Core service tasks shut down")
            
            # Wait for result listener to finish
            if 'result_listener_task' in locals():
                await result_listener_task
            
            await self.cleanup_connections()
            logger.info("IO-Engine shutdown complete")


def main(config, work_q, result_q, test_mode=False, worker_id=None):  # worker_id is unused here
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
    engine = IOEngine(config, work_q, result_q, test_mode)
    
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logger.info("IO-Engine received interrupt signal")
    except Exception as e:
        logger.error(f"IO-Engine fatal error: {e}")
        raise