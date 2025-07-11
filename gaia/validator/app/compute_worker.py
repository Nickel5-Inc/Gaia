import time
import resource
import os
import queue
import logging
import traceback
import psutil
import gc
from typing import Optional, Any, Dict
from gaia.validator.utils.ipc_types import ResultUnit

# Handler registry for task dispatch
HANDLER_REGISTRY = {
    # Weather task handlers
    "weather.hash.gfs_compute": None,  # Will be lazy-loaded
    "weather.hash.verification_compute": None,  # Will be lazy-loaded  
    "weather.hash.forecast_verify": None,  # Will be lazy-loaded
    "weather.scoring.day1_compute": None,  # Will be lazy-loaded
    "weather.scoring.era5_final_compute": None,  # Will be lazy-loaded
    "weather.scoring.forecast_verification": None,  # Will be lazy-loaded
}


def _lazy_load_handler(task_name: str, logger: logging.Logger):
    """Lazy load handlers to keep worker startup fast."""
    logger.info(f"🔄 Loading handler for task: {task_name}")
    
    handler_start_time = time.monotonic()
    handler = None
    
    try:
        if task_name == "weather.hash.gfs_compute":
            from gaia.tasks.defined_tasks.weather.validator.hashing import handle_gfs_hash_computation
            HANDLER_REGISTRY[task_name] = handle_gfs_hash_computation
            handler = handle_gfs_hash_computation
        elif task_name == "weather.hash.verification_compute":
            from gaia.tasks.defined_tasks.weather.validator.hashing import handle_verification_hash_computation
            HANDLER_REGISTRY[task_name] = handle_verification_hash_computation
            handler = handle_verification_hash_computation
        elif task_name == "weather.hash.forecast_verify":
            from gaia.tasks.defined_tasks.weather.validator.hashing import handle_forecast_hash_verification
            HANDLER_REGISTRY[task_name] = handle_forecast_hash_verification
            handler = handle_forecast_hash_verification
        elif task_name == "weather.scoring.day1_compute":
            from gaia.tasks.defined_tasks.weather.validator.scoring import handle_day1_scoring_computation
            HANDLER_REGISTRY[task_name] = handle_day1_scoring_computation
            handler = handle_day1_scoring_computation
        elif task_name == "weather.scoring.era5_final_compute":
            from gaia.tasks.defined_tasks.weather.validator.scoring import handle_era5_final_scoring_computation
            HANDLER_REGISTRY[task_name] = handle_era5_final_scoring_computation
            handler = handle_era5_final_scoring_computation
        elif task_name == "weather.scoring.forecast_verification":
            from gaia.tasks.defined_tasks.weather.validator.scoring import handle_forecast_verification_computation
            HANDLER_REGISTRY[task_name] = handle_forecast_verification_computation
            handler = handle_forecast_verification_computation
        
        load_time_ms = (time.monotonic() - handler_start_time) * 1000
        
        if handler:
            logger.info(f"✅ Handler loaded successfully in {load_time_ms:.2f}ms: {task_name}")
        else:
            logger.error(f"❌ No handler available for task: {task_name}")
            
        return handler
        
    except ImportError as e:
        load_time_ms = (time.monotonic() - handler_start_time) * 1000
        logger.error(f"❌ Failed to import handler for {task_name} after {load_time_ms:.2f}ms: {e}")
        return None
    except Exception as e:
        load_time_ms = (time.monotonic() - handler_start_time) * 1000
        logger.error(f"❌ Unexpected error loading handler for {task_name} after {load_time_ms:.2f}ms: {e}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return None


def _get_memory_info() -> Dict[str, Any]:
    """Get current memory usage information."""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        return {
            "rss_mb": memory_info.rss / (1024 * 1024),
            "vms_mb": memory_info.vms / (1024 * 1024),
            "percent": process.memory_percent(),
        }
    except Exception:
        return {"rss_mb": 0, "vms_mb": 0, "percent": 0}


def _get_cpu_info() -> Dict[str, Any]:
    """Get current CPU usage information."""
    try:
        process = psutil.Process()
        return {
            "percent": process.cpu_percent(),
            "num_threads": process.num_threads(),
        }
    except Exception:
        return {"percent": 0, "num_threads": 0}


def main(config, work_q, result_q, worker_id: int):
    """The main execution loop for a compute worker process."""
    # Setup logging for this worker
    pid = os.getpid()
    worker_name = f"ComputeWorker-{worker_id}"
    
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper()),
        format=f'%(asctime)s - {worker_name}[{pid}] - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(worker_name)
    
    # Log startup with system information
    logger.info(f"🚀 {worker_name} starting up (PID: {pid})")
    
    try:
        system_info = {
            "cpu_count": os.cpu_count(),
            "memory_total_gb": psutil.virtual_memory().total / (1024**3),
            "memory_available_gb": psutil.virtual_memory().available / (1024**3),
        }
        logger.info(f"💻 System info: {system_info}")
    except Exception as e:
        logger.warning(f"Could not gather system info: {e}")

    # Set memory limits and log them
    limit_mb = config.PROCESS_MAX_RSS_MB.get('compute', 1024)
    limit_bytes = limit_mb * 1024 * 1024
    
    try:
        resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
        logger.info(f"🔒 Memory limit set to {limit_mb}MB ({limit_bytes:,} bytes)")
    except Exception as e:
        logger.error(f"❌ Failed to set memory limit: {e}")

    # Job execution statistics
    jobs_completed = 0
    jobs_failed = 0
    total_execution_time_ms = 0
    handlers_loaded = set()
    
    logger.info(f"🎯 Ready to process up to {config.MAX_JOBS_PER_WORKER} jobs (timeout: {config.WORKER_QUEUE_TIMEOUT_S}s)")

    for job_number in range(config.MAX_JOBS_PER_WORKER):
        try:
            # Log resource usage before job
            pre_job_memory = _get_memory_info()
            pre_job_cpu = _get_cpu_info()
            
            logger.debug(f"📊 Pre-job resources: Memory={pre_job_memory['rss_mb']:.1f}MB ({pre_job_memory['percent']:.1f}%), CPU={pre_job_cpu['percent']:.1f}%")
            
            # Wait for work from the queue
            logger.debug(f"⏳ Waiting for job {job_number+1}/{config.MAX_JOBS_PER_WORKER} (timeout: {config.WORKER_QUEUE_TIMEOUT_S}s)")
            work_unit = work_q.get(timeout=config.WORKER_QUEUE_TIMEOUT_S)
            
            logger.info(f"📥 Job {job_number+1}/{config.MAX_JOBS_PER_WORKER} received: {work_unit.job_id}")
            logger.info(f"🏷️  Task: {work_unit.task_name}")
            logger.debug(f"📦 Payload keys: {list(work_unit.payload.keys()) if work_unit.payload else 'None'}")

            job_start_time = time.monotonic()
            execution_success = False
            error_details = None
            
            try:
                # Handler resolution and loading
                handler = HANDLER_REGISTRY.get(work_unit.task_name)
                if not handler:
                    logger.info(f"🔄 Handler not cached, lazy loading: {work_unit.task_name}")
                    handler = _lazy_load_handler(work_unit.task_name, logger)
                    if handler:
                        handlers_loaded.add(work_unit.task_name)
                    
                if not handler:
                    raise ValueError(f"No handler found for task: {work_unit.task_name}")
                else:
                    logger.debug(f"✅ Handler ready: {work_unit.task_name}")

                # Execute the actual task
                logger.info(f"🚀 Executing task: {work_unit.task_name}")
                task_start_time = time.monotonic()
                
                result_payload = handler(config, **work_unit.payload)
                
                task_execution_time_ms = (time.monotonic() - task_start_time) * 1000
                logger.info(f"✅ Task completed successfully in {task_execution_time_ms:.2f}ms")
                
                # Log result summary
                if result_payload is not None:
                    if isinstance(result_payload, dict):
                        logger.debug(f"📋 Result keys: {list(result_payload.keys())}")
                    elif hasattr(result_payload, '__len__'):
                        logger.debug(f"📋 Result length: {len(result_payload)}")
                    else:
                        logger.debug(f"📋 Result type: {type(result_payload).__name__}")
                else:
                    logger.debug("📋 Result: None")

                result = ResultUnit(
                    job_id=work_unit.job_id,
                    task_name=work_unit.task_name,
                    success=True,
                    result=result_payload,
                    worker_pid=pid,
                    execution_time_ms=0  # Will be set below
                )
                execution_success = True
                jobs_completed += 1

            except Exception as e:
                task_execution_time_ms = (time.monotonic() - job_start_time) * 1000
                error_details = f"{type(e).__name__}: {e}"
                
                logger.error(f"❌ Task failed after {task_execution_time_ms:.2f}ms: {error_details}")
                logger.error(f"🔍 Stack trace:\n{traceback.format_exc()}")
                
                result = ResultUnit(
                    job_id=work_unit.job_id,
                    task_name=work_unit.task_name,
                    success=False,
                    error=error_details,
                    worker_pid=pid,
                    execution_time_ms=0  # Will be set below
                )
                jobs_failed += 1

            # Calculate total job time and update result
            job_execution_time_ms = (time.monotonic() - job_start_time) * 1000
            result.execution_time_ms = job_execution_time_ms
            total_execution_time_ms += job_execution_time_ms
            
            # Log post-job resource usage
            post_job_memory = _get_memory_info()
            post_job_cpu = _get_cpu_info()
            
            memory_delta = post_job_memory['rss_mb'] - pre_job_memory['rss_mb']
            logger.info(f"📊 Job completed in {job_execution_time_ms:.2f}ms")
            logger.debug(f"📊 Post-job resources: Memory={post_job_memory['rss_mb']:.1f}MB ({post_job_memory['percent']:.1f}%, Δ{memory_delta:+.1f}MB)")
            
            # Memory cleanup if usage is high
            if post_job_memory['rss_mb'] > limit_mb * 0.8:  # 80% of limit
                logger.warning(f"🧹 High memory usage ({post_job_memory['rss_mb']:.1f}MB), running garbage collection")
                collected = gc.collect()
                logger.info(f"🧹 Garbage collection freed {collected} objects")
            
            # Send result back to IO-Engine
            try:
                result_q.put(result)
                logger.debug(f"📤 Result sent to IO-Engine for job {work_unit.job_id}")
            except Exception as e:
                logger.error(f"❌ Failed to send result to IO-Engine: {e}")

        except queue.Empty:
            logger.info(f"⏰ Queue timeout after {config.WORKER_QUEUE_TIMEOUT_S}s - no more work available")
            break
            
        except Exception as e:
            jobs_failed += 1
            logger.error(f"💥 CRITICAL ERROR in worker loop: {e}")
            logger.error(f"🔍 Stack trace:\n{traceback.format_exc()}")
            # Exit to be restarted by the supervisor
            break

    # Final statistics and cleanup
    avg_execution_time = total_execution_time_ms / max(jobs_completed + jobs_failed, 1)
    success_rate = (jobs_completed / max(jobs_completed + jobs_failed, 1)) * 100
    
    logger.info(f"📈 Worker statistics:")
    logger.info(f"  • Jobs completed: {jobs_completed}")
    logger.info(f"  • Jobs failed: {jobs_failed}")
    logger.info(f"  • Success rate: {success_rate:.1f}%")
    logger.info(f"  • Average execution time: {avg_execution_time:.2f}ms")
    logger.info(f"  • Total execution time: {total_execution_time_ms:.2f}ms")
    logger.info(f"  • Handlers loaded: {len(handlers_loaded)} ({', '.join(sorted(handlers_loaded))})")
    
    final_memory = _get_memory_info()
    logger.info(f"📊 Final memory usage: {final_memory['rss_mb']:.1f}MB ({final_memory['percent']:.1f}%)")
    
    logger.info(f"🏁 {worker_name} (PID: {pid}) shutting down cleanly")