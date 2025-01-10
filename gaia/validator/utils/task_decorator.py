import asyncio
import functools
import time
from typing import Any, Callable, Optional, TypeVar, ParamSpec
from dataclasses import dataclass
from datetime import datetime, timezone
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

P = ParamSpec('P')
R = TypeVar('R')

@dataclass
class TaskStep:
    """Represents a step within a validator task"""
    name: str
    description: str
    timeout_seconds: int
    max_retries: int = 3
    retry_delay_seconds: int = 5

class TaskStepError(Exception):
    """Custom exception for task step failures"""
    def __init__(self, step_name: str, original_error: Exception):
        self.step_name = step_name
        self.original_error = original_error
        super().__init__(f"Task step '{step_name}' failed: {str(original_error)}")

def task_step(
    name: str,
    description: str,
    timeout_seconds: int,
    max_retries: int = 3,
    retry_delay_seconds: int = 5
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator for handling task steps with timeout and retry logic.
    
    Args:
        name: Name of the task step
        description: Description of what the step does
        timeout_seconds: Maximum time allowed for step execution
        max_retries: Maximum number of retry attempts
        retry_delay_seconds: Delay between retry attempts
    
    Returns:
        Decorated function with timeout and retry handling
    """
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                step = TaskStep(
                    name=name,
                    description=description,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    retry_delay_seconds=retry_delay_seconds
                )
                
                attempt = 0
                last_error = None
                
                while attempt < step.max_retries:
                    attempt += 1
                    start_time = time.time()
                    
                    try:
                        logger.info(f"Starting task step '{step.name}' (attempt {attempt}/{step.max_retries})")
                        logger.debug(f"Step description: {step.description}")
                        
                        # Execute the function with timeout
                        result = await asyncio.wait_for(
                            func(*args, **kwargs),
                            timeout=step.timeout_seconds
                        )
                        
                        duration = time.time() - start_time
                        logger.info(f"Successfully completed task step '{step.name}' in {duration:.2f}s")
                        return result
                        
                    except asyncio.TimeoutError:
                        duration = time.time() - start_time
                        error_msg = f"Task step '{step.name}' timed out after {duration:.2f}s"
                        logger.error(error_msg)
                        last_error = asyncio.TimeoutError(error_msg)
                        
                    except Exception as e:
                        duration = time.time() - start_time
                        logger.error(f"Task step '{step.name}' failed after {duration:.2f}s: {str(e)}")
                        last_error = e
                        
                    if attempt < step.max_retries:
                        logger.info(f"Retrying task step '{step.name}' in {step.retry_delay_seconds}s...")
                        await asyncio.sleep(step.retry_delay_seconds)
                    
                # If we get here, all retries failed
                raise TaskStepError(step.name, last_error)
                
            return async_wrapper
            
        else:
            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                step = TaskStep(
                    name=name,
                    description=description,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    retry_delay_seconds=retry_delay_seconds
                )
                
                attempt = 0
                last_error = None
                
                while attempt < step.max_retries:
                    attempt += 1
                    start_time = time.time()
                    
                    try:
                        logger.info(f"Starting task step '{step.name}' (attempt {attempt}/{step.max_retries})")
                        logger.debug(f"Step description: {step.description}")
                        
                        # For sync functions, we'll use asyncio.wait_for in an event loop
                        async def run_with_timeout():
                            return await asyncio.wait_for(
                                asyncio.get_event_loop().run_in_executor(None, func, *args, **kwargs),
                                timeout=step.timeout_seconds
                            )
                            
                        result = asyncio.get_event_loop().run_until_complete(run_with_timeout())
                        
                        duration = time.time() - start_time
                        logger.info(f"Successfully completed task step '{step.name}' in {duration:.2f}s")
                        return result
                        
                    except asyncio.TimeoutError:
                        duration = time.time() - start_time
                        error_msg = f"Task step '{step.name}' timed out after {duration:.2f}s"
                        logger.error(error_msg)
                        last_error = asyncio.TimeoutError(error_msg)
                        
                    except Exception as e:
                        duration = time.time() - start_time
                        logger.error(f"Task step '{step.name}' failed after {duration:.2f}s: {str(e)}")
                        last_error = e
                        
                    if attempt < step.max_retries:
                        logger.info(f"Retrying task step '{step.name}' in {step.retry_delay_seconds}s...")
                        time.sleep(step.retry_delay_seconds)
                    
                # If we get here, all retries failed
                raise TaskStepError(step.name, last_error)
                
            return sync_wrapper
            
    return decorator

# Example usage:
"""
@task_step(
    name="sync_metagraph",
    description="Synchronize metagraph nodes from the network",
    timeout_seconds=30,
    max_retries=3,
    retry_delay_seconds=5
)
async def sync_metagraph(self):
    self.metagraph.sync_nodes()
    return len(self.metagraph.nodes)
""" 