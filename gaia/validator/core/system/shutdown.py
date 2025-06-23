"""
Graceful Shutdown Functions
===========================

Functions for handling graceful shutdown of the validator system.
"""

import asyncio
import signal
from typing import Dict, Any, List, Callable
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


async def handle_graceful_shutdown(
    validator_state: Any,
    tasks: List[asyncio.Task] = None,
    signal_received: signal.Signals = None
) -> Dict[str, Any]:
    """
    Handle graceful shutdown of the validator.
    
    Args:
        validator_state: Current validator state
        tasks: List of running tasks to cleanup
        signal_received: Signal that triggered shutdown
        
    Returns:
        Dict containing shutdown results
    """
    shutdown_result = {
        'success': True,
        'signal': signal_received.name if signal_received else 'programmatic',
        'tasks_cancelled': 0,
        'resources_cleaned': False,
        'errors': []
    }
    
    try:
        logger.info(f"🛑 Graceful shutdown initiated (signal: {shutdown_result['signal']})")
        
        # Cancel background tasks
        if tasks:
            logger.info(f"Canceling {len(tasks)} background tasks...")
            for task in tasks:
                if not task.done():
                    task.cancel()
                    shutdown_result['tasks_cancelled'] += 1
            
            # Wait for tasks to complete cancellation
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                logger.info(f"✅ Cancelled {shutdown_result['tasks_cancelled']} tasks")
        
        # Cleanup resources
        try:
            from .memory import cleanup_resources
            cleanup_result = cleanup_resources(validator_state)
            shutdown_result['resources_cleaned'] = not cleanup_result.get('errors', [])
            if cleanup_result.get('errors'):
                shutdown_result['errors'].extend(cleanup_result['errors'])
        except Exception as e:
            shutdown_result['errors'].append(f"Resource cleanup error: {e}")
        
        logger.info(f"✅ Graceful shutdown completed with {len(shutdown_result['errors'])} errors")
        
    except Exception as e:
        logger.error(f"Error during graceful shutdown: {e}")
        shutdown_result['success'] = False
        shutdown_result['errors'].append(f"Shutdown error: {e}")
    
    return shutdown_result


def setup_signal_handlers(shutdown_callback: Callable):
    """
    Setup signal handlers for graceful shutdown.
    
    Args:
        shutdown_callback: Async function to call on shutdown signal
    """
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig.name}, initiating graceful shutdown...")
        asyncio.create_task(shutdown_callback(sig))
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler) 