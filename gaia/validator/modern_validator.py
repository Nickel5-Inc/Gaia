"""
Modern Gaia Validator with Service-Oriented Architecture.
Integrates the service architecture with existing business logic while preserving 100% compatibility.
"""

import asyncio
import logging
import os
import signal
import sys
import time
import traceback
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import Dict, Any, Optional

# Import existing business logic components
from .validator import GaiaValidator  # Existing validator for fallback
from .services.orchestrator import ServiceOrchestrator

logger = logging.getLogger(__name__)


class ModernGaiaValidator:
    """
    Modern Gaia Validator that uses service-oriented architecture while preserving
    100% business logic compatibility with the existing validator.
    
    This class serves as a bridge between the new service architecture and the
    existing business logic, ensuring seamless operation and gradual migration.
    """
    
    def __init__(self, args):
        """Initialize the modern validator with service architecture."""
        logger.info("🚀 Initializing Modern Gaia Validator with Service Architecture...")
        
        self.args = args
        
        # Initialize the original validator first to preserve all business logic
        self.legacy_validator = GaiaValidator(args)
        
        # Initialize service orchestrator with legacy validator integration
        self.service_orchestrator = ServiceOrchestrator(self.legacy_validator)
        
        # Service architecture state
        self.services_enabled = os.getenv('VALIDATOR_SERVICES_ENABLED', 'true').lower() in ['true', '1', 'yes']
        self.services_initialized = False
        self.services_running = False
        
        # Setup signal handlers for graceful shutdown
        self._cleanup_done = False
        self._shutdown_event = asyncio.Event()
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)
            
        logger.info(f"Modern validator initialized - Services enabled: {self.services_enabled}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signame = signal.Signals(signum).name
        logger.info(f"Modern validator received shutdown signal {signame}")
        
        if not self._cleanup_done:
            if asyncio.get_event_loop().is_running():
                self._shutdown_event.set()
            else:
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                loop.run_until_complete(self._initiate_shutdown())
    
    async def initialize(self) -> bool:
        """Initialize the modern validator and optionally the service architecture."""
        try:
            logger.info("🔧 Setting up core validator components...")
            
            # Always setup the legacy validator first for business logic compatibility
            if not self.legacy_validator.setup_neuron():
                logger.error("Failed to setup core validator neuron")
                return False
            
            logger.info("✅ Core validator components initialized")
            
            # Initialize services if enabled
            if self.services_enabled:
                logger.info("🎛️ Initializing service architecture...")
                
                # Initialize service orchestrator
                if await self.service_orchestrator.initialize():
                    self.services_initialized = True
                    logger.info("✅ Service architecture initialized successfully")
                else:
                    logger.warning("⚠️ Service architecture initialization failed - running in legacy mode")
                    self.services_enabled = False
            else:
                logger.info("📋 Running in legacy mode (services disabled)")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during modern validator initialization: {e}", exc_info=True)
            return False
    
    async def start(self) -> bool:
        """Start the modern validator and services."""
        try:
            logger.info("🎯 Starting modern validator...")
            
            # Start services if initialized
            if self.services_initialized:
                logger.info("🎛️ Starting service architecture...")
                
                if await self.service_orchestrator.start():
                    self.services_running = True
                    logger.info("✅ Service architecture started successfully")
                    
                    # Log service status for debugging
                    self.service_orchestrator.log_service_status()
                else:
                    logger.warning("⚠️ Service architecture failed to start - falling back to legacy mode")
                    self.services_enabled = False
                    self.services_running = False
            
            logger.info("✅ Modern validator started successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error starting modern validator: {e}", exc_info=True)
            return False
    
    async def run(self):
        """Run the modern validator main loop."""
        try:
            # Initialize and start
            if not await self.initialize():
                logger.error("❌ Modern validator initialization failed")
                return
                
            if not await self.start():
                logger.error("❌ Modern validator start failed")
                return
            
            # Run the main execution loop
            await self._run_main_loop()
            
        except KeyboardInterrupt:
            logger.info("🛑 Keyboard interrupt received")
        except Exception as e:
            logger.error(f"❌ Unhandled exception in modern validator: {e}", exc_info=True)
        finally:
            if not self._cleanup_done:
                await self._initiate_shutdown()
    
    async def _run_main_loop(self):
        """Run the main execution loop with service integration."""
        try:
            logger.info("🔄 Starting main execution loop...")
            
            if self.services_running:
                # Service-oriented execution
                await self._run_with_services()
            else:
                # Legacy execution for fallback
                await self._run_legacy_mode()
                
        except Exception as e:
            logger.error(f"❌ Error in main execution loop: {e}", exc_info=True)
            raise
    
    async def _run_with_services(self):
        """Run with service architecture enabled."""
        logger.info("🎛️ Running with service architecture...")
        
        # The services handle most of the execution through the ExecutionService
        # We just need to monitor and handle shutdown
        
        # Create a monitor task for service health
        monitor_task = asyncio.create_task(self._monitor_services())
        
        # Create shutdown waiter
        shutdown_waiter = asyncio.create_task(self._shutdown_event.wait())
        
        try:
            # Wait for either shutdown signal or monitor completion
            done, pending = await asyncio.wait(
                [monitor_task, shutdown_waiter],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
                
            # Wait for cancellation to complete
            await asyncio.gather(*pending, return_exceptions=True)
            
            logger.info("🔄 Service-oriented execution completed")
            
        except Exception as e:
            logger.error(f"❌ Error in service-oriented execution: {e}")
            # Cancel all tasks
            for task in [monitor_task, shutdown_waiter]:
                if not task.done():
                    task.cancel()
            await asyncio.gather(monitor_task, shutdown_waiter, return_exceptions=True)
    
    async def _run_legacy_mode(self):
        """Run in legacy mode as fallback."""
        logger.info("📋 Running in legacy mode...")
        
        # Use the existing validator's main execution
        try:
            await self.legacy_validator.main()
        except Exception as e:
            logger.error(f"❌ Error in legacy mode execution: {e}")
            raise
    
    async def _monitor_services(self):
        """Monitor service health and handle failures."""
        logger.info("🩺 Starting service health monitoring...")
        
        consecutive_failures = 0
        max_failures = 3
        
        while not self._shutdown_event.is_set():
            try:
                # Check service health
                health_status = await self.service_orchestrator.get_service_health()
                
                # Count failed services
                failed_services = []
                for service_name, status in health_status.get("services", {}).items():
                    if status.get("status") == "error":
                        failed_services.append(service_name)
                
                if failed_services:
                    consecutive_failures += 1
                    logger.warning(f"⚠️ Service health issues detected: {failed_services} "
                                 f"(consecutive failures: {consecutive_failures})")
                    
                    # If too many consecutive failures, consider emergency action
                    if consecutive_failures >= max_failures:
                        logger.error(f"🚨 Too many consecutive service failures ({consecutive_failures}) - "
                                   f"triggering emergency procedures")
                        
                        # Try emergency cleanup
                        await self.service_orchestrator.emergency_shutdown()
                        
                        # Fall back to legacy mode
                        logger.warning("🔄 Falling back to legacy mode due to service failures")
                        self.services_running = False
                        
                        # Start legacy execution
                        asyncio.create_task(self._run_legacy_mode())
                        break
                else:
                    # Reset failure counter on success
                    if consecutive_failures > 0:
                        logger.info("✅ Service health recovered")
                        consecutive_failures = 0
                
                # Check memory pressure via services
                memory_status = await self.service_orchestrator.check_memory_pressure_via_service()
                if memory_status.get("status") in ["emergency", "critical"]:
                    logger.warning(f"🚨 Memory pressure detected: {memory_status}")
                    
                    # Trigger cleanup via services
                    cleanup_result = await self.service_orchestrator.cleanup_resources_via_service()
                    logger.info(f"🧹 Emergency cleanup result: {cleanup_result.get('status', 'unknown')}")
                
                # Sleep before next check
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                logger.info("🛑 Service monitoring cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in service monitoring: {e}")
                consecutive_failures += 1
                await asyncio.sleep(30)  # Shorter retry on error
    
    async def _initiate_shutdown(self):
        """Handle graceful shutdown of the modern validator."""
        if self._cleanup_done:
            logger.info("🔄 Cleanup already completed")
            return
        
        logger.info("🛑 Initiating graceful shutdown of modern validator...")
        
        try:
            # Set shutdown event
            self._shutdown_event.set()
            
            # Stop services first if running
            if self.services_running:
                logger.info("🎛️ Stopping service architecture...")
                try:
                    await asyncio.wait_for(self.service_orchestrator.stop(), timeout=30)
                    logger.info("✅ Service architecture stopped")
                except asyncio.TimeoutError:
                    logger.warning("⚠️ Service shutdown timed out - forcing emergency shutdown")
                    await self.service_orchestrator.emergency_shutdown()
                except Exception as e:
                    logger.error(f"❌ Error stopping services: {e}")
                    await self.service_orchestrator.emergency_shutdown()
            
            # Clean up legacy validator
            logger.info("📋 Cleaning up legacy validator...")
            try:
                await asyncio.wait_for(self.legacy_validator._initiate_shutdown(), timeout=30)
                logger.info("✅ Legacy validator cleanup completed")
            except asyncio.TimeoutError:
                logger.warning("⚠️ Legacy validator cleanup timed out")
            except Exception as e:
                logger.error(f"❌ Error during legacy validator cleanup: {e}")
            
            # Create cleanup completion file for auto updater
            try:
                cleanup_file = "/tmp/modern_validator_cleanup_done"
                with open(cleanup_file, "w") as f:
                    f.write(f"Modern validator cleanup completed at {time.time()}\n")
                logger.info(f"📝 Created cleanup completion file: {cleanup_file}")
            except Exception as e:
                logger.error(f"❌ Failed to create cleanup completion file: {e}")
            
            self._cleanup_done = True
            logger.info("✅ Modern validator graceful shutdown completed")
            
        except Exception as e:
            logger.error(f"❌ Error during modern validator shutdown: {e}", exc_info=True)
            self._cleanup_done = True
    
    # Compatibility methods for existing code
    
    def __getattr__(self, name):
        """Delegate attribute access to legacy validator for compatibility."""
        if hasattr(self.legacy_validator, name):
            return getattr(self.legacy_validator, name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
    
    async def query_miners(self, payload: Dict, endpoint: str, hotkeys: Optional[list] = None) -> Dict:
        """Query miners using services if available, otherwise use legacy method."""
        if self.services_running:
            return await self.service_orchestrator.query_miners_via_service(payload, endpoint, hotkeys)
        else:
            return await self.legacy_validator.query_miners(payload, endpoint, hotkeys)
    
    async def _calc_task_weights(self):
        """Calculate task weights using services if available, otherwise use legacy method."""
        if self.services_running:
            return await self.service_orchestrator.calculate_weights_via_service()
        else:
            return await self.legacy_validator._calc_task_weights()
    
    async def cleanup_resources(self):
        """Cleanup resources using services if available, otherwise use legacy method."""
        if self.services_running:
            result = await self.service_orchestrator.cleanup_resources_via_service()
            logger.info(f"Service-based cleanup result: {result.get('status', 'unknown')}")
        else:
            await self.legacy_validator.cleanup_resources()
    
    def get_service_health(self) -> Optional[Dict[str, Any]]:
        """Get service health status if services are running."""
        if self.services_running:
            return asyncio.create_task(self.service_orchestrator.get_service_health())
        return None


async def main():
    """Main entry point for the modern validator."""
    parser = ArgumentParser()
    
    # Add all the same arguments as the original validator
    subtensor_group = parser.add_argument_group("subtensor")
    parser.add_argument("--wallet", type=str, help="Name of the wallet to use")
    parser.add_argument("--hotkey", type=str, help="Name of the hotkey to use")
    parser.add_argument("--netuid", type=int, help="Netuid to use")
    subtensor_group.add_argument(
        "--subtensor.chain_endpoint", type=str, help="Subtensor chain endpoint to use"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run tasks in test mode - runs immediately and with limited scope",
    )
    
    args = parser.parse_args()
    
    # Create and run modern validator
    modern_validator = ModernGaiaValidator(args)
    
    try:
        await modern_validator.run()
    except KeyboardInterrupt:
        logger.info("🛑 Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.critical(f"💥 Unhandled exception in modern validator: {e}", exc_info=True)
    finally:
        logger.info("🏁 Modern validator execution completed")


if __name__ == "__main__":
    # Run the modern validator
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Keyboard interrupt")
    except Exception as e:
        print(f"💥 Critical error: {e}")
        traceback.print_exc()
    finally:
        print("🏁 Modern validator finished") 