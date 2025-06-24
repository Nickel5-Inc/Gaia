#!/usr/bin/env python3
"""
Test script for verifying the service integration works correctly.
This script can be used to test the service architecture without running the full validator.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from gaia.validator.services.orchestrator import ServiceOrchestrator
from gaia.validator.services.messaging.bus import AsyncMessageBus
from gaia.validator.services.base.message import MessageTypes, ServiceMessage

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockValidator:
    """Mock validator for testing service integration without full validator setup."""
    
    def __init__(self):
        self.database_manager = None
        self.miner_client = None
        self.api_client = None
        
        # Mock business logic attributes
        self.metagraph = None
        self.substrate = None
        self.keypair = None
        self.netuid = 237
        self.validator_uid = 0
        
        # Mock task instances
        self.geomagnetic_task = MockTask("geomagnetic")
        self.soil_task = MockTask("soil")
        self.weather_task = MockTask("weather")
        
        # Mock methods that services might call
        self.nodes = {}
        self.weights = [0.0] * 256
    
    async def query_miners(self, payload, endpoint, hotkeys=None):
        """Mock miner query method."""
        logger.info(f"Mock query_miners called: endpoint={endpoint}, hotkeys={hotkeys}")
        return {"mock_response": True, "miners_queried": len(hotkeys) if hotkeys else 10}
    
    async def _calc_task_weights(self):
        """Mock weight calculation method."""
        logger.info("Mock _calc_task_weights called")
        return [0.1] * 256  # Mock weights
    
    async def cleanup_resources(self):
        """Mock cleanup method."""
        logger.info("Mock cleanup_resources called")
    
    def _log_memory_usage(self, context, threshold=100):
        """Mock memory logging method."""
        logger.info(f"Mock memory usage: {context}")


class MockTask:
    """Mock task for testing."""
    
    def __init__(self, name):
        self.name = name
    
    async def validator_execute(self, validator):
        """Mock task execution."""
        logger.info(f"Mock task {self.name} executing...")
        await asyncio.sleep(1)  # Simulate work
        logger.info(f"Mock task {self.name} completed")


async def test_message_bus():
    """Test the message bus functionality."""
    logger.info("🧪 Testing message bus...")
    
    bus = AsyncMessageBus()
    await bus.start()
    
    try:
        # Test message creation
        message = ServiceMessage.create(
            source="test_source",
            target="test_target", 
            msg_type=MessageTypes.HEALTH_CHECK,
            data={"test": "data"}
        )
        
        logger.info(f"✅ Created message: {message.id}")
        
        # Test message serialization
        json_str = message.to_json()
        restored_message = ServiceMessage.from_json(json_str)
        
        assert restored_message.id == message.id
        assert restored_message.message_type == message.message_type
        logger.info("✅ Message serialization/deserialization works")
        
    finally:
        await bus.stop()
    
    logger.info("✅ Message bus test completed")


async def test_service_orchestrator():
    """Test the service orchestrator with mock validator."""
    logger.info("🧪 Testing service orchestrator...")
    
    # Create mock validator
    mock_validator = MockValidator()
    
    # Create orchestrator
    orchestrator = ServiceOrchestrator(mock_validator)
    
    try:
        # Test initialization
        logger.info("Testing orchestrator initialization...")
        if await orchestrator.initialize():
            logger.info("✅ Orchestrator initialized successfully")
        else:
            logger.error("❌ Orchestrator initialization failed")
            return False
        
        # Test starting services
        logger.info("Testing orchestrator start...")
        if await orchestrator.start():
            logger.info("✅ Orchestrator started successfully")
        else:
            logger.error("❌ Orchestrator start failed")
            return False
        
        # Test service health
        logger.info("Testing service health check...")
        health_status = await orchestrator.get_service_health()
        logger.info(f"Service health: {len(health_status.get('services', {}))} services")
        
        for service_name, status in health_status.get('services', {}).items():
            logger.info(f"  {service_name}: {status.get('status', 'unknown')}")
        
        # Test service methods
        logger.info("Testing service integration methods...")
        
        # Test miner query via service
        miner_result = await orchestrator.query_miners_via_service(
            payload={"test": "data"},
            endpoint="/test",
            hotkeys=["test_hotkey_1", "test_hotkey_2"]
        )
        logger.info(f"✅ Miner query result: {miner_result}")
        
        # Test weight calculation via service
        weights = await orchestrator.calculate_weights_via_service()
        if weights:
            logger.info(f"✅ Weight calculation result: {len(weights)} weights")
        else:
            logger.warning("⚠️ Weight calculation returned None")
        
        # Test resource cleanup via service
        cleanup_result = await orchestrator.cleanup_resources_via_service()
        logger.info(f"✅ Resource cleanup result: {cleanup_result.get('status', 'unknown')}")
        
        # Test memory pressure check
        memory_status = await orchestrator.check_memory_pressure_via_service()
        logger.info(f"✅ Memory pressure check: {memory_status.get('status', 'unknown')}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during orchestrator test: {e}", exc_info=True)
        return False
        
    finally:
        # Cleanup
        logger.info("Stopping orchestrator...")
        await orchestrator.stop()
        logger.info("✅ Orchestrator stopped")


async def test_individual_services():
    """Test individual services."""
    logger.info("🧪 Testing individual services...")
    
    mock_validator = MockValidator()
    bus = AsyncMessageBus()
    await bus.start()
    
    try:
        # Test DatabaseService (will likely fail without real DB)
        from gaia.validator.services.database.service import DatabaseService
        db_service = DatabaseService(bus, None)  # No real DB manager
        
        # Test service lifecycle
        if await db_service.initialize():
            logger.info("✅ DatabaseService initialized")
            if await db_service.start():
                logger.info("✅ DatabaseService started")
                await db_service.stop()
                logger.info("✅ DatabaseService stopped")
        else:
            logger.warning("⚠️ DatabaseService initialization failed (expected without real DB)")
        
        # Test NetworkService
        from gaia.validator.services.network.service import NetworkService
        network_service = NetworkService(bus, mock_validator)
        
        if await network_service.initialize():
            logger.info("✅ NetworkService initialized")
            if await network_service.start():
                logger.info("✅ NetworkService started")
                await network_service.stop()
                logger.info("✅ NetworkService stopped")
        
        # Test ResourceService
        from gaia.validator.services.resources.service import ResourceService
        resource_service = ResourceService(bus, mock_validator)
        
        if await resource_service.initialize():
            logger.info("✅ ResourceService initialized")
            if await resource_service.start():
                logger.info("✅ ResourceService started")
                
                # Test resource operations
                memory_stats = resource_service.memory_manager.get_memory_usage()
                if memory_stats:
                    logger.info(f"✅ Memory stats retrieved: {memory_stats['process_memory_mb']:.1f}MB")
                
                await resource_service.stop()
                logger.info("✅ ResourceService stopped")
        
        logger.info("✅ Individual service tests completed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during individual service test: {e}", exc_info=True)
        return False
        
    finally:
        await bus.stop()


async def main():
    """Main test function."""
    logger.info("🚀 Starting service integration tests...")
    
    tests = [
        ("Message Bus", test_message_bus),
        ("Service Orchestrator", test_service_orchestrator), 
        ("Individual Services", test_individual_services),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"Running test: {test_name}")
        logger.info(f"{'='*50}")
        
        try:
            result = await test_func()
            results[test_name] = result
            
            if result:
                logger.info(f"✅ {test_name} test PASSED")
            else:
                logger.error(f"❌ {test_name} test FAILED")
                
        except Exception as e:
            logger.error(f"💥 {test_name} test CRASHED: {e}", exc_info=True)
            results[test_name] = False
    
    # Summary
    logger.info(f"\n{'='*50}")
    logger.info("TEST SUMMARY")
    logger.info(f"{'='*50}")
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Service integration is working correctly.")
        return True
    else:
        logger.error(f"🚨 {total - passed} tests failed. Service integration needs fixes.")
        return False


if __name__ == "__main__":
    # Set test environment
    os.environ['VALIDATOR_SERVICES_ENABLED'] = 'true'
    os.environ['VALIDATOR_MEMORY_MONITORING_ENABLED'] = 'true'
    
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("🛑 Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Critical test error: {e}", exc_info=True)
        sys.exit(1) 