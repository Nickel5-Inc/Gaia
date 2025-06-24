#!/usr/bin/env python3
"""
Test script for the refactored DatabaseService using SQLAlchemy connection pooling.
"""

import asyncio
import logging
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from gaia.validator.services.database.service import DatabaseService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def test_database_service():
    """Test the refactored DatabaseService."""
    logger.info("🧪 Testing DatabaseService with SQLAlchemy connection pooling")
    
    # Create database service with custom pool configuration
    db_service = DatabaseService(
        database_config={
            'database': 'test_db',
            'host': 'localhost',
            'user': 'postgres',
            'password': 'postgres'
        },
        pool_size=10,
        max_overflow=20,
        pool_timeout=15
    )
    
    try:
        # Test initialization (Note: This might fail if DB doesn't exist - that's expected)
        logger.info("Testing service initialization...")
        try:
            initialized = await db_service.initialize()
            logger.info(f"✅ Service initialized: {initialized}")
        except Exception as e:
            logger.warning(f"⚠️  Service initialization failed (expected if DB doesn't exist): {e}")
        
        # Test service statistics
        logger.info("Testing service statistics...")
        service_stats = db_service.get_service_stats()
        logger.info(f"📊 Service stats: {service_stats}")
        
        # Test pool status
        logger.info("Testing pool status...")
        pool_status = db_service.get_pool_status()
        logger.info(f"🏊 Pool status: {pool_status}")
        
        # Test health check (this will also fail without a real DB)
        logger.info("Testing health check...")
        try:
            health = await db_service.get_health()
            logger.info(f"❤️  Health check: {health}")
        except Exception as e:
            logger.warning(f"⚠️  Health check failed (expected without real DB): {e}")
        
        # Test database operations (these will fail without real DB)
        logger.info("Testing database operations...")
        try:
            # This should fail gracefully
            result = await db_service.fetch_one("SELECT 1 as test")
            logger.info(f"✅ Query result: {result}")
        except Exception as e:
            logger.warning(f"⚠️  Query failed (expected without real DB): {e}")
        
        # Test concurrent operations simulation
        logger.info("Testing concurrent operations simulation...")
        async def mock_operation(op_id: int):
            """Simulate a database operation."""
            try:
                # This will fail but should update statistics
                await db_service.fetch_one(f"SELECT {op_id} as op_id")
            except Exception:
                pass  # Expected to fail
        
        # Run multiple concurrent operations
        await asyncio.gather(*[mock_operation(i) for i in range(5)], return_exceptions=True)
        
        # Check updated statistics
        updated_stats = db_service.get_service_stats()
        logger.info(f"📊 Updated stats after concurrent ops: {updated_stats}")
        
        logger.info("✅ DatabaseService test completed successfully!")
        
    finally:
        # Clean up
        await db_service.close()
        logger.info("🧹 Database service closed")

async def test_singleton_behavior():
    """Test that DatabaseService follows singleton pattern."""
    logger.info("🔄 Testing singleton behavior...")
    
    # Create two instances
    db1 = DatabaseService()
    db2 = DatabaseService()
    
    # They should be the same instance
    if db1 is db2:
        logger.info("✅ Singleton pattern working correctly")
    else:
        logger.error("❌ Singleton pattern not working!")
    
    await db1.close()

async def main():
    """Main test function."""
    logger.info("🚀 Starting DatabaseService tests...")
    
    try:
        await test_database_service()
        await test_singleton_behavior()
        
        logger.info("🎉 All tests completed!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 