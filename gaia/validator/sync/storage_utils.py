import os
from typing import Optional, Union
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

# Import storage managers
try:
    from gaia.validator.sync.r2_storage_utils import R2StorageManager, get_r2_storage_manager_for_db_sync
    R2_AVAILABLE = True
except ImportError as e:
    logger.warning(f"R2 storage not available: {e}")
    R2StorageManager = None
    get_r2_storage_manager_for_db_sync = None
    R2_AVAILABLE = False

# Type alias for storage managers
StorageManager = Optional[R2StorageManager]


async def get_storage_manager_for_db_sync() -> Optional[StorageManager]:
    """
    Factory function to create the appropriate storage manager based on environment configuration.
    Currently, this only supports R2.
    """
    storage_backend = os.getenv("STORAGE_BACKEND", "r2").lower()

    if storage_backend == "r2":
        if R2_AVAILABLE and get_r2_storage_manager_for_db_sync:
            logger.info("Using R2 storage backend for DB sync.")
            return await get_r2_storage_manager_for_db_sync()
        else:
            logger.error("STORAGE_BACKEND is set to 'r2', but R2 dependencies are not installed or available.")
            return None
    
    logger.error(f"Unsupported STORAGE_BACKEND: '{storage_backend}'. Only 'r2' is supported.")
    return None


def get_storage_backend_name(manager: StorageManager) -> str:
    """Get the name of the storage backend from the manager instance."""
    if isinstance(manager, R2StorageManager):
        return "R2"
    elif manager is None: # Should not happen if manager is correctly obtained
        return "None" 
    else:
        return "Unknown"


async def test_storage_configuration():
    """
    Tests the configured storage backend by attempting to initialize the manager
    and list items in the root of the bucket/container.
    """
    logger.info("--- Testing Storage Configuration ---")
    
    storage_backend = os.getenv("STORAGE_BACKEND", "r2").lower()
    
    print(f"Detected Storage Backend: {storage_backend.upper()}")
    
    # R2 Check
    print("\n--- R2 Details ---")
    if R2_AVAILABLE:
        print("  - R2 Library: ✅ Found")
        r2_endpoint = os.getenv('R2_ENDPOINT_URL')
        r2_bucket = os.getenv('R2_BUCKET_NAME_DB_SYNC')
        if r2_endpoint and r2_bucket:
            print(f"  - Endpoint: {r2_endpoint}")
            print(f"  - Bucket:   {r2_bucket}")
            print("  - Status:   Configured")
        else:
            print("  - Status:   ⚠️ Not Configured (missing R2_ENDPOINT_URL or R2_BUCKET_NAME_DB_SYNC)")
    else:
        print("  - R2 Library: ❌ Not Found")

    print("\n--- Live Connection Test ---")
    storage_manager = await get_storage_manager_for_db_sync()

    if not storage_manager:
        logger.error("Could not initialize storage manager. Test failed.")
        return

    try:
        # A lightweight operation to test credentials and connectivity.
        logger.info(f"Attempting to list items in bucket '{storage_manager.bucket_name}' to verify connection...")
        async for _ in storage_manager.list_items("", max_items=1):
            pass  # We just need the call to succeed
        logger.info("✅ Storage connection test successful.")
        print("Live Connection: ✅ Successful")
    except Exception as e:
        logger.error(f"Storage connection test failed: {e}", exc_info=True)
        print(f"Live Connection: ❌ Failed. Check logs for details.")


# Export the main interface
__all__ = [
    'StorageManager',
    'get_storage_manager_for_db_sync',
    'get_storage_backend_name',
    'test_storage_configuration',
    'R2StorageManager',
    'R2_AVAILABLE'
]


# Test function for when module is run directly
async def _test_main():
    """Test storage configuration when run as main module."""
    print("🧪 Testing Gaia Validator Storage Configuration\n")
    
    # Test availability
    print("📦 Available Storage Backends:")
    print(f"  R2:    {'✅ Available' if R2_AVAILABLE else '❌ Not Available'}")
    print("  Azure: ❌ Not Available (Support Removed)")
    print()
    
    # Test configuration
    print("🔧 Testing Storage Configuration...")
    result = await test_storage_configuration()
    
    print("📊 Test Results:")
    for key, value in result.items():
        if key == 'test_successful':
            status = "✅ SUCCESS" if value else "❌ FAILED"
            print(f"  {key}: {status}")
        elif key == 'error' and value:
            print(f"  {key}: ❌ {value}")
        elif key != 'error': # Print other non-error values
            print(f"  {key}: {value}")
    
    print()
    
    if result.get('test_successful'): # Use .get for safety
        print("🎉 Storage configuration is working correctly!")
        print(f"   Using: {result.get('backend_type', 'N/A')} storage")
    else:
        print("⚠️  Storage configuration needs attention.")
        print("\n💡 Setup Guide (R2 Only):")
        print("   For R2:")
        print("     export R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com")
        print("     export R2_ACCESS_KEY_ID=your-access-key")
        print("     export R2_SECRET_ACCESS_KEY=your-secret-key")
        print("     export R2_BUCKET_NAME_DB_SYNC=your-r2-bucket-name")
        print()
        print("   Backend Selection:")
        print("     export STORAGE_BACKEND=r2  (or 'auto', which defaults to R2)")


if __name__ == "__main__":
    import asyncio
    asyncio.run(_test_main()) 