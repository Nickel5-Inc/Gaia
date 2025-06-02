import os
from typing import Optional, Union
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

# Import storage managers
try:
    from gaia.validator.sync.azure_blob_utils import AzureBlobManager, get_azure_blob_manager_for_db_sync
    AZURE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Azure storage not available: {e}")
    AzureBlobManager = None
    get_azure_blob_manager_for_db_sync = None
    AZURE_AVAILABLE = False

try:
    from gaia.validator.sync.r2_storage_utils import R2StorageManager, get_r2_storage_manager_for_db_sync
    R2_AVAILABLE = True
except ImportError as e:
    logger.warning(f"R2 storage not available: {e}")
    R2StorageManager = None
    get_r2_storage_manager_for_db_sync = None
    R2_AVAILABLE = False

# Type alias for storage managers
StorageManager = Union[AzureBlobManager, R2StorageManager] if AZURE_AVAILABLE and R2_AVAILABLE else (
    AzureBlobManager if AZURE_AVAILABLE else R2StorageManager if R2_AVAILABLE else None
)


async def get_storage_manager_for_db_sync() -> Optional[StorageManager]:
    """
    Factory function to create the appropriate storage manager for database sync.
    
    Checks environment variables to determine which storage backend to use:
    - STORAGE_BACKEND: "azure" or "r2" (default: "auto")
    - If "auto", tries R2 first, then Azure
    
    Returns:
        Configured storage manager or None if no valid configuration found
    """
    storage_backend = os.getenv("STORAGE_BACKEND", "auto").lower()
    
    # If specific backend is requested
    if storage_backend == "r2":
        if not R2_AVAILABLE:
            logger.error("R2 storage backend requested but not available")
            return None
        logger.info("Using R2 storage backend for database sync")
        return await get_r2_storage_manager_for_db_sync()
    
    elif storage_backend == "azure":
        if not AZURE_AVAILABLE:
            logger.error("Azure storage backend requested but not available")
            return None
        logger.info("Using Azure storage backend for database sync")
        return await get_azure_blob_manager_for_db_sync()
    
    # Auto-detection mode (default)
    elif storage_backend == "auto":
        logger.info("Auto-detecting storage backend for database sync...")
        
        # Try R2 first (recommended for new deployments)
        if R2_AVAILABLE:
            r2_manager = await get_r2_storage_manager_for_db_sync()
            if r2_manager:
                logger.info("Successfully configured R2 storage backend")
                return r2_manager
            else:
                logger.info("R2 configuration not available, trying Azure...")
        
        # Fall back to Azure
        if AZURE_AVAILABLE:
            azure_manager = await get_azure_blob_manager_for_db_sync()
            if azure_manager:
                logger.info("Successfully configured Azure storage backend")
                return azure_manager
            else:
                logger.info("Azure configuration not available")
        
        logger.error("No storage backend could be configured")
        return None
    
    else:
        logger.error(f"Unknown storage backend: {storage_backend}. Use 'azure', 'r2', or 'auto'")
        return None


def get_storage_backend_name(manager: StorageManager) -> str:
    """Get the name of the storage backend from the manager instance."""
    if isinstance(manager, R2StorageManager):
        return "R2"
    elif isinstance(manager, AzureBlobManager):
        return "Azure"
    else:
        return "Unknown"


async def test_storage_configuration() -> dict:
    """
    Test storage configuration and return status information.
    
    Returns:
        Dictionary with test results and configuration status
    """
    result = {
        "azure_available": AZURE_AVAILABLE,
        "r2_available": R2_AVAILABLE,
        "configured_backend": None,
        "backend_type": None,
        "test_successful": False,
        "error": None
    }
    
    try:
        manager = await get_storage_manager_for_db_sync()
        if manager:
            result["configured_backend"] = True
            result["backend_type"] = get_storage_backend_name(manager)
            
            # Test basic functionality
            test_content = f"Storage test at {os.urandom(8).hex()}"
            test_key = f"test/storage_test_{os.urandom(4).hex()}.txt"
            
            # Upload test
            upload_success = await manager.upload_blob_content(test_content, test_key)
            if upload_success:
                # Download test
                retrieved_content = await manager.read_blob_content(test_key)
                if retrieved_content == test_content:
                    result["test_successful"] = True
                    
                # Cleanup
                await manager.delete_blob(test_key)
            
            await manager.close()
        else:
            result["configured_backend"] = False
            result["error"] = "No storage backend could be configured"
            
    except Exception as e:
        result["error"] = str(e)
        result["test_successful"] = False
    
    return result


# Backward compatibility aliases
# These allow existing code to continue working without changes
async def get_azure_blob_manager_for_db_sync_compat():
    """Backward compatibility function that tries to get any available storage manager."""
    return await get_storage_manager_for_db_sync()


# Export the main interface
__all__ = [
    'StorageManager',
    'get_storage_manager_for_db_sync',
    'get_storage_backend_name',
    'test_storage_configuration',
    'AzureBlobManager',
    'R2StorageManager',
    'AZURE_AVAILABLE',
    'R2_AVAILABLE'
]


# Test function for when module is run directly
async def _test_main():
    """Test storage configuration when run as main module."""
    print("🧪 Testing Gaia Validator Storage Configuration\n")
    
    # Test availability
    print("📦 Available Storage Backends:")
    print(f"  Azure: {'✅ Available' if AZURE_AVAILABLE else '❌ Not Available'}")
    print(f"  R2:    {'✅ Available' if R2_AVAILABLE else '❌ Not Available'}")
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
        elif key != 'error':
            print(f"  {key}: {value}")
    
    print()
    
    if result['test_successful']:
        print("🎉 Storage configuration is working correctly!")
        print(f"   Using: {result['backend_type']} storage")
    else:
        print("⚠️  Storage configuration needs attention.")
        print("\n💡 Setup Guide:")
        print("   For R2:")
        print("     export R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com")
        print("     export R2_ACCESS_KEY_ID=your-access-key")
        print("     export R2_SECRET_ACCESS_KEY=your-secret-key")
        print()
        print("   For Azure:")
        print("     export AZURE_STORAGE_CONNECTION_STRING=your-connection-string")
        print("     # OR")
        print("     export AZURE_STORAGE_ACCOUNT_URL=your-account-url")
        print("     export AZURE_STORAGE_SAS_TOKEN=your-sas-token")
        print()
        print("   Backend Selection:")
        print("     export STORAGE_BACKEND=r2|azure|auto")


if __name__ == "__main__":
    import asyncio
    asyncio.run(_test_main()) 