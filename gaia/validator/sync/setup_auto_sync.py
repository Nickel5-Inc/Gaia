#!/usr/bin/env python3
"""
One-Command Database Sync Setup

This script provides a single command to set up database synchronization
using the AutoSyncManager, eliminating the need for manual pgbackrest configuration.

Usage:
    # Primary node setup
    python setup_auto_sync.py --primary
    
    # Replica node setup  
    python setup_auto_sync.py --replica
    
    # Test mode (faster, smaller retention)
    python setup_auto_sync.py --primary --test
"""

import asyncio
import argparse
import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parents[3]
sys.path.insert(0, str(project_root))

from gaia.validator.sync.auto_sync_manager import get_auto_sync_manager
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

def load_env_file():
    """Load environment variables from .env file."""
    env_paths = [
        Path(".env"),
        Path("/root/Gaia/.env"),
        project_root / ".env"
    ]
    
    for env_path in env_paths:
        if env_path.exists():
            logger.info(f"Loading environment from: {env_path}")
            with open(env_path) as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        key, _, value = line.partition('=')
                        os.environ[key.strip()] = value.strip()
            return True
    
    logger.error("No .env file found. Please create one with PGBACKREST_* variables.")
    return False

def validate_environment(is_primary: bool):
    """Validate required environment variables."""
    required_vars = [
        'PGBACKREST_R2_BUCKET',
        'PGBACKREST_R2_ENDPOINT', 
        'PGBACKREST_R2_ACCESS_KEY_ID',
        'PGBACKREST_R2_SECRET_ACCESS_KEY'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error("❌ Missing required environment variables:")
        for var in missing_vars:
            logger.error(f"  - {var}")
        logger.error("\nPlease configure these in your .env file:")
        logger.error("Example:")
        logger.error("PGBACKREST_R2_BUCKET=my-backup-bucket")
        logger.error("PGBACKREST_R2_ENDPOINT=https://account-id.r2.cloudflarestorage.com")
        logger.error("PGBACKREST_R2_ACCESS_KEY_ID=your-access-key")
        logger.error("PGBACKREST_R2_SECRET_ACCESS_KEY=your-secret-key")
        return False
    
    # Set the primary/replica flag
    os.environ['IS_SOURCE_VALIDATOR_FOR_DB_SYNC'] = 'True' if is_primary else 'False'
    
    return True

async def setup_database_sync(is_primary: bool, test_mode: bool):
    """Set up database synchronization."""
    try:
        logger.info("🚀 Starting automated database sync setup...")
        logger.info(f"Mode: {'Primary' if is_primary else 'Replica'}")
        logger.info(f"Test mode: {test_mode}")
        
        # Check if running as root
        if os.geteuid() != 0:
            logger.error("❌ This script must be run as root (use sudo)")
            return False
        
        # Load and validate environment
        if not load_env_file():
            return False
        
        if not validate_environment(is_primary):
            return False
        
        # Create and setup AutoSyncManager
        manager = await get_auto_sync_manager(test_mode=test_mode)
        if not manager:
            logger.error("❌ Failed to create AutoSyncManager")
            return False
        
        # Run the automated setup
        success = await manager.setup()
        
        if success:
            logger.info("✅ Database sync setup completed successfully!")
            
            if is_primary:
                logger.info("\n📋 Next steps for PRIMARY node:")
                logger.info("1. The backup system is now running automatically")
                logger.info("2. Backups are scheduled in the application (no cron needed)")
                logger.info("3. Check backup status with: python -c 'from gaia.validator.sync.auto_sync_manager import *; import asyncio; asyncio.run(check_status())'")
            else:
                logger.info("\n📋 Next steps for REPLICA node:")
                logger.info("1. To restore from backup, run:")
                logger.info("   python -c 'from gaia.validator.sync.auto_sync_manager import *; import asyncio; asyncio.run(restore_latest())'")
                logger.info("2. The system is ready to receive data from the primary")
            
            # Keep manager running if this is being used for immediate operation
            if test_mode:
                logger.info("Test mode: Keeping manager running for 60 seconds...")
                await asyncio.sleep(60)
                await manager.shutdown()
            
            return True
        else:
            logger.error("❌ Database sync setup failed")
            return False
        
    except KeyboardInterrupt:
        logger.info("Setup interrupted by user")
        return False
    except Exception as e:
        logger.error(f"❌ Setup failed with error: {e}", exc_info=True)
        return False

async def check_status():
    """Quick status check utility."""
    manager = await get_auto_sync_manager()
    if manager:
        status = await manager.get_backup_status()
        logger.info(f"Backup system status: {'✅ Healthy' if status['healthy'] else '❌ Unhealthy'}")
        if status.get('info'):
            logger.info(f"Details: {status['info']}")
        if status.get('error'):
            logger.error(f"Error: {status['error']}")

async def restore_latest():
    """Restore from latest backup."""
    if not load_env_file():
        return
    
    manager = await get_auto_sync_manager()
    if manager:
        logger.info("🔄 Starting restore from latest backup...")
        success = await manager.restore_from_backup()
        if success:
            logger.info("✅ Restore completed successfully!")
        else:
            logger.error("❌ Restore failed")

def main():
    parser = argparse.ArgumentParser(description='Automated Database Sync Setup')
    
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--primary', action='store_true', 
                           help='Set up as primary node (source for backups)')
    mode_group.add_argument('--replica', action='store_true',
                           help='Set up as replica node (restore target)')
    
    parser.add_argument('--test', action='store_true',
                       help='Enable test mode (faster, smaller retention)')
    
    args = parser.parse_args()
    
    # Run the setup
    success = asyncio.run(setup_database_sync(args.primary, args.test))
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main() 