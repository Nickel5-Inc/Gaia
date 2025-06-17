"""
Database Setup module for the Gaia validator.

This module contains comprehensive database setup functionality
extracted from the original validator.py implementation.
"""

import logging
import os
import sys
from typing import Optional

logger = logging.getLogger(__name__)


async def run_comprehensive_database_setup():
    """
    Run comprehensive database setup procedures.
    
    This function sets up the database with all necessary configurations,
    migrations, and optimizations required for the validator.
    """
    try:
        logger.info("🚀 Starting comprehensive database setup and validation...")
        print("\n" + "🔧" * 80)
        print("🔧 COMPREHENSIVE DATABASE SETUP STARTING 🔧")
        print("🔧" * 80)
        
        # Import the comprehensive database setup
        from gaia.validator.database.comprehensive_db_setup import setup_comprehensive_database, DatabaseConfig
        
        # Create database configuration from environment variables
        db_config = DatabaseConfig(
            database_name=os.getenv("DB_NAME", "gaia_validator"),
            postgres_version=os.getenv("POSTGRES_VERSION", "14"),
            postgres_password=os.getenv("DB_PASSWORD", "postgres"),
            postgres_user=os.getenv("DB_USER", "postgres"),
            port=int(os.getenv("DB_PORT", "5432")),
            data_directory=os.getenv("POSTGRES_DATA_DIR", "/var/lib/postgresql/14/main"),
            config_directory=os.getenv("POSTGRES_CONFIG_DIR", "/etc/postgresql/14/main")
        )
        
        logger.info(f"Database configuration: {db_config.database_name} on port {db_config.port}")
        
        # Determine test mode - this would need to be passed from the caller
        test_mode = os.getenv("VALIDATOR_TEST_MODE", "false").lower() == "true"
        
        # Run comprehensive database setup
        setup_success = await setup_comprehensive_database(
            test_mode=test_mode,
            config=db_config
        )
        
        if not setup_success:
            logger.error("❌ Comprehensive database setup failed - validator cannot start safely")
            print("❌ DATABASE SETUP FAILED - EXITING ❌")
            sys.exit(1)
        
        logger.info("✅ Comprehensive database setup completed successfully")
        print("✅ DATABASE SETUP COMPLETED - STARTING VALIDATOR ✅")
        print("🔧" * 80 + "\n")
        
    except Exception as e:
        logger.error(f"❌ Critical error in comprehensive database setup: {e}", exc_info=True)
        print(f"❌ CRITICAL DATABASE ERROR: {e} ❌")
        sys.exit(1)