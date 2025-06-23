"""
Database System Initialization
==============================

Clean functions for setting up the database system, handling migrations,
and ensuring the validator database is ready for operation.
"""

import os
import sys
from typing import Dict, Any
from fiber.logging_utils import get_logger

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.validator.utils.db_wipe import handle_db_wipe

logger = get_logger(__name__)


async def initialize_database_system(config: Dict[str, Any], test_mode: bool = False) -> ValidatorDatabaseManager:
    """
    Initialize the complete database system.
    
    Args:
        config: Database configuration dictionary
        test_mode: Whether running in test mode
        
    Returns:
        ValidatorDatabaseManager: Initialized database manager
    """
    logger.info("🚀 Starting comprehensive database initialization...")
    
    # Step 1: Run comprehensive database setup
    await _run_comprehensive_setup(config, test_mode)
    
    # Step 2: Initialize database manager
    database_manager = ValidatorDatabaseManager()
    await database_manager.initialize_database()
    logger.info("✅ Database manager initialized")
    
    # Step 3: Handle database wipe if triggered
    logger.info("🔍 Checking for database wipe trigger...")
    await handle_db_wipe(database_manager)
    
    logger.info("✅ Database system initialization completed")
    return database_manager


async def _run_comprehensive_setup(config: Dict[str, Any], test_mode: bool) -> None:
    """Run the comprehensive database setup process."""
    try:
        from gaia.validator.database.comprehensive_db_setup import (
            setup_comprehensive_database, 
            DatabaseConfig
        )
        
        # Create database configuration
        db_config = DatabaseConfig(
            database_name=config.get("db_name", "gaia_validator"),
            postgres_version=os.getenv("POSTGRES_VERSION", "14"),
            postgres_password=config.get("db_password", "postgres"),
            postgres_user=config.get("db_user", "postgres"),
            port=config.get("db_port", 5432),
            data_directory=os.getenv("POSTGRES_DATA_DIR", "/var/lib/postgresql/14/main"),
            config_directory=os.getenv("POSTGRES_CONFIG_DIR", "/etc/postgresql/14/main")
        )
        
        logger.info(f"Database config: {db_config.database_name} on port {db_config.port}")
        
        # Run comprehensive setup
        setup_success = await setup_comprehensive_database(
            test_mode=test_mode,
            config=db_config
        )
        
        if not setup_success:
            logger.error("❌ Comprehensive database setup failed")
            raise RuntimeError("Database setup failed - validator cannot start")
        
        logger.info("✅ Comprehensive database setup completed")
        
    except Exception as e:
        logger.error(f"❌ Critical database setup error: {e}", exc_info=True)
        raise


async def ensure_requirements_updated() -> None:
    """Ensure Python requirements are up to date."""
    try:
        import subprocess
        
        logger.info("📦 Ensuring Python requirements are up to date...")
        
        # Get requirements.txt path
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_script_dir, "..", "..", "..", ".."))
        requirements_path = os.path.join(project_root, "requirements.txt")
        
        if not os.path.exists(requirements_path):
            logger.warning(f"requirements.txt not found at {requirements_path}")
            return
        
        # Run pip install with timeout
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", requirements_path],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            cwd=project_root
        )
        
        if result.returncode == 0:
            logger.info("✅ Python requirements updated successfully")
        else:
            logger.warning(f"⚠️ Pip install returned exit code {result.returncode}")
            if result.stderr:
                logger.warning(f"Pip error: {result.stderr}")
                
    except subprocess.TimeoutExpired:
        logger.warning("⚠️ Pip install timed out, continuing...")
    except Exception as e:
        logger.warning(f"⚠️ Error updating requirements: {e}")


def clear_python_cache() -> None:
    """Clear Python bytecode cache files to prevent issues."""
    try:
        import subprocess
        import importlib
        
        # Get repository root
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        
        logger.info(f"🧹 Clearing Python cache files in {repo_root}...")
        
        # Clear .pyc files
        subprocess.run(f"find {repo_root} -name '*.pyc' -delete", shell=True, capture_output=True)
        
        # Clear __pycache__ directories  
        subprocess.run(f"find {repo_root} -name '__pycache__' -type d -exec rm -rf {{}} + 2>/dev/null || true", shell=True, capture_output=True)
        
        # Clear Python import cache
        if hasattr(importlib, 'invalidate_caches'):
            importlib.invalidate_caches()
        
        logger.info("✅ Python cache cleanup completed")
        
    except Exception as e:
        logger.warning(f"⚠️ Cache cleanup failed: {e}")
        # Don't fail startup if cache clearing fails 