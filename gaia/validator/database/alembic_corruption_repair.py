"""
Alembic Corruption Repair Script

This script detects and fixes corrupted alembic_version tables that contain
invalid migration IDs like '67ea64fffc7f'. It must run BEFORE Alembic attempts
to run migrations.

This addresses the issue where nodes have phantom migration IDs that prevent
Alembic from functioning at all.
"""

import os
import asyncio
from pathlib import Path
from typing import Optional
from sqlalchemy import create_engine, text
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class AlembicCorruptionRepair:
    """Handles repair of corrupted alembic_version tables."""
    
    def __init__(self, db_url: Optional[str] = None):
        """
        Initialize the repair tool.
        
        Args:
            db_url: Database URL. If None, will try to construct from environment.
        """
        if db_url:
            self.db_url = db_url
        else:
            # Try to construct from environment variables
            self.db_url = self._construct_db_url_from_env()
        
        self.valid_migrations = [
            'f75e4f7343a1',  # initial_validator_schema_separate_config
            '2e00df6800b9',  # comprehensive_validator_schema_convergence  
            '3704fd24c76d',  # add_indexes_for_performance
            '9184456655c8',  # add_retry_columns_to_predictions_tables
            'a1b2c3d4e5f6',  # add_unique_constraint_soil_moisture_history
        ]
    
    def _construct_db_url_from_env(self) -> str:
        """Construct database URL from environment variables."""
        # Try socket connection first (preferred)
        socket_url = "postgresql+psycopg2://postgres:postgres@/validator_db?host=/var/run/postgresql"
        
        # Test if we can use socket connection
        try:
            engine = create_engine(socket_url)
            with engine.connect():
                pass
            logger.info("Using PostgreSQL socket connection")
            return socket_url
        except Exception:
            pass
        
        # Fall back to TCP connection
        db_password = os.getenv("DB_PASSWORD", "postgres")
        tcp_url = f"postgresql+psycopg2://postgres:{db_password}@localhost:5432/validator_db"
        logger.info("Using PostgreSQL TCP connection") 
        return tcp_url
    
    def check_and_repair_corruption(self) -> bool:
        """
        Check for and repair alembic_version corruption.
        
        Returns:
            True if repair was successful or no corruption found.
            False if repair failed.
        """
        try:
            logger.info("🔍 Checking for alembic_version corruption...")
            
            engine = create_engine(self.db_url)
            
            with engine.connect() as conn:
                # Check if alembic_version table exists
                table_check = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'alembic_version'
                    )
                """))
                
                if not table_check.scalar():
                    logger.info("✅ No alembic_version table found - no corruption possible")
                    return True
                
                # Get current version
                result = conn.execute(text("SELECT version_num FROM alembic_version;"))
                current_version = result.scalar()
                
                if not current_version:
                    logger.warning("⚠️ Empty alembic_version table - this is unusual but not corruption")
                    return True
                
                logger.info(f"📋 Current alembic version: {current_version}")
                
                # Check if version is valid
                if current_version in self.valid_migrations:
                    logger.info("✅ Alembic version is valid - no corruption detected")
                    return True
                
                # Corruption detected!
                logger.error(f"❌ CORRUPTION DETECTED: Invalid migration ID '{current_version}'")
                logger.info("🔧 Attempting automatic repair...")
                
                # Determine correct version based on database state
                target_version = self._determine_correct_version(conn)
                
                if not target_version:
                    logger.error("❌ Could not determine correct migration version")
                    return False
                
                # Apply the fix
                logger.info(f"🔧 Updating alembic_version from '{current_version}' to '{target_version}'")
                conn.execute(
                    text("UPDATE alembic_version SET version_num = :version"),
                    {"version": target_version}
                )
                conn.commit()
                
                # Verify the fix
                verify_result = conn.execute(text("SELECT version_num FROM alembic_version;"))
                new_version = verify_result.scalar()
                
                if new_version == target_version:
                    logger.info(f"✅ Corruption repaired successfully! Version is now: {new_version}")
                    return True
                else:
                    logger.error(f"❌ Repair verification failed. Expected {target_version}, got {new_version}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Error during corruption check/repair: {e}")
            logger.error("💡 You may need to manually fix the alembic_version table")
            return False
    
    def _determine_correct_version(self, conn) -> Optional[str]:
        """
        Determine the correct migration version based on database schema state.
        
        Args:
            conn: Database connection
            
        Returns:
            The correct migration ID, or None if it cannot be determined
        """
        try:
            # Check if the soil_moisture_history unique constraint exists
            constraint_check = conn.execute(text("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.table_constraints 
                    WHERE table_name = 'soil_moisture_history' 
                    AND constraint_name = 'uq_smh_region_miner_target_time'
                    AND constraint_type = 'UNIQUE'
                )
            """))
            constraint_exists = constraint_check.scalar()
            
            if constraint_exists:
                logger.info("✅ Unique constraint detected - setting version to a1b2c3d4e5f6")
                return 'a1b2c3d4e5f6'
            
            # Check if soil_moisture_history table exists at all
            table_check = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'soil_moisture_history'
                )
            """))
            
            if table_check.scalar():
                logger.info("✅ soil_moisture_history table exists but no constraint - setting version to 9184456655c8")
                # Need to add the constraint since the code expects it
                self._add_missing_constraint(conn)
                return 'a1b2c3d4e5f6'  # After adding constraint
            else:
                logger.info("⚠️ soil_moisture_history table missing - using initial migration")
                return 'f75e4f7343a1'
                
        except Exception as e:
            logger.error(f"Error determining correct version: {e}")
            # Default to a safe version
            return '9184456655c8'
    
    def _add_missing_constraint(self, conn) -> bool:
        """Add the missing unique constraint if needed."""
        try:
            logger.info("🔧 Adding missing unique constraint to soil_moisture_history...")
            
            # Check for and remove duplicates first
            duplicate_check = conn.execute(text("""
                SELECT COUNT(*) FROM (
                    SELECT region_id, miner_uid, target_time, COUNT(*) as count
                    FROM soil_moisture_history
                    GROUP BY region_id, miner_uid, target_time
                    HAVING COUNT(*) > 1
                ) duplicates
            """))
            duplicate_count = duplicate_check.scalar()
            
            if duplicate_count > 0:
                logger.info(f"🧹 Removing {duplicate_count} sets of duplicate records...")
                conn.execute(text("""
                    DELETE FROM soil_moisture_history
                    WHERE id NOT IN (
                        SELECT MAX(id)
                        FROM soil_moisture_history
                        GROUP BY region_id, miner_uid, target_time
                    )
                """))
                conn.commit()
            
            # Add the unique constraint
            conn.execute(text("""
                ALTER TABLE soil_moisture_history 
                ADD CONSTRAINT uq_smh_region_miner_target_time 
                UNIQUE (region_id, miner_uid, target_time)
            """))
            conn.commit()
            logger.info("✅ Unique constraint added successfully")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Could not add constraint (may already exist): {e}")
            return False


def repair_alembic_corruption(db_url: Optional[str] = None) -> bool:
    """
    Main entry point for repairing alembic corruption.
    
    Args:
        db_url: Optional database URL
        
    Returns:
        True if successful, False if failed
    """
    repairer = AlembicCorruptionRepair(db_url)
    return repairer.check_and_repair_corruption()


if __name__ == "__main__":
    import sys
    success = repair_alembic_corruption()
    sys.exit(0 if success else 1) 