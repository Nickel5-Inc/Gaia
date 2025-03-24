import os
import time
from pathlib import Path
import asyncio
from typing import List, Optional
from fiber.logging_utils import get_logger

logger = get_logger(__name__)
DEFAULT_TRIGGER_PATH = '/root/Gaia/db_wipe_trigger'

async def check_for_db_wipe_trigger(trigger_path: str = DEFAULT_TRIGGER_PATH) -> bool:
    """
    Check if the database wipe trigger file exists.
    
    Args:
        trigger_path: Path to the trigger file
        
    Returns:
        bool: True if the trigger file exists, False otherwise
    """
    try:
        if os.path.exists(trigger_path):
            logger.warning(f"Database wipe trigger file found at {trigger_path}")
            file_stats = os.stat(trigger_path)
            
            creation_time = file_stats.st_ctime
            current_time = time.time()
            file_age_hours = (current_time - creation_time) / 3600
            logger.warning(f"Trigger file age: {file_age_hours:.2f} hours")
            
            try:
                with open(trigger_path, 'r') as f:
                    content = f.read().strip()
                    if content:
                        logger.info(f"Trigger file contains: {content}")
                    return True
            except Exception as e:
                logger.error(f"Error reading trigger file: {e}")
                return True
        return False
    except Exception as e:
        logger.error(f"Error checking for db wipe trigger: {e}")
        return False

async def get_tables_to_wipe(trigger_path: str = DEFAULT_TRIGGER_PATH) -> Optional[List[str]]:
    """
    Get list of tables to wipe from the trigger file content.
    If the file exists but is empty or can't be read, return None (indicating all tables).
    
    Args:
        trigger_path: Path to the trigger file
        
    Returns:
        Optional[List[str]]: List of table names to wipe, or None for all tables
    """
    if not os.path.exists(trigger_path):
        return []
    
    try:
        with open(trigger_path, 'r') as f:
            content = f.read().strip()
            if not content:
                return None  # Empty file means wipe all tables
            
            # Parse content as comma-separated list of table names
            tables = [table.strip() for table in content.split(',')]
            return tables if tables else None
    except Exception as e:
        logger.error(f"Error reading tables from trigger file: {e}")
        return None  # If there's an error reading, default to all tables

async def wipe_database(db_manager, tables: Optional[List[str]] = None) -> bool:
    """
    Wipe the database tables.
    
    Args:
        db_manager: Database manager instance
        tables: Optional list of specific tables to wipe. If None, wipe all tables.
        
    Returns:
        bool: True if wipe was successful, False otherwise
    """
    try:
        if not tables:
            # Get all table names from the database
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            """
            result = await db_manager.fetch_all(query)
            tables = [row['table_name'] for row in result if row['table_name'] != 'node_table']
            
            logger.warning("\n" + "#" * 60)
            logger.warning("## WIPING ALL TABLES - FULL DATABASE RESET ##")
            logger.warning("#" * 60)
            
            # Preserve the node_table structure but clear data
            node_table_clear = """
            UPDATE node_table 
            SET hotkey = NULL, coldkey = NULL, ip = NULL, ip_type = NULL, 
                port = NULL, incentive = NULL, stake = NULL, trust = NULL, 
                vtrust = NULL, protocol = NULL
            """
            await db_manager.execute(node_table_clear)
            logger.warning("## CLEARED node_table data while preserving structure ##")
        else:
            logger.warning("\n" + "#" * 60)
            logger.warning(f"## WIPING SPECIFIC TABLES: {', '.join(tables)} ##")
            logger.warning("#" * 60)
        
        success_count = 0
        error_count = 0
        
        for table in tables:
            if table.lower() == 'node_table':
                # Skip node_table as it's handled separately
                continue
                
            try:
                logger.warning(f"## WIPING TABLE: {table}")
                # Truncate table to remove all data
                await db_manager.execute(f"TRUNCATE TABLE {table} CASCADE")
                logger.warning(f"## SUCCESSFULLY WIPED table: {table}")
                success_count += 1
            except Exception as table_error:
                logger.error(f"## ERROR WIPING table {table}: {table_error}")
                error_count += 1
        
        logger.warning("\n" + "#" * 60)
        logger.warning(f"## DATABASE WIPE SUMMARY: {success_count} tables wiped, {error_count} errors ##")
        logger.warning("#" * 60)
        
        return success_count > 0 or error_count == 0  # Success if we wiped at least one table or had no errors
        
    except Exception as e:
        logger.error(f"Error wiping database: {e}")
        return False

async def handle_db_wipe(db_manager, trigger_path: str = DEFAULT_TRIGGER_PATH) -> bool:
    """
    Check for trigger file and wipe database if found.
    
    Args:
        db_manager: Database manager instance
        trigger_path: Path to the trigger file
        
    Returns:
        bool: True if wipe was performed, False otherwise
    """
    try:
        trigger_exists = await check_for_db_wipe_trigger(trigger_path)
        if not trigger_exists:
            return False
            
        logger.warning("\n" + "#" * 80)
        logger.warning("#" * 30 + " DATABASE WIPE INITIATED " + "#" * 30)
        logger.warning("#" * 80)
        
        # Get specific tables to wipe (if specified in trigger file)
        tables_to_wipe = await get_tables_to_wipe(trigger_path)
        
        # Perform the wipe
        success = await wipe_database(db_manager, tables_to_wipe)
        
        if success:
            logger.warning("\n" + "#" * 80)
            logger.warning("#" * 30 + " DATABASE WIPE COMPLETED " + "#" * 30)
            logger.warning("#" * 80)
            
            # Create backup of trigger file before removal (for audit purposes)
            backup_path = f"{trigger_path}.executed.{int(time.time())}"
            try:
                import shutil
                shutil.copy2(trigger_path, backup_path)
                logger.info(f"Created backup of trigger file at {backup_path}")
            except Exception as backup_error:
                logger.error(f"Failed to create backup of trigger file: {backup_error}")
            
            # Remove trigger file to prevent repeated wipes
            try:
                os.remove(trigger_path)
                logger.info(f"Removed trigger file {trigger_path}")
            except Exception as rm_error:
                logger.error(f"Failed to remove trigger file: {rm_error}")
                
            return True
        else:
            logger.error("\n" + "#" * 80)
            logger.error("#" * 30 + " DATABASE WIPE FAILED " + "#" * 30)
            logger.error("#" * 80)
            return False
            
    except Exception as e:
        logger.error(f"Error in handle_db_wipe: {e}")
        return False 