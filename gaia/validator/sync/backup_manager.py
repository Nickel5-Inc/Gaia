# gaia/validator/sync/backup_manager.py
import asyncio
import os
import datetime
import random
import time
import subprocess # For running pg_dump
from fiber.logging_utils import get_logger
from gaia.validator.sync.storage_utils import StorageManager, get_storage_manager_for_db_sync, get_storage_backend_name
# Assuming ValidatorDatabaseManager might be needed for DB config, though pg_dump uses env vars or pgpass
# from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager 

logger = get_logger(__name__)

# Configuration - consider moving to a shared config module or direct env var usage in methods
DB_NAME_DEFAULT = "validator_db"
DB_USER_DEFAULT = "postgres"
DB_HOST_DEFAULT = "localhost"
DB_PORT_DEFAULT = "5432"
# DB_PASS_DEFAULT should not be hardcoded; expect from env.
BACKUP_DIR_DEFAULT = "/tmp/db_backups_gaia" # Local temporary storage for dumps
BACKUP_PREFIX_DEFAULT = "validator_db_backup_"
MANIFEST_FILENAME_DEFAULT = "latest_backup_manifest.txt"
MAX_BACKUPS_DEFAULT = 5 # Keep last 5 backups
PG_DUMP_TIMEOUT_SECONDS = 1800 # Timeout for pg_dump command (30 minutes)
TARGET_BACKUP_MINUTE = 30 # Target minute of the hour to start backups (e.g., HH:30)

class BackupManager:
    def __init__(self, storage_manager: StorageManager, 
                 db_name: str, db_user: str, db_host: str, db_port: str, db_password: str | None,
                 local_backup_dir: str, backup_prefix: str, 
                 manifest_filename: str, max_backups: int):
        self.storage_manager = storage_manager
        self.storage_backend_name = get_storage_backend_name(storage_manager)
        self.db_name = db_name
        self.db_user = db_user
        self.db_host = db_host
        self.db_port = db_port
        self.db_password = db_password # Can be None
        self.local_backup_dir = local_backup_dir
        self.backup_prefix = backup_prefix
        self.manifest_filename = manifest_filename
        self.max_backups = max_backups
        os.makedirs(self.local_backup_dir, exist_ok=True)
        
        logger.info(f"BackupManager initialized with {self.storage_backend_name} storage backend")

    async def _run_pg_dump(self, backup_file_path: str) -> bool:
        cmd = [
            "pg_dump",
            "-U", self.db_user,
            "-h", self.db_host,
            "-p", self.db_port,
            "-d", self.db_name,
            "-Fc", # Custom format, compressed
            "-f", backup_file_path
        ]
        logger.info(f"Running pg_dump: {' '.join(cmd)} (Password presence: {'yes' if self.db_password else 'no'})")
        
        # Prepare environment for the subprocess
        sub_env = os.environ.copy()
        if self.db_password:
            sub_env["PGPASSWORD"] = self.db_password
        else:
            # If no password, ensure PGPASSWORD is not set from a higher environment,
            # to allow other auth methods like .pgpass or peer authentication to work.
            if "PGPASSWORD" in sub_env:
                del sub_env["PGPASSWORD"]

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                env=sub_env, # Pass the modified environment
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            logger.info(f"Waiting for pg_dump (PID: {process.pid}) to complete (timeout: {PG_DUMP_TIMEOUT_SECONDS}s)...")
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(), 
                    timeout=PG_DUMP_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                logger.error(f"pg_dump timed out after {PG_DUMP_TIMEOUT_SECONDS} seconds.")
                try:
                    process.kill() # Ensure the process is killed
                    await process.wait() # Wait for the process to terminate
                except Exception as e_kill:
                    logger.error(f"Error trying to kill timed-out pg_dump process: {e_kill}")
                return False # pg_dump failed due to timeout
            
            if process.returncode == 0:
                logger.info(f"pg_dump completed successfully for {backup_file_path}.")
                if stderr:
                    logger.warning(f"pg_dump stderr (though successful): {stderr.decode().strip()}")
                return True
            else:
                logger.error(f"pg_dump failed with return code {process.returncode}.")
                logger.error(f"pg_dump stdout: {stdout.decode().strip()}")
                logger.error(f"pg_dump stderr: {stderr.decode().strip()}")
                return False
        except FileNotFoundError:
            logger.error("pg_dump command not found. Ensure PostgreSQL client tools are installed and in PATH.")
            return False
        except Exception as e:
            logger.error(f"An exception occurred while running pg_dump: {e}")
            return False

    async def _update_latest_backup_manifest(self, latest_dump_blob_name: str) -> bool:
        logger.info(f"Updating manifest file '{self.manifest_filename}' with new backup: {latest_dump_blob_name}")
        return await self.storage_manager.upload_blob_content(latest_dump_blob_name, self.manifest_filename)

    async def _prune_old_backups(self) -> None:
        logger.info(f"Pruning old backups from {self.storage_backend_name} storage...")
        try:
            # Assuming dumps are stored with a prefix or in a specific virtual folder for listing
            blob_list = await self.storage_manager.list_blobs(prefix=f"dumps/{self.backup_prefix}")
            
            # Filter and sort backups by timestamp in filename (descending, newest first)
            # Example filename: dumps/validator_db_backup_20230101_120000.dump
            backups = []
            for blob_name in blob_list:
                try:
                    filename_part = blob_name.split('/')[-1] # Get filename from full blob path
                    if filename_part.startswith(self.backup_prefix) and filename_part.endswith(".dump"):
                        # Extract timestamp string: validator_db_backup_YYYYMMDD_HHMMSS.dump
                        ts_str = filename_part[len(self.backup_prefix):-len(".dump")]
                        dt_obj = datetime.datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
                        backups.append((dt_obj, blob_name))
                except ValueError as e:
                    logger.warning(f"Could not parse timestamp from blob name '{blob_name}': {e}")
            
            backups.sort(key=lambda item: item[0], reverse=True) # Sort by datetime object, newest first
            
            if len(backups) > self.max_backups:
                backups_to_delete = backups[self.max_backups:]
                logger.info(f"Found {len(backups_to_delete)} old backups to delete from {self.storage_backend_name}.")
                for _, blob_name_to_delete in backups_to_delete:
                    logger.info(f"Deleting old {self.storage_backend_name} backup: {blob_name_to_delete}")
                    await self.storage_manager.delete_blob(blob_name_to_delete)
            else:
                logger.info(f"No old {self.storage_backend_name} backups to prune based on current count and max limit.")
        except Exception as e:
            logger.error(f"Error during {self.storage_backend_name} backup pruning: {e}", exc_info=True)
    
    def _os_remove_sync(self, file_path: str):
        """Synchronous wrapper for os.remove."""
        if os.path.exists(file_path):
            os.remove(file_path)
            return True
        return False

    async def _prune_local_backup(self, local_file_path: str) -> None:
        try:
            loop = asyncio.get_event_loop()
            removed = await loop.run_in_executor(None, self._os_remove_sync, local_file_path)
            if removed:
                logger.info(f"Successfully pruned local backup: {local_file_path}")
            elif os.path.exists(local_file_path): # Check again in case of race or if remove failed silently
                logger.warning(f"Local backup file still exists after prune attempt: {local_file_path}")
            else:
                logger.info(f"Local backup file did not exist or was already pruned: {local_file_path}")
        except Exception as e:
            logger.error(f"Error pruning local backup {local_file_path}: {e}")

    async def perform_backup_cycle(self) -> bool:
        timestamp_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        local_dump_filename = f"{self.backup_prefix}{timestamp_str}.dump"
        local_dump_full_path = os.path.join(self.local_backup_dir, local_dump_filename)
        storage_blob_name = f"dumps/{local_dump_filename}" # Store in a 'dumps' folder in storage

        logger.info(f"Starting database backup cycle. Target: {local_dump_full_path}, {self.storage_backend_name} Blob: {storage_blob_name}")

        # 1. Run pg_dump
        logger.info("Step 1: Running pg_dump...")
        if not await self._run_pg_dump(local_dump_full_path):
            logger.error("pg_dump failed. Aborting backup cycle.")
            await self._prune_local_backup(local_dump_full_path) # Clean up failed dump
            return False
        logger.info("Step 1: pg_dump successful.")

        # 2. Upload to storage
        logger.info(f"Step 2: Uploading '{local_dump_full_path}' to {self.storage_backend_name} as '{storage_blob_name}'...")
        if not await self.storage_manager.upload_blob(local_dump_full_path, storage_blob_name):
            logger.error(f"{self.storage_backend_name} upload failed. Aborting backup cycle.")
            await self._prune_local_backup(local_dump_full_path) # Clean up local dump if upload fails
            return False
        logger.info(f"Step 2: {self.storage_backend_name} upload successful.")

        # 3. Update manifest
        logger.info("Step 3: Updating latest backup manifest...")
        if not await self._update_latest_backup_manifest(storage_blob_name):
            logger.warning("Failed to update latest backup manifest. Continuing, but replicas might not find this backup immediately.")
            # Not returning False here as the backup itself is successful and uploaded.
        else:
            logger.info("Step 3: Manifest update successful.")

        # 4. Prune old backups
        logger.info(f"Step 4: Pruning old {self.storage_backend_name} backups...")
        await self._prune_old_backups()
        logger.info(f"Step 4: {self.storage_backend_name} pruning finished.")

        # 5. Prune local dump file
        logger.info(f"Step 5: Pruning local dump file '{local_dump_full_path}'...")
        await self._prune_local_backup(local_dump_full_path)
        logger.info("Step 5: Local pruning finished.")
        
        logger.info(f"Database backup cycle completed successfully for {storage_blob_name}.")
        return True

    async def start_periodic_backups(self, interval_hours: int):
        if interval_hours <= 0:
            logger.warning("DB Sync Backup interval is not positive. Periodic backups will not run.")
            return

        logger.info(f"Starting periodic database backups, aiming for {TARGET_BACKUP_MINUTE} minutes past the hour, every {interval_hours} hour(s).")
        
        while True:
            now = datetime.datetime.now(datetime.timezone.utc)
            
            # Calculate the next target start time
            next_run_hour = now.hour
            if now.minute >= TARGET_BACKUP_MINUTE:
                # If current minute is past the target, schedule for the next interval_hours block
                # This logic needs to be careful if interval_hours is not 1
                # For simplicity with interval_hours, let's assume we just advance to next scheduled slot based on current hour.
                # More robust: consider the last run time if we stored it.
                # Current simple approach: if it's 10:45 and interval is 1h, next is 11:30.
                # If it's 10:15 and interval is 1h, next is 10:30.
                # If interval_hours > 1, this needs to be smarter about which hour is next.
                # For now, let's just align to the *next* HH:TARGET_BACKUP_MINUTE that also respects the interval.
                
                # Simpler approach: Calculate time to next HH:MM, then if that time is too soon for the interval, add interval.
                # This will drift if perform_backup_cycle takes significant time.
                
                # Revised approach for better alignment with interval:
                # Calculate how many 'interval_hours' blocks have passed since midnight UTC for the current day for the target minute.
                # Example: interval_hours = 3. Runs at 00:30, 03:30, 06:30, etc.
                # If current time is 05:00, target is 06:30.
                # If current time is 02:50, target is 03:30.
                # If current time is 03:35, target is 06:30.

                # Determine the 'current' or 'next' valid slot based on interval_hours and TARGET_BACKUP_MINUTE
                current_hour_block = now.hour // interval_hours 
                next_potential_hour = current_hour_block * interval_hours
                
                target_datetime_this_block = now.replace(hour=next_potential_hour, minute=TARGET_BACKUP_MINUTE, second=0, microsecond=0)
                
                if now >= target_datetime_this_block:
                    # We are past the target time for the current block, or right at it.
                    # Move to the next interval block.
                    next_run_base_hour = (current_hour_block + 1) * interval_hours
                    # Handle day overflow if next_run_base_hour >= 24
                    days_to_add = next_run_base_hour // 24
                    final_target_hour = next_run_base_hour % 24
                    target_run_time = now.replace(hour=final_target_hour, minute=TARGET_BACKUP_MINUTE, second=0, microsecond=0) + datetime.timedelta(days=days_to_add)
                else:
                    # The target time in the current block is still in the future.
                    target_run_time = target_datetime_this_block
            else: # now.minute < TARGET_BACKUP_MINUTE
                # Target minute is in the future for the current hour (or a past hour if interval > 1, handled by block logic above)
                current_hour_block = now.hour // interval_hours
                target_base_hour_this_block = current_hour_block * interval_hours 
                # Ensure we are targeting a valud hour slot based on interval
                target_run_time = now.replace(hour=target_base_hour_this_block, minute=TARGET_BACKUP_MINUTE, second=0, microsecond=0)
                if target_run_time < now : # If this calculated time is in the past (e.g. now is 05:00, interval 3, target_base_hour is 03:00)
                    # Move to the next interval block.
                    next_run_base_hour = (current_hour_block + 1) * interval_hours
                    days_to_add = next_run_base_hour // 24
                    final_target_hour = next_run_base_hour % 24
                    target_run_time = now.replace(hour=final_target_hour, minute=TARGET_BACKUP_MINUTE, second=0, microsecond=0) + datetime.timedelta(days=days_to_add)


            wait_seconds = (target_run_time - now).total_seconds()
            if wait_seconds < 0 : # Should ideally not happen with the logic above, but as a safeguard
                logger.warning(f"Calculated wait time is negative ({wait_seconds}s). Scheduling for next interval. This might indicate a logic issue in scheduling.")
                wait_seconds = interval_hours * 3600 # Fallback to simple interval sleep
            
            logger.info(f"Next backup cycle scheduled for {target_run_time.strftime('%Y-%m-%d %H:%M:%S %Z')}. Waiting for {wait_seconds / 3600:.2f} hours.")
            await asyncio.sleep(wait_seconds)

            try:
                await self.perform_backup_cycle()
            except Exception as e:
                logger.error(f"Critical error during periodic backup cycle: {e}", exc_info=True)

    async def close(self):
        # The storage_manager is passed in, so its lifecycle is managed externally (e.g., by GaiaValidator)
        logger.info("BackupManager closed. (Storage client lifecycle managed externally)")

async def get_backup_manager(storage_manager: StorageManager) -> BackupManager | None:
    if not storage_manager:
        logger.error("StorageManager instance is required to create BackupManager.")
        return None

    db_name = os.getenv("DB_NAME", DB_NAME_DEFAULT) # Using DB_NAME from env
    db_user = os.getenv("DB_USER", DB_USER_DEFAULT) # Using DB_USER from env
    db_host = os.getenv("DB_HOST", DB_HOST_DEFAULT) # Using DB_HOST from env
    db_port = os.getenv("DB_PORT", DB_PORT_DEFAULT) # Using DB_PORT from env
    db_password = os.getenv("DB_PASS") # Using DB_PASS from env (can be None)
    
    local_backup_dir = os.getenv("DB_SYNC_BACKUP_DIR", BACKUP_DIR_DEFAULT)
    backup_prefix = os.getenv("DB_SYNC_BACKUP_PREFIX", BACKUP_PREFIX_DEFAULT)
    manifest_filename = os.getenv("DB_SYNC_MANIFEST_FILENAME", MANIFEST_FILENAME_DEFAULT)
    max_backups = int(os.getenv("DB_SYNC_MAX_BACKUPS", MAX_BACKUPS_DEFAULT))

    return BackupManager(
        storage_manager=storage_manager,
        db_name=db_name,
        db_user=db_user,
        db_host=db_host,
        db_port=db_port,
        db_password=db_password,
        local_backup_dir=local_backup_dir,
        backup_prefix=backup_prefix,
        manifest_filename=manifest_filename,
        max_backups=max_backups
    )


# Example Usage (for testing)
async def _example_main_backup():
    from gaia.validator.sync.storage_utils import get_storage_manager_for_db_sync # Relative import for example
    logger.info("Testing BackupManager...")
    
    # Check for storage configuration - supports both Azure and R2
    azure_configured = bool(os.getenv("AZURE_STORAGE_CONNECTION_STRING") or 
                           (os.getenv("AZURE_STORAGE_ACCOUNT_URL") and os.getenv("AZURE_STORAGE_SAS_TOKEN")))
    r2_configured = bool(os.getenv("R2_ENDPOINT_URL") and 
                        os.getenv("R2_ACCESS_KEY_ID") and 
                        os.getenv("R2_SECRET_ACCESS_KEY"))
    
    if not azure_configured and not r2_configured:
        logger.error("""
        No storage backend configured. Please set one of:
        
        For Azure:
        - AZURE_STORAGE_CONNECTION_STRING, or
        - AZURE_STORAGE_ACCOUNT_URL + AZURE_STORAGE_SAS_TOKEN
        
        For R2:
        - R2_ENDPOINT_URL + R2_ACCESS_KEY_ID + R2_SECRET_ACCESS_KEY
        
        You can also set STORAGE_BACKEND=azure|r2|auto to specify preference.
        """)
        return

    # It's also assumed that PostgreSQL is running and accessible for pg_dump
    # and that pg_dump is in the system PATH. Adjust DB_USER and DB_NAME if needed via env vars.

    storage_manager_instance = await get_storage_manager_for_db_sync()
    if not storage_manager_instance:
        logger.error("Failed to initialize StorageManager for BackupManager test.")
        return

    backup_manager_instance = await get_backup_manager(storage_manager_instance)
    if not backup_manager_instance:
        logger.error("Failed to initialize BackupManager.")
        await storage_manager_instance.close()
        return
    
    try:
        logger.info("Performing a single backup cycle for testing...")
        success = await backup_manager_instance.perform_backup_cycle()
        if success:
            logger.info("SUCCESS: Test backup cycle completed.")
        else:
            logger.error("FAILURE: Test backup cycle did not complete successfully.")
    except Exception as e:
        logger.error(f"Error during BackupManager test: {e}", exc_info=True)
    finally:
        await backup_manager_instance.close()
        await storage_manager_instance.close()

if __name__ == "__main__":
    # To run: python -m gaia.validator.sync.backup_manager
    asyncio.run(_example_main_backup()) 