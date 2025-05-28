# gaia/validator/sync/restore_manager.py
import asyncio
import os
import datetime
import time
import subprocess # For pg_restore, psql
from fiber.logging_utils import get_logger
from gaia.validator.sync.azure_blob_utils import AzureBlobManager # Direct import
import random
from typing import Optional

logger = get_logger(__name__)

# Configuration - similar to BackupManager
DB_NAME_DEFAULT = "validator_db"
DB_USER_DEFAULT = "postgres" # User for pg_restore and DB operations
DB_HOST_DEFAULT = "localhost"
DB_PORT_DEFAULT = "5432"
# DB_PASS_DEFAULT not hardcoded
RESTORE_DIR_DEFAULT = "/tmp/db_restores_gaia" # Local dir for downloaded dumps
MANIFEST_FILENAME_DEFAULT = "latest_backup_manifest.txt" # Should match BackupManager
LAST_RESTORED_MARKER_FILE_DEFAULT = ".last_restored_backup_marker"

class RestoreManager:
    def __init__(self, azure_manager: AzureBlobManager, 
                 db_name: str, db_user: str, db_host: str, db_port: str, db_password: str | None,
                 local_restore_dir: str, manifest_filename: str):
        self.azure_manager = azure_manager
        self.db_name = db_name
        self.db_user = db_user
        self.db_host = db_host
        self.db_port = db_port
        self.db_password = db_password # Can be None
        self.local_restore_dir = local_restore_dir
        self.manifest_filename = manifest_filename
        self.last_restored_marker_path = os.path.join(self.local_restore_dir, LAST_RESTORED_MARKER_FILE_DEFAULT)
        os.makedirs(self.local_restore_dir, exist_ok=True)
        self._is_restoring_lock = asyncio.Lock() # Prevent concurrent restore attempts
        self._stop_event = asyncio.Event() # For graceful shutdown

    def _read_marker_file_sync(self, file_path: str) -> Optional[str]:
        """Synchronous helper to read marker file content."""
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    return f.read().strip()
        except Exception as e:
            logger.error(f"Sync Error reading marker file {file_path}: {e}")
        return None

    async def _get_latest_backup_blob_name_from_manifest(self) -> str | None:
        logger.info(f"Fetching latest backup name from Azure manifest: {self.manifest_filename}")
        blob_name = await self.azure_manager.read_blob_content(self.manifest_filename)
        if blob_name:
            logger.info(f"Latest backup blob name from manifest: {blob_name}")
            return blob_name
        else:
            logger.warning("Could not read latest backup blob name from manifest.")
            return None

    async def _get_last_restored_backup_name(self) -> str | None:
        try:
            loop = asyncio.get_event_loop()
            last_restored = await loop.run_in_executor(None, self._read_marker_file_sync, self.last_restored_marker_path)
            if last_restored:
                logger.info(f"Last locally restored backup marker: {last_restored}")
                return last_restored
            logger.info("No last restored backup marker file found or error reading it.")
            return None
        except Exception as e:
            logger.error(f"Async Error reading last restored backup marker: {e}") # Should be caught by sync helper ideally
            return None

    def _write_marker_file_sync(self, file_path: str, content: str) -> bool:
        """Synchronous helper to write content to a marker file."""
        try:
            with open(file_path, 'w') as f:
                f.write(content)
            return True
        except Exception as e:
            logger.error(f"Sync Error writing marker file {file_path}: {e}")
        return False

    async def _set_last_restored_backup_name(self, blob_name: str) -> None:
        try:
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(None, self._write_marker_file_sync, self.last_restored_marker_path, blob_name)
            if success:
                logger.info(f"Successfully updated last restored backup marker to: {blob_name}")
            else:
                logger.error(f"Failed to write last restored backup marker for: {blob_name}")
        except Exception as e:
            logger.error(f"Async Error writing last restored backup marker for {blob_name}: {e}")

    async def _run_psql_command(self, command: str, dbname: str = None) -> bool:
        psql_cmd = ["psql", 
                    "-U", self.db_user,
                    "-h", self.db_host,
                    "-p", self.db_port]
        if dbname:
            psql_cmd.extend(["-d", dbname])
        psql_cmd.extend(["-c", command])
        
        sub_env = os.environ.copy()
        if self.db_password:
            sub_env["PGPASSWORD"] = self.db_password
        elif "PGPASSWORD" in sub_env:
            del sub_env["PGPASSWORD"]

        logger.info(f"Running psql command: {' '.join(psql_cmd)}")
        process = await asyncio.create_subprocess_exec(
            *psql_cmd,
            env=sub_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            logger.info(f"psql command successful: {command}")
            if stderr:
                logger.warning(f"psql stderr (though successful): {stderr.decode().strip()}")
            return True
        else:
            logger.error(f"psql command '{command}' failed with code {process.returncode}")
            logger.error(f"psql stdout: {stdout.decode().strip()}")
            logger.error(f"psql stderr: {stderr.decode().strip()}")
            return False

    async def _run_pg_restore(self, downloaded_dump_path: str) -> bool:
        logger.info(f"Starting pg_restore for database '{self.db_name}' from '{downloaded_dump_path}'.")
        # Ideally, the application should handle connection closing/reopening around this.
        # This is a simplified version. For production, ensure connections are gracefully handled.

        # 1. Terminate existing connections (forcefully, be careful in prod)
        # Note: This requires superuser or specific grants. Alternative: application coordination.
        terminate_sql = f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{self.db_name}' AND pid <> pg_backend_pid();"
        logger.info("Attempting to terminate existing connections to the database...")
        if not await self._run_psql_command(terminate_sql, dbname="postgres"): # Connect to a default DB like postgres or template1
            logger.warning("Failed to terminate connections. Restore might fail if DB is in use.")
            # Not returning False here, will let pg_restore try.

        # 2. Drop database
        logger.info(f"Dropping database '{self.db_name}'...")
        if not await self._run_psql_command(f"DROP DATABASE IF EXISTS {self.db_name};", dbname="postgres"):
            logger.error("Failed to drop database. Aborting restore.")
            return False

        # 3. Create database
        logger.info(f"Creating database '{self.db_name}' with owner '{self.db_user}'...")
        if not await self._run_psql_command(f"CREATE DATABASE {self.db_name} OWNER {self.db_user};", dbname="postgres"):
            logger.error("Failed to create database. Aborting restore.")
            return False

        # 4. Run pg_restore
        restore_cmd = [
            "pg_restore",
            "-U", self.db_user,
            "-h", self.db_host,
            "-p", self.db_port,
            "-d", self.db_name,
            "--no-owner",       # Often useful if restoring to a DB owned by a different user than original
            "--clean",          # Add clean option if dropping individual objects instead of whole DB
            "--if-exists",      # Add if-exists with --clean
            "-j", "4",         # Number of parallel jobs, adjust based on server cores
            downloaded_dump_path
        ]
        logger.info(f"Running pg_restore: {' '.join(restore_cmd)}")
        
        sub_env = os.environ.copy()
        if self.db_password:
            sub_env["PGPASSWORD"] = self.db_password
        elif "PGPASSWORD" in sub_env:
            del sub_env["PGPASSWORD"]
            
        process = await asyncio.create_subprocess_exec(
            *restore_cmd,
            env=sub_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"pg_restore completed successfully for {self.db_name}.")
            if stderr:
                 logger.warning(f"pg_restore stderr (though successful): {stderr.decode().strip()}")
            return True
        else:
            logger.error(f"pg_restore failed with return code {process.returncode} for {self.db_name}.")
            logger.error(f"pg_restore stdout: {stdout.decode().strip()}")
            logger.error(f"pg_restore stderr: {stderr.decode().strip()}")
            return False
        
    def _os_remove_sync(self, file_path: str):
        """Synchronous wrapper for os.remove."""
        if os.path.exists(file_path):
            os.remove(file_path)
            return True
        return False

    async def _prune_local_download(self, local_file_path: str) -> None:
        try:
            loop = asyncio.get_event_loop()
            removed = await loop.run_in_executor(None, self._os_remove_sync, local_file_path)
            if removed:
                logger.info(f"Successfully pruned local downloaded backup: {local_file_path}")
            elif os.path.exists(local_file_path):
                logger.warning(f"Local downloaded backup file still exists after prune attempt: {local_file_path}")
            else:
                logger.info(f"Local downloaded backup file did not exist or was already pruned: {local_file_path}")
        except Exception as e:
            logger.error(f"Error pruning local downloaded backup {local_file_path}: {e}")

    async def perform_restore_cycle(self) -> bool:
        if self._is_restoring_lock.locked():
            logger.info("Restore cycle already in progress. Skipping.")
            return False
        
        async with self._is_restoring_lock:
            logger.info("Starting database restore check cycle...")
            latest_azure_backup_blob = await self._get_latest_backup_blob_name_from_manifest()
            if not latest_azure_backup_blob:
                logger.info("No new backup found in Azure manifest. Restore cycle ending.")
                return False

            last_locally_restored = await self._get_last_restored_backup_name()

            if latest_azure_backup_blob == last_locally_restored:
                logger.info(f"Database is already up-to-date with the latest backup: {latest_azure_backup_blob}. Restore cycle ending.")
                return False

            logger.info(f"New backup found: {latest_azure_backup_blob}. Current local: {last_locally_restored or 'None'}. Proceeding with restore.")
            
            local_download_filename = latest_azure_backup_blob.split('/')[-1]
            local_download_full_path = os.path.join(self.local_restore_dir, local_download_filename)

            # Download new backup
            logger.info(f"Downloading '{latest_azure_backup_blob}' to '{local_download_full_path}'...")
            if not await self.azure_manager.download_blob(latest_azure_backup_blob, local_download_full_path):
                logger.error("Failed to download new backup. Aborting restore.")
                await self._prune_local_download(local_download_full_path)
                return True # Attempted restore

            # Run pg_restore
            logger.info("Performing database restore...")
            restore_successful = await self._run_pg_restore(local_download_full_path)

            # Prune downloaded file regardless of restore success/failure
            await self._prune_local_download(local_download_full_path)

            if restore_successful:
                logger.info(f"Database restore successful from {latest_azure_backup_blob}.")
                await self._set_last_restored_backup_name(latest_azure_backup_blob)
            else:
                logger.error(f"Database restore FAILED from {latest_azure_backup_blob}.")
            
            logger.info("Restore cycle finished.")
            return True # Attempted restore

    async def start_periodic_restores(self, interval_hours: int):
        if interval_hours <= 0:
            logger.warning("DB Sync Restore interval is not positive. Periodic restores will not run.")
            return
            
        logger.info(f"Starting periodic database restore checks every {interval_hours} hour(s).")
        await asyncio.sleep(random.uniform(60, 120)) # Stagger initial start slightly more than backup

        while not self._stop_event.is_set():
            try:
                await self.perform_restore_cycle()
            except Exception as e:
                logger.error(f"Critical error during periodic restore cycle: {e}", exc_info=True)
            
            try:
                # Wait for the interval or until stop_event is set
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval_hours * 3600)
                if self._stop_event.is_set():
                    logger.info("Stop event received, exiting periodic restore loop.")
                    break
            except asyncio.TimeoutError:
                # This is expected, means the interval passed
                logger.info(f"Restore check interval ended. Next check in ~{interval_hours}h.")
            except Exception as e:
                 logger.error(f"Error in restore sleep logic: {e}") # Should not happen
                 await asyncio.sleep(interval_hours * 3600) # Fallback sleep
                 
    async def stop_periodic_restores(self):
        logger.info("Stopping periodic database restores...")
        self._stop_event.set()
            
    async def close(self):
        await self.stop_periodic_restores()
        # Wait for lock to release if a restore is in progress, with a timeout
        try:
            await asyncio.wait_for(self._is_restoring_lock.acquire(), timeout=300) # Wait up to 5 mins
            self._is_restoring_lock.release()
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for restore lock to release during close. Potential unterminated restore.")
        except Exception as e:
            logger.error(f"Error managing restore lock during close: {e}")
        logger.info("RestoreManager closed. (Azure client lifecycle managed externally)")

async def get_restore_manager(azure_manager: AzureBlobManager) -> RestoreManager | None:
    if not azure_manager:
        logger.error("AzureBlobManager instance is required to create RestoreManager.")
        return None

    db_name = os.getenv("DB_NAME", DB_NAME_DEFAULT)
    db_user = os.getenv("DB_USER", DB_USER_DEFAULT)
    db_host = os.getenv("DB_HOST", DB_HOST_DEFAULT)
    db_port = os.getenv("DB_PORT", DB_PORT_DEFAULT)
    db_password = os.getenv("DB_PASS") # Can be None

    local_restore_dir = os.getenv("DB_SYNC_RESTORE_DIR", RESTORE_DIR_DEFAULT)
    manifest_filename = os.getenv("DB_SYNC_MANIFEST_FILENAME", MANIFEST_FILENAME_DEFAULT)

    return RestoreManager(
        azure_manager=azure_manager,
        db_name=db_name,
        db_user=db_user,
        db_host=db_host,
        db_port=db_port,
        db_password=db_password,
        local_restore_dir=local_restore_dir,
        manifest_filename=manifest_filename
    )

# Example Usage (for testing)
async def _example_main_restore():
    from gaia.validator.sync.azure_blob_utils import get_azure_blob_manager_for_db_sync
    logger.info("Testing RestoreManager...")

    if not os.getenv("AZURE_STORAGE_CONNECTION_STRING"):
        logger.error("Please set AZURE_STORAGE_CONNECTION_STRING environment variable to test RestoreManager.")
        return

    # Assumes a backup has been created by BackupManager and manifest exists.
    # Assumes PostgreSQL is running and accessible for psql/pg_restore.

    azure_manager_instance = await get_azure_blob_manager_for_db_sync()
    if not azure_manager_instance:
        logger.error("Failed to initialize AzureBlobManager for RestoreManager test.")
        return

    restore_manager_instance = await get_restore_manager(azure_manager_instance)
    if not restore_manager_instance:
        logger.error("Failed to initialize RestoreManager.")
        await azure_manager_instance.close()
        return

    try:
        logger.info("Performing a single restore cycle for testing...")
        # To test this, ensure a backup is in Azure and manifest is updated (e.g., run backup_manager example first)
        # For a real test, you might want to manually place a .last_restored_backup_marker with an older backup name.
        # e.g., with open(os.path.join(RESTORE_DIR_DEFAULT, LAST_RESTORED_MARKER_FILE_DEFAULT), 'w') as f: 
        #    f.write("dumps/validator_db_backup_YYYYMMDD_HHMMSS_old.dump") 

        restored_something = await restore_manager_instance.perform_restore_cycle()
        if restored_something:
            logger.info("Test restore cycle attempted. Check logs for success/failure.")
        else:
            logger.info("Test restore cycle found no new backup or was already up-to-date.")
            
    except Exception as e:
        logger.error(f"Error during RestoreManager test: {e}", exc_info=True)
    finally:
        await restore_manager_instance.close()
        await azure_manager_instance.close()

if __name__ == "__main__":
    # To run: python -m gaia.validator.sync.restore_manager
    asyncio.run(_example_main_restore()) 