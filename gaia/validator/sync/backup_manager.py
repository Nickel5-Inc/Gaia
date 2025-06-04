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
from datetime import timedelta, timezone
from typing import Optional

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
    def __init__(self, storage_manager: StorageManager, test_mode: bool = False):
        """
        Manages database backups using pgBackRest.

        Args:
            storage_manager: The storage manager for uploading backup metadata or status.
                             (Note: pgBackRest handles direct backup storage).
            test_mode: If True, backups run more frequently for testing.
        """
        self.storage_manager = storage_manager
        self.test_mode = test_mode
        self.pgbackrest_stanza = os.getenv("PGBACKREST_STANZA", "main") # Or your default stanza name
        
        self.next_full_backup_time_utc: Optional[datetime] = None
        self.next_wal_push_time_utc: Optional[datetime] = None
        self._schedule_initial_backups()

    def _schedule_initial_backups(self):
        """Calculates and sets the initial next backup times."""
        now_utc = datetime.now(timezone.utc)
        self.next_full_backup_time_utc = self._calculate_next_full_backup_time(now_utc)
        self.next_wal_push_time_utc = self._calculate_next_wal_push_time(now_utc)
        logger.info(f"Initial backup schedule: Next Full Backup at {self.next_full_backup_time_utc.isoformat()}, Next WAL Push at {self.next_wal_push_time_utc.isoformat()}")

    def _calculate_next_full_backup_time(self, current_time_utc: datetime) -> datetime:
        """
        Calculates the next scheduled full backup time (00:10 UTC).
        If current time is past 00:10 UTC today, it schedules for 00:10 UTC tomorrow.
        """
        scheduled_time_today = current_time_utc.replace(hour=0, minute=10, second=0, microsecond=0)
        if current_time_utc > scheduled_time_today:
            return scheduled_time_today + timedelta(days=1)
        return scheduled_time_today

    def _calculate_next_wal_push_time(self, current_time_utc: datetime) -> datetime:
        """
        Calculates the next scheduled WAL push time (HH:15 UTC).
        If current time is past HH:15 UTC for the current hour, it schedules for the next hour.
        """
        scheduled_time_this_hour = current_time_utc.replace(minute=15, second=0, microsecond=0)
        if current_time_utc > scheduled_time_this_hour:
            return scheduled_time_this_hour + timedelta(hours=1)
        return scheduled_time_this_hour

    async def _trigger_pgbackrest_command(self, command_args: list[str], operation_name: str) -> bool:
        """
        Triggers a pgBackRest command.
        
        Args:
            command_args: List of arguments for the pgbackrest command.
            operation_name: Name of the operation for logging (e.g., "Full Backup", "WAL Push").
        
        Returns:
            True if successful, False otherwise.
        """
        full_command = ["pgbackrest"] + command_args
        logger.info(f"Executing pgBackRest {operation_name}: {' '.join(full_command)}")
        try:
            # Using asyncio.to_thread to run the blocking subprocess call
            process = await asyncio.to_thread(
                subprocess.run,
                full_command,
                capture_output=True,
                text=True,
                check=False # Don't raise exception on non-zero exit, handle manually
            )
            if process.returncode == 0:
                logger.info(f"pgBackRest {operation_name} successful. Output:\n{process.stdout}")
                return True
            else:
                logger.error(f"pgBackRest {operation_name} failed with code {process.returncode}. Error:\n{process.stderr}\nStdout:\n{process.stdout}")
                return False
        except FileNotFoundError:
            logger.error(f"pgBackRest command not found. Ensure it's installed and in PATH.")
            return False
        except Exception as e:
            logger.error(f"Error during pgBackRest {operation_name}: {e}", exc_info=True)
            return False

    async def _trigger_pgbackrest_full_backup(self, is_test: bool):
        """Triggers a pgBackRest full backup."""
        backup_type = "full" # Could be "incr" or "diff" for other strategies
        args = [f"--stanza={self.pgbackrest_stanza}", "backup", f"--type={backup_type}"]
        if is_test:
            args.append("--archive-timeout=30") # Example: Shorter timeout for test backups
            args.append("--compress-level=0") # Faster test backup
            logger.info(f"[Test Mode] Simulating pgBackRest Full Backup for stanza {self.pgbackrest_stanza}")
            # In a real test, you might use a dedicated test stanza or log only.
            # For now, we'll run the command but with test-friendly options if possible.
        
        success = await self._trigger_pgbackrest_command(args, "Full Backup")
        if success:
            logger.info(f"Full backup for stanza '{self.pgbackrest_stanza}' completed.")
        else:
            logger.error(f"Full backup for stanza '{self.pgbackrest_stanza}' failed.")


    async def _trigger_pgbackrest_wal_push(self, is_test: bool):
        """Triggers a pgBackRest WAL push."""
        # This command pushes already archived WAL files from the PG archive_status
        # directory to the pgBackRest repository. This is useful if archive_async is used
        # or to ensure WALs are pushed promptly.
        args = [f"--stanza={self.pgbackrest_stanza}", "archive-push"]
        if is_test:
            logger.info(f"[Test Mode] Simulating pgBackRest WAL Push for stanza {self.pgbackrest_stanza}")
            # For actual testing, this might verify WAL segments are created and pushed.

        success = await self._trigger_pgbackrest_command(args, "WAL Push")
        if success:
            logger.info(f"WAL push for stanza '{self.pgbackrest_stanza}' completed.")
        else:
            logger.error(f"WAL push for stanza '{self.pgbackrest_stanza}' failed.")


    async def start_periodic_backups(self, check_interval_hours: float):
        """
        Starts the periodic backup process.
        In test mode, it triggers backups based on check_interval_hours.
        In production, it schedules daily full backups and hourly WAL pushes.
        """
        logger.info(
            f"BackupManager started. Test mode: {self.test_mode}. "
            f"Stanza: {self.pgbackrest_stanza}. "
            f"Base check interval: {check_interval_hours} hours."
        )

        while True:
            try:
                sleep_duration_seconds = check_interval_hours * 3600
                
                current_time_utc = datetime.now(timezone.utc)

                if self.test_mode:
                    # In test mode, trigger both operations based on the short check_interval_hours
                    logger.info(f"[Test Mode] Interval reached. Triggering test operations at {current_time_utc.isoformat()}")
                    await self._trigger_pgbackrest_full_backup(is_test=True)
                    await self._trigger_pgbackrest_wal_push(is_test=True)
                    # Sleep for the short test interval
                    await asyncio.sleep(sleep_duration_seconds)
                    continue # Restart loop for next test cycle

                # Non-test mode: Check against scheduled times
                # Determine the earliest next operation to calculate sleep time
                next_op_time = min(self.next_full_backup_time_utc, self.next_wal_push_time_utc)
                
                time_until_next_op_seconds = (next_op_time - current_time_utc).total_seconds()

                if time_until_next_op_seconds <= 0: # If an operation is due or overdue
                    if self.next_full_backup_time_utc <= current_time_utc:
                        logger.info(f"Scheduled time for FULL backup reached: {current_time_utc.isoformat()}")
                        await self._trigger_pgbackrest_full_backup(is_test=False)
                        self.next_full_backup_time_utc = self._calculate_next_full_backup_time(current_time_utc + timedelta(minutes=1)) # Schedule for next occurrence
                        logger.info(f"Next FULL backup scheduled for: {self.next_full_backup_time_utc.isoformat()}")

                    if self.next_wal_push_time_utc <= current_time_utc:
                        logger.info(f"Scheduled time for WAL push reached: {current_time_utc.isoformat()}")
                        await self._trigger_pgbackrest_wal_push(is_test=False)
                        self.next_wal_push_time_utc = self._calculate_next_wal_push_time(current_time_utc + timedelta(minutes=1)) # Schedule for next occurrence
                        logger.info(f"Next WAL push scheduled for: {self.next_wal_push_time_utc.isoformat()}")
                    
                    # After handling due operations, recalculate sleep for the *new* earliest next operation
                    next_op_time_after_run = min(self.next_full_backup_time_utc, self.next_wal_push_time_utc)
                    actual_sleep_duration = max(60, (next_op_time_after_run - datetime.now(timezone.utc)).total_seconds()) # Sleep at least 60s
                else:
                    # Sleep until the earliest next operation, or for the check_interval, whichever is shorter, but at least 60s
                    actual_sleep_duration = max(60, min(time_until_next_op_seconds, sleep_duration_seconds))

                logger.debug(f"Backup manager sleeping for {actual_sleep_duration:.2f} seconds.")
                await asyncio.sleep(actual_sleep_duration)

            except asyncio.CancelledError:
                logger.info("BackupManager task was cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in BackupManager loop: {e}", exc_info=True)
                # Avoid rapid cicllying on error
                await asyncio.sleep(300) # Sleep for 5 minutes before retrying

    async def close(self):
        # The storage_manager is passed in, so its lifecycle is managed externally (e.g., by GaiaValidator)
        logger.info("BackupManager closed. (Storage client lifecycle managed externally)")

async def get_backup_manager(storage_manager: StorageManager, test_mode: bool = False) -> Optional[BackupManager]:
    """
    Factory function to create a BackupManager instance.
    This function primarily exists to match the pattern of get_restore_manager
    and to allow for potential future pre-configuration checks.
    """
    if not os.getenv("PGBACKREST_STANZA"):
        logger.warning("PGBACKREST_STANZA environment variable not set. BackupManager might not function correctly without a stanza.")
        # Depending on strictness, could return None here.
        # For now, allow it to proceed and log error later if pgbackrest command fails.

    # You could add checks here to ensure pgbackrest is installed, config exists, etc.
    # For example:
    # try:
    #     subprocess.run(["pgbackrest", "info"], capture_output=True, text=True, check=True)
    # except (FileNotFoundError, subprocess.CalledProcessError) as e:
    #     logger.error(f"pgBackRest not configured or not found: {e}. BackupManager will not be started.")
    #     return None
        
    logger.info(f"Creating BackupManager. Test mode: {test_mode}")
    return BackupManager(storage_manager, test_mode=test_mode)


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