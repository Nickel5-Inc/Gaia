# gaia/validator/sync/restore_manager.py
import asyncio
import os
import subprocess # For pgBackRest commands
from fiber.logging_utils import get_logger
from typing import Optional

logger = get_logger(__name__)

# pgBackRest related constants
PGBACKREST_STANZA_DEFAULT = "gaia"

class RestoreManager:
    def __init__(self, test_mode: bool = False):
        """
        Manages pgBackRest restore operations.
        Restoring a database is a significant operation and typically requires
        PostgreSQL to be stopped and the data directory to be empty.
        This manager provides a way to trigger the command, but orchestration
        (stopping/starting PG, clearing PGDATA) is outside its direct scope here
        and should be handled by a higher-level process or script.

        Args:
            test_mode: If True, may influence command options if specific test flags were added.
        """
        self.test_mode = test_mode # Currently not used for restore, but kept for consistency
        self.pgbackrest_stanza = os.getenv("PGBACKREST_STANZA", PGBACKREST_STANZA_DEFAULT)
        self._is_restoring_lock = asyncio.Lock() # Prevent concurrent restore attempts
        self._stop_event = asyncio.Event() # For graceful shutdown if it were a long-running service

        logger.info(f"RestoreManager initialized. Stanza: {self.pgbackrest_stanza}. "
                    f"Test_mode: {self.test_mode}.")
        logger.warning("Database restore is a destructive operation. Ensure PostgreSQL is stopped "
                       "and PGDATA is prepared before running a restore.")

    async def _trigger_pgbackrest_command(self, command_args: list[str], operation_name: str) -> bool:
        """
        Triggers a pgBackRest command (typically run as postgres user).
        """
        # Ensure stanza is part of the command if not already included by caller
        if not any(arg.startswith("--stanza=") for arg in command_args):
            command_args = [f"--stanza={self.pgbackrest_stanza}"] + command_args
        
        full_command = ["pgbackrest"] + command_args
        logger.info(f"Executing pgBackRest {operation_name}: {' '.join(full_command)}")
        logger.warning(f"Ensure this command is run with appropriate permissions (e.g., as 'postgres' user) "
                       f"and that PostgreSQL is stopped if performing a full restore.")
        try:
            # It's often required to run pgbackrest as the postgres user.
            # Using sudo -u postgres is common. This simplified trigger doesn't enforce user switching.
            # The caller or execution environment (e.g. a script) must ensure correct user.
            process = await asyncio.to_thread(
                subprocess.run,
                full_command,
                capture_output=True,
                text=True,
                check=False
            )
            if process.returncode == 0:
                logger.info(f"pgBackRest {operation_name} successful. Output:\\n{process.stdout}")
                return True
            else:
                logger.error(f"pgBackRest {operation_name} failed with code {process.returncode}. Error:\\n{process.stderr}\\nStdout:\\n{process.stdout}")
                return False
        except FileNotFoundError:
            logger.error(f"pgBackRest command not found. Ensure it's installed and in PATH.")
            return False
        except Exception as e:
            logger.error(f"Error during pgBackRest {operation_name}: {e}", exc_info=True)
            return False

    async def trigger_restore(self, options: Optional[list[str]] = None) -> bool:
        """
        Triggers a `pgbackrest restore` command.

        The caller is responsible for ensuring PostgreSQL is stopped and the
        PGDATA directory is empty or appropriately prepared for the restore.
        This method simply issues the command.

        Args:
            options: A list of additional command line options for pgBackRest restore
                     (e.g., ["--delta"], ["--type=pitr", "--target=YYYY-MM-DD HH:MM:SS"]).

        Returns:
            True if the command was issued successfully (exit code 0), False otherwise.
        """
        if self._is_restoring_lock.locked():
            logger.info("Restore operation already in progress. Skipping.")
            return False

        async with self._is_restoring_lock:
            logger.info(f"Attempting to trigger pgBackRest restore for stanza '{self.pgbackrest_stanza}'.")
            logger.warning("CRITICAL: PostgreSQL server should be STOPPED and PGDATA directory prepared (usually empty) before restore!")
            
            command = ["restore"]
            if options:
                command.extend(options)
            
            # Example of how test_mode could be used, though less common for `restore` itself:
            # if self.test_mode:
            #     logger.info("[Test Mode] Restore command - currently no specific test options applied.")

            success = await self._trigger_pgbackrest_command(command, "Restore")
            if success:
                logger.info(f"pgBackRest restore command for stanza '{self.pgbackrest_stanza}' completed.")
            else:
                logger.error(f"pgBackRest restore command for stanza '{self.pgbackrest_stanza}' failed.")
            return success

    # Removed periodic restore logic and manifest/pg_dump based restore methods.
    # The concept of a periodic restore cycle downloading dumps doesn't fit pgBackRest well.
    # pgBackRest replicas are typically standbys or restored as a specific action.

    async def stop_service_if_running(self):
        logger.info("Stopping RestoreManager service (if it were a long-running service)...")
        self._stop_event.set()
            
    async def close(self):
        await self.stop_service_if_running()
        # Wait for lock to release if a restore is in progress, with a timeout
        try:
            if self._is_restoring_lock.locked():
                 logger.info("Waiting for ongoing restore operation to complete before closing...")
                 await asyncio.wait_for(self._is_restoring_lock.acquire(), timeout=30) # Short wait, actual restore can be long
                 self._is_restoring_lock.release() # Release immediately after acquiring
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for restore lock to release during close. Restore might still be in pgBackRest process.")
        except Exception as e:
            logger.error(f"Error managing restore lock during close: {e}")
        logger.info("RestoreManager closed.")

async def get_restore_manager(test_mode: bool = False) -> Optional[RestoreManager]:
    """
    Factory function to create a RestoreManager instance.
    """
    if not os.getenv("PGBACKREST_STANZA"):
        logger.warning(f"PGBACKREST_STANZA environment variable not set. Using default: {PGBACKREST_STANZA_DEFAULT}. "
                       "RestoreManager might not function correctly if this doesn't match the actual stanza.")

    # Could add a check for pgbackrest CLI availability, similar to BackupManager
    try:
        process = await asyncio.to_thread(
            subprocess.run,
            ["pgbackrest", "version"],
            capture_output=True, text=True, check=False
        )
        if process.returncode != 0:
            logger.error(f"pgBackRest command-line tool may not be available or configured. Error: {process.stderr}")
    except FileNotFoundError:
        logger.error(f"pgBackRest command-line tool not found in PATH. RestoreManager will likely fail.")

    return RestoreManager(test_mode=test_mode)


# Example Usage (for testing ad-hoc restore trigger)
async def _example_main_restore_adhoc():
    logger.info("Testing RestoreManager (Ad-hoc Restore Trigger)...")
    logger.warning("This example is for DEMONSTRATION only. Executing a restore is a destructive action.")
    logger.warning("Ensure you understand the implications and have backed up data if necessary.")
    logger.warning("PostgreSQL should be STOPPED and PGDATA prepared on the target machine.")

    # This example assumes pgBackRest is configured, the stanza exists and has backups.
    # It also assumes PGBACKREST_STANZA env var is set or default is used.

    restore_manager_instance = await get_restore_manager(test_mode=False) # test_mode for restore is less common
    if not restore_manager_instance:
        logger.error("Failed to initialize RestoreManager for ad-hoc test.")
        return

    try:
        # --- IMPORTANT --- 
        # The following line is commented out by default to prevent accidental execution.
        # Only uncomment if you are in a safe test environment and understand the consequences.
        # logger.info("Attempting to trigger pgBackRest restore...")
        # success = await restore_manager_instance.trigger_restore() # Example: no extra options
        # # success = await restore_manager_instance.trigger_restore(options=["--delta"])
        # # success = await restore_manager_instance.trigger_restore(options=["--type=time", "--target=\"2023-10-26 15:00:00\""])
        
        # if success:
        #     logger.info("SUCCESS: Restore command completed. Check pgBackRest logs and PostgreSQL.")
        # else:
        #     logger.error("FAILURE: Restore command did not complete successfully. Check logs.")
        logger.info("Restore trigger example complete. Restore command was NOT executed (commented out by default). ")

    except Exception as e:
        logger.error(f"Error during RestoreManager ad-hoc test: {e}", exc_info=True)
    finally:
        await restore_manager_instance.close()

if __name__ == "__main__":
    # To run: python -m gaia.validator.sync.restore_manager
    # BE EXTREMELY CAREFUL IF UNCOMMENTING THE RESTORE CALL IN _example_main_restore_adhoc
    asyncio.run(_example_main_restore_adhoc()) 