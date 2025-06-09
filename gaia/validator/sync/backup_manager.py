# gaia/validator/sync/backup_manager.py
import asyncio
import os
import subprocess # For running pgBackRest
from fiber.logging_utils import get_logger
from datetime import datetime, timezone # Keep for logging or potential future use
from typing import Optional

logger = get_logger(__name__)

# pgBackRest related constants (stanza is primary)
PGBACKREST_STANZA_DEFAULT = "gaia" # Default stanza name

class BackupManager:
    def __init__(self, test_mode: bool = False): # Removed storage_manager
        """
        Manages ad-hoc pgBackRest operations.
        Scheduling of regular backups is handled by cron (see setup-primary.sh).

        Args:
            test_mode: If True, may use test-friendly options for commands.
        """
        # self.storage_manager = storage_manager # Removed, manifest not handled here
        self.test_mode = test_mode
        self.pgbackrest_stanza = os.getenv("PGBACKREST_STANZA", PGBACKREST_STANZA_DEFAULT)
        
        logger.info(f"BackupManager initialized. Stanza: {self.pgbackrest_stanza}, Test_mode: {self.test_mode}. "
                    f"Regular backup scheduling is handled by system cron via setup-primary.sh.")

    async def _trigger_pgbackrest_command(self, command_args: list[str], operation_name: str) -> bool:
        """
        Triggers a pgBackRest command.
        
        Args:
            command_args: List of arguments for the pgbackrest command.
            operation_name: Name of the operation for logging.
        
        Returns:
            True if successful, False otherwise.
        """
        # Ensure stanza is part of the command if not already included by caller
        if not any(arg.startswith("--stanza=") or arg == self.pgbackrest_stanza for arg in command_args):
             # Add stanza if it's a common command like 'backup', 'info', 'check', 'archive-push'
            if operation_name not in ["Stanza Create"]: # Stanza Create takes stanza as a positional arg
                 # Check if the command itself is the stanza name (e.g. for stanza-create)
                is_stanza_command = False
                if len(command_args) > 0 and command_args[0] == self.pgbackrest_stanza:
                    is_stanza_command = True

                if not is_stanza_command and not any(arg.startswith(f"--stanza={self.pgbackrest_stanza}") for arg in command_args):
                     # Prepend --stanza only if it's not there. Some commands might have it later.
                     # A more robust way would be to have specific methods define their args fully.
                     # For now, this is a basic check.
                     if command_args[0] != 'stanza-create': # stanza-create uses positional stanza
                        command_args = [f"--stanza={self.pgbackrest_stanza}"] + command_args


        full_command = ["pgbackrest"] + command_args
        logger.info(f"Executing pgBackRest {operation_name}: {' '.join(full_command)}")
        try:
            process = await asyncio.to_thread(
                subprocess.run,
                full_command,
                capture_output=True,
                text=True,
                check=False 
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

    async def trigger_backup(self, backup_type: str = "full") -> bool:
        """
        Triggers an ad-hoc pgBackRest backup.
        Args:
            backup_type: Type of backup ("full", "diff", "incr").
        """
        if backup_type not in ["full", "diff", "incr"]:
            logger.error(f"Invalid backup type: {backup_type}. Must be 'full', 'diff', or 'incr'.")
            return False
            
        args = [f"--stanza={self.pgbackrest_stanza}", "backup", f"--type={backup_type}"]
        if self.test_mode:
            # Example: Shorter archive timeout, no compression for faster test "backup"
            # These settings might vary depending on actual pgBackRest version and desired test behavior
            args.extend(["--archive-timeout=30s", "--compress-level=0"]) 
            logger.info(f"[Test Mode] Ad-hoc pgBackRest Backup ({backup_type}) with test options.")
        
        return await self._trigger_pgbackrest_command(args, f"Ad-hoc Backup ({backup_type})")

    async def trigger_archive_push(self) -> bool:
        """
        Triggers an ad-hoc pgBackRest archive-push command.
        This is generally not needed if archive_command in postgresql.conf is set correctly,
        but can be useful for flushing any pending WAL segments if archive_async is enabled.
        """
        args = [f"--stanza={self.pgbackrest_stanza}", "archive-push"]
        return await self._trigger_pgbackrest_command(args, "Ad-hoc Archive Push")

    async def check_stanza_config(self) -> bool:
        """
        Runs `pgbackrest check --stanza=xxx`
        """
        args = [f"--stanza={self.pgbackrest_stanza}", "check"]
        return await self._trigger_pgbackrest_command(args, "Stanza Check")

    async def get_info(self) -> bool: # Returns bool for consistency, output is logged
        """
        Runs `pgbackrest info --stanza=xxx`
        """
        args = [f"--stanza={self.pgbackrest_stanza}", "info"]
        return await self._trigger_pgbackrest_command(args, "Info")
        
    async def create_stanza_if_needed(self) -> bool:
        """
        Attempts to create the stanza. Idempotent if stanza already exists.
        This is typically done by setup-primary.sh but can be called ad-hoc.
        Note: requires pgBackRest to be configured to communicate with R2.
        """
        # Stanza create does not take --stanza=, it's a positional argument
        args = ["stanza-create", self.pgbackrest_stanza]
        # Check if we need to pass repo type etc, or if global config is enough
        # The setup-primary.sh does `sudo -u postgres pgbackrest --stanza="$STANZA_NAME" stanza-create`
        # which relies on the global config in /etc/pgbackrest/pgbackrest.conf for repo details.
        # So, this should be fine.
        return await self._trigger_pgbackrest_command(args, "Stanza Create")


    # Removed start_periodic_backups and related scheduling methods as cron handles this.
    # Removed pg_dump related methods and constants.
    # Removed manifest related logic.

    async def close(self):
        # No external resources like storage_manager to close for this version.
        logger.info("BackupManager closed.")

async def get_backup_manager(test_mode: bool = False) -> Optional[BackupManager]: # Removed storage_manager argument
    """
    Factory function to create a BackupManager instance.
    """
    if not os.getenv("PGBACKREST_STANZA"):
        logger.warning(f"PGBACKREST_STANZA environment variable not set. Using default: {PGBACKREST_STANZA_DEFAULT}. "\
                       "BackupManager might not function correctly if this doesn't match the actual stanza.")
    
    # Could add a check to see if pgbackrest CLI is available
    try:
        process = await asyncio.to_thread(
            subprocess.run,
            ["pgbackrest", "version"], # Simple command to check availability
            capture_output=True, text=True, check=False
        )
        if process.returncode != 0:
            logger.error(f"pgBackRest command-line tool may not be available or configured. Error: {process.stderr}")
            # return None # Decide if this should be a fatal error for the manager
    except FileNotFoundError:
        logger.error(f"pgBackRest command-line tool not found in PATH. BackupManager will likely fail.")
        # return None # Decide if this should be a fatal error for the manager
        
    logger.info(f"Creating BackupManager. Test mode: {test_mode}")
    return BackupManager(test_mode=test_mode)


# Example Usage (for testing ad-hoc commands)
async def _example_main_backup_adhoc():
    logger.info("Testing BackupManager (Ad-hoc Commands)...")
    
    # This example assumes pgBackRest is configured and the stanza exists (e.g., via setup-primary.sh)
    # It also assumes the necessary PGBACKREST_STANZA env var is set or default is used.

    backup_manager_instance = await get_backup_manager(test_mode=True)
    if not backup_manager_instance:
        logger.error("Failed to initialize BackupManager for ad-hoc test.")
        return
    
    try:
        logger.info("Attempting to get pgBackRest info...")
        await backup_manager_instance.get_info()
        
        logger.info("\nAttempting to run pgBackRest check...")
        await backup_manager_instance.check_stanza_config()

        # Example: Trigger an ad-hoc backup (ensure this is safe in your test environment)
        # logger.info("\nAttempting to trigger an ad-hoc FULL backup (test mode)...")
        # success = await backup_manager_instance.trigger_backup(backup_type="full")
        # if success:
        #     logger.info("SUCCESS: Ad-hoc full backup command completed.")
        # else:
        #     logger.error("FAILURE: Ad-hoc full backup command did not complete successfully.")

    except Exception as e:
        logger.error(f"Error during BackupManager ad-hoc test: {e}", exc_info=True)
    finally:
        await backup_manager_instance.close()

if __name__ == "__main__":
    # To run: python -m gaia.validator.sync.backup_manager
    asyncio.run(_example_main_backup_adhoc()) 