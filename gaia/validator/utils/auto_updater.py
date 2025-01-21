import subprocess
import traceback
import os
import json
import configparser
import time
import asyncio
import sys
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


async def check_and_install_dependencies():
    """Check and install required dependencies before code execution"""
    try:
        # Core dependencies needed for update process
        core_deps = {
            "pip": "--upgrade pip",
            "setuptools": "--upgrade setuptools",
            "wheel": "--upgrade wheel",
        }

        for package, install_args in core_deps.items():
            try:
                await asyncio.to_thread(
                    lambda: subprocess.check_call(
                        [sys.executable, "-m", "pip", "show", package],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                )
            except subprocess.CalledProcessError:
                logger.info(f"Installing core dependency: {package}")
                try:
                    await asyncio.to_thread(
                        lambda: subprocess.check_call(
                            [sys.executable, "-m", "pip", "install"]
                            + install_args.split(),
                            stdout=subprocess.DEVNULL,
                        )
                    )
                except subprocess.CalledProcessError as e:
                    logger.error(f"Failed to install {package}: {e}")
                    return False

        return True
    except Exception as e:
        logger.error(f"Error in dependency check: {e}")
        return False


async def install_requirements():
    """Install project requirements"""
    try:
        requirements_path = Path("requirements.txt")
        if not requirements_path.exists():
            logger.error("requirements.txt not found")
            return False

        logger.info("Installing project requirements...")
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "pip",
            "install",
            "-r",
            "requirements.txt",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            logger.error(f"Failed to install requirements: {stderr.decode()}")
            return False

        return True
    except Exception as e:
        logger.error(f"Error installing requirements: {e}")
        return False


async def get_current_branch():
    try:
        branch = await asyncio.to_thread(
            lambda: subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"]
            )
            .decode()
            .strip()
        )
        return branch
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get current git branch: {e}")
        return None


async def get_remote_hash(branch):
    try:
        remote_hash = await asyncio.to_thread(
            lambda: subprocess.check_output(["git", "rev-parse", f"origin/{branch}"])
            .decode()
            .strip()
        )
        return remote_hash
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get remote hash for branch {branch}: {e}")
        return None


async def get_local_hash():
    try:
        local_hash = await asyncio.to_thread(
            lambda: subprocess.check_output(["git", "rev-parse", "HEAD"])
            .decode()
            .strip()
        )
        return local_hash
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get local git hash: {e}")
        return None


async def pull_latest_changes():
    try:
        current_branch = await get_current_branch()
        if not current_branch:
            logger.error("Failed to get current branch.")
            return False

        # Fetch the latest changes
        await asyncio.to_thread(
            lambda: subprocess.check_call(["git", "fetch", "origin"])
        )

        # Get the hash of the current HEAD
        local_hash = await asyncio.to_thread(
            lambda: subprocess.check_output(["git", "rev-parse", "HEAD"])
            .decode()
            .strip()
        )

        # Get the hash of the remote branch
        remote_hash = await asyncio.to_thread(
            lambda: subprocess.check_output(
                ["git", "rev-parse", f"origin/{current_branch}"]
            )
            .decode()
            .strip()
        )

        # Compare the hashes
        if local_hash == remote_hash:
            logger.info("No new changes to pull.")
            return False

        # Stash any changes
        await asyncio.to_thread(lambda: subprocess.check_call(["git", "stash"]))

        # If hashes are different, pull the changes
        await asyncio.to_thread(
            lambda: subprocess.check_call(["git", "pull", "origin", current_branch])
        )

        # Get the new local hash after pulling
        new_local_hash = await asyncio.to_thread(
            lambda: subprocess.check_output(["git", "rev-parse", "HEAD"])
            .decode()
            .strip()
        )

        # Verify that changes were actually pulled
        if new_local_hash != local_hash:
            logger.info(
                f"Successfully pulled latest changes from {current_branch} branch."
            )
            return True
        else:
            logger.warning("Git pull was executed, but no changes were applied.")
            return False

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to pull latest changes: {e}")
        return False


async def get_pm2_process_name():
    try:
        # Get the PM2 process ID from the environment variable
        pm_id = os.environ.get("pm_id")

        if pm_id is None:
            return None  # Not running under PM2

        # Use PM2 to get process info
        result = await asyncio.to_thread(
            lambda: subprocess.run(["pm2", "jlist"], capture_output=True, text=True)
        )
        processes = json.loads(result.stdout)

        # Find the process with matching pm_id
        for process in processes:
            if str(process.get("pm_id")) == pm_id:
                return process.get("name")

        return None  # Process not found
    except Exception as e:
        print(f"Error getting PM2 process name: {e}")
        return None


async def perform_update(validator):
    """Enhanced update process with dependency management"""
    logger.info("Starting update process...")

    # First check and install core dependencies
    if not await check_and_install_dependencies():
        logger.error("Failed to install core dependencies")
        return False

    if await pull_latest_changes():
        try:
            # Install updated requirements
            if not await install_requirements():
                logger.error("Failed to install updated requirements")
                return False

            # Check if scoring reset is needed
            if check_version_and_reset():
                try:
                    await validator.reset_scoring_system()
                    logger.info("Scoring system reset successfully")

                    config = configparser.ConfigParser()
                    config.read("setup.cfg")
                    config.set("metadata", "reset_validator_scores", "False")
                    with open("setup.cfg", "w") as f:
                        config.write(f)

                except Exception as e:
                    logger.error(f"Failed to reset scoring system: {e}")
                    logger.error(traceback.format_exc())

            # Handle PM2 restart
            process_name = await get_pm2_process_name()
            if process_name:
                logger.info(f"Restarting PM2 process: {process_name}")
                success = await restart_pm2_process(process_name)
                if not success:
                    logger.error("Failed to restart via PM2")
                    return False
            else:
                logger.warning("Not running under PM2, manual restart may be required")

            return True

        except Exception as e:
            logger.error(f"Error during update process: {e}")
            logger.error(traceback.format_exc())
            return False

    return False


async def restart_pm2_process(process_name):
    """Handle PM2 process restart with retries"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # First send SIGTERM to allow graceful shutdown
            logger.info("Sending SIGTERM for graceful shutdown...")
            term_proc = await asyncio.create_subprocess_exec(
                "pm2", "sendSignal", "SIGTERM", process_name,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Wait for process to indicate cleanup is done via its status file
            cleanup_file = "/tmp/validator_cleanup_done"
            max_wait = 60  # Maximum seconds to wait for cleanup
            wait_interval = 1
            for _ in range(max_wait // wait_interval):
                if os.path.exists(cleanup_file):
                    logger.info("Detected cleanup completion flag, proceeding with restart")
                    try:
                        os.remove(cleanup_file)  # Clean up the file
                    except Exception as e:
                        logger.warning(f"Could not remove cleanup file: {e}")
                    break
                await asyncio.sleep(wait_interval)
            else:
                logger.warning("Cleanup completion not detected after timeout, proceeding with restart")
            
            # Then do the restart
            logger.info(f"Restarting process {process_name}...")
            restart_proc = await asyncio.create_subprocess_exec(
                "pm2", "restart", process_name, "--update-env",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await restart_proc.communicate()
            
            if restart_proc.returncode == 0:
                logger.info(f"PM2 restart successful on attempt {attempt + 1}")
                return True
            
            logger.warning(f"PM2 restart failed on attempt {attempt + 1}: {stderr.decode()}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5 * (attempt + 1))
            
        except Exception as e:
            logger.error(f"Error during PM2 restart attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5 * (attempt + 1))
            continue
    
    return False


def check_version_and_reset():
    config = configparser.ConfigParser()
    try:
        config.read("setup.cfg")
        reset_scores = config.getboolean(
            "metadata", "reset_validator_scores", fallback=False
        )
        return reset_scores
    except configparser.Error as e:
        logger.error(f"Error reading setup.cfg: {e}")
        return False
