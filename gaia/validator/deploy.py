#!/usr/bin/env python3
import os
import sys
import asyncio
import subprocess
import time
from pathlib import Path
from typing import Optional, List
import yaml
from prefect.client import get_client
from prefect.deployments import Deployment
from prefect.infrastructure import Process
from prefect.filesystems import LocalFileSystem
from prefect.utilities.asyncutils import sync_compatible
from dotenv import load_dotenv
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class PrefectDeploymentManager:
    """Manages Prefect deployments for Gaia validator."""

    def __init__(self):
        load_dotenv()
        self.network = os.getenv("SUBTENSOR_NETWORK", "test")
        self.base_path = Path(__file__).parent
        self.work_pools_file = self.base_path / "prefect-work-pools.yaml"
        self.deployment_file = self.base_path / "prefect-deployment.yaml"
        self.processes: List[subprocess.Popen] = []

    def check_env_vars(self) -> bool:
        """Check if all required environment variables are set."""
        required_vars = [
            "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT",
            "WALLET_NAME", "HOTKEY_NAME", "NETUID", "SUBTENSOR_NETWORK",
            "SUBTENSOR_ADDRESS", "EARTHDATA_USERNAME", "EARTHDATA_PASSWORD",
            "EARTHDATA_API_KEY"
        ]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            logger.error(f"Missing required environment variables: {', '.join(missing)}")
            return False
        logger.info("All required environment variables are set")
        return True

    def _load_yaml(self, file_path: Path) -> dict:
        """Load and process YAML file with environment variable substitution."""
        with open(file_path, 'r') as f:
            content = f.read()
            # Replace environment variables
            content = content.replace("${NETWORK}", self.network)
            return yaml.safe_load(content)

    async def setup_work_pools(self):
        """Set up work pools from configuration."""
        try:
            config = self._load_yaml(self.work_pools_file)
            async with get_client() as client:
                for pool in config["work-pools"]:
                    try:
                        await client.create_work_pool(
                            name=pool["name"],
                            work_pool_type=pool["type"],
                            base_job_template=pool["base_job_template"],
                            description=pool.get("description", ""),
                            concurrency_limit=pool.get("scheduling", {}).get("max_concurrent_jobs", 10)
                        )
                        logger.info(f"Created/updated work pool: {pool['name']}")
                    except Exception as e:
                        logger.error(f"Error setting up work pool {pool['name']}: {e}")
                        raise

        except Exception as e:
            logger.error(f"Error loading work pool configuration: {e}")
            raise

    async def create_deployments(self):
        """Create deployments from configuration."""
        try:
            config = self._load_yaml(self.deployment_file)
            async with get_client() as client:
                for deploy_config in config["deployments"]:
                    try:
                        # Create deployment
                        deployment = await client.create_deployment(
                            name=deploy_config["name"],
                            flow_name=deploy_config["flow_name"],
                            work_pool_name=deploy_config["work_pool"],
                            tags=deploy_config.get("tags", []),
                            parameters=deploy_config.get("parameters", {}),
                            schedule=deploy_config.get("schedule"),
                            description=deploy_config.get("description", "")
                        )
                        logger.info(f"Created/updated deployment: {deploy_config['name']}")
                    except Exception as e:
                        logger.error(f"Error creating deployment {deploy_config['name']}: {e}")
                        raise

        except Exception as e:
            logger.error(f"Error loading deployment configuration: {e}")
            raise

    async def ensure_prefect_running(self):
        """Ensure Prefect server is running."""
        try:
            # Check if server is running
            result = subprocess.run(
                ["prefect", "server", "status"],
                capture_output=True,
                text=True
            )
            
            if "Server is not running" in result.stdout:
                logger.info("Starting Prefect server...")
                # Start server in background
                server_proc = subprocess.Popen(
                    ["prefect", "server", "start"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                self.processes.append(server_proc)
                # Wait for server to start
                await asyncio.sleep(5)
            else:
                logger.info("Prefect server is already running")

        except Exception as e:
            logger.error(f"Error managing Prefect server: {e}")
            raise

    async def start_worker(self):
        """Start Prefect worker for our work pool."""
        try:
            worker_proc = subprocess.Popen(
                ["prefect", "worker", "start", "-p", f"gaia-validator-{self.network}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.processes.append(worker_proc)
            logger.info(f"Started worker for pool: gaia-validator-{self.network}")
        except Exception as e:
            logger.error(f"Error starting worker: {e}")
            raise

    async def monitor_processes(self):
        """Monitor all running processes."""
        while True:
            for i, proc in enumerate(self.processes):
                if proc.poll() is not None:
                    logger.error(f"Process {i} died unexpectedly")
                    return False
            await asyncio.sleep(5)

    def cleanup(self):
        """Clean up all running processes."""
        logger.info("Cleaning up processes...")
        for proc in self.processes:
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()

    async def deploy(self):
        """Run full deployment process."""
        try:
            # Check environment variables first
            if not self.check_env_vars():
                raise ValueError("Missing required environment variables")

            # Ensure server is running
            await self.ensure_prefect_running()
            
            # Set up work pools
            await self.setup_work_pools()
            
            # Create deployments
            await self.create_deployments()

            # Start worker
            await self.start_worker()
            
            logger.info(f"Successfully deployed Gaia validator for network: {self.network}")
            
            # Monitor processes
            await self.monitor_processes()
            
        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            raise
        finally:
            self.cleanup()

async def main():
    """Main deployment function."""
    deployer = PrefectDeploymentManager()
    try:
        await deployer.deploy()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal...")
    except Exception as e:
        logger.error(f"Error during deployment: {e}")
    finally:
        deployer.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 