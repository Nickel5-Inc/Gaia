from datetime import datetime, timezone, timedelta
import os
import asyncio
import traceback
from typing import Any, Optional, List, Dict
from dotenv import load_dotenv
import httpx
import pandas as pd
import json
import base64
from fiber.chain import chain_utils
from fiber.logging_utils import get_logger
from fiber.validator import client as vali_client
from fiber.chain.metagraph import Metagraph
from substrateinterface import SubstrateInterface
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask
from gaia.validator.database.database_manager import ValidatorDatabaseManager
from gaia.validator.work_persistence import WorkPersistenceManager
from gaia.validator.weights.set_weights import FiberWeightSetter
from gaia.validator.utils.auto_updater import perform_update
from gaia.validator.flows.scoring_flow import ScoringFlow

logger = get_logger(__name__)

class GaiaValidator:
    """Main validator class that orchestrates all validator operations.
    
    Responsibilities:
    - Initialize and manage all components
    - Set up and maintain network connections
    - Manage task execution and scheduling
    - Handle system updates and maintenance
    """
    
    def __init__(self, args):
        """Initialize the validator with all necessary components."""
        self.args = args
        self.metagraph = None
        self.config = None
        
        # Initialize managers
        self.database_manager = ValidatorDatabaseManager()
        self.work_manager = WorkPersistenceManager(self.database_manager)
        
        # Initialize tasks
        self.soil_task = SoilMoistureTask(
            db_manager=self.database_manager,
            node_type="validator",
            test_mode=args.test_soil
        )
        self.geomagnetic_task = GeomagneticTask(
            db_manager=self.database_manager
        )
        
        # Initialize chain components
        self.weights = [0.0] * 256
        self.last_set_weights_block = 0
        self.current_block = 0
        self.nodes = {}  # Initialize the in-memory node table state
        
        # Initialize network client
        self.httpx_client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            verify=False,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            ),
            transport=httpx.AsyncHTTPTransport(retries=3),
        )

    def setup_neuron(self) -> bool:
        """Set up the neuron with necessary configurations and connections."""
        try:
            load_dotenv(".env")
            self.netuid = (
                self.args.netuid if self.args.netuid else int(os.getenv("NETUID", 237))
            )
            logger.info(f"Using netuid: {self.netuid}")

            self.subtensor_chain_endpoint = (
                self.args.subtensor.chain_endpoint
                if hasattr(self.args, "subtensor")
                and hasattr(self.args.subtensor, "chain_endpoint")
                else os.getenv(
                    "SUBTENSOR_ADDRESS", "wss://test.finney.opentensor.ai:443/"
                )
            )

            self.subtensor_network = (
                self.args.subtensor.network
                if hasattr(self.args, "subtensor")
                and hasattr(self.args.subtensor, "network")
                else os.getenv("SUBTENSOR_NETWORK", "test")
            )

            self.wallet_name = (
                self.args.wallet
                if self.args.wallet
                else os.getenv("WALLET_NAME", "default")
            )
            self.hotkey_name = (
                self.args.hotkey
                if self.args.hotkey
                else os.getenv("HOTKEY_NAME", "default")
            )
            self.keypair = chain_utils.load_hotkey_keypair(
                self.wallet_name, self.hotkey_name
            )

            self.substrate = SubstrateInterface(url=self.subtensor_chain_endpoint)
            self.metagraph = Metagraph(substrate=self.substrate, netuid=self.netuid)

            self.current_block = self.substrate.get_block()["header"]["number"]
            self.last_set_weights_block = self.current_block - 300

            # Initialize flows after neuron setup
            self.scoring_flow = ScoringFlow(
                work_manager=self.work_manager,
                geomagnetic_task=self.geomagnetic_task,
                soil_task=self.soil_task,
                weight_setter=FiberWeightSetter(
                    netuid=self.netuid,
                    wallet_name=self.wallet_name,
                    hotkey_name=self.hotkey_name,
                    network=self.subtensor_network,
                    last_set_block=self.last_set_weights_block,
                    current_block=self.current_block
                )
            ).create_flow()

            return True
        except Exception as e:
            logger.error(f"Error setting up neuron: {e}")
            return False

    def custom_serializer(self, obj):
        """Custom JSON serializer for handling datetime objects and bytes."""
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return {
                "_type": "bytes",
                "encoding": "base64",
                "data": base64.b64encode(obj).decode("ascii"),
            }
        raise TypeError(f"Type {type(obj)} not serializable")

    async def update_miner_table(self):
        """Update the miner table with the latest miner information from the metagraph."""
        try:
            if self.metagraph is None:
                logger.error("Metagraph not initialized")
                return

            self.metagraph.sync_nodes()
            logger.info(f"Synced {len(self.metagraph.nodes)} nodes from the network")

            for index, (hotkey, node) in enumerate(self.metagraph.nodes.items()):
                await self.database_manager.update_miner_info(
                    index=index,  # Use the enumerated index
                    hotkey=node.hotkey,
                    coldkey=node.coldkey,
                    ip=node.ip,
                    ip_type=str(node.ip_type),
                    port=node.port,
                    incentive=float(node.incentive),
                    stake=float(node.stake),
                    trust=float(node.trust),
                    vtrust=float(node.vtrust),
                    protocol=str(node.protocol),
                )
                self.nodes[index] = {"hotkey": node.hotkey, "uid": index}
                logger.debug(f"Updated information for node {index}")

            logger.info("Successfully updated miner table and in-memory state")

        except Exception as e:
            logger.error(f"Error updating miner table: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def handle_miner_deregistration_loop(self):
        """Run miner deregistration checks every 60 seconds."""
        while True:
            try:
                self.metagraph.sync_nodes()
                active_miners = {
                    idx: {"hotkey": hotkey, "uid": idx}
                    for idx, (hotkey, _) in enumerate(self.metagraph.nodes.items())
                }

                if not self.nodes:
                    query = "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL"
                    rows = await self.database_manager.fetch_many(query)
                    self.nodes = {
                        row["uid"]: {"hotkey": row["hotkey"], "uid": row["uid"]}
                        for row in rows
                    }

                deregistered_miners = []
                for uid, registered in self.nodes.items():
                    if active_miners[uid]["hotkey"] != registered["hotkey"]:
                        deregistered_miners.append(registered)

                if deregistered_miners:
                    logger.info(f"Found {len(deregistered_miners)} deregistered miners:")
                    for miner in deregistered_miners:
                        logger.info(
                            f"UID {miner['uid']}: {miner['hotkey']} -> {active_miners[miner['uid']]['hotkey']}"
                        )

                    for miner in deregistered_miners:
                        self.nodes[miner["uid"]]["hotkey"] = active_miners[
                            miner["uid"]
                        ]["hotkey"]

                    uids = [int(miner["uid"]) for miner in deregistered_miners]

                    for idx in uids:
                        self.weights[idx] = 0.0

                    await self.soil_task.recalculate_recent_scores(uids)
                    await self.geomagnetic_task.recalculate_recent_scores(uids)

                    logger.info(f"Processed {len(deregistered_miners)} deregistered miners")
                else:
                    logger.debug("No deregistered miners found")

            except Exception as e:
                logger.error(f"Error in deregistration loop: {e}")
                logger.error(traceback.format_exc())
            finally:
                await asyncio.sleep(60)

    async def initialize(self):
        """Initialize all validator components."""
        logger.info("Setting up neuron...")
        if not self.setup_neuron():
            logger.error("Failed to setup neuron, exiting...")
            return False

        logger.info("Checking metagraph initialization...")
        if self.metagraph is None:
            logger.error("Metagraph not initialized, exiting...")
            return False

        logger.info("Initializing work persistence...")
        await self.work_manager.initialize()

        logger.info("Initializing database tables...")
        await self.database_manager.initialize_database()

        logger.info("Updating miner table...")
        await self.update_miner_table()
        
        return True

    async def run(self):
        """Main execution using Prefect flows."""
        if not await self.initialize():
            return

        try:
            # Initialize all flows
            flows = {
                "scoring": self.scoring_flow,
                "geomagnetic": self.geomagnetic_task.create_flow(self),
                "soil_moisture": self.soil_task.create_flow(self),
                "maintenance": self.create_maintenance_flow()
            }

            # Deploy all flows
            for flow_name, flow in flows.items():
                deployment = await flow.to_deployment(
                    name=f"{flow_name}-{self.subtensor_network}",
                    work_pool_name=f"gaia-validator-{self.subtensor_network}",
                    schedule=flow.default_schedule
                )
                await deployment.apply()
                logger.info(f"Deployed {flow_name} flow")

            # Start the Prefect agent to execute the flows
            from prefect.agent import start_agent
            await start_agent(
                work_pool_name=f"gaia-validator-{self.subtensor_network}",
                prefetch_seconds=60,
                limit=10
            )

        except Exception as e:
            logger.error(f"Error in Prefect flow execution: {e}")
            logger.error(traceback.format_exc())

    def create_maintenance_flow(self):
        """Create a flow for maintenance tasks like deregistration checks and updates."""
        from prefect import flow, task, get_run_logger
        from prefect.tasks import task_input_hash
        from datetime import timedelta

        @task(
            retries=3,
            retry_delay_seconds=60,
            cache_key_fn=task_input_hash,
            cache_expiration=timedelta(minutes=5)
        )
        async def check_deregistrations():
            await self.handle_miner_deregistration_loop()

        @task(
            retries=3,
            retry_delay_seconds=300,
            cache_key_fn=task_input_hash,
            cache_expiration=timedelta(hours=1)
        )
        async def check_updates():
            await self.check_for_updates()

        @task(
            retries=3,
            retry_delay_seconds=60,
            cache_key_fn=task_input_hash,
            cache_expiration=timedelta(minutes=5)
        )
        async def log_status():
            await self.status_logger()

        @flow(
            name="maintenance-flow",
            description="Handles system maintenance tasks",
            version=os.getenv("GAIA_VERSION", "0.0.1")
        )
        async def maintenance_flow():
            flow_logger = get_run_logger()
            flow_logger.info("Starting maintenance tasks")
            
            await asyncio.gather(
                check_deregistrations(),
                check_updates(),
                log_status()
            )

        # Set default schedule for maintenance flow
        maintenance_flow.default_schedule = "*/5 * * * *"  # Every 5 minutes
        return maintenance_flow

    async def check_for_updates(self):
        """Check for and apply system updates."""
        try:
            await perform_update()
        except Exception as e:
            logger.error(f"Error checking for updates: {e}")
            logger.error(traceback.format_exc())
        finally:
            await asyncio.sleep(3600)  # Check every hour

    async def status_logger(self):
        """Log validator status periodically."""
        while True:
            try:
                current_time_utc = datetime.now(timezone.utc)
                formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")

                try:
                    block = self.substrate.get_block()
                    self.current_block = block["header"]["number"]
                    blocks_since_weights = (
                        self.current_block - self.last_set_weights_block
                    )
                except Exception as block_error:
                    logger.error(f"Error getting block: {block_error}")
                    try:
                        self.substrate = SubstrateInterface(
                            url=self.subtensor_chain_endpoint
                        )
                    except Exception as e:
                        logger.error(f"Failed to reconnect to substrate: {e}")

                active_nodes = len(self.metagraph.nodes) if self.metagraph else 0

                logger.info(
                    f"\n"
                    f"---Status Update ---\n"
                    f"Time (UTC): {formatted_time} | \n"
                    f"Block: {self.current_block} | \n"
                    f"Nodes: {active_nodes}/256 | \n"
                    f"Weights Set: {blocks_since_weights} blocks ago"
                )

            except Exception as e:
                logger.error(f"Error in status logger: {e}")
                logger.error(traceback.format_exc())
            finally:
                await asyncio.sleep(60)
