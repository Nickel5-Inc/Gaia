import torch
from datetime import datetime, timezone, timedelta
from typing import List, Tuple
import asyncio
from fiber.chain import interface, chain_utils, weights
import random
from fiber.chain.fetch_nodes import get_nodes_for_netuid
import sys
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class FiberWeightSetter:
    def __init__(
        self,
        netuid: int,
        wallet_name: str = "default",
        hotkey_name: str = "default",
        network: str = "finney",
    ):
        """
        Initialize the weight setter with fiber instead of bittensor
        """
        self.netuid = netuid
        self.wallet_name = wallet_name
        self.hotkey_name = hotkey_name
        self.network = network
        self.substrate = interface.get_substrate(subtensor_network=network)
        self.keypair = chain_utils.load_hotkey_keypair(
            wallet_name=wallet_name, hotkey_name=hotkey_name
        )
        self.timer = datetime.now(timezone.utc)

    def is_time_to_set_weights(self) -> bool:
        now = datetime.now(timezone.utc)
        time_diff = now - self.timer
        return time_diff >= timedelta(hours=1)

    def calculate_weights(self, n_nodes: int) -> torch.Tensor:
        random_weights = torch.tensor(
            [random.random() for _ in range(n_nodes)], dtype=torch.float32
        )  # Random scores for testing
        normalized_weights = random_weights / random_weights.sum()
        return normalized_weights

    def find_validator_uid(self, nodes) -> int:
        """Find the validator's UID from the list of nodes"""
        for node in nodes:
            if node.hotkey == self.keypair.ss58_address:
                return node.node_id
        logger.info("❗Validator not found in nodes list")
        return None

    async def set_weights(self):
        """Set random weights on the network using fiber"""
        try:
            logger.info(f"\nAttempting to set weights for subnet {self.netuid}...")

            # Get all neurons/nodes
            nodes = get_nodes_for_netuid(substrate=self.substrate, netuid=self.netuid)
            if not nodes:
                logger.error(f"❗No nodes found for subnet {self.netuid}")
                return

            # Find validator's UID from nodes list
            validator_uid = self.find_validator_uid(nodes)
            if validator_uid is None:
                logger.error("❗Failed to get validator UID")
                return

            # Generate weights
            calculated_weights = self.calculate_weights(len(nodes))

            # Get node IDs
            node_ids = [node.node_id for node in nodes]

            try:
                logger.info("\nSetting weights...")
                result = weights.set_node_weights(
                    substrate=self.substrate,
                    keypair=self.keypair,
                    node_ids=node_ids,
                    node_weights=calculated_weights.tolist(),
                    netuid=self.netuid,
                    validator_node_id=validator_uid,
                    wait_for_inclusion=True,
                    wait_for_finalization=True,
                )

                # If `result` is awaitable, await it
                if asyncio.iscoroutine(result):
                    result = await result

                if result:  # If the result is True or indicates success
                    logger.info("✅ Successfully set weights and finalized")
                else:
                    logger.error(f"❗Failed to set weights: {result}")
            except Exception as e:
                logger.error(f"❗Error setting weights: {str(e)}")
                raise
            self.timer = datetime.now(timezone.utc)

        except Exception as e:
            logger.error(f"❗Error setting weights: {str(e)}")
            import traceback

            print(traceback.format_exc())
            raise


async def main():
    try:
        weight_setter = FiberWeightSetter(
            netuid=237, wallet_name="gaiatest", hotkey_name="default", network="test"
        )

        await weight_setter.set_weights()

    except KeyboardInterrupt:
        logger.info("\nStopping...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
