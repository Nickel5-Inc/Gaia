import torch
from datetime import datetime, timezone, timedelta
from typing import List
import asyncio
import traceback
from fiber.chain import interface, chain_utils, weights as w
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
        timeout: int = 30,
    ):
        """Initialize the weight setter with fiber"""
        self.netuid = netuid
        self.network = network
        self.substrate = interface.get_substrate(subtensor_network=network)
        self.keypair = chain_utils.load_hotkey_keypair(
            wallet_name=wallet_name, hotkey_name=hotkey_name
        )
        self.timeout = timeout

    def calculate_weights(self, weights: List[float] = None) -> torch.Tensor:
        """Convert input weights to normalized tensor with min/max bounds"""
        if weights is None:
            logger.warning("No weights provided")
            return None

        nodes = get_nodes_for_netuid(substrate=self.substrate, netuid=self.netuid)
        node_ids = [node.node_id for node in nodes]

        aligned_weights = [
            max(0.0, weights[node_id]) if node_id < len(weights) else 0.0
            for node_id in node_ids
        ]

        weights_tensor = torch.tensor(aligned_weights, dtype=torch.float32)
        active_nodes = (weights_tensor > 0).sum().item()

        if active_nodes == 0:
            logger.warning("No active nodes found")
            return None

        weights_tensor /= weights_tensor.sum()
        min_weight = 1.0 / (2 * active_nodes)
        max_weight = 0.5  # Maximum 50% weight for any node

        mask = weights_tensor > 0
        weights_tensor[mask] = torch.clamp(weights_tensor[mask], min_weight, max_weight)
        weights_tensor /= weights_tensor.sum()

        logger.info(
            f"Weight distribution stats:"
            f"\n- Active nodes: {active_nodes}"
            f"\n- Max weight: {weights_tensor.max().item():.4f}"
            f"\n- Min non-zero weight: {weights_tensor[weights_tensor > 0].min().item():.4f}"
            f"\n- Total weight: {weights_tensor.sum().item():.4f}"
        )

        return weights_tensor, node_ids

    async def set_weights(self, weights: List[float] = None) -> bool:
        """Set weights on chain"""
        try:
            if weights is None:
                logger.info("No weights provided - skipping weight setting")
                return False

            logger.info(f"\nSetting weights for subnet {self.netuid}...")
            
            self.substrate = interface.get_substrate(subtensor_network=self.network)
            nodes = get_nodes_for_netuid(substrate=self.substrate, netuid=self.netuid)
            logger.info(f"Found {len(nodes)} nodes in subnet")

            validator_uid = self.substrate.query(
                "SubtensorModule", 
                "Uids", 
                [self.netuid, self.keypair.ss58_address]
            ).value
            
            version_key = self.substrate.query(
                "SubtensorModule", 
                "WeightsVersionKey", 
                [self.netuid]
            ).value
            
            if validator_uid is None:
                logger.error("❗Validator not found in nodes list")
                return False

            calculated_weights, node_ids = self.calculate_weights(weights)
            if calculated_weights is None:
                return False

            try:
                await self._async_set_node_weights(
                    substrate=self.substrate,
                    keypair=self.keypair,
                    node_ids=[node.node_id for node in nodes],
                    node_weights=calculated_weights.tolist(),
                    netuid=self.netuid,
                    validator_node_id=validator_uid,
                    version_key=version_key,
                    wait_for_inclusion=False,
                    wait_for_finalization=False,
                )
                logger.info("Weight commit initiated, continuing...")
                return True

            except Exception as e:
                logger.error(f"Error initiating weight commit: {str(e)}")
                return False

        except Exception as e:
            logger.error(f"Error in weight setting: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    async def _async_set_node_weights(self, **kwargs):
        """Async wrapper for the synchronous set_node_weights function"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: w.set_node_weights(**kwargs))

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
