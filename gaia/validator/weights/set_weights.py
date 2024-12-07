import torch
from datetime import datetime, timezone, timedelta
from typing import List, Tuple
import asyncio
import traceback
from fiber.chain import interface, chain_utils, weights as w
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
        last_set_block: int = None,
        current_block: int = None
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
        self.last_set_block = last_set_block
        self.current_block = current_block

    def is_time_to_set_weights(self) -> bool:
        """Check if enough blocks have passed since last weight setting"""
        if self.last_set_block is None or self.current_block is None:
            return True
            
        blocks_passed = self.current_block - self.last_set_block
        if blocks_passed < 300:
            logger.info(f"Need to wait {300 - blocks_passed} more blocks")
            logger.info(f"Current block: {self.current_block}")
            logger.info(f"Last set block: {self.last_set_block}")
            return False
            
        return True

    def calculate_weights(self, n_nodes: int, weights: List[float] = None) -> torch.Tensor:
        """Convert input weights to normalized tensor."""
        if weights is None:
            logger.warning("No weights provided")
            return None
  
        nodes = get_nodes_for_netuid(substrate=self.substrate, netuid=self.netuid)
        node_ids = [node.node_id for node in nodes]
        aligned_weights = []
        for node_id in node_ids:
            if node_id < len(weights):
                aligned_weights.append(max(0.0, weights[node_id]))
            else:
                aligned_weights.append(0.0)

        weights_tensor = torch.tensor(aligned_weights, dtype=torch.float32)
        active_nodes = (weights_tensor > 0).sum().item()
        
        if active_nodes == 0:
            logger.warning("No active nodes found")
            return None
        
        weights_tensor = weights_tensor / weights_tensor.sum()
        
        min_weight = 1.0 / (2 * active_nodes)
        max_weight = 0.5  # Maximum 50% weight for any node

        mask = weights_tensor > 0
        weights_tensor[mask] = torch.clamp(weights_tensor[mask], min_weight, max_weight)
        weights_tensor = weights_tensor / weights_tensor.sum()  # Renormalize
        
        logger.info(f"Weight distribution stats:"
                    f"\n- Active nodes: {active_nodes}"
                    f"\n- Max weight: {weights_tensor.max().item():.4f}"
                    f"\n- Min non-zero weight: {weights_tensor[weights_tensor > 0].min().item():.4f}"
                    f"\n- Total weight: {weights_tensor.sum().item():.4f}")
        
        return weights_tensor

    def find_validator_uid(self, nodes) -> int:
        """Find the validator's UID from the list of nodes"""
        for node in nodes:
            if node.hotkey == self.keypair.ss58_address:
                return node.node_id
        logger.info("❗Validator not found in nodes list")
        return None

    async def set_weights(self, weights: List[float] = None):
        """Set weights on the network using fiber"""
        try:
            if weights is None:
                logger.info("No weights provided - skipping weight setting")
                return False
            
            logger.info(f"\nAttempting to set weights for subnet {self.netuid}...")
            logger.debug(f"Input weights: {weights[:10]}...")  # Show first 10 weights
            
            nodes = get_nodes_for_netuid(substrate=self.substrate, netuid=self.netuid)
            if not nodes:
                logger.error(f"❗No nodes found for subnet {self.netuid}")
                return False
            logger.debug(f"Found {len(nodes)} nodes in subnet")
            
            validator_uid = self.find_validator_uid(nodes)
            if validator_uid is None:
                logger.error("❗Failed to get validator UID")
                return False
            logger.info(f"Validator UID: {validator_uid}")
            
            calculated_weights = self.calculate_weights(len(nodes), weights)
            if calculated_weights is None:
                logger.info("No valid weights to set")
                return False
            
            logger.info(f"Calculated weights: {calculated_weights[:10]}...")
            node_ids = [node.node_id for node in nodes]
            logger.info(f"Node IDs: {node_ids}")
            
            if not self.is_time_to_set_weights():
                logger.warning("Not enough time has passed since last weight setting")
                return False
            
            logger.info("Setting weights on chain...")
            result = w.set_node_weights(
                substrate=self.substrate,
                keypair=self.keypair,
                node_ids=node_ids,
                node_weights=calculated_weights.tolist(),
                netuid=self.netuid,
                validator_node_id=validator_uid,
                wait_for_inclusion=True,
                wait_for_finalization=True,
            )
            
            if result:
                logger.info("✅ Successfully set weights and finalized")
                self.last_set_block = self.current_block
                return True
            else:
                logger.error(f"❗Failed to set weights: {result}")
                logger.info(f"Last weight set time: {self.last_set_block}")
                logger.info(f"Time since last set: {self.current_block - self.last_set_block}")
                return False
            
        except Exception as e:
            logger.error(f"❗Error setting weights: {str(e)}")
            logger.error(traceback.format_exc())
            return False


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
