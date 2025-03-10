import torch
from datetime import datetime, timezone, timedelta
from typing import List
import asyncio
import traceback
from fiber.chain import interface, chain_utils, weights as w
from fiber.chain.fetch_nodes import get_nodes_for_netuid
import sys
from fiber.logging_utils import get_logger
import numpy as np
from gaia import __spec_version__

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
        """Initialize the weight setter with Fiber"""
        self.netuid = netuid
        self.network = network
        self.substrate = interface.get_substrate(subtensor_network=network)
        self.nodes = None
        self.keypair = chain_utils.load_hotkey_keypair(
            wallet_name=wallet_name, hotkey_name=hotkey_name
        )
        self.timeout = timeout

    def calculate_weights(self, weights: list[float] = None) -> torch.Tensor:
        """Normalizes and analyzes miner weights before setting them on-chain."""
        if weights is None:
            logger.warning("⚠️ No weights provided. Skipping weight setting.")
            return None

        self.nodes = get_nodes_for_netuid(substrate=self.substrate, netuid=self.netuid)

        validator_uids = []
        for node in self.nodes:
            is_validator = False
            if hasattr(node, 'vtrust') and node.vtrust > 0:
                is_validator = True
            elif hasattr(node, 'is_validator') and node.is_validator:
                is_validator = True
            
            if is_validator:
                validator_uids.append(node.node_id)
                logger.info(f"Identified validator: UID {node.node_id}, hotkey {node.hotkey}")
        
        if not validator_uids:
            logger.warning("No validators identified from node properties. This is unusual.")
            try:
                validator_uid = self.substrate.query(
                    "SubtensorModule", 
                    "Uids", 
                    [self.netuid, self.keypair.ss58_address]
                ).value
                if validator_uid is not None:
                    validator_uids.append(validator_uid)
                    logger.info(f"Added our own validator UID as fallback: {validator_uid}")
            except Exception as e:
                logger.error(f"Could not identify our own validator UID: {e}")

        logger.info(f"Excluding {len(validator_uids)} validators from weight setting: {validator_uids}")
        
        self.nodes = [node for node in self.nodes if node.node_id not in validator_uids]
        self.nodes = sorted(self.nodes, key=lambda node: node.node_id)
        node_ids = [node.node_id for node in self.nodes]

        if not node_ids:
            logger.warning("⚠️ All nodes were validators. No miners to set weights for.")
            empty_tensor = torch.tensor([], dtype=torch.float32)
            return empty_tensor, []

        aligned_weights = [max(0.0, weights[node_id]) if node_id < len(weights) else 0.0 for node_id in node_ids]
        weights_tensor = torch.tensor(aligned_weights, dtype=torch.float32)

        active_nodes = (weights_tensor > 0).sum().item()
        if active_nodes == 0:
            logger.warning("⚠️ No active nodes found. Skipping weight setting.")
            return weights_tensor, node_ids

        if weights_tensor.sum() > 0:
            weights_tensor /= weights_tensor.sum()

        warning_threshold = 0.5
        high_weight_nodes = torch.where(weights_tensor > warning_threshold)[0]
        for idx in high_weight_nodes:
            logger.warning(f"⚠️ Node {idx} weight {weights_tensor[idx]:.6f} exceeds 50% of total weight.")

        non_zero_weights = weights_tensor[weights_tensor > 0].numpy()
        total_weight = weights_tensor.sum().item()

        if len(non_zero_weights) > 0:
            sorted_weights = np.sort(non_zero_weights)[::-1]
            cumulative_weights = np.cumsum(sorted_weights)

            stats = {
                'mean': float(np.mean(non_zero_weights)),
                'median': float(np.median(non_zero_weights)),
                'std': float(np.std(non_zero_weights)),
                'min': float(np.min(non_zero_weights)),
                'max': float(np.max(non_zero_weights)),
                'count': len(non_zero_weights),
                'zero_count': len(weights_tensor) - len(non_zero_weights),
            }

            logger.info("\n📊 **Final Weight Distribution Analysis:**")
            for key, value in stats.items():
                logger.info(f"✅ {key.capitalize()}: {value:.6f}")

            percentile_intervals = np.linspace(0, 100, 11)
            percentiles = np.percentile(non_zero_weights, percentile_intervals)

            logger.info("\n📉 **Percentile Analysis:**")
            logger.info(
                f"{'Percentile':>10} {'Weight':>12} {'Avg/Node':>12} {'Pool Share %':>12} {'Cumulative %':>12}")
            logger.info("-" * 70)

            for i in range(len(percentile_intervals) - 1):
                start_percentile = 100 - percentile_intervals[i]
                end_percentile = 100 - percentile_intervals[i + 1]

                segment_weights = sorted_weights[
                                  int(len(sorted_weights) * (1 - percentile_intervals[i + 1] / 100)):
                                  int(len(sorted_weights) * (1 - percentile_intervals[i] / 100))
                                  ]

                if len(segment_weights) == 0:
                    continue

                segment_total = np.sum(segment_weights)
                avg_weight = segment_total / len(segment_weights)
                pool_share = (segment_total / total_weight) * 100
                cumulative_share = (cumulative_weights[int(len(sorted_weights) * (
                        1 - percentile_intervals[i + 1] / 100))] / total_weight) * 100

                logger.info(
                    f"Top {start_percentile:>3}-{end_percentile:<3} {percentiles[i]:>12.6f} {avg_weight:>12.6f} {pool_share:>12.2f}% {cumulative_share:>12.2f}%")

        return weights_tensor, node_ids

    async def set_weights(self, weights: list[float] = None) -> bool:
        try:
            if weights is None:
                logger.info("No weights provided - skipping weight setting.")
                return False

            logger.info(f"🔄 Setting weights for subnet {self.netuid}...")
            self.substrate = interface.get_substrate(subtensor_network=self.network)
            self.nodes = get_nodes_for_netuid(substrate=self.substrate, netuid=self.netuid)
            logger.info(f"✅ Found {len(self.nodes)} nodes in subnet.")

            validator_uid = self.substrate.query(
                "SubtensorModule",
                "Uids",
                [self.netuid, self.keypair.ss58_address]
            ).value

            if validator_uid is None:
                logger.error("❗Validator not found in nodes list")
                return False

            version_key = __spec_version__

            calculated_weights, node_ids = self.calculate_weights(weights)
            if calculated_weights is None or len(calculated_weights) == 0:
                logger.warning("No valid weights to set. Skipping weight setting.")
                return False

            HARDCODED_UID = 244
            MAX_PERCENTAGE = 0.30
            BOTTOM_PERCENT = 0.5

            if node_ids and HARDCODED_UID in node_ids:
                uid_244_index = node_ids.index(HARDCODED_UID)
                uid_244_initial_weight = calculated_weights[uid_244_index].item()

                precision = 8
                rounded_weights = torch.round(calculated_weights * 10**precision) / 10**precision
                
                weight_tuples = [(i, node_ids[i], rounded_weights[i].item()) for i in range(len(node_ids)) if node_ids[i] != HARDCODED_UID]
                
                weight_tuples.sort(key=lambda x: (x[2], x[1]))
                
                bottom_count = int(len(weight_tuples) * BOTTOM_PERCENT)
                bottom_indices = [t[0] for t in weight_tuples[:bottom_count]]
                
                bottom_mask = torch.zeros_like(calculated_weights, dtype=torch.bool)
                for idx in bottom_indices:
                    bottom_mask[idx] = True
                
                total_bottom_weight = calculated_weights[bottom_mask].sum()
                
                if total_bottom_weight.item() > 0:
                    actual_amount_to_take = min(MAX_PERCENTAGE, total_bottom_weight.item())
                    
                    calculated_weights[uid_244_index] += actual_amount_to_take
                    
                    calculated_weights[bottom_mask] = 0.0

                    calculated_weights = torch.clamp(calculated_weights, min=0.0)
                    calculated_weights /= calculated_weights.sum()

                    logger.info(
                        f"✅ UID {HARDCODED_UID} weight before: {uid_244_initial_weight:.6f}, after: {calculated_weights[uid_244_index]:.6f}"
                    )
                    logger.info(f"⚖️ Amount taken from bottom {BOTTOM_PERCENT*100}% miners: {actual_amount_to_take:.6f}")
                    logger.info(f"🔄 Number of bottom miners set to zero: {bottom_mask.sum().item()}")

            try:
                logger.info(f"Setting weights for {len(self.nodes)} nodes")
                await self._async_set_node_weights(
                    substrate=self.substrate,
                    keypair=self.keypair,
                    node_ids=node_ids,
                    node_weights=calculated_weights.tolist(),
                    netuid=self.netuid,
                    validator_node_id=validator_uid,
                    version_key=version_key,
                    wait_for_inclusion=True,
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
        """Async wrapper for setting weights with timeout"""
        try:
            loop = asyncio.get_event_loop()
            return await asyncio.wait_for(
                loop.run_in_executor(None, lambda: w.set_node_weights(**kwargs)),
                timeout=340
            )
        except asyncio.TimeoutError:
            logger.error("❌ Weight setting timed out after 120 seconds.")
            raise
        except Exception as e:
            logger.error(f"❌ Error in weight setting: {str(e)}")
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
