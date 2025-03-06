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

        nodes = get_nodes_for_netuid(substrate=self.substrate,
                                     netuid=self.netuid) if self.nodes is None else self.nodes
        node_ids = [node.node_id for node in nodes]

        aligned_weights = [max(0.0, weights[node_id]) if node_id < len(weights) else 0.0 for node_id in node_ids]
        weights_tensor = torch.tensor(aligned_weights, dtype=torch.float32)

        active_nodes = (weights_tensor > 0).sum().item()
        if active_nodes == 0:
            logger.warning("⚠️ No active nodes found. Skipping weight setting.")
            return None

        weights_tensor /= weights_tensor.sum()

        # Log weight concentration issues
        warning_threshold = 0.5
        high_weight_nodes = torch.where(weights_tensor > warning_threshold)[0]
        for idx in high_weight_nodes:
            logger.warning(f"⚠️ Node {idx} weight {weights_tensor[idx]:.6f} exceeds 50% of total weight.")

        # Analyzing weight distribution
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

            # Compute proper percentiles
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
        """Ensures UID 244 gets 40-50% dynamically and sets weights on chain."""
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
            if calculated_weights is None:
                return False

            HARDCODED_UID = 244
            MIN_PERCENTAGE = 0.40
            MAX_PERCENTAGE = 0.50

            if isinstance(calculated_weights, torch.Tensor):
                weights_tensor = calculated_weights.clone().detach().to(dtype=torch.float32)
            else:
                weights_tensor = torch.tensor(calculated_weights, dtype=torch.float32)

            if HARDCODED_UID in node_ids:
                uid_244_index = node_ids.index(HARDCODED_UID)
                uid_244_initial_weight = weights_tensor[uid_244_index].item()

                below_median_mask = weights_tensor < np.median(weights_tensor)
                total_below_median_weight = torch.sum(weights_tensor[below_median_mask])

                amount_to_take = min(MAX_PERCENTAGE, total_below_median_weight.item())
                amount_to_take = max(MIN_PERCENTAGE, amount_to_take)

                if total_below_median_weight.item() > 0:
                    actual_amount_to_take = min(amount_to_take, total_below_median_weight.item())
                    scale_factor = (total_below_median_weight.item() - actual_amount_to_take) / total_below_median_weight.item()
                    weights_tensor[below_median_mask] *= scale_factor
                    
                    if actual_amount_to_take < amount_to_take:
                        logger.warning(f"Could only take {actual_amount_to_take:.6f} from below-median miners (requested {amount_to_take:.6f})")
                    
                    weights_tensor[uid_244_index] += actual_amount_to_take
                    
                weights_tensor = torch.clamp(weights_tensor, min=0.0)
                weights_tensor /= weights_tensor.sum()

                logger.info(
                    f"✅ UID {HARDCODED_UID} weight before: {uid_244_initial_weight:.6f}, after: {weights_tensor[uid_244_index]:.6f}")
                logger.info(f"⚖️ Amount taken from below-median miners: {actual_amount_to_take:.6f}")

            try:
                logger.info(f"Setting weights for {len(self.nodes)} nodes")
                await self._async_set_node_weights(
                    substrate=self.substrate,
                    keypair=self.keypair,
                    node_ids=node_ids,
                    node_weights=weights_tensor.tolist(),
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
