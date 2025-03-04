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


    def calculate_weights(self, weights: List[float] = None) -> torch.Tensor:
        """Convert input weights to normalized tensor with min/max bounds"""
        if weights is None:
            logger.warning("No weights provided")
            return None

        nodes = get_nodes_for_netuid(substrate=self.substrate, netuid=self.netuid) if self.nodes is None else self.nodes
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

        logger.info(f"Raw computed weights before normalization: {weights}")

        weights_tensor /= weights_tensor.sum()
        warning_threshold = 0.5  # Warn if any node exceeds 50% of total weight

        # Check for concerning weight concentration
        if torch.any(weights_tensor > warning_threshold):
            high_weight_nodes = torch.where(weights_tensor > warning_threshold)[0]
            for idx in high_weight_nodes:
                logger.warning(f"Node {idx} weight {weights_tensor[idx]:.6f} exceeds 50% of total weight - potential reward concentration issue")

        # Final chain-side distribution analysis before setting weights
        non_zero_weights = weights_tensor[weights_tensor > 0].numpy()
        if len(non_zero_weights) > 0:
            total_weight = weights_tensor.sum().item()
            sorted_weights = np.sort(non_zero_weights)[::-1]  # Sort in descending order
            cumulative_weights = np.cumsum(sorted_weights)
            
            stats = {
                'mean': float(np.mean(non_zero_weights)),
                'median': float(np.median(non_zero_weights)),
                'std': float(np.std(non_zero_weights)),
                'min': float(np.min(non_zero_weights)),
                'max': float(np.max(non_zero_weights)),
                'count': len(non_zero_weights),
                'zero_count': len(weights_tensor) - len(non_zero_weights)
            }
            
            # Calculate percentiles in 10% increments
            percentile_points = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
            percentiles = np.percentile(non_zero_weights, percentile_points)
            percentile_indices = [int(p * len(sorted_weights) / 100) for p in percentile_points]
            
            logger.info("\nFinal Weight Distribution Analysis (after min/max bounds):")
            logger.info(f"Mean: {stats['mean']:.6f}")
            logger.info(f"Median: {stats['median']:.6f}")
            logger.info(f"Std Dev: {stats['std']:.6f}")
            logger.info(f"Min: {stats['min']:.6f}")
            logger.info(f"Max: {stats['max']:.6f}")
            logger.info(f"Non-zero weights: {stats['count']}")
            logger.info(f"Zero weights: {stats['zero_count']}")
            
            logger.info("\nPercentile Analysis (for non-zero weights, sorted by performance):")
            logger.info(f"{'Performance':>10} {'Weight':>12} {'Avg/Node':>12} {'Pool Share %':>12} {'Cumulative %':>12} {'Node Count':>10}")
            logger.info("-" * 70)
            
            for i in range(len(percentile_points)-1):
                start_idx = percentile_indices[i]
                end_idx = percentile_indices[i+1]
                if i == len(percentile_points)-2:  # Last segment
                    end_idx = len(sorted_weights)
                    
                segment_weights = sorted_weights[start_idx:end_idx]
                segment_total = float(np.sum(segment_weights))
                nodes_in_segment = len(segment_weights)
                avg_weight_per_node = segment_total / nodes_in_segment if nodes_in_segment > 0 else 0
                pool_share = (segment_total / total_weight) * 100
                cumulative_share = (cumulative_weights[end_idx-1] / total_weight) * 100 if end_idx > 0 else 0
                
                # Convert to "Top X%" format
                top_start = 100 - percentile_points[i]
                top_end = 100 - percentile_points[i+1]
                
                logger.info(f"Top {top_start:>3}-{top_end:<6} "
                          f"{percentiles[i]:>12.6f} "
                          f"{avg_weight_per_node:>12.6f} "
                          f"{pool_share:>12.2f}% "
                          f"{cumulative_share:>12.2f}% "
                          f"{nodes_in_segment:>10}")

        logger.info(
            f"Weight distribution stats:"
            f"\n- Active nodes: {active_nodes}"
            f"\n- Max weight: {weights_tensor.max().item():.4f}"
            f"\n- Min non-zero weight: {weights_tensor[weights_tensor > 0].min().item():.4f}"
            f"\n- Total weight: {weights_tensor.sum().item():.4f}"
        )
        logger.info(f"Weights tensor: {weights_tensor}")

        return weights_tensor, node_ids

    async def set_weights(self, weights: list[float] = None) -> bool:
        """Set weights on chain with 50% allocation to UID 244 and detailed logs."""

        try:
            if weights is None:
                logger.info("No weights provided - skipping weight setting")
                return False

            logger.info(f"🔄 Setting weights for subnet {self.netuid}...")

            # Retrieve network and node info from Fiber
            self.substrate = interface.get_substrate(subtensor_network=self.network)
            self.nodes = get_nodes_for_netuid(substrate=self.substrate, netuid=self.netuid)
            logger.info(f"✅ Found {len(self.nodes)} nodes in subnet")

            validator_uid = self.substrate.query(
                "SubtensorModule",
                "Uids",
                [self.netuid, self.wallet_name]  # Using wallet_name as the validator reference
            ).value

            version_key = __spec_version__

            if validator_uid is None:
                logger.error("❗Validator not found in nodes list")
                return False

            # Get calculated weights
            calculated_weights, node_ids = self.calculate_weights(weights)
            if calculated_weights is None:
                return False

            # 💥 Ensure UID 244 receives 50% emissions 💥
            HARDCODED_UID = 244  # The miner receiving 50%
            HARDCODED_PERCENTAGE = 0.50  # Fixed 50% allocation

            if HARDCODED_UID not in node_ids:
                logger.warning(f"⚠️ UID {HARDCODED_UID} not found in node list. Skipping hardcoded allocation.")
            else:
                # Convert list to tensor for manipulation
                weights_tensor = torch.tensor(calculated_weights, dtype=torch.float32)

                # Find UID 244's index in node_ids
                uid_244_index = node_ids.index(HARDCODED_UID)

                # Log initial weight of UID 244 before modification
                initial_uid_244_weight = weights_tensor[uid_244_index].item()
                logger.info(f"🔍 Before allocation: UID {HARDCODED_UID} had weight {initial_uid_244_weight:.6f}")

                # Scale other weights down proportionally
                remaining_percentage = 1.0 - HARDCODED_PERCENTAGE
                total_other_weight = sum(weights_tensor) - weights_tensor[uid_244_index]

                if total_other_weight > 0:
                    scale_factor = remaining_percentage / total_other_weight
                    for idx, uid in enumerate(node_ids):
                        if uid != HARDCODED_UID:
                            weights_tensor[idx] *= scale_factor

                # Assign exactly 50% to UID 244
                weights_tensor[uid_244_index] = HARDCODED_PERCENTAGE

                # Log the updated weight assigned to UID 244
                final_uid_244_weight = weights_tensor[uid_244_index].item()
                logger.info(f"✅ After allocation: UID {HARDCODED_UID} now has weight {final_uid_244_weight:.6f}")

                # Final normalization to ensure sum = 1
                weights_tensor /= weights_tensor.sum()

            # Convert back to list for Fiber submission
            list_weights = weights_tensor.tolist()
            logger.info(f"⚖️ Final computed weights: {list_weights}")

            # ✅ Continue with weight setting process
            try:
                logger.info(f"🚀 Setting weights for {len(self.nodes)} nodes...")
                await self._async_set_node_weights(
                    substrate=self.substrate,
                    keypair=self.keypair,
                    node_ids=[node.node_id for node in self.nodes],
                    node_weights=list_weights,
                    netuid=self.netuid,
                    validator_node_id=validator_uid,
                    version_key=version_key,
                    wait_for_inclusion=True,
                    wait_for_finalization=False,
                )
                logger.info("✅ Weight commit initiated, continuing...")
                return True

            except Exception as e:
                logger.error(f"❗Error initiating weight commit: {str(e)}")
                return False

        except Exception as e:
            logger.error(f"❗Error in weight setting: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    async def _async_set_node_weights(self, **kwargs):
        """Async wrapper for the synchronous set_node_weights function with timeout"""
        try:
            loop = asyncio.get_event_loop()
            return await asyncio.wait_for(
                loop.run_in_executor(None, lambda: w.set_node_weights(**kwargs)),
                timeout=340
            )
        except asyncio.TimeoutError:
            logger.error("Weight setting timed out after 120 seconds")
            raise
        except Exception as e:
            logger.error(f"Error in weight setting: {str(e)}")
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