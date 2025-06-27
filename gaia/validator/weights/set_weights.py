import torch
from datetime import datetime, timezone, timedelta
from typing import List, Optional
import asyncio
import traceback
from fiber.chain import interface, chain_utils, weights as w
from fiber.chain.fetch_nodes import get_nodes_for_netuid
from fiber import SubstrateInterface
from fiber.chain.interface import get_substrate
import sys
import os
from dotenv import load_dotenv
from fiber.logging_utils import get_logger
import numpy as np
from gaia import __spec_version__
from gaia.validator.utils.substrate_manager import get_process_isolated_substrate

logger = get_logger(__name__)


async def get_active_validator_uids(netuid, subtensor_network="finney", chain_endpoint=None, recent_blocks=500):
    try:
        # Use process isolated substrate for consistency
        substrate = get_process_isolated_substrate(
            subtensor_network=subtensor_network,
            chain_endpoint=chain_endpoint or ""
        )
        
        loop = asyncio.get_event_loop()
        
        # Get validator permits - nodes with vpermit == 1 are validators
        validator_permits = await loop.run_in_executor(
            None, 
            lambda: substrate.query("SubtensorModule", "ValidatorPermit", [netuid])
        )
        
        # Get last update times for all nodes
        last_update = await loop.run_in_executor(
            None,
            lambda: substrate.query("SubtensorModule", "LastUpdate", [netuid])
        )
        
        # Get current block number using rpc_request (consistent with validator.py pattern)
        current_block = await loop.run_in_executor(
            None,
            lambda: int(substrate.rpc_request("chain_getHeader", [])["result"]["number"], 16)
        )
        
        # Find active validators: vpermit == 1 AND recently updated
        active_validators = [
            uid for uid, permit in enumerate(validator_permits)
            if permit == 1 and uid < len(last_update) and (current_block - last_update[uid]) < recent_blocks
        ]
        
        logger.info(f"Found {len(active_validators)} active validators for netuid {netuid}")
        return active_validators
    except Exception as e:
        logger.error(f"Error getting active validators: {e}")
        logger.error(traceback.format_exc())
        return []


class FiberWeightSetter:
    def __init__(
            self,
            netuid: int,
            wallet_name: str = "default",
            hotkey_name: str = "default",
            network: str = "finney",
            timeout: int = 30,
    ):
        """Initialize the weight setter with process-isolated substrate as a drop-in replacement"""
        self.netuid = netuid
        self.network = network
        # Use process isolated substrate - acts as drop-in replacement for standard substrate
        self.substrate = get_process_isolated_substrate(
            subtensor_network=network,
            chain_endpoint=""
        )
        self.nodes = None
        self.keypair = chain_utils.load_hotkey_keypair(
            wallet_name=wallet_name, hotkey_name=hotkey_name
        )
        self.timeout = timeout

    def cleanup(self):
        """Process isolated substrate handles cleanup automatically"""
        pass

    def __del__(self):
        """Process isolated substrate handles cleanup automatically"""
        pass



    async def calculate_weights(self, weights: List[float] = None) -> tuple[torch.Tensor, List[int]]:
        """Convert input weights to normalized tensor with min/max bounds"""
        if weights is None:
            logger.warning("No weights provided")
            return None, []

        # Use standard fiber function but with process-isolated substrate
        if self.nodes is None:
            try:
                # The process isolated substrate acts as a drop-in replacement
                self.nodes = get_nodes_for_netuid(substrate=self.substrate, netuid=self.netuid)
                logger.info(f"Successfully fetched {len(self.nodes)} nodes")
            except Exception as e:
                logger.error(f"Failed to fetch nodes: {e}")
                logger.error("Substrate network appears to be completely broken - cannot set weights without node data")
                return None, []
        
        node_ids = [node.node_id for node in self.nodes]


        aligned_weights = [
            max(0.0, weights[node_id]) if node_id < len(weights) else 0.0
            for node_id in node_ids
        ]

        weights_tensor = torch.tensor(aligned_weights, dtype=torch.float32)
        active_nodes = (weights_tensor > 0).sum().item()

        if active_nodes == 0:
            logger.warning("No active nodes found")
            # Return zero weights tensor instead of None to maintain consistent return type
            zero_weights = torch.zeros(len(node_ids), dtype=torch.float32)
            return zero_weights, node_ids

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

    async def set_weights(self, weights: List[float] = None) -> bool:
        """Set weights on chain using standard fiber approach with process isolation"""
        try:
            if weights is None:
                logger.info("No weights provided - skipping weight setting")
                return False

            logger.info(f"Setting weights for subnet {self.netuid}...")
            
            # Get active validators to zero out their weights (with timeout)
            try:
                active_validator_uids = await asyncio.wait_for(
                    get_active_validator_uids(
                        netuid=self.netuid, 
                        subtensor_network=self.network
                    ),
                    timeout=30.0  # 30 second timeout for active validator detection
                )
                logger.info(f"Found {len(active_validator_uids)} active validators - zeroing their weights")
            except asyncio.TimeoutError:
                logger.warning("Active validator detection timed out after 30s - substrate network may be overloaded")
                logger.warning("Continuing weight setting without active validator detection")
                active_validator_uids = []
            except Exception as e:
                logger.error(f"Failed to get active validators: {e}")
                logger.warning("Continuing weight setting without active validator detection")
                active_validator_uids = []

            # Copy weights and zero out validators
            weights_copy = weights.copy()
            for uid in active_validator_uids:
                if uid < len(weights_copy):
                    if weights_copy[uid] > 0:
                        logger.info(f"Setting validator UID {uid} weight to zero (was {weights_copy[uid]:.6f})")
                        weights_copy[uid] = 0.0

            # Calculate final weights (this includes node fetching)
            calculated_weights, node_ids = await self.calculate_weights(weights_copy)
            if calculated_weights is None:
                logger.error("Failed to calculate weights - substrate network may be completely broken")
                logger.error("Skipping weight setting until network recovers")
                return False

            # Get validator UID (with timeout)
            try:
                validator_uid = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: self.substrate.query("SubtensorModule", "Uids", [self.netuid, self.keypair.ss58_address])
                    ),
                    timeout=30.0  # 30 second timeout
                )
            except asyncio.TimeoutError:
                logger.error("Validator UID query timed out after 30s - substrate network is completely broken")
                return False
            except Exception as e:
                logger.error(f"Failed to get validator UID: {e}")
                return False

            version_key = __spec_version__

            # Set weights using standard fiber function (with timeout)
            try:
                logger.info(f"Setting weights for {len(self.nodes)} nodes")
                await asyncio.wait_for(
                    self._async_set_node_weights(
                        substrate=self.substrate,
                        keypair=self.keypair,
                        node_ids=[node.node_id for node in self.nodes],
                        node_weights=calculated_weights.tolist(),
                        netuid=self.netuid,
                        validator_node_id=validator_uid,
                        version_key=version_key,
                        wait_for_inclusion=True,
                        wait_for_finalization=False,
                    ),
                    timeout=120.0  # 2 minute timeout for weight transaction
                )
                logger.info("✅ Weight commit completed successfully")
                return True

            except asyncio.TimeoutError:
                logger.error("⏰ Weight transaction timed out after 2 minutes - substrate network is severely overloaded")
                logger.error("Weight setting will be retried in next cycle when network improves")
                return False
            except Exception as e:
                logger.error(f"Error setting weights: {str(e)}")
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
        load_dotenv()
        
        network = os.getenv("SUBTENSOR_NETWORK", "test")
        netuid = int(os.getenv("NETUID", "237"))
        wallet_name = os.getenv("WALLET_NAME", "test_val")
        hotkey_name = os.getenv("HOTKEY_NAME", "test_val_hot")
        
        logger.info(f"Network configuration from .env: {network} (netuid: {netuid})")
        
        weight_setter = FiberWeightSetter(
            netuid=netuid,
            wallet_name=wallet_name, 
            hotkey_name=hotkey_name, 
            network=network
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