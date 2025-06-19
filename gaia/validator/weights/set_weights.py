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

logger = get_logger(__name__)


async def get_active_validator_uids(netuid, substrate_manager=None, subtensor_network="finney", chain_endpoint=None, recent_blocks=500):
    try:
        # Use managed connection if available, otherwise create a new one
        if substrate_manager:
            substrate = substrate_manager.get_connection()
        else:
            substrate = SubstrateInterface(url=chain_endpoint) if chain_endpoint else get_substrate(subtensor_network=subtensor_network)
        
        loop = asyncio.get_event_loop()
        validator_permits = await loop.run_in_executor(
            None, 
            lambda: substrate.query("SubtensorModule", "ValidatorPermit", [netuid]).value
        )
        last_update = await loop.run_in_executor(
            None,
            lambda: substrate.query("SubtensorModule", "LastUpdate", [netuid]).value
        )
        current_block = await loop.run_in_executor(
            None,
            lambda: int(substrate.get_block()["header"]["number"])
        )
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
    finally:
        # Clean up substrate connection if we created it locally
        if not substrate_manager and 'substrate' in locals():
            try:
                substrate.close()
            except Exception as cleanup_error:
                logger.debug(f"Error cleaning up substrate connection: {cleanup_error}")


class FiberWeightSetter:
    def __init__(
            self,
            netuid: int,
            wallet_name: str = "default",
            hotkey_name: str = "default",
            network: str = "finney",
            timeout: int = 30,
            substrate_manager=None,
    ):
        """Initialize the weight setter with fiber and optional substrate manager"""
        self.netuid = netuid
        self.network = network
        self.substrate_manager = substrate_manager
        # Always prefer substrate manager to prevent memory leaks
        if self.substrate_manager:
            self.substrate = self.substrate_manager.get_connection()
        else:
            # This should rarely happen in production - log warning
            logger.warning("Creating unmanaged substrate connection in FiberWeightSetter.__init__ - potential memory leak")
            self.substrate = interface.get_substrate(subtensor_network=network)
        self.nodes = None
        self.keypair = chain_utils.load_hotkey_keypair(
            wallet_name=wallet_name, hotkey_name=hotkey_name
        )
        self.timeout = timeout

    def cleanup(self):
        """Clean up substrate connection if it's unmanaged."""
        if not self.substrate_manager and hasattr(self, 'substrate') and self.substrate:
            try:
                logger.debug("Cleaning up unmanaged substrate connection in FiberWeightSetter")
                self.substrate.close()
                self.substrate = None
            except Exception as e:
                logger.debug(f"Error cleaning up substrate connection: {e}")

    def __del__(self):
        """Destructor to ensure cleanup happens when object is destroyed."""
        try:
            self.cleanup()
        except Exception:
            pass  # Ignore errors in destructor



    def calculate_weights(self, weights: List[float] = None) -> tuple[torch.Tensor, List[int]]:
        """Convert input weights to normalized tensor with min/max bounds"""
        if weights is None:
            logger.warning("No weights provided")
            return None, []

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
        """Set weights on chain"""
        try:
            if weights is None:
                logger.info("No weights provided - skipping weight setting")
                return False

            logger.info(f"\nSetting weights for subnet {self.netuid}...")

            # SAFETY MEASURE: Always create a fresh substrate connection for weight setting
            # This ensures we have the most current blockchain state and aren't affected by
            # any potential caching or staleness issues in managed connections
            logger.info("Creating fresh substrate connection for weight setting (bypassing managed connection)")
            
            # Clean up any existing connection first
            if hasattr(self, 'substrate') and self.substrate:
                try:
                    self.substrate.close()
                except Exception as e:
                    logger.debug(f"Error closing old connection before fresh creation: {e}")
            
            # Create completely fresh substrate connection
            self.substrate = interface.get_substrate(subtensor_network=self.network)
            logger.info("✅ Fresh substrate connection created for weight setting")
            self.nodes = get_nodes_for_netuid(substrate=self.substrate, netuid=self.netuid)
            logger.info(f"Found {len(self.nodes)} nodes in subnet")

            validator_uid = self.substrate.query(
                "SubtensorModule",
                "Uids",
                [self.netuid, self.keypair.ss58_address]
            ).value

            version_key = __spec_version__

            if validator_uid is None:
                logger.error("❗Validator not found in nodes list")
                return False

            active_validator_uids = await get_active_validator_uids(
                netuid=self.netuid, 
                substrate_manager=None,  # Force fresh connection instead of managed
                subtensor_network=self.network
            )
            logger.info(f"Found {len(active_validator_uids)} active validators - zeroing their weights")
            
            weights_copy = weights.copy()
            
            for uid in active_validator_uids:
                if uid < len(weights_copy):
                    if weights_copy[uid] > 0:
                        logger.info(f"Setting validator UID {uid} weight to zero (was {weights_copy[uid]:.6f})")
                        weights_copy[uid] = 0.0

            calculated_weights, node_ids = self.calculate_weights(weights_copy)
            if calculated_weights is None:
                logger.warning("No weights calculated - skipping weight setting")
                return False

            # Log if all weights are zero but still proceed to set them
            if torch.is_tensor(calculated_weights) and calculated_weights.sum().item() == 0:
                logger.info("All calculated weights are zero - proceeding to set zero weights on-chain")

            try:
                logger.info(f"Setting weights for {len(self.nodes)} nodes")
                await self._async_set_node_weights(
                    substrate=self.substrate,
                    keypair=self.keypair,
                    node_ids=[node.node_id for node in self.nodes],
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
            finally:
                # Clean up the fresh substrate connection to prevent memory leaks
                if hasattr(self, 'substrate') and self.substrate:
                    try:
                        logger.debug("Cleaning up fresh substrate connection after weight setting")
                        self.substrate.close()
                        self.substrate = None
                    except Exception as cleanup_e:
                        logger.debug(f"Error cleaning up fresh substrate connection: {cleanup_e}")

        except Exception as e:
            logger.error(f"Error in weight setting: {str(e)}")
            logger.error(traceback.format_exc())
            return False
        finally:
            # Ensure cleanup happens even if outer exception occurs
            if hasattr(self, 'substrate') and self.substrate:
                try:
                    logger.debug("Final cleanup of fresh substrate connection")
                    self.substrate.close()
                    self.substrate = None
                except Exception as cleanup_e:
                    logger.debug(f"Error in final substrate cleanup: {cleanup_e}")

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