import torch
from datetime import datetime, timezone, timedelta
from typing import List, Optional
import asyncio
import traceback
from fiber.chain import interface, chain_utils, weights as w
# Note: get_nodes_for_netuid replaced with process-isolated _fetch_nodes_process_isolated
from fiber import SubstrateInterface
from fiber.chain.interface import get_substrate
import sys
import os
from dotenv import load_dotenv
from fiber.logging_utils import get_logger
import numpy as np
from gaia import __spec_version__
from gaia.validator.utils.substrate_manager import get_fresh_substrate_connection, get_process_isolated_substrate
from gaia.validator.utils.substrate_manager import get_fresh_substrate_connection, get_process_isolated_substrate

logger = get_logger(__name__)


async def get_active_validator_uids(netuid, subtensor_network="finney", chain_endpoint=None, recent_blocks=500):
    try:
        # Use process-isolated substrate for validator UID queries
        substrate = get_process_isolated_substrate(
        # Use process-isolated substrate for validator UID queries
        substrate = get_process_isolated_substrate(
            subtensor_network=subtensor_network,
            chain_endpoint=chain_endpoint or ""
        )
        logger.info("🛡️ Using process-isolated substrate for get_active_validator_uids")
        logger.info("🛡️ Using process-isolated substrate for get_active_validator_uids")
        
        loop = asyncio.get_event_loop()
        validator_permits = await loop.run_in_executor(
            None, 
            lambda: substrate.query("SubtensorModule", "ValidatorPermit", [netuid])
            lambda: substrate.query("SubtensorModule", "ValidatorPermit", [netuid])
        )
        last_update = await loop.run_in_executor(
            None,
            lambda: substrate.query("SubtensorModule", "LastUpdate", [netuid])
            lambda: substrate.query("SubtensorModule", "LastUpdate", [netuid])
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
        # Process-isolated substrate automatically cleans up when processes terminate
        logger.debug("🛡️ get_active_validator_uids complete - process isolation prevents ABC memory leaks automatically")
        # Process-isolated substrate automatically cleans up when processes terminate
        logger.debug("🛡️ get_active_validator_uids complete - process isolation prevents ABC memory leaks automatically")


class FiberWeightSetter:
    def __init__(
            self,
            netuid: int,
            wallet_name: str = "default",
            hotkey_name: str = "default",
            network: str = "finney",
            timeout: int = 30,
    ):
        """Initialize the weight setter with fiber connections"""
        self.netuid = netuid
        self.network = network
        # Use process-isolated substrate for initialization 
        logger.info("🛡️ FiberWeightSetter using process-isolated substrate for initialization")
        self.substrate = get_process_isolated_substrate(
        # Use process-isolated substrate for initialization 
        logger.info("🛡️ FiberWeightSetter using process-isolated substrate for initialization")
        self.substrate = get_process_isolated_substrate(
            subtensor_network=network,
            chain_endpoint=""  # Use default endpoint
        )
        self.nodes = None
        self.keypair = chain_utils.load_hotkey_keypair(
            wallet_name=wallet_name, hotkey_name=hotkey_name
        )
        self.timeout = timeout

    async def _fetch_nodes_process_isolated(self, netuid: int):
        """
        Fetch nodes using process-isolated substrate queries to prevent ABC memory leaks.
        This replaces the fiber library's get_nodes_for_netuid function.
        """
        try:
            logger.debug(f"🛡️ Weight setter fetching nodes for netuid {netuid} using process-isolated substrate")
            
            # Use process-isolated substrate for all queries
            substrate = get_process_isolated_substrate(
                subtensor_network=self.network,
                chain_endpoint=""  # Use default endpoint
            )
            
            # Query node data using process isolation with longer timeout for complex operations
            # Use 30s timeout since we're making many queries sequentially
            timeout = 30.0
            logger.debug(f"🛡️ Weight setter making {11} substrate queries with {timeout}s timeout each")
            
            hotkeys = substrate._run_substrate_operation("query", "SubtensorModule", "Hotkeys", [netuid], timeout)
            coldkeys = substrate._run_substrate_operation("query", "SubtensorModule", "Coldkeys", [netuid], timeout)
            stakes = substrate._run_substrate_operation("query", "SubtensorModule", "Stake", [netuid], timeout)
            trust = substrate._run_substrate_operation("query", "SubtensorModule", "Trust", [netuid], timeout)
            vtrust = substrate._run_substrate_operation("query", "SubtensorModule", "ValidatorTrust", [netuid], timeout)
            incentive = substrate._run_substrate_operation("query", "SubtensorModule", "Incentive", [netuid], timeout)
            emission = substrate._run_substrate_operation("query", "SubtensorModule", "Emission", [netuid], timeout)
            consensus = substrate._run_substrate_operation("query", "SubtensorModule", "Consensus", [netuid], timeout)
            dividends = substrate._run_substrate_operation("query", "SubtensorModule", "Dividends", [netuid], timeout)
            last_update = substrate._run_substrate_operation("query", "SubtensorModule", "LastUpdate", [netuid], timeout)
            
            # Get axon info with extended timeout
            axons = substrate._run_substrate_operation("query", "SubtensorModule", "Axons", [netuid], timeout)
            
            # Build node objects (similar to fiber's get_nodes_for_netuid)
            nodes = []
            
            if hotkeys and len(hotkeys) > 0:
                for i, hotkey in enumerate(hotkeys):
                    try:
                        # Create a Node-like object with the data
                        from types import SimpleNamespace
                        
                        node = SimpleNamespace()
                        node.node_id = i
                        node.hotkey = hotkey
                        node.coldkey = coldkeys[i] if i < len(coldkeys) else ""
                        
                        # Handle stake (might be in different format)
                        if stakes and i < len(stakes):
                            stake_value = stakes[i]
                            # Convert stake to float (handle different formats)
                            if isinstance(stake_value, dict):
                                node.stake = float(sum(stake_value.values())) if stake_value else 0.0
                            else:
                                node.stake = float(stake_value) if stake_value else 0.0
                        else:
                            node.stake = 0.0
                        
                        # Set other attributes with safe indexing
                        node.trust = float(trust[i]) if trust and i < len(trust) else 0.0
                        node.vtrust = float(vtrust[i]) if vtrust and i < len(vtrust) else 0.0
                        node.incentive = float(incentive[i]) if incentive and i < len(incentive) else 0.0
                        node.emission = float(emission[i]) if emission and i < len(emission) else 0.0
                        node.consensus = float(consensus[i]) if consensus and i < len(consensus) else 0.0
                        node.dividends = float(dividends[i]) if dividends and i < len(dividends) else 0.0
                        node.last_update = int(last_update[i]) if last_update and i < len(last_update) else 0
                        
                        # Handle axon info
                        if axons and i < len(axons):
                            axon_info = axons[i]
                            if axon_info:
                                node.ip = axon_info.get('ip', '')
                                node.port = int(axon_info.get('port', 0))
                                node.ip_type = int(axon_info.get('ip_type', 4))
                                node.protocol = int(axon_info.get('protocol', 4))
                            else:
                                node.ip = ''
                                node.port = 0
                                node.ip_type = 4
                                node.protocol = 4
                        else:
                            node.ip = ''
                            node.port = 0
                            node.ip_type = 4
                            node.protocol = 4
                        
                        nodes.append(node)
                        
                    except Exception as e:
                        logger.warning(f"Error processing weight setter node {i}: {e}")
                        continue
            
            logger.info(f"🛡️ Weight setter process-isolated node fetch completed: {len(nodes)} nodes (NO ABC memory leak)")
            return nodes
            
        except Exception as e:
            logger.error(f"Error in weight setter process-isolated node fetching: {e}")
            logger.error(traceback.format_exc())
            return []

    def cleanup(self):
        """No cleanup needed with process-isolated substrate."""
        logger.debug("🛡️ No cleanup needed - process-isolated substrate handles cleanup automatically")
        """No cleanup needed with process-isolated substrate."""
        logger.debug("🛡️ No cleanup needed - process-isolated substrate handles cleanup automatically")

    def __del__(self):
        """No cleanup needed with process-isolated substrate."""
        pass  # Process isolation handles cleanup automatically



    async def calculate_weights(self, weights: List[float] = None) -> tuple[torch.Tensor, List[int]]:
        """Convert input weights to normalized tensor with min/max bounds"""
        if weights is None:
            logger.warning("No weights provided")
            return None, []

        # Use process-isolated node fetching to prevent ABC memory leaks
        if self.nodes is None:
            nodes = await self._fetch_nodes_process_isolated(self.netuid)
        else:
            nodes = self.nodes
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

            # MEMORY LEAK PREVENTION: Use process-isolated substrate for weight setting
            # This ensures ABC objects are contained in separate processes and automatically destroyed
            logger.info("🛡️ Creating process-isolated substrate connection for weight setting")
            # MEMORY LEAK PREVENTION: Use process-isolated substrate for weight setting
            # This ensures ABC objects are contained in separate processes and automatically destroyed
            logger.info("🛡️ Creating process-isolated substrate connection for weight setting")
            
            # Use process-isolated substrate connection for weight setting
            self.substrate = get_process_isolated_substrate(
                subtensor_network=self.network,
                chain_endpoint=""  # Use default endpoint
            )
            logger.info("✅ Process-isolated substrate connection created for weight setting")
            self.nodes = await self._fetch_nodes_process_isolated(self.netuid)
            logger.info(f"Found {len(self.nodes)} nodes in subnet")

            validator_uid = self.substrate.query(
                "SubtensorModule",
                "Uids",
                [self.netuid, self.keypair.ss58_address]
            )
            )

            version_key = __spec_version__

            if validator_uid is None:
                logger.error("❗Validator not found in nodes list")
                return False

            active_validator_uids = await get_active_validator_uids(
                netuid=self.netuid, 
                subtensor_network=self.network
            )
            logger.info(f"Found {len(active_validator_uids)} active validators - zeroing their weights")
            
            weights_copy = weights.copy()
            
            for uid in active_validator_uids:
                if uid < len(weights_copy):
                    if weights_copy[uid] > 0:
                        logger.info(f"Setting validator UID {uid} weight to zero (was {weights_copy[uid]:.6f})")
                        weights_copy[uid] = 0.0

            calculated_weights, node_ids = await self.calculate_weights(weights_copy)
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
                # Process-isolated substrate automatically cleans up when processes terminate
                logger.debug("Process-isolated substrate cleanup is automatic - no manual cleanup needed")
                # Process-isolated substrate automatically cleans up when processes terminate
                logger.debug("Process-isolated substrate cleanup is automatic - no manual cleanup needed")

        except Exception as e:
            logger.error(f"Error in weight setting: {str(e)}")
            logger.error(traceback.format_exc())
            return False
        finally:
            # Process-isolated substrate automatically prevents ABC memory leaks
            # No manual cleanup needed - each operation runs in separate processes that auto-terminate
            logger.debug("🛡️ Weight setting complete - process isolation prevents ABC memory leaks automatically")
            # Process-isolated substrate automatically prevents ABC memory leaks
            # No manual cleanup needed - each operation runs in separate processes that auto-terminate
            logger.debug("🛡️ Weight setting complete - process isolation prevents ABC memory leaks automatically")

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