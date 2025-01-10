from datetime import datetime, timezone, timedelta
import math
import traceback
from typing import List, Optional, Dict
import asyncio
import time

from fiber.logging_utils import get_logger
from fiber.chain.weights import blocks_since_last_update, min_interval_to_set_weights, can_set_weights
from fiber.chain.metagraph import Metagraph
from substrateinterface import SubstrateInterface
from gaia.validator.utils.task_decorator import task_step

logger = get_logger(__name__)

class WeightManager:
    """
    Manages weight calculations, normalization, and setting for the validator.
    
    This class handles:
    1. Weight calculation from task scores
    2. Weight normalization and ranking
    3. Weight setting permissions and timing
    4. Chain interaction for weight updates
    """
    
    def __init__(
        self,
        database_manager,
        metagraph: Metagraph,
        substrate: SubstrateInterface,
        netuid: int,
        wallet_name: str,
        hotkey_name: str,
        network: str
    ):
        self.database_manager = database_manager
        self.metagraph = metagraph
        self.substrate = substrate
        self.netuid = netuid
        self.wallet_name = wallet_name
        self.hotkey_name = hotkey_name
        self.network = network
        self.last_weights_block = 0
        
        # State
        self.weights = [0.0] * 256
        self.last_set_weights_block = 0
        self.last_successful_weight_set = time.time()

    @task_step(
        name="fetch_task_scores",
        description="Fetch recent task scores from database",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _fetch_task_scores(self):
        """
        Fetch recent scores for both tasks from the database.
        
        Returns:
            tuple: (geomagnetic_scores, soil_scores) lists of scores
        """
        three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
        
        query = """
        SELECT score 
        FROM score_table 
        WHERE task_name = :task_name
        AND created_at >= :start_time
        ORDER BY created_at DESC 
        LIMIT 1
        """
        
        try:
            # Get geomagnetic scores
            geomagnetic_result = await self.database_manager.fetch_one(
                query=query,
                params={"task_name": "geomagnetic", "start_time": three_days_ago}
            )
            
            # Get soil moisture scores
            soil_result = await self.database_manager.fetch_one(
                query=query,
                params={"task_name": "soil_moisture", "start_time": three_days_ago}
            )

            geomagnetic_scores = geomagnetic_result["score"] if geomagnetic_result else [float("nan")] * 256
            soil_scores = soil_result["score"] if soil_result else [float("nan")] * 256
            
            return geomagnetic_scores, soil_scores
            
        except Exception as e:
            logger.error(f"Error fetching task scores: {e}")
            return [float("nan")] * 256, [float("nan")] * 256

    @task_step(
        name="calculate_aggregate_weights",
        description="Calculate aggregate weights from task scores",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _calculate_aggregate_weights(self, geomagnetic_scores, soil_scores):
        """
        Calculate aggregate weights from individual task scores.
        
        Args:
            geomagnetic_scores (List[float]): Geomagnetic task scores
            soil_scores (List[float]): Soil moisture task scores
            
        Returns:
            List[float]: Combined weights before normalization
        """
        weights = [0.0] * 256
        for idx in range(256):
            geomagnetic_score = geomagnetic_scores[idx]
            soil_score = soil_scores[idx]

            if math.isnan(geomagnetic_score) and math.isnan(soil_score):
                weights[idx] = 0.0
                logger.debug(f"UID {idx}: Both scores nan - setting weight to 0")
            elif math.isnan(geomagnetic_score):
                weights[idx] = 0.5 * soil_score
                logger.debug(f"UID {idx}: Geo score nan - using soil score: {weights[idx]}")
            elif math.isnan(soil_score):
                geo_normalized = math.exp(-abs(geomagnetic_score) / 10)
                weights[idx] = 0.5 * geo_normalized
                logger.debug(f"UID {idx}: Soil score nan - normalized geo score: {geo_normalized} -> weight: {weights[idx]}")
            else:
                geo_normalized = math.exp(-abs(geomagnetic_score) / 10)
                weights[idx] = (0.5 * geo_normalized) + (0.5 * soil_score)
                logger.debug(f"UID {idx}: Both scores valid - geo_norm: {geo_normalized}, soil: {soil_score} -> weight: {weights[idx]}")

        return weights

    @task_step(
        name="normalize_weights",
        description="Normalize and rank weights",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _normalize_weights(self, weights):
        """
        Normalize and rank the calculated weights.
        
        Args:
            weights (List[float]): Raw weights to normalize
            
        Returns:
            Optional[List[float]]: Normalized weights, or None if normalization fails
        """
        non_zero_weights = [w for w in weights if w != 0.0]
        if not non_zero_weights:
            logger.warning("All weights are zero or nan, skipping weight setting")
            return None
            
        sorted_indices = sorted(
            range(len(weights)),
            key=lambda k: (weights[k] if weights[k] != 0.0 else float("-inf")),
        )

        new_weights = [0.0] * len(weights)
        for rank, idx in enumerate(sorted_indices):
            if weights[idx] != 0.0:
                try:
                    normalized_rank = 1.0 - (rank / len(non_zero_weights))
                    exponent = max(min(-20 * (normalized_rank - 0.5), 709), -709)
                    new_weights[idx] = 1 / (1 + math.exp(exponent))
                except OverflowError:
                    logger.warning(f"Overflow prevented for rank {rank}, idx {idx}")
                    if normalized_rank > 0.5:
                        new_weights[idx] = 1.0
                    else:
                        new_weights[idx] = 0.0

        total = sum(new_weights)
        if total > 0:
            return [w / total for w in new_weights]
        else:
            logger.warning("No positive weights after normalization")
            return None

    @task_step(
        name="calculate_task_weights",
        description="Calculate and normalize weights from task scores",
        timeout_seconds=120,  # 2 minutes
        max_retries=2,
        retry_delay_seconds=10
    )
    async def calculate_task_weights(self) -> Optional[List[float]]:
        """
        Calculate and normalize weights from task scores.
        
        This method:
        1. Syncs metagraph nodes
        2. Fetches recent task scores
        3. Calculates aggregate weights
        4. Normalizes and ranks weights
        
        Returns:
            Optional[List[float]]: Normalized weights if successful, None otherwise
        """
        try:
            # Stage 1: Sync metagraph
            logger.info("Syncing metagraph nodes...")
            self.metagraph.sync_nodes()
            logger.info("Metagraph synced. Fetching recent scores...")

            # Stage 2: Fetch scores
            geomagnetic_scores, soil_scores = await self._fetch_task_scores()
            logger.info("Recent scores fetched. Calculating aggregate scores...")

            # Stage 3: Calculate aggregate weights
            weights = await self._calculate_aggregate_weights(geomagnetic_scores, soil_scores)
            logger.info("Aggregate weights calculated. Normalizing...")

            # Stage 4: Normalize weights
            normalized_weights = await self._normalize_weights(weights)
            
            if normalized_weights:
                logger.info("Successfully calculated normalized weights")
                self.weights = normalized_weights
                return normalized_weights
            else:
                logger.warning("Failed to calculate valid weights")
                return None
                
        except Exception as e:
            logger.error(f"Error calculating task weights: {e}")
            logger.error(traceback.format_exc())
            return None

    @task_step(
        name="check_validator_status",
        description="Check validator registration and update status",
        timeout_seconds=30,
        max_retries=2
    )
    async def check_validator_status(self, validator_ss58_address: str):
        """Check if validator is registered and get its UID."""
        validator_uid = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self.substrate.query(
                "SubtensorModule", 
                "Uids", 
                [self.netuid, validator_ss58_address]
            ).value
        )
        
        if validator_uid is None:
            logger.error("Validator not found on chain")
            return False
            
        return validator_uid

    @task_step(
        name="check_weight_setting_interval",
        description="Check if enough blocks have passed to set weights",
        timeout_seconds=30,
        max_retries=2
    )
    async def check_weight_setting_interval(self, validator_uid) -> tuple[int, int]:
        """Check if enough blocks have passed to set weights."""
        blocks_since_update = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: blocks_since_last_update(
                self.substrate, 
                self.netuid, 
                validator_uid
            )
        )
        
        min_interval = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: min_interval_to_set_weights(
                self.substrate, 
                self.netuid
            )
        )
        
        return blocks_since_update, min_interval

    @task_step(
        name="verify_weight_setting_permission",
        description="Verify if validator can set weights",
        timeout_seconds=30,
        max_retries=2
    )
    async def verify_weight_setting_permission(self, validator_uid) -> bool:
        """Verify if validator has permission to set weights."""
        return await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: can_set_weights(
                self.substrate, 
                self.netuid, 
                validator_uid
            )
        )

    async def update_last_weights_block(self):
        """Update the last block where weights were set."""
        try:
            block = self.substrate.get_block()
            if block and block.get("header", {}).get("number"):
                self.last_set_weights_block = block["header"]["number"]
                self.last_successful_weight_set = time.time()
            else:
                logger.error("Invalid block data")
        except Exception as e:
            logger.error(f"Error updating last weights block: {e}")

    async def fetch_task_scores(self) -> tuple[dict, dict]:
        """
        Fetch recent task scores from database.
        
        Returns:
            tuple[dict, dict]: (soil_scores, geo_scores) dictionaries mapping UIDs to scores
        """
        try:
            # Get raw scores
            geomagnetic_scores, soil_scores = await self._fetch_task_scores()
            
            # Convert to dictionaries
            geo_dict = {
                i: score 
                for i, score in enumerate(geomagnetic_scores)
                if not math.isnan(score)
            }
            
            soil_dict = {
                i: score
                for i, score in enumerate(soil_scores)
                if not math.isnan(score)
            }
            
            return soil_dict, geo_dict
            
        except Exception as e:
            logger.error(f"Error fetching task scores: {e}")
            return {}, {}

    async def calculate_weights(self, soil_scores: dict, geo_scores: dict) -> list[float]:
        """Calculate weights based on task scores."""
        try:
            weights = [0.0] * 256  # Initialize weights array
            
            # Combine scores with appropriate weights
            for uid in range(256):
                soil_score = soil_scores.get(uid, 0.0)
                geo_score = geo_scores.get(uid, 0.0)
                weights[uid] = 0.5 * soil_score + 0.5 * geo_score

            # Normalize weights
            total = sum(weights)
            if total > 0:
                weights = [w/total for w in weights]

            return weights
        except Exception as e:
            logger.error(f"Error calculating weights: {e}")
            return [] 

    @task_step(
        name="set_weights",
        description="Set weights on chain",
        timeout_seconds=120,  # 2 minutes
        max_retries=3,
        retry_delay_seconds=10
    )
    async def set_weights(self, keypair) -> bool:
        """
        Set weights on chain.
        
        Args:
            keypair: Validator keypair for signing
            
        Returns:
            bool: True if weights set successfully
        """
        try:
            # Check validator status
            validator_uid = await self.check_validator_status(keypair.ss58_address)
            if not validator_uid:
                return False

            # Check if enough blocks have passed
            blocks_since_update, min_interval = await self.check_weight_setting_interval(validator_uid)
            
            # Check if recently set weights
            try:
                block = self.substrate.get_block()
                if block and block.get("header", {}).get("number"):
                    current_block = block["header"]["number"]
                else:
                    logger.error("Invalid block data")
                    return False
            except Exception as e:
                logger.error(f"Error getting current block: {e}")
                return False
                
            if current_block - self.last_set_weights_block < min_interval:
                logger.info(
                    f"Recently set weights {current_block - self.last_set_weights_block} blocks ago. "
                    f"Waiting {min_interval - (current_block - self.last_set_weights_block)} more blocks"
                )
                return False

            if min_interval is None or (blocks_since_update is not None and blocks_since_update >= min_interval):
                # Verify permission to set weights
                can_set = await self.verify_weight_setting_permission(validator_uid)
                
                if not can_set:
                    logger.warning("Validator does not have permission to set weights")
                    return False
                    
                # Calculate weights
                weights = await self.calculate_task_weights()
                if not weights:
                    logger.warning("No valid weights to set")
                    return False

                # Set weights on chain
                try:
                    # Create call
                    call = self.substrate.compose_call(
                        call_module='SubtensorModule',
                        call_function='set_weights',
                        call_params={
                            'netuid': self.netuid,
                            'dests': list(range(len(weights))),
                            'weights': weights
                        }
                    )
                    
                    # Create signed extrinsic
                    extrinsic = self.substrate.create_signed_extrinsic(
                        call=call,
                        keypair=keypair
                    )
                    
                    # Submit and wait for inclusion
                    receipt = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: self.substrate.submit_extrinsic(
                            extrinsic,
                            wait_for_inclusion=True
                        )
                    )

                    if receipt and receipt.is_success:
                        await self.update_last_weights_block()
                        logger.info(
                            f"Successfully set weights (block: {current_block}, "
                            f"hash: {receipt.extrinsic_hash})"
                        )
                        return True
                    else:
                        logger.error("Failed to set weights - transaction failed")
                        return False
                except Exception as e:
                    logger.error(f"Error submitting weight extrinsic: {e}")
                    return False
            else:
                logger.info(
                    f"Waiting for weight setting: {blocks_since_update}/{min_interval} blocks"
                )
                return False

        except Exception as e:
            logger.error(f"Error setting weights: {e}")
            logger.error(traceback.format_exc())
            return False

    def get_weight_state(self) -> Dict:
        """Get current weight state."""
        return {
            "last_set_weights_block": self.last_set_weights_block,
            "last_successful_weight_set": datetime.fromtimestamp(
                self.last_successful_weight_set,
                tz=timezone.utc
            ) if self.last_successful_weight_set else None,
            "weights": self.weights
        } 