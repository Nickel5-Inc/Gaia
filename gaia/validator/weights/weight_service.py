from __future__ import annotations

import asyncio
import numpy as np
from typing import List

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.validator.weights.set_weights import FiberWeightSetter
from fiber.chain import weights as w
from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)


async def get_last_weight_set_block_from_chain(substrate, netuid: int, validator_uid: int) -> int:
    """Get the block number when this validator last set weights from chain data."""
    try:
        logger.debug(f"[ChainQuery] Querying LastUpdate for netuid={netuid}, validator_uid={validator_uid}")
        
        # Query LastUpdate for all UIDs in the subnet
        last_update = await asyncio.get_event_loop().run_in_executor(
            None, lambda: substrate.query("SubtensorModule", "LastUpdate", [netuid])
        )
        
        if validator_uid < len(last_update):
            last_weight_block = last_update[validator_uid]
            logger.info(f"[ChainQuery] Chain reports last weight set at block {last_weight_block} for UID {validator_uid}")
            return int(last_weight_block)
        else:
            logger.warning(f"[ChainQuery] Validator UID {validator_uid} not found in LastUpdate array (length: {len(last_update)})")
            return 0
            
    except Exception as e:
        logger.error(f"[ChainQuery] Failed to get last weight block from chain: {e}")
        return 0


async def compute_normalized_weights(validator, db: ValidatorDatabaseManager) -> List[float] | None:
    try:
        weights = await validator._calc_task_weights()
        return weights
    except Exception:
        return None


async def commit_weights_if_eligible(validator) -> bool:
    try:
        logger.info("[WeightEligibility] Starting weight eligibility check")
        
        # Check if we have a proper validator instance with substrate access
        if not hasattr(validator, 'substrate') or not hasattr(validator, 'netuid'):
            logger.error("[WeightEligibility] ❌ Invalid validator instance - missing substrate or netuid attributes")
            logger.error(f"[WeightEligibility] Validator type: {type(validator)}, attributes: {dir(validator)[:10]}...")
            return False
        
        substrate = validator.substrate
        netuid = validator.netuid
        validator_uid = validator.validator_uid
        
        if validator_uid is None:
            logger.info("[WeightEligibility] Validator UID is None, querying from chain")
            try:
                validator_uid = validator.substrate.query(
                    "SubtensorModule", "Uids", [netuid, validator.keypair.ss58_address]
                )
                validator.validator_uid = int(validator_uid)
                logger.info(f"[WeightEligibility] Retrieved validator UID: {validator.validator_uid}")
            except Exception as e:
                logger.error(f"[WeightEligibility] Failed to get validator UID: {e}")
                return False
        else:
            logger.info(f"[WeightEligibility] Using cached validator UID: {validator_uid}")
        
        current_block = validator._get_current_block_cached()
        
        # Get last weight set block from chain instead of local tracking
        last_weight_block_chain = await get_last_weight_set_block_from_chain(substrate, netuid, int(validator_uid))
        blocks_since_weights = current_block - last_weight_block_chain
        
        logger.info(f"[WeightEligibility] Current block: {current_block}")
        logger.info(f"[WeightEligibility] Chain last weight block: {last_weight_block_chain} (local was: {validator.last_set_weights_block})")
        logger.info(f"[WeightEligibility] Blocks since last weight set: {blocks_since_weights}")
        
        min_interval = w.min_interval_to_set_weights(substrate, netuid)
        min_interval = int(min_interval) if min_interval is not None else None
        
        logger.info(f"[WeightEligibility] Chain min interval: {min_interval}, Blocks since last set: {blocks_since_weights}")
        
        if min_interval is not None and blocks_since_weights < min_interval:
            logger.info(f"[WeightEligibility] ❌ Not eligible - need {min_interval} blocks, only {blocks_since_weights} since last set")
            return False
        
        can_set = w.can_set_weights(substrate, netuid, int(validator_uid))
        logger.info(f"[WeightEligibility] Chain can_set_weights check: {can_set}")
        if not can_set:
            logger.info("[WeightEligibility] ❌ Chain reports cannot set weights")
            return False
        
        logger.info("[WeightEligibility] Computing normalized weights...")
        weights = await compute_normalized_weights(validator, validator.database_manager)
        if not weights:
            logger.warning("[WeightEligibility] ❌ No weights computed")
            return False
        
        logger.info(f"[WeightEligibility] ✅ Computed {len(weights)} weights, proceeding to set")
        if getattr(validator, "perturbation_manager", None):
            logger.info("[WeightEligibility] Applying weight perturbation...")
            try:
                arr = np.array(weights)
                perturbed = await validator.perturbation_manager.get_perturbed_weights(arr)
                weights = perturbed.tolist()
                logger.info("[WeightEligibility] Weight perturbation applied successfully")
            except Exception as e:
                logger.warning(f"[WeightEligibility] Weight perturbation failed: {e}")
        
        logger.info(f"[WeightEligibility] Creating FiberWeightSetter for netuid {netuid}")
        setter = FiberWeightSetter(
            netuid=netuid,
            wallet_name=validator.wallet_name,
            hotkey_name=validator.hotkey_name,
            network=validator.subtensor_network,
        )
        
        logger.info("[WeightEligibility] Calling setter.set_weights() with 8-minute timeout...")
        ok = await asyncio.wait_for(setter.set_weights(weights), timeout=480)
        
        if ok:
            logger.info("[WeightEligibility] ✅ Weight setting successful")
            # Note: No need to update local tracking since we now use chain data
            # The chain's LastUpdate will be automatically updated by the weight transaction
            import time as _t
            validator.last_successful_weight_set = _t.time()
            validator._shared_block_cache["block_number"] = None
            try:
                validator._aggressive_substrate_cleanup("post_weight_setting")
            except Exception as cleanup_error:
                logger.warning(f"[WeightEligibility] Substrate cleanup warning: {cleanup_error}")
            return True
        else:
            logger.error("[WeightEligibility] ❌ Weight setting failed")
            return False
    except Exception as e:
        logger.error(f"[WeightEligibility] ❌ Exception in commit_weights_if_eligible: {e}", exc_info=True)
        return False


