from __future__ import annotations

import asyncio
import numpy as np
from typing import List

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.validator.weights.set_weights import FiberWeightSetter
from fiber.chain import weights as w


async def compute_normalized_weights(validator, db: ValidatorDatabaseManager) -> List[float] | None:
    try:
        weights = await validator._calc_task_weights()
        return weights
    except Exception:
        return None


async def commit_weights_if_eligible(validator) -> bool:
    try:
        substrate = validator.substrate
        netuid = validator.netuid
        validator_uid = validator.validator_uid
        if validator_uid is None:
            try:
                validator_uid = validator.substrate.query(
                    "SubtensorModule", "Uids", [netuid, validator.keypair.ss58_address]
                )
                validator.validator_uid = int(validator_uid)
            except Exception:
                return False
        current_block = validator._get_current_block_cached()
        min_interval = w.min_interval_to_set_weights(substrate, netuid)
        min_interval = int(min_interval) if min_interval is not None else None
        if min_interval is not None and (current_block - validator.last_set_weights_block) < min_interval:
            return False
        can_set = w.can_set_weights(substrate, netuid, int(validator_uid))
        if not can_set:
            return False
        weights = await compute_normalized_weights(validator, validator.database_manager)
        if not weights:
            return False
        if getattr(validator, "perturbation_manager", None):
            try:
                arr = np.array(weights)
                perturbed = await validator.perturbation_manager.get_perturbed_weights(arr)
                weights = perturbed.tolist()
            except Exception:
                pass
        setter = FiberWeightSetter(
            netuid=netuid,
            wallet_name=validator.wallet_name,
            hotkey_name=validator.hotkey_name,
            network=validator.subtensor_network,
        )
        ok = await asyncio.wait_for(setter.set_weights(weights), timeout=480)
        if ok:
            await validator.update_last_weights_block()
            import time as _t
            validator.last_successful_weight_set = _t.time()
            validator._shared_block_cache["block_number"] = None
            try:
                validator._aggressive_substrate_cleanup("post_weight_setting")
            except Exception:
                pass
            return True
        return False
    except Exception:
        return False


