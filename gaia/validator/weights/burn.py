import os
from typing import List, Optional
from dotenv import load_dotenv
from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)

# Load .env once at module import
try:
    load_dotenv()
except Exception:
    pass


def _parse_bool(val: Optional[str]) -> bool:
    if val is None:
        return False
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def apply_burn_from_env(
    weights_by_uid: List[float],
    substrate,
    netuid: int,
) -> List[float]:
    """
    Apply burn on precomputed weights by UID:
      - If BURN_MINER_EMISSION=true, direct BURN_RATE fraction of total weight to BURN_HOTKEY's UID.
      - Reduce all weights proportionally by (1 - BURN_RATE) so ordering among non-burn UIDs is preserved.
      - Sum of weights remains unchanged (normalization downstream preserves final fractions).
    """
    try:
        enabled = _parse_bool(os.getenv("BURN_MINER_EMISSION", "true"))
        if not enabled:
            return weights_by_uid
        rate_str = os.getenv("BURN_RATE", "0.75")
        try:
            burn_rate = float(rate_str)
        except Exception:
            burn_rate = 0.0
        burn_rate = max(0.0, min(1.0, burn_rate))
        if burn_rate <= 0.0:
            return weights_by_uid
        burn_hotkey = os.getenv("BURN_HOTKEY", "5GmhjCqDTsRcKqPFw4FDig8tVBfFGNrwP8ywe5En5hnELwDQ").strip()
        if not burn_hotkey:
            logger.warning("[Burn] BURN_MINER_EMISSION enabled but BURN_HOTKEY is not set - skipping burn")
            return weights_by_uid

        # Resolve UID for the burn hotkey
        try:
            burn_uid = substrate.query("SubtensorModule", "Uids", [int(netuid), burn_hotkey])
            if not isinstance(burn_uid, int):
                # Substrate returns scale types sometimes; try casting
                burn_uid = int(burn_uid)
        except Exception as e:
            logger.error(f"[Burn] Failed to resolve UID for burn hotkey {burn_hotkey[:8]}...: {e}")
            return weights_by_uid

        if burn_uid < 0 or burn_uid >= len(weights_by_uid):
            logger.error(
                f"[Burn] Resolved burn UID {burn_uid} is out of bounds for weights length {len(weights_by_uid)} - skipping burn"
            )
            return weights_by_uid

        # Compute total weight (use non-negative slice)
        base = [max(0.0, float(w)) for w in weights_by_uid]
        total = sum(base)
        if total <= 0.0:
            logger.warning("[Burn] Total weight is zero or negative - skipping burn")
            return weights_by_uid

        # Apply proportional reduction and redirect the carved-out portion to burn_uid
        reduced = [(1.0 - burn_rate) * w for w in base]
        reduced[burn_uid] = reduced[burn_uid] + burn_rate * total

        logger.info(
            f"[Burn] Applied burn: rate={burn_rate:.4f}, target_hotkey={burn_hotkey[:8]}..., target_uid={burn_uid}"
        )
        # Optional: print brief top entries
        try:
            import numpy as _np
            arr = _np.array(reduced, dtype=float)
            top_idx = _np.argsort(-arr)[:10]
            top_pairs = [(int(i), float(arr[i])) for i in top_idx if arr[i] > 0]
            logger.debug(f"[Burn] Top weights after burn (uid,weight): {top_pairs}")
        except Exception:
            pass

        return reduced
    except Exception as e:
        logger.error(f"[Burn] Unexpected error applying burn: {e}")
        return weights_by_uid


def get_burn_uid_if_enabled(substrate, netuid: int) -> Optional[int]:
    """
    Return the UID of the burn hotkey if burn is enabled and resolvable; otherwise None.
    """
    try:
        enabled = _parse_bool(os.getenv("BURN_MINER_EMISSION", "true"))
        if not enabled:
            return None
        rate_str = os.getenv("BURN_RATE", "0.5")
        try:
            burn_rate = float(rate_str)
        except Exception:
            burn_rate = 0.0
        burn_rate = max(0.0, min(1.0, burn_rate))
        if burn_rate <= 0.0:
            return None
        burn_hotkey = os.getenv("BURN_HOTKEY", "5GmhjCqDTsRcKqPFw4FDig8tVBfFGNrwP8ywe5En5hnELwDQ").strip()
        if not burn_hotkey:
            return None
        try:
            burn_uid = substrate.query("SubtensorModule", "Uids", [int(netuid), burn_hotkey])
            return int(burn_uid)
        except Exception:
            return None
    except Exception:
        return None

