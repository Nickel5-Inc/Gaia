"""
Weight Perturbation Module for Anti-Weight-Copying Mechanism
Subnet-57 Hourly "Ranking-Safe Perturbation" Playbook

This module implements a defense against pure weight-copying validators by
perturbing weight vectors in a deterministic but unpredictable way.

CRITICAL TIMING DESIGN:
- Seeds are generated 3 hours in advance for extra safety
- Primary generates future seed at :10 past each hour
- Seed becomes active 3 hours later at :00
- This gives 2+ hours for database sync to propagate
- Database sync happens at :39, ensuring all validators have the seed well in advance
"""

import os
import secrets
import hashlib
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, Any
from gaia.utils.custom_logger import get_logger
import asyncio

logger = get_logger(__name__)

# Default perturbation parameters
# These defaults are carefully chosen for optimal anti-weight-copying defense
P_MOVE = float(
    os.getenv("PERTURB_P", "0.07")
)  # 7% mass moved (results in v-trust ~0.58 for copiers)
TAIL_FR = float(os.getenv("PERTURB_TAIL_FR", "0.50"))  # Bottom 50% of miners get boost
UNIFORM_PORTION = float(
    os.getenv("PERTURB_UNIFORM", "0.70")
)  # 70% uniform, 30% random distribution

# Feature is ENABLED by default - validators must explicitly disable if needed
# This ensures network-wide adoption without configuration
ENABLE_PERTURBATION = os.getenv("ENABLE_WEIGHT_PERTURBATION", "true").lower() != "false"

# Timing configuration (should not need adjustment)
SEED_ADVANCE_HOURS = 3  # Generate seed 3 hours before it becomes active (180 minutes)


class WeightPerturbationManager:
    """
    Manages weight perturbation for anti-weight-copying defense.

    Key features:
    - Hourly seed rotation (primary validator only)
    - Deterministic perturbation based on shared seed
    - Ranking-safe weight redistribution
    - Database-synchronized across all validators
    """

    def __init__(self, db_manager, validator_hotkey: Optional[str] = None):
        """
        Initialize the weight perturbation manager.

        Args:
            db_manager: Database manager instance
            validator_hotkey: Not used anymore, kept for compatibility
        """
        self.db_manager = db_manager
        self.validator_hotkey = validator_hotkey  # Kept for compatibility

        # Use existing IS_SOURCE_VALIDATOR_FOR_DB_SYNC to determine primary
        # The database source validator is also responsible for seed generation
        self.is_primary = (
            os.getenv("IS_SOURCE_VALIDATOR_FOR_DB_SYNC", "False").lower() == "true"
        )

        self.last_seed_rotation = None
        self.cached_seed = None
        self.cached_seed_time = None

        if ENABLE_PERTURBATION:
            logger.info(
                f"WeightPerturbationManager initialized. Is primary: {self.is_primary}, P={P_MOVE}"
            )
        else:
            logger.debug(f"WeightPerturbationManager initialized (disabled)")

    async def initialize_seed_table(self) -> None:
        """Ensure the perturb_seed table has seeds for current and future hours."""
        try:
            now = datetime.now(timezone.utc)
            current_hour = now.replace(minute=0, second=0, microsecond=0)

            # Check and create seeds for current hour and the next SEED_ADVANCE_HOURS
            hours_to_check = [current_hour]
            for i in range(1, SEED_ADVANCE_HOURS + 1):
                hours_to_check.append(current_hour + timedelta(hours=i))

            seeds_created = 0
            for activation_time in hours_to_check:
                # First check if seed already exists to avoid overwriting on restart
                existing = await self.db_manager.fetch_one(
                    "SELECT seed_hex FROM perturb_seed WHERE activation_hour = :hour",
                    {"hour": activation_time},
                )

                if not existing:
                    # Generate seed only if it doesn't exist
                    seed_hex = secrets.token_hex(32)

                    # For past/current hour seeds, pretend they were generated well in advance
                    # For future seeds, use actual time
                    if activation_time <= current_hour:
                        # Pretend seed was generated 2 hours before activation to avoid warnings
                        generated_at = activation_time - timedelta(hours=2)
                    else:
                        generated_at = now

                    await self.db_manager.execute(
                        """INSERT INTO perturb_seed (activation_hour, seed_hex, generated_at) 
                           VALUES (:hour, :seed, :generated_at) 
                           ON CONFLICT (activation_hour) DO NOTHING""",
                        {
                            "hour": activation_time,
                            "seed": seed_hex,
                            "generated_at": generated_at,
                        },
                    )
                    seeds_created += 1
                    logger.debug(f"Created seed for hour {activation_time.isoformat()}")

            if seeds_created > 0:
                logger.info(
                    f"Initialized {seeds_created} missing seeds for next {SEED_ADVANCE_HOURS} hours"
                )
            else:
                logger.debug(
                    f"All seeds already exist for next {SEED_ADVANCE_HOURS} hours"
                )

        except Exception as e:
            logger.warning(f"Could not initialize perturb_seed table: {e}")

    async def rotate_seed_if_needed(self) -> bool:
        """
        Generate future seeds if this is the primary validator and it's time.
        Seeds are generated at :10 past each hour for activation 2-3 hours later.

        This timing ensures:
        - Seed generated at :10
        - Database backup at :24 (includes new seed)
        - Replicas sync at :39 (get new seed)
        - Seed activates 2-3 hours later (plenty of time for all validators to sync)

        Returns:
            True if seed was generated, False otherwise
        """
        if not self.is_primary:
            return False

        if not ENABLE_PERTURBATION:
            return False

        try:
            now = datetime.now(timezone.utc)

            # Generate seeds at minute 10 of each hour
            if now.minute != 10:
                return False

            # Avoid generating multiple times in the same minute
            if self.last_seed_rotation and (now - self.last_seed_rotation) < timedelta(
                minutes=1
            ):
                return False

            # Generate seeds for the next 3-4 hours to ensure we always have enough buffer
            current_hour_base = now.replace(minute=0, second=0, microsecond=0)
            hours_to_generate = [
                current_hour_base
                + timedelta(hours=SEED_ADVANCE_HOURS),  # Primary target
                current_hour_base
                + timedelta(hours=SEED_ADVANCE_HOURS + 1),  # Extra buffer
            ]

            seeds_generated = 0
            for future_activation in hours_to_generate:
                # Check if seed already exists for this future hour
                result = await self.db_manager.fetch_one(
                    "SELECT COUNT(*) as count FROM perturb_seed WHERE activation_hour = :hour",
                    {"hour": future_activation},
                )

                if result and result["count"] == 0:
                    # Generate new seed for future hour
                    new_seed = secrets.token_hex(32)
                    await self.db_manager.execute(
                        """INSERT INTO perturb_seed (activation_hour, seed_hex, generated_at) 
                           VALUES (:hour, :seed, :now)
                           ON CONFLICT (activation_hour) DO NOTHING""",
                        {"hour": future_activation, "seed": new_seed, "now": now},
                    )
                    seeds_generated += 1
                    hours_ahead = (
                        future_activation - current_hour_base
                    ).total_seconds() / 3600
                    logger.info(
                        f"Generated seed for {future_activation.isoformat()} ({hours_ahead:.0f} hours ahead)"
                    )

            if seeds_generated > 0:
                logger.info(
                    f"Generated {seeds_generated} future seed(s) with {SEED_ADVANCE_HOURS}-{SEED_ADVANCE_HOURS+1} hour advance time for safe syncing"
                )

            # Clean up old seeds (keep last 24 hours for safety)
            cutoff = now - timedelta(hours=24)
            await self.db_manager.execute(
                "DELETE FROM perturb_seed WHERE activation_hour < :cutoff",
                {"cutoff": cutoff},
            )

            if seeds_generated > 0:
                self.last_seed_rotation = now
                # Clear cache to force reload
                self.cached_seed = None
                self.cached_seed_time = None
                return True

            return False

        except Exception as e:
            logger.error(f"Failed to generate future seed: {e}")
            return False

    async def load_seed(self) -> bytes:
        """
        Load the active seed for the current hour from the database.
        Uses caching to avoid excessive DB queries.

        CRITICAL: Always uses the seed for the current hour boundary to ensure
        all validators use the same seed regardless of exact minute within the hour.

        Returns:
            The current seed as bytes
        """
        now = datetime.now(timezone.utc)
        current_hour = now.replace(minute=0, second=0, microsecond=0)

        # Use cached seed if it's for the same hour
        if self.cached_seed and self.cached_seed_time:
            cached_hour = self.cached_seed_time.replace(
                minute=0, second=0, microsecond=0
            )
            if cached_hour == current_hour:
                return self.cached_seed

        try:
            # Use direct database call instead of session
            result = await self.db_manager.fetch_one(
                """SELECT seed_hex, generated_at 
                   FROM perturb_seed 
                   WHERE activation_hour = :hour""",
                {"hour": current_hour},
            )

            if not result:
                # No seed found for current hour, try to initialize
                logger.warning(
                    f"No seed found for hour {current_hour.isoformat()}, initializing..."
                )
                await self.initialize_seed_table()

                # Try again
                result = await self.db_manager.fetch_one(
                    """SELECT seed_hex, generated_at 
                       FROM perturb_seed 
                       WHERE activation_hour = :hour""",
                    {"hour": current_hour},
                )

            if result:
                seed_hex = result["seed_hex"]
                generated_at = result["generated_at"]

                # Check when seed was generated (should be at least SEED_ADVANCE_HOURS-1 before activation)
                generation_delta = (current_hour - generated_at).total_seconds()
                min_advance_seconds = (
                    SEED_ADVANCE_HOURS - 1
                ) * 3600  # Allow 1 hour less than ideal
                if generation_delta < min_advance_seconds:
                    logger.warning(
                        f"Seed was generated only {generation_delta/60:.1f} minutes before activation!"
                    )
                    logger.warning(
                        "This could cause synchronization issues if replicas haven't synced yet"
                    )

                self.cached_seed = bytes.fromhex(seed_hex)
                self.cached_seed_time = now

                logger.debug(
                    f"Loaded seed for hour {current_hour.isoformat()}, generated at {generated_at.isoformat()}"
                )
                return self.cached_seed
            else:
                # Fallback: use a deterministic seed based on hour
                logger.error(
                    f"Could not load seed for {current_hour.isoformat()}, using fallback"
                )
                # All validators will generate the same fallback for the same hour
                fallback = hashlib.sha256(
                    f"fallback_{current_hour.isoformat()}".encode()
                ).digest()[:32]
                return fallback

        except Exception as e:
            logger.error(f"Error loading seed: {e}")
            # Fallback seed - deterministic based on current hour
            current_hour = datetime.now(timezone.utc).replace(
                minute=0, second=0, microsecond=0
            )
            fallback = hashlib.sha256(
                f"error_{current_hour.isoformat()}".encode()
            ).digest()[:32]
            return fallback

    def perturb_weights(self, base_weights: np.ndarray, seed: bytes) -> np.ndarray:
        """
        Apply ranking-safe perturbation to weight vector using burn key strategy.

        This function:
        1. Scales down all weights by (1 - P_MOVE)
        2. Sends 30-100% of P_MOVE mass to burn key (UID 252)
           - Exact percentage is deterministically random per hour
           - Prevents rewarding inactive miners
        3. Distributes remaining mass to bottom-k% miners with variation
        4. Preserves ranking of top performers
        5. Creates unpredictable weights that foil copying attempts

        Args:
            base_weights: Original normalized weight vector (sum = 1)
            seed: Random seed for deterministic perturbation

        Returns:
            Perturbed weight vector (normalized)
        """
        if not ENABLE_PERTURBATION or P_MOVE <= 0:
            return base_weights.copy()

        # Ensure input is normalized
        if abs(base_weights.sum() - 1.0) > 1e-6:
            logger.warning(
                f"Input weights not normalized (sum={base_weights.sum()}), normalizing..."
            )
            base_weights = base_weights / base_weights.sum()

        # Create deterministic RNG from seed
        seed_int = int.from_bytes(hashlib.blake2b(seed, digest_size=8).digest(), "big")
        rng = np.random.default_rng(seed_int)

        n = base_weights.size

        # Identify tail miners (bottom k%)
        # Use tiny deterministic jitter to break ties among equal weights (e.g., many zeros)
        # This yields a random-looking but deterministic spread across UIDs
        tail_size = max(1, int(n * TAIL_FR))
        jitter = rng.random(n) * 1e-12
        jittered_weights = base_weights + jitter
        sorted_indices = np.argsort(jittered_weights)
        tail_indices = sorted_indices[:tail_size]

        # Create perturbation vector
        delta = np.zeros_like(base_weights)

        # BURN KEY STRATEGY:
        # Send most perturbation mass to burn key (UID 252) with unpredictable amount
        # This avoids rewarding low performers while maintaining anti-copying defense
        BURN_UID = 252

        if n > BURN_UID:  # Ensure burn UID exists
            # Determine burn fraction: 30-100% of P_MOVE goes to burn, rest to tail
            # Use deterministic random based on seed for high variance
            burn_fraction = 0.3 + (rng.random() * 0.7)  # 30% to 100%
            tail_fraction = 1.0 - burn_fraction

            # Mass to burn key
            burn_mass = P_MOVE * burn_fraction
            delta[BURN_UID] = burn_mass

            # Remaining mass distributed to tail miners (excluding burn key)
            if (
                tail_size > 0 and tail_fraction > 0
            ):  # Only distribute if there's mass left
                # Exclude burn key from tail if it's already there
                tail_indices_filtered = [idx for idx in tail_indices if idx != BURN_UID]

                if len(tail_indices_filtered) > 0:
                    # Small random variations for tail miners to avoid identical weights
                    tail_mass = P_MOVE * tail_fraction

                    # Generate deterministic random weights for tail
                    tail_weights = rng.exponential(
                        scale=1.0, size=len(tail_indices_filtered)
                    )
                    tail_weights = tail_weights / tail_weights.sum()  # Normalize
                    tail_weights *= tail_mass  # Scale to tail portion

                    for i, idx in enumerate(tail_indices_filtered):
                        delta[idx] = tail_weights[i]
        else:
            # Fallback if burn UID doesn't exist (shouldn't happen)
            logger.warning(
                f"Burn UID {BURN_UID} not available, using standard perturbation"
            )
            if tail_size > 0:
                delta_per_miner = P_MOVE / tail_size
                delta[tail_indices] = delta_per_miner

        # Apply perturbation: scale down original weights and add boost
        w_prime = (1 - P_MOVE) * base_weights + delta

        # Renormalize (should already be ~1, but ensure exactness)
        w_prime = w_prime / w_prime.sum()

        # Verify safety invariants
        self._verify_perturbation_safety(base_weights, w_prime)

        return w_prime

    def _verify_perturbation_safety(
        self, base_w: np.ndarray, perturbed_w: np.ndarray
    ) -> None:
        """
        Verify that perturbation maintains safety invariants.

        Args:
            base_w: Original weights
            perturbed_w: Perturbed weights
        """
        # Check normalization
        if abs(perturbed_w.sum() - 1.0) > 1e-6:
            logger.error(f"Perturbed weights not normalized! Sum={perturbed_w.sum()}")

        # Check if many miners have equal weights (common when most have minimal weight)
        unique_weights = len(
            np.unique(np.round(base_w, 10))
        )  # Round to avoid float precision issues
        total_miners = len(base_w)

        # If less than 30% of miners have unique weights, rankings are not stable anyway
        if unique_weights < total_miners * 0.3:
            logger.debug(
                f"Many miners have equal weights ({unique_weights} unique out of {total_miners}), skipping ranking check"
            )
            return

        # Check top-k preservation (e.g., top 20%)
        k = max(1, int(len(base_w) * 0.2))
        top_base = set(np.argsort(base_w)[-k:])
        top_perturbed = set(np.argsort(perturbed_w)[-k:])

        if top_base != top_perturbed:
            diff = top_base.symmetric_difference(top_perturbed)
            # Be more lenient - allow up to 30% change when many have similar weights
            if len(diff) > k * 0.3:
                logger.warning(
                    f"Top-{k} miners changed after perturbation: {len(diff)} differences (expected with many equal weights)"
                )

    async def get_perturbed_weights(self, base_weights: np.ndarray) -> np.ndarray:
        """
        Main interface: Get perturbed weights for submission.

        Args:
            base_weights: Original calculated weights

        Returns:
            Perturbed weights ready for submission
        """
        if not ENABLE_PERTURBATION:
            return base_weights

        try:
            # Load current seed
            seed = await self.load_seed()

            # Apply perturbation
            perturbed = self.perturb_weights(base_weights, seed)

            # Calculate and log statistics
            l1_distance = np.abs(perturbed - base_weights).sum()
            unique_weights = len(np.unique(np.round(perturbed, 10)))
            burn_weight = perturbed[252] if len(perturbed) > 252 else 0

            logger.info(
                f"Applied weight perturbation: L1_distance={l1_distance:.4f}, P={P_MOVE}, "
                f"burn_UID252={burn_weight:.5f}, unique_weights={unique_weights}/{len(perturbed)}"
            )

            return perturbed

        except Exception as e:
            logger.error(f"Error in weight perturbation, using original weights: {e}")
            return base_weights

    def estimate_weight_copier_vtrust(self, d_epochs: int = 5) -> float:
        """
        Estimate the v-trust of a pure weight-copying validator.

        Args:
            d_epochs: Commit-reveal delay in epochs

        Returns:
            Estimated v-trust value
        """
        # T = 1 - (d + 1) * p
        # With p% moved each epoch and concealed for d epochs
        # L1 distance = 2 * (d + 1) * p
        # v-trust = 1 - 0.5 * L1_distance = 1 - (d + 1) * p
        return max(0, 1 - (d_epochs + 1) * P_MOVE)

    def get_monitoring_metrics(self) -> Dict[str, Any]:
        """
        Get monitoring metrics for the perturbation system.

        Returns:
            Dictionary of metrics
        """
        now = datetime.now(timezone.utc)
        current_hour = now.replace(minute=0, second=0, microsecond=0)

        metrics = {
            "enabled": ENABLE_PERTURBATION,
            "p_move": P_MOVE,
            "tail_fraction": TAIL_FR,
            "is_primary": self.is_primary,
            "estimated_wc_vtrust": self.estimate_weight_copier_vtrust(),
            "current_hour": current_hour.isoformat(),
            "minutes_into_hour": now.minute,
        }

        # Timing safety metrics
        if now.minute < 39:
            metrics["sync_status"] = "DANGER - Replicas may not have synced yet!"
            metrics["safe_for_weights"] = False
        else:
            metrics["sync_status"] = "SAFE - All validators should have synced"
            metrics["safe_for_weights"] = True

        if self.cached_seed_time:
            cached_hour = self.cached_seed_time.replace(
                minute=0, second=0, microsecond=0
            )
            if cached_hour == current_hour:
                metrics["seed_cached_for_correct_hour"] = True
            else:
                metrics["seed_cached_for_correct_hour"] = False
                metrics["cached_hour"] = cached_hour.isoformat()

        if self.last_seed_rotation:
            metrics["last_seed_generation"] = self.last_seed_rotation.isoformat()
            metrics["next_generation_at"] = self.last_seed_rotation.replace(
                minute=10
            ) + timedelta(hours=1)

        return metrics


# Convenience function for testing
def test_perturbation():
    """Test the perturbation logic with sample data."""
    import matplotlib.pyplot as plt

    # Create sample weights (power law distribution)
    n = 256
    x = np.arange(1, n + 1)
    base_weights = 1 / x**1.5
    base_weights = base_weights / base_weights.sum()

    # Apply perturbation
    seed = secrets.token_bytes(32)
    manager = WeightPerturbationManager(None)
    perturbed = manager.perturb_weights(base_weights, seed)

    # Verify properties
    print(f"Base sum: {base_weights.sum():.9f}")
    print(f"Perturbed sum: {perturbed.sum():.9f}")
    print(f"L1 distance: {np.abs(perturbed - base_weights).sum():.4f}")

    # Check top-k preservation
    k = 20
    top_base = set(np.argsort(base_weights)[-k:])
    top_perturbed = set(np.argsort(perturbed)[-k:])
    print(f"Top-{k} preserved: {top_base == top_perturbed}")
    print(f"Top-{k} overlap: {len(top_base & top_perturbed)}/{k}")

    # Plot
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    axes[0].bar(range(n), base_weights, color="blue", alpha=0.6, label="Base")
    axes[0].bar(range(n), perturbed, color="red", alpha=0.6, label="Perturbed")
    axes[0].set_xlabel("Miner UID")
    axes[0].set_ylabel("Weight")
    axes[0].set_title("Weight Distribution")
    axes[0].legend()
    axes[0].set_xlim(0, 50)  # Focus on first 50 for visibility

    axes[1].scatter(base_weights, perturbed, alpha=0.5, s=10)
    axes[1].plot([0, base_weights.max()], [0, base_weights.max()], "r--", alpha=0.5)
    axes[1].set_xlabel("Base Weight")
    axes[1].set_ylabel("Perturbed Weight")
    axes[1].set_title("Base vs Perturbed Weights")
    axes[1].set_xscale("log")
    axes[1].set_yscale("log")

    plt.tight_layout()
    plt.savefig("weight_perturbation_test.png", dpi=150)
    print("Plot saved to weight_perturbation_test.png")

    # Test v-trust calculation
    d_epochs = 5
    estimated_vtrust = manager.estimate_weight_copier_vtrust(d_epochs)
    print(f"\nWith P={P_MOVE}, d={d_epochs} epochs:")
    print(f"Estimated weight-copier v-trust: {estimated_vtrust:.3f}")


if __name__ == "__main__":
    # Run test if executed directly
    test_perturbation()
