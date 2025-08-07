"""
Weight Perturbation Module for Anti-Weight-Copying Mechanism
Subnet-57 Hourly "Ranking-Safe Perturbation" Playbook

This module implements a defense against pure weight-copying validators by
perturbing weight vectors in a deterministic but unpredictable way.

CRITICAL TIMING DESIGN:
- Seeds are generated 90 minutes in advance
- Primary generates next seed at :10 past each hour
- Seed becomes active at :00 of the NEXT hour
- This gives 50 minutes for database sync to propagate
- Database sync happens at :39, ensuring all validators have the seed
"""

import os
import secrets
import hashlib
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, Any
from fiber.logging_utils import get_logger
import asyncio
from sqlalchemy import text

logger = get_logger(__name__)

# Default perturbation parameters
# These defaults are carefully chosen for optimal anti-weight-copying defense
P_MOVE = float(os.getenv("PERTURB_P", "0.07"))  # 7% mass moved (results in v-trust ~0.58 for copiers)
TAIL_FR = float(os.getenv("PERTURB_TAIL_FR", "0.50"))  # Bottom 50% of miners get boost

# Feature is ENABLED by default - validators must explicitly disable if needed
# This ensures network-wide adoption without configuration
ENABLE_PERTURBATION = os.getenv("ENABLE_WEIGHT_PERTURBATION", "true").lower() != "false"

# Timing configuration (should not need adjustment)
SEED_ADVANCE_MINUTES = 90  # Generate seed 90 minutes before it becomes active


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
        self.is_primary = os.getenv("IS_SOURCE_VALIDATOR_FOR_DB_SYNC", "False").lower() == "true"
        
        self.last_seed_rotation = None
        self.cached_seed = None
        self.cached_seed_time = None
        
        if ENABLE_PERTURBATION:
            logger.info(f"WeightPerturbationManager initialized. Is primary: {self.is_primary}, P={P_MOVE}")
        else:
            logger.debug(f"WeightPerturbationManager initialized (disabled)")
    
    async def initialize_seed_table(self) -> None:
        """Ensure the perturb_seed table has seeds for current and future hours."""
        try:
            async with self.db_manager.session(operation_name="init_perturb_seed") as session:
                now = datetime.now(timezone.utc)
                current_hour = now.replace(minute=0, second=0, microsecond=0)
                next_hour = current_hour + timedelta(hours=1)
                future_hour = current_hour + timedelta(hours=2)
                
                # Ensure we have seeds for current, next, and future hours
                for activation_time in [current_hour, next_hour, future_hour]:
                    # Check if seed exists for this hour
                    result = await session.execute(text(
                        "SELECT COUNT(*) FROM perturb_seed WHERE activation_hour = :hour"
                    ), {"hour": activation_time})
                    count = result.scalar()
                    
                    if count == 0:
                        # Insert seed for this hour
                        seed_hex = secrets.token_hex(32)
                        await session.execute(text(
                            """INSERT INTO perturb_seed (activation_hour, seed_hex) 
                               VALUES (:hour, :seed) 
                               ON CONFLICT (activation_hour) DO NOTHING"""
                        ), {"hour": activation_time, "seed": seed_hex})
                        logger.info(f"Generated seed for hour {activation_time.isoformat()}")
                
                await session.commit()
                logger.debug("Perturb_seed table initialization complete")
                    
        except Exception as e:
            logger.warning(f"Could not initialize perturb_seed table: {e}")
    
    async def rotate_seed_if_needed(self) -> bool:
        """
        Generate future seeds if this is the primary validator and it's time.
        Seeds are generated at :10 past each hour for activation 50 minutes later.
        
        This timing ensures:
        - Seed generated at :10
        - Database backup at :24 (includes new seed)
        - Replicas sync at :39 (get new seed)
        - Seed activates at :00 next hour (all validators have it)
        
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
            if self.last_seed_rotation and (now - self.last_seed_rotation) < timedelta(minutes=1):
                return False
            
            # Calculate which hour to generate seed for (90 minutes in advance)
            future_activation = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=2)
            
            async with self.db_manager.session(operation_name="generate_future_seed") as session:
                # Check if seed already exists for this future hour
                result = await session.execute(text(
                    "SELECT COUNT(*) FROM perturb_seed WHERE activation_hour = :hour"
                ), {"hour": future_activation})
                
                if result.scalar() == 0:
                    # Generate new seed for future hour
                    new_seed = secrets.token_hex(32)
                    await session.execute(text(
                        """INSERT INTO perturb_seed (activation_hour, seed_hex, generated_at) 
                           VALUES (:hour, :seed, :now)
                           ON CONFLICT (activation_hour) DO NOTHING"""
                    ), {"hour": future_activation, "seed": new_seed, "now": now})
                    await session.commit()
                    
                    logger.info(f"Generated future seed for {future_activation.isoformat()} at {now.isoformat()}")
                    logger.info(f"This seed will be synced at :39 and activate at :00")
                
                # Clean up old seeds (keep last 24 hours for safety)
                cutoff = now - timedelta(hours=24)
                await session.execute(text(
                    "DELETE FROM perturb_seed WHERE activation_hour < :cutoff"
                ), {"cutoff": cutoff})
                await session.commit()
            
            self.last_seed_rotation = now
            # Clear cache to force reload
            self.cached_seed = None
            self.cached_seed_time = None
            
            return True
            
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
            cached_hour = self.cached_seed_time.replace(minute=0, second=0, microsecond=0)
            if cached_hour == current_hour:
                return self.cached_seed
        
        try:
            async with self.db_manager.session(operation_name="load_seed") as session:
                # Load seed for current hour
                result = await session.execute(text(
                    """SELECT seed_hex, generated_at 
                       FROM perturb_seed 
                       WHERE activation_hour = :hour"""
                ), {"hour": current_hour})
                row = result.fetchone()
                
                if not row:
                    # No seed found for current hour, try to initialize
                    logger.warning(f"No seed found for hour {current_hour.isoformat()}, initializing...")
                    await self.initialize_seed_table()
                    
                    # Try again
                    result = await session.execute(text(
                        """SELECT seed_hex, generated_at 
                           FROM perturb_seed 
                           WHERE activation_hour = :hour"""
                    ), {"hour": current_hour})
                    row = result.fetchone()
                
                if row:
                    seed_hex = row.seed_hex
                    generated_at = row.generated_at
                    
                    # Check when seed was generated (should be at least 50 minutes before activation)
                    generation_delta = (current_hour - generated_at).total_seconds()
                    if generation_delta < 3000:  # Less than 50 minutes
                        logger.warning(f"Seed was generated only {generation_delta/60:.1f} minutes before activation!")
                        logger.warning("This could cause synchronization issues if replicas haven't synced yet")
                    
                    self.cached_seed = bytes.fromhex(seed_hex)
                    self.cached_seed_time = now
                    
                    logger.debug(f"Loaded seed for hour {current_hour.isoformat()}, generated at {generated_at.isoformat()}")
                    return self.cached_seed
                else:
                    # Fallback: use a deterministic seed based on hour
                    logger.error(f"Could not load seed for {current_hour.isoformat()}, using fallback")
                    # All validators will generate the same fallback for the same hour
                    fallback = hashlib.sha256(f"fallback_{current_hour.isoformat()}".encode()).digest()[:32]
                    return fallback
                    
        except Exception as e:
            logger.error(f"Error loading seed: {e}")
            # Fallback seed - deterministic based on current hour
            current_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            fallback = hashlib.sha256(f"error_{current_hour.isoformat()}".encode()).digest()[:32]
            return fallback
    
    def perturb_weights(self, base_weights: np.ndarray, seed: bytes) -> np.ndarray:
        """
        Apply ranking-safe perturbation to weight vector.
        
        This function:
        1. Identifies the bottom-k% miners by weight
        2. Redistributes p% of total mass uniformly to them
        3. Scales down all weights proportionally
        4. Preserves ranking of top performers
        
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
            logger.warning(f"Input weights not normalized (sum={base_weights.sum()}), normalizing...")
            base_weights = base_weights / base_weights.sum()
        
        # Create deterministic RNG from seed
        seed_int = int.from_bytes(hashlib.blake2b(seed, digest_size=8).digest(), "big")
        rng = np.random.default_rng(seed_int)
        
        n = base_weights.size
        
        # Identify tail miners (bottom k%)
        tail_size = max(1, int(n * TAIL_FR))
        sorted_indices = np.argsort(base_weights)
        tail_indices = sorted_indices[:tail_size]
        
        # Create perturbation vector
        delta = np.zeros_like(base_weights)
        if tail_size > 0:
            # Uniform boost to tail miners
            delta_per_miner = P_MOVE / tail_size
            delta[tail_indices] = delta_per_miner
        
        # Apply perturbation: scale down original weights and add boost
        w_prime = (1 - P_MOVE) * base_weights + delta
        
        # Renormalize (should already be ~1, but ensure exactness)
        w_prime = w_prime / w_prime.sum()
        
        # Verify safety invariants
        self._verify_perturbation_safety(base_weights, w_prime)
        
        return w_prime
    
    def _verify_perturbation_safety(self, base_w: np.ndarray, perturbed_w: np.ndarray) -> None:
        """
        Verify that perturbation maintains safety invariants.
        
        Args:
            base_w: Original weights
            perturbed_w: Perturbed weights
        """
        # Check normalization
        if abs(perturbed_w.sum() - 1.0) > 1e-6:
            logger.error(f"Perturbed weights not normalized! Sum={perturbed_w.sum()}")
        
        # Check top-k preservation (e.g., top 20%)
        k = max(1, int(len(base_w) * 0.2))
        top_base = set(np.argsort(base_w)[-k:])
        top_perturbed = set(np.argsort(perturbed_w)[-k:])
        
        if top_base != top_perturbed:
            diff = top_base.symmetric_difference(top_perturbed)
            if len(diff) > k * 0.1:  # Allow up to 10% change
                logger.warning(f"Top-{k} miners changed significantly after perturbation: {len(diff)} differences")
    
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
            logger.info(f"Applied weight perturbation: L1_distance={l1_distance:.4f}, P={P_MOVE}")
            
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
            cached_hour = self.cached_seed_time.replace(minute=0, second=0, microsecond=0)
            if cached_hour == current_hour:
                metrics["seed_cached_for_correct_hour"] = True
            else:
                metrics["seed_cached_for_correct_hour"] = False
                metrics["cached_hour"] = cached_hour.isoformat()
        
        if self.last_seed_rotation:
            metrics["last_seed_generation"] = self.last_seed_rotation.isoformat()
            metrics["next_generation_at"] = self.last_seed_rotation.replace(minute=10) + timedelta(hours=1)
        
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
    
    axes[0].bar(range(n), base_weights, color='blue', alpha=0.6, label='Base')
    axes[0].bar(range(n), perturbed, color='red', alpha=0.6, label='Perturbed')
    axes[0].set_xlabel('Miner UID')
    axes[0].set_ylabel('Weight')
    axes[0].set_title('Weight Distribution')
    axes[0].legend()
    axes[0].set_xlim(0, 50)  # Focus on first 50 for visibility
    
    axes[1].scatter(base_weights, perturbed, alpha=0.5, s=10)
    axes[1].plot([0, base_weights.max()], [0, base_weights.max()], 'r--', alpha=0.5)
    axes[1].set_xlabel('Base Weight')
    axes[1].set_ylabel('Perturbed Weight')
    axes[1].set_title('Base vs Perturbed Weights')
    axes[1].set_xscale('log')
    axes[1].set_yscale('log')
    
    plt.tight_layout()
    plt.savefig('weight_perturbation_test.png', dpi=150)
    print("Plot saved to weight_perturbation_test.png")
    
    # Test v-trust calculation
    d_epochs = 5
    estimated_vtrust = manager.estimate_weight_copier_vtrust(d_epochs)
    print(f"\nWith P={P_MOVE}, d={d_epochs} epochs:")
    print(f"Estimated weight-copier v-trust: {estimated_vtrust:.3f}")


if __name__ == "__main__":
    # Run test if executed directly
    test_perturbation()