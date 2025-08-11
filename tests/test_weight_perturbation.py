"""
Unit tests for weight perturbation anti-weight-copying mechanism.
"""

import pytest
import numpy as np
import secrets
import hashlib
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gaia.validator.weight_perturbation import WeightPerturbationManager


class TestWeightPerturbation:
    """Test suite for weight perturbation functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db_manager = MagicMock()
        self.mock_db_manager.session = AsyncMock()
        self.manager = WeightPerturbationManager(self.mock_db_manager, "test_hotkey")

    def test_perturbation_preserves_normalization(self):
        """Test that perturbed weights remain normalized."""
        # Create sample weights
        n = 256
        base_weights = np.random.random(n)
        base_weights = base_weights / base_weights.sum()

        # Apply perturbation
        seed = secrets.token_bytes(32)
        perturbed = self.manager.perturb_weights(base_weights, seed)

        # Check normalization
        assert (
            abs(perturbed.sum() - 1.0) < 1e-9
        ), f"Perturbed weights not normalized: sum={perturbed.sum()}"

    def test_perturbation_preserves_top_k(self):
        """Test that perturbation preserves top-k miners."""
        # Create power-law distributed weights
        n = 256
        x = np.arange(1, n + 1)
        base_weights = 1 / x**1.5
        base_weights = base_weights / base_weights.sum()

        # Apply perturbation
        seed = secrets.token_bytes(32)
        perturbed = self.manager.perturb_weights(base_weights, seed)

        # Check top-20 preservation
        k = 20
        top_base = set(np.argsort(base_weights)[-k:])
        top_perturbed = set(np.argsort(perturbed)[-k:])

        # Allow for small changes but expect high overlap
        overlap = len(top_base & top_perturbed)
        assert (
            overlap >= k * 0.8
        ), f"Top-{k} not sufficiently preserved: {overlap}/{k} overlap"

    def test_perturbation_l1_distance(self):
        """Test that L1 distance matches theoretical expectation."""
        n = 256
        base_weights = np.random.random(n)
        base_weights = base_weights / base_weights.sum()

        seed = secrets.token_bytes(32)
        perturbed = self.manager.perturb_weights(base_weights, seed)

        # Calculate L1 distance
        l1_distance = np.abs(perturbed - base_weights).sum()

        # Expected L1 distance should be approximately 2 * P_MOVE
        # (P_MOVE mass removed from all, P_MOVE mass added to tail)
        from gaia.validator.weight_perturbation import P_MOVE

        expected_l1 = 2 * P_MOVE

        # Allow for some tolerance due to normalization
        assert (
            abs(l1_distance - expected_l1) < 0.01
        ), f"L1 distance {l1_distance} != expected {expected_l1}"

    def test_deterministic_perturbation(self):
        """Test that same seed produces same perturbation."""
        n = 256
        base_weights = np.random.random(n)
        base_weights = base_weights / base_weights.sum()

        seed = secrets.token_bytes(32)

        # Apply perturbation twice with same seed
        perturbed1 = self.manager.perturb_weights(base_weights, seed)
        perturbed2 = self.manager.perturb_weights(base_weights, seed)

        # Should be identical
        assert np.allclose(
            perturbed1, perturbed2
        ), "Same seed did not produce identical perturbation"

    def test_different_seeds_produce_different_results(self):
        """Test that different seeds produce different perturbations."""
        n = 256
        base_weights = np.random.random(n)
        base_weights = base_weights / base_weights.sum()

        seed1 = secrets.token_bytes(32)
        seed2 = secrets.token_bytes(32)

        perturbed1 = self.manager.perturb_weights(base_weights, seed1)
        perturbed2 = self.manager.perturb_weights(base_weights, seed2)

        # Should be different
        assert not np.allclose(
            perturbed1, perturbed2
        ), "Different seeds produced identical perturbation"

    def test_tail_boost(self):
        """Test that bottom-k% miners receive boost."""
        n = 256
        # Create weights where bottom half has very low weight
        base_weights = np.ones(n)
        base_weights[: n // 2] = 0.001  # Bottom half gets tiny weight
        base_weights = base_weights / base_weights.sum()

        seed = secrets.token_bytes(32)
        perturbed = self.manager.perturb_weights(base_weights, seed)

        # Check that bottom half received boost
        from gaia.validator.weight_perturbation import TAIL_FR

        tail_size = int(n * TAIL_FR)
        sorted_indices = np.argsort(base_weights)
        tail_indices = sorted_indices[:tail_size]

        for idx in tail_indices[:10]:  # Check first 10 tail miners
            assert (
                perturbed[idx] > base_weights[idx]
            ), f"Tail miner {idx} did not receive boost"

    def test_vtrust_estimation(self):
        """Test v-trust estimation for weight copiers."""
        from gaia.validator.weight_perturbation import P_MOVE

        d_epochs = 5
        estimated_vtrust = self.manager.estimate_weight_copier_vtrust(d_epochs)

        # Expected: T = 1 - (d + 1) * p
        expected_vtrust = 1 - (d_epochs + 1) * P_MOVE

        assert (
            abs(estimated_vtrust - expected_vtrust) < 0.001
        ), f"V-trust estimation incorrect: {estimated_vtrust} != {expected_vtrust}"

        # Check that with default settings, v-trust is below 0.60
        assert (
            estimated_vtrust <= 0.60
        ), f"V-trust {estimated_vtrust} not below target 0.60"

    def test_disabled_perturbation(self):
        """Test that perturbation can be disabled."""
        with patch.dict(os.environ, {"PERTURB_P": "0"}):
            # Reload module to pick up env change
            import importlib
            import gaia.validator.weight_perturbation

            importlib.reload(gaia.validator.weight_perturbation)

            manager = gaia.validator.weight_perturbation.WeightPerturbationManager(
                self.mock_db_manager
            )

            n = 256
            base_weights = np.random.random(n)
            base_weights = base_weights / base_weights.sum()

            seed = secrets.token_bytes(32)
            perturbed = manager.perturb_weights(base_weights, seed)

            # Should be unchanged
            assert np.allclose(perturbed, base_weights), "Weights changed when P=0"

    @pytest.mark.asyncio
    async def test_seed_initialization(self):
        """Test seed table initialization."""
        # Mock database session
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 0  # No seed exists
        mock_session.execute.return_value = mock_result

        self.mock_db_manager.session.return_value.__aenter__.return_value = mock_session

        await self.manager.initialize_seed_table()

        # Check that INSERT was called
        assert mock_session.execute.call_count >= 2  # SELECT and INSERT
        insert_call = mock_session.execute.call_args_list[1]
        assert "INSERT INTO perturb_seed" in str(insert_call)

    @pytest.mark.asyncio
    async def test_seed_rotation(self):
        """Test seed rotation for primary validator."""
        # Set as primary
        self.manager.is_primary = True

        # Mock datetime to be at minute 55
        with patch("gaia.validator.weight_perturbation.datetime") as mock_dt:
            mock_now = datetime(2024, 1, 1, 12, 55, 0, tzinfo=timezone.utc)
            mock_dt.now.return_value = mock_now

            # Mock database session
            mock_session = AsyncMock()
            self.mock_db_manager.session.return_value.__aenter__.return_value = (
                mock_session
            )

            # Enable perturbation
            with patch.dict(os.environ, {"ENABLE_WEIGHT_PERTURBATION": "true"}):
                rotated = await self.manager.rotate_seed_if_needed()

                assert rotated, "Seed should have been rotated at minute 55"

                # Check UPDATE was called
                update_call = mock_session.execute.call_args
                assert "UPDATE perturb_seed" in str(update_call)

    @pytest.mark.asyncio
    async def test_seed_loading_with_cache(self):
        """Test seed loading with caching."""
        # Mock database response
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.seed_hex = "a" * 64  # 32-byte hex
        mock_row.generated_at = datetime.now(timezone.utc)
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result

        self.mock_db_manager.session.return_value.__aenter__.return_value = mock_session

        # First load - should hit database
        seed1 = await self.manager.load_seed()
        assert len(seed1) == 32
        assert mock_session.execute.call_count == 1

        # Second load within 5 minutes - should use cache
        seed2 = await self.manager.load_seed()
        assert seed1 == seed2
        assert mock_session.execute.call_count == 1  # No additional DB call

    @pytest.mark.asyncio
    async def test_get_perturbed_weights_integration(self):
        """Test the main interface for getting perturbed weights."""
        # Mock seed loading
        mock_seed = secrets.token_bytes(32)
        self.manager.load_seed = AsyncMock(return_value=mock_seed)

        # Create sample weights
        n = 256
        base_weights = np.random.random(n)
        base_weights = base_weights / base_weights.sum()

        # Get perturbed weights
        with patch.dict(os.environ, {"ENABLE_WEIGHT_PERTURBATION": "true"}):
            perturbed = await self.manager.get_perturbed_weights(base_weights)

        # Check properties
        assert abs(perturbed.sum() - 1.0) < 1e-9
        assert perturbed.shape == base_weights.shape

        # Check L1 distance is non-zero (perturbation applied)
        l1_distance = np.abs(perturbed - base_weights).sum()
        assert l1_distance > 0.01

    def test_monitoring_metrics(self):
        """Test monitoring metrics generation."""
        metrics = self.manager.get_monitoring_metrics()

        assert "enabled" in metrics
        assert "p_move" in metrics
        assert "tail_fraction" in metrics
        assert "is_primary" in metrics
        assert "estimated_wc_vtrust" in metrics

        # Check v-trust estimation is reasonable
        assert 0 <= metrics["estimated_wc_vtrust"] <= 1.0


def test_edge_cases():
    """Test edge cases and error conditions."""

    def test_empty_weights():
        """Test handling of empty weight array."""
        manager = WeightPerturbationManager(None)
        empty_weights = np.array([])
        seed = secrets.token_bytes(32)

        perturbed = manager.perturb_weights(empty_weights, seed)
        assert len(perturbed) == 0

    def test_single_miner():
        """Test with single miner (edge case)."""
        manager = WeightPerturbationManager(None)
        single_weight = np.array([1.0])
        seed = secrets.token_bytes(32)

        perturbed = manager.perturb_weights(single_weight, seed)
        assert abs(perturbed[0] - 1.0) < 1e-9  # Should remain 1.0

    def test_uniform_weights():
        """Test with uniform weight distribution."""
        manager = WeightPerturbationManager(None)
        n = 256
        uniform_weights = np.ones(n) / n
        seed = secrets.token_bytes(32)

        perturbed = manager.perturb_weights(uniform_weights, seed)

        # Check normalization
        assert abs(perturbed.sum() - 1.0) < 1e-9

        # Bottom half should have increased weight
        from gaia.validator.weight_perturbation import TAIL_FR

        tail_size = int(n * TAIL_FR)
        sorted_indices = np.argsort(uniform_weights)  # All same, so order is arbitrary

        # Average weight of bottom half should increase
        bottom_half_original = uniform_weights[:tail_size].mean()
        bottom_half_perturbed = perturbed[sorted_indices[:tail_size]].mean()
        assert bottom_half_perturbed > bottom_half_original


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
