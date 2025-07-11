"""
Soil Scoring Mechanism compatibility module.

This module provides backward compatibility by importing from the legacy 
soil_scoring_mechanism_legacy module.

This is part of the task modularization process.
"""

# Import from legacy implementation
from .soil_scoring_mechanism_legacy import SoilScoringMechanism

# Re-export for backward compatibility
__all__ = [
    "SoilScoringMechanism",
] 