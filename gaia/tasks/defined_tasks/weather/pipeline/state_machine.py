from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class MinerForecastState(str, Enum):
    received = "received"
    verified = "verified"
    verification_failed = "verification_failed"
    day1_scored = "day1_scored"
    era5_scoring = "era5_scoring"
    completed = "completed"
    failed = "failed"


@dataclass(frozen=True)
class StepKey:
    run_id: int
    miner_uid: int
    step: str
    lead_hours: Optional[int] = None

    def idempotency_key(self) -> str:
        if self.lead_hours is None:
            return f"{self.run_id}:{self.miner_uid}:{self.step}"
        return f"{self.run_id}:{self.miner_uid}:{self.step}:{self.lead_hours}"


def can_transition(current: str, target: str) -> bool:
    """Guard valid state transitions. States are intentionally permissive for recovery."""
    if current == target:
        return True
    allowed = {
        MinerForecastState.received: {
            MinerForecastState.verified,
            MinerForecastState.verification_failed,
        },
        MinerForecastState.verified: {
            MinerForecastState.day1_scored,
            MinerForecastState.era5_scoring,
            MinerForecastState.failed,
        },
        MinerForecastState.day1_scored: {
            MinerForecastState.era5_scoring,
            MinerForecastState.completed,
            MinerForecastState.failed,
        },
        MinerForecastState.era5_scoring: {
            MinerForecastState.completed,
            MinerForecastState.failed,
        },
        MinerForecastState.verification_failed: {
            MinerForecastState.failed,
        },
        MinerForecastState.completed: set(),
        MinerForecastState.failed: set(),
    }
    try:
        return MinerForecastState(target) in allowed[MinerForecastState(current)]
    except Exception:
        return False


