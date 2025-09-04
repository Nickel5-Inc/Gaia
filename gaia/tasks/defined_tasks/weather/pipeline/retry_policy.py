from __future__ import annotations

from datetime import datetime, timedelta, timezone


DEFAULTS = {
    "verify": {"base_minutes": 30, "cap_minutes": 60},
    "day1": {"base_minutes": 30, "cap_minutes": 120},
    "era5": {"base_minutes": 30, "cap_minutes": 180},
}


def next_retry_time(step_name: str, retry_count: int) -> datetime:
    cfg = DEFAULTS.get(step_name, DEFAULTS["verify"])  # fallback
    base = cfg["base_minutes"]
    cap = cfg["cap_minutes"]
    # Exponential backoff with cap: base * 2^(n-1)
    minutes = min(base * (2 ** max(0, retry_count - 1)), cap)
    # Enforce global minimum wait of 30 minutes after failure
    minutes = max(30, minutes)
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def compute_next_retry(
    *,
    attempt: int,
    base_delay_seconds: int = 1800,
    backoff_type: str = "exponential",
    cap_seconds: int | None = None,
    now: datetime | None = None,
) -> datetime:
    """Generic backoff calculator with global 30-minute minimum.

    - backoff_type: 'exponential' | 'linear' | 'none'
    - attempt: 1-based attempt counter
    """
    if now is None:
        now = datetime.now(timezone.utc)
    # Enforce global minimum
    base_delay_seconds = max(1800, int(base_delay_seconds or 0))
    if backoff_type == "linear":
        delay = base_delay_seconds * max(1, attempt)
    elif backoff_type == "none":
        delay = base_delay_seconds
    else:
        delay = base_delay_seconds * (2 ** max(0, attempt - 1))
    if cap_seconds is not None and cap_seconds > 0:
        delay = min(delay, cap_seconds)
    delay = max(1800, delay)
    return now + timedelta(seconds=delay)


