from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class Endpoint:
    """Generic network endpoint for a miner.

    - url: Optional fully qualified URL (e.g., https://1.2.3.4:8091/forward). Not required if axon_info is present.
    - hotkey: Optional miner hotkey (ss58) for identity.
    - axon_info: Optional Bittensor "AxonInfo" (or equivalent) object returned by metagraph.

    Intention: Business logic depends only on Endpoint; adapters know how to use either a URL
    or native Bittensor axon_info when talking to dendrite. This keeps the rest of the codebase
    free of SDK-specific types but allows zero-copy usage of the metagraph data when available.
    """

    url: Optional[str] = None
    hotkey: Optional[str] = None
    axon_info: Optional[Any] = None


