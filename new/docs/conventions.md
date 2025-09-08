### Conventions

Structure

- One class per file (except pure dataclass/DTO bundles like protocol.py).
- Group by concern: core/, shared/, miner/, validator/ with clear subpackages.
- Keep files < 500 LOC; split earlier if needed.

Imports

- Prefer explicit imports from concrete modules to align with PEP 8.
- Avoid star imports and avoid relying on package-level re-exports in application code.
- Keep imports grouped: stdlib, third-party, local.

Config

- YAML per node type for operational knobs; .env only for secrets.
- Validators: endpoints + scheduler config; Miners: proxy + hosting.

Jobs & scheduling

- Jobs are typed and registered in a central registry; handlers are async and idempotent.
- Scheduler defines recurring jobs (interval+jitter) and enqueues; workers claim and execute.

Networking

- Axon/Dendrite hidden behind adapters; protocol (ForwardRequest/Response) is SDK-agnostic.
- Enforce protocol version and freshness in miner router.

Security

- Standardize on Let’s Encrypt for miners (HTTP-01) or DNS-01 when ports are unavailable.
- Validators are non-public; initiate outbound HTTPS only.
