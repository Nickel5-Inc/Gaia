## Coding and Module Guidelines (NASA-inspired)

### File and Module Limits
- Max 500 LOC per file. Split modules proactively.
- Single responsibility per module. Avoid deep nesting and implicit coupling.
- Async-first APIs; do not encode sync/async in names.

### Structure
- `new/validator/core/` contains pure contracts and datatypes; no side effects.
- `new/validator/` contains validator-side runtime services (queues, pools, executors).
- `new/shared/` contains adapters and cross-role utilities (e.g., chain clients).
- `new/adapters/` for legacy integration layers (e.g., weather task bridge).

### Interfaces and Naming
- Use explicit type hints. Avoid `Any` in public APIs.
- Verb-based function names; nouns for data objects.
- Avoid chains of `elif`; prefer mappings/dispatch tables.

### Concurrency
- Prefer `asyncio` for IO; use process pools for CPU-bound sections.
- Process pool must use `spawn`; avoid global heavy imports on module import.
- All long-running services provide start/stop and graceful shutdown.

### Testing and Observability
- Each component has minimal unit tests and integration points with logging.
- Do not log excessively in import-time code.

### Migration Rules
- No schema changes without explicit approval.
- No new legacy fallbacks; controlled feature flags only during transition.


