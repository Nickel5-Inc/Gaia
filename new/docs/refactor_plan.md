## Gaia Refactor Plan and Decision Log

Scope: Extract a general, composable worker/queue/executor architecture, reorganize modules per NASA coding standards, and methodically migrate weather logic without breaking current production.

### Objectives
- Enforce ≤500 LOC per file, clear module boundaries, single responsibility.
- Centralize core contracts: Task, TaskContext, TaskResult, Job, Worker, Queue.
- Generalize worker execution and process pool separate from weather task.
- Remove dead/duplicate code paths during migration, with explicit replacements.

### Directory Plan (new/)
- `new/validator/core/`: Contracts, datatypes, and service abstractions (validator-facing).
- `new/validator/`: Validator-side executors, worker pools, queues, adapters.
- `new/miner/`: Miner-side services (migrated later).
- `new/shared/`: Chain clients, cross-cutting utilities.
- `new/docs/`: This plan, guidelines, migration notes.

### Milestones
1) Working doc + guidelines
2) Package scaffold and core contracts
3) In-memory queue + async WorkerPool
4) General memory-aware ProcessPool (spawn, psutil optional)
5) Weather adapter: route scoring jobs via new contracts
6) Remove legacy weather-local worker code after verification

### Progress Checklist
- [x] Create refactor working doc and decision log
- [ ] Scaffold new/ package hierarchy and __init__ files
- [ ] Define core contracts: Task, TaskContext, TaskResult, Worker, Job, Queue
- [ ] Document NASA-style file limits and module layout guidelines
- [ ] Inventory current worker/multiprocessing patterns in weather task
- [ ] Design generalizable worker execution model and queue interfaces
- [ ] Implement in-memory queue adapter and async worker pool
- [ ] Implement general memory-aware ProcessPool
- [ ] Draft migration plan for weather scoring to new architecture

### Current Worker/MP Inventory (source references)
- Weather task:
  - `gaia/tasks/defined_tasks/weather/utils/multiprocessing_pool.py`: Memory-aware ProcessPoolExecutor, psutil hooks, cleanup.
  - `gaia/tasks/defined_tasks/weather/weather_scoring_mechanism.py`: Async orchestration; multiprocessed climatology precompute.
  - `gaia/tasks/defined_tasks/weather/processing/weather_workers.py`: Initial scoring workers and cleanup workers (asyncio).
- Validator:
  - `gaia/validator/validator.py`: Standalone worker processes (`mp.Process`), worker monitor, job enqueuer startup.

Key gaps to generalize:
- Decouple pool lifecycle/config from task-specific modules.
- Standardize job envelope (id, payload, metadata) and result schema.
- Provide queue interface enabling in-memory now, pluggable later (e.g., Redis/SQS).
- Async-first API surface, no sync/async suffixes.

### Decisions
- API is asynchronous first to match system-wide preference and integration [[async-first]].
- Use `spawn` start method for process pools to avoid fork-safety issues.
- Keep psutil optional; degrade gracefully when unavailable.
- Enforce ≤500 LOC per file; split modules early to maintain limits.

### Migration Plan (draft)
1) Land contracts + infra (no integration yet).
2) Implement weather adapter that translates existing scoring jobs to `Job` and uses new `WorkerPool` for asyncio workers and `ProcessPool` for CPU tasks.
3) Redirect a small, safe subset of weather runs behind a feature flag to new path.
4) Compare outputs and performance; iterate and remove legacy-only branches.
5) Fully cutover; delete weather-local worker code that duplicates infra.

### Open Risks
- Hidden implicit coupling inside weather modules; address via adapter and incremental routing.
- Long import times in multiprocess; mitigate with lazy imports and fewer global side effects.

### Notes
- Follow `new/docs/guidelines.md` for file layout and review checklist.


