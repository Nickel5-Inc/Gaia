import time
import resource
import os
import queue
from gaia.validator.utils.ipc_types import ResultUnit

# This registry will be populated during the logic migration phase
HANDLER_REGISTRY = {}


def main(config, work_q, result_q, worker_id: int):
    """The main execution loop for a compute worker process."""
    pid = os.getpid()
    worker_name = f"ComputeWorker-{worker_id}"
    print(f"{worker_name} (PID: {pid}) started.")

    # Set a hard memory limit for this process to prevent runaway jobs
    limit_mb = config.PROCESS_MAX_RSS_MB.get('compute', 1024)
    limit_bytes = limit_mb * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))

    for job_number in range(config.MAX_JOBS_PER_WORKER):
        try:
            work_unit = work_q.get(timeout=config.WORKER_QUEUE_TIMEOUT_S)
            print(f"{worker_name}: Received job {job_number+1}/{config.MAX_JOBS_PER_WORKER}: {work_unit.job_id}")

            start_time = time.monotonic()
            try:
                handler = HANDLER_REGISTRY.get(work_unit.task_name)
                if not handler:
                    raise ValueError(f"No handler found for task: {work_unit.task_name}")

                # This is where the magic happens: the handler is a pure function
                result_payload = handler(config, **work_unit.payload)

                result = ResultUnit(
                    job_id=work_unit.job_id,
                    task_name=work_unit.task_name,
                    success=True,
                    result=result_payload,
                    worker_pid=pid,
                    execution_time_ms=0
                )
            except Exception as e:
                print(f"{worker_name} ERROR processing job {work_unit.job_id}: {e}")
                result = ResultUnit(
                    job_id=work_unit.job_id,
                    task_name=work_unit.task_name,
                    success=False,
                    error=f"{type(e).__name__}: {e}",
                    worker_pid=pid,
                    execution_time_ms=0
                )

            result.execution_time_ms = (time.monotonic() - start_time) * 1000
            result_q.put(result)

        except queue.Empty:
            # It's okay to time out, just means the queue is idle.
            # The worker can exit if it's been idle for too long.
            print(f"{worker_name}: Queue idle, exiting after {config.MAX_JOBS_PER_WORKER} jobs.")
            break
        except Exception as e:
            # Catch-all for unexpected errors in the worker loop itself
            print(f"{worker_name} CRITICAL ERROR: {e}")
            # Exit to be restarted by the supervisor
            break

    print(f"{worker_name} (PID: {pid}) finished max jobs or was idle, exiting cleanly.")