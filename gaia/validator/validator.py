import multiprocessing
import time
import psutil
import signal
import os
from gaia.validator.app.io_engine import main as io_main
from gaia.validator.app.compute_worker import main as compute_main
from gaia.validator.utils.config import settings  # Assuming Phase 0 is complete


def main():
    """The main entrypoint for the Gaia Validator application."""
    config = settings

    # Use a manager for queues if more complex state needs to be shared in the future.
    # For now, simple queues are sufficient.
    work_queue = multiprocessing.Queue(maxsize=config.IPC_QUEUE_SIZE)
    result_queue = multiprocessing.Queue(maxsize=config.IPC_QUEUE_SIZE)

    children = {}

    def shutdown_gracefully(signum, frame):
        print("Supervisor: Signal received, shutting down all child processes...")
        for child in children.values():
            if child.is_alive():
                # Send SIGTERM to allow for graceful shutdown
                child.terminate()
        # Wait for processes to exit
        for child in children.values():
            child.join(timeout=5)
            if child.is_alive():
                # Force kill if still alive
                child.kill()
        print("Supervisor: Shutdown complete.")
        exit(0)

    signal.signal(signal.SIGINT, shutdown_gracefully)
    signal.signal(signal.SIGTERM, shutdown_gracefully)

    while True:
        # Spawn IO Engine
        io_proc = multiprocessing.Process(
            target=io_main,
            name="io_engine",
            args=(config, work_queue, result_queue),
            daemon=True
        )
        io_proc.start()
        children['io_engine'] = io_proc

        # Spawn Compute Pool
        for i in range(config.NUM_COMPUTE_WORKERS):
            name = f"compute_worker_{i}"
            compute_proc = multiprocessing.Process(
                target=compute_main,
                name=name,
                args=(config, work_queue, result_queue, i),  # Pass worker ID
                daemon=True
            )
            compute_proc.start()
            children[name] = compute_proc

        print(f"Supervisor: All {len(children)} processes started. Monitoring...")
        monitor_loop(children, config)

        # This point is only reached if a child failed and needs restarting
        print("Supervisor: Child process failure detected, restarting all processes in 5s...")
        time.sleep(5)


def monitor_loop(children, config):
    """Monitors child processes for crashes or excessive memory usage."""
    while True:
        time.sleep(config.SUPERVISOR_CHECK_INTERVAL_S)
        for name, proc in list(children.items()):
            if not proc.is_alive():
                print(f"Supervisor: Process {name} (PID {proc.pid}) died unexpectedly with exit code {proc.exitcode}!")
                # Terminate siblings before restarting
                for p in children.values():
                    p.terminate()
                return  # Exit inner loop to trigger a full restart

            try:
                rss_mb = psutil.Process(proc.pid).memory_info().rss / (1024 * 1024)
                limit_mb = config.PROCESS_MAX_RSS_MB.get(name.split('_')[0], 2048)
                if rss_mb > limit_mb:
                    print(f"Supervisor: Process {name} (PID {proc.pid}) exceeded memory limit ({rss_mb:.2f}MB > {limit_mb}MB)! Terminating all.")
                    for p in children.values():
                        p.terminate()
                    return  # Exit inner loop to trigger a full restart
            except psutil.NoSuchProcess:
                continue  # Process may have just been terminated; the is_alive() check will catch it next iteration


if __name__ == "__main__":
    # 'spawn' is safer and more consistent across platforms than 'fork'
    multiprocessing.set_start_method('spawn', force=True)
    main()
