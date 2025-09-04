import asyncio
import os
import sys
from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)


async def _run() -> None:
    try:
        # Optional uvloop
        try:
            import uvloop  # type: ignore
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except Exception:
            pass

        from gaia.validator.database.validator_database_manager import (
            ValidatorDatabaseManager,
        )
        from gaia.websync.worker import WebSyncWorker

        db = ValidatorDatabaseManager()
        try:
            await db.initialize_database()
        except Exception:
            # Proceed; queries will retry
            pass

        poll_seconds_env = os.getenv("WEBSYNC_POLL_SECONDS", "60")
        try:
            poll_seconds = int(poll_seconds_env)
        except Exception:
            poll_seconds = 60

        worker = WebSyncWorker(database_manager=db, poll_interval_seconds=poll_seconds)
        await worker.run()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"WebSync process fatal error: {e}")


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        try:
            logger.info("[WebSync] KeyboardInterrupt received - exiting cleanly")
        except Exception:
            pass
        sys.exit(0)


if __name__ == "__main__":
    main()


