from __future__ import annotations


def apply_gcsfs_threadsafe_close_patch() -> None:
    """Monkey-patch gcsfs GCSFileSystem.close_session to be thread-safe.

    Prevents cross-thread loop.create_task() at interpreter shutdown by using
    fsspec.asyn.sync when loop is running in another thread, and falling back
    to connector._close() as a last resort.
    Safe to call multiple times; no-op on failure.
    """
    try:
        import asyncio  # noqa: WPS433 - runtime import for tight scope
        from fsspec import asyn as fsspec_asyn  # noqa: WPS433
        import gcsfs.core as gcs_core  # noqa: WPS433

        def safe_close_session(loop, session, asynchronous=False):  # type: ignore[no-untyped-def]
            try:
                if session is None or getattr(session, "closed", False):
                    return
                try:
                    current_loop = asyncio.get_running_loop()
                except RuntimeError:
                    current_loop = None

                # Same loop/thread: schedule normally
                if loop is not None and loop.is_running() and current_loop is loop:
                    loop.create_task(session.close())
                    return

                # Cross-thread but loop is running: schedule thread-safe
                if loop is not None and loop.is_running():
                    try:
                        fsspec_asyn.sync(loop, session.close, timeout=0.2)
                        return
                    except Exception:
                        # Fall through to hard close
                        pass

                # We're in some running asyncio context and caller marked asynchronous
                if current_loop is not None and asynchronous and current_loop.is_running():
                    asyncio.create_task(session.close())
                    return

            except Exception:
                # Fall back to hard close
                pass

            # During shutdown or on failures, close connector directly
            connector = getattr(session, "_connector", None)
            if connector is not None:
                try:
                    connector._close()
                except Exception:
                    pass

        # Apply the monkey patch
        gcs_core.GCSFileSystem.close_session = staticmethod(safe_close_session)
    except Exception:
        # Never allow patch issues to break startup
        pass













