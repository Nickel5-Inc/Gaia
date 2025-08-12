from __future__ import annotations

from datetime import timedelta


def get_effective_gfs_init(task, gfs_init):
    """Return adjusted GFS init for test mode hindcast.

    If task.test_mode is True, shift gfs_init backward by config:
    - test_hindcast_days (int, default 0)
    - test_hindcast_hours (int, default 0)
    Otherwise, return original.
    """
    try:
        if getattr(task, "test_mode", False):
            # Defaults: -7 days hindcast
            days = int(getattr(task.config, "test_hindcast_days", task.config.get("test_hindcast_days", 7)))
            hours = int(getattr(task.config, "test_hindcast_hours", task.config.get("test_hindcast_hours", 0)))
            if days or hours:
                return gfs_init - timedelta(days=days, hours=hours)
    except Exception:
        pass
    return gfs_init


