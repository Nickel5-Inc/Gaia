#!/usr/bin/env python3
import asyncio
import os

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline.workers import (
    process_verify_one,
    process_day1_one,
    process_era5_one,
    process_one,
)


async def main():
    db = ValidatorDatabaseManager()
    await db.initialize_database()
    try:
        if await process_one(db):
            print("[workers] processed one job")
        else:
            print("[workers] no work processed (no candidates)")
    finally:
        await db.close_all_connections()


if __name__ == "__main__":
    asyncio.run(main())


