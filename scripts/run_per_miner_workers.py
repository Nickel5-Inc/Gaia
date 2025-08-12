#!/usr/bin/env python3
import asyncio
import os

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline.workers import (
    process_verify_one,
    process_day1_one,
    process_era5_one,
)


async def main():
    db = ValidatorDatabaseManager()
    await db.initialize_database()
    try:
        processed_any = False
        if await process_verify_one(db):
            print("[workers] processed one verify")
            processed_any = True
        if await process_day1_one(db):
            print("[workers] processed one day1")
            processed_any = True
        if await process_era5_one(db):
            print("[workers] processed one era5")
            processed_any = True
        if not processed_any:
            print("[workers] no work processed (no candidates or not yet implemented)")
    finally:
        await db.close_all_connections()


if __name__ == "__main__":
    asyncio.run(main())


