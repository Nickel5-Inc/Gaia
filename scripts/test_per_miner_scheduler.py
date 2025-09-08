#!/usr/bin/env python3
import asyncio

from gaia.tasks.defined_tasks.weather.pipeline import MinerWorkScheduler
from gaia.validator.database.validator_database_manager import \
    ValidatorDatabaseManager


async def main():
    db = ValidatorDatabaseManager()
    await db.initialize_database()

    sched = MinerWorkScheduler(db)

    to_verify = await sched.next_to_verify(limit=10)
    day1 = await sched.next_day1(limit=10)
    era5 = await sched.next_era5(limit=10)

    print(f"next_to_verify: {len(to_verify)}")
    if to_verify:
        print(f"sample verify: {to_verify[0]}")
    print(f"next_day1: {len(day1)}")
    if day1:
        print(f"sample day1: {day1[0]}")
    print(f"next_era5: {len(era5)}")
    if era5:
        print(f"sample era5: {era5[0]}")

    await db.close_all_connections()


if __name__ == "__main__":
    asyncio.run(main())


