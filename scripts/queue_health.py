import asyncio
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager


async def main():
    db = ValidatorDatabaseManager()
    await db.initialize_database()

    qs = await db.get_queue_summary()
    ss = await db.get_step_summary()
    dups = await db.find_active_job_duplicates()

    print("=== validator_jobs summary ===")
    print(qs)
    print("\n=== weather_forecast_steps summary ===")
    print(ss)
    print("\n=== potential duplicate active jobs ===")
    print(dups)


if __name__ == "__main__":
    asyncio.run(main())


