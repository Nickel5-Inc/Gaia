from abc import ABC, abstractmethod


class GaiaTask(ABC):
    """
    An abstract base class that defines the interface for all validation tasks.
    The IO-Engine will interact with tasks solely through this interface.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """A unique, machine-readable name for the task (e.g., 'weather')."""
        ...

    @property
    @abstractmethod
    def cron_schedule(self) -> str:
        """An APScheduler-compatible cron string (e.g., '5 18 * * *' for every day at 18:05 UTC)."""
        ...

    @abstractmethod
    async def run_scheduled_job(self, io_engine) -> None:
        """
        The single entry point called by the scheduler. This method is responsible
        for orchestrating the task's entire validation lifecycle for one run,
        including dispatching work to the compute pool and updating the database.
        """
        ...