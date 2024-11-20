from database.database_manager import BaseDatabaseManager


class MinerDatabaseManager(BaseDatabaseManager):
    def __init__(self):
        super().__init__("miner")
