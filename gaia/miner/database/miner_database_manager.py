from gaia.database.database_manager import BaseDatabaseManager


class MinerDatabaseManager(BaseDatabaseManager):
    def __init__(self):
        # Ensure "miner" fulfills the required node_type argument for BaseDatabaseManager
        super().__init__(node_type="miner")
