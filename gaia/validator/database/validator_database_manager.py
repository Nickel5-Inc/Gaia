import warnings
from typing import Any, Dict, Optional, List
from datetime import datetime
from .database_manager import ValidatorDatabaseManager as NewValidatorDatabaseManager

class ValidatorDatabaseManager(NewValidatorDatabaseManager):
    """
    Compatibility layer for the old ValidatorDatabaseManager.
    Redirects calls to the new consolidated manager while maintaining the old interface.
    """
    
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "Using deprecated ValidatorDatabaseManager. Please migrate to the new database_manager.ValidatorDatabaseManager",
            DeprecationWarning,
            stacklevel=2
        )
        super().__init__(*args, **kwargs)

    # Compatibility methods that map to new methods
    async def initialize_database(self, session=None):
        """Initialize database tables and schemas for validator tasks."""
        await super().initialize_database()
        await self.ensure_node_table_size()

    async def update_miner_info(
        self,
        index: int,
        hotkey: str,
        coldkey: str,
        ip: str = None,
        ip_type: str = None,
        port: int = None,
        incentive: float = None,
        stake: float = None,
        trust: float = None,
        vtrust: float = None,
        protocol: str = None,
    ):
        """Compatibility method for updating miner information."""
        await super().update_miner_info(
            index=index,
            hotkey=hotkey,
            coldkey=coldkey,
            ip=ip,
            ip_type=ip_type,
            port=port,
            incentive=incentive,
            stake=stake,
            trust=trust,
            vtrust=vtrust,
            protocol=protocol
        )

    async def get_connection(self):
        """Provide a database session/connection."""
        return await super().get_connection()

    async def clear_miner_info(self, index: int):
        """Clear miner information at a specific index."""
        await super().clear_miner_info(index)

    async def get_miner_info(self, index: int) -> Optional[Dict[str, Any]]:
        """Get miner information from the database."""
        return await super().get_miner_info(index)

    async def get_all_active_miners(self) -> List[Dict[str, Any]]:
        """Get information for all miners with non-null hotkeys."""
        return await super().get_all_active_miners()

    async def get_recent_scores(self, task_type: str) -> List[float]:
        """Compatibility method for fetching recent scores."""
        scores = await super().get_recent_scores(task_name=task_type, limit=100)
        # Convert to old format (256-length float array)
        if not scores:
            return [float('nan')] * 256
        return scores[0]['score'] if scores else [float('nan')] * 256
