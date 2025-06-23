"""
Weight Manager
=============

Bridge between functional scoring operations and the existing system
"""

import asyncio
import time
from typing import Any, Optional, Dict
from fiber.logging_utils import get_logger
from ...core.weight_calculations import WeightCalculationsManager

logger = get_logger(__name__)


class WeightManager:
    """
    Clean bridge between functional architecture and existing weight calculations.
    """
    
    def __init__(self):
        """Initialize the weight manager."""
        self.weight_calc_manager = WeightCalculationsManager()
        self._initialized = False
        
    async def initialize(self, neuron, database, memory_manager, main_execution_manager):
        """
        Initialize the weight manager with required components.
        
        Args:
            neuron: Network components
            database: Database manager  
            memory_manager: Memory management functions
            main_execution_manager: Main execution manager
        """
        try:
            logger.info("Initializing weight manager...")
            
            # Initialize the underlying weight calculations manager
            await self.weight_calc_manager.initialize(
                neuron, database, memory_manager, main_execution_manager
            )
            
            self._initialized = True
            logger.info("Weight manager initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing weight manager: {e}")
            raise
    
    async def run_scoring_cycle(self) -> bool:
        """
        Run a complete scoring cycle using the existing business logic.
        
        Returns:
            bool: True if scoring was successful
        """
        if not self._initialized:
            logger.error("Weight manager not initialized")
            return False
            
        try:
            # Delegate to the existing scoring logic
            await self.weight_calc_manager.main_scoring()
            return True
            
        except Exception as e:
            logger.error(f"Error in scoring cycle: {e}")
            return False
    
    def get_current_task_weights(self) -> Dict[str, float]:
        """
        Get current task weights for display/monitoring.
        
        Returns:
            Dict mapping task names to weights
        """
        if not self._initialized:
            return {}
            
        try:
            return self.weight_calc_manager.get_current_task_weights()
        except Exception as e:
            logger.error(f"Error getting current task weights: {e}")
            return {}
    
    async def update_last_weights_block(self):
        """Update the last weights block number."""
        if not self._initialized:
            return
            
        try:
            await self.weight_calc_manager.update_last_weights_block()
        except Exception as e:
            logger.error(f"Error updating last weights block: {e}")
    
    @property
    def last_set_weights_block(self) -> int:
        """Get the last block when weights were set."""
        if not self._initialized:
            return 0
        return getattr(self.weight_calc_manager, 'last_set_weights_block', 0)
    
    @property
    def last_successful_weight_set(self) -> float:
        """Get timestamp of last successful weight set."""
        if not self._initialized:
            return 0
        return getattr(self.weight_calc_manager, 'last_successful_weight_set', 0) 