"""
Core modules for the Gaia Validator.

This package contains the core functionality broken down into logical modules:
- neuron: Neuron setup, substrate connections, metagraph synchronization
- miner_comms: Miner communication, handshakes, HTTP client management
- scoring_engine: Weight calculations, scoring algorithms, normalization
- miner_manager: Miner state management, registration/deregistration
- task_orchestrator: Task scheduling, health monitoring, watchdog operations
"""

from .neuron import ValidatorNeuron
from .miner_comms import MinerCommunicator
from .scoring_engine import ScoringEngine
from .miner_manager import MinerManager
from .task_orchestrator import TaskOrchestrator

__all__ = [
    'ValidatorNeuron',
    'MinerCommunicator', 
    'ScoringEngine',
    'MinerManager',
    'TaskOrchestrator'
]