"""
Gaia Validator Service-Oriented Architecture

Simple, lightweight service architecture for the Gaia Validator that replaces
key system components with thread-safe services.

Core Services:
- DatabaseService: Thread-safe database operations (replaces ValidatorDatabaseManager)
- NetworkService: Miner communication (wraps core/network and core/http_client)  
- ExecutionService: Code execution in threads/processes (general-purpose executor)

Architecture:
- Direct async calls (thread-safe via SQLAlchemy)
- Optional AsyncMessageBus for coordination when needed
- No complex orchestration - validator acts as orchestrator
"""

__version__ = "2.0.0"  
__phase__ = "Simplified Core Services"

# Export core services
from .database.service import DatabaseService
from .network.service import NetworkService
from .execution.service import ExecutionService, ExecutionMode, ExecutionResult

# Export messaging for optional coordination
from .messaging.bus import AsyncMessageBus

__all__ = [
    'DatabaseService',
    'NetworkService', 
    'ExecutionService',
    'ExecutionMode',
    'ExecutionResult',
    'AsyncMessageBus',
] 