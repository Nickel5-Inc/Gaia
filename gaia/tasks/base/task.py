"""
Base Task Classes

Provides the foundational structure for validator and miner tasks.
Each task type has its own flow structure and responsibilities:
- ValidatorTask: Orchestration, verification, and scoring
- MinerTask: Data processing and computation
"""

from prefect import flow, task
from pydantic import BaseModel, Field, ConfigDict
from .components.metadata import Metadata
from .components.inputs import Inputs
from .components.outputs import Outputs
from .components.scoring_mechanism import ScoringMechanism
from .components.preprocessing import Preprocessing
from .decorators import task_timer
from abc import ABC, abstractmethod
import networkx as nx
from typing import Optional, Any, Dict, Protocol, List, ClassVar
from datetime import timedelta, datetime, timezone
from prefect.tasks import task_input_hash
import json
import traceback
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class TaskState(BaseModel):
    """State model for tasks"""
    status: str = "idle"
    last_run: Optional[datetime] = None
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    metrics: Dict[str, Any] = Field(default_factory=dict)

class ValidatorState(TaskState):
    """State model for validator tasks"""
    last_weight_set: Optional[int] = None
    last_score: Optional[float] = None
    miner_responses: Dict[str, Any] = Field(default_factory=dict)
    resource_usage: Dict[str, Any] = Field(default_factory=dict)

class BaseTask(BaseModel):
    """Base class for all tasks."""
    name: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    state: TaskState = Field(default_factory=TaskState)
    
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    def update_state(self, status: str, error: Optional[str] = None):
        """Update task state."""
        self.state.status = status
        self.state.last_run = datetime.now(timezone.utc)
        if error:
            self.state.errors.append({
                'time': datetime.now(timezone.utc),
                'error': error
            })
    
    def log_metrics(self, metrics: Dict[str, Any]):
        """Log task metrics."""
        self.state.metrics.update(metrics)
    
    def get_status(self) -> str:
        """Get current task status."""
        return self.state.status
    
    def get_errors(self) -> list:
        """Get task errors."""
        return self.state.errors
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get task metrics."""
        return self.state.metrics

class ValidatorTask(BaseTask):
    """
    Base class for validator tasks.
    Responsible for:
    1. Task orchestration
    2. Miner response verification
    3. Score calculation and storage
    """
    
    # For composite validator tasks
    subtasks: Optional[nx.Graph] = None
    node_type: str = "validator"
    test_mode: bool = False
    state: ValidatorState = Field(default_factory=ValidatorState)
    
    # Mark flow methods as class variables to exclude them from model fields
    run: ClassVar[Any]
    prepare_flow: ClassVar[Any]
    execute_flow: ClassVar[Any]
    score_flow: ClassVar[Any]
    
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **data):
        super().__init__(**data)
        self.initialize_validator_state()

    def initialize_validator_state(self):
        """Initialize validator-specific state."""
        self.state.update({
            'last_weight_set': None,
            'last_score': None,
            'miner_responses': {},
            'resource_usage': {}
        })

    def update_weight_set(self, block: int):
        """Update last weight set block."""
        self.state.last_weight_set = block

    def record_score(self, score: float):
        """Record task score."""
        self.state.last_score = score

    def record_miner_response(self, miner_key: str, response: Any):
        """Record miner response."""
        self.state.miner_responses[miner_key] = {
            'time': datetime.now(timezone.utc),
            'response': response
        }

    def update_resource_usage(self, usage: Dict[str, Any]):
        """Update resource usage metrics."""
        self.state.resource_usage = usage

    @flow(name="validator_run", retries=3)
    async def run(self, **kwargs):
        """Main validator flow"""
        try:
            if not hasattr(self, 'db'):
                await self.initialize_database()
            
            # Prepare validation parameters
            inputs = await self.prepare_flow(**kwargs)
            
            # Execute validation
            result = await self.execute_flow(inputs)
            
            # Score results
            if result is not None:
                score = await self.score_flow(result)
                if score is not None:
                    await self._store_score(score)
            
            return result
            
        except Exception as e:
            await self._handle_error(e)
            raise

    @abstractmethod
    @flow(name="validator_prepare", retries=2)
    async def prepare_flow(self, **kwargs) -> Dict[str, Any]:
        """
        Prepare validation parameters and criteria.
        Override this flow to implement task-specific preparation logic.
        """
        if self.subtasks:
            return {
                'order': list(nx.topological_sort(self.subtasks)),
                'params': kwargs
            }
        # Concrete tasks must override this if not using subtasks
        raise NotImplementedError("Validator prepare_flow must be implemented")

    @abstractmethod
    @flow(name="validator_execute", retries=2)
    async def execute_flow(self, inputs: Dict[str, Any]) -> Any:
        """
        Execute validation logic.
        Override this flow to implement task-specific validation logic.
        """
        if self.subtasks:
            results = {}
            for task_id in inputs['order']:
                subtask = self.subtasks.nodes[task_id]['task']
                results[task_id] = await subtask.run(**inputs['params'])
            return results
        # Concrete tasks must override this if not using subtasks
        raise NotImplementedError("Validator execute_flow must be implemented")

    @abstractmethod
    @flow(name="validator_score", retries=1)
    async def score_flow(self, result: Any) -> float:
        """
        Score miner responses.
        Override this flow to implement task-specific scoring logic.
        """
        if self.subtasks:
            scores = []
            for task_id, res in result.items():
                subtask = self.subtasks.nodes[task_id]['task']
                score = await subtask.score_flow(res)
                if score is not None:
                    scores.append(score)
            return sum(scores) / len(scores) if scores else None
        # Concrete tasks must override this if not using subtasks
        raise NotImplementedError("Validator score_flow must be implemented")

    async def _store_score(self, score: float):
        """Store task score in database"""
        if hasattr(self, 'db'):
            await self.db.store_score(self.name, score)


class MinerTask(BaseTask):
    """
    Base class for miner tasks.
    Responsible for:
    1. Data preprocessing
    2. Task computation
    3. Result formatting
    """
    node_type: str = "miner"
    test_mode: bool = False
    
    run: ClassVar[Any]
    prepare_flow: ClassVar[Any]
    execute_flow: ClassVar[Any]
    
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @flow(name="miner_run", retries=3)
    async def run(self, **kwargs):
        """Main miner flow"""
        try:
            if not hasattr(self, 'db'):
                await self.initialize_database()
            
            # Preprocess inputs
            inputs = await self.prepare_flow(**kwargs)
            
            # Execute computation
            result = await self.execute_flow(inputs)
            
            return result
            
        except Exception as e:
            await self._handle_error(e)
            raise

    @abstractmethod
    @flow(name="miner_prepare", retries=2)
    async def prepare_flow(self, **kwargs) -> Dict[str, Any]:
        """
        Preprocess input data.
        Override this flow to implement task-specific preprocessing logic.
        """
        raise NotImplementedError("Miner prepare_flow must be implemented")

    @abstractmethod
    @flow(name="miner_execute", retries=2)
    async def execute_flow(self, inputs: Dict[str, Any]) -> Any:
        """
        Execute computation logic.
        Override this flow to implement task-specific computation logic.
        """
        raise NotImplementedError("Miner execute_flow must be implemented")
