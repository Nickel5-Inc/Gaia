"""
Base Task Classes

Provides the foundational structure for validator and miner tasks.
Each task type has its own flow structure and responsibilities:
- ValidatorTask: Orchestration, verification, and scoring
- MinerTask: Data processing and computation
"""

from prefect import flow, task
from pydantic import BaseModel, Field
from .components.metadata import Metadata
from .components.inputs import Inputs
from .components.outputs import Outputs
from .components.scoring_mechanism import ScoringMechanism
from .components.preprocessing import Preprocessing
from .decorators import task_timer
from abc import ABC, abstractmethod
import networkx as nx
from typing import Optional, Any, Dict, Protocol
from datetime import timedelta
from prefect.tasks import task_input_hash


class BaseTask(BaseModel, ABC):
    """Common base for both validator and miner tasks"""
    
    name: str = Field(..., description="Task name")
    description: str = Field(..., description="Task description")
    schema_path: str = Field(..., description="Path to task schema file")
    db: Optional[Any] = Field(None, description="Database connection")
    
    # Task state tracking
    state: Dict[str, Any] = Field(
        default_factory=lambda: {
            'status': 'idle',
            'last_execution': None,
            'errors': [],
            'metrics': {}
        }
    )

    class Config:
        arbitrary_types_allowed = True

    async def initialize_database(self):
        """Initialize task-specific database tables"""
        if not self.db:
            raise RuntimeError("Database not initialized")

    async def _handle_error(self, error: Exception):
        """Unified error handling"""
        self.state['errors'].append(str(error))
        self.state['status'] = 'failed'
        self.state['metrics']['last_error_time'] = timedelta.now()


class ValidatorTask(BaseTask):
    """
    Base class for validator tasks.
    Responsible for:
    1. Task orchestration
    2. Miner response verification
    3. Score calculation and storage
    """
    
    # For composite validator tasks
    subtasks: Optional[nx.Graph] = Field(None, description="DAG for composite tasks")

    @flow(name="validator_run", retries=3)
    async def run(self, **kwargs):
        """Main validator flow"""
        try:
            if not self.db:
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
        if self.db:
            await self.db.store_score(self.name, score)


class MinerTask(BaseTask):
    """
    Base class for miner tasks.
    Responsible for:
    1. Data preprocessing
    2. Task computation
    3. Result formatting
    """

    @flow(name="miner_run", retries=3)
    async def run(self, **kwargs):
        """Main miner flow"""
        try:
            if not self.db:
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
