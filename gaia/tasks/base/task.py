from pydantic import BaseModel, Field
from .components.metadata import Metadata
from .components.inputs import Inputs
from .components.outputs import Outputs
from .components.scoring_mechanism import ScoringMechanism
from .components.preprocessing import Preprocessing
from .decorators import task_timer
from abc import ABC, abstractmethod
import networkx as nx
from typing import Optional, Any


class Task(BaseModel, ABC):
    """Base class for all tasks in the system.

    A unified Task class that can represent either:
    1. A composite task made up of subtasks (when subtasks is provided)
    2. An atomic task (when subtasks is None)

    For composite tasks:
    - Orchestrates the execution of subtasks
    - Manages task dependencies and execution order
    - Aggregates results and scoring

    For atomic tasks:
    - Defines a specific workload
    - Handles direct input/output processing
    - Manages its own execution and scoring
    """

    # required fields
    name: str = Field(..., description="The name of the task")
    description: str = Field(..., description="Description of what the task does")
    task_type: str = Field(..., description="Type of task (atomic or composite)")
    metadata: Metadata = Field(..., description="Task metadata")

    # atomic tasks
    inputs: Optional[Inputs] = Field(None, description="Task inputs")
    outputs: Optional[Outputs] = Field(None, description="Task outputs")
    scoring_mechanism: Optional[ScoringMechanism] = Field(
        None, description="Scoring mechanism"
    )
    preprocessing: Optional[Preprocessing] = Field(
        None, description="Preprocessing steps"
    )

    # composite tasks
    subtasks: Optional[nx.Graph] = Field(
        None, description="Graph of subtasks for composite tasks"
    )

    db: Optional[Any] = Field(
        None, description="Database connection, set by validator/miner"
    )
    model_config = {"arbitrary_types_allowed": True}

    async def initialize_database(self):
        """Initialize task-specific database tables"""
        if not self.db:
            raise RuntimeError("Database not initialized")

    ############################################################
    # Validator methods
    ############################################################

    @abstractmethod
    @task_timer
    def validator_prepare_subtasks(self):
        """
        Prepare the subtasks for execution. Read the graph structure, determine the order of execution, and prepare the inputs for each subtask.
        """
        pass

    @abstractmethod
    @task_timer
    def validator_execute(self):
        """
        Execute the task from the validator neuron. For composite tasks, this orchestrates subtask execution.
        For atomic tasks, this executes the actual workload. This call should define the order and execution of the subtasks or
        any preprocessing that needs to be done, as well as calling the scoring method once miners have returned their results.
        """
        if self.subtasks is not None:
            # Composite task execution
            pass
        else:
            # Atomic task execution
            pass

    @abstractmethod
    @task_timer
    def validator_score(self, result=None):
        """
        Score the task results. For composite tasks, this may aggregate subtask scores.
        For atomic tasks, this uses the defined scoring mechanism.
        """
        if self.subtasks is not None:
            # Composite task scoring
            pass
        else:
            # Atomic task scoring using self.scoring_mechanism
            pass

    ############################################################
    # Miner methods
    ############################################################

    @abstractmethod
    @task_timer
    def miner_preprocess(
        self,
        preprocessing: Optional[Preprocessing] = None,
        inputs: Optional[Inputs] = None,
    ) -> Optional[Inputs]:
        """
        Preprocess the inputs for the miner neuron. Basically just wraps the defined preprocessing class in the task.
        """
        pass

    @abstractmethod
    @task_timer
    def miner_execute(self, inputs: Optional[Inputs] = None) -> Optional[Outputs]:
        """
        Execute the task from the miner neuron. For composite tasks, this should perform the workload for a specific subtask.
        For atomic tasks, this executes the actual workload.
        """
        pass
