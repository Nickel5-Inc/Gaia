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
from prefect import flow, task, get_run_logger
from datetime import timedelta, datetime, timezone
from uuid import uuid4


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

    # Default schedule for the task flow
    default_schedule: str = "*/5 * * * *"  # Every 5 minutes by default

    async def initialize_database(self):
        """Initialize task-specific database tables"""
        if not self.db:
            raise RuntimeError("Database not initialized")

    ############################################################
    # Validator methods
    ############################################################

    @abstractmethod
    async def fetch_data(self):
        """Fetch necessary data for task execution."""
        pass

    @abstractmethod
    async def query_miners(self, validator, data):
        """Query miners with the fetched data."""
        pass

    @abstractmethod
    async def process_responses(self, responses, context):
        """Process and validate miner responses."""
        pass

    @abstractmethod
    async def score_responses(self, responses, context):
        """Score the processed responses."""
        pass

    @abstractmethod
    async def update_scores(self, scores, context):
        """Update the scoring system with new scores."""
        pass

    def create_flow(self, validator):
        """Create a Prefect flow for this task."""
        task_name = self.name.lower().replace(" ", "_")
        
        @task(
            name=f"{task_name}_fetch",
            retries=3,
            retry_delay_seconds=60,
            description=f"Fetch data for {self.name}"
        )
        async def fetch_task():
            return await self.fetch_data()

        @task(
            name=f"{task_name}_query",
            retries=3,
            retry_delay_seconds=60,
            description=f"Query miners for {self.name}"
        )
        async def query_task(data):
            return await self.query_miners(validator, data)

        @task(
            name=f"{task_name}_process",
            retries=3,
            retry_delay_seconds=60,
            description=f"Process responses for {self.name}"
        )
        async def process_task(responses, context):
            return await self.process_responses(responses, context)

        @task(
            name=f"{task_name}_score",
            retries=3,
            retry_delay_seconds=60,
            description=f"Score responses for {self.name}"
        )
        async def score_task(responses, context):
            return await self.validator_score(responses, context)

        @task(
            name=f"{task_name}_update",
            retries=3,
            retry_delay_seconds=60,
            description=f"Update scores for {self.name}"
        )
        async def update_task(scores, context):
            return await self.update_scores(scores, context)

        @flow(
            name=f"{task_name}_flow",
            description=f"Flow for {self.name}",
            version=validator.args.version
        )
        async def task_flow():
            flow_logger = get_run_logger()
            flow_logger.info(f"Starting {self.name} flow")
            
            # Create execution context with validator instance
            context = {
                'current_time': datetime.now(timezone.utc),
                'validator': validator,
                'flow_id': str(uuid4())
            }
            
            # Execute flow steps
            data = await fetch_task()
            responses = await query_task(data)
            processed = await process_task(responses, context)
            scores = await score_task(processed, context)
            await update_task(scores, context)

        # Set the schedule from the task's default_schedule
        task_flow.default_schedule = self.default_schedule
        return task_flow

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
