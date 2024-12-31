from pydantic import BaseModel, Field
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from ..decorators import task_timer
from .inputs import Inputs
from .outputs import Outputs


class ScoringMechanism(BaseModel, ABC):
    """Base class for all scoring mechanisms.

    The ScoringMechanism base class represents the scoring mechanism of a subtask.
    Should be extended to define specific scoring mechanisms for different subtasks, to be run by the validator.

    - Inputs here are NOT the same as the inputs of the subtask. Rather, they are the inputs of the scoring mechanism (Ground Truth, API data, etc)

    - Outputs ARE the the result of the subtask completed by the miner.

    inputs: Dict[str, Any]
    outputs: Dict[str, Any]
    """

    name: str = Field(..., description="Name of the scoring mechanism")
    description: str = Field(..., description="Description of how scoring works")
    normalize_score: bool = Field(
        default=True, description="Whether to normalize the score"
    )
    max_score: float = Field(default=100.0, description="Maximum possible score")

    model_config = {"arbitrary_types_allowed": True}

    @abstractmethod
    @task_timer
    def score(self, predictions: Any, ground_truth: Any) -> Dict[str, float]:
        """
        Score the outputs of a subtask against ground truth.
        Returns a dictionary of score metrics.
        """
        pass
