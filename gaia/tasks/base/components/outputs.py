from pydantic import BaseModel
from abc import ABC, abstractmethod
from typing import Dict, Any
from ..decorators import handle_validation_error


class Outputs(BaseModel, ABC):
    """
    The Outputs data class represents the output data for a subtask. Should be extended to define specific output types.
    """

    outputs: Dict[str, Any]

    @abstractmethod
    @handle_validation_error
    def validate_outputs(self, outputs: Dict[str, Any]):
        """
        Validate the outputs against the output schema.
        """
        pass
