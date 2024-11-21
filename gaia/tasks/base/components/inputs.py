from pydantic import BaseModel, field_validator
from abc import ABC, abstractmethod
from typing import Dict, Any
from ..decorators import handle_validation_error


class Inputs(BaseModel, ABC):
    """
    The Inputs class represents the input data for a subtask. Should be extended to define specific input schemas for different subtasks. Base class is just an empty pydantic model. Should be able to handle any input data type.
    Most common types will be images, data arrays, text, etc.

    """

    inputs: Dict[str, Any]

    @abstractmethod
    @handle_validation_error
    def validate_inputs(self, inputs: Dict[str, Any]):
        """
        Validate the inputs against the input schema.
        - Must raise a ValidationError if the inputs are invalid.
        - Should provide clear and helpful error messages.
        """
        pass
