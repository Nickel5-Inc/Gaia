from abc import ABC, abstractmethod

from typing import Dict, Any, Optional
from ..decorators import handle_validation_error
from pydantic import BaseModel, Field


class CoreMetadata(BaseModel):
    """
    Core metadata is required for all tasks and subtasks and includes information such as the name, description, and dependencies.
    """

    name: str = Field(..., description="The name of the task or subtask.")
    description: str = Field(..., description="A description of the task or subtask.")
    dependencies_file: str = Field(
        ...,
        description="Path to the file containing the list of dependencies for this task or subtask.",
    )
    hardware_requirements_file: str = Field(
        ...,
        description="Path to the file containing the hardware requirements for this task or subtask. YML file.",
    )
    author: str = Field(
        ..., description="The name of the author of the task or subtask."
    )
    version: str = Field(..., description="The version of the task or subtask.")


class Metadata(BaseModel, ABC):
    """
    The Metadata class represents the metadata of a task or subtask. It is meant to be extended to define specific metadata for a task or subtask.
    Metadata is used to store information about the task or subtask, such as the dependencies, the hardware requirements, the expected input and output, etc.

    It is split into two parts: core_metadata and extended_metadata.
    Core metadata is required for all tasks and subtasks and includes information such as the name, description, and dependencies.

    Extended metadata is optional and can be used to store additional information about the task or subtask.
    """

    core_metadata: CoreMetadata

    extended_metadata: Optional[Dict[str, Any]] = None

    @abstractmethod
    @handle_validation_error
    def validate_metadata(
        self, core_metadata: Dict[str, Any], extended_metadata: Dict[str, Any]
    ):
        """
        Validate the metadata against the metadata schema.
        - Decorator will catch any validation errors and raise them as system errors.
        - Should provide clear and helpful error messages.
        """
        pass
