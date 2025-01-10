from abc import ABC, abstractmethod
from typing import Dict, Any, Union, Optional
from gaia.tasks.base.components.inputs import Inputs

class Preprocessing(ABC):
    """
    The Preprocessing base class represents the preprocessing steps that need to be applied to the input data for a subtask.
    Should be extended to define specific preprocessing steps.
    """

    def __init__(self):
        pass

    @abstractmethod
    def process_miner_data(self, data: Union[Dict[str, Any], Inputs]) -> Optional[Union[Dict[str, Any], Inputs]]:
        """
        Process raw data for model input on the miner side.

        Args:
            data: Raw data to be processed, either as a dictionary or Inputs object
        Returns:
            Processed data ready for model input, either as a dictionary or Inputs object
        """
        pass
