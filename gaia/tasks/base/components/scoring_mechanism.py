

from typing import Dict, Any
from abc import ABC, abstractmethod
from pydantic import BaseModel
from ..decorators import task_timer
from .inputs import Inputs
from .outputs import Outputs

class ScoringMechanism(BaseModel, ABC):
    '''
    The ScoringMechanism base class represents the scoring mechanism of a subtask.
    Should be extended to define specific scoring mechanisms for different subtasks, to be run by the validator.

    - Inputs here are NOT the same as the inputs of the subtask. Rather, they are the inputs of the scoring mechanism (Ground Truth, API data, etc)
    
    - Outputs ARE the the result of the subtask completed by the miner.

    '''
    inputs: Dict[str, Any]
    outputs: Dict[str, Any]
 


    @abstractmethod
    @task_timer
    def score(self, inputs: Inputs, outputs: Outputs):
        '''
        Score the outputs of a subtask. 
        '''
        pass