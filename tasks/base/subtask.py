
from pathlib import Path
from tasks.base.components.inputs import Inputs
from tasks.base.components.outputs import Outputs
from tasks.base.components.scoring_mechanism import ScoringMechanism
from tasks.base.components.preprocessing import Preprocessing
from tasks.base.decorators import task_timer


class Subtask:
    def __init__(self,
                 name: str,
                 description: str,
                 inputs: Inputs,
                 outputs: Outputs,
                 scoring_mechanism: ScoringMechanism,
                 preprocessing: Preprocessing):
        """
        The Subtask base class represents a single subtask that is part of a task. It is meant to be extended to define a specific model workload that a single miner completes.
        The definition should include the following:

        name:
        - The name of the subtask

        description:
        - Text description of the subtask

        metadata:
        - The dependencies of the subtask, including libraries, apis etc.
        - Any additional metadata that is relevant to the subtask, such as the number of epochs, the batch size, the learning rate, etc.
        - The hardware requirements of the subtask, including the number of GPUs, CPUs, memory, etc.

        inputs:
        - The expected input of the subtask, including the format of the input and the expected results.
        - Any preprocessing steps that need to be applied to the input data.

        outputs:
        - The expected output of the subtask, including the format of the output and the expected results.

        scoring_mechanism:
        - The scoring mechanism of the subtask, including the metric to be optimized and the expected range of values.    
        """


        self.name = name
        self.description = description
        self.inputs = inputs
        self.outputs = outputs
        self.scoring_mechanism = scoring_mechanism
        self.preprocessing = preprocessing

    @task_timer
    def execute(self):
        '''
        Execute the subtask based on the schema.
        '''
        pass