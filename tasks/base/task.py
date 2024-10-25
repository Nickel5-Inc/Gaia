from pydantic import BaseModel
from .subtask import Subtask
from .components.metadata import Metadata
from .decorators import task_timer
from abc import ABC, abstractmethod

from .components.scoring_mechanism import ScoringMechanism
import networkx as nx



class Task(BaseModel, ABC):
    def __init__(self, name: str, description: str, subtasks: nx.Graph, metadata: Metadata):
        """
        The Task base class represents a main task that is completed by miners. It is composed of one or more subtasks,
        which could be chained in a linear or graph-like structure. This class is meant to be extended to define specific
        types of tasks. The definition should include the following:
        - The name of the task
        - The description of the task
        - The subtasks that are part of the task, and their definitions
        - The structure of subtasks, i.e. which subtasks need to be completed before others, which are parallel, etc.
        - The dependencies of the task, including libraries, etc.

        The task class methods are executed by the validator neuron. The validator is responsible for:
        - Orchestrating the execution of the subtasks by delegating them to miners
        - Collecting the results from the miners
        - Scoring the results of subtasks using the defined scoring mechanism for each subtask
        - Performing any additional processing of the results as needed
        - Returning the results to the user/website/etc. 
        """
        self.name = name
        self.description = description
        self.subtasks = subtasks
        self.metadata = metadata

    def prepare_subtasks(self):
        '''
        Prepare the subtasks for execution. Read the graph structure, determine the order of execution, and prepare the inputs for each subtask.
        '''
        pass




    @abstractmethod
    @task_timer
    def execute(self):
        '''
        Execute the whole task based on the schema. 
        '''
        pass

    
    


    @abstractmethod
    @task_timer
    def score(self,subtask: Subtask):
        '''
        Score a subtask based on its defined scoring mechanism.
        '''
        pass