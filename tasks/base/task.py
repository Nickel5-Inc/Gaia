from .subtask import Subtask
from .components.metadata import Metadata
import networkx as nx



class Task:
    def __init__(self, name: str, description: str, subtasks: nx.Graph, metadata: Metadata):
        """
        The Task base class represents a main task that is completed by miners. It is composed of one or more subtasks,
        which could be chained in a linear or graph structure. This class is meant to be extended to define specific
        types of tasks. The definition should include the following:
        - The name of the task
        - The description of the task
        - The subtasks that are part of the task, and their definitions
        - The structure of subtasks, i.e. which subtasks need to be completed before others, which are parallel, etc.
        - The dependencies of the task, including libraries, etc.
        """
        self.name = name
        self.description = description
        self.subtasks = subtasks
        self.metadata = metadata
