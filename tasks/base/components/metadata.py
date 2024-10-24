


from pydantic import BaseModel




class Metadata:
    def __init__(self):
        """
        The Metadata class represents the metadata of a task or subtask. It is meant to be extended to define specific metadata for a task or subtask.
        Metadata is used to store information about the task or subtask, such as the dependencies, the hardware requirements, the expected input and output, etc.

        """
        
