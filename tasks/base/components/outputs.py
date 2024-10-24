
from pydantic import BaseModel

class Outputs(BaseModel):
    '''
    The Outputs class represents the output data for a subtask. Should be extended to define specific output types.
    '''
    def __init__(self):
        self.outputs = BaseModel()
        