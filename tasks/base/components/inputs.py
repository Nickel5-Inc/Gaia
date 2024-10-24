
from pydantic import BaseModel

class Inputs(BaseModel):
    '''
    The Inputs class represents the input data for a subtask. Should be extended to define specific input types. Base class is just an empty pydantic model. Should be able to handle any input data type.
    Most common types will be images, data arrays, text, etc.
    
    '''
    def __init__(self):
        self.inputs = BaseModel()