
from tasks.base.task import Task
from tasks.defined_tasks.geomagnetic.geomagnetic_metadata import GeomagneticMetadata
from tasks.defined_tasks.geomagnetic.geomagnetic_inputs import GeomagneticInputs
from tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import GeomagneticPreprocessing
from tasks.defined_tasks.geomagnetic.geomagnetic_scoring_mechanism import GeomagneticScoringMechanism
from tasks.defined_tasks.geomagnetic.geomagnetic_outputs import GeomagneticOutputs




class GeomagneticTask(Task):
    '''
    Geomagnetic prediction task class.

    (Detailed Description)
    '''
    def __init__(self):
        super().__init__(name="GeomagneticTask",
                         description="Task for geomagnetic data processing",
                         task_type="atomic",
                         metadata=GeomagneticMetadata(),
                         inputs=GeomagneticInputs(),
                         preprocessing=GeomagneticPreprocessing(),
                         scoring_mechanism=GeomagneticScoringMechanism(),
                         outputs=GeomagneticOutputs())
    

    ############################################################
    # Validator methods
    ############################################################

    def validator_prepare_subtasks(self):
        '''
        Atomic task, so no subtasks to prepare.
        '''
        pass

    def validator_execute(self):
        '''
        Execute the geomagnetic prediction task.
        Calls preprocessing, querying, and scoring methods.
        '''

        # decide which miners to query

        # call the preprocessing methods

        # query the miners and collect the results

        # call the scoring method on the results

        #return scores to the validator scoring aggregator method

        
        pass
    
    def validator_preprocess(self):
        '''
        Atomic task, so no preprocessing to perform.
        '''
        pass

    def validator_score(self):
        '''
        Atomic task, so no scoring to perform.
        '''
        pass



    ############################################################
    # Miner methods
    ############################################################

    def miner_preprocess(self):
        pass

    def miner_execute(self):
        pass