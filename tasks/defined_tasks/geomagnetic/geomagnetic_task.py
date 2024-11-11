
from tasks.base.task import Task
from tasks.defined_tasks.geomagnetic.geomagnetic_metadata import GeomagneticMetadata
from tasks.defined_tasks.geomagnetic.geomagnetic_inputs import GeomagneticInputs
from tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import GeomagneticPreprocessing
from tasks.defined_tasks.geomagnetic.geomagnetic_scoring_mechanism import GeomagneticScoringMechanism
from tasks.defined_tasks.geomagnetic.geomagnetic_outputs import GeomagneticOutputs


class GeomagneticTask(Task):
    """
       A task class for processing and analyzing geomagnetic data.

       The task workflow includes:
           1. Metadata Configuration: Initializes task-specific metadata
              for data source tracking, date range configuration, and other
              relevant parameters through the GeomagneticMetadata class.

           2. Input Handling: Uses GeomagneticInputs to load and validate raw
              geomagnetic data from specified sources or file paths.

           3. Preprocessing: Applies geomagnetic-specific preprocessing steps
              on validated data via GeomagneticPreprocessing, utilizing utilities
              for data transformation, filtering, and structuring.

           4. Scoring Mechanism: Calculates scores or performance metrics
              based on the preprocessed data using GeomagneticScoringMechanism,
              which supports custom scoring logic tailored for geomagnetic data.

           5. Output Management: Formats and saves the task’s output, ensuring
              results are stored in the desired format and location, managed
              by the GeomagneticOutputs component.

       Attributes:
           name (str): The name of the task, set as "GeomagneticTask".
           description (str): A description of the task's purpose.
           task_type (str): Specifies the type of task (e.g., "atomic").
           metadata (GeomagneticMetadata): Metadata associated with the task.
           inputs (GeomagneticInputs): Handles data loading and validation.
           preprocessing (GeomagneticPreprocessing): Processes raw data.
           scoring_mechanism (GeomagneticScoringMechanism): Computes scores.
           outputs (GeomagneticOutputs): Manages output formatting and saving.

       Example:
           task = GeomagneticTask()
           task.validator_execute()

       This example initializes a GeomagneticTask instance and executes
       the task, performing each stage of data processing and scoring.
       """
    def __init__(self):
        super().__init__(
            name="GeomagneticTask",
            description="Task for geomagnetic data processing",
            task_type="atomic",
            metadata=GeomagneticMetadata(),
            inputs=GeomagneticInputs(),
            preprocessing=GeomagneticPreprocessing(),
            scoring_mechanism=GeomagneticScoringMechanism(),
            outputs=GeomagneticOutputs()
        )

    def validator_execute(self):
        data = self.inputs.load_data("input_path")
        validated_data = self.inputs.validate_data(data)
        preprocessed_data = self.preprocessing.preprocess(validated_data)
        score = self.scoring_mechanism.calculate_score(preprocessed_data)
        formatted_results = self.outputs.format_results(score)
        self.outputs.save_results(formatted_results, "output_path")

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