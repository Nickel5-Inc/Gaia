class Scoring:
    """
    Scoring Class - Instantiated by the validator,
    Handles aggregation of scores from multiple miners, and calculating final weights.

    """

    def __init__(self, db_manager):
        self.db_manager = db_manager
        pass

    def score(self):
        pass

    def _fetch_task_scores(self):
        """
        Fetches scores from the database for all tasks. Tasks should submit scores as an array of 256 floats. The scoring run will average these scores in
        buckets for each tasks.
        """
        pass

    def _aggregate_scores(self):
        pass

    def _calculate_final_weights(self):
        pass
