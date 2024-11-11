from tasks.base.components.preprocessing import Preprocessing
from utils.process_geomag_data import get_latest_geomag_data

class GeomagneticPreprocessing(Preprocessing):
    def preprocess(self, data):
        # Use get_latest_geomag_data utility for preprocessing
        return get_latest_geomag_data(data)
