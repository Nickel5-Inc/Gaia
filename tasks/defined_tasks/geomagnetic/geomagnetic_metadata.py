from tasks.base.components.metadata import Metadata

class GeomagneticMetadata(Metadata):
    def __init__(self):
        self.source = "Geomagnetic Data"
        self.date_range = None  # Placeholder for configurable date range
        # Additional metadata attributes as needed
