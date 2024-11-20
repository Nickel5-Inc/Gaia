from gaia.tasks.base.components.metadata import Metadata

class GeomagneticMetadata(Metadata):
    def __init__(self):
        self.source = "Geomagnetic Data"
        self.date_range = None
        self.refresh_interval = "1 hour"  # Example attribute

    def set_date_range(self, start_date, end_date):
        """
        Sets the date range for the metadata.

        Args:
            start_date (str or datetime): Start of the date range.
            end_date (str or datetime): End of the date range.
        """
        self.date_range = (start_date, end_date)

