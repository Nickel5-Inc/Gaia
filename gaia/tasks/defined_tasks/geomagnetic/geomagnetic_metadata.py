from gaia.tasks.base.components.metadata import Metadata


class GeomagneticMetadata(Metadata):
    def __init__(self):
        super().__init__()
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

    def validate_metadata(self):
        """
        Validates the metadata to ensure it meets the requirements.

        Raises:
            ValueError: If any required metadata attributes are invalid or missing.
        """
        if not self.source:
            raise ValueError("Metadata must include a source.")
        if self.date_range:
            start_date, end_date = self.date_range
            if not start_date or not end_date:
                raise ValueError("Both start_date and end_date must be provided.")
            if start_date > end_date:
                raise ValueError("start_date cannot be later than end_date.")
        print("GeomagneticMetadata is valid.")

