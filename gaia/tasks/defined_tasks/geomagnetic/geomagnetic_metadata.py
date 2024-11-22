from gaia.tasks.base.components.metadata import Metadata, CoreMetadata


class GeomagneticMetadata(Metadata):
    def __init__(self):
        # Initialize the core metadata fields
        core_metadata = CoreMetadata(
            name="Geomagnetic Task",
            description="Processes geomagnetic data for DST predictions.",
            dependencies_file="dependencies.yml",  # Path to a valid dependencies file
            hardware_requirements_file="hardware_requirements.yml",  # Path to a valid hardware file
            author="Your Name or Organization",
            version="1.0.0",
        )
        super().__init__(core_metadata=core_metadata)

        # Extended metadata (if needed)
        self.extended_metadata = {
            "refresh_interval": "1 hour",  # Example extended field
        }

        # Additional Geomagnetic-specific metadata
        self.date_range = None

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
        # Validate core metadata using the parent class's validation mechanism
        if not self.core_metadata:
            raise ValueError("Core metadata is missing.")
        if not self.core_metadata.name or not self.core_metadata.description:
            raise ValueError("Core metadata must include a name and description.")

        # Additional validation for extended metadata
        if self.date_range:
            start_date, end_date = self.date_range
            if not start_date or not end_date:
                raise ValueError("Both start_date and end_date must be provided.")
            if start_date > end_date:
                raise ValueError("start_date cannot be later than end_date.")

        print("GeomagneticMetadata is valid.")
