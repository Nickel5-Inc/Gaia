from gaia.tasks.base.components.metadata import Metadata, CoreMetadata


class GeomagneticMetadata(Metadata):
    def __init__(self, **kwargs):
        # Provide default values for core_metadata
        default_core_metadata = CoreMetadata(
            name="Geomagnetic Task",
            description="Processes geomagnetic data for predictions and validation.",
            dependencies_file="dependencies.yml",  # Replace with actual path
            hardware_requirements_file="hardware.yml",  # Replace with actual path
            author="Your Name",  # Replace with the actual author's name
            version="1.0",
        )
        kwargs["core_metadata"] = kwargs.get("core_metadata", default_core_metadata)
        super().__init__(**kwargs)

    def validate_metadata(self, core_metadata, extended_metadata):
        """
        Implements the abstract validate_metadata method from Metadata.
        This can include checks to ensure metadata is correct and complete.

        Args:
            core_metadata (dict): Core metadata for validation.
            extended_metadata (dict): Extended metadata for validation.

        Returns:
            bool: True if validation passes, raises an exception otherwise.
        """
        # Example validation: Ensure core_metadata contains expected keys
        required_keys = [
            "name",
            "description",
            "dependencies_file",
            "hardware_requirements_file",
            "author",
            "version",
        ]
        missing_keys = [key for key in required_keys if key not in core_metadata]
        if missing_keys:
            raise ValueError(f"Missing required core metadata keys: {missing_keys}")

        # Perform additional validation as needed for extended_metadata
        if extended_metadata and not isinstance(extended_metadata, dict):
            raise ValueError("Extended metadata must be a dictionary.")

        return True
