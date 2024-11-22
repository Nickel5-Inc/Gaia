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
            version="1.0"
        )
        # Use default_core_metadata if none is provided
        kwargs['core_metadata'] = kwargs.get('core_metadata', default_core_metadata)
        super().__init__(**kwargs)

    # Additional methods for GeomagneticMetadata can be added here
