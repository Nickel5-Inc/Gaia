from pydantic import BaseModel, Field
from gaia.tasks.base.components.metadata import Metadata
from typing import Optional, Tuple, Dict, Any


class GeomagneticMetadata(Metadata):
    date_range: Optional[Tuple[str, str]] = Field(
        default=None,
        description="A tuple representing the start and end dates for the geomagnetic data."
    )
    refresh_interval: str = Field(
        default="1 hour",
        description="The interval at which geomagnetic data is refreshed."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def set_date_range(self, start_date, end_date):
        """
        Sets the date range for the metadata.

        Args:
            start_date (str or datetime): Start of the date range.
            end_date (str or datetime): End of the date range.
        """
        self.date_range = (start_date, end_date)

    def validate_metadata(
        self, core_metadata: Dict[str, Any], extended_metadata: Dict[str, Any]
    ):
        """
        Concrete implementation of the abstract validate_metadata method.

        Args:
            core_metadata (dict): Core metadata to validate.
            extended_metadata (dict): Extended metadata to validate.

        Raises:
            ValueError: If any metadata validation fails.
        """
        if not core_metadata.get("name"):
            raise ValueError("Core metadata must include a 'name' field.")
        if not core_metadata.get("description"):
            raise ValueError("Core metadata must include a 'description' field.")

        # Add additional validations as required
        print("Metadata validated successfully.")
