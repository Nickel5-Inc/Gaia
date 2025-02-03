import traceback
import pandas as pd
from datetime import datetime, timedelta, timezone
from gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data import fetch_data
import asyncio
from typing import Tuple, Optional, List, Dict, Any
from prefect import task
from fiber.logging_utils import get_logger


# Constants
PLACEHOLDER_VALUE = "999999999999999"  # Adjusted for realistic placeholder length

logger = get_logger(__name__)


def parse_data(data):
    dates = []
    hourly_values = []

    def parse_line(line):
        try:
            # Extract year, month, and day
            year = int("20" + line[3:5])  # Prefix with "20" for full year
            month = int(line[5:7])
            day = int(line[8:10].strip())
        except ValueError:
            print(f"Skipping line due to invalid date format: {line}")
            return

        # Iterate over 24 hourly values
        for hour in range(24):
            start_idx = 20 + (hour * 4)
            end_idx = start_idx + 4
            value_str = line[start_idx:end_idx].strip()

            # Skip placeholder and invalid values
            if value_str != PLACEHOLDER_VALUE and value_str:
                try:
                    value = int(value_str)
                    timestamp = datetime(year, month, day, hour, tzinfo=timezone.utc)

                    # Only include valid timestamps and exclude future timestamps
                    if timestamp < datetime.now(timezone.utc):
                        dates.append(timestamp)
                        hourly_values.append(value)
                except ValueError:
                    print(f"Skipping invalid value: {value_str}")

    # Parse all lines that start with "DST"
    for line in data.splitlines():
        if line.startswith("DST"):
            parse_line(line)

    # Create a DataFrame with parsed data
    return pd.DataFrame({"timestamp": dates, "Dst": hourly_values})


def clean_data(df):
    now = datetime.now(timezone.utc)

    # Drop duplicate timestamps
    df = df.drop_duplicates(subset="timestamp")

    # Filter valid Dst range
    df = df[df["Dst"].between(-500, 500)]

    # Exclude future timestamps (ensure strictly less than current time)
    df = df[df["timestamp"] < now]

    # Reset index
    return df.reset_index(drop=True)


@task(
    name="get_latest_geomag_data",
    retries=3,
    retry_delay_seconds=60,
    description="Fetch latest geomagnetic data from the source"
)
async def get_latest_geomag_data(include_historical: bool = False) -> Tuple[datetime, float, Optional[pd.DataFrame]]:
    """
    Fetch the latest geomagnetic data from the source.

    Args:
        include_historical (bool): Whether to include historical data

    Returns:
        Tuple[datetime, float, Optional[pd.DataFrame]]: 
            - Timestamp of the measurement
            - DST value
            - Historical data if requested, None otherwise
            
    Raises:
        Exception: If there's an error fetching or processing the data
    """
    try:
        # TODO: Implement actual data fetching from source
        # This is a placeholder that returns mock data
        current_time = datetime.now(timezone.utc)
        dst_value = -25.0  # Mock DST value
        
        if include_historical:
            # Create mock historical data
            dates = pd.date_range(
                start=current_time - timedelta(days=30),
                end=current_time,
                freq='H'
            )
            historical_data = pd.DataFrame({
                'timestamp': dates,
                'Dst': [-20.0 + i * 0.1 for i in range(len(dates))]
            })
            return current_time, dst_value, historical_data
        
        return current_time, dst_value, None

    except Exception as e:
        logger.error(f"Error fetching geomagnetic data: {e}")
        return "N/A", "N/A", None


@task(
    name="process_geomag_response",
    retries=2,
    retry_delay_seconds=30,
    description="Process raw geomagnetic response data"
)
async def process_geomag_response(response_data: Dict[str, Any]) -> Tuple[datetime, float, Optional[pd.DataFrame]]:
    """
    Process raw geomagnetic response data into structured format.

    Args:
        response_data (Dict[str, Any]): Raw response data from the source

    Returns:
        Tuple[datetime, float, Optional[pd.DataFrame]]:
            - Timestamp of the measurement
            - Processed DST value
            - Processed historical data if available
            
    Raises:
        ValueError: If response data is invalid
        Exception: If there's an error processing the data
    """
    try:
        if not isinstance(response_data, dict):
            raise ValueError("Invalid response data format")

        # Extract timestamp and value
        timestamp = datetime.fromisoformat(response_data["timestamp"])
        dst_value = float(response_data["value"])

        # Process historical data if present
        historical_data = None
        if "historical_values" in response_data and response_data["historical_values"]:
            historical_records = []
            for record in response_data["historical_values"]:
                historical_records.append({
                    "timestamp": datetime.fromisoformat(record["timestamp"]),
                    "Dst": float(record["Dst"])
                })
            historical_data = pd.DataFrame(historical_records)

        return timestamp, dst_value, historical_data

    except Exception as e:
        logger.error(f"Error processing geomagnetic response: {e}")
        raise


@task(
    name="validate_geomag_data",
    retries=2,
    retry_delay_seconds=15,
    description="Validate geomagnetic data values and format"
)
def validate_geomag_data(timestamp: datetime, dst_value: float, historical_data: Optional[pd.DataFrame] = None) -> bool:
    """
    Validate geomagnetic data values and format.

    Args:
        timestamp (datetime): Timestamp to validate
        dst_value (float): DST value to validate
        historical_data (Optional[pd.DataFrame]): Historical data to validate

    Returns:
        bool: True if data is valid, False otherwise
        
    Raises:
        ValueError: If input types are invalid
    """
    try:
        # Validate timestamp
        if not isinstance(timestamp, datetime):
            logger.error(f"Invalid timestamp type: {type(timestamp)}")
            return False

        # Validate DST value
        if not isinstance(dst_value, (int, float)):
            logger.error(f"Invalid DST value type: {type(dst_value)}")
            return False

        # Check DST value range (-500 to 100 nT is typical range)
        if not -500 <= dst_value <= 100:
            logger.warning(f"DST value {dst_value} outside typical range (-500 to 100 nT)")
            return False

        # Validate historical data if provided
        if historical_data is not None:
            if not isinstance(historical_data, pd.DataFrame):
                logger.error(f"Invalid historical data type: {type(historical_data)}")
                return False

            required_columns = {"timestamp", "Dst"}
            if not all(col in historical_data.columns for col in required_columns):
                logger.error(f"Missing required columns in historical data")
                return False

        return True

    except Exception as e:
        logger.error(f"Error validating geomagnetic data: {e}")
        return False
