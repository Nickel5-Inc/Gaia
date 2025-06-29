import traceback
import pandas as pd
from datetime import datetime, timedelta, timezone
from gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data import fetch_data
import asyncio


# Constants
PLACEHOLDER_VALUE = "999999999999999"  # Adjusted for realistic placeholder length

from fiber.logging_utils import get_logger

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


def _parse_data_sync(data):
    return parse_data(data)


def clean_data(df):
    now = datetime.now(timezone.utc)

    # Drop duplicate timestamps
    df = df.drop_duplicates(subset="timestamp")

    # Filter valid Dst range
    df = df[df["Dst"].between(-500, 500)]

    # Exclude future timestamps (ensure strictly less than current time)
    df = df[df["timestamp"] < now]

    # Normalize Dst values to the range (-5, 5)
    df["Dst"] = df["Dst"] / 100

    # Reset index
    return df.reset_index(drop=True)


def _clean_data_sync(df):
    return clean_data(df)


async def get_latest_geomag_data(include_historical=False):
    """
    Fetch, parse, clean, and return the latest valid geomagnetic data point.

    Args:
        include_historical (bool): Whether to include current month's historical data.

    Returns:
        tuple: (timestamp, Dst value, historical_data) of the latest geomagnetic data point.
               `historical_data` will be a DataFrame if `include_historical=True`, otherwise None.
    """
    try:
        # Fetch raw data
        raw_data = await fetch_data()
        loop = asyncio.get_event_loop()

        # Parse and clean raw data into DataFrame
        parsed_df = await loop.run_in_executor(None, _parse_data_sync, raw_data)
        cleaned_df = await loop.run_in_executor(None, _clean_data_sync, parsed_df)

        # Extract the latest data point
        if not cleaned_df.empty:
            latest_data_point = cleaned_df.iloc[-1]
            timestamp = latest_data_point["timestamp"]
            dst_value = float(latest_data_point["Dst"])
        else:
            # Return consistent format based on include_historical flag
            if include_historical:
                return "N/A", "N/A", None
            else:
                return "N/A", "N/A"

        # If historical data is requested, filter the DataFrame for the current month
        if include_historical:
            now = datetime.now(timezone.utc)
            start_of_month = now.replace(
                day=1, hour=0, minute=0, second=0, microsecond=0
            )
            historical_data = cleaned_df[cleaned_df["timestamp"] >= start_of_month]
            return timestamp, dst_value, historical_data
        else:
            return timestamp, dst_value
    except Exception as e:
        logger.error(f"Error fetching geomagnetic data: {e}")
        logger.error(f"{traceback.format_exc()}")
        # Return consistent format based on include_historical flag
        if include_historical:
            return "N/A", "N/A", None
        else:
            return "N/A", "N/A"
