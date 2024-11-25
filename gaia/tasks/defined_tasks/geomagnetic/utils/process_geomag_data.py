import pandas as pd
from datetime import datetime, timedelta
from gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data import fetch_data


# Constants
PLACEHOLDER_VALUE = "999999999999999"  # Adjusted for realistic placeholder length


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
                    timestamp = datetime(year, month, day, hour)

                    # Only include valid timestamps and exclude future timestamps
                    if timestamp < datetime.utcnow():
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
    now = datetime.utcnow()

    # Drop duplicate timestamps
    df = df.drop_duplicates(subset="timestamp")

    # Filter valid Dst range
    df = df[df["Dst"].between(-500, 500)]

    # Exclude future timestamps (ensure strictly less than current time)
    df = df[df["timestamp"] < now]

    # Reset index
    return df.reset_index(drop=True)


async def get_latest_geomag_data():
    """
    Fetch, parse, clean, and return the latest valid geomagnetic data point.

    Returns:
        tuple: (timestamp, Dst value) of the latest geomagnetic data point.
    """
    raw_data = await fetch_data()  # Fetch raw data
    parsed_df = parse_data(raw_data)  # Parse raw data into DataFrame
    cleaned_df = clean_data(parsed_df)  # Clean data

    # Get the latest data point
    if not cleaned_df.empty:
        latest_data_point = cleaned_df.iloc[-1]
        timestamp = latest_data_point["timestamp"]
        dst_value = int(
            latest_data_point["Dst"]
        )  # Convert to native int for JSON compatibility
        return timestamp, dst_value
    else:
        # If no data available, return placeholders
        return "N/A", "N/A"
