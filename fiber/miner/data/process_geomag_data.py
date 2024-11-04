import pandas as pd
from datetime import datetime, timedelta
from fiber.miner.data.pull_geomag_data import fetch_data

# Constants
PLACEHOLDER_VALUE = '9999999999999999'

def parse_data(data):
    dates = []
    hourly_values = []

    def parse_line(line):
        # Extract the year and month from the line, assuming format "DSTYYMM*"
        try:
            year = int("20" + line[3:5])  # Prefix with "20" to handle the century
            month = int(line[5:7])
        except ValueError:
            # If parsing fails, skip this line
            print(f"Skipping line due to invalid date format: {line}")
            return

        day = int(line[8:10].strip())  # Extract day

        for hour in range(24):
            start_idx = 20 + (hour * 4)
            end_idx = start_idx + 4
            value_str = line[start_idx:end_idx].strip()

            # Skip placeholder and empty values
            if value_str != PLACEHOLDER_VALUE and value_str:
                try:
                    value = int(value_str)
                    timestamp = (datetime(year, month, day, 0) + timedelta(days=1)
                                 if hour == 23 else datetime(year, month, day, hour + 1))
                    dates.append(timestamp)
                    hourly_values.append(value)
                except ValueError:
                    continue

    # Adjusted condition to capture lines starting with "DST" for flexibility
    for line in data.splitlines():
        if line.startswith("DST"):
            parse_line(line)

    # Debugging output to check if any dates and values are parsed
    print(f"Parsed dates: {dates[:5]}")  # Display first 5 dates for inspection
    print(f"Parsed hourly_values: {hourly_values[:5]}")  # Display first 5 values for inspection

    return pd.DataFrame({'timestamp': dates, 'Dst': hourly_values})

def clean_data(df):
    df = df.drop_duplicates(subset='timestamp')
    df = df[df['Dst'].between(-500, 500)]
    df = df.dropna().reset_index(drop=True)
    return df

def get_latest_geomag_data():
    """
    Fetch, parse, clean, and return the latest geomagnetic data point.

    Returns:
        tuple: (timestamp, Dst value) of the latest geomagnetic data point.
    """
    raw_data = fetch_data()  # Fetch raw data
    parsed_df = parse_data(raw_data)  # Parse raw data into DataFrame
    cleaned_df = clean_data(parsed_df)  # Clean data

    # Get the latest data point
    if not cleaned_df.empty:
        latest_data_point = cleaned_df.iloc[-1]
        timestamp = latest_data_point['timestamp']
        dst_value = int(latest_data_point['Dst'])  # Convert to native int for JSON compatibility
        return timestamp, dst_value
    else:
        # If no data available, return placeholders
        return "N/A", "N/A"
