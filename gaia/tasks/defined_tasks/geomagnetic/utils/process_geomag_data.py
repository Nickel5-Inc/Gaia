import traceback
import pandas as pd
from datetime import datetime, timedelta, timezone
from gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data import fetch_data
import asyncio


# Constants
PLACEHOLDER_VALUE = "999999999999999"  # Adjusted for realistic placeholder length

from fiber.logging_utils import get_logger

logger = get_logger(__name__)


def debug_raw_data(data):
    """
    Debug function to examine the raw data format and structure.
    This helps understand what we're actually receiving from Kyoto WDC.
    """
    logger.info("=== RAW DATA ANALYSIS ===")
    lines = data.splitlines()
    logger.info(f"Total lines in response: {len(lines)}")
    
    dst_lines = [line for line in lines if line.startswith("DST")]
    logger.info(f"Lines starting with 'DST': {len(dst_lines)}")
    
    if dst_lines:
        logger.info("=== SAMPLE DST LINES ===")
        for i, line in enumerate(dst_lines[:5]):  # Show first 5 DST lines
            logger.info(f"DST Line {i+1} (length {len(line)}): '{line}'")
            logger.info(f"  Characters 0-10: '{line[:10]}'")
            logger.info(f"  Characters 10-20: '{line[10:20]}'")
            logger.info(f"  Characters 20-30: '{line[20:30]}'")
            if len(line) > 30:
                logger.info(f"  Characters 30-40: '{line[30:40]}'")
            logger.info("")
    
    # Show non-DST lines for context
    non_dst_lines = [line for line in lines if not line.startswith("DST") and line.strip()]
    if non_dst_lines:
        logger.info("=== SAMPLE NON-DST LINES ===")
        for i, line in enumerate(non_dst_lines[:3]):  # Show first 3 non-DST lines
            logger.info(f"Non-DST Line {i+1}: '{line}'")
    
    logger.info("=== END RAW DATA ANALYSIS ===")


def parse_data(data):
    dates = []
    hourly_values = []
    
    # Add diagnostic logging
    debug_raw_data(data)

    def parse_line(line):
        try:
            # Extract year, month, and day
            year = int("20" + line[3:5])  # Prefix with "20" for full year
            month = int(line[5:7])
            day = int(line[8:10].strip())
        except ValueError as e:
            logger.error(f"Skipping line due to invalid date format: {line} - Error: {e}")
            return

        # Extract the data portion (after position 20) and split by spaces
        data_portion = line[20:].strip()
        # Split by whitespace and filter out empty strings
        raw_values = [v for v in data_portion.split() if v]
        
        # Handle "squished together" values where a positive value (one- or two-digit) is immediately
        # followed by one or more "9999" placeholder blocks, e.g. '4999999...' which really means
        # value 4 followed by repeated 9999 missing-data markers.
        values = []
        for value in raw_values:
            if ((value.startswith('-') and '9999' in value and len(value) > 5) or
                (value[:1].isdigit() and '9999' in value and len(value) > 5)):
                # Find where the actual negative value ends and the 9999 markers begin
                # Look for the pattern where we have a reasonable negative number followed by 9999s
                found_split = False
                
                # For positive numbers, start split_pos at 1; for negative, at 2 (to skip the '-')
                start_pos = 2 if value.startswith('-') else 1
                for split_pos in range(start_pos, min(6, len(value))):  # Check positions for X, XX  or -X, -XX, -XXX
                    potential_value = value[:split_pos]
                    remainder = value[split_pos:]
                    
                    # Check if the potential value is a reasonable negative number
                    # and the remainder consists only of repeated "9999" patterns
                    try:
                        int(potential_value)  # Validate it's a number
                        # Check if remainder is composed of "9999" repeated
                        if len(remainder) > 0 and len(remainder) % 4 == 0 and all(remainder[i:i+4] == "9999" for i in range(0, len(remainder), 4)):
                            # Split the value
                            values.append(potential_value)
                            # Add each 9999 as a separate placeholder
                            num_9999s = len(remainder) // 4
                            values.extend(["9999"] * num_9999s)
                            logger.debug(f"Split squished value '{value}' into ['{potential_value}'] + {num_9999s} × '9999'")
                            found_split = True
                            break
                    except ValueError:
                        continue
                
                if not found_split:
                    # Fallback: if we can't parse it properly, just add as-is
                    values.append(value)
                    logger.warning(f"Could not parse squished value '{value}', adding as-is")
            else:
                values.append(value)
        
        # Process up to 24 hourly values
        # NOTE: Data format uses 1-24 hour indexing, not 0-23
        # values[0] = hour 1 (01:00), values[1] = hour 2 (02:00), ..., values[23] = hour 24 (00:00 next day)
        # IMPORTANT: The last value in the data is the daily mean (columns 117-120), NOT hourly data
        # So we only process the first 24 values, excluding the daily mean
        hourly_values_only = values[:-1] if len(values) > 24 else values  # Exclude daily mean (last value)
        
        for i in range(min(24, len(hourly_values_only))):
            value_str = hourly_values_only[i]
            
            # Convert 1-24 indexing to 0-23 hour format
            # Hour 1-23 maps to 1-23, Hour 24 maps to 0 (next day)
            if i < 23:
                hour = i + 1  # hours 1-23
                target_day = day
            else:
                hour = 0  # hour 24 becomes hour 0 of next day
                # Calculate next day (handle month/year rollover)
                try:
                    next_day_date = datetime(year, month, day, tzinfo=timezone.utc) + timedelta(days=1)
                    target_day = next_day_date.day
                    target_month = next_day_date.month  
                    target_year = next_day_date.year
                except:
                    # Skip if date calculation fails
                    logger.debug(f"Skipping hour 24 due to date calculation error")
                    continue
            
            # Skip placeholder values (9999, 999, etc.)
            if value_str in ['9999', '999', '99999', PLACEHOLDER_VALUE]:
                continue
                
            # Skip empty values
            if not value_str:
                continue
                
            try:
                value = int(value_str)
                
                # Create timestamp with correct date
                if i < 23:
                    timestamp = datetime(year, month, day, hour, tzinfo=timezone.utc)
                else:
                    timestamp = datetime(target_year, target_month, target_day, hour, tzinfo=timezone.utc)

                # Only include valid timestamps and exclude future timestamps
                if timestamp < datetime.now(timezone.utc):
                    dates.append(timestamp)
                    hourly_values.append(value)
                # Future timestamps and invalid values are silently skipped as expected behavior
            except ValueError:
                pass  # Invalid values are expected and silently skipped

    # Parse all lines that start with "DST"
    for line in data.splitlines():
        if line.startswith("DST"):
            parse_line(line)

    logger.info(f"Parsed {len(dates)} data points from {len([l for l in data.splitlines() if l.startswith('DST')])} DST lines")
    
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


async def get_geomag_data_for_hour(target_hour, include_historical=False, max_wait_minutes=30):
    """
    Fetch geomagnetic data for a specific hour, waiting if necessary.
    
    Args:
        target_hour (datetime): The specific hour to fetch data for
        include_historical (bool): Whether to include current month's historical data
        max_wait_minutes (int): Maximum minutes to wait for data to become available
        
    Returns:
        tuple: (timestamp, Dst value, historical_data) for the target hour ONLY.
               Returns "N/A" values if target hour data is not available within time limit.
    """
    target_hour_aligned = target_hour.replace(minute=0, second=0, microsecond=0)
    start_time = datetime.now(timezone.utc)
    max_wait_time = start_time + timedelta(minutes=max_wait_minutes)
    
    logger.info(f"Fetching geomagnetic data for target hour: {target_hour_aligned} (will wait up to {max_wait_minutes} minutes)")
    
    while datetime.now(timezone.utc) < max_wait_time:
        try:
            # Fetch raw data
            raw_data = await fetch_data()
            loop = asyncio.get_event_loop()

            # Parse and clean raw data into DataFrame
            parsed_df = await loop.run_in_executor(None, _parse_data_sync, raw_data)
            cleaned_df = await loop.run_in_executor(None, _clean_data_sync, parsed_df)

            if not cleaned_df.empty:
                # Check if we have data for the target hour
                target_data = cleaned_df[cleaned_df["timestamp"] == target_hour_aligned]
                
                if not target_data.empty:
                    # Found target hour data!
                    timestamp = target_data.iloc[0]["timestamp"]
                    dst_value = float(target_data.iloc[0]["Dst"])
                    logger.info(f"✅ Found data for target hour {target_hour_aligned}: {dst_value}")
                    
                    if include_historical:
                        now = datetime.now(timezone.utc)
                        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                        historical_data = cleaned_df[cleaned_df["timestamp"] >= start_of_month]
                        return timestamp, dst_value, historical_data
                    else:
                        return timestamp, dst_value
                else:
                    # Target hour data not available yet
                    latest_timestamp = cleaned_df["timestamp"].max()
                    wait_time_left = (max_wait_time - datetime.now(timezone.utc)).total_seconds()
                    
                    if wait_time_left > 30:  # Only wait if we have more than 30 seconds left
                        logger.info(f"⏳ Target hour {target_hour_aligned} not available yet (latest: {latest_timestamp}), waiting...")
                        await asyncio.sleep(30)  # Wait 30 seconds before retry
                        continue
                    else:
                        # Time's up - do NOT use latest data, return N/A
                        logger.warning(f"❌ Target hour {target_hour_aligned} not available within {max_wait_minutes} minutes. Skipping cycle.")
                        break
            else:
                # No data at all, wait and retry
                wait_time_left = (max_wait_time - datetime.now(timezone.utc)).total_seconds()
                if wait_time_left > 30:
                    logger.warning("No geomagnetic data available, waiting...")
                    await asyncio.sleep(30)
                    continue
                else:
                    break
                    
        except Exception as e:
            logger.error(f"Error fetching geomagnetic data: {e}")
            wait_time_left = (max_wait_time - datetime.now(timezone.utc)).total_seconds()
            if wait_time_left > 30:
                await asyncio.sleep(30)
                continue
            else:
                break
    
    # Failed to get target hour data within time limit - return N/A
    logger.error(f"❌ Failed to fetch data for target hour {target_hour_aligned} within {max_wait_minutes} minutes. Cycle will be skipped.")
    if include_historical:
        return "N/A", "N/A", None
    else:
        return "N/A", "N/A"


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
