import traceback
import httpx
from datetime import datetime
from fiber.logging_utils import get_logger
import pytz
import asyncio

logger = get_logger(__name__)


async def fetch_data(url=None, max_retries=3):
    """
    Fetch raw geomagnetic data from the specified or dynamically generated URL.

    Args:
        url (str, optional): The URL to fetch data from. If not provided, a URL will be generated
                             based on the current year and month.
        max_retries (int): Maximum number of retry attempts

    Returns:
        str: The raw data as a text string.
    """
    # Generate the default URL based on the current year and month if not provided
    if url is None:
        current_time = datetime.now(pytz.UTC)
        current_year = current_time.year
        current_month = current_time.month
        # Format the URL dynamically
        url = f"https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/{current_year}{current_month:02d}/dst{str(current_year)[-2:]}{current_month:02d}.for.request"

    logger.info(f"Fetching data from URL: {url}")

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.text

        except (httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            if attempt == max_retries - 1:  # Last attempt
                logger.error(f"Failed to fetch data after {max_retries} attempts: {e}")
                raise RuntimeError(
                    f"Error fetching data after {max_retries} retries: {e}"
                )
            else:
                wait_time = (attempt + 1) * 5  # Exponential backoff
                logger.warning(
                    f"Attempt {attempt + 1} failed, retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)

        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            logger.error(f"{traceback.format_exc()}")
            raise e
