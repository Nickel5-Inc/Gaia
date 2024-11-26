import traceback
import httpx
from datetime import datetime
from fiber.logging_utils import get_logger
import pytz

logger = get_logger(__name__)

async def fetch_data(url=None):
    """
    Fetch raw geomagnetic data from the specified or dynamically generated URL.

    Args:
        url (str, optional): The URL to fetch data from. If not provided, a URL will be generated
                             based on the current year and month.

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

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()  # Raise an error for bad status codes
            return response.text
    except httpx.RequestError as e:
        raise RuntimeError(f"Error fetching data: {e}")
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        logger.error(f'{traceback.format_exc()}')
        raise e
