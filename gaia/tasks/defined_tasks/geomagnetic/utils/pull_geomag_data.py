import traceback
import httpx
from datetime import datetime
from fiber.logging_utils import get_logger
import pytz
import asyncio

logger = get_logger(__name__)


async def fetch_data(url=None, max_retries=3):
    """
    Fetch raw geomagnetic data from Kyoto WDC.
    """

    # Add delay logic to avoid premature data fetch near the top of the hour (2 mins)
    now_utc = datetime.now(pytz.UTC)
    if now_utc.minute == 0 and now_utc.second < 120:
        wait_seconds = 120 - now_utc.second
        logger.info(f"⏳ Waiting {wait_seconds}s for Kyoto data to refresh...")
        await asyncio.sleep(wait_seconds)
        now_utc = datetime.now(pytz.UTC)  # Refresh after wait

    # Proceed as before with generating URL
    if url is None:
        current_year = now_utc.year
        current_month = now_utc.month
        url = f"https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/{current_year}{current_month:02d}/dst{str(current_year)[-2:]}{current_month:02d}.for.request"

    logger.info(f"Fetching data from URL: {url}")

    # Configure timeout and connection settings
    timeout_config = httpx.Timeout(
        connect=30.0,  # Connection timeout
        read=60.0,     # Read timeout
        write=30.0,    # Write timeout
        pool=60.0      # Pool timeout
    )
    
    limits = httpx.Limits(
        max_keepalive_connections=5,
        max_connections=10,
        keepalive_expiry=30.0
    )

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(
                timeout=timeout_config, 
                limits=limits,
                verify=False,  # Disable SSL verification if needed
                follow_redirects=True
            ) as client:
                response = await client.get(url)
                response.raise_for_status()
                logger.info(f"Successfully fetched data on attempt {attempt + 1}")
                return response.text

        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as e:
            if attempt == max_retries - 1:  # Last attempt
                logger.error(f"Failed to fetch data after {max_retries} attempts: {e}")
                raise RuntimeError(
                    f"Error fetching data after {max_retries} retries: {e}"
                )
            else:
                wait_time = (attempt + 1) * 10  # Longer exponential backoff
                logger.warning(
                    f"Attempt {attempt + 1} failed with {type(e).__name__}: {e}, retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error {e.response.status_code}: {e}")
            if e.response.status_code in [500, 502, 503, 504]:  # Server errors, retry
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 10  
                    logger.warning(f"Server error, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
            raise RuntimeError(f"HTTP error {e.response.status_code}: {e}")

        except Exception as e:
            logger.error(f"Unexpected error fetching data: {e}")
            logger.error(f"{traceback.format_exc()}")
            raise e
