# test_pull_geomag_data.py (minimal httpx test with logging)
import unittest
from unittest.mock import patch, AsyncMock, MagicMock
import httpx
import asyncio
import logging

from gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data import fetch_data

# --- Add this logging configuration ---
logging.basicConfig(
    level=logging.DEBUG, 
    format="%(levelname)s: %(name)s: %(message)s"
)
# To make httpcore and httpx more verbose if needed:
# logging.getLogger("httpcore").setLevel(logging.DEBUG)
# logging.getLogger("httpx").setLevel(logging.DEBUG)
# You might see a lot of output with the above two lines uncommented.
# The root logger at DEBUG should catch most httpx/httpcore messages by default.
# --- End of logging configuration ---

# Use a consistent "real" URL structure for testing purposes
REAL_URL_FOR_TESTING = "https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/202505/dst2505.for.request"

class TestFetchGeomagData(unittest.IsolatedAsyncioTestCase):

    @patch('gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data.asyncio.sleep', new_callable=AsyncMock)
    @patch('gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data.httpx.AsyncClient')
    async def test_fetch_data_success(self, mock_async_client_constructor, mock_sleep):
        """Test successful data fetching using the real URL structure."""
        mock_response_text = "Sample geomagnetic data"
        
        mock_response = MagicMock()
        mock_response.text = mock_response_text
        mock_response.raise_for_status = MagicMock()

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        
        mock_async_client_constructor.return_value.__aenter__.return_value = mock_client_instance

        result = await fetch_data(url=REAL_URL_FOR_TESTING, max_retries=1)

        self.assertEqual(result, mock_response_text)
        mock_client_instance.get.assert_called_once_with(REAL_URL_FOR_TESTING)
        # In this success case (and max_retries=1), no sleeps related to retries are expected.
        # The initial conditional sleep for Kyoto data refresh might be called if datetime isn't mocked for this specific test.
        # For simplicity, if we're only testing direct fetch success here, we assume conditions for initial sleep aren't met or datetime is mocked to prevent it.

    @patch('gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data.asyncio.sleep', new_callable=AsyncMock)
    @patch('gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data.httpx.AsyncClient')
    async def test_fetch_data_failure_after_retries(self, mock_async_client_constructor, mock_sleep):
        """Test failure after all retries using the real URL structure."""
        mock_client_instance = AsyncMock()
        # Simulate ConnectTimeout for all attempts
        mock_client_instance.get.side_effect = httpx.ConnectTimeout("Connection timed out")
        
        mock_async_client_constructor.return_value.__aenter__.return_value = mock_client_instance

        max_retries = 2 # Example: test with 2 retries

        with self.assertRaisesRegex(RuntimeError, f"Error fetching data after {max_retries} retries: Connection timed out"):
            await fetch_data(url=REAL_URL_FOR_TESTING, max_retries=max_retries)

        self.assertEqual(mock_client_instance.get.call_count, max_retries)
        for call_args in mock_client_instance.get.call_args_list:
            self.assertEqual(call_args[0][0], REAL_URL_FOR_TESTING)
        # We expect sleep to be called (max_retries - 1) times for backoff
        self.assertEqual(mock_sleep.call_count, max_retries - 1)

    @patch('gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data.asyncio.sleep', new_callable=AsyncMock)
    @patch('gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data.httpx.AsyncClient')
    async def test_fetch_data_retry_then_succeed(self, mock_async_client_constructor, mock_sleep):
        """Test successful fetch after an initial retry using the real URL structure."""
        mock_response_text = "Successful data after retry"

        mock_successful_response = MagicMock()
        mock_successful_response.text = mock_response_text
        mock_successful_response.raise_for_status = MagicMock()

        mock_client_instance = AsyncMock()
        mock_client_instance.get.side_effect = [
            httpx.ReadTimeout("Read timed out"), # First attempt fails
            mock_successful_response             # Second attempt succeeds
        ]
        
        mock_async_client_constructor.return_value.__aenter__.return_value = mock_client_instance
        
        max_retries = 2 
        result = await fetch_data(url=REAL_URL_FOR_TESTING, max_retries=max_retries)

        self.assertEqual(result, mock_response_text)
        self.assertEqual(mock_client_instance.get.call_count, 2) # Called twice
        for call_args in mock_client_instance.get.call_args_list:
            self.assertEqual(call_args[0][0], REAL_URL_FOR_TESTING)
        # Sleep called once before the successful retry
        mock_sleep.assert_called_once()

    @patch('gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data.datetime')
    @patch('gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data.asyncio.sleep', new_callable=AsyncMock)
    @patch('gaia.tasks.defined_tasks.geomagnetic.utils.pull_geomag_data.httpx.AsyncClient')
    async def test_fetch_data_initial_wait(self, mock_async_client_constructor, mock_sleep, mock_datetime):
        """Test the initial wait logic and dynamic URL generation."""
        # Mock datetime.now() to be at the start of an hour, triggering initial sleep
        mock_initial_now_utc = MagicMock()
        mock_initial_now_utc.minute = 0
        mock_initial_now_utc.second = 30 # e.g., 30 seconds into the minute
        
        # Mock for datetime.now() after the initial sleep, used for URL generation
        mock_refreshed_now_utc = MagicMock()
        mock_refreshed_now_utc.year = 2025 # Consistent with REAL_URL_FOR_TESTING year/month
        mock_refreshed_now_utc.month = 5
        
        # If datetime.now() is called more times, provide a consistent mock
        mock_datetime.now.side_effect = [mock_initial_now_utc, mock_refreshed_now_utc, mock_refreshed_now_utc]

        mock_response_text = "Data after initial wait"
        mock_response = MagicMock()
        mock_response.text = mock_response_text
        mock_response.raise_for_status = MagicMock()

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_async_client_constructor.return_value.__aenter__.return_value = mock_client_instance

        expected_initial_wait_seconds = 120 - 30
        expected_url_generated = f"https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/{mock_refreshed_now_utc.year}{mock_refreshed_now_utc.month:02d}/dst{str(mock_refreshed_now_utc.year)[-2:]}{mock_refreshed_now_utc.month:02d}.for.request"

        # Call fetch_data with url=None to trigger dynamic URL generation and initial wait
        result = await fetch_data(max_retries=1) 

        self.assertEqual(result, mock_response_text)
        # Check that the initial sleep was called with the correct duration
        # And that httpx client was called with the dynamically generated URL
        mock_sleep.assert_any_call(expected_initial_wait_seconds)
        mock_client_instance.get.assert_called_once_with(expected_url_generated)

if __name__ == '__main__':
    unittest.main() 