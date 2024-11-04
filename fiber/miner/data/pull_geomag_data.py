import requests
from datetime import datetime

def fetch_data(url=None):
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
        current_year = datetime.now().year
        current_month = datetime.now().month
        # Format the URL dynamically
        url = f'https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/{current_year}{current_month:02d}/dst{str(current_year)[-2:]}{current_month:02d}.for.request'

    print(f"Fetching data from URL: {url}")  # Debug print to verify the URL

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.text
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error fetching data: {e}")

