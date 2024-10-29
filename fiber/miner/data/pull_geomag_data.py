import requests

# URL and any constants needed
DEFAULT_URL = 'https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/presentmonth/dst2410.for.request'

def fetch_data(url=DEFAULT_URL):
    """
    Fetch raw geomagnetic data from the specified URL.

    Args:
        url (str): The URL to fetch data from.

    Returns:
        str: The raw data as a text string.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.text
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error fetching data: {e}")

