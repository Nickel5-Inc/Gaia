import os

from dotenv import load_dotenv

load_dotenv("dev.env")  

import argparse

from fiber.logging_utils import get_logger
from fiber.miner import server
from fiber.miner.endpoints.subnet import factory_router as get_subnet_router
from fiber.miner.middleware import configure_extra_logging_middleware



'''
Miner main execution code - this is the entry point for the miner neuron.


'''


logger = get_logger(__name__)
app = server.factory_app(debug=True)
app.include_router(get_subnet_router())

# Set up argparse to handle the flag
parser = argparse.ArgumentParser(description="Start the miner with optional flags.")
parser.add_argument('--use_base_model', action='store_true', help='Enable base model usage')

# Parse the arguments
args = parser.parse_args()

# Automatically fetch the cleaned DataFrame
from tasks.defined_tasks.geomagnetic.utils.process_geomag_data import get_latest_geomag_data

def fetch_miner_cleaned_df():
    # Fetch the cleaned DataFrame
    cleaned_df = get_latest_geomag_data()

fetch_miner_cleaned_df()

# Check if the flag is set and execute the function
if args.use_base_model:
    # this import can go on the top BUT since it is not req, I left it here for now
    from models.geomag_basemodel import GeoMagBaseModel

    def initialize_base_model():
        geomag_model = GeoMagBaseModel()

    initialize_base_model()


if os.getenv("ENV", "dev").lower() == "dev":
    configure_extra_logging_middleware(app)

if __name__ == "__main__":
    import uvicorn

    # Change host to "0.0.0.0" to allow external connections
    uvicorn.run(app, host="0.0.0.0", port=33334)
