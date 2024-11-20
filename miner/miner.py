import os
import traceback

from dotenv import load_dotenv

load_dotenv("dev.env")  

import argparse
import uvicorn

from fiber.logging_utils import get_logger
from fiber.miner import server
from miner.utils.subnet import factory_router 
from fiber.miner.middleware import configure_extra_logging_middleware

'''
Miner main execution code - this is the entry point for the miner neuron.

'''




class Miner:
    def __init__(self, args):
        self.args = args
        self.logger = get_logger(__name__)
        # Set up argparse to handle the flag
        self.parser = argparse.ArgumentParser(description="Start the miner with optional flags.")
        self.parser.add_argument('--use_base_model', action='store_true', help='Enable base model usage')

        # Parse the arguments
        self.args = self.parser.parse_args()

        
    def run(self):
        try:
            app = server.factory_app(debug=True)
            app.include_router(factory_router())
            # Change host to "0.0.0.0" to allow external connections
            uvicorn.run(app, host="0.0.0.0", port=33334)

        except Exception as e:
            self.logger.error(f"Error starting miner: {e}")
            self.logger.error(traceback.format_exc())
            raise e
        while True:
            # Main miner loop
            # listen to routes for new tasks, then process them
            pass
        






if __name__ == "__main__":
    miner = Miner() 
    miner.run()



