import asyncio
import os
from argparse import ArgumentParser
from .validator import GaiaValidator

os.environ["NODE_TYPE"] = "validator"

def main():
    """Main entry point for the validator."""
    parser = ArgumentParser(description="Gaia Validator")
    
    # Subtensor arguments
    subtensor_group = parser.add_argument_group("subtensor")
    parser.add_argument("--wallet", type=str, help="Name of the wallet to use")
    parser.add_argument("--hotkey", type=str, help="Name of the hotkey to use")
    parser.add_argument("--netuid", type=int, help="Netuid to use")
    subtensor_group.add_argument(
        "--subtensor.chain_endpoint",
        type=str,
        help="Subtensor chain endpoint to use"
    )
    
    # Task arguments
    parser.add_argument(
        "--test-soil",
        action="store_true",
        help="Run soil moisture task immediately without waiting for windows"
    )
    
    args = parser.parse_args()
    validator = GaiaValidator(args)
    asyncio.run(validator.run())

if __name__ == "__main__":
    main() 