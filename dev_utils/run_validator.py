import os

from dotenv import load_dotenv
from fiber.miner.data.process_geomag_data import get_latest_geomag_data

load_dotenv(".env")
import asyncio

import httpx
from cryptography.fernet import Fernet

from fiber.chain import chain_utils
from fiber.logging_utils import get_logger
from fiber.validator import client as vali_client
from fiber.validator import handshake

logger = get_logger(__name__)


async def main():
    # Load needed stuff
    wallet_name = os.getenv("WALLET_NAME", "default")
    hotkey_name = os.getenv("HOTKEY_NAME", "default")
    keypair = chain_utils.load_hotkey_keypair(wallet_name, hotkey_name)
    httpx_client = httpx.AsyncClient()

    # Handshake with miner
    miner_address = "http://localhost:7999"
    miner_hotkey_ss58_address = "5H1JxH6w3hGZGaw8pRb7pksr5vrpHRPjsBWf6jz1rVHvLxpL"

    symmetric_key_str, symmetric_key_uuid = await handshake.perform_handshake(
        keypair=keypair,
        httpx_client=httpx_client,
        server_address=miner_address,
        miner_hotkey_ss58_address=miner_hotkey_ss58_address,
    )

    if symmetric_key_str is None or symmetric_key_uuid is None:
        raise ValueError("Symmetric key or UUID is None :-(")
    else:
        logger.info("Wohoo - handshake worked! :)")
    
    # Gabriel - this payload will need to be filled in with the proper data dynamically
    # Is this what we are looking for?
    timestamp, dst_value = get_latest_geomag_data()  # Fetch latest data

    payload = {
        "nonce": "12345",
        "data": {
            "name": "Geomagnetic data",
            "timestamp": str(timestamp),  # Convert timestamp to string for JSON compatibility
            "value": dst_value
        }
}

    fernet = Fernet(symmetric_key_str)

    resp = await vali_client.make_non_streamed_post(
        httpx_client=httpx_client,
        server_address=miner_address,
        fernet=fernet,
        keypair=keypair,
        symmetric_key_uuid=symmetric_key_uuid,
        validator_ss58_address=keypair.ss58_address,
        miner_ss58_address=miner_hotkey_ss58_address,
        payload=payload,
        endpoint="/geomagnetic-request",
    )
    resp.raise_for_status()
    logger.info(f"Geomagnetic request sent! Response: {resp.text}")


if __name__ == "__main__":
    asyncio.run(main())
