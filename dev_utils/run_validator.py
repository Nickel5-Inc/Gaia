import os
import asyncio
import ssl
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import httpx
from fiber.chain import chain_utils
from fiber.logging_utils import get_logger
from fiber.validator import client as vali_client, handshake
from fiber.chain.metagraph import Metagraph
from substrateinterface import SubstrateInterface
from fiber.miner.data.process_geomag_data import get_latest_geomag_data

logger = get_logger(__name__)

async def main():
    load_dotenv(".env")
    wallet_name = os.getenv("WALLET_NAME", "default")
    hotkey_name = os.getenv("HOTKEY_NAME", "default")
    keypair = chain_utils.load_hotkey_keypair(wallet_name, hotkey_name)

    # Create an httpx client with SSL verification disabled for self-signed certificates
    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        verify=False  # This disables SSL certificate verification
    ) as httpx_client:

        # Initialize Metagraph and sync nodes
        substrate = SubstrateInterface(url="wss://test.finney.opentensor.ai:443/")
        netuid = os.getenv("NETUID")
        metagraph = Metagraph(substrate=substrate, netuid=netuid)
        metagraph.sync_nodes()

        # Iterate over synced nodes (miners)
        for miner_hotkey, node in metagraph.nodes.items():
            base_url = f"https://{node.ip}:{node.port}"

            try:
                # Perform handshake with each miner using the httpx client
                print(keypair, httpx_client, base_url, miner_hotkey)
                symmetric_key_str, symmetric_key_uuid = await handshake.perform_handshake(
                    keypair=keypair,
                    httpx_client=httpx_client,
                    server_address=base_url,
                    miner_hotkey_ss58_address=miner_hotkey,
                )

                if symmetric_key_str and symmetric_key_uuid:
                    logger.info(f"Handshake successful with miner {miner_hotkey}")

                    # Prepare payload data
                    timestamp, dst_value = get_latest_geomag_data()
                    payload = {
                        "nonce": "12345",
                        "data": {
                            "name": "Geomagnetic data",
                            "timestamp": str(timestamp),
                            "value": dst_value,
                        }
                    }

                    fernet = Fernet(symmetric_key_str)

                    # Send the request
                    resp = await vali_client.make_non_streamed_post(
                        httpx_client=httpx_client,
                        server_address=base_url,
                        fernet=fernet,
                        keypair=keypair,
                        symmetric_key_uuid=symmetric_key_uuid,
                        validator_ss58_address=keypair.ss58_address,
                        miner_ss58_address=miner_hotkey,
                        payload=payload,
                        endpoint="/geomagnetic-request"
                    )
                    resp.raise_for_status()
                    logger.info(f"Geomagnetic request sent to {miner_hotkey}! Response: {resp.text}")
                else:
                    logger.warning(f"Failed handshake with miner {miner_hotkey}")

            except Exception as e:
                logger.error(f"Error with miner {miner_hotkey}: {e}")

if __name__ == "__main__":
    asyncio.run(main())