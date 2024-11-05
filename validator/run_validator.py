import os
import asyncio
import ssl
import traceback
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import httpx
from fiber.chain import chain_utils
from fiber.chain.post_ip_to_chain import post_node_ip_to_chain
from fiber.logging_utils import get_logger
from fiber.validator import client as vali_client, handshake
from fiber.chain.metagraph import Metagraph
from substrateinterface import SubstrateInterface
from argparse import ArgumentParser


logger = get_logger(__name__)



class GaiaValidator:
    '''
    Gaia validator class.
    '''
    def __init__(self, args):
        '''
        Setup the validator.
        Config, Bittensor Wallet Stuff, etc.

        '''
        # Parse arguments
        self.args = ArgumentParser().parse_args()

    

    def setup_neuron(self) -> bool:
        '''
        Setup the neuron.
        Loads variables from args or .env file, args take precedence
        '''
        try:
            # Load environment variables
            load_dotenv(".env")

            # Load wallet, hotkey, keypair
            self.wallet_name = self.args.wallet if self.args.wallet else os.getenv("WALLET_NAME", "default")
            self.hotkey_name = self.args.hotkey if self.args.hotkey else os.getenv("HOTKEY_NAME", "default")
            self.keypair = chain_utils.load_hotkey_keypair(self.wallet_name, self.hotkey_name)

            # Setup substrate interface and metagraph
            self.substrate = SubstrateInterface(url=self.subtensor_chain_endpoint)
            self.metagraph = Metagraph(substrate=self.substrate, netuid=self.netuid)

            #get coldkey address from metagraph
            self.coldkey_ss58_address = self.metagraph

            # Load external IP from machine
            try:
                self.external_ip = os.popen("curl ifconfig.me").read().strip()
            except Exception as e:
                logger.error(f"Error getting external IP: {e}")
                logger.error(f"traceback: {traceback.format_exc()}")
                return False
            self.external_port = self.args.port if self.args.port else os.getenv("PORT", 33334)
            self.netuid = self.args.netuid if self.args.netuid else os.getenv("NETUID", 1)
            self.subtensor_network = self.args.subtensor.network if self.args.subtensor.network else os.getenv("SUBSTRATE_NETWORK", "finney")
            self.subtensor_chain_endpoint = self.args.subtensor.chain_endpoint if self.args.subtensor.chain_endpoint else os.getenv("SUBSTRATE_CHAIN_ENDPOINT", "wss://test.finney.opentensor.ai:443/")

            # Try posting IP to chain, if fails, return False
            try:
                post_node_ip_to_chain(
                    substrate=self.substrate,
                    keypair=self.keypair,
                    netuid=self.netuid,
                    external_ip=self.external_ip,
                    external_port=self.external_port,
                    netuid=self.netuid,
                    wallet_name=self.wallet_name,
                    hotkey_name=self.hotkey_name
                )

            except Exception as e:
                logger.error(f"Error posting IP to chain: {e}")
                logger.error(f"traceback: {traceback.format_exc()}")
                return False





        except Exception as e:
            logger.error(f"Error setting up neuron: {e}")
            logger.error(f"traceback: {traceback.format_exc()}")
            return False
        
        finally:
            logger.info("Neuron setup complete")
            return True



    async def main(self):
        '''
        Main function to run the validator.

        '''
        # Setup neuron
        if not self.setup_neuron():
            logger.error("Failed to setup neuron, check config for errors")
            return


        # Create an httpx client with SSL verification disabled for self-signed certificates
        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            verify=False  # This disables SSL certificate verification
        ) as self.httpx_client:

            # Metagraph sync nodes
            self.metagraph.sync_nodes()

            # Iterate over synced nodes (miners)
            for miner_hotkey, node in self.metagraph.nodes.items():
                base_url = f"https://{node.ip}:{node.port}"

                try:
                    # Perform handshake with each miner using the httpx client
                    print(self.keypair, self.httpx_client, base_url, miner_hotkey)

                    symmetric_key_str, symmetric_key_uuid = await handshake.perform_handshake(
                        keypair=self.keypair,
                        httpx_client=self.httpx_client,
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
                            httpx_client=self.httpx_client,
                            server_address=base_url,
                            fernet=fernet,
                            keypair=self.keypair,
                            symmetric_key_uuid=symmetric_key_uuid,
                            validator_ss58_address=self.keypair.ss58_address,
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
    args = ArgumentParser()

    # bittensor args
    args.add_argument("--wallet.name", type=str, help="Name of the wallet to use")
    args.add_argument("--wallet.hotkey", type=str, help="Name of the hotkey to use")
    args.add_argument("--netuid", type=int, help="Netuid to use")
    args.add_argument("--subtensor.network", type=str, help="Subtensor network to use")
    args.add_argument("--subtensor.chain_endpoint", type=str, help="Subtensor chain endpoint to use")

    # other args
    args.add_argument("--env", type=str, help="Environment to use")
    args.add_argument("--log_level", type=str, help="Log level to use")
    args.add_argument("--port", type=int, help="Port to use")

    validator = GaiaValidator(args)
    asyncio.run(validator.main())