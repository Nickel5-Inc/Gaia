import time

from cryptography.fernet import Fernet
from fastapi import APIRouter, Depends, Header

from fiber import constants as cst
from fiber.logging_utils import get_logger
from fiber.miner.core.configuration import Config
from fiber.miner.core.models.encryption import PublicKeyResponse, SymmetricKeyExchange
from fiber.miner.dependencies import blacklist_low_stake, get_config, verify_request
from fiber.miner.security.encryption import get_symmetric_key_b64_from_payload

logger = get_logger(__name__)


async def get_public_key(config: Config = Depends(get_config)):
    public_key = config.encryption_keys_handler.public_bytes.decode()
    return PublicKeyResponse(
        public_key=public_key,
        timestamp=time.time(),
    )


async def exchange_symmetric_key(
    payload: SymmetricKeyExchange,
    validator_hotkey_address: str = Header(..., alias=cst.VALIDATOR_HOTKEY),
    nonce: str = Header(..., alias=cst.NONCE),
    symmetric_key_uuid: str = Header(..., alias=cst.SYMMETRIC_KEY_UUID),
    config: Config = Depends(get_config),
):
    base64_symmetric_key = get_symmetric_key_b64_from_payload(payload, config.encryption_keys_handler.private_key)
    fernet = Fernet(base64_symmetric_key)
    config.encryption_keys_handler.add_symmetric_key(
        uuid=symmetric_key_uuid,
        hotkey_ss58_address=validator_hotkey_address,
        fernet=fernet,
    )

    return {"status": "Symmetric key exchanged successfully"}


def factory_router() -> APIRouter:
    router = APIRouter(tags=["Handshake"])
    router.add_api_route("/public-encryption-key", get_public_key, methods=["GET"])
    router.add_api_route(
        "/exchange-symmetric-key",
        exchange_symmetric_key,
        methods=["POST"],
        dependencies=[
            Depends(blacklist_low_stake),
            Depends(verify_request),
        ],
    )
    return router

async def perform_handshake(keypair, httpx_client, server_address, miner_hotkey_ss58_address):
    """
    Perform a handshake with the miner to exchange a symmetric key.

    Args:
        keypair: The validator's keypair used to sign requests and for encryption.
        httpx_client: The HTTPX client used to make asynchronous HTTP requests.
        server_address: The address of the miner (e.g., http://localhost:7999).
        miner_hotkey_ss58_address: The SS58 address of the miner.

    Returns:
        A tuple (symmetric_key_str, symmetric_key_uuid) if the handshake is successful,
        otherwise (None, None).
    """
    try:
        # Step 1: Get the miner's public encryption key
        logger.info(f"Requesting public encryption key from {server_address}")
        public_key_response = await httpx_client.get(f"{server_address}/public-encryption-key")
        public_key_response.raise_for_status()  # Ensure request was successful

        public_key = public_key_response.json()["public_key"]
        logger.info(f"Received public encryption key: {public_key}")

        # Step 2: Generate a symmetric key
        symmetric_key = Fernet.generate_key()
        symmetric_key_str = symmetric_key.decode("utf-8")
        logger.info(f"Generated symmetric key: {symmetric_key_str}")

        # Step 3: Encrypt the symmetric key using the miner's public key
        encrypted_symmetric_key = base64.b64encode(
            Fernet(public_key.encode()).encrypt(symmetric_key)
        ).decode("utf-8")
        logger.info(f"Encrypted symmetric key: {encrypted_symmetric_key}")

        # Generate a UUID for the symmetric key exchange
        symmetric_key_uuid = str(base64.urlsafe_b64encode(os.urandom(16)).decode("utf-8"))

        # Step 4: Define headers for the exchange-symmetric-key request
        headers = {
            "validator-hotkey": keypair.ss58_address,  # Validator's SS58 address
            "signature": keypair.sign(symmetric_key_str),  # Signature proving identity
            "miner-hotkey": miner_hotkey_ss58_address,  # Miner's SS58 address
            "nonce": str(int(time.time())),  # A unique nonce, here using a timestamp
            "symmetric-key-uuid": symmetric_key_uuid,  # UUID for the symmetric key exchange
        }

        # Step 5: Send the encrypted symmetric key to the miner
        logger.info(f"Sending symmetric key to miner at {server_address}")
        payload = {"encrypted_symmetric_key": encrypted_symmetric_key}
        response = await httpx_client.post(
            f"{server_address}/exchange-symmetric-key",
            headers=headers,
            json=payload
        )
        response.raise_for_status()  # Ensure the request was successful

        logger.info("Symmetric key exchange successful!")
        return symmetric_key_str, symmetric_key_uuid

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred during handshake: {e}")
        return None, None

    except Exception as e:
        logger.error(f"An error occurred during handshake: {e}")
        return None, None