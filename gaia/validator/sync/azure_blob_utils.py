import os
from azure.storage.blob.aio import BlobServiceClient
from fiber.logging_utils import get_logger
import asyncio
# from azure.identity.aio import DefaultAzureCredential # Removed as Service Principal is no longer an option
# from azure.core.exceptions import ClientAuthenticationError # Removed with DefaultAzureCredential
import datetime

logger = get_logger(__name__)

class AzureBlobManager:
    def __init__(self, container_name: str, 
                 storage_account_url: str | None = None, 
                 sas_token: str | None = None, 
                 connection_string: str | None = None):
        
        self.storage_account_url = storage_account_url
        self.container_name = container_name
        self.connection_string = connection_string
        self.sas_token = sas_token
        self._blob_service_client = None
        self._container_client = None

        if not container_name:
            raise ValueError("Azure Blob container name is required.")

        # Determine auth method and validate
        self.auth_method = None
        if self.storage_account_url and self.sas_token:
            logger.info("AzureBlobManager configured with SAS token.")
            self.auth_method = "sas"
        elif self.connection_string:
            logger.info("AzureBlobManager configured with connection string.")
            self.auth_method = "connection_string"
        # Service Principal option removed
        # elif self.storage_account_url and os.getenv("AZURE_CLIENT_ID") and os.getenv("AZURE_TENANT_ID") and (os.getenv("AZURE_CLIENT_SECRET") or os.getenv("AZURE_CLIENT_CERTIFICATE_PATH")):
        #     logger.info("AzureBlobManager configured with DefaultAzureCredential (Service Principal).")
        #     self.auth_method = "service_principal"
        else:
            raise ValueError("AzureBlobManager initialized with an invalid combination or insufficient authentication parameters. Ensure SAS token or Connection String is provided.")

    async def _get_or_create_blob_service_client(self):
        if self._blob_service_client is None:
            if self.auth_method == "sas":
                logger.info("Creating BlobServiceClient with SAS token.")
                if not self.storage_account_url or not self.sas_token:
                    raise ValueError("Missing storage_account_url or sas_token for SAS authentication.")
                self._blob_service_client = BlobServiceClient(account_url=self.storage_account_url, credential=self.sas_token)
                try:
                    async with self._blob_service_client:
                        async for _ in self._blob_service_client.list_containers(name_starts_with="authchecksas"): 
                            break
                    logger.info("Successfully tested BlobServiceClient with SAS token.")
                except Exception as e:
                    logger.error(f"Failed to test BlobServiceClient with SAS token: {e}. Ensure SAS token is valid and has permissions for the storage account.")
                    raise ValueError(f"Failed to initialize or test BlobServiceClient with SAS token. Original error: {e}")

            elif self.auth_method == "connection_string":
                logger.info("Creating BlobServiceClient from connection string.")
                if not self.connection_string:
                    raise ValueError("Missing connection_string for connection string authentication.")
                self._blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            
            # Service Principal option removed
            # elif self.auth_method == "service_principal":
            #     logger.info("Creating BlobServiceClient with DefaultAzureCredential (Service Principal).")
            #     # ... (Service Principal logic was here)
            else:
                raise RuntimeError("AzureBlobManager auth_method not set or invalid, or required parameters missing.")
        return self._blob_service_client

    async def _get_container_client(self):
        client = await self._get_or_create_blob_service_client()
        # Check if container_client needs re-initialization, e.g., if it wasn't created or account_url changed (not typical here)
        if self._container_client is None or self._container_client.container_name != self.container_name:
            self._container_client = client.get_container_client(self.container_name)
            try:
                # Check existence only if client was just created or changed
                if not await self._container_client.exists():
                    logger.info(f"Container '{self.container_name}' does not exist. Creating it...")
                    await self._container_client.create_container()
                    logger.info(f"Container '{self.container_name}' created successfully.")
            except Exception as e:
                logger.error(f"Failed to check or create container '{self.container_name}': {e}")
                # Depending on policy, you might want to raise here or let subsequent operations fail.
        return self._container_client

    async def upload_blob(self, local_file_path: str, blob_name: str) -> bool:
        try:
            container_client = await self._get_container_client()
            blob_client = container_client.get_blob_client(blob_name)
            with open(local_file_path, "rb") as data:
                await blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Successfully uploaded '{local_file_path}' to Azure Blob Storage as '{blob_name}'.")
            return True
        except Exception as e:
            logger.error(f"Failed to upload '{local_file_path}' to Azure Blob Storage as '{blob_name}': {e}")
            return False

    async def download_blob(self, blob_name: str, local_file_path: str) -> bool:
        try:
            container_client = await self._get_container_client()
            blob_client = container_client.get_blob_client(blob_name)
            with open(local_file_path, "wb") as download_file:
                download_stream = await blob_client.download_blob()
                data = await download_stream.readall()
                download_file.write(data)
            logger.info(f"Successfully downloaded '{blob_name}' from Azure Blob Storage to '{local_file_path}'.")
            return True
        except Exception as e:
            logger.error(f"Failed to download '{blob_name}' from Azure Blob Storage to '{local_file_path}': {e}")
            return False

    async def list_blobs(self, prefix: str = None) -> list[str]:
        blob_names = []
        try:
            container_client = await self._get_container_client()
            async for blob in container_client.list_blobs(name_starts_with=prefix):
                blob_names.append(blob.name)
            return blob_names
        except Exception as e:
            logger.error(f"Failed to list blobs with prefix '{prefix}': {e}")
            return []

    async def delete_blob(self, blob_name: str) -> bool:
        try:
            container_client = await self._get_container_client()
            blob_client = container_client.get_blob_client(blob_name)
            await blob_client.delete_blob()
            logger.info(f"Successfully deleted blob '{blob_name}'.")
            return True
        except Exception as e:
            logger.error(f"Failed to delete blob '{blob_name}': {e}")
            return False

    async def read_blob_content(self, blob_name: str) -> str | None:
        try:
            container_client = await self._get_container_client()
            blob_client = container_client.get_blob_client(blob_name)
            if not await blob_client.exists():
                logger.warning(f"Manifest blob '{blob_name}' does not exist.")
                return None
            download_stream = await blob_client.download_blob()
            content_bytes = await download_stream.readall()
            return content_bytes.decode('utf-8').strip()
        except Exception as e:
            logger.error(f"Failed to read content from blob '{blob_name}': {e}")
            return None

    async def upload_blob_content(self, content: str, blob_name: str) -> bool:
        try:
            container_client = await self._get_container_client()
            blob_client = container_client.get_blob_client(blob_name)
            await blob_client.upload_blob(content.encode('utf-8'), overwrite=True, content_settings={'contentType': 'text/plain'})
            logger.info(f"Successfully uploaded content to blob '{blob_name}'.")
            return True
        except Exception as e:
            logger.error(f"Failed to upload content to blob '{blob_name}': {e}")
            return False

    async def close(self):
        if self._blob_service_client:
            await self._blob_service_client.close()
            self._blob_service_client = None
            self._container_client = None # Also reset container client
            logger.info("Azure Blob Service client closed.")

async def get_azure_blob_manager_for_db_sync() -> AzureBlobManager | None:
    container_name = os.getenv("AZURE_BLOB_CONTAINER_NAME_DB_SYNC", "validator-db-sync")
    
    storage_account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
    sas_token = os.getenv("AZURE_STORAGE_SAS_TOKEN")

    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    
    # Service Principal environment variables are no longer read here
    # client_id = os.getenv("AZURE_CLIENT_ID")
    # tenant_id = os.getenv("AZURE_TENANT_ID")
    # client_secret = os.getenv("AZURE_CLIENT_SECRET")
    # client_cert_path = os.getenv("AZURE_CLIENT_CERTIFICATE_PATH")

    # Priority 1: SAS Token authentication
    if storage_account_url and sas_token:
        logger.info("Attempting to initialize AzureBlobManager with SAS Token.")
        try:
            return AzureBlobManager(container_name=container_name, 
                                    storage_account_url=storage_account_url, 
                                    sas_token=sas_token)
        except ValueError as e:
            logger.warning(f"Failed to initialize AzureBlobManager with SAS Token: {e}. Checking other methods.")
    
    # Priority 2: Service Principal / DefaultAzureCredential - REMOVED
    # if storage_account_url and not sas_token and client_id and tenant_id and (client_secret or client_cert_path):
    #    # ... (Service Principal logic was here)
    
    # Priority 2 (was 3): Connection String
    if conn_str and not (storage_account_url and sas_token): # ensure not misconfigured with SAS if conn_str is also present
        logger.info("Attempting to initialize AzureBlobManager with Connection String.")
        try:
            return AzureBlobManager(container_name=container_name, 
                                    connection_string=conn_str)
        except ValueError as e:
            logger.error(f"Failed to initialize AzureBlobManager with connection string: {e}")
            return None
            
    logger.error("Azure Storage credentials not sufficiently configured for DB sync. "
                 "Please provide one of the following methods:\n"
                 "1. SAS Token: Set AZURE_STORAGE_ACCOUNT_URL and AZURE_STORAGE_SAS_TOKEN.\n"
                 "2. Connection String: Set AZURE_STORAGE_CONNECTION_STRING.")
    return None

# Example Usage (for testing, not part of the final app logic here)
async def _example_main():
    logger.info("Testing AzureBlobManager...")
    # This example primarily tests the connection string path or a fully configured DefaultAzureCredential environment.
    
    manager = await get_azure_blob_manager_for_db_sync()
    if not manager:
        logger.error("Failed to get AzureBlobManager instance. Ensure credentials are set (see logs above).")
        return

    try:
        # Test content upload/read (manifest file)
        manifest_blob_name = "test_latest_backup.txt"
        test_content = f"validator_backup_test_{datetime.datetime.utcnow().timestamp()}.dump"
        logger.info(f"Attempting to upload content to '{manifest_blob_name}'...")
        if await manager.upload_blob_content(test_content, manifest_blob_name):
            logger.info(f"Reading content from '{manifest_blob_name}'...")
            read_content = await manager.read_blob_content(manifest_blob_name)
            if read_content == test_content:
                logger.info(f"SUCCESS: Content upload and read matches for '{manifest_blob_name}'.")
            else:
                logger.error(f"FAILURE: Content mismatch. Expected '{test_content}', got '{read_content}'.")
        else:
            logger.error(f"FAILURE: Could not upload content to '{manifest_blob_name}'.")

        # Test file upload
        local_test_file = "test_upload.txt"
        with open(local_test_file, "w") as f:
            f.write("This is a test file for Azure blob upload for DB sync.")
        
        test_blob_name = f"test_dir/test_upload_{datetime.datetime.utcnow().timestamp()}.txt"
        logger.info(f"Attempting to upload file '{local_test_file}' to '{test_blob_name}'...")
        if await manager.upload_blob(local_test_file, test_blob_name):
            logger.info(f"SUCCESS: File upload for '{test_blob_name}'.")

            logger.info("Listing blobs with prefix 'test_dir/'...")
            blobs = await manager.list_blobs(prefix="test_dir/")
            if test_blob_name in blobs:
                logger.info(f"SUCCESS: Listed blobs includes '{test_blob_name}'.")
            else:
                logger.error(f"FAILURE: Blob '{test_blob_name}' not found in list: {blobs}")

            downloaded_file = "test_downloaded.txt"
            logger.info(f"Attempting to download '{test_blob_name}' to '{downloaded_file}'...")
            if await manager.download_blob(test_blob_name, downloaded_file):
                with open(local_test_file, "r") as f_orig, open(downloaded_file, "r") as f_down:
                    if f_orig.read() == f_down.read():
                        logger.info(f"SUCCESS: Downloaded file content matches for '{downloaded_file}'.")
                    else:
                        logger.error(f"FAILURE: Downloaded file content mismatch for '{downloaded_file}'.")
                os.remove(downloaded_file)
            else:
                logger.error(f"FAILURE: Could not download '{test_blob_name}'.")
            await manager.delete_blob(test_blob_name) # Clean up uploaded file
        else:
            logger.error(f"FAILURE: Could not upload '{local_test_file}'.")
        
        await manager.delete_blob(manifest_blob_name) # Clean up manifest
        if os.path.exists(local_test_file):
            os.remove(local_test_file)
        
        logger.info("AzureBlobManager test completed.")

    except Exception as e:
        logger.error(f"Error during AzureBlobManager _example_main: {e}", exc_info=True)
    finally:
        if manager:
            await manager.close()

if __name__ == "__main__":
    asyncio.run(_example_main()) 