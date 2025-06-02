import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Union
import aioboto3
import boto3
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, ClientError
from fiber.logging_utils import get_logger
import threading
import time

logger = get_logger(__name__)

class R2StorageManager:
    """
    High-performance Cloudflare R2 storage manager using aioboto3 and boto3.
    Optimized for speed with multipart uploads, connection pooling, and concurrent operations.
    """
    
    def __init__(self, bucket_name: str, 
                 endpoint_url: str,
                 access_key_id: str,
                 secret_access_key: str,
                 region_name: str = "auto"):
        
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region_name = region_name
        
        # High-performance configuration
        self.config = Config(
            region_name=region_name,
            retries={
                'max_attempts': 3,
                'mode': 'adaptive'
            },
            max_pool_connections=50,  # High connection pool for concurrency
            parameter_validation=False,  # Skip validation for speed
            tcp_keepalive=True,
        )
        
        # Session and client management
        self._session = None
        self._sync_client = None
        self._thread_pool = ThreadPoolExecutor(max_workers=10)
        self._lock = threading.Lock()
        
        # Multipart upload settings
        self.multipart_threshold = 100 * 1024 * 1024  # 100MB
        self.multipart_chunksize = 10 * 1024 * 1024   # 10MB chunks
        self.max_concurrency = 10
        
        if not all([bucket_name, endpoint_url, access_key_id, secret_access_key]):
            raise ValueError("All R2 parameters (bucket_name, endpoint_url, access_key_id, secret_access_key) are required.")
        
        logger.info(f"R2StorageManager initialized for bucket '{bucket_name}' with high-performance settings")

    async def _get_session(self):
        """Get or create aioboto3 session with optimal settings."""
        if self._session is None:
            self._session = aioboto3.Session(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region_name
            )
        return self._session

    def _get_sync_client(self):
        """Get or create synchronous boto3 client for threading operations."""
        if self._sync_client is None:
            with self._lock:
                if self._sync_client is None:  # Double-check locking
                    self._sync_client = boto3.client(
                        's3',
                        endpoint_url=self.endpoint_url,
                        aws_access_key_id=self.access_key_id,
                        aws_secret_access_key=self.secret_access_key,
                        region_name=self.region_name,
                        config=self.config
                    )
        return self._sync_client

    async def _ensure_bucket_exists(self):
        """Ensure the bucket exists, create if it doesn't."""
        session = await self._get_session()
        async with session.client('s3', 
                                  endpoint_url=self.endpoint_url,
                                  config=self.config) as s3:
            try:
                await s3.head_bucket(Bucket=self.bucket_name)
                logger.debug(f"Bucket '{self.bucket_name}' exists")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    logger.info(f"Bucket '{self.bucket_name}' does not exist. Creating it...")
                    try:
                        await s3.create_bucket(Bucket=self.bucket_name)
                        logger.info(f"Bucket '{self.bucket_name}' created successfully")
                    except ClientError as create_error:
                        logger.error(f"Failed to create bucket '{self.bucket_name}': {create_error}")
                        raise
                else:
                    logger.error(f"Error checking bucket '{self.bucket_name}': {e}")
                    raise

    def _sync_upload_part(self, client, bucket, key, upload_id, part_number, data):
        """Synchronous helper for uploading a single part."""
        return client.upload_part(
            Bucket=bucket,
            Key=key,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=data
        )

    async def _multipart_upload(self, local_file_path: str, object_key: str) -> bool:
        """High-performance multipart upload for large files."""
        try:
            file_size = os.path.getsize(local_file_path)
            logger.info(f"Starting multipart upload for {local_file_path} ({file_size / (1024*1024):.1f}MB)")
            
            session = await self._get_session()
            async with session.client('s3', 
                                      endpoint_url=self.endpoint_url,
                                      config=self.config) as s3:
                
                # Initiate multipart upload
                response = await s3.create_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=object_key
                )
                upload_id = response['UploadId']
                
                try:
                    # Calculate parts
                    part_size = self.multipart_chunksize
                    parts = []
                    part_number = 1
                    
                    # Use thread pool for concurrent part uploads
                    sync_client = self._get_sync_client()
                    upload_tasks = []
                    
                    with open(local_file_path, 'rb') as f:
                        while True:
                            data = f.read(part_size)
                            if not data:
                                break
                            
                            # Submit part upload to thread pool
                            task = asyncio.get_event_loop().run_in_executor(
                                self._thread_pool,
                                self._sync_upload_part,
                                sync_client, self.bucket_name, object_key, upload_id, part_number, data
                            )
                            upload_tasks.append((part_number, task))
                            part_number += 1
                    
                    # Wait for all parts to complete
                    for part_num, task in upload_tasks:
                        result = await task
                        parts.append({
                            'ETag': result['ETag'],
                            'PartNumber': part_num
                        })
                    
                    # Complete multipart upload
                    await s3.complete_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=object_key,
                        UploadId=upload_id,
                        MultipartUpload={'Parts': parts}
                    )
                    
                    logger.info(f"Multipart upload completed for {object_key} ({len(parts)} parts)")
                    return True
                    
                except Exception as e:
                    # Abort multipart upload on error
                    try:
                        await s3.abort_multipart_upload(
                            Bucket=self.bucket_name,
                            Key=object_key,
                            UploadId=upload_id
                        )
                    except Exception as abort_error:
                        logger.error(f"Failed to abort multipart upload: {abort_error}")
                    raise e
                    
        except Exception as e:
            logger.error(f"Multipart upload failed for {object_key}: {e}")
            return False

    async def upload_blob(self, local_file_path: str, blob_name: str) -> bool:
        """
        Upload a file to R2 storage with automatic multipart for large files.
        Compatible with Azure blob interface.
        """
        try:
            await self._ensure_bucket_exists()
            
            file_size = os.path.getsize(local_file_path)
            
            # Use multipart upload for large files
            if file_size >= self.multipart_threshold:
                return await self._multipart_upload(local_file_path, blob_name)
            
            # Regular upload for smaller files
            session = await self._get_session()
            async with session.client('s3', 
                                      endpoint_url=self.endpoint_url,
                                      config=self.config) as s3:
                
                with open(local_file_path, 'rb') as f:
                    await s3.upload_fileobj(
                        f, 
                        self.bucket_name, 
                        blob_name,
                        ExtraArgs={
                            'StorageClass': 'STANDARD'  # R2 default
                        }
                    )
                
                logger.info(f"Successfully uploaded '{local_file_path}' to R2 as '{blob_name}' ({file_size / (1024*1024):.1f}MB)")
                return True
                
        except Exception as e:
            logger.error(f"Failed to upload '{local_file_path}' to R2 as '{blob_name}': {e}")
            return False

    async def download_blob(self, blob_name: str, local_file_path: str) -> bool:
        """
        Download a file from R2 storage with optimized streaming.
        Compatible with Azure blob interface.
        """
        try:
            session = await self._get_session()
            async with session.client('s3', 
                                      endpoint_url=self.endpoint_url,
                                      config=self.config) as s3:
                
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                
                with open(local_file_path, 'wb') as f:
                    await s3.download_fileobj(self.bucket_name, blob_name, f)
                
                file_size = os.path.getsize(local_file_path)
                logger.info(f"Successfully downloaded '{blob_name}' from R2 to '{local_file_path}' ({file_size / (1024*1024):.1f}MB)")
                return True
                
        except Exception as e:
            logger.error(f"Failed to download '{blob_name}' from R2 to '{local_file_path}': {e}")
            return False

    async def list_blobs(self, prefix: Optional[str] = None) -> List[str]:
        """
        List objects in R2 bucket with optional prefix.
        Compatible with Azure blob interface.
        """
        blob_names = []
        try:
            session = await self._get_session()
            async with session.client('s3', 
                                      endpoint_url=self.endpoint_url,
                                      config=self.config) as s3:
                
                kwargs = {'Bucket': self.bucket_name}
                if prefix:
                    kwargs['Prefix'] = prefix
                
                paginator = s3.get_paginator('list_objects_v2')
                async for page in paginator.paginate(**kwargs):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            blob_names.append(obj['Key'])
                
                logger.debug(f"Listed {len(blob_names)} objects with prefix '{prefix}'")
                return blob_names
                
        except Exception as e:
            logger.error(f"Failed to list blobs with prefix '{prefix}': {e}")
            return []

    async def delete_blob(self, blob_name: str) -> bool:
        """
        Delete an object from R2 storage.
        Compatible with Azure blob interface.
        """
        try:
            session = await self._get_session()
            async with session.client('s3', 
                                      endpoint_url=self.endpoint_url,
                                      config=self.config) as s3:
                
                await s3.delete_object(Bucket=self.bucket_name, Key=blob_name)
                logger.info(f"Successfully deleted object '{blob_name}' from R2")
                return True
                
        except Exception as e:
            logger.error(f"Failed to delete object '{blob_name}' from R2: {e}")
            return False

    async def read_blob_content(self, blob_name: str) -> Optional[str]:
        """
        Read text content from R2 object.
        Compatible with Azure blob interface.
        """
        try:
            session = await self._get_session()
            async with session.client('s3', 
                                      endpoint_url=self.endpoint_url,
                                      config=self.config) as s3:
                
                try:
                    response = await s3.get_object(Bucket=self.bucket_name, Key=blob_name)
                    content_bytes = await response['Body'].read()
                    return content_bytes.decode('utf-8').strip()
                except ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchKey':
                        logger.warning(f"Object '{blob_name}' does not exist in R2")
                        return None
                    raise
                    
        except Exception as e:
            logger.error(f"Failed to read content from R2 object '{blob_name}': {e}")
            return None

    async def upload_blob_content(self, content: str, blob_name: str) -> bool:
        """
        Upload text content to R2 object.
        Compatible with Azure blob interface.
        """
        try:
            await self._ensure_bucket_exists()
            
            session = await self._get_session()
            async with session.client('s3', 
                                      endpoint_url=self.endpoint_url,
                                      config=self.config) as s3:
                
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=blob_name,
                    Body=content.encode('utf-8'),
                    ContentType='text/plain'
                )
                
                logger.info(f"Successfully uploaded content to R2 object '{blob_name}'")
                return True
                
        except Exception as e:
            logger.error(f"Failed to upload content to R2 object '{blob_name}': {e}")
            return False

    async def close(self):
        """Clean up resources."""
        if self._sync_client:
            # Note: boto3 clients don't need explicit closing
            self._sync_client = None
        
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)
            self._thread_pool = None
        
        self._session = None
        logger.info("R2 Storage Manager closed")


async def get_r2_storage_manager_for_db_sync() -> Optional[R2StorageManager]:
    """
    Factory function to create R2StorageManager for database sync.
    Reads configuration from environment variables.
    """
    bucket_name = os.getenv("R2_BUCKET_NAME", "validator-db-sync")
    endpoint_url = os.getenv("R2_ENDPOINT_URL")
    access_key_id = os.getenv("R2_ACCESS_KEY_ID")
    secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY")
    region_name = os.getenv("R2_REGION_NAME", "auto")
    
    if not all([endpoint_url, access_key_id, secret_access_key]):
        logger.warning("R2 configuration incomplete. Required: R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
        return None
    
    try:
        manager = R2StorageManager(
            bucket_name=bucket_name,
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            region_name=region_name
        )
        
        # Test connection
        await manager._ensure_bucket_exists()
        logger.info("Successfully initialized R2 Storage Manager for database sync")
        return manager
        
    except Exception as e:
        logger.error(f"Failed to initialize R2 Storage Manager: {e}")
        return None


# Example/test function
async def _example_main():
    """Example usage of R2StorageManager."""
    # This would use environment variables in real usage
    manager = await get_r2_storage_manager_for_db_sync()
    if not manager:
        logger.error("Could not create R2 manager")
        return
    
    try:
        # Test operations
        test_content = "Hello from R2!"
        test_blob = "test/hello.txt"
        
        # Upload content
        success = await manager.upload_blob_content(test_content, test_blob)
        if success:
            logger.info("Test upload successful")
            
            # Read back content
            content = await manager.read_blob_content(test_blob)
            logger.info(f"Read back: {content}")
            
            # List blobs
            blobs = await manager.list_blobs("test/")
            logger.info(f"Found blobs: {blobs}")
            
            # Delete test blob
            await manager.delete_blob(test_blob)
            logger.info("Test cleanup completed")
        
    finally:
        await manager.close()


if __name__ == "__main__":
    asyncio.run(_example_main()) 