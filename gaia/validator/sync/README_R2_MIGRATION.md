# Cloudflare R2 Storage Migration Guide

This guide explains how to migrate the Gaia Validator database sync system from Azure Blob Storage to Cloudflare R2 for improved performance and cost efficiency.

## Overview

The database sync system now supports both Azure Blob Storage and Cloudflare R2. R2 is recommended for new deployments due to:

- **Better Performance**: Optimized multipart uploads and downloads
- **Lower Costs**: No egress fees for data retrieval
- **Global Distribution**: Edge locations worldwide
- **S3 Compatibility**: Standard S3 API with broad ecosystem support

## Quick Start

### 1. Install R2 Dependencies

```bash
pip install -r requirements_r2.txt
```

### 2. Configure Environment Variables

Choose one of the configuration methods below:

#### Option A: Use R2 Only (Recommended)

```bash
# R2 Configuration
export STORAGE_BACKEND=r2
export R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
export R2_ACCESS_KEY_ID=your-r2-access-key
export R2_SECRET_ACCESS_KEY=your-r2-secret-key
export R2_BUCKET_NAME=validator-db-sync

# Optional: R2 region (default: auto)
export R2_REGION_NAME=auto
```

#### Option B: Auto-Detection (R2 First, Azure Fallback)

```bash
# Will try R2 first, then fall back to Azure if R2 not configured
export STORAGE_BACKEND=auto

# R2 Configuration (primary)
export R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
export R2_ACCESS_KEY_ID=your-r2-access-key
export R2_SECRET_ACCESS_KEY=your-r2-secret-key

# Azure Configuration (fallback) - keep existing vars
export AZURE_STORAGE_CONNECTION_STRING=your-azure-connection-string
```

### 3. Test Configuration

```bash
python -m gaia.validator.sync.storage_utils
```

## Cloudflare R2 Setup

### 1. Create R2 Bucket

1. Log into Cloudflare Dashboard
2. Go to R2 Object Storage
3. Create a new bucket (e.g., `validator-db-sync`)
4. Note your Account ID from the R2 dashboard

### 2. Generate API Tokens

1. Go to "Manage R2 API tokens" in your R2 dashboard
2. Create a new API token with:
   - **Permissions**: Object Read & Write
   - **Bucket**: Specific bucket or All buckets
   - **Duration**: No expiry (or as needed)
3. Save the Access Key ID and Secret Access Key

### 3. Construct Endpoint URL

Your endpoint URL format:
```
https://<ACCOUNT_ID>.r2.cloudflarestorage.com
```

## Environment Variables Reference

### Storage Backend Selection

| Variable | Values | Description |
|----------|--------|-------------|
| `STORAGE_BACKEND` | `r2`, `azure`, `auto` | Storage backend preference |

### R2 Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `R2_ENDPOINT_URL` | Yes | Your R2 endpoint URL |
| `R2_ACCESS_KEY_ID` | Yes | R2 API token access key |
| `R2_SECRET_ACCESS_KEY` | Yes | R2 API token secret |
| `R2_BUCKET_NAME` | No | Bucket name (default: `validator-db-sync`) |
| `R2_REGION_NAME` | No | Region (default: `auto`) |

### Azure Configuration (Legacy)

| Variable | Required | Description |
|----------|----------|-------------|
| `AZURE_STORAGE_CONNECTION_STRING` | Yes* | Azure connection string |
| `AZURE_STORAGE_ACCOUNT_URL` | Yes* | Azure storage account URL |
| `AZURE_STORAGE_SAS_TOKEN` | Yes* | Azure SAS token |

*Either connection string OR (account URL + SAS token) required

### Common Database Sync Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_SYNC_ENABLED` | `True` | Enable/disable database sync |
| `IS_SOURCE_VALIDATOR_FOR_DB_SYNC` | `False` | Source (backup) vs replica (restore) |
| `DB_SYNC_INTERVAL_HOURS` | `1` | Sync interval in hours |
| `DB_SYNC_MAX_BACKUPS` | `5` | Maximum backups to retain |

## Migration Process

### For Source Validators (Backup Nodes)

1. **Configure R2**: Set up R2 credentials and bucket
2. **Test Connection**: Verify R2 connectivity
3. **Switch Backend**: Set `STORAGE_BACKEND=r2`
4. **Restart Validator**: New backups will go to R2
5. **Optional**: Migrate existing Azure backups to R2

### For Replica Validators (Restore Nodes)

1. **Wait for Source**: Ensure source validator is using R2
2. **Configure R2**: Use same bucket as source
3. **Test Connection**: Verify R2 connectivity  
4. **Switch Backend**: Set `STORAGE_BACKEND=r2`
5. **Restart Validator**: Will restore from R2 backups

### Migrating Existing Backups (Optional)

To migrate existing Azure backups to R2:

```python
import asyncio
from gaia.validator.sync.azure_blob_utils import get_azure_blob_manager_for_db_sync
from gaia.validator.sync.r2_storage_utils import get_r2_storage_manager_for_db_sync

async def migrate_backups():
    # Initialize both storage managers
    azure_manager = await get_azure_blob_manager_for_db_sync()
    r2_manager = await get_r2_storage_manager_for_db_sync()
    
    # List Azure backups
    backups = await azure_manager.list_blobs(prefix="dumps/")
    
    for backup_name in backups:
        print(f"Migrating {backup_name}...")
        
        # Download from Azure
        local_path = f"/tmp/{backup_name.replace('/', '_')}"
        await azure_manager.download_blob(backup_name, local_path)
        
        # Upload to R2
        await r2_manager.upload_blob(local_path, backup_name)
        
        # Clean up local file
        os.remove(local_path)
    
    # Migrate manifest file
    manifest = await azure_manager.read_blob_content("latest_backup_manifest.txt")
    if manifest:
        await r2_manager.upload_blob_content(manifest, "latest_backup_manifest.txt")
    
    await azure_manager.close()
    await r2_manager.close()

# Run migration
asyncio.run(migrate_backups())
```

## Performance Optimizations

The R2 implementation includes several performance optimizations:

### Automatic Multipart Upload
- Files >100MB use multipart upload (10MB chunks)
- Concurrent part uploads (10 parallel by default)
- Optimized for large database dumps

### Connection Pooling
- 50 connection pool for high concurrency
- TCP keepalive enabled
- Adaptive retry strategy

### Memory Optimization
- Streaming uploads/downloads
- Minimal memory footprint
- Efficient cleanup

## Monitoring and Troubleshooting

### Test Storage Configuration

```bash
python -c "
import asyncio
from gaia.validator.sync.storage_utils import test_storage_configuration

async def test():
    result = await test_storage_configuration()
    print('Storage Test Results:')
    for key, value in result.items():
        print(f'  {key}: {value}')

asyncio.run(test())
"
```

### Enable Debug Logging

```bash
export LOG_LEVEL=DEBUG
```

### Common Issues

1. **Connection Timeout**: Check endpoint URL format
2. **Permission Denied**: Verify API token permissions
3. **Bucket Not Found**: Ensure bucket exists and name is correct
4. **Migration Issues**: Run storage test after each step

## Cost Comparison

### Cloudflare R2
- Storage: $0.015/GB/month
- Class A operations: $4.50/million
- Class B operations: $0.36/million
- **Egress: FREE** 🎉

### Azure Blob Storage
- Storage: ~$0.018/GB/month (hot tier)
- Operations: ~$0.065/10k transactions
- Egress: $0.087/GB (after 100GB free)

**Estimated savings: 40-60% for typical validator workloads**

## Rollback Plan

If you need to rollback to Azure:

1. Set `STORAGE_BACKEND=azure`
2. Ensure Azure credentials are still configured
3. Restart validator
4. Optionally migrate recent R2 backups back to Azure

## Support

For issues with this migration:

1. Check the troubleshooting section above
2. Test storage configuration
3. Review validator logs for specific error messages
4. Ensure all environment variables are correctly set

The unified storage interface maintains backward compatibility, so existing Azure deployments continue to work without changes. 