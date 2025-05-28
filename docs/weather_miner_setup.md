# Weather Task Setup for Miners

## Overview

The Weather Task is an **opt-in** feature for miners due to its heavy computational requirements. By default, it is **disabled** to ensure miners without appropriate hardware are not overwhelmed.

## Requirements

### Hardware Requirements (for Local Inference)
- **GPU**: NVIDIA GPU with at least 24GB VRAM (e.g., RTX 3090, RTX 4090, A5000, or better)
- **RAM**: Minimum 32GB system memory
- **Storage**: At least 500GB free space for:
  - GFS data caching
  - Model weights (~10GB)
  - Generated forecast outputs (Zarr directories)
- **CPU**: Modern multi-core processor (8+ cores recommended)
- **Network**: Stable high-speed internet for downloading GFS data

### Software Requirements
- CUDA 11.8+ with compatible drivers (for local inference)
- Python environment with PyTorch CUDA support
- All dependencies from `requirements.txt`

## Initial Setup

### 1. Generate JWT Secret Key

First, generate a secure JWT secret key for authenticating forecast data access:

```bash
cd /root/Gaia  # or your project root
python gaia/miner/utils/generate_jwt_secret.py
```

This will either:
- Add `MINER_JWT_SECRET_KEY` to your `.env` file automatically, or
- Display a new key if one already exists (you'll need to update it manually)

### 2. Configure Forecast Storage Directory

Set up where forecast outputs (Zarr directories) will be stored:

```bash
# In your .env file
MINER_FORECAST_DIR=/root/Gaia/miner_forecasts_background
```

Make sure this directory:
- Has sufficient free space (at least 100GB recommended)
- Is on a fast storage device (SSD preferred)
- Has proper read/write permissions

### 3. Choose Inference Type

You can run inference either locally (requires GPU) or using Azure Foundry (cloud-based).

#### Option A: Local Inference (Default)
```bash
# In your .env file
WEATHER_INFERENCE_TYPE=local  # or omit this line
```

#### Option B: Azure Foundry Inference
```bash
# In your .env file
WEATHER_INFERENCE_TYPE=azure_foundry
FOUNDRY_ENDPOINT_URL=<your-azure-endpoint>
FOUNDRY_ACCESS_TOKEN=<your-access-token>
BLOB_URL_WITH_RW_SAS=<your-blob-url-with-sas>
```

**Important**: Never share your Azure credentials or SAS tokens publicly. Keep them secure in your `.env` file.

### 4. Enable the Weather Task

Finally, enable the weather task:

```bash
# In your .env file
WEATHER_MINER_ENABLED=True
```

## Complete .env Configuration Example

### For Local Inference:
```bash
# Weather task configuration
WEATHER_MINER_ENABLED=True
MINER_JWT_SECRET_KEY=<generated-secret-key>
MINER_FORECAST_DIR=/root/Gaia/miner_forecasts_background
WEATHER_INFERENCE_TYPE=local  # Optional, defaults to local
```

### For Azure Foundry Inference:
```bash
# Weather task configuration
WEATHER_MINER_ENABLED=True
MINER_JWT_SECRET_KEY=<generated-secret-key>
MINER_FORECAST_DIR=/root/Gaia/miner_forecasts_background

# Azure Foundry configuration
WEATHER_INFERENCE_TYPE=azure_foundry
FOUNDRY_ENDPOINT_URL=<your-azure-endpoint>
FOUNDRY_ACCESS_TOKEN=<your-access-token>
BLOB_URL_WITH_RW_SAS=<your-blob-url-with-sas>
```

## Verification

When you start your miner, you should see one of these messages in the logs:

### If Enabled:
```
Weather task ENABLED for this miner (WEATHER_MINER_ENABLED=True)
Weather routes registered (weather task is enabled)
```

### If Disabled (default):
```
Weather task DISABLED for this miner. Set WEATHER_MINER_ENABLED=True to enable.
Weather routes NOT registered (weather task is disabled)
```

## Important Notes

1. **Zarr Storage**: The weather task generates Zarr directories containing forecast data. These can be large (several GB per forecast), so ensure adequate disk space.

2. **JWT Security**: The `MINER_JWT_SECRET_KEY` is used to sign access tokens for forecast data. Keep it secret and secure.

3. **Resource Usage**: When enabled, the weather task will:
   - Use significant GPU memory during inference (for local mode)
   - Download large GFS data files (several GB per forecast)
   - Generate and store Zarr directories for each forecast

4. **Validators**: Validators will only query miners that have weather endpoints available. Miners without the weather task enabled will simply be skipped for weather-related queries.

5. **Rewards**: Weather task participation may affect your rewards structure. Check the subnet documentation for current reward allocations.

6. **Monitoring**: Keep an eye on:
   - GPU memory usage (`nvidia-smi`) for local inference
   - Disk space for forecast storage
   - Network bandwidth during GFS downloads
   - Zarr directory sizes in your forecast directory

## Troubleshooting

### Weather task not starting
- Check that `WEATHER_MINER_ENABLED=True` is in your `.env` file
- Ensure you have generated and set `MINER_JWT_SECRET_KEY`
- Verify `MINER_FORECAST_DIR` exists and is writable
- Ensure you have restarted the miner after changing settings
- For local inference, verify GPU is available with `nvidia-smi`

### JWT errors
- Regenerate the JWT secret using `generate_jwt_secret.py`
- Ensure the key is properly set in your `.env` file
- Check for any special characters that might need escaping

### Azure Foundry errors
- Verify all Azure credentials are correctly set
- Check that the SAS token hasn't expired
- Test the endpoint URL is accessible
- Ensure proper network connectivity to Azure

### Out of memory errors
- For local inference: Ensure GPU has at least 24GB VRAM
- Check that no other processes are using the GPU
- Monitor system RAM usage
- Consider using Azure Foundry if local resources are insufficient

### Storage issues
- Check disk space in `MINER_FORECAST_DIR`
- Clean up old Zarr directories periodically
- Ensure fast storage (SSD) for better performance

### Network timeouts
- GFS downloads can be large; ensure stable internet
- Check firewall settings for outbound HTTPS connections
- Consider implementing a local GFS cache

## Disabling the Weather Task

To disable the weather task, either:
- Set `WEATHER_MINER_ENABLED=False` in your `.env` file
- Remove the `WEATHER_MINER_ENABLED` line entirely (defaults to false)
- Restart your miner 