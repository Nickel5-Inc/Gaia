# Gaia Miner Setup Guide

The definitive guide for setting up and running a Gaia miner that participates in Geomagnetic, Soil Moisture, and Weather forecasting tasks.

## Table of Contents

- [Quick Start](#quick-start)
- [Weather Task Setup](#weather-task-setup)
- [Complete Configuration Reference](#complete-configuration-reference)
- [Critical Variable Names](#critical-variable-names)
- [Task Descriptions](#task-descriptions)
- [Troubleshooting](#troubleshooting)
- [Hardware Requirements](#hardware-requirements)

---

## Quick Start

### 1. Environment Configuration

Create a `.env` file in your miner directory:

```bash
# --- Basic Miner Configuration ---
WALLET_NAME=<YOUR_WALLET_NAME>
HOTKEY_NAME=<YOUR_HOTKEY_NAME>
NETUID=<NETUID>  # 57 for mainnet, 237 for testnet
SUBTENSOR_NETWORK=<NETWORK>  # finney or test
MIN_STAKE_THRESHOLD=<STAKE>  # 10000 for mainnet, 0 for testnet

# --- Database Configuration ---
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=miner_db
DB_TARGET=miner
DB_CONNECTION_TYPE=socket
ALEMBIC_AUTO_UPGRADE=True

# --- Network Configuration ---
PUBLIC_PORT=33333  # Port posted to the chain
PORT=33334         # Internal port the miner listens on
EXTERNAL_IP="your_external_ip_address"

# --- General Settings ---
MINER_LOGGING_LEVEL=INFO
ENV=prod
MINER_JWT_SECRET_KEY=<GENERATE_WITH_SCRIPT>  # See generation instructions below
```

### 2. Generate JWT Secret Key

```bash
cd /root/Gaia  # or your project root
python gaia/miner/utils/generate_jwt_secret.py
```

### 3. Run the Miner

```bash
cd gaia/miner
python miner.py

# Or with PM2:
pm2 start --name miner --instances 1 python -- gaia/miner/miner.py
```

---

## Weather Task Setup

The Weather Task is **opt-in** due to high computational requirements and is disabled by default.

### Basic Weather Configuration

```bash
# Enable weather task
WEATHER_MINER_ENABLED=True

# Storage directories
MINER_FORECAST_DIR=/root/Gaia/miner_forecasts_background
MINER_GFS_ANALYSIS_CACHE_DIR="./gfs_analysis_cache_miner"

# File serving mode
WEATHER_FILE_SERVING_MODE=local  # or "r2_proxy"
```

### Inference Options

#### Option 1: HTTP Inference Service (Recommended)

Best for most users - uses remote GPU infrastructure:

```bash
WEATHER_INFERENCE_TYPE=http_service
WEATHER_INFERENCE_SERVICE_URL="http://localhost:8000/run_inference"

# ⚠️ CRITICAL: R2 Storage - Use these EXACT variable names ⚠️
R2_ENDPOINT_URL=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
R2_BUCKET=<YOUR_R2_BUCKET_NAME>
R2_ACCESS_KEY=<YOUR_R2_ACCESS_KEY_ID>
R2_SECRET_ACCESS_KEY=<YOUR_R2_SECRET_ACCESS_KEY>

# Inference Service API Key
INFERENCE_SERVICE_API_KEY=<YOUR_API_KEY>
```

#### Option 2: Local Inference (Requires GPU)

For users with powerful local hardware:

```bash
WEATHER_INFERENCE_TYPE=local

# Hardware Requirements:
# - NVIDIA GPU with 24GB+ VRAM (RTX 3090, RTX 4090, A5000+)
# - 32GB+ system RAM
# - 500GB+ free storage
```

#### Option 3: Azure Foundry Inference

For cloud-based inference:

```bash
WEATHER_INFERENCE_TYPE=azure_foundry
FOUNDRY_ENDPOINT_URL=<YOUR_AZURE_ENDPOINT>
FOUNDRY_ACCESS_TOKEN=<YOUR_AZURE_TOKEN>
BLOB_URL_WITH_RW_SAS=<YOUR_AZURE_BLOB_SAS_URL>
```

### Weather File Serving Modes

#### Local Storage Mode (Default)
```bash
WEATHER_FILE_SERVING_MODE=local
```
- Downloads forecast files from R2 to local storage
- Serves files directly via HTTP/zarr
- **Pros:** Faster validator access, original zarr design
- **Cons:** Requires more local storage space

#### R2 Proxy Mode 
```bash
WEATHER_FILE_SERVING_MODE=r2_proxy
```
- Streams files from R2 on-demand without local storage
- Miner acts as a proxy between validators and R2
- **Pros:** Minimal storage requirements, R2 credentials stay private
- **Cons:** Higher network usage, slight latency for validator requests

---

## Complete Configuration Reference

See [`miner_template.env`](../../miner_template.env) for a complete template with all possible configuration options.

### Required Variables
```bash
# Basic identification
WALLET_NAME=<YOUR_WALLET_NAME>
HOTKEY_NAME=<YOUR_HOTKEY_NAME>
NETUID=<NETUID>
SUBTENSOR_NETWORK=<NETWORK>

# Essential security
MINER_JWT_SECRET_KEY=<GENERATED_SECRET>

# Network ports
PUBLIC_PORT=33333
PORT=33334
```

### Database Configuration
```bash
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=miner_db
DB_TARGET=miner
DB_CONNECTION_TYPE=socket  # or "tcp"
ALEMBIC_AUTO_UPGRADE=True
```

### Weather Task Variables (Optional)
```bash
# Core weather configuration
WEATHER_MINER_ENABLED=False  # Set to True to enable
WEATHER_INFERENCE_TYPE=http_service
MINER_FORECAST_DIR=/root/Gaia/miner_forecasts_background
WEATHER_FILE_SERVING_MODE=local

# R2 Storage (for HTTP inference)
R2_ENDPOINT_URL=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
R2_BUCKET=<BUCKET_NAME>
R2_ACCESS_KEY=<ACCESS_KEY>
R2_SECRET_ACCESS_KEY=<SECRET_KEY>
INFERENCE_SERVICE_API_KEY=<API_KEY>

# HTTP Service URL
WEATHER_INFERENCE_SERVICE_URL="http://localhost:8000/run_inference"
```

---

## Critical Variable Names

### ⚠️ MUST Use Exact Names

The following variable names **must** match exactly what the code expects:

**R2 Storage:**
- ✅ `R2_BUCKET` (NOT `R2_BUCKET_NAME`)
- ✅ `R2_ACCESS_KEY` (NOT `R2_ACCESS_KEY_ID`)
- ✅ `R2_SECRET_ACCESS_KEY` (correct)
- ✅ `R2_ENDPOINT_URL` (correct)

**Port Configuration:**
- ✅ `PORT=33334` (NOT `INTERNAL_PORT`)
- ✅ `PUBLIC_PORT=33333` (correct)

**API Keys:**
- ✅ `INFERENCE_SERVICE_API_KEY` (primary)
- ✅ `WEATHER_RUNPOD_API_KEY` (fallback)

### Common Variable Name Mistakes

❌ **WRONG:**
```bash
R2_BUCKET_NAME=my-bucket      # Wrong!
R2_ACCESS_KEY_ID=my-key       # Wrong!
INTERNAL_PORT=33334           # Wrong!
```

✅ **CORRECT:**
```bash
R2_BUCKET=my-bucket           # Correct
R2_ACCESS_KEY=my-key          # Correct  
PORT=33334                    # Correct
```

### Verification Commands

Check your configuration:

```bash
# Verify R2 variable names
grep -E "R2_BUCKET|R2_ACCESS_KEY" .env
# Should show R2_BUCKET= and R2_ACCESS_KEY= (without suffixes)

# Check for deprecated variables
grep -E "INTERNAL_PORT|R2_BUCKET_NAME|R2_ACCESS_KEY_ID" .env
# Should return no matches (these are wrong)

# Verify port configuration
grep "PORT=" .env
# Should show PORT=33334
```

---

## Task Descriptions

### Geomagnetic Task

**Purpose:** Forecast the DST (Disturbance Storm Time) index to predict geomagnetic disturbances affecting GPS, communications, and power grids.

**Data Sources:**
- Hourly DST index values from validators
- Optional historical DST data for model improvement

**Output:**
- Predicted DST value for the next hour
- UTC timestamp of the last observation

**Process:**
1. Receive cleaned DataFrame with timestamp and DST values
2. Process historical data if available
3. Generate prediction using `GeomagneticPreprocessing`
4. Return prediction and timestamp

### Soil Moisture Task

**Purpose:** Predict soil moisture levels using satellite imagery and weather data to support agriculture and environmental monitoring.

**Data Sources:**
- Sentinel-2 satellite imagery
- IFS weather forecast data
- SMAP soil moisture data (for scoring)
- SRTM elevation data
- NDVI vegetation indices

**Process:**
1. **Region Selection:** Choose analysis regions avoiding urban/water areas
2. **Data Retrieval:** Gather multi-source datasets via APIs
3. **Data Compilation:** Create .tiff files with band order: [Sentinel-2, IFS, SRTM, NDVI]
4. **Model Inference:** Process through `soil_model.py`
5. **Validation:** Compare predictions against ground truth SMAP data

**IFS Weather Variables (in order):**
- t2m: Surface air temperature (2m) [Kelvin]
- tp: Total precipitation [m/day]
- ssrd: Surface solar radiation downwards [J/m²]
- st: Surface soil temperature [Kelvin]
- stl2/stl3: Soil temperature at 2m/3m depth [Kelvin]
- sp: Surface pressure [Pascals]
- d2m: Dewpoint temperature [Kelvin]
- u10/v10: Wind components at 10m [m/s]
- ro: Total runoff [m/day]
- msl: Mean sea level pressure [Pascals]
- et0: Reference evapotranspiration [mm/day]
- bare_soil_evap: Bare soil evaporation [mm/day]
- svp: Saturated vapor pressure [kPa]
- avp: Actual vapor pressure [kPa]
- r_n: Net radiation [MJ/m²/day]

### Weather Task

**Purpose:** Generate detailed weather forecasts using the Microsoft Aurora model for meteorological prediction.

**Key Features:**
- 40-step forecasts at 6-hour intervals (10-day forecasts)
- Zarr-based output format for efficient data access
- Multiple inference backends (local, HTTP service, Azure Foundry)
- Configurable file serving (local storage vs R2 proxy)
- Comprehensive verification and scoring systems

**Workflow:**
1. **Data Reception:** Receive GFS initialization data from validators
2. **Data Processing:** Convert GFS data into Aurora-compatible format
3. **Inference:** Run multi-step forecast generation (local or remote)
4. **Output Generation:** Create Zarr stores with forecast data
5. **File Serving:** Serve data to validators via HTTP/zarr or R2 proxy
6. **Verification:** Enable validator verification and scoring

**Architecture Comparison:**

**Local Storage Mode:**
```
RunPod → R2 Upload → Download to Miner → Serve Local Zarr → Validator
```

**R2 Proxy Mode:**
```
RunPod → R2 Upload → Miner Proxy → Validator
                        ↑
                   (Streams from R2)
```

---

## Hardware Requirements

### Basic Miner (Geomagnetic + Soil Moisture)
- **CPU:** 4+ cores
- **RAM:** 8GB minimum
- **Storage:** 50GB+ for databases and caching
- **Network:** Stable broadband internet

### Weather Task Local Inference
- **GPU:** NVIDIA with 24GB+ VRAM
  - Recommended: RTX 3090, RTX 4090, A5000, A6000, H100
- **CPU:** 8+ cores (16+ recommended)
- **RAM:** 32GB+ system memory (64GB recommended)
- **Storage:** 500GB+ fast storage (NVMe SSD preferred)
  - GFS cache: ~50GB
  - Forecast outputs: ~100GB (local mode)
  - Model weights: ~10GB
- **Network:** High-speed internet for GFS downloads (multi-GB files)

### Weather Task HTTP Service
- **CPU:** 4+ cores
- **RAM:** 16GB+ 
- **Storage:** 100GB+ (for local mode) or 20GB+ (for R2 proxy mode)
- **Network:** Stable high-speed internet
- **External GPU:** Access to RunPod or similar GPU service

---

## Troubleshooting

### Common Issues

#### Weather Task Not Starting
**Symptoms:**
```
Weather task DISABLED for this miner
```

**Solutions:**
1. Set `WEATHER_MINER_ENABLED=True`
2. Generate JWT secret: `python gaia/miner/utils/generate_jwt_secret.py`
3. Ensure forecast directory exists and is writable
4. Restart miner after configuration changes
5. For local inference: verify GPU with `nvidia-smi`

#### R2 Connection Errors
**Symptoms:**
```
R2 client configuration is incomplete
R2 connection failed
```

**Solutions:**
1. Verify exact variable names: `R2_BUCKET`, `R2_ACCESS_KEY`
2. Check R2 credentials and permissions
3. Ensure endpoint URL includes account ID
4. Test R2 connectivity independently

#### API Key Issues
**Symptoms:**
```
No RunPod API Key found
Authentication failed
```

**Solutions:**
1. Use `INFERENCE_SERVICE_API_KEY` (primary)
2. Or `WEATHER_RUNPOD_API_KEY` (fallback)
3. Ensure API key matches inference service configuration
4. Check for typos or extra spaces

#### Database Connection Issues
**Symptoms:**
```
Database connection failed
psycopg2.OperationalError
```

**Solutions:**
1. Verify PostgreSQL is running: `sudo systemctl status postgresql`
2. Check database credentials in `.env`
3. Ensure database exists: `createdb miner_db`
4. Test connection manually

#### Out of Memory Errors (Local Weather)
**Symptoms:**
```
CUDA out of memory
RuntimeError: CUDA error
```

**Solutions:**
1. Ensure 24GB+ GPU VRAM available
2. Check no other processes using GPU: `nvidia-smi`
3. Monitor system RAM usage
4. Consider switching to HTTP service or Azure Foundry
5. Reduce batch size if using custom configurations

#### Port Connection Issues
**Symptoms:**
```
Connection refused
Port already in use
```

**Solutions:**
1. Use `PORT=33334` (not `INTERNAL_PORT`)
2. Check port availability: `netstat -tlnp | grep 33334`
3. Ensure nginx forwards correctly to internal port
4. Check firewall settings

### Expected Log Messages

**Successful Startup:**
```
Weather task ENABLED for this miner (WEATHER_MINER_ENABLED=True)
Weather routes registered (weather task is enabled)
Weather file serving mode: local
RunPod API Key loaded from INFERENCE_SERVICE_API_KEY env var
Miner started successfully on port 33334
```

**Disabled Weather Task:**
```
Weather task DISABLED for this miner. Set WEATHER_MINER_ENABLED=True to enable.
Weather routes NOT registered (weather task is disabled)
```

### Migration from Old Configuration

If updating from older documentation:

```bash
# Backup existing config
cp .env .env.backup

# Fix variable names
sed -i 's/R2_BUCKET_NAME=/R2_BUCKET=/g' .env
sed -i 's/R2_ACCESS_KEY_ID=/R2_ACCESS_KEY=/g' .env
sed -i 's/INTERNAL_PORT=/PORT=/g' .env

# Add new features
echo "WEATHER_FILE_SERVING_MODE=local" >> .env

# Verify changes
grep -E "R2_BUCKET|R2_ACCESS_KEY|PORT=" .env
```

### Getting Help

1. **Check Logs:** Review miner logs for specific error messages
2. **Verify Configuration:** Use verification commands above
3. **Test Components:** Ensure all services (PostgreSQL, inference service) are running
4. **Network Connectivity:** Test external service connectivity
5. **Hardware Check:** Verify GPU availability for local inference

### Performance Monitoring

**System Resources:**
```bash
# GPU usage (for local weather inference)
nvidia-smi

# System memory
free -h

# Disk space (critical for weather task)
df -h

# Process monitoring
htop
```

**Miner-Specific:**
- Monitor forecast directory size growth
- Check GFS cache usage
- Review database size
- Track network bandwidth during GFS downloads

---

## Security Notes

### Credential Management
- **Never commit** `.env` files to version control
- **Rotate keys** regularly, especially R2 and API keys
- **Use strong passwords** for database and JWT secrets
- **Limit R2 permissions** to minimum required (read/write to specific bucket)

### Network Security
- **Configure firewall** to allow only necessary ports
- **Use HTTPS** for all external communications
- **Monitor access logs** for unusual activity
- **Keep systems updated** with security patches

### Best Practices
- **Regular backups** of configuration and database
- **Monitor logs** for security events
- **Test disaster recovery** procedures
- **Document access procedures** for team members

---

## Summary

This guide provides everything needed to set up and run a Gaia miner:

1. **Quick Setup:** Basic configuration for immediate functionality
2. **Weather Task:** Comprehensive opt-in weather forecasting
3. **Variable Names:** Critical exact naming requirements
4. **Task Details:** Complete description of all supported tasks
5. **Troubleshooting:** Solutions for common issues

The key to success is using the **exact variable names** expected by the code and following the configuration templates provided. All documentation is now aligned with the current codebase to prevent configuration failures.

---

## Advanced: Custom Models & Inference Service

### Custom Models

Miners can create custom models for improved performance:

#### File Structure
```bash
gaia/models/custom_models/
├── custom_soil_model.py           # CustomSoilModel class
├── custom_geomagnetic_model.py    # CustomGeomagneticModel class
└── custom_weather_model.py        # CustomWeatherModel class (future)
```

#### Requirements
- **Exact class names**: `CustomSoilModel`, `CustomGeomagneticModel`
- **Required method**: `run_inference()` with specific input/output formats
- **Soil moisture output**: 11x11 arrays for surface/rootzone (0-1 range)
- **Geomagnetic output**: Next-hour DST prediction with UTC timestamp

---

## Weather Inference Service Setup

The Weather Inference Service provides remote GPU-based inference for weather forecasting. This section covers complete setup from Docker building to cloud deployment.

### Overview

The inference service is a FastAPI-based application that:
- Receives weather data from miners via HTTP API
- Runs Aurora model inference on GPU hardware
- Uploads results to R2 storage for miner access
- Supports both RunPod serverless and dedicated deployments

### Architecture

```
Miner → HTTP Request → Inference Service → Aurora Model → R2 Upload → Response
```

**Key Components:**
- **FastAPI Server**: Handles HTTP requests and authentication
- **Aurora Model**: Microsoft's weather prediction model
- **R2 Storage**: Cloudflare R2 for forecast data storage
- **Docker Container**: Portable deployment environment

---

### Prerequisites

#### Required Accounts & Services

1. **Cloudflare R2 Storage**
   ```bash
   # Create R2 bucket for forecast storage
   # Get R2 credentials: Account ID, Access Key, Secret Key
   ```

2. **GPU Infrastructure** (Choose one):
   - **RunPod**: Serverless GPU platform (recommended)
   - **Vast.ai**: GPU rental marketplace
   - **AWS/GCP/Azure**: Cloud GPU instances
   - **Local GPU**: NVIDIA GPU with 24GB+ VRAM

3. **Docker Environment**
   ```bash
   # Install Docker and Docker Compose
   sudo apt update
   sudo apt install docker.io docker-compose
   sudo usermod -aG docker $USER
   ```

#### Hardware Requirements

**Minimum GPU Requirements:**
- **VRAM**: 24GB+ (RTX 3090, RTX 4090, A5000, A6000, H100)
- **CUDA**: 11.8+ or 12.x
- **Memory**: 32GB+ system RAM
- **Storage**: 50GB+ for model and temporary files

---

### Step 1: Configure the Inference Service

#### 1.1 Configuration Files

Navigate to the inference service directory:
```bash
cd gaia/miner/inference_service
```

#### 1.2 Edit Settings (`config/settings.yaml`)

```yaml
model:
  # Aurora model configuration
  model_repo: "/app/models/aurora_local"  # Local path in container
  checkpoint: "aurora-0.25-pretrained.ckpt"
  device: "auto"
  inference_steps: 40
  forecast_step_hours: 6
  resolution: "0.25"

api:
  port: 8000
  host: "0.0.0.0"

logging:
  level: "INFO"
```

#### 1.3 Environment Variables

Create `.env` file for local testing:
```bash
# API Authentication
INFERENCE_SERVICE_API_KEY=your_secure_api_key_here

# R2 Storage Configuration
R2_BUCKET=your-weather-forecasts-bucket
R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
R2_ACCESS_KEY=your_r2_access_key
R2_SECRET_ACCESS_KEY=your_r2_secret_key

# Logging
LOG_LEVEL=INFO

# Optional: Custom model paths
# CUSTOM_MODEL_PATH=/app/local_models/custom_aurora
```

---

### Step 2: Build Docker Image

#### 2.1 Standard Build

```bash
cd gaia/miner/inference_service

# Build the Docker image
docker build -t weather-inference-service:latest .

# Verify build success
docker images | grep weather-inference-service
```

#### 2.2 Build with Custom Aurora Model

If you have a custom Aurora model:

```bash
# 1. Create local model directory
mkdir -p local_models/custom_aurora

# 2. Copy your model files
cp /path/to/your/custom_model.ckpt local_models/custom_aurora/
cp /path/to/your/config.json local_models/custom_aurora/

# 3. Uncomment the COPY line in Dockerfile
sed -i 's/# COPY \.\/local_models/COPY \.\/local_models/' Dockerfile

# 4. Update settings.yaml to point to custom model
sed -i 's|model_repo: "/app/models/aurora_local"|model_repo: "/app/local_models/custom_aurora"|' config/settings.yaml

# 5. Build with custom model
docker build -t weather-inference-service:custom .
```

#### 2.3 Build Arguments

Customize the build process:
```bash
# Use different Aurora model version
docker build \
  --build-arg AURORA_MODEL_REPO="microsoft/aurora" \
  --build-arg AURORA_CHECKPOINT_NAME="aurora-0.25-pretrained.ckpt" \
  -t weather-inference-service:latest .

# Build for specific CUDA version
docker build \
  --build-arg CUDA_VERSION="11.8" \
  -t weather-inference-service:cuda118 .
```

---

### Step 3: Local Testing

#### 3.1 Run Locally with GPU

```bash
# Run with GPU support
docker run --gpus all \
  -p 8000:8000 \
  --env-file .env \
  weather-inference-service:latest

# Run with specific GPU
docker run --gpus '"device=0"' \
  -p 8000:8000 \
  --env-file .env \
  weather-inference-service:latest
```

#### 3.2 Test Health Endpoint

```bash
# Check service health
curl http://localhost:8000/health

# Expected response:
{
  "status": "ok",
  "model_status": "loaded"
}
```

#### 3.3 Test Inference Endpoint

```bash
# Test with sample data (requires valid API key)
curl -X POST http://localhost:8000/run_inference \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secure_api_key_here" \
  -d '{
    "input": {
      "action": "run_inference_from_r2",
      "input_r2_object_key": "test/sample_input.pkl",
      "job_run_uuid": "test-job-123"
    }
  }'
```

---

### Step 4: RunPod Deployment

#### 4.1 Push to Container Registry

**Option A: Docker Hub**
```bash
# Tag and push to Docker Hub
docker tag weather-inference-service:latest yourusername/weather-inference:latest
docker push yourusername/weather-inference:latest
```

**Option B: GitHub Container Registry**
```bash
# Login to GitHub Container Registry
echo $GITHUB_TOKEN | docker login ghcr.io -u yourusername --password-stdin

# Tag and push
docker tag weather-inference-service:latest ghcr.io/yourusername/weather-inference:latest
docker push ghcr.io/yourusername/weather-inference:latest
```

#### 4.2 RunPod Serverless Setup

1. **Create RunPod Account**
   - Sign up at [runpod.io](https://runpod.io)
   - Add payment method and credits

2. **Create Network Volume (Required)**
   ```bash
   # Navigate to Storage → Network Volumes → Create Volume
   
   # Configuration:
   Name: weather-data-volume
   Size: 50GB (minimum required for input/output files)
   Region: [same as your endpoint region]
   ```

3. **Create Serverless Endpoint**
   ```bash
   # Navigate to Serverless → Endpoints → Create Endpoint
   
   # Configuration:
   Name: weather-inference-service
   Container Image: yourusername/weather-inference:latest
   Container Registry Credentials: [if private registry]
   Network Volume: weather-data-volume → /workspace/data
   ```

4. **Configure Environment Variables**
   ```bash
   # In RunPod Endpoint Settings → Environment Variables:
   INFERENCE_SERVICE_API_KEY=your_secure_api_key_here
   R2_BUCKET=your-weather-forecasts-bucket
   R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
   R2_ACCESS_KEY=your_r2_access_key
   R2_SECRET_ACCESS_KEY=your_r2_secret_key
   LOG_LEVEL=INFO
   ```

5. **GPU Configuration**
   ```bash
   # Recommended GPU types:
   - RTX 3090 (24GB VRAM) - Cost effective
   - RTX 4090 (24GB VRAM) - Faster inference
   - A5000 (24GB VRAM) - Professional grade
   - H100 (80GB VRAM) - Highest performance
   
   # Container Configuration:
   Container Disk: 50GB
   Network Volume: 50GB (REQUIRED - for input/output file storage)
   ```

6. **Deploy and Test**
   ```bash
   # Deploy the endpoint
   # Copy the endpoint URL (e.g., https://api.runpod.ai/v2/your-endpoint-id)
   
   # Test deployment
   curl https://api.runpod.ai/v2/your-endpoint-id/health
   ```

#### 4.3 RunPod Pod (Dedicated Instance)

For consistent availability, use a dedicated pod:

```bash
# Create Pod Configuration:
Template: Custom
Container Image: yourusername/weather-inference:latest
GPU: RTX 3090/4090 (24GB VRAM minimum)
Container Disk: 50GB
Volume Disk: 100GB
Ports: 8000 (HTTP)

# Environment Variables: [same as serverless]

# Startup Command:
python -u -m app.main
```

---

### Step 5: Miner Configuration

#### 5.1 Update Miner Environment

```bash
# In your miner .env file:
WEATHER_MINER_ENABLED=True
WEATHER_INFERENCE_TYPE=http_service

# RunPod Serverless Endpoint
WEATHER_INFERENCE_SERVICE_URL="https://api.runpod.ai/v2/your-endpoint-id/run"

# OR RunPod Pod (dedicated instance)
WEATHER_INFERENCE_SERVICE_URL="https://your-pod-id-8000.proxy.runpod.net/run_inference"

# API Key (must match inference service)
INFERENCE_SERVICE_API_KEY=your_secure_api_key_here

# R2 Configuration (for miner)
R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
R2_BUCKET=your-weather-forecasts-bucket
R2_ACCESS_KEY=your_r2_access_key
R2_SECRET_ACCESS_KEY=your_r2_secret_key
```

#### 5.2 Test Miner Integration

```bash
# Restart miner
cd gaia/miner
python miner.py

# Check logs for successful connection
tail -f logs/miner.log | grep -i "weather\|inference\|runpod"

# Expected log messages:
# "Weather task ENABLED for this miner"
# "RunPod API Key loaded from INFERENCE_SERVICE_API_KEY"
# "HTTP Inference Service URL is present"
```

---

### Step 6: Monitoring & Maintenance

#### 6.1 Health Monitoring

**Automated Health Checks:**
```bash
#!/bin/bash
# health_check.sh
ENDPOINT_URL="https://api.runpod.ai/v2/your-endpoint-id"
API_KEY="your_secure_api_key_here"

response=$(curl -s -w "%{http_code}" -o /tmp/health_response.json \
  -H "X-API-Key: $API_KEY" \
  "$ENDPOINT_URL/health")

if [ "$response" = "200" ]; then
  echo "✅ Inference service healthy"
else
  echo "❌ Inference service unhealthy (HTTP $response)"
  cat /tmp/health_response.json
fi
```

**RunPod Monitoring:**
```bash
# Monitor RunPod usage and costs
# Check endpoint logs in RunPod dashboard
# Set up billing alerts
```

#### 6.2 Log Analysis

**Common Log Patterns:**
```bash
# Successful inference
"Successfully processed inference request"
"Uploaded forecast to R2"

# Authentication issues
"Invalid API key"
"Authentication failed"

# Model issues
"Model loading failed"
"CUDA out of memory"

# R2 issues
"R2 upload failed"
"R2 connection timeout"
```

#### 6.3 Performance Optimization

**GPU Optimization:**
```yaml
# In settings.yaml
model:
  device: "cuda"  # Force CUDA instead of auto
  batch_size: 1   # Adjust based on VRAM
  precision: "fp16"  # Use half precision if supported
```

**R2 Optimization:**
```yaml
# Concurrent upload limits
r2:
  max_concurrent_uploads: 10
  upload_timeout_seconds: 300
  retry_attempts: 3
```

---

### Step 7: Troubleshooting

#### 7.1 Common Issues

**Docker Build Failures:**
```bash
# Issue: CUDA compatibility
# Solution: Use NVIDIA base image
FROM nvidia/cuda:11.8-runtime-ubuntu20.04

# Issue: Model download timeout
# Solution: Increase timeout or pre-download
RUN timeout 1800 python -c "from huggingface_hub import snapshot_download; ..."
```

**RunPod Deployment Issues:**
```bash
# Issue: Container won't start
# Check: Environment variables are set correctly
# Check: Container image is accessible
# Check: GPU requirements are met
# Check: Network volume is attached (required for serverless)

# Issue: Network volume not accessible
# Solution: Ensure 50GB network volume is created and attached
# Check: Volume mount path is /workspace/data
# Check: Volume is in same region as endpoint

# Issue: Out of memory
# Solution: Use larger GPU or optimize model
# Check: nvidia-smi output in container logs
```

**API Connection Issues:**
```bash
# Issue: Authentication failed
# Check: API key matches between miner and service
# Check: API key environment variable name

# Issue: Connection timeout
# Check: RunPod endpoint URL is correct
# Check: Network connectivity from miner
```

**R2 Storage Issues:**
```bash
# Issue: R2 upload failed
# Check: R2 credentials and permissions
# Check: Bucket exists and is accessible
# Test: Manual R2 connection with AWS CLI

# Test R2 connection:
aws s3 ls s3://your-bucket --endpoint-url=https://your-account-id.r2.cloudflarestorage.com
```

#### 7.2 Debug Commands

**Container Debugging:**
```bash
# Run container interactively
docker run -it --gpus all --entrypoint /bin/bash weather-inference-service:latest

# Check GPU availability
nvidia-smi

# Test model loading
python -c "import torch; print(torch.cuda.is_available())"

# Check environment variables
env | grep -E "R2_|INFERENCE_"
```

**Network Testing:**
```bash
# Test endpoint connectivity
curl -v https://api.runpod.ai/v2/your-endpoint-id/health

# Test with authentication
curl -H "X-API-Key: your-key" https://api.runpod.ai/v2/your-endpoint-id/health

# Check DNS resolution
nslookup api.runpod.ai
```

#### 7.3 Performance Monitoring

**GPU Monitoring:**
```bash
# Monitor GPU usage during inference
watch -n 1 nvidia-smi

# Check memory usage
nvidia-smi --query-gpu=memory.used,memory.total --format=csv
```

**Cost Monitoring:**
```bash
# RunPod cost tracking
# Monitor usage in RunPod dashboard
# Set up billing alerts
# Track inference requests per hour
```

---

### Step 8: Advanced Configuration

#### 8.1 Custom Aurora Models

**Preparing Custom Models:**
```bash
# 1. Train or fine-tune Aurora model
# 2. Save checkpoint in compatible format
# 3. Create model configuration

# Directory structure:
local_models/custom_aurora/
├── custom_model.ckpt
├── config.json
└── metadata.json
```

**Docker Integration:**
```dockerfile
# Add to Dockerfile
COPY ./local_models/custom_aurora /app/local_models/custom_aurora

# Update settings.yaml
model:
  model_repo: "/app/local_models/custom_aurora"
  checkpoint: "custom_model.ckpt"
```

#### 8.2 Multi-GPU Setup

**For Multiple GPUs:**
```yaml
# settings.yaml
model:
  device: "cuda:0"  # Specify GPU
  multi_gpu: true
  gpu_ids: [0, 1]   # Use multiple GPUs
```

**Docker Configuration:**
```bash
# Run with multiple GPUs
docker run --gpus '"device=0,1"' \
  -p 8000:8000 \
  weather-inference-service:latest
```

#### 8.3 Scaling & Load Balancing

**Multiple Endpoints:**
```bash
# Deploy multiple RunPod endpoints
# Use load balancer or round-robin in miner
WEATHER_INFERENCE_SERVICE_URL="https://api.runpod.ai/v2/endpoint-1/run,https://api.runpod.ai/v2/endpoint-2/run"
```

**Auto-scaling:**
```bash
# Configure RunPod auto-scaling
# Set min/max workers
# Configure scale-up/down policies
```

---

### Step 9: Security Best Practices

#### 9.1 API Key Management

```bash
# Generate secure API keys
python -c "import secrets; print(secrets.token_urlsafe(32))"

# Rotate keys regularly
# Use different keys for different environments
# Store keys securely (not in code)
```

#### 9.2 Network Security

```bash
# Restrict access to inference service
# Use VPN or private networks when possible
# Monitor access logs
# Implement rate limiting
```

#### 9.3 Container Security

```bash
# Use minimal base images
# Scan for vulnerabilities
docker scan weather-inference-service:latest

# Run as non-root user
USER 1000:1000

# Limit container capabilities
--cap-drop=ALL --cap-add=SYS_NICE
```

---

### Step 10: Cost Optimization

#### 10.1 RunPod Cost Management

**Serverless vs Dedicated:**
```bash
# Serverless: Pay per inference
# - Good for: Variable workload
# - Cost: $0.50-2.00 per hour of GPU time

# Dedicated Pod: Fixed hourly rate
# - Good for: Consistent workload
# - Cost: $0.30-1.50 per hour continuous
```

**GPU Selection:**
```bash
# Cost-effective options:
RTX 3090: ~$0.30/hour (24GB VRAM)
RTX 4090: ~$0.50/hour (24GB VRAM, faster)
A5000: ~$0.70/hour (24GB VRAM, professional)

# High-performance options:
A6000: ~$1.00/hour (48GB VRAM)
H100: ~$2.00/hour (80GB VRAM, fastest)
```

#### 10.2 Storage Optimization

**R2 Storage Costs:**
```bash
# Storage: $0.015/GB/month
# Requests: $0.36/million requests
# Egress: Free (major advantage over S3)

# Optimization strategies:
# - Compress forecast data
# - Implement data lifecycle policies
# - Clean up old forecasts automatically
```

#### 10.3 Monitoring & Alerts

```bash
# Set up cost alerts
# Monitor inference frequency
# Track storage usage
# Optimize based on usage patterns
```

---

This comprehensive guide covers everything needed to set up a production-ready weather inference service, from initial Docker build to advanced scaling and optimization strategies. 