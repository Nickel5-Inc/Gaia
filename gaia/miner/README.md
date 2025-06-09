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

### Inference Service Setup (Docker)

For HTTP inference service deployment:

#### Configuration (`config/settings.yaml`)
```yaml
model:
  model_repo: "microsoft/aurora"  # or "/app/local_models/custom_aurora"
  checkpoint: "aurora-0.25-pretrained.ckpt"
  device: "auto"
  inference_steps: 40
  forecast_step_hours: 6

api:
  host: "0.0.0.0"
  port: 8000

logging:
  level: "INFO"
```

#### Build & Run
```bash
# Navigate to inference service directory
cd gaia/miner/inference_service/

# Build Docker image
docker build -t weather-inference-service .

# Run with GPU support
docker run --gpus all -p 8000:8000 weather-inference-service

# Run with custom config
docker run -p 8000:8000 \
  -v /path/to/custom_settings.yaml:/app/config/settings.yaml \
  weather-inference-service
```

#### API Endpoints
- **Health Check**: `GET /health`
- **Inference**: `POST /run_inference`
  - Input: `{"serialized_aurora_batch": "base64_encoded_data"}`
  - Output: Forecast data with job ID and status

#### Environment Variables
```bash
INFERENCE_CONFIG_PATH=config/settings.yaml
LOG_LEVEL=INFO
INFERENCE_SERVICE_API_KEY=<your-api-key>

# R2 Configuration (for R2 operations)
WORKER_R2_BUCKET_NAME=<bucket-name>
WORKER_R2_ENDPOINT_URL=<r2-endpoint>
WORKER_R2_ACCESS_KEY_ID=<access-key>
WORKER_R2_SECRET_ACCESS_KEY=<secret-key>
```

---

## Database Migration (Miners)

### Miner Database Migration
```bash
# Check current schema version
DB_CONNECTION_TYPE=socket alembic -c alembic_miner.ini current

# Upgrade to latest schema
DB_CONNECTION_TYPE=socket alembic -c alembic_miner.ini upgrade head

# View migration history
DB_CONNECTION_TYPE=socket alembic -c alembic_miner.ini history
```

**Core miner table**: `weather_miner_jobs` with complete column set

**Migration Features**:
- Handles divergent database states gracefully
- Adds missing columns with appropriate defaults
- Preserves existing data while ensuring schema compliance
- Removes forbidden validator-specific tables 