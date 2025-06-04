## GeoMagnetic Task

### Why It Matters
Geomagnetic disturbances, driven by solar activity, can have significant impacts on Earth's technology systems, including:
- GPS and communication systems.
- Power grids and satellite operations.
- Navigation and aviation industries.

Accurate forecasting of the DST (Disturbance Storm Time) index allows for proactive mitigation strategies, protecting critical infrastructure and reducing risks associated with geomagnetic storms.

---

### What Data It Uses
The Geomagnetic Task uses DST index data, a time-series measure reflecting the intensity of geomagnetic disturbances:
1. **Hourly DST Index**:
   - A cleaned DataFrame containing recent hourly DST values, sent by the Validator.
   - Includes `timestamp` and `value` columns for the current month.
2. **Historical DST Data** (optional):
   - Miners can gather additional historical data to improve model performance.

Validators preprocess the DST data to ensure consistency and provide it to Miners for prediction.

---

### How to Run the Model

#### Step 1: Prepare Data
- The Validator sends a cleaned DataFrame (`data`) with columns:
  - `timestamp`: The time of the observation (in UTC).
  - `value`: The geomagnetic disturbance intensity.

#### Step 2: Generate Model Input
- Use **`process_geomag_data.py`** to retrieve and preprocess historical DST data (if needed).
- Combine this data with the Validator-provided DataFrame to create a prediction-ready input.

#### Step 3: Run the Geomagnetic Model
- Use **`GeomagneticPreprocessing`** to generate predictions:
  ```python
  from tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import GeomagneticPreprocessing

  preprocessing = GeomagneticPreprocessing()
  prediction_result = preprocessing.predict_next_hour(data, model=geomag_model)


### Summary of What the Miner Should Return to the Validator

The miner must return the following to the Validator for evaluation:

- **Predicted DST Value**: The miner's predicted DST index for the next hour.
- **Timestamp (UTC)**: The UTC timestamp of the last observation used in the prediction, ensuring it's standardized.

---

## Soil Moisture Task

### Why It Matters
Soil moisture is a critical factor in understanding environmental processes, agriculture, and weather forecasting. 
Accurate soil moisture data helps: 
- Optimize agricultural water use.
- Predict droughts and floods.
- Enhance weather and climate models.
- Support ecological research.

### What Data It Uses
The soil moisture model integrates various datasets to provide comprehensive insights:
1. **Sentinel-2 Imagery**:
   - High-resolution satellite data for monitoring vegetation and land cover.
2. **IFS Forecasting Data**:
   - Supplies weather forecasts, including precipitation and temperature, relevant for soil moisture modeling.
3. **SMAP Data**:
   - Global soil moisture data for scoring and analysis.
4. **SRTM Data**:
   - Elevation data from the Shuttle Radar Topography Mission to incorporate terrain information.
5. **NDVI (Normalized Difference Vegetation Index)**:
   - Tracks vegetation health and coverage, crucial for understanding land surface conditions.

**These datasets are aligned based on the Sentinel-2 region boundaries to ensure spatial consistency and precision.**

### How to Run the Model

#### Step 1: Prepare Data
- Use **`region_selection.py`** to select random regions for analysis, avoiding urban areas and large water bodies.
- Retrieve necessary datasets using **`soil_apis.py`**:
  - Sentinel-2 imagery.
  - IFS weather forecasts.
  - SRTM elevation tiles.
  - NDVI vegetation data.

#### Step 2: Generate Model Input
- Compile the data into a `.tiff` file with the following band order:
  - `[Sentinel-2, IFS, SRTM, NDVI]`.
- Store the corresponding bounds and CRS (Coordinate Reference System) for later validation.

#### Step 3: Run the Soil Moisture Model
- Use **`soil_model.py`** to process the `.tiff` file and generate soil moisture predictions.

#### Step 4: Post-Processing
- Run **`Inference_classes.py`** to format predictions and prepare them for validation.

#### Step 5: Validation
- Submit the predictions, along with region bounds and CRS, to the validator for comparison with ground truth data.


---

#### Create dev.env file for miner with the following components:
```bash
# --- Gaia Miner Environment Variables Template ---
# This file serves as a template. Rename to .env (or your specific config name) 
# in your miner's configuration directory and fill in your actual values.

# --- Miner Database Configuration ---
# These settings are for the PostgreSQL database shared by tasks running on the miner,
# including the WeatherTask for its job tracking.

DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=miner_db 
DB_TARGET=miner
DB_CONNECTION_TYPE=socket
ALEMBIC_AUTO_UPGRADE=True




# --- General Miner & Bittensor Configuration ---
WALLET_NAME=gt0
HOTKEY_NAME=default
SUBTENSOR_NETWORK=test # Example: "finney" (for mainnet), "test" (for testnet), or "local" (for local)
NETUID=237 # NetUID for the subnet you are connecting to



# Logging level for the miner
# Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
MINER_LOGGING_LEVEL=INFO

# Environment type
# Options: "dev", "prod", "test"
ENV=prod

# Secret key for JWT if the miner uses its own API with authentication
MINER_JWT_SECRET_KEY=xxx # Please change this to a strong, random secret in your actual .env file. use `python ./gaia/miner/utils/generate_jwt_secret.py` to auto generate one and save to .env

# Minimum stake threshold for validators to interact with this miner (specific to some blacklist logic). Denoted in Alpha Tokens.
# testnet: 0
# finney: 10000 
MIN_STAKE_THRESHOLD=0

# --- Bittensor Axon Configuration (if miner is on Bittensor network) ---
# Public Port (this is the port that gets posted to the chain)
PUBLIC_PORT=33333
INTERNAL_PORT=33334 # This is the port that the miner application listens on. Outside requests are forwarded to this port from nginx proxy.
EXTERNAL_IP="your_external_ip_address" 



# --- WeatherTask Configuration (for Miner) ---
# This section is particularly relevant if this miner is participating in the WeatherTask.

#Enable task
WEATHER_MINER_ENABLED=False #default is false, as task is "opt-in"

# Defines how the miner performs inference for the WeatherTask.
# Options:
#   "local"         - Runs the Aurora model directly within the WeatherTask process (requires local GPU/CPU resources & model files).
#   "azure_foundry" - Uses Azure AI Foundry for inference (requires Azure credentials and setup - see WeatherTask docs).
#   "http_service"  - Delegates inference to the containerized HTTP-based inference service (in gaia/miner/inference_service).
WEATHER_INFERENCE_TYPE=http_service 

# Directory where the miner caches GFS analysis data. This is used for:
#   1. Computing the input data hash sent to the validator.
#   2. Preparing the Aurora Batch if WEATHER_INFERENCE_TYPE="local".
#   3. Preparing the Aurora Batch before sending to the service if WEATHER_INFERENCE_TYPE="http_service".
# Ensure this path is writable by the miner process.
MINER_GFS_ANALYSIS_CACHE_DIR="./gfs_analysis_cache_miner" # Added variable

# Directory where the miner temporarily stores its final forecast outputs (e.g., Zarr files)
# after receiving results from local inference or the remote inference service.
MINER_FORECAST_DIR=/root/Gaia/miner_forecasts_background

# --- Configuration for WEATHER_INFERENCE_TYPE="local" ---
# (These are typically handled by the Aurora SDK defaults or internal WeatherTask logic if not set,
# but can be overridden. Refer to WeatherInferenceRunner in WeatherTask for details.)
# WEATHER_MODEL_REPO="microsoft/aurora" # HuggingFace repo or local path for the Aurora model
# WEATHER_MODEL_CHECKPOINT="aurora-0.25-pretrained.ckpt" # Checkpoint file name
# WEATHER_DEVICE="auto" # "cuda", "cpu", or "auto" for local inference

# --- Configuration for WEATHER_INFERENCE_TYPE="azure_foundry" ---
# (These are environment variables Azure AI Foundry integration expects)

# FOUNDRY_ENDPOINT_URL="your_foundry_endpoint_url"
# FOUNDRY_ACCESS_TOKEN="your_foundry_access_token"
# BLOB_URL_WITH_RW_SAS="your_azure_blob_storage_sas_url_for_data_exchange"

# --- Configuration for WEATHER_INFERENCE_TYPE="http_service" ---
# URL of the standalone weather inference service.

WEATHER_INFERENCE_SERVICE_URL="http://localhost:8000/run_inference"
# Example if inference service is Dockerized and miner is on host: "http://localhost:8000/run_inference"
# Example if both miner and service are Dockerized on same Docker network: "http://weather-inference-service:8000/run_inference"
# Example if the container is hosted on a Serverless/On-Demand endpoint : "http://runpod.
# (where 'weather-inference-service' is the Docker service name)

# API Key to authenticate the miner with the remote inference service.
# This key MUST match the API key configured on the inference service itself
# (e.g., via its INFERENCE_SERVICE_API_KEY environment variable if the service implements API key auth).
# If the inference service does not have an API key configured or uses a different auth method,
# this variable might be unused or need to be adapted.
MINER_API_KEY_FOR_INFRA_SERVICE="" # Example: "your_secret_api_key_to_access_inference_service"

# Additional generic credentials for accessing the inference service, if required by its hosting platform.
# These might be used for basic authentication or other custom authentication schemes.
# How these are used depends on the inference service's specific security setup.
CREDENTIAL_NAME=""  # e.g., a specific credential identifier for a cloud provider
USERNAME=""         # Username for basic auth or platform-specific auth
PASSWORD=""         # Password for basic auth or platform-specific auth






# Note: The WeatherTask's database manager for the miner will be configured to use PostgreSQL
# and these shared credentials and database name.

```

## Remote Inference Service for Computationally Intensive Tasks (e.g., Weather Task)

Some tasks, like the Weather Task, require significant computational resources for model inference and are often run on dedicated remote services (e.g., RunPod serverless workers).

**Miner Configuration for Remote Inference:**

Miners running these tasks will typically need to:
1.  **Deploy or Utilize a Provided Inference Service:** This service handles the actual model execution.
    *   For detailed instructions on setting up, configuring (including R2 environment variables for data transfer), and deploying the reference Weather Inference Service, see: [`gaia/miner/inference_service/README.md`](./inference_service/README.md).

2.  **Configure their Local Miner Environment:** The local miner application (the one running `miner.py`) needs to know how to communicate with this remote inference service. This includes:
    *   **Inference Service URL:** The endpoint of your deployed inference service.
    *   **API Keys:** If the inference service is protected by an API key.
    *   **R2 Credentials (for the Miner):** The local miner also needs its own R2 credentials if it's directly pre-uploading any data or post-downloading results, separate from the R2 credentials used *by the inference service itself*.

**Environment Variables for the Miner Process (`miner.py`):**

While the inference service has its own set of environment variables (detailed in its README), your main miner process (`miner.py`) will also need certain environment variables to interact with it and the broader system. These are typically set in your primary `.env` file for the miner (similar to the `dev.env` example above).

*   `WEATHER_INFERENCE_TYPE`: Set to `http_service` to use a remote inference endpoint for the WeatherTask.
*   `WEATHER_INFERENCE_SERVICE_URL`: The URL of your deployed Weather Inference Service.
    *   Example: `WEATHER_INFERENCE_SERVICE_URL=https://your-runpod-endpoint.runpod.net`
*   `WEATHER_RUNPOD_API_KEY`: (If applicable) An API key specifically for authenticating the miner's requests *to* the RunPod service endpoint if it's protected directly by RunPod's API gateway or a similar mechanism. This might be different from an API key used *within* the inference service for `runpodctl`.

*   **R2 Credentials for the Miner Application (if directly interacting with R2):**
    *   `R2_BUCKET`: The R2 bucket name the miner uses for its operations.
    *   `R2_ENDPOINT_URL`: The R2 endpoint URL for the miner.
    *   `R2_ACCESS_KEY`: R2 Access Key ID for the miner.
    *   `R2_SECRET_ACCESS_KEY`: R2 Secret Access Key for the miner.

**Validator-Provided Configurations:**

In some setups, particularly managed environments, the validator or network operators might provide templates or pre-configured settings for the inference service environment. Refer to documentation provided by your network operators or the `validator_template.env` for insights into how these might be structured. The variables like `MINER_INF_SVC_R2_BUCKET_NAME` in `validator_template.env` are placeholders for such scenarios.


#### Run the miner
```bash
cd gaia
cd miner
python miner.py
```