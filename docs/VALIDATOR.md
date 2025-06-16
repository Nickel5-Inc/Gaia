# *Validator Information*

---


## **API's required**


### NASA EarthData
1. Create an account at https://urs.earthdata.nasa.gov/
2. Accept the necessary EULAs for the following collections:
    - GESDISC Test Data Archive 
    - OB.DAAC Data Access 
    - Sentinel EULA

3. Generate an API token and save it in the .env file (details below)

### Climate Data Store
1. create an account at https://cds.climate.copernicus.eu/ 

2. accept the relevant licences 
    -  Licence to use Copernicus Products 
    -  Creative Commons Attribution 4.0 International Public Licence 

3. Navigate to Your Profile and copy your API token 

4. create a file called .cdsapirc at your root directory 
#### /root/.cdsapirc
```bash
url: https://cds.climate.copernicus.eu/api
key: <your_cds_api_key>
```
---

#### Create .env file for validator with the following components:
```bash
# Gaia Validator Configuration Template
# Rename this file to .env and fill in your specific values.

# --- Database Configuration ---
DB_USER=postgres
DB_PASSWORD=<YOUR_DB_PASSWORD> # Replace with your actual database password - default is configured to 'postgres'
DB_HOST=localhost
DB_PORT=5432
DB_NAME=validator_db
DB_TARGET=validator
DB_CONNECTION_TYPE=socket
ALEMBIC_AUTO_UPGRADE=True

# --- Application Environment ---
# Set to 'prod' for production or 'dev' for development (enables more verbose logging)
ENV=prod

# --- Subtensor/Blockchain Configuration ---
WALLET_NAME=default       # Your Bittensor wallet name
HOTKEY_NAME=default       # Your validator's hotkey name
NETUID=237                # Network UID (e.g., 237 for testnet, 57 for mainnet)
SUBTENSOR_NETWORK=test    # Bittensor network ('test' or 'finney')
SUBTENSOR_ADDRESS=wss://test.finney.opentensor.ai:443/ # Subtensor chain endpoint

# --- NASA Earthdata Credentials (Sensitive - DO NOT COMMIT ACTUAL VALUES TO PUBLIC REPOS) ---
# These are required for downloading data from NASA.
# Create an account at https://urs.earthdata.nasa.gov/
# Accept EULAs for: GESDISC Test Data Archive, OB.DAAC Data Access, Sentinel EULA
EARTHDATA_USERNAME=<YOUR_EARTHDATA_USERNAME>
EARTHDATA_PASSWORD=<YOUR_EARTHDATA_PASSWORD>
EARTHDATA_API_KEY=<YOUR_EARTHDATA_API_KEY>  # This refers to your Earthdata login credentials used by the application.

# --- Database Synchronization System (Optional, highly recommended) ---
# Enable database synchronization (uses pgBackRest + R2)
DB_SYNC_ENABLED=true

# Set to "true" ONLY if this validator is the PRIMARY/SOURCE (typically one per network)
# For replica validators, set to "false" or omit
IS_SOURCE_VALIDATOR_FOR_DB_SYNC=false

# --- R2 Storage Configuration (required for DB sync) ---
# Cloudflare R2 bucket and credentials
PGBACKREST_R2_BUCKET=your-r2-bucket-name
PGBACKREST_R2_ENDPOINT=https://ACCOUNT_ID.r2.cloudflarestorage.com
PGBACKREST_R2_ACCESS_KEY_ID=your-r2-access-key
PGBACKREST_R2_SECRET_ACCESS_KEY=your-r2-secret-key

# --- pgBackRest Configuration (optional - defaults provided) ---
PGBACKREST_STANZA_NAME=gaia
PGBACKREST_R2_REGION=auto
```

#### Run the validator
```bash
pm2 start --name validator --instances 1 python -- gaia/validator/validator.py 
```

---

## **Database Synchronization System (Optional, highly recommended)**

This system allows for near real-time replication of the validator database from a designated source validator to other replica validators using pgBackRest with Cloudflare R2 storage. This provides efficient incremental backups and point-in-time recovery capabilities.

### Overview

1.  **Source Validator**: One validator is designated as the "source of truth". It runs automated pgBackRest backups to R2 storage.
2.  **pgBackRest + R2**: Modern backup solution with incremental backups, compression, and encryption.
3.  **Replica Validators**: Other validators can restore from R2 backups for rapid synchronization.
4.  **AutoSyncManager**: Streamlined management with automated setup and progress tracking.

This process provides efficient database synchronization with incremental backups and minimal storage overhead.

### Configuration (Environment Variables)

**Core DB Sync Settings:**
```bash
# Enable/disable database synchronization
DB_SYNC_ENABLED=true

# Set the validator role (only ONE primary validator should be true)
IS_SOURCE_VALIDATOR_FOR_DB_SYNC=true  # For primary/source validator
IS_SOURCE_VALIDATOR_FOR_DB_SYNC=false # For replica validators
```

**pgBackRest + R2 Configuration:**
```bash
# R2 Storage Settings (required)
PGBACKREST_R2_BUCKET=your-r2-bucket-name
PGBACKREST_R2_ENDPOINT=https://ACCOUNT_ID.r2.cloudflarestorage.com
PGBACKREST_R2_ACCESS_KEY_ID=your-r2-access-key
PGBACKREST_R2_SECRET_ACCESS_KEY=your-r2-secret-key
PGBACKREST_R2_REGION=auto

# pgBackRest Settings (optional - defaults provided)
PGBACKREST_STANZA_NAME=gaia
PGBACKREST_PGDATA=/var/lib/postgresql/data
PGBACKREST_PGPORT=5432
PGBACKREST_PGUSER=postgres
```

### Setup Instructions

**Option 1: Automated Setup (Recommended)**
```bash
# For primary validator (source)
python gaia/validator/sync/setup_auto_sync.py --primary

# For replica validator
python gaia/validator/sync/setup_auto_sync.py --replica
```

**Option 2: Manual Setup**
```bash
# Primary validator
sudo bash gaia/validator/sync/pgbackrest/setup-primary.sh

# Replica validator  
sudo bash gaia/validator/sync/pgbackrest/setup-replica.sh
```

### Required Dependencies

*   **pgBackRest**: Modern PostgreSQL backup solution
*   **R2 Access**: Cloudflare R2 storage account and bucket

### Monitoring and Management

**Check backup status:**
```bash
sudo -u postgres pgbackrest --stanza=gaia info
```

**Manual operations:**
```bash
# Force backup (primary)
sudo -u postgres pgbackrest --stanza=gaia backup

# Manual restore (replicas)
sudo systemctl stop postgresql
sudo -u postgres pgbackrest --stanza=gaia restore
sudo systemctl start postgresql
```

**Progress tracking** is available through the AutoSyncManager with real-time backup progress monitoring.

---

## Database Migration

The system uses separate Alembic configurations for miner and validator databases:

### Validator Database Migration
```bash
# Check current schema version
DB_CONNECTION_TYPE=socket alembic -c alembic_validator.ini current

# Upgrade to latest schema
DB_CONNECTION_TYPE=socket alembic -c alembic_validator.ini upgrade head

# View migration history
DB_CONNECTION_TYPE=socket alembic -c alembic_validator.ini history
```

**Core validator tables**: `node_table`, `score_table`, `weather_forecast_runs`, `weather_miner_responses`

**Migration Safety**: All migrations preserve existing data and validate required tables exist.

---

## Custom Models (Advanced)

Validators can benefit from miners using custom models for better performance:

### Encouraging Custom Models
- Miners with custom models typically provide better predictions
- Custom models can be task-specific (geomagnetic, soil moisture, weather)
- Improved accuracy leads to better rewards for miners

### Model Requirements
- **Soil Moisture**: Must output 11x11 arrays for surface/rootzone moisture (0-1 range)
- **Geomagnetic**: Must predict next-hour DST index with UTC timestamp
- **Weather**: Must generate 40-step forecasts in Zarr format

Custom model files must follow naming conventions and implement specific methods (`run_inference()`).

---