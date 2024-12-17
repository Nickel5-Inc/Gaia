# Gaia Validator Guide

## Overview
Gaia is a platform for research and development of geospatial machine learning models. Validators play a crucial role by connecting to various APIs to provide miners with the data they need to run their models. For the complete platform vision, visit our [whitepaper](https://www.gaiaresearch.ai/whitepaper).

## System Requirements
- CPU: 8-core processor
- RAM: 16 GB
- Network: Reliable connection with at least 80 Mbps upload and download speeds. 1Tb monthly bandwidth.

## Initial Setup

### 1. Clone the Repository
```bash
git clone https://github.com/Nickel5-Inc/Gaia.git
cd Gaia
```

### 2. Run the Setup Script
```bash
python ./scripts/setup.py
```
This will:
- Create a virtual environment (.gaia) above the gaia directory
- Install all dependencies

Activate the environment:
```bash
source ../.gaia/bin/activate
```

### 3. Install Fiber
```bash
pip install "git+https://github.com/rayonlabs/fiber.git@1.0.0#egg=fiber[full]"
```

### 4. Required API Setup

#### NASA EarthData
1. Create an account at https://urs.earthdata.nasa.gov/
2. Accept the necessary EULAs for the following collections:
    - GESDISC Test Data Archive 
    - OB.DAAC Data Access 
    - Sentinel EULA
3. Generate an API token for the .env file

### 5. Environment Configuration
Create a .env file with the following components:
```bash
# Database Configuration
DB_USER=<YOUR_DB_USER>          # postgres is default from setup script
DB_PASSWORD=<YOUR_DB_PASSWORD>   # postgres is default from setup script
DB_HOST=<YOUR_DB_HOST>          # localhost is default from setup script
DB_PORT=<YOUR_DB_PORT>          # 5432

# Environment Configuration
ENV=prod                        # Use 'dev' for debug logs

# Bittensor Configuration
WALLET_NAME=<YOUR_WALLET_NAME>
HOTKEY_NAME=<YOUR_WALLET_HOTKEY>
NETUID=<NETUID>                # 57 for mainnet, 237 for testnet
SUBTENSOR_NETWORK=<NETWORK>     # finney or test
SUBTENSOR_ADDRESS=<SUBTENSOR_ADDRESS>  # Chain endpoint
                               # Use wss://test.finney.opentensor.ai:443/ for testnet
                               # Use wss://entrypoint-finney.opentensor.ai:443/ for mainnet

# NASA EarthData Configuration
EARTHDATA_USERNAME=<YOUR_EARTHDATA_USERNAME>
EARTHDATA_PASSWORD=<YOUR_EARTHDATA_PASSWORD>
EARTHDATA_API_KEY=<YOUR_EARTHDATA_API_KEY>

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=your_redis_password
REDIS_MAX_CONNECTIONS=10
```

### 6. Proxy Server Setup
Gaia uses a Proxy server to handle validator connections:

```bash
./setup_proxy_server.sh --ip <YOUR IP> --port <PORT> --forwarding_port <PORT_FOR_VALIDATOR> --server_name <n>
```
Note: 
- The proxy must run as a background process
- The port argument is your external facing port
- The forwarding_port is the INTERNAL port for validator communication
- Server name is optional

### 7. Register on Subnet and Post IP
Register:
```bash
btcli subnets register --subtensor.network <NETWORK> --netuid <NETUID> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY>
```

Post IP:
```bash
fiber-post-ip --netuid <NETUID> --external_ip <YOUR_IP> --external_port <YOUR_PORT> --subtensor.network <NETWORK> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY>
```

## Running the Validator

### Install Dependencies
```bash
# Install Python dependencies
pip install -r requirements.txt

# Install PM2 if not already installed
npm install pm2 -g
```

### Start the Validator
```bash
# Start with PM2 (recommended for production)
pm2 start --name validator --instances 1 python -- gaia/validator/deploy.py

# Or run directly for development
python gaia/validator/deploy.py
```

The deploy script will:
1. Check environment variables
2. Start Prefect server
3. Apply Prefect deployment configuration
4. Start Prefect worker
5. Start the validator
6. Monitor all processes and handle restarts

### Monitoring
```bash
# View logs
pm2 logs validator

# Monitor processes
pm2 monit
```

### Dashboard Access (Remote Server)
For remote servers, set up SSH port forwarding:

```bash
# Forward both Prefect and Ray dashboards
ssh -L 4200:localhost:4200 -L 8265:localhost:8265 user@your-server-ip
```

Add to ~/.ssh/config for persistent access:
```
Host gaia-validator
    HostName your-server-ip
    User your-username
    LocalForward 4200 localhost:4200
    LocalForward 8265 localhost:8265
```

Access dashboards:
- Prefect Dashboard: http://localhost:4200
- Ray Dashboard: http://localhost:8265

### Auto-updates
The validator includes an auto-update system that:
- Checks for updates every 2 minutes
- Installs new dependencies if needed
- Safely restarts all components
- Resets scoring system if required

No manual intervention needed for updates.

## Data Acknowledgements

### ECMWF Open Data
Copyright © [2024] European Centre for Medium-Range Weather Forecasts (ECMWF)
[ECMWF Open Data](https://www.ecmwf.int/en/forecasts/datasets/open-data)

### HLS Sentinel-2
[HLSS30](https://lpdaac.usgs.gov/products/hlss30v002/)
Masek, J., et al. (2021). HLS Sentinel-2 Multi-spectral Instrument Surface Reflectance Daily Global 30m v2.0

### Soil Moisture Active Passive
[SPL4SMGP](https://nsidc.org/data/spl4smgp/versions/7)
Reichle, R., et al. (2022). SMAP L4 Global 3-hourly 9 km EASE-Grid Surface and Root Zone Soil Moisture

### NASA Shuttle Radar Topography Mission
[SRTMl1v003](https://lpdaac.usgs.gov/products/srtmgl1v003/)
NASA JPL (2013). NASA Shuttle Radar Topography Mission Global 1 arc second

### World Data Center for Geomagnetism, Kyoto
[Dst Open Data](https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/index.html)
World Data Center for Geomagnetism, Kyoto. Kyoto University.
```


