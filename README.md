![Project Logo](docs/logo-full.png)

# 🌍 Gaia - Decentralized Weather Forecasting Network

<div align="center">

[![Website](https://img.shields.io/badge/Website-gaiaresearch.ai-blue?style=for-the-badge)](https://www.gaiaresearch.ai/)
[![Beta](https://img.shields.io/badge/Version-Beta%202.0.0-orange?style=for-the-badge)](https://github.com/Nickel5-Inc/Gaia)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

</div>

## 🚀 What is Gaia?

Gaia is a **decentralized weather forecasting network** built on Bittensor that leverages the Microsoft Aurora foundation model to create accurate, global weather predictions. 

**Current Focus**: Weather forecasting using advanced AI models to predict atmospheric conditions with comprehensive scoring across multiple variables and pressure levels.

> 📖 **Learn More**: Read our [whitepaper](docs/whitepaper.md) for the complete vision and technical details.

## 🎯 Quick Start

### 1. Clone & Setup
```bash
git clone https://github.com/Nickel5-Inc/Gaia.git
cd Gaia
python ./scripts/setup.py
source ../.gaia/bin/activate
```

### 2. Choose Your Role

<table>
<tr>
<td width="50%">

## 🔨 **Miner** 
*Generate weather forecasts*

- Develop weather forecasting models using Microsoft Aurora
- Receive GFS data from validators
- Generate detailed 40-step weather forecasts
- Earn rewards for accurate predictions

**[📋 Mining Guide →](docs/MINER.md)**

</td>
<td width="50%">

## ✅ **Validator**
*Coordinate the network*

- Provide GFS data to miners
- Score forecasts against ERA5 ground truth
- Manage network operations
- Connect to meteorological APIs

**[📋 Validating Guide →](docs/VALIDATOR.md)**

</td>
</tr>
</table>

### 3. System Requirements

| Role | CPU | RAM | Network |
|------|-----|-----|---------|
| **Miner** | 6-core | 8 GB | 80 Mbps, 1TB/month |
| **Validator** | 8-core | 16 GB | 80 Mbps, 1TB/month |

---

## 📋 Table of Contents

- [🚀 What is Gaia?](#-what-is-gaia)
- [🎯 Quick Start](#-quick-start)
- [⚙️ Detailed Installation](#️-detailed-installation)
- [🔧 Network Setup](#-network-setup)
- [🗄️ Database Configuration](#️-database-configuration)
- [🔍 Troubleshooting](#-troubleshooting)
- [📚 Documentation](#-documentation)
- [📄 Data Sources & Acknowledgments](#-data-sources--acknowledgments)

---

## ⚙️ Detailed Installation

Gaia is built on [Fiber](https://github.com/rayonlabs/fiber) - special thanks to namoray and the Rayon labs team.

### Prerequisites
- Python 3.8+
- Git
- PostgreSQL (for validators)

### Step 1: Environment Setup
```bash
# Clone repository
git clone https://github.com/Nickel5-Inc/Gaia.git
cd Gaia

# Run automated setup (creates virtual environment & installs dependencies)
python ./scripts/setup.py

# Activate virtual environment
source ../.gaia/bin/activate
```

### Step 2: Install Fiber
```bash
pip install "git+https://github.com/rayonlabs/fiber.git@production#egg=fiber[full]"
```

### Step 3: Build Repository Modules
```bash
pip install -e .
```

---

## 🔧 Network Setup

### Register on Subnet
```bash
btcli subnets register --subtensor.network <NETWORK> --netuid <NETUID> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY>
```

### Setup Proxy Server
Gaia uses a proxy server to handle connections between miners and validators.

```bash
./setup_proxy_server.sh --ip <YOUR_IP> --port <PORT> --forwarding_port <PORT_FOR_MINER_OR_VALIDATOR> --server_name <NAME>
```

**Important Notes:**
- The proxy server must be running for proper communication
- `--port` is your external facing port
- `--forwarding_port` is the internal port for miner/validator communication
- `--server_name` is optional

### Post IP to Chain
```bash
fiber-post-ip --netuid <NETUID> --external_ip <YOUR_IP> --external_port <YOUR_PORT> --subtensor.network <NETWORK> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY>
```

**Important:**
- Only needed once per key
- Use the EXTERNAL port from your proxy server configuration
- Re-post if you get deregistered or IP changes

---

## 🗄️ Database Configuration

### PostgreSQL Authentication Fix

If you encounter a "Peer authentication failed" error when running the validator locally:

1. **Locate your `pg_hba.conf` file:**
   ```bash
   # Find the file location
   sudo -u postgres psql -c "SHOW hba_file;"
   ```

2. **Edit the configuration:**
   ```bash
   sudo nano /path/to/your/pg_hba.conf
   ```

3. **Change authentication method:**
   Find this line:
   ```
   local   all             postgres                                peer
   ```
   Change `peer` to `md5`:
   ```
   local   all             postgres                                md5
   ```

4. **Reload PostgreSQL:**
   ```bash
   sudo systemctl reload postgresql
   ```

5. **Set environment variables in your `.env` file:**
   ```bash
   DB_USER=postgres
   DB_PASSWORD=your_actual_postgres_password
   DB_HOST=/var/run/postgresql
   DB_NAME=validator_db
   ```

### 🚀 Automated Database Sync System

**NEW: Streamlined database synchronization with one-command setup!**

#### Quick Setup

**Primary Node (creates backups):**
```bash
sudo python gaia/validator/sync/setup_auto_sync.py --primary
```

**Replica Node (restores from backups):**
```bash
sudo python gaia/validator/sync/setup_auto_sync.py --replica
```

#### Environment Variables
Add to your `.env` file:
```bash
# Required for R2 backup storage
PGBACKREST_R2_BUCKET=your-backup-bucket-name
PGBACKREST_R2_ENDPOINT=https://your-account-id.r2.cloudflarestorage.com
PGBACKREST_R2_ACCESS_KEY_ID=your-r2-access-key
PGBACKREST_R2_SECRET_ACCESS_KEY=your-r2-secret-key

# Set to True for primary node, False for replica
IS_SOURCE_VALIDATOR_FOR_DB_SYNC=True
```

#### Key Benefits
✅ **One-command setup** - No manual pgbackrest configuration  
✅ **Application-controlled scheduling** - No cron jobs needed  
✅ **Automated recovery** - Self-healing backup system  
✅ **Real-time monitoring** - Health checks and status reporting  

---

## 🔍 Troubleshooting

### Common Issues and Solutions

#### 🐛 Multiprocessing SyntaxError with 'async' keyword

**Problem:** Validator crashes with `SyntaxError: invalid syntax` related to `async` keyword.

**Solution:** The validator automatically detects and fixes this issue. For manual fix:
```bash
pip uninstall asyncio -y
python -c "import asyncio, multiprocessing; print('✅ Fixed')"
```

#### 📝 Excessive DEBUG Logging Spam

**Problem:** Logs flooded with DEBUG messages from fiber modules.

**Solution:** Automatically suppressed by default. To enable:
```bash
export GAIA_ENABLE_DEBUG_SPAM=true  # Not recommended in production
```

#### ⚡ Multiprocessing Performance Issues

**Problem:** Small tasks run slower with multiprocessing overhead.

**Solution:** Configure thresholds:
```bash
# Set minimum computations for multiprocessing (default: 10)
export GAIA_MP_THRESHOLD=20

# Disable multiprocessing completely
export GAIA_DISABLE_MP=true
```

**When to use multiprocessing:**
- ✅ Large weather scoring runs (10+ computations)
- ✅ Production validation with many miners

**When to use sequential:**
- ✅ Small test runs (< 10 computations)
- ✅ Development/debugging

---

## 📚 Documentation

### Setup Guides
- **[📋 Mining Guide](docs/MINER.md)** - Complete guide for miners
- **[📋 Validating Guide](docs/VALIDATOR.md)** - Complete guide for validators

### Additional Resources
- **[📖 Whitepaper](docs/whitepaper.md)** - Technical vision and architecture
- **[📊 Weather Scoring System](docs/weather_scoring_system.md)** - Scoring methodology
- **[📈 Stats Coverage Analysis](docs/stats_table_coverage_analysis.md)** - Performance metrics

---

## 📄 Data Sources & Acknowledgments

### Core Technologies
- **[Microsoft Aurora](https://doi.org/10.1038/s41586-025-09005-y)** - Foundation model for Earth system prediction
- **[Fiber](https://github.com/rayonlabs/fiber)** - Distributed computing framework
- **[Bittensor](https://bittensor.com/)** - Decentralized AI network

### Data Sources

| Source | Description | License |
|--------|-------------|---------|
| **[ECMWF Open Data](https://www.ecmwf.int/en/forecasts/datasets/open-data)** | European weather forecasts | Open Data |
| **[HLS Sentinel-2](https://lpdaac.usgs.gov/products/hlss30v002/)** | Multi-spectral satellite imagery | NASA Open Data |
| **[SMAP](https://nsidc.org/data/spl4smgp/versions/7)** | Soil moisture measurements | NASA Open Data |
| **[SRTM](https://lpdaac.usgs.gov/products/srtmgl1v003/)** | Global elevation data | NASA Open Data |
| **[Dst Index](https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/index.html)** | Geomagnetic activity data | Kyoto University |

### License
Copyright 2024 Nickel5 Inc. Licensed under MIT License.

**Disclaimer:** ECMWF does not accept any liability whatsoever for any error or omission in the data, their availability, or for any loss or damage arising from their use.
