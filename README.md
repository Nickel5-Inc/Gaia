![Project Logo](docs/logo-full.png)

# <center>Welcome to Gaia</center>

<div style="text-align: center;"><a href="https://www.gaiaresearch.ai/">Website</a></div>

----

Gaia is a platform for research and development of geospatial machine learning models. 
Read more about the long-term vision in our [whitepaper](https://www.gaiaresearch.ai/whitepaper).

>[!NOTE]
> BETA VERSION = 2.0.0
>
> The beta version of Gaia launches with limited functionality. Many of the planned features are not yet available, however we are still accepting miners and validators for the initial tasks. 

### **Clone the Repository**
```bash
git clone https://github.com/Nickel5-Inc/Gaia.git
cd Gaia
```
## **Build the Repository Modules**
```bash
pip install -e .
```

## Miners
[Quicklink - Mining Guide](docs/MINER.md)

Miners develop models to understand future events. These events currently include soil moisture and geomagnetic readings at the equator. Miners will receive data from validators for the models that we have in place. They are also free to gather their own data from other resources. The tasks are consistent in design and in timing; this predictability allows miners the flexibility to retrieve any data that their model requires. 

Miners can choose between these two tasks or perform both. 
Incentive Distribution:
40% of emissions are allocated to Geomagnetic Dst Index Prediction.
60% of emissions are allocated based on a sigmoid-weighted scoring mechanism.
The incentive split was previously 50:50, but has been adjusted to favor higher-quality predictions.


## Validators
[Quicklink - Validating Guide](docs/VALIDATOR.md)

Validators will connect to a few API's to provide miners with the data they need to run models.

## Installation (For ALL Neurons)

Gaia is built on [Fiber](https://github.com/rayonlabs/fiber) - special thanks to namoray and the Rayon labs team.

### Run the setup script

```bash
python./scripts/setup.py
```
- This will create a virtual environment and install all dependencies
- The virtual environment (.gaia) will be located above the gaia directory.
- Activate it after running the setup script using 
```bash
source ../.gaia/bin/activate
```


## Install fiber

----
```bash
pip install "git+https://github.com/rayonlabs/fiber.git@production#egg=fiber[full]"
```

### PostgreSQL Configuration for Local Connections

If you are running the validator and PostgreSQL on the same machine and intend to use the default database user (`postgres`), you might encounter a "Peer authentication failed" error. This typically happens if the validator application is run as an OS user different from `postgres` (e.g., as `root` via `pm2`).

To resolve this and enable password authentication for the `postgres` user via local Unix domain sockets (recommended for security over `peer` when OS users don't match):

1.  **Locate your `pg_hba.conf` file.**
    This file is critical for PostgreSQL's client authentication. Common locations include:
    *   `/etc/postgresql/<YOUR_PG_VERSION>/main/pg_hba.conf`
    *   `/var/lib/pgsql/data/pg_hba.conf`
    You can find its exact location by connecting to `psql` and running `SHOW hba_file;`.

2.  **Edit `pg_hba.conf` with `sudo` privileges.**
    Open the file using a text editor, for example:
    ```bash
    sudo nano /path/to/your/pg_hba.conf 
    ```
    (Replace `/path/to/your/pg_hba.conf` with the actual path).

3.  **Modify the `local` connection rule for the `postgres` user.**
    Look for a line similar to:
    ```
    # TYPE  DATABASE        USER            ADDRESS                 METHOD
    local   all             postgres                                peer
    ```
    Change `peer` to `md5`. The line should now look like:
    ```
    # TYPE  DATABASE        USER            ADDRESS                 METHOD
    local   all             postgres                                md5
    ```
    If you have a more general rule like `local all all peer`, you can either change that to `md5` (which will require passwords for all local users) or add the more specific line for `postgres` *before* the general `peer` rule.

4.  **Save the `pg_hba.conf` file.**

5.  **Reload the PostgreSQL configuration.**
    For the changes to take effect, PostgreSQL needs to reload its configuration:
    ```bash
    sudo systemctl reload postgresql
    # Or, for older systems:
    # sudo service postgresql reload
    ```

6.  **Ensure Environment Variables are Set.**
    Make sure your `.env` file (or your environment configuration method for `pm2`) correctly sets:
    *   `DB_USER=postgres`
    *   `DB_PASSWORD=your_actual_postgres_password`
    *   `DB_HOST=/var/run/postgresql` (or your correct Unix socket directory)
    *   `DB_NAME=your_database_name` (e.g., `validator_db`)
    *   `DB_PORT` (will be ignored if `DB_HOST` is a Unix socket path, but good to have)

After these steps, your validator should be able to connect to the local PostgreSQL server using the `postgres` user and its password. For enhanced security in production, consider creating a dedicated, less-privileged PostgreSQL user for the validator application.

#### Register miner and/or validator on subnet
```bash
btcli subnets register --subtensor.network <NETWORK> --netuid <NETUID> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY>
```



#### Setup Proxy server
Gaia uses a Proxy server to handle connections to Validators. You can setup your own proxy server, or use our script for an nginx server as follows:

```bash
./setup_proxy_server.sh --ip <YOUR IP> --port <PORT> --forwarding_port <PORT_FOR_MINER_OR_VALIDATOR> --server_name <NAME>
```

- This will run as a background process, but it must be running for proper communication between Miners and Validators
- IMPORTANT: the port argument is your external facing port, and the forwarding port is the INTERNAL port that the miner or validator will be using to communicate with the proxy server
- The server name argument is optional and could be set to whatever you'd like

#### Post IP to chain -- IMPORTANT
```bash
fiber-post-ip --netuid <NETUID> --external_ip <YOUR_IP> --external_port <YOUR_PORT> --subtensor.network <NETWORK> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY> 
```
- You only need to do this once per key, but if you get deregistered or your IP changes, you'll need to re-post your IP. We recommend using a static IP for all nodes.
- Make sure that you use the EXTERNAL port that you configured in the proxy server script above
- This will be automated in a future version, but for now you'll need to post your IP manually


### Starting Miner
- Further instructions are linked below, but ensure that you start the miner with the `--port` argument pointing to the FORWARDING/INTERNAL port configured in the proxy server script above.


## Database Synchronization (Validators)

**🚀 NEW: Automated Database Sync System**

Gaia now includes a streamlined database synchronization system that eliminates manual pgBackRest configuration and provides application-controlled backup scheduling. This system automatically sets up and manages database backups to Cloudflare R2 storage.

### Key Benefits

✅ **One-command setup** - No more manual pgbackrest configuration  
✅ **Application-controlled scheduling** - No cron jobs needed, easily configurable  
✅ **Automated recovery** - Self-healing backup system  
✅ **Simplified replica setup** - No manual IP coordination required  
✅ **Real-time monitoring** - Health checks and status reporting  

### Quick Setup

#### 1. Configure Environment Variables

Add these to your `.env` file:

```bash
# Required for R2 backup storage
PGBACKREST_R2_BUCKET=your-backup-bucket-name
PGBACKREST_R2_ENDPOINT=https://your-account-id.r2.cloudflarestorage.com
PGBACKREST_R2_ACCESS_KEY_ID=your-r2-access-key
PGBACKREST_R2_SECRET_ACCESS_KEY=your-r2-secret-key

# Optional - defaults shown
PGBACKREST_STANZA_NAME=gaia
PGBACKREST_R2_REGION=auto
PGBACKREST_PGDATA=/var/lib/postgresql/data
PGBACKREST_PGPORT=5432
PGBACKREST_PGUSER=postgres

# Set to True for primary node, False for replica
IS_SOURCE_VALIDATOR_FOR_DB_SYNC=True
```

#### 2. Run Automated Setup

**Primary Node (creates backups):**
```bash
sudo python gaia/validator/sync/setup_auto_sync.py --primary
```

**Replica Node (restores from backups):**
```bash
sudo python gaia/validator/sync/setup_auto_sync.py --replica
```

**Test Mode (faster setup, shorter retention):**
```bash
sudo python gaia/validator/sync/setup_auto_sync.py --primary --test
```

#### 3. The System is Ready!

- **Primary nodes** automatically create backups (full daily at 2 AM, differential every 4 hours)
- **Replica nodes** can restore with a simple command
- **All scheduling happens in the application** - no cron jobs to manage
- **Health monitoring** runs continuously with automatic recovery

### Advanced Operations

**Check backup status:**
```python
from gaia.validator.sync.auto_sync_manager import *
import asyncio
asyncio.run(check_status())
```

**Restore from latest backup (replicas):**
```python
from gaia.validator.sync.auto_sync_manager import *
import asyncio
asyncio.run(restore_latest())
```

**Change backup schedule dynamically:**
```python
# No need to edit cron - just update in the application
manager.update_schedule({
    'full_backup_time': '03:00',
    'diff_backup_interval': 6  # Every 6 hours instead of 4
})
```

### Migration from Legacy System

If you're currently using the manual pgBackRest setup, the new system will:
- ✅ Detect existing configuration and work alongside it
- ✅ Gradually replace cron-based scheduling with application control
- ✅ Provide the same functionality with better monitoring and error handling

The legacy scripts in `gaia/validator/sync/pgbackrest/` remain available but are no longer recommended for new installations.

### Troubleshooting

1. **Setup fails:** Check environment variables and ensure running as root
2. **Backup failures:** The system includes automatic recovery and will retry failed operations
3. **Connection issues:** Verify R2 credentials and network connectivity
4. **Status monitoring:** All operations are logged and monitored automatically

For detailed logs, check `/var/log/pgbackrest/` and the validator application logs.

## Common Issues and Solutions

### Multiprocessing SyntaxError with 'async' keyword

**Problem:** Validator crashes with `SyntaxError: invalid syntax` related to `async` keyword in multiprocessing workers.

**Cause:** An old `asyncio` package (v3.4.3) was installed that uses `async` as a function name, but `async` became a reserved keyword in Python 3.7+.

**Solution:** The validator automatically detects and fixes this issue on startup. However, to prevent it from happening:

1. **Manual fix (immediate):**
   ```bash
   # Remove the problematic package
   pip uninstall asyncio -y
   
   # Remove any remaining files
   find ~/.local/lib/python*/site-packages -name "*asyncio*" -exec rm -rf {} +
   find /usr/local/lib/python*/site-packages -name "*asyncio*" -exec rm -rf {} +
   ```

2. **Automatic prevention:** The validator now includes automatic detection and removal of problematic asyncio packages during startup.

3. **Prevention in new installs:** Always use Python's built-in `asyncio` module. Never install the separate `asyncio` package on Python 3.7+.

**Verification:** After fixing, test multiprocessing compatibility:
```bash
python -c "import asyncio, multiprocessing; print('✅ Asyncio import successful')"
```

### Excessive DEBUG Logging Spam

**Problem:** Validator logs are flooded with hundreds of `"Logging mode is DEBUG"` messages from fiber modules.

**Cause:** Every module logs its logging mode during import, and multiprocessing workers import modules independently.

**Solution:** The validator automatically suppresses these verbose DEBUG messages by default.

**Control options:**
- **Default behavior:** DEBUG spam is suppressed for cleaner logs
- **Enable all DEBUG messages:** Set `GAIA_ENABLE_DEBUG_SPAM=true` in your environment
- **Only important messages are shown:** INFO level and above are still logged normally

```bash
# To see all DEBUG spam (not recommended in production)
export GAIA_ENABLE_DEBUG_SPAM=true

# Default behavior (recommended) - suppress spam
unset GAIA_ENABLE_DEBUG_SPAM
```

### Multiprocessing Performance Configuration

**Problem:** Small weather tasks may run slower with multiprocessing due to overhead.

**Cause:** Process creation and module imports (~30-40 seconds) can exceed computation time for small workloads.

**Solution:** Smart multiprocessing with configurable thresholds.

**Configuration options:**
```bash
# Set minimum computations required for multiprocessing (default: 10)
export GAIA_MP_THRESHOLD=20

# Completely disable multiprocessing (forces sequential)
export GAIA_DISABLE_MP=true

# Default behavior (recommended) - auto-detect based on workload size
unset GAIA_MP_THRESHOLD
unset GAIA_DISABLE_MP
```

**When multiprocessing helps:**
- ✅ Large weather scoring runs (10+ computations)
- ✅ Production validation with many miners
- ✅ Complex climatology computations

**When to use sequential:**
- ✅ Small test runs (< 10 computations)
- ✅ Development/debugging
- ✅ Memory-constrained systems

---

### Follow the Setup Guides for [Miner](docs/MINER.md) or [Validator](docs/VALIDATOR.md)
#### Custom Miner Models: [HERE](gaia/models/custom_models/CUSTOMMODELS.md)

---

## Compute Requirements (min)

### Miners
- CPU: 6-core processor
- RAM: 8 GB
- Network: Reliable connection with at least 80 Mbps upload and download speeds. 1Tb monthly bandwidth.
### Validators
- CPU: 8-core processor
- RAM: 16 GB
- Network: Reliable connection with at least 80 Mbps upload and download speeds. 1Tb monthly bandwidth.

--- 

## Data/Model acknowledgements

### ECMWF Open Data
#### Copyright statement
Copyright © [2024] European Centre for Medium-Range Weather Forecasts (ECMWF).

#### Licence Statement
Copyright 2024 Nickel5 Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

### Microsoft Aurora

**A Foundation Model for the Earth System.**<br>
Cristian Bodnar, Wessel P. Bruinsma, Ana Lucic, Megan Stanley, Anna Allen, Johannes Brandstetter, Patrick Garvan, Maik Riechert, Jonathan A. Weyn, Haiyu Dong, Jayesh K. Gupta, Kit Thambiratnam, Alexander T. Archibald, Chun-Chieh Wu, Elizabeth Heider, Max Welling, Richard E. Turner, and Paris Perdikaris.<br>
*Nature*, 2025.<br>
DOI: [10.1038/s41586-025-09005-y](https://doi.org/10.1038/s41586-025-09005-y)

#### Disclaimer
ECMWF does not accept any liability whatsoever for any error or omission in the data, their availability, or for any loss or damage arising from their use.

Material has been modified by the following: resolution transformations, spatial transformations, and evapotranspiration calculations on existing bands of ECMWF data.

[ECMWF Open Data](https://www.ecmwf.int/en/forecasts/datasets/open-data)

### HLS Sentinel-2

Masek, J., Ju, J., Roger, J., Skakun, S., Vermote, E., Claverie, M., Dungan, J., Yin, Z., Freitag, B., Justice, C. (2021). HLS Sentinel-2 Multi-spectral Instrument Surface Reflectance Daily Global 30m v2.0 [Data set]. NASA EOSDIS Land Processes Distributed Active Archive Center. Accessed 2024-11-27 from https://doi.org/10.5067/HLS/HLSS30.002

[HLSS30](https://lpdaac.usgs.gov/products/hlss30v002/)

### Soil Moisture Active Passive

Reichle, R., De Lannoy, G., Koster, R. D., Crow, W. T., Kimball, J. S., Liu, Q. & Bechtold, M. (2022). SMAP L4 Global 3-hourly 9 km EASE-Grid Surface and Root Zone Soil Moisture Geophysical Data. (SPL4SMGP, Version 7). [Data Set]. Boulder, Colorado USA. NASA National Snow and Ice Data Center Distributed Active Archive Center. https://doi.org/10.5067/EVKPQZ4AFC4D. Date Accessed 11-27-2024.

[SPL4SMGP](https://nsidc.org/data/spl4smgp/versions/7)

### Nasa Shuttle Radar Topography Mission

NASA JPL (2013). NASA Shuttle Radar Topography Mission Global 1 arc second [Data set]. NASA EOSDIS Land Processes Distributed Active Archive Center. Accessed 2024-11-27 from https://doi.org/10.5067/MEaSUREs/SRTM/SRTMGL1.003

[SRTMl1v003](https://lpdaac.usgs.gov/products/srtmgl1v003/)

### World Data Center for Geomagnetism, Kyoto

World Data Center for Geomagnetism, Kyoto. World Data Center for Geomagnetism, Kyoto. Kyoto University. Accessed December 2, 2024.

[Dst Open Data](https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/index.html)
