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

---

#### Create .env file for validator with the following components:
```bash
DB_USER=<YOUR_DB_USER> # postgres is default from setup script
DB_PASSWORD=<YOUR_DB_PASSWORD> # postgres is default from setup script
DB_HOST=<YOUR_DB_HOST> # localhost is default from setup script
DB_PORT=<YOUR_DB_PORT> # 5432

ENV=prod #dev for debug logs


WALLET_NAME=<YOUR_WALLET_NAME>
HOTKEY_NAME=<YOUR_WALLET_HOTKEY>
NETUID=<NETUID> # 57 for mainnet, 237 for testnet
SUBTENSOR_NETWORK=<NETWORK> # finney or test
SUBTENSOR_ADDRESS=<SUBTENSOR_ADDRESS> # wss://test.finney.opentensor.ai:443/ for testnet, wss://entrypoint-finney.opentensor.ai:443/ for mainnet (chain endpoint)

EARTHDATA_USERNAME=<YOUR_EARTHDATA_USERNAME> 
EARTHDATA_PASSWORD=<YOUR_EARTHDATA_PASSWORD>
EARTHDATA_API_KEY=<YOUR_EARTHDATA_API_KEY> # earthdata api key for downloading data from NASA
```

#### Run the validator
```bash
pm2 start --name validator --instances 1 python -- gaia/validator/validator.py 
```

---

## **Database Synchronization System (Optional, highly recommended)**

This system allows for near real-time replication of the validator database from a designated source validator to other replica validators using Azure Blob Storage as a central staging point. This is useful for maintaining consistent state across multiple validator instances or for quickly bringing new validators online with current data.

### Overview

1.  **Source Validator**: One validator is designated as the "source of truth". It periodically performs a `pg_dump` of its database. The source validator is run by Nickel5.
2.  **Azure Blob Storage**: The database dump from the source validator is compressed and uploaded to a configured Azure Blob Storage container.
3.  **Replica Validators**: Other validators (replicas) periodically check Azure Blob Storage for new database dumps.
4.  **Restore**: If a new dump is found, replica validators download it and use `pg_restore` to replace their local database with the contents of the dump.

This process aims to keep replica databases synchronized with the source, typically within an hour (configurable) of the last successful backup.

### Configuration (Environment Variables)

To enable and configure the database synchronization system, set the following environment variables on your validator nodes:

*   `DB_SYNC_ENABLED`: (boolean string: "true" or "false")
    *   Master switch to enable or disable the entire database synchronization feature.
    *   Set to `"false"` to completely turn off DB sync (both backup and restore operations).
    *   Defaults to `"true"` if not set, meaning the sync system will attempt to run if other relevant DB sync variables are configured.
    *   Example: `DB_SYNC_ENABLED=false`

*   `IS_SOURCE_VALIDATOR_FOR_DB_SYNC`: (boolean string: "true" or "false")
    *   **Only relevant if `DB_SYNC_ENABLED` is `"true"` (or not set).**
    *   Set to `"true"` ONLY if this validator is the designated source for DB backups (typically the main Nickel5 validator).

*   `DB_SYNC_INTERVAL_HOURS`: (integer)
    *   Defines how often the backup (on source) or restore check (on replicas) process runs.
    *   Example: `1` for hourly synchronization.
    *   Defaults to `1` if not set or set to a non-positive value.
    *   We recommend keeping this value set to 1 for the most up-to-date data.

*   **Azure Authentication (provide one of the following methods):**
    *   **Method 1: SAS Token (Recommended)**
        *   `AZURE_STORAGE_ACCOUNT_URL`: (string)
            *   The Blob service endpoint for your storage account.
            *   Example: `https://<youraccountname>.blob.core.windows.net`
        *   `AZURE_STORAGE_SAS_TOKEN`: (string)
            *   The SAS token string (including the leading `?` if applicable, or just the token part).
            *   This token should grant necessary permissions (Read, List for replicas; Read, Write, List, Delete for source) to the specified `AZURE_BLOB_CONTAINER_NAME_DB_SYNC`.
    *   **Method 2: Connection String (Simpler, but generally less secure than SAS Token)**
        *   `AZURE_STORAGE_CONNECTION_STRING`: (string)
            *   The full connection string for your Azure Blob Storage account.

*   `AZURE_BLOB_CONTAINER_NAME_DB_SYNC`: (string)
    *   Name of the Azure Blob container to use for storing database dumps.
    *   Example: `validator-db-sync`
    *   Defaults to `validator-db-sync` if not set.
*   `DB_NAME`: (string)
    *   The name of the PostgreSQL database to be backed up and restored.
    *   Should match the main database used by the validator (e.g., `validator_db`).
    *   Defaults to `validator_db` if `DB_NAME` is not set.
*   `DB_USER`: (string)
    *   PostgreSQL username for connecting to the database for dump/restore operations.
    *   This user needs permissions to perform `pg_dump` and, on replicas, to drop/create the database and terminate connections.
    *   Example: `postgres`
    *   Defaults to `postgres` if `DB_USER` is not set.
*   `DB_HOST`: (string)
    *   Hostname or IP address of the PostgreSQL server.
    *   Example: `localhost`
    *   Defaults to `localhost` if `DB_HOST` is not set.
*   `DB_PORT`: (integer)
    *   Port number of the PostgreSQL server.
    *   Example: `5432`
    *   Defaults to `5432` if `DB_PORT` is not set.
*   `DB_PASS`: (string)
    *   Password for the `DB_USER` to connect to PostgreSQL. The default config password is `postgres`.
    *   This is passed to `pg_dump` and `pg_restore` via the `PGPASSWORD` environment variable for the subprocess.
*   `DB_SYNC_BACKUP_DIR`: (path string)
    *   Local temporary directory on the **source validator** where `pg_dump` will create the database dump file before uploading to Azure.
    *   Example: `/tmp/db_backups_gaia`
    *   Defaults to `/tmp/db_backups_gaia`.
*   `DB_SYNC_RESTORE_DIR`: (path string)
    *   Local temporary directory on **replica validators** where new database dumps will be downloaded from Azure before being restored.
    *   Example: `/tmp/db_restores_gaia`
    *   Defaults to `/tmp/db_restores_gaia`.
*   `DB_SYNC_MAX_AZURE_BACKUPS`: (integer)
    *   The number of recent database backups to retain in Azure Blob Storage. Older backups beyond this count will be automatically pruned by the source validator after a successful backup.
    *   Example: `5` (keeps the last 5 backups).
    *   Defaults to `5`.

### Required Dependencies

*   **Python Package**: `azure-storage-blob` (install via `pip install azure-storage-blob`)
*   **Command-Line Tools**: `pg_dump`, `