# Database Sync with Cloudflare R2

This guide explains how to configure the Gaia Validator database sync system using Cloudflare R2 and `pgBackRest`.

## Overview

The primary mechanism for database backup and restore is `pgBackRest`, configured to use Cloudflare R2 as its S3-compatible backend. This approach provides robust, efficient, and cost-effective database synchronization.

R2 is utilized for:
- **Performance**: `pgBackRest` is optimized for large database operations, and R2 provides a performant object storage backend.
- **Cost-Effectiveness**: R2 typically offers lower costs, especially regarding egress fees.
- **S3 Compatibility**: `pgBackRest` uses the standard S3 API to interact with R2.

## Setup Steps

Setting up database synchronization involves configuring a primary (source) validator that performs backups and one or more replica validators that can restore from these backups.

### 1. Prerequisites

- Ensure `pgBackRest` and the PostgreSQL client are installable/installed on your systems (the setup scripts attempt to install them).
- Have your Cloudflare R2 credentials ready: Account ID, Access Key ID, and Secret Access Key.
- Create an R2 bucket dedicated to `pgBackRest` backups.

### 2. Environment Variables

Configure these environment variables in your `.env` file on both primary and replica nodes. The setup scripts will source this file.

**Required for `pgBackRest` with R2:**

| Variable                            | Description                                                                 |
|-------------------------------------|-----------------------------------------------------------------------------|
| `PGBACKREST_STANZA_NAME`            | The `pgBackRest` stanza name (e.g., `gaia`). Must be consistent across nodes. Default: `gaia`. |
| `PGBACKREST_R2_BUCKET`              | The R2 bucket name you created for `pgBackRest` backups.                      |
| `PGBACKREST_R2_ENDPOINT`            | Your R2 endpoint URL (e.g., `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`). |
| `PGBACKREST_R2_ACCESS_KEY_ID`       | Your R2 API token Access Key ID.                                            |
| `PGBACKREST_R2_SECRET_ACCESS_KEY`   | Your R2 API token Secret Access Key.                                        |
| `PGBACKREST_R2_REGION`              | The R2 region. For Cloudflare R2, `auto` is usually sufficient. Default: `auto`. |
| `PGBACKREST_PGDATA`                 | Path to your PostgreSQL data directory (e.g., `/var/lib/postgresql/data`). Default: `/var/lib/postgresql/data`. |
| `PGBACKREST_PGPORT`                 | PostgreSQL port. Default: `5432`.                                           |
| `PGBACKREST_PGUSER`                 | PostgreSQL user for `pgBackRest` operations. Default: `postgres`.             |

**General Database Sync (used by Python service wrapper if applicable):**

| Variable                            | Default | Description                                                                                                |
|-------------------------------------|---------|------------------------------------------------------------------------------------------------------------|
| `DB_SYNC_ENABLED`                   | `True`  | Globally enable/disable database sync features (if Python service wrappers are used for monitoring/control). |
| `IS_SOURCE_VALIDATOR_FOR_DB_SYNC`   | `False` | Set to `True` for the primary/source validator. Replicas should have this as `False`.                        |

### 3. Primary Node Setup (Source for Backups)

1.  **Set Environment Variables**: Ensure the `.env` file on the primary node has all the `PGBACKREST_*` variables correctly defined, and `IS_SOURCE_VALIDATOR_FOR_DB_SYNC=True`.
2.  **Run Setup Script**: Execute the `gaia/validator/sync/pgbackrest/setup-primary.sh` script as root.
    ```bash
    sudo bash gaia/validator/sync/pgbackrest/setup-primary.sh
    ```
    This script will:
    - Install `pgbackrest` and `postgresql-client`.
    - Configure PostgreSQL for archiving (`postgresql.conf`, `pg_hba.conf`).
    - Create the `pgBackRest` configuration file (`/etc/pgbackrest/pgbackrest.conf`) using the R2 environment variables.
    - Restart PostgreSQL.
    - Create the `pgBackRest` stanza in the R2 bucket.
    - Take an initial full backup.
    - Set up cron jobs for regular full and differential backups, and checks.
3.  **Verify**: Check the script's log output (e.g., in `/var/log/gaia-pgbackrest/`) and your R2 bucket to confirm backups are being stored.
    ```bash
    sudo -u postgres pgbackrest --stanza=<your_stanza_name> info
    ```

### 4. Replica Node Setup (Restore Target)

1.  **Set EnvironmentVariables**: Ensure the `.env` file on the replica node has the same `PGBACKREST_*` variables as the primary (pointing to the same R2 bucket and stanza). `IS_SOURCE_VALIDATOR_FOR_DB_SYNC` should be `False`.
2.  **Run Setup Script**: Execute the `gaia/validator/sync/pgbackrest/setup-replica.sh` script as root. *(Note: The detailed functionality of `setup-replica.sh` should be verified and documented here. It typically configures `pgBackRest` on the replica, and prepares it for restoring from the stanza).* You will likely need to provide the primary node's IP or hostname to the script for initial configuration or if it sets up streaming replication.
    ```bash
    # Example: sudo bash gaia/validator/sync/pgbackrest/setup-replica.sh <primary_node_ip_or_hostname>
    ```
3.  **Restore Database**: To perform a restore, you generally need to stop PostgreSQL on the replica, clear the data directory (if it exists and is not empty), and then run the `pgBackRest restore` command. The `setup-replica.sh` script might handle the initial restore, or you might do it manually:
    ```bash
    sudo systemctl stop postgresql
    # Ensure PGDATA directory is empty or non-existent, or move/backup existing data
    # Example: sudo -u postgres rm -rf /var/lib/postgresql/data/* 
    sudo -u postgres pgbackrest --stanza=<your_stanza_name> restore
    sudo systemctl start postgresql
    ```
    Consult the `setup-replica.sh` script and `pgBackRest` documentation for the exact procedure, especially if setting up standby/streaming replication.

## Monitoring and Troubleshooting

### `pgBackRest` Commands

Use `pgbackrest` commands to check status, info, and perform operations:
```bash
sudo -u postgres pgbackrest --stanza=<your_stanza_name> info
sudo -u postgres pgbackrest --stanza=<your_stanza_name> check
# stanza-create, backup, restore, etc.
```

### Logging

- **Setup Scripts**: Check logs in `/var/log/gaia-pgbackrest/`.
- **`pgBackRest`**: Logs are typically in `/var/log/pgbackrest/` (e.g., `<stanza_name>-backup.log`). Configure `log-level-file` in `pgbackrest.conf` for verbosity.
- **PostgreSQL**: Check PostgreSQL server logs for issues related to archiving or replication.

### Common Issues

1.  **R2 Credentials/Permissions**: Double-check `R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`. Ensure the API token has correct permissions (Read & Write) for the specified R2 bucket.
2.  **Bucket/Stanza Misconfiguration**: Ensure `PGBACKREST_R2_BUCKET` is correct and the stanza name (`PGBACKREST_STANZA_NAME`) matches on all nodes and in commands.
3.  **Connectivity**: Ensure network connectivity to R2 from your validator nodes.
4.  **PostgreSQL Configuration**: Verify `postgresql.conf` and `pg_hba.conf` are correctly set up for archiving and replication by the setup scripts.
5.  **File System Permissions**: Ensure the `postgres` user has necessary permissions for `pgBackRest` log and lib directories (`/var/log/pgbackrest`, `/var/lib/pgbackrest`).

## Advanced: Python Service Wrappers (`BackupManager`, `RestoreManager`)

The Python scripts `BackupManager.py` and `RestoreManager.py` in `gaia.validator.sync` might exist for:
-   Orchestrating `pgBackRest` commands via application logic (e.g., triggering a check or an ad-hoc backup).
-   Monitoring backup status by interacting with `pgBackRest` or its logs.
-   Providing a higher-level abstraction if the application needs to manage sync tasks beyond what cron provides.

If `pgBackRest` is fully managed by its cron jobs and manual commands, the direct operational role of these Python managers in executing backups/restores might be minimal. Their utility would shift towards monitoring, status reporting, or integrating `pgBackRest` operations into a broader application workflow if required. The `storage_utils.py` and `r2_storage_utils.py` provide foundational R2 access that these services (or other parts of the application) can leverage if they need to interact with R2 directly for tasks outside of `pgBackRest`'s own operations (e.g., managing metadata files, although with `pgBackRest`, such custom manifest files are generally not needed for the backup data itself).

Review their implementation to understand their current role in your specific setup. If they are primarily for managing a legacy pg_dump flow, their relevance decreases significantly with a full `pgBackRest` adoption.

## Cloudflare R2 Pricing (Example)

-   **Storage**: Typically around $0.015 per GB per month.
-   **Class A Operations** (e.g., writes, listings): ~$4.50 per million requests.
-   **Class B Operations** (e.g., reads): ~$0.36 per million requests.
-   **Egress Fees**: Generally $0 (Free) - a significant advantage.

*Always refer to the official Cloudflare R2 pricing page for the most current information.* 