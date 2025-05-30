# PostgreSQL Database Synchronization with pgBackRest

This guide provides comprehensive instructions for setting up PostgreSQL database synchronization using pgBackRest with Azure Blob Storage for the Gaia validator network.

## Overview

pgBackRest provides:
- **Continuous WAL archiving** to Azure Blob Storage
- **Point-in-time recovery** capabilities
- **Incremental backups** with minimal data transfer
- **Fast node synchronization** via WAL replay instead of full database downloads
- **High availability** with automatic failover support

## Prerequisites

- Ubuntu 20.04+ or similar Linux distribution
- PostgreSQL 12+ installed
- Azure Storage Account with blob storage
- Network connectivity between validator nodes
- Root or sudo access on all nodes

## Architecture

```
Primary Node (Source Validator)
    ↓ WAL Logs
Azure Blob Storage
    ↓ WAL Logs + Backups
Replica Nodes (Other Validators)
```

## Quick Start

1. **Primary Node Setup** (Source validator):
   ```bash
   cd pgbackrest
   sudo ./setup-primary.sh
   ```

2. **Replica Node Setup** (Other validators):
   ```bash
   cd pgbackrest
   sudo ./setup-replica.sh <PRIMARY_NODE_IP>
   ```

3. **Monitor synchronization**:
   ```bash
   ./monitor-sync.sh
   ```

## Detailed Setup Instructions

### Step 1: Environment Configuration

Create `/etc/gaia/pgbackrest.env`:
```bash
# Azure Storage Configuration
AZURE_STORAGE_ACCOUNT=your_storage_account_name
AZURE_STORAGE_KEY=your_storage_account_key
AZURE_CONTAINER=gaia-db-backups

# PostgreSQL Configuration
PGDATA=/var/lib/postgresql/data
PGPORT=5432
PGUSER=postgres

# Network Configuration
PRIMARY_HOST=primary.validator.ip
REPLICA_HOSTS="replica1.ip,replica2.ip,replica3.ip"

# pgBackRest Configuration
STANZA_NAME=gaia
RETENTION_FULL=7
RETENTION_DIFF=2
PROCESS_MAX=4
```

### Step 2: PostgreSQL Configuration

#### postgresql.conf Changes
The setup scripts will automatically configure these, but for reference:

```ini
# WAL Configuration
wal_level = replica
archive_mode = on
archive_command = 'pgbackrest --stanza=gaia archive-push %p'
archive_timeout = 60

# Replication Settings
max_wal_senders = 10
wal_keep_size = 2GB
hot_standby = on

# Performance Settings
checkpoint_completion_target = 0.9
checkpoint_timeout = 15min
max_wal_size = 4GB
min_wal_size = 80MB

# Connection Settings
listen_addresses = '*'
max_connections = 200

# Logging
log_min_duration_statement = 1000
log_checkpoints = on
log_connections = on
log_disconnections = on
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
```

#### pg_hba.conf Configuration
Add these lines to allow replication connections:

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# Local connections
local   all             postgres                                peer
local   all             all                                     peer

# IPv4 local connections
host    all             postgres        127.0.0.1/32            md5
host    all             all             127.0.0.1/32            md5

# IPv6 local connections
host    all             postgres        ::1/128                 md5
host    all             all             ::1/128                 md5

# Replication connections for pgBackRest
host    replication     postgres        127.0.0.1/32            trust
host    replication     postgres        ::1/128                 trust

# Allow connections from validator network (adjust IP ranges as needed)
host    all             postgres        10.0.0.0/8              md5
host    all             gaia_user       10.0.0.0/8              md5
host    replication     postgres        10.0.0.0/8              md5

# Allow connections from specific validator IPs (replace with actual IPs)
# host    all             postgres        1.2.3.4/32              md5
# host    replication     postgres        1.2.3.4/32              md5

# Cloud provider private networks (adjust as needed)
host    all             postgres        172.16.0.0/12           md5
host    all             gaia_user       172.16.0.0/12           md5
host    replication     postgres        172.16.0.0/12           md5

host    all             postgres        192.168.0.0/16          md5
host    all             gaia_user       192.168.0.0/16          md5
host    replication     postgres        192.168.0.0/16          md5
```

### Step 3: Node Setup

#### Primary Node (Source Validator)

1. **Install pgBackRest**:
   ```bash
   sudo apt update
   sudo apt install -y pgbackrest
   ```

2. **Configure environment**:
   ```bash
   sudo mkdir -p /etc/gaia
   sudo cp pgbackrest.env.template /etc/gaia/pgbackrest.env
   # Edit /etc/gaia/pgbackrest.env with your Azure credentials
   ```

3. **Run primary setup**:
   ```bash
   sudo ./setup-primary.sh
   ```

4. **Initialize stanza and take first backup**:
   ```bash
   sudo -u postgres pgbackrest --stanza=gaia stanza-create
   sudo -u postgres pgbackrest --stanza=gaia backup --type=full
   ```

#### Replica Nodes (Other Validators)

1. **Install pgBackRest**:
   ```bash
   sudo apt update
   sudo apt install -y pgbackrest
   ```

2. **Configure environment**:
   ```bash
   sudo mkdir -p /etc/gaia
   sudo cp pgbackrest.env.template /etc/gaia/pgbackrest.env
   # Edit /etc/gaia/pgbackrest.env with your Azure credentials and primary host IP
   ```

3. **Run replica setup**:
   ```bash
   sudo ./setup-replica.sh <PRIMARY_NODE_IP>
   ```

### Step 4: Monitoring and Maintenance

#### Check Synchronization Status
```bash
# Check WAL archiving
sudo -u postgres psql -c "SELECT * FROM pg_stat_archiver;"

# Check replication status (on primary)
sudo -u postgres psql -c "SELECT * FROM pg_stat_replication;"

# Check backup status
sudo -u postgres pgbackrest --stanza=gaia info

# Monitor WAL lag
./monitor-sync.sh
```

#### Manual Operations

**Take manual backup**:
```bash
sudo -u postgres pgbackrest --stanza=gaia backup --type=diff
```

**Restore to specific point in time**:
```bash
sudo systemctl stop postgresql
sudo -u postgres pgbackrest --stanza=gaia restore --target="2024-01-01 12:00:00"
sudo systemctl start postgresql
```

**Check node synchronization**:
```bash
./check-sync.sh
```

## Automated Maintenance

The setup includes cron jobs for:
- **Full backups**: Weekly on Sundays at 2 AM
- **Differential backups**: Daily at 2 AM
- **WAL monitoring**: Hourly checks
- **Cleanup**: Automatic retention management

## Troubleshooting

### Common Issues

1. **WAL archiving failing**:
   ```bash
   # Check archive command
   sudo -u postgres psql -c "SHOW archive_command;"
   
   # Test manual archive
   sudo -u postgres pgbackrest --stanza=gaia archive-push /var/lib/postgresql/data/pg_wal/000000010000000000000001
   ```

2. **Azure connection issues**:
   ```bash
   # Test Azure connectivity
   pgbackrest --stanza=gaia info
   ```

3. **Replication lag**:
   ```bash
   # Check WAL sender processes
   sudo -u postgres psql -c "SELECT * FROM pg_stat_replication;"
   ```

4. **Node out of sync**:
   ```bash
   # Force resync
   sudo ./force-resync.sh
   ```

### Log Locations

- pgBackRest logs: `/var/log/pgbackrest/`
- PostgreSQL logs: `/var/log/postgresql/`
- Setup logs: `/var/log/gaia-pgbackrest/`

## Security Considerations

1. **Azure Storage**: Use dedicated storage account with restricted access
2. **Network**: Configure firewalls to allow only validator node IPs
3. **Authentication**: Use strong passwords and consider certificate-based auth
4. **Encryption**: Enable encryption at rest in Azure Storage
5. **Access Control**: Limit PostgreSQL user permissions

## Performance Tuning

### For High-Volume Environments

```ini
# postgresql.conf optimizations
shared_buffers = 256MB                 # 25% of RAM
effective_cache_size = 1GB             # 75% of RAM
maintenance_work_mem = 64MB
checkpoint_segments = 32               # For PostgreSQL < 9.5
max_wal_size = 8GB                     # For PostgreSQL >= 9.5
```

### pgBackRest Optimizations

```ini
# /etc/pgbackrest/pgbackrest.conf
process-max=8                          # Increase for faster backups
compress-level=3                       # Balance compression vs speed
```

## Integration with Gaia Validator

The Gaia validator automatically detects pgBackRest configuration and adjusts its database synchronization behavior:

1. **Disable built-in DB sync** when pgBackRest is detected
2. **Monitor WAL lag** and alert if synchronization falls behind
3. **Automatic failover** to backup nodes if primary becomes unavailable

## Support

For issues specific to this setup:
1. Check logs in `/var/log/gaia-pgbackrest/`
2. Run diagnostic script: `./diagnose.sh`
3. Review pgBackRest documentation: https://pgbackrest.org/
4. Consult Gaia validator documentation

## Version Compatibility

- **PostgreSQL**: 12, 13, 14, 15, 16
- **pgBackRest**: 2.40+
- **Ubuntu**: 20.04, 22.04
- **Azure**: All storage account types 