# Gaia Validator pgBackRest Quick Start Guide

This guide will get you up and running with pgBackRest database synchronization in 10 minutes.

## Prerequisites

- Ubuntu 20.04+ with PostgreSQL installed
- Azure Storage Account credentials
- Root/sudo access

## Step 1: Configure Environment (5 minutes)

1. **Copy the environment template:**
   ```bash
   sudo mkdir -p /etc/gaia
   sudo cp pgbackrest/pgbackrest.env.template /etc/gaia/pgbackrest.env
   ```

2. **Edit the environment file:**
   ```bash
   sudo nano /etc/gaia/pgbackrest.env
   ```
   
   **REQUIRED**: Update these values:
   ```bash
   AZURE_STORAGE_ACCOUNT=your_actual_storage_account
   AZURE_STORAGE_KEY=your_actual_storage_key
   AZURE_CONTAINER=gaia-db-backups
   PRIMARY_HOST=1.2.3.4  # IP of your primary validator
   ```

## Step 2: Setup Node Type (3 minutes)

### For PRIMARY node (source validator):
```bash
cd pgbackrest
sudo ./setup-primary.sh
```

### For REPLICA nodes (other validators):
```bash
cd pgbackrest
sudo ./setup-replica.sh <PRIMARY_NODE_IP>
```

## Step 3: Verify Setup (2 minutes)

Check if everything is working:
```bash
./monitor-sync.sh
```

If you see any issues:
```bash
./diagnose.sh
```

## What Happens Next

- **Primary**: WAL logs are continuously uploaded to Azure Storage
- **Replicas**: Automatically sync by downloading and replaying WAL logs
- **Backups**: Full backup weekly, differential daily (automated)
- **Monitoring**: Status checked every 15 minutes

## Common Issues & Quick Fixes

### "Azure connectivity failed"
```bash
# Check your Azure credentials
sudo nano /etc/gaia/pgbackrest.env
# Test connectivity
sudo -u postgres pgbackrest --stanza=gaia check
```

### "PostgreSQL not running"
```bash
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### "Replication lag too high"
```bash
# Check network connectivity to primary
ping <PRIMARY_IP>
# Check PostgreSQL port
nc -z <PRIMARY_IP> 5432
```

### "Permission denied errors"
```bash
# Fix ownership
sudo chown -R postgres:postgres /var/log/pgbackrest /var/lib/pgbackrest
```

## Monitoring Commands

| Command | Purpose |
|---------|---------|
| `./monitor-sync.sh` | Overall status check |
| `./diagnose.sh` | Detailed troubleshooting |
| `sudo -u postgres pgbackrest --stanza=gaia info` | Backup information |
| `sudo -u postgres psql -c "SELECT pg_is_in_recovery();"` | Check if replica |

## Files Created

- `/etc/gaia/pgbackrest.env` - Configuration
- `/etc/pgbackrest/pgbackrest.conf` - pgBackRest config
- `/var/log/gaia-pgbackrest/` - Setup logs
- `/var/log/pgbackrest/` - pgBackRest logs

## Security Notes

1. **Firewall**: Ensure port 5432 is open between validators
2. **Azure**: Use dedicated storage account with restricted access
3. **Network**: Consider VPN or private networking between validators
4. **Credentials**: Store Azure keys securely, rotate regularly

## Performance Expectations

- **Initial sync**: 10-30 minutes (depending on database size)
- **Ongoing sync**: < 30 seconds lag
- **Backup size**: ~50% of database size (compressed)
- **Network usage**: Minimal (only WAL logs, ~1-10MB/hour typical)

## Support

If you encounter issues:

1. Run `./diagnose.sh` and check the output
2. Review logs in `/var/log/gaia-pgbackrest/`
3. Check Azure Storage connectivity
4. Verify network connectivity between nodes

For detailed configuration options, see the full [README.md](README.md). 