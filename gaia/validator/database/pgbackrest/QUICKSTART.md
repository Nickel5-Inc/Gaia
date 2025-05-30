# Gaia Validator pgBackRest Quick Start Guide

This guide will get you up and running with pgBackRest database synchronization in 10 minutes.

## Prerequisites

- Ubuntu 20.04+ with PostgreSQL installed
- Azure Storage Account credentials
- Root/sudo access
- Existing Gaia validator with configured .env file

## Step 1: Configure Environment (5 minutes)

1. **Add pgBackRest configuration to your existing .env file:**
   
   Edit your main validator `.env` file and add the pgBackRest configuration section:
   
   **REQUIRED**: Update these values in your `.env` file:
   ```bash
   # pgBackRest Configuration
   PGBACKREST_AZURE_STORAGE_ACCOUNT=your_actual_storage_account
   PGBACKREST_AZURE_STORAGE_KEY=your_actual_storage_key
   PGBACKREST_AZURE_CONTAINER=gaia-db-backups
   PGBACKREST_PRIMARY_HOST=1.2.3.4  # IP of your primary validator
   
   # Optional pgBackRest settings (defaults shown)
   PGBACKREST_STANZA_NAME=gaia
   PGBACKREST_PGDATA=/var/lib/postgresql/data
   PGBACKREST_PGPORT=5432
   PGBACKREST_PGUSER=postgres
   ```

2. **Restart your validator to load the new configuration:**
   ```bash
   pm2 restart validator
   ```

## Step 2: Setup Node Type (3 minutes)

### For PRIMARY node (source validator):
```bash
cd gaia/validator/database/pgbackrest
sudo ./setup-primary.sh
```

### For REPLICA nodes (other validators):
```bash
cd gaia/validator/database/pgbackrest
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
- **Integration**: Validator automatically detects pgBackRest and disables built-in DB sync

## Common Issues & Quick Fixes

### "Azure connectivity failed"
```bash
# Check your Azure credentials in .env file
nano .env
# Look for PGBACKREST_AZURE_* variables
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

## Environment Variables Reference

Add these to your main validator `.env` file:

```bash
# Required
PGBACKREST_AZURE_STORAGE_ACCOUNT=your_storage_account
PGBACKREST_AZURE_STORAGE_KEY=your_storage_key
PGBACKREST_AZURE_CONTAINER=gaia-db-backups
PGBACKREST_PRIMARY_HOST=primary.validator.ip

# Optional (with defaults)
PGBACKREST_STANZA_NAME=gaia
PGBACKREST_PGDATA=/var/lib/postgresql/data
PGBACKREST_PGPORT=5432
PGBACKREST_PGUSER=postgres
PGBACKREST_RETENTION_FULL=7
PGBACKREST_RETENTION_DIFF=2
PGBACKREST_PROCESS_MAX=4
```

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
5. Ensure your `.env` file has all required PGBACKREST_ variables

For detailed configuration options, see the full [README.md](README.md). 