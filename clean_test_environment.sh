#!/bin/bash

# Comprehensive test environment cleanup script
# Cleans database and pgBackRest backups without deleting stanza configuration

set -e

echo "🧹 Starting comprehensive test environment cleanup..."
echo "=================================================="

# Step 1: Stop validator if running
echo "1️⃣ Stopping validator..."
pm2 stop validator 2>/dev/null || echo "   Validator not running or already stopped"

# Step 2: Clear pgBackRest backup data (but keep stanza configuration)
echo "2️⃣ Wiping pgBackRest backup data..."

# Remove any stop files that might be blocking operations
echo "   Removing any pgBackRest stop files..."
sudo rm -f /var/lib/pgbackrest/stop/gaia-test.stop 2>/dev/null || true

# Start pgBackRest operations to allow cleanup
echo "   Starting pgBackRest operations..."
sudo -u postgres pgbackrest --stanza=gaia-test start || true

# Get current backup info before wiping
echo "   Checking current backups..."
sudo -u postgres pgbackrest --stanza=gaia-test info || echo "   No backup info available"

# Wipe all backup data from the repository (keeps stanza config)
echo "   Wiping all backup data from repository..."
# This removes all backup files but keeps the stanza configuration
sudo -u postgres pgbackrest --stanza=gaia-test expire --repo1-retention-full=0 || true

# Alternative approach: manually clean the backup directory in S3/R2
echo "   Cleaning backup repository files..."
# Remove backup.info files to force clean state (they'll be recreated on next backup)
sudo rm -f /var/lib/pgbackrest/backup/gaia-test/backup.info* 2>/dev/null || true
sudo rm -f /var/lib/pgbackrest/archive/gaia-test/archive.info* 2>/dev/null || true

echo "   pgBackRest data wipe completed (stanza config preserved)"

# Step 3: Drop and recreate database
echo "3️⃣ Recreating database..."
sudo -u postgres psql -c "DROP DATABASE IF EXISTS validator_db;"
sudo -u postgres psql -c "CREATE DATABASE validator_db;"

# Step 4: Run Alembic migrations
echo "4️⃣ Running Alembic migrations..."
cd /root/Gaia
alembic -c alembic_validator.ini upgrade head

# Step 5: Clear any cached data
echo "5️⃣ Clearing cached data..."
rm -rf /root/Gaia/clim_cache/* 2>/dev/null || true
rm -rf /root/Gaia/verification_logs/* 2>/dev/null || true
rm -rf /tmp/weather_* 2>/dev/null || true

echo ""
echo "✅ Test environment cleanup completed!"
echo "=================================================="
echo "🚀 Ready to restart validator with clean state"
echo ""
echo "To restart validator:"
echo "   pm2 restart validator"
echo ""
echo "To monitor logs:"
echo "   pm2 logs validator"
echo ""
