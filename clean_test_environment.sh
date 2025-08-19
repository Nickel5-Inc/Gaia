#!/bin/bash

# Comprehensive test environment cleanup script
# Cleans database and pgBackRest backups without deleting stanza configuration

set -e

echo "🧹 Starting comprehensive test environment cleanup..."
echo "=================================================="

# Step 1: Stop validator if running
echo "1️⃣ Stopping validator..."
pm2 stop validator 2>/dev/null || echo "   Validator not running or already stopped"

# Step 2: Clear pgBackRest backups (but keep stanza)
echo "2️⃣ Clearing pgBackRest backups..."
echo "   Stopping pgBackRest operations..."
sudo -u postgres pgbackrest --stanza=gaia-test stop || true

echo "   Expiring all existing backups..."
# Get list of all backups and expire them
BACKUPS=$(sudo -u postgres pgbackrest --stanza=gaia-test info --output=json 2>/dev/null | jq -r '.[0].backup[]?.label // empty' || echo "")
if [ ! -z "$BACKUPS" ]; then
    for backup in $BACKUPS; do
        echo "   Expiring backup: $backup"
        sudo -u postgres pgbackrest --stanza=gaia-test expire --set=$backup || true
    done
else
    echo "   No backups found to expire"
fi

echo "   Starting pgBackRest operations..."
sudo -u postgres pgbackrest --stanza=gaia-test start || true

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
