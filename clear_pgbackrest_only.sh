#!/bin/bash

# Clear pgBackRest backups without deleting stanza or database
# Useful when you just want to prevent backup restoration of corrupted data

set -e

echo "🗑️ Clearing pgBackRest backups (keeping stanza)..."
echo "=============================================="

# Check if stanza exists first
if ! sudo -u postgres pgbackrest --stanza=gaia-test info >/dev/null 2>&1; then
    echo "❌ Stanza 'gaia-test' doesn't exist. Creating it..."
    sudo -u postgres pgbackrest --stanza=gaia-test stanza-create
    echo "✅ Stanza created successfully"
    echo ""
    echo "✅ pgBackRest cleanup completed!"
    echo "   Fresh stanza created, no backup data to clear"
    exit 0
fi

# Stop pgBackRest operations
echo "1️⃣ Stopping pgBackRest operations..."
sudo -u postgres pgbackrest --stanza=gaia-test stop || true

# Expire all existing backups
echo "2️⃣ Expiring all existing backups..."
BACKUPS=$(sudo -u postgres pgbackrest --stanza=gaia-test info --output=json 2>/dev/null | jq -r '.[0].backup[]?.label // empty' || echo "")
if [ ! -z "$BACKUPS" ]; then
    for backup in $BACKUPS; do
        echo "   Expiring backup: $backup"
        sudo -u postgres pgbackrest --stanza=gaia-test expire --set=$backup || true
    done
    echo "   ✅ All backups expired"
else
    echo "   ℹ️ No backups found to expire"
fi

# Clear WAL archives (optional - be careful!)
echo "3️⃣ Clearing WAL archives..."
sudo -u postgres pgbackrest --stanza=gaia-test expire --repo1-retention-archive=1 || true

# Restart pgBackRest operations
echo "4️⃣ Starting pgBackRest operations..."
sudo -u postgres pgbackrest --stanza=gaia-test start || true

echo ""
echo "✅ pgBackRest cleanup completed!"
echo "   Stanza preserved, all backup data cleared"
echo "   Next database drop/recreate will be truly clean"
echo ""
