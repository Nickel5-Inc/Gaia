#!/bin/bash

# Gaia Validator pgBackRest Synchronization Monitor
set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Load environment if available
ENV_FILE=".env"
if [[ -f "$ENV_FILE" ]]; then
    set -a; source "$ENV_FILE"; set +a
elif [[ -f "/root/Gaia/.env" ]]; then
    set -a; source "/root/Gaia/.env"; set +a
elif [[ -f "../../.env" ]]; then
    set -a; source "../../.env"; set +a
fi

STANZA_NAME="${PGBACKREST_STANZA_NAME:-gaia}"

echo "=============================================================================="
echo "                    Gaia Validator Database Sync Monitor"
echo "=============================================================================="
echo "Timestamp: $(date)"
echo "Stanza: $STANZA_NAME"
echo ""

# Check if PostgreSQL is running
echo -e "${BLUE}PostgreSQL Status:${NC}"
if systemctl is-active --quiet postgresql; then
    echo -e "${GREEN}✓ PostgreSQL is running${NC}"
else
    echo -e "${RED}✗ PostgreSQL is not running${NC}"
    exit 1
fi

# Check if this is a primary or replica
echo ""
echo -e "${BLUE}Node Type:${NC}"
if sudo -u postgres psql -t -c "SELECT pg_is_in_recovery();" 2>/dev/null | grep -q "f"; then
    NODE_TYPE="PRIMARY"
    echo -e "${GREEN}✓ Primary Node${NC}"
elif sudo -u postgres psql -t -c "SELECT pg_is_in_recovery();" 2>/dev/null | grep -q "t"; then
    NODE_TYPE="REPLICA"
    echo -e "${YELLOW}✓ Replica Node${NC}"
else
    NODE_TYPE="UNKNOWN"
    echo -e "${RED}✗ Unknown node type${NC}"
fi

# WAL Archiving Status (Primary only)
if [[ "$NODE_TYPE" == "PRIMARY" ]]; then
    echo ""
    echo -e "${BLUE}WAL Archiving Status:${NC}"
    
    # Check archive command
    ARCHIVE_COMMAND=$(sudo -u postgres psql -t -c "SHOW archive_command;" 2>/dev/null | xargs)
    if [[ "$ARCHIVE_COMMAND" == *"pgbackrest"* ]]; then
        echo -e "${GREEN}✓ Archive command configured: $ARCHIVE_COMMAND${NC}"
    else
        echo -e "${RED}✗ Archive command not configured properly: $ARCHIVE_COMMAND${NC}"
    fi
    
    # Check archiver stats
    echo ""
    echo "Archive Statistics:"
    sudo -u postgres psql -c "SELECT archived_count, last_archived_wal, last_archived_time, failed_count, last_failed_wal, last_failed_time FROM pg_stat_archiver;" 2>/dev/null || echo "Could not retrieve archiver stats"
    
    # Check replication connections
    echo ""
    echo "Replication Connections:"
    sudo -u postgres psql -c "SELECT application_name, client_addr, state, sync_state FROM pg_stat_replication;" 2>/dev/null || echo "No replication connections"
fi

# Replication Status (Replica only)
if [[ "$NODE_TYPE" == "REPLICA" ]]; then
    echo ""
    echo -e "${BLUE}Replication Status:${NC}"
    
    # Check if receiving WAL
    echo "WAL Receive/Replay Status:"
    sudo -u postgres psql -c "SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() AS synced;" 2>/dev/null || echo "Could not retrieve WAL status"
    
    # Check replication lag
    echo ""
    echo "Replication Lag:"
    sudo -u postgres psql -c "SELECT CASE WHEN pg_is_in_recovery() THEN EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::INT || ' seconds' ELSE 'Not in recovery' END AS lag;" 2>/dev/null || echo "Could not calculate lag"
fi

# pgBackRest Status
echo ""
echo -e "${BLUE}pgBackRest Status:${NC}"
if command -v pgbackrest &> /dev/null; then
    echo -e "${GREEN}✓ pgBackRest is installed${NC}"
    
    # Check stanza info
    echo ""
    echo "Backup Information:"
    if sudo -u postgres pgbackrest --stanza="$STANZA_NAME" info 2>/dev/null; then
        echo -e "${GREEN}✓ pgBackRest stanza accessible${NC}"
        check_status_code=0
        # Stanza check successful, can proceed with more detailed checks
        # Check last backup time
        last_backup=$(sudo -u postgres pgbackrest info --output=json | jq -r ".[] | .backup | .[] | select(.type == \"full\" or .type == \"diff\") | .timestamp.stop" | sort -n | tail -1)
        if [[ -n "$last_backup" ]]; then
            last_backup_date=$(date -d @$last_backup)
            echo "Last full/diff backup completed at: $last_backup_date"
        else
            echo -e "${YELLOW}⚠ Could not determine last backup time.${NC}"
        fi
    else
        echo -e "${RED}✗ pgBackRest stanza not accessible or R2 connection failed${NC}"
        check_status_code=1
    fi
else
    echo -e "${RED}✗ pgBackRest is not installed${NC}"
fi

# Disk Usage
echo ""
echo -e "${BLUE}Disk Usage:${NC}"
echo "PostgreSQL Data Directory:"
df -h "${PGDATA:-/var/lib/postgresql/data}" 2>/dev/null || echo "Could not check disk usage"

echo ""
echo "WAL Directory:"
if [[ -d "${PGDATA:-/var/lib/postgresql/data}/pg_wal" ]]; then
    du -sh "${PGDATA:-/var/lib/postgresql/data}/pg_wal" 2>/dev/null || echo "Could not check WAL directory size"
else
    echo "WAL directory not found"
fi

# Recent Log Entries
echo ""
echo -e "${BLUE}Recent pgBackRest Logs:${NC}"
if [[ -d "/var/log/pgbackrest" ]]; then
    echo "Last 5 log entries:"
    find /var/log/pgbackrest -name "*.log" -type f -exec tail -n 1 {} + 2>/dev/null | tail -5 || echo "No recent log entries found"
else
    echo "pgBackRest log directory not found"
fi

# Summary
echo ""
echo "=============================================================================="
echo -e "${BLUE}SUMMARY:${NC}"

if [[ "$NODE_TYPE" == "PRIMARY" ]]; then
    echo "This node is configured as the PRIMARY database source."
    echo "Ensure WAL archiving is working and replica nodes can connect."
elif [[ "$NODE_TYPE" == "REPLICA" ]]; then
    echo "This node is configured as a REPLICA."
    echo "Ensure replication lag is minimal and WAL replay is current."
else
    echo "Node type could not be determined. Check PostgreSQL configuration."
fi

echo ""
echo "For detailed monitoring, check:"
echo "- PostgreSQL logs: journalctl -u postgresql"
echo "- pgBackRest logs: /var/log/pgbackrest/"
echo "- Gaia setup logs: /var/log/gaia-pgbackrest/"
echo "=============================================================================="

# --- Summary ---
echo -e "\n${BLUE}--- Sync Status Summary ---${NC}"
if [[ $check_status_code -eq 0 ]]; then
    echo -e "${GREEN}✓ DB Sync appears HEALTHY${NC}"
else
    echo -e "${RED}✗ DB Sync appears UNHEALTHY${NC}"
fi

exit $check_status_code 