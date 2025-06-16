#!/bin/bash

# Gaia Validator pgBackRest Diagnostic Script
set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "=============================================================================="
echo "                    Gaia Validator pgBackRest Diagnostics"
echo "=============================================================================="
echo "Timestamp: $(date)"

# Load environment
ENV_FILE="/etc/gaia/pgbackrest.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a; source "$ENV_FILE"; set +a
    echo "Environment loaded from: $ENV_FILE"
else
    echo -e "${YELLOW}⚠ Environment file not found: $ENV_FILE${NC}"
fi

STANZA_NAME="${STANZA_NAME:-gaia}"
echo "Stanza: $STANZA_NAME"
echo ""

# System Information
echo -e "${BLUE}=== SYSTEM INFORMATION ===${NC}"
echo "OS: $(lsb_release -d 2>/dev/null | cut -f2 || echo "Unknown")"
echo "Kernel: $(uname -r)"
echo "Hostname: $(hostname)"
echo "Date: $(date)"
echo ""

# Package Versions
echo -e "${BLUE}=== PACKAGE VERSIONS ===${NC}"
echo "PostgreSQL: $(psql --version 2>/dev/null || echo "Not installed")"
echo "pgBackRest: $(pgbackrest version 2>/dev/null || echo "Not installed")"
echo ""

# Service Status
echo -e "${BLUE}=== SERVICE STATUS ===${NC}"
echo "PostgreSQL Service:"
systemctl status postgresql --no-pager -l || echo "Service not found"
echo ""

# Configuration Files
echo -e "${BLUE}=== CONFIGURATION FILES ===${NC}"

echo "Environment Configuration ($ENV_FILE):"
if [[ -f "$ENV_FILE" ]]; then
    echo -e "${GREEN}✓ Found${NC}"
    echo "Key variables:"
    grep -E "^(PGBACKREST_R2_BUCKET|PGBACKREST_R2_ENDPOINT|PGBACKREST_STANZA_NAME|PGBACKREST_PGDATA)=" "$ENV_FILE" 2>/dev/null | sed 's/PGBACKREST_R2_SECRET_ACCESS_KEY=.*/PGBACKREST_R2_SECRET_ACCESS_KEY=***HIDDEN***/' || echo "Could not read variables"
else
    echo -e "${RED}✗ Not found${NC}"
fi
echo ""

echo "pgBackRest Configuration (/etc/pgbackrest/pgbackrest.conf):"
if [[ -f "/etc/pgbackrest/pgbackrest.conf" ]]; then
    echo -e "${GREEN}✓ Found${NC}"
    echo "Configuration preview:"
    sed 's/repo1-s3-key-secret=.*/repo1-s3-key-secret=***HIDDEN***/' /etc/pgbackrest/pgbackrest.conf 2>/dev/null | head -20 || echo "Could not read configuration"
else
    echo -e "${RED}✗ Not found${NC}"
fi
echo ""

echo "PostgreSQL Configuration:"
POSTGRES_CONF="${PGDATA:-/var/lib/postgresql/data}/postgresql.conf"
if [[ -f "$POSTGRES_CONF" ]]; then
    echo -e "${GREEN}✓ Found: $POSTGRES_CONF${NC}"
    echo "pgBackRest-related settings:"
    grep -E "(wal_level|archive_mode|archive_command|max_wal_senders)" "$POSTGRES_CONF" 2>/dev/null || echo "No pgBackRest settings found"
else
    echo -e "${RED}✗ Not found: $POSTGRES_CONF${NC}"
fi
echo ""

echo "pg_hba.conf:"
HBA_CONF="${PGDATA:-/var/lib/postgresql/data}/pg_hba.conf"
if [[ -f "$HBA_CONF" ]]; then
    echo -e "${GREEN}✓ Found: $HBA_CONF${NC}"
    echo "Replication entries:"
    grep -E "replication.*postgres" "$HBA_CONF" 2>/dev/null || echo "No replication entries found"
else
    echo -e "${RED}✗ Not found: $HBA_CONF${NC}"
fi
echo ""

# Directory Permissions
echo -e "${BLUE}=== DIRECTORY PERMISSIONS ===${NC}"
echo "pgBackRest directories:"
for dir in "/var/log/pgbackrest" "/var/lib/pgbackrest" "/etc/pgbackrest"; do
    if [[ -d "$dir" ]]; then
        echo "$dir: $(ls -ld "$dir" 2>/dev/null || echo "Cannot read")"
    else
        echo "$dir: ${RED}Not found${NC}"
    fi
done
echo ""

echo "PostgreSQL data directory:"
PGDATA_DIR="${PGDATA:-/var/lib/postgresql/data}"
if [[ -d "$PGDATA_DIR" ]]; then
    echo "$PGDATA_DIR: $(ls -ld "$PGDATA_DIR" 2>/dev/null || echo "Cannot read")"
    echo "Contents: $(ls -la "$PGDATA_DIR" 2>/dev/null | wc -l || echo "0") items"
else
    echo "$PGDATA_DIR: ${RED}Not found${NC}"
fi
echo ""

# PostgreSQL Status
echo -e "${BLUE}=== POSTGRESQL STATUS ===${NC}"
if systemctl is-active --quiet postgresql; then
    echo -e "${GREEN}✓ PostgreSQL is running${NC}"
    
    echo "Database connections:"
    sudo -u postgres psql -c "SELECT count(*) as active_connections FROM pg_stat_activity;" 2>/dev/null || echo "Could not check connections"
    
    echo "Current WAL file:"
    sudo -u postgres psql -c "SELECT pg_current_wal_lsn();" 2>/dev/null || echo "Could not get WAL info"
    
    echo "Archive status:"
    sudo -u postgres psql -c "SELECT archived_count, failed_count FROM pg_stat_archiver;" 2>/dev/null || echo "Could not get archive stats"
    
else
    echo -e "${RED}✗ PostgreSQL is not running${NC}"
    echo "Recent PostgreSQL logs:"
    journalctl -u postgresql --no-pager -n 10 || echo "Could not retrieve logs"
fi
echo ""

# pgBackRest Connectivity
echo -e "${BLUE}=== PGBACKREST CONNECTIVITY ===${NC}"
if command -v pgbackrest &> /dev/null; then
    echo -e "${GREEN}✓ pgBackRest is installed${NC}"
    
    echo "Testing R2 connectivity via pgBackRest..."
    if sudo -u postgres pgbackrest --stanza=${PGBACKREST_STANZA_NAME:-gaia} check; then
        echo -e "${GREEN}✓ R2 connectivity and stanza check OK${NC}"
    else
        echo -e "${RED}✗ R2 connectivity or stanza check failed${NC}"
        echo "  - Verify R2 credentials in .env file (PGBACKREST_R2_...)"
        echo "  - Ensure the stanza '${PGBACKREST_STANZA_NAME:-gaia}' has been created on the primary node."
        echo "  - Check network connectivity to ${PGBACKREST_R2_ENDPOINT}"
    fi
    
    echo ""
    echo "Stanza information:"
    sudo -u postgres pgbackrest --stanza=${PGBACKREST_STANZA_NAME:-gaia} info 2>/dev/null || echo "Could not retrieve stanza info"
    
else
    echo -e "${RED}✗ pgBackRest is not installed${NC}"
fi
echo ""

# Network Connectivity
echo -e "${BLUE}=== NETWORK CONNECTIVITY ===${NC}"
if [[ -n "${PRIMARY_HOST:-}" ]] && [[ "$PRIMARY_HOST" != "primary.validator.ip" ]]; then
    echo "Testing connection to primary host: $PRIMARY_HOST"
    if ping -c 1 "$PRIMARY_HOST" &>/dev/null; then
        echo -e "${GREEN}✓ Ping to $PRIMARY_HOST successful${NC}"
    else
        echo -e "${RED}✗ Ping to $PRIMARY_HOST failed${NC}"
    fi
    
    echo "Testing PostgreSQL port:"
    if nc -z "$PRIMARY_HOST" "${PGPORT:-5432}" 2>/dev/null; then
        echo -e "${GREEN}✓ Port ${PGPORT:-5432} on $PRIMARY_HOST is open${NC}"
    else
        echo -e "${RED}✗ Port ${PGPORT:-5432} on $PRIMARY_HOST is closed${NC}"
    fi
else
    echo "No primary host configured or using default placeholder"
fi
echo ""

# Log Files
echo -e "${BLUE}=== LOG FILES ===${NC}"
echo "Recent pgBackRest logs:"
if [[ -d "/var/log/pgbackrest" ]]; then
    find /var/log/pgbackrest -name "*.log" -type f -mtime -1 -exec echo "=== {} ===" \; -exec tail -5 {} \; 2>/dev/null | head -50 || echo "No recent logs found"
else
    echo "pgBackRest log directory not found"
fi
echo ""

echo "Recent PostgreSQL logs:"
journalctl -u postgresql --no-pager -n 20 --since "1 hour ago" 2>/dev/null | tail -20 || echo "Could not retrieve PostgreSQL logs"
echo ""

# Disk Space
echo -e "${BLUE}=== DISK SPACE ===${NC}"
echo "Overall disk usage:"
df -h 2>/dev/null || echo "Could not check disk usage"
echo ""

echo "PostgreSQL WAL directory:"
WAL_DIR="${PGDATA:-/var/lib/postgresql/data}/pg_wal"
if [[ -d "$WAL_DIR" ]]; then
    echo "WAL files: $(ls -1 "$WAL_DIR"/0* 2>/dev/null | wc -l || echo "0")"
    echo "WAL directory size: $(du -sh "$WAL_DIR" 2>/dev/null | cut -f1 || echo "Unknown")"
else
    echo "WAL directory not found: $WAL_DIR"
fi
echo ""

# Firewall Status
echo -e "${BLUE}=== FIREWALL STATUS ===${NC}"
if command -v ufw &> /dev/null; then
    echo "UFW Status:"
    ufw status 2>/dev/null || echo "Could not check UFW status"
elif command -v firewall-cmd &> /dev/null; then
    echo "Firewalld Status:"
    firewall-cmd --state 2>/dev/null || echo "Could not check firewalld status"
else
    echo "No recognized firewall found"
fi
echo ""

# Summary and Recommendations
echo -e "${BLUE}=== SUMMARY AND RECOMMENDATIONS ===${NC}"

# Check for common issues
issues_found=0

if ! systemctl is-active --quiet postgresql; then
    echo -e "${RED}✗ PostgreSQL is not running${NC}"
    echo "  → Fix: sudo systemctl start postgresql"
    ((issues_found++))
fi

if [[ ! -f "/etc/pgbackrest/pgbackrest.conf" ]]; then
    echo -e "${RED}✗ pgBackRest configuration missing${NC}"
    echo "  → Fix: Run setup-primary.sh or setup-replica.sh"
    ((issues_found++))
fi

if [[ ! -f "$ENV_FILE" ]]; then
    echo -e "${RED}✗ Environment configuration missing${NC}"
    echo "  → Fix: Copy pgbackrest.env.template to $ENV_FILE and configure"
    ((issues_found++))
fi

if ! command -v pgbackrest &> /dev/null; then
    echo -e "${RED}✗ pgBackRest not installed${NC}"
    echo "  → Fix: sudo apt install pgbackrest"
    ((issues_found++))
fi

if [[ $issues_found -eq 0 ]]; then
    echo -e "${GREEN}✓ No critical issues detected${NC}"
    echo "System appears to be configured correctly."
else
    echo -e "${YELLOW}⚠ Found $issues_found issue(s) that need attention${NC}"
fi

echo ""
echo "For additional help:"
echo "- Check the README.md file"
echo "- Review setup logs in /var/log/gaia-pgbackrest/"
echo "- Monitor with: ./monitor-sync.sh"
echo "==============================================================================" 