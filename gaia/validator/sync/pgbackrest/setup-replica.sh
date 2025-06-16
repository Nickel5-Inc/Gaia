#!/bin/bash

# Gaia Validator pgBackRest Replica Node Setup Script
set -euo pipefail

# Colors and logging
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

LOG_DIR="/var/log/gaia-pgbackrest"
LOG_FILE="$LOG_DIR/setup-replica-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$LOG_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
info() { echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1" | tee -a "$LOG_FILE"; }
error() { echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"; }

# Check arguments
if [[ $# -lt 1 ]]; then
    error "Usage: $0 <PRIMARY_NODE_IP>"
    exit 1
fi

PRIMARY_IP="$1"

# Check root
if [[ $EUID -ne 0 ]]; then
   error "This script must be run as root (use sudo)"
   exit 1
fi

info "Starting Gaia Validator pgBackRest Replica Node Setup"
info "Primary node IP: $PRIMARY_IP"

# Load environment from main validator .env file
ENV_FILE=".env"
if [[ ! -f "$ENV_FILE" ]]; then
    # Try alternative paths
    if [[ -f "/root/Gaia/.env" ]]; then
        ENV_FILE="/root/Gaia/.env"
    elif [[ -f "../../.env" ]]; then
        ENV_FILE="../../.env"
    else
        error "Environment file not found. Please ensure .env file exists with PGBACKREST_ variables configured"
        exit 1
    fi
fi

info "Loading environment from: $ENV_FILE"
set -a; source "$ENV_FILE"; set +a

# Map PGBACKREST_ prefixed variables to the expected names
R2_BUCKET="${PGBACKREST_R2_BUCKET}"
R2_ENDPOINT="${PGBACKREST_R2_ENDPOINT}"
R2_ACCESS_KEY_ID="${PGBACKREST_R2_ACCESS_KEY_ID}"
R2_SECRET_ACCESS_KEY="${PGBACKREST_R2_SECRET_ACCESS_KEY}"
R2_REGION="${PGBACKREST_R2_REGION:-auto}"

STANZA_NAME="${PGBACKREST_STANZA_NAME:-gaia}"
PGDATA="${PGBACKREST_PGDATA:-/var/lib/postgresql/data}"
PGPORT="${PGBACKREST_PGPORT:-5432}"
PGUSER="${PGBACKREST_PGUSER:-postgres}"
PRIMARY_HOST="${1:-}" # First argument to script

# Validate required variables
if [[ -z "$R2_BUCKET" ]] || [[ -z "$R2_ENDPOINT" ]] || [[ -z "$R2_ACCESS_KEY_ID" ]] || [[ -z "$R2_SECRET_ACCESS_KEY" ]]; then
    error "Missing required pgBackRest R2 environment variables. Please configure:"
    error "- PGBACKREST_R2_BUCKET"
    error "- PGBACKREST_R2_ENDPOINT"
    error "- PGBACKREST_R2_ACCESS_KEY_ID"
    error "- PGBACKREST_R2_SECRET_ACCESS_KEY"
    exit 1
fi
if [[ -z "$PRIMARY_HOST" ]]; then
    error "Missing primary host IP/hostname. Usage: $0 <primary_host>"
    exit 1
fi

info "Configuration loaded:"
info "- R2 Bucket: $R2_BUCKET"
info "- R2 Endpoint: $R2_ENDPOINT"
info "- Stanza Name: $STANZA_NAME"
info "- PostgreSQL Data Dir: $PGDATA"
info "- Primary Host: $PRIMARY_HOST"

# Install dependencies
info "Installing pgBackRest..."
apt-get update && apt-get install -y pgbackrest postgresql-client

# Stop PostgreSQL
info "Stopping PostgreSQL..."
systemctl stop postgresql

# Backup existing data directory
if [[ -d "$PGDATA" ]] && [[ "$(ls -A $PGDATA)" ]]; then
    info "Backing up existing PostgreSQL data..."
    mv "$PGDATA" "$PGDATA.backup.$(date +%Y%m%d-%H%M%S)"
fi

# Create new data directory
mkdir -p "$PGDATA"
chown postgres:postgres "$PGDATA"

# Configure pgBackRest
info "Configuring pgBackRest..."
mkdir -p /var/log/pgbackrest /var/lib/pgbackrest /etc/pgbackrest
chown -R postgres:postgres /var/log/pgbackrest /var/lib/pgbackrest

cat > /etc/pgbackrest/pgbackrest.conf << EOF
[global]
repo1-type=s3
repo1-s3-bucket=$R2_BUCKET
repo1-s3-endpoint=$R2_ENDPOINT
repo1-s3-key=$R2_ACCESS_KEY_ID
repo1-s3-key-secret=$R2_SECRET_ACCESS_KEY
repo1-s3-region=$R2_REGION
repo1-path=/pgbackrest
log-level-console=info
log-level-file=debug

[$STANZA_NAME]
pg1-path=$PGDATA
pg1-port=$PGPORT
pg1-user=$PGUSER
pg1-host=$PRIMARY_HOST
pg1-host-user=postgres
EOF

# Restore from backup
info "Restoring database from pgBackRest backup..."
sudo -u postgres pgbackrest --stanza="$STANZA_NAME" restore

# Configure replica-specific settings
info "Configuring replica settings..."

# Update postgresql.conf for replica
cat >> "$PGDATA/postgresql.conf" << EOF

# Replica-specific configuration
hot_standby = on
primary_conninfo = 'host=$PRIMARY_HOST port=$PGPORT user=$PGUSER application_name=replica_$(hostname)'
restore_command = 'pgbackrest --stanza=$STANZA_NAME archive-get %f %p'
recovery_target_timeline = 'latest'
EOF

# Create recovery.signal file (PostgreSQL 12+)
info "Creating recovery configuration..."
touch "$PGDATA/standby.signal"
chown postgres:postgres "$PGDATA/standby.signal"

# Start PostgreSQL
info "Starting PostgreSQL in standby mode..."
systemctl start postgresql
systemctl enable postgresql

# Wait for PostgreSQL to start
sleep 10

# Check if PostgreSQL is running
if systemctl is-active --quiet postgresql; then
    success "PostgreSQL started successfully in standby mode"
else
    error "Failed to start PostgreSQL. Check logs: journalctl -u postgresql"
    exit 1
fi

# Test replication connection
info "Testing replication connection to primary..."
if sudo -u postgres pg_isready -h "$PRIMARY_HOST" -p "$PGPORT" &>> "$LOG_FILE"; then
    success "Connection to primary node successful"
else
    warn "Could not connect to primary node. Check network connectivity and firewall rules."
fi

# Check replication status
info "Checking replication status..."
sudo -u postgres psql -c "SELECT pg_is_in_recovery();" || warn "Could not check recovery status"

# Setup monitoring
info "Setting up monitoring..."
cat > /usr/local/bin/gaia-replica-monitor << 'EOF'
#!/bin/bash

STANZA_NAME="${STANZA_NAME:-gaia}"
LOG_FILE="/var/log/gaia-pgbackrest/replica-monitor.log"

echo "[$(date)] === Replica Monitoring Report ===" | tee -a "$LOG_FILE"

echo "Recovery Status:" | tee -a "$LOG_FILE"
sudo -u postgres psql -c "SELECT pg_is_in_recovery();" | tee -a "$LOG_FILE"

echo "Replication Lag:" | tee -a "$LOG_FILE"
sudo -u postgres psql -c "SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() ELSE NULL END AS synced;" | tee -a "$LOG_FILE"

echo "Last WAL Received:" | tee -a "$LOG_FILE"
sudo -u postgres psql -c "SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn();" | tee -a "$LOG_FILE"

echo "pgBackRest Status:" | tee -a "$LOG_FILE"
sudo -u postgres pgbackrest --stanza="$STANZA_NAME" info | tee -a "$LOG_FILE"

echo "=======================================" | tee -a "$LOG_FILE"
EOF

chmod +x /usr/local/bin/gaia-replica-monitor

# Add monitoring to cron
(crontab -l 2>/dev/null | grep -v "gaia-replica-monitor"; echo "*/15 * * * * /usr/local/bin/gaia-replica-monitor") | crontab -

success "Replica node setup completed!"
echo ""
echo "=============================================================================="
echo "REPLICA SETUP SUMMARY"
echo "=============================================================================="
echo "Primary Host: $PRIMARY_HOST"
echo "Stanza Name: $STANZA_NAME"
echo "Data Directory: $PGDATA"
echo "Log File: $LOG_FILE"
echo ""
echo "VERIFICATION STEPS:"
echo "1. Check replication status: sudo -u postgres psql -c \"SELECT pg_is_in_recovery();\""
echo "2. Monitor replication lag: /usr/local/bin/gaia-replica-monitor"
echo "3. Check logs: tail -f /var/log/pgbackrest/"
echo ""
echo "The replica is now syncing with the primary node via WAL streaming."
echo "==============================================================================" 