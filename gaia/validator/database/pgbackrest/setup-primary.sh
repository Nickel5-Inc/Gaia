# Gaia Validator pgBackRest Primary Node Setup Script
set -euo pipefail

# Colors and logging
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

LOG_DIR="/var/log/gaia-pgbackrest"
LOG_FILE="$LOG_DIR/setup-primary-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$LOG_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
info() { echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1" | tee -a "$LOG_FILE"; }
error() { echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"; }

# Check root
if [[ $EUID -ne 0 ]]; then
   error "This script must be run as root (use sudo)"
   exit 1
fi

info "Starting Gaia Validator pgBackRest Primary Node Setup"

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
AZURE_STORAGE_ACCOUNT="${PGBACKREST_AZURE_STORAGE_ACCOUNT}"
AZURE_STORAGE_KEY="${PGBACKREST_AZURE_STORAGE_KEY}"
AZURE_CONTAINER="${PGBACKREST_AZURE_CONTAINER}"
STANZA_NAME="${PGBACKREST_STANZA_NAME:-gaia}"
PGDATA="${PGBACKREST_PGDATA:-/var/lib/postgresql/data}"
PGPORT="${PGBACKREST_PGPORT:-5432}"
PGUSER="${PGBACKREST_PGUSER:-postgres}"

# Validate required variables
if [[ -z "$AZURE_STORAGE_ACCOUNT" ]] || [[ -z "$AZURE_STORAGE_KEY" ]] || [[ -z "$AZURE_CONTAINER" ]]; then
    error "Missing required pgBackRest environment variables. Please configure:"
    error "- PGBACKREST_AZURE_STORAGE_ACCOUNT"
    error "- PGBACKREST_AZURE_STORAGE_KEY"
    error "- PGBACKREST_AZURE_CONTAINER"
    exit 1
fi

info "Configuration loaded:"
info "- Azure Storage Account: $AZURE_STORAGE_ACCOUNT"
info "- Azure Container: $AZURE_CONTAINER"
info "- Stanza Name: $STANZA_NAME"
info "- PostgreSQL Data Dir: $PGDATA"

# Install dependencies
info "Installing pgBackRest..."
apt-get update && apt-get install -y pgbackrest postgresql-client

# Configure PostgreSQL
POSTGRES_CONF="$PGDATA/postgresql.conf"
HBA_CONF="$PGDATA/pg_hba.conf"

info "Backing up PostgreSQL configuration..."
[[ -f "$POSTGRES_CONF" ]] && cp "$POSTGRES_CONF" "$POSTGRES_CONF.backup.$(date +%Y%m%d-%H%M%S)"
[[ -f "$HBA_CONF" ]] && cp "$HBA_CONF" "$HBA_CONF.backup.$(date +%Y%m%d-%H%M%S)"

info "Updating postgresql.conf..."
cat >> "$POSTGRES_CONF" << EOF

# pgBackRest Configuration
wal_level = replica
archive_mode = on
archive_command = 'pgbackrest --stanza=$STANZA_NAME archive-push %p'
archive_timeout = 60
max_wal_senders = 10
wal_keep_size = 2GB
hot_standby = on
listen_addresses = '*'
max_connections = 200
log_checkpoints = on
EOF

info "Updating pg_hba.conf..."
cat >> "$HBA_CONF" << EOF

# pgBackRest replication
local   replication     postgres                                peer
host    replication     postgres        127.0.0.1/32            trust
host    all             postgres        10.0.0.0/8              md5
host    replication     postgres        10.0.0.0/8              md5
EOF

# Configure pgBackRest
info "Configuring pgBackRest..."
mkdir -p /var/log/pgbackrest /var/lib/pgbackrest /etc/pgbackrest
chown -R postgres:postgres /var/log/pgbackrest /var/lib/pgbackrest

cat > /etc/pgbackrest/pgbackrest.conf << EOF
[global]
repo1-type=azure
repo1-azure-account=$AZURE_STORAGE_ACCOUNT
repo1-azure-container=$AZURE_CONTAINER
repo1-azure-key=$AZURE_STORAGE_KEY
repo1-path=/pgbackrest
repo1-retention-full=7
repo1-retention-diff=2
process-max=4
log-level-console=info
log-level-file=debug

[$STANZA_NAME]
pg1-path=$PGDATA
pg1-port=$PGPORT
pg1-user=$PGUSER
EOF

# Restart PostgreSQL
info "Restarting PostgreSQL..."
systemctl restart postgresql
systemctl enable postgresql

# Initialize pgBackRest
info "Creating pgBackRest stanza..."
sudo -u postgres pgbackrest --stanza="$STANZA_NAME" stanza-create

info "Taking initial backup..."
sudo -u postgres pgbackrest --stanza="$STANZA_NAME" backup --type=full

# Setup cron jobs
info "Setting up backup schedule..."
crontab -u postgres -l 2>/dev/null | grep -v "pgbackrest" > /tmp/postgres_cron || true
cat >> /tmp/postgres_cron << EOF
0 2 * * 0 pgbackrest --stanza=$STANZA_NAME backup --type=full
0 2 * * 1-6 pgbackrest --stanza=$STANZA_NAME backup --type=diff
0 * * * * pgbackrest --stanza=$STANZA_NAME check
EOF
crontab -u postgres /tmp/postgres_cron && rm /tmp/postgres_cron

success "Primary node setup completed!"
echo "Log file: $LOG_FILE"
echo "Next: Set up replica nodes with ./setup-replica.sh <primary_ip>" 
#!/bin/bash
 