#!/bin/bash
echo "Starting Gaia Validator Setup..."

echo "Performing thorough cleanup of existing processes..."
pkill -f "prefect"
pkill -f "python"
pkill -f "dask"
pkill -f "validator.py"
sleep 3

pkill -9 -f "prefect" 2>/dev/null || true
pkill -9 -f "python" 2>/dev/null || true
pkill -9 -f "dask" 2>/dev/null || true
pkill -9 -f "validator.py" 2>/dev/null || true
rm -f /tmp/prefect* 2>/dev/null || true
rm -f /tmp/dask* 2>/dev/null || true
fuser -k 4200/tcp 2>/dev/null || true
lsof -t -i:4200 | xargs -r kill -9 2>/dev/null || true
sleep 2

echo "Setting up Nginx reverse proxy..."
if [ ! -f /etc/nginx/.htpasswd ]; then
    echo "Checking and installing required packages..."
    apt install -y nginx apache2-utils
    echo "Creating new authentication credentials..."
    ADMIN_PASS=$(hostname)
    echo "admin:$(openssl passwd -apr1 $ADMIN_PASS)" > /etc/nginx/.htpasswd
    echo "Prefect UI credentials:"
    echo "Username: admin"
    echo "Password: $ADMIN_PASS"
    echo "Please save these credentials!"
    echo "-----------------------------------"
else
    echo "Using existing authentication credentials"
fi

cat > /etc/nginx/sites-available/prefect << EOL
server {
    listen 80;
    server_name _;
    location /api/events/in {
        proxy_pass http://127.0.0.1:4200;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_read_timeout 86400;  # 24h timeout for long-running connections
    }
    location / {
        proxy_pass http://127.0.0.1:4200;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }
    auth_basic "Restricted Access";
    auth_basic_user_file /etc/nginx/.htpasswd;
}
EOL

ln -sf /etc/nginx/sites-available/prefect /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl restart nginx

PUBLIC_IP=$(curl -s ifconfig.me)
echo "Prefect UI is proxied through Nginx and available at: http://$PUBLIC_IP"
echo "Setting up environment..."
cd /root/Gaia
source /root/venv/bin/activate

if [[ "$VIRTUAL_ENV" != "/root/venv" ]]; then
    echo "Failed to activate virtual environment"
    exit 1
fi

echo "Initializing validator configuration..."

if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

export SUBTENSOR_CHAIN_ENDPOINT="$SUBTENSOR_ADDRESS"
export PREFECT_API_URL="http://127.0.0.1:4200/api"
export PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/prefect_db"
export PREFECT_WORKER_PREFETCH_SECONDS=30
export PREFECT_DATABASE_QUERY_BATCH_SIZE=1000
export PREFECT_LOGGING_LEVEL="WARNING"
export PREFECT_DATABASE_CONNECTION_TIMEOUT=60
export PREFECT_API_KEEPALIVE_TIMEOUT=300
export PREFECT_API_REQUEST_TIMEOUT=300
export PREFECT_TASK_RUNNER_CLASS="prefect_dask.DaskTaskRunner"
export PYTHONPATH="${PYTHONPATH}:/root/Gaia"
export DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES=10
export DASK_DISTRIBUTED__WORKER__MULTIPROCESSING_METHOD="fork"

echo "Using configuration:"
echo "SUBTENSOR_CHAIN_ENDPOINT: $SUBTENSOR_CHAIN_ENDPOINT"
echo "NETUID: $NETUID"
echo "WALLET_NAME: $WALLET_NAME"
echo "HOTKEY_NAME: $HOTKEY_NAME"
echo "SUBTENSOR_NETWORK: $SUBTENSOR_NETWORK"

PGPASSWORD=postgres psql -U postgres -h localhost -c "CREATE DATABASE prefect_db;" 2>/dev/null || true

# Start Prefect server
echo "Starting Prefect server..."
prefect server start --host 127.0.0.1 &
PREFECT_PID=$!

echo "Waiting for Prefect API server to start..."
until curl -s -f http://127.0.0.1:4200/api/health > /dev/null 2>&1; do
    if ! kill -0 $PREFECT_PID 2>/dev/null; then
        echo "Error: Prefect server failed to start"
        exit 1
    fi
    echo -n "."
    sleep 1
done
echo " Ready!"

prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

echo "Creating work pool..."
prefect work-pool create default --type process --overwrite || true

echo "Starting Prefect worker..."
prefect worker start -p default &
WORKER_PID=$!

echo "Deploying flows..."
python -m gaia.scheduling.apply_deployments

echo "Verifying deployments..."
MAX_RETRIES=5
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    echo "Checking deployments (attempt $((RETRY_COUNT+1))/$MAX_RETRIES)..."
    DEPLOYMENTS=$(prefect deployment ls)
    if echo "$DEPLOYMENTS" | grep -q "geomagnetic-validator" && echo "$DEPLOYMENTS" | grep -q "soil-validator"; then
        echo "Deployments successfully created!"
        break
    fi
    echo "Waiting for deployments to be registered..."
    sleep 10
    RETRY_COUNT=$((RETRY_COUNT+1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "Error: Failed to verify deployment creation"
    echo "Current deployments:"
    prefect deployment ls
    exit 1
fi

echo "Starting validator..."
python gaia/validator/validator.py \
    --wallet $WALLET_NAME \
    --hotkey $HOTKEY_NAME \
    --netuid $NETUID \
    --chain_endpoint $SUBTENSOR_CHAIN_ENDPOINT \
    --network $SUBTENSOR_NETWORK \
    #--test &
VALIDATOR_PID=$!
sleep 5

echo "Waiting 30 seconds for Prefect deployments to be registered..."
sleep 30

echo "Deploying flows..."
python -m gaia.scheduling.apply_deployments

check_process() {
    if ! kill -0 $1 2>/dev/null; then
        echo "Process $2 (PID: $1) has died!"
        cleanup
    fi
}

cleanup() {
    echo "Cleaning up..."
    kill -TERM $VALIDATOR_PID 2>/dev/null || true
    kill -TERM $PREFECT_PID 2>/dev/null || true
    kill -TERM $WORKER_PID 2>/dev/null || true
    pkill -f "prefect"
    pkill -f "dask"
    exit 0
}

trap cleanup SIGINT SIGTERM
echo "Setup complete! Validator is running continuously with the following schedule:"
echo "- Core validation: Every 5 minutes"
echo "- Scoring: Every 10 minutes"
echo "- Task processing: Every 15 minutes"
echo "- Monitoring: Every 5 minutes"
echo "- Soil validation: Every 5 minutes"
echo "- Geomagnetic validation: Every hour"
echo ""
echo "Press Ctrl+C to stop all processes."

while true; do
    check_process $VALIDATOR_PID "Validator"
    check_process $PREFECT_PID "Prefect server"
    check_process $WORKER_PID "Prefect worker"
    sleep 30
done 