#!/bin/bash

# Default values for flags
SERVER_NAME="example.com"
SERVER_IP="127.0.0.1"
PORT="33333"

# Function to display usage
usage() {
    echo "Usage: $0 --server_name <server_name> --ip <ip_address> --port <port>"
    echo "Example: $0 --server_name example.com --ip 192.168.1.100 --port 33333"
    exit 1
}

# Parse flags using getopt
OPTS=$(getopt -o '' -l server_name:,ip:,port: -- "$@")
if [ $? != 0 ]; then usage; fi

eval set -- "$OPTS"
while true; do
    case "$1" in
        --server_name)
            SERVER_NAME="$2"; shift 2 ;;
        --ip)
            SERVER_IP="$2"; shift 2 ;;
        --port)
            PORT="$2"; shift 2 ;;
        --)
            shift; break ;;
        *)
            usage ;;
    esac
done

# Check if IP address is provided
if [[ -z "$SERVER_IP" ]]; then
    echo "Error: IP address is required."
    usage
fi

# Install NGINX if not already installed
if ! command -v nginx &> /dev/null; then
    sudo apt update && sudo apt install -y nginx || { echo "Failed to install NGINX"; exit 1; }
fi

# Create SSL directory
sudo mkdir -p /etc/nginx/ssl

# Generate self-signed certificate if it doesn't exist
if [ ! -f /etc/nginx/ssl/nginx.crt ]; then
    sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout /etc/nginx/ssl/nginx.key \
        -out /etc/nginx/ssl/nginx.crt \
        -subj "/C=US/ST=State/L=City/O=Organization/CN=$SERVER_NAME"
fi

# Create or update the NGINX config file for this port
CONFIG_FILE="/etc/nginx/sites-available/validator-miner-${PORT}"

# Create NGINX config for this specific port
sudo bash -c "cat > ${CONFIG_FILE} << 'EOF'
# Server block for port ${PORT}
server {
    listen ${PORT} ssl;
    listen [::]:${PORT} ssl;
    server_name ${SERVER_NAME};

    ssl_certificate /etc/nginx/ssl/nginx.crt;
    ssl_certificate_key /etc/nginx/ssl/nginx.key;

    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384';
    ssl_prefer_server_ciphers off;

    location / {
        proxy_pass http://127.0.0.1:$((${PORT} + 1));
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF"

# Enable the configuration for this port
sudo ln -sf ${CONFIG_FILE} /etc/nginx/sites-enabled/

# Remove any conflicting configurations
sudo rm -f /etc/nginx/sites-enabled/default

# Kill any process using the specified port
sudo fuser -k "${PORT}"/tcp 2>/dev/null || true

# Make sure NGINX is running and then reload configuration
echo "Starting NGINX service..."
sudo systemctl start nginx || { echo "Failed to start NGINX"; exit 1; }

echo "Testing NGINX configuration..."
if sudo nginx -t; then
    echo "NGINX configuration test passed. Reloading NGINX..."
    sudo systemctl reload nginx || sudo systemctl restart nginx
else
    echo "NGINX configuration test failed!"
    exit 1
fi

echo "NGINX setup complete! Server available on $SERVER_NAME with IP $SERVER_IP at port $PORT"
echo "Forwarding to port $((PORT + 1))"