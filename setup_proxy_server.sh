#!/bin/bash

# Default values for flags
SERVER_NAME="example.com"
SERVER_IP="127.0.0.1"
PORT="33333"
FORWARDING_PORT=""  # Will be set to PORT + 1 if not specified

# Function to display usage
usage() {
    echo "Usage: $0 --server_name <server_name> --ip <ip_address> --port <port> [--forwarding_port <forwarding_port>]"
    echo "Example: $0 --server_name example.com --ip 192.168.1.100 --port 33333 --forwarding_port 33334"
    exit 1
}

# Parse flags using getopt
OPTS=$(getopt -o '' -l server_name:,ip:,port:,forwarding_port: -- "$@")
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
        --forwarding_port)
            FORWARDING_PORT="$2"; shift 2 ;;
        --)
            shift; break ;;
        *)
            usage ;;
    esac
done

# Set default forwarding port if not specified
if [[ -z "$FORWARDING_PORT" ]]; then
    FORWARDING_PORT=$((PORT + 1))
fi

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

# Create OpenSSL config file with IP SANs
cat > /tmp/openssl.cnf << EOF
[req]
default_bits = 2048
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[dn]
C=US
ST=State
L=City
O=Organization
CN=${SERVER_NAME}

[req_ext]
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${SERVER_NAME}
DNS.2 = localhost
IP.1 = ${SERVER_IP}
IP.2 = 127.0.0.1
EOF

# Generate self-signed certificate with IP SANs
sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout /etc/nginx/ssl/nginx.key \
    -out /etc/nginx/ssl/nginx.crt \
    -config /tmp/openssl.cnf \
    -extensions req_ext

# Remove temporary OpenSSL config
rm /tmp/openssl.cnf

# Set proper permissions for the certificate files
sudo chmod 644 /etc/nginx/ssl/nginx.crt
sudo chmod 600 /etc/nginx/ssl/nginx.key

# Create NGINX config file for this port
CONFIG_FILE="/etc/nginx/sites-available/validator-miner-${PORT}"

# Create NGINX config for this specific port
sudo bash -c "cat > ${CONFIG_FILE} << 'EOF'
# Server block for port ${PORT}
server {
    listen ${PORT} ssl;
    listen [::]:${PORT} ssl;
    server_name ${SERVER_NAME} ${SERVER_IP};

    ssl_certificate /etc/nginx/ssl/nginx.crt;
    ssl_certificate_key /etc/nginx/ssl/nginx.key;

    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384';
    ssl_prefer_server_ciphers off;
    client_max_body_size 100M;

    location / {
        proxy_pass http://127.0.0.1:${FORWARDING_PORT};
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
echo "Forwarding to port $FORWARDING_PORT"