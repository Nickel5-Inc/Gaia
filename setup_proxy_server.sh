#!/bin/bash

# Check if IP address is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <ip-address>"
    echo "Example: $0 192.168.1.100"
    exit 1
fi

SERVER_IP="$1"

# Install NGINX if not already installed
sudo apt update
sudo apt install -y nginx

# Create SSL directory
sudo mkdir -p /etc/nginx/ssl

# Generate self-signed certificate if it doesn't exist
if [ ! -f /etc/nginx/ssl/nginx.crt ]; then
    sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout /etc/nginx/ssl/nginx.key \
        -out /etc/nginx/ssl/nginx.crt \
        -subj "/C=US/ST=State/L=City/O=Organization/CN=$SERVER_IP"
fi

# Create NGINX config with properly escaped variables
sudo bash -c "cat > /etc/nginx/sites-available/validator-miner << 'EOF'
server {
    listen 80;
    server_name $SERVER_IP;
    return 301 https://\$server_name\$request_uri;
}

server {
    listen 443 ssl;
    server_name $SERVER_IP;

    ssl_certificate /etc/nginx/ssl/nginx.crt;
    ssl_certificate_key /etc/nginx/ssl/nginx.key;

    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384;
    ssl_prefer_server_ciphers off;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}

server {
    listen 33333 ssl;
    server_name $SERVER_IP;

    ssl_certificate /etc/nginx/ssl/nginx.crt;
    ssl_certificate_key /etc/nginx/ssl/nginx.key;

    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384;
    ssl_prefer_server_ciphers off;

    location / {
        proxy_pass http://127.0.0.1:33334;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF"

# Enable the configuration
sudo ln -sf /etc/nginx/sites-available/validator-miner /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default

# Kill any process using our ports
sudo fuser -k 80/tcp 2>/dev/null || true
sudo fuser -k 443/tcp 2>/dev/null || true
sudo fuser -k 33333/tcp 2>/dev/null || true

# Stop NGINX if it's running
sudo systemctl stop nginx

# Test and restart NGINX
sudo nginx -t && sudo systemctl restart nginx

echo "NGINX setup complete! Both services are now accessible via HTTPS."
