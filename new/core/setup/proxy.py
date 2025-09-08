from __future__ import annotations

import os
import subprocess
import textwrap
from dataclasses import asdict
from typing import Any, Dict

import yaml

from new.core.config.node_config import (MinerConfig, NodeConfig, ProxyConfig,
                                         ValidatorConfig)


def _load_yaml(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _ensure_nginx_installed() -> None:
    if subprocess.call(["bash", "-lc", "command -v nginx >/dev/null 2>&1"]) != 0:
        subprocess.check_call(["bash", "-lc", "sudo apt update && sudo apt install -y nginx openssl"])  # best-effort


def _write_file(path: str, content: str, mode: int = 0o644) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def _render_self_signed_conf(server_name: str, server_ip: str) -> str:
    return textwrap.dedent(
        f"""
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
        CN={server_name}

        [req_ext]
        subjectAltName = @alt_names

        [alt_names]
        DNS.1 = {server_name}
        DNS.2 = localhost
        IP.1 = {server_ip}
        IP.2 = 127.0.0.1
        """
    ).strip()


def _ensure_self_signed_cert(server_name: str, server_ip: str) -> None:
    os.makedirs("/etc/nginx/ssl", exist_ok=True)
    conf = _render_self_signed_conf(server_name, server_ip)
    _write_file("/tmp/openssl.cnf", conf, 0o644)
    subprocess.check_call(
        [
            "bash",
            "-lc",
            "sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 "
            "-keyout /etc/nginx/ssl/nginx.key -out /etc/nginx/ssl/nginx.crt "
            "-config /tmp/openssl.cnf -extensions req_ext",
        ]
    )
    os.remove("/tmp/openssl.cnf")
    subprocess.check_call(["bash", "-lc", "sudo chmod 644 /etc/nginx/ssl/nginx.crt && sudo chmod 600 /etc/nginx/ssl/nginx.key"])  # nosec


def _render_site(server_name: str, server_ip: str, public_port: int, forwarding_port: int, *, ssl_cert_path: str, ssl_key_path: str) -> str:
    return textwrap.dedent(
        f"""
        server {{
            listen {public_port} ssl;
            listen [::]:{public_port} ssl;
            server_name {server_name} {server_ip};

            ssl_certificate {ssl_cert_path};
            ssl_certificate_key {ssl_key_path};

            ssl_protocols TLSv1.2 TLSv1.3;
            ssl_ciphers 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384';
            ssl_prefer_server_ciphers off;
            client_max_body_size 100M;

            location / {{
                proxy_pass http://127.0.0.1:{forwarding_port};
                proxy_http_version 1.1;
                proxy_set_header Host $host;
                proxy_set_header X-Real-IP $remote_addr;
                proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
                proxy_set_header X-Forwarded-Proto $scheme;
            }}
        }}
        """
    ).strip()


def ensure_proxy_config(config_path: str) -> None:
    """Ensures nginx TLS proxy is configured based on node YAML config.

    The YAML should contain a top-level `role` and a `proxy` section. For miners, an optional `hosting` section can be present.
    This function is idempotent and safe to call on each boot.
    """
    cfg = _load_yaml(config_path)
    role = cfg.get("role")
    proxy_cfg = cfg.get("proxy") or {}
    if not proxy_cfg or not proxy_cfg.get("enabled", True):
        return

    proxy = ProxyConfig(
        enabled=True,
        server_name=proxy_cfg.get("server_name", "example.com"),
        server_ip=proxy_cfg.get("server_ip", "127.0.0.1"),
        public_port=int(proxy_cfg.get("public_port", 33333)),
        forwarding_port=int(proxy_cfg.get("forwarding_port", 33334)),
        tls_mode=proxy_cfg.get("tls_mode", "self_signed"),
    )

    _ensure_nginx_installed()

    ssl_cert_path = "/etc/nginx/ssl/nginx.crt"
    ssl_key_path = "/etc/nginx/ssl/nginx.key"
    if proxy.tls_mode == "self_signed":
        _ensure_self_signed_cert(proxy.server_name, proxy.server_ip)
    elif proxy.tls_mode == "letsencrypt":
        ssl_cert_path, ssl_key_path = _ensure_letsencrypt_cert(cfg)
    elif proxy.tls_mode == "off":
        # No TLS site configured; exit early.
        return

    site_name = f"validator-miner-{proxy.public_port}"
    site_path = f"/etc/nginx/sites-available/{site_name}"
    site_enabled = f"/etc/nginx/sites-enabled/{site_name}"
    _write_file(site_path, _render_site(proxy.server_name, proxy.server_ip, proxy.public_port, proxy.forwarding_port, ssl_cert_path=ssl_cert_path, ssl_key_path=ssl_key_path))
    if os.path.islink(site_enabled):
        os.remove(site_enabled)
    os.symlink(site_path, site_enabled)

    # Remove default if present
    default_site = "/etc/nginx/sites-enabled/default"
    if os.path.exists(default_site):
        try:
            os.remove(default_site)
        except Exception:
            pass

    # Start/reload nginx
    subprocess.call(["bash", "-lc", "sudo systemctl start nginx"])  # best-effort
    subprocess.check_call(["bash", "-lc", "sudo nginx -t"])  # ensure config valid
    subprocess.call(["bash", "-lc", "sudo systemctl reload nginx || sudo systemctl restart nginx"])  # best-effort


def _render_acme_http_site(server_name: str, webroot: str) -> str:
    return textwrap.dedent(
        f"""
        server {{
            listen 80;
            listen [::]:80;
            server_name {server_name};

            location /.well-known/acme-challenge/ {{
                root {webroot};
            }}
        }}
        """
    ).strip()


def _ensure_letsencrypt_cert(cfg: Dict[str, Any]) -> tuple[str, str]:
    proxy = cfg.get("proxy") or {}
    server_name = proxy.get("server_name")
    email = proxy.get("email")
    challenge = (proxy.get("challenge") or "http-01").lower()
    if not server_name or not email:
        raise RuntimeError("letsencrypt requires proxy.server_name and proxy.email")

    # Install certbot
    subprocess.check_call(["bash", "-lc", "sudo apt update && sudo apt install -y certbot"])  # nosec

    cert_dir = f"/etc/letsencrypt/live/{server_name}"

    if challenge == "http-01":
        # Prepare webroot and ACME port 80 site
        webroot = "/var/www/certbot"
        os.makedirs(webroot, exist_ok=True)

        site_name = f"acme-{server_name}"
        site_path = f"/etc/nginx/sites-available/{site_name}"
        site_enabled = f"/etc/nginx/sites-enabled/{site_name}"
        _write_file(site_path, _render_acme_http_site(server_name, webroot))
        if os.path.islink(site_enabled):
            os.remove(site_enabled)
        os.symlink(site_path, site_enabled)

        subprocess.call(["bash", "-lc", "sudo systemctl start nginx"])  # best-effort
        subprocess.check_call(["bash", "-lc", "sudo nginx -t"])  # ensure config valid
        subprocess.call(["bash", "-lc", "sudo systemctl reload nginx || sudo systemctl restart nginx"])  # best-effort

        # Issue certificate using webroot
        cmd = f"sudo certbot certonly --webroot -w {webroot} -d {server_name} -m {email} --agree-tos --non-interactive"
        subprocess.check_call(["bash", "-lc", cmd])
    elif challenge == "dns-01":
        provider = proxy.get("dns_provider")
        if not provider:
            raise RuntimeError("dns-01 requires proxy.dns_provider and provider credentials in environment")
        # Install a common provider plugin example (cloudflare). Users can adjust per provider.
        if provider == "cloudflare":
            subprocess.check_call(["bash", "-lc", "sudo apt install -y python3-certbot-dns-cloudflare"])  # nosec
            # Expects CLOUDFLARE_API_TOKEN in environment and an ini file path; keep minimal by env usage with certbot >=2.6
            cmd = f"sudo certbot certonly --dns-cloudflare -d {server_name} -m {email} --agree-tos --non-interactive"
            subprocess.check_call(["bash", "-lc", cmd])
            # After obtaining certs, write nginx ssl_certificate paths to /etc/letsencrypt/live/{server_name}/
            # And reload; but --nginx automation is simpler, so http-01 is preferred.
        else:
            raise RuntimeError(f"Unsupported dns provider: {provider}")
    else:
        raise RuntimeError(f"Unknown challenge: {challenge}")

    # Systemd timers auto-renew; add a deploy hook to reload nginx on renew
    _write_file(
        "/etc/letsencrypt/renewal-hooks/deploy/reload-nginx.sh",
        "#!/bin/sh\nsudo systemctl reload nginx || sudo systemctl restart nginx\n",
        0o755,
    )

    return (f"{cert_dir}/fullchain.pem", f"{cert_dir}/privkey.pem")


