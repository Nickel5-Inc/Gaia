from __future__ import annotations

import os
import subprocess
import textwrap
from typing import Any, Dict, Tuple

import yaml


def _load_yaml(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _write_file(path: str, content: str, mode: int = 0o644) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def _render_static_site(server_name: str, server_ip: str, public_port: int, root_path: str, *, ssl_cert_path: str, ssl_key_path: str) -> str:
    return textwrap.dedent(
        f"""
        server {{
            listen {public_port} ssl;
            listen [::]:{public_port} ssl;
            server_name {server_name} {server_ip};

            ssl_certificate {ssl_cert_path};
            ssl_certificate_key {ssl_key_path};

            ssl_protocols TLSv1.2 TLSv1.3;
            ssl_prefer_server_ciphers off;
            client_max_body_size 100M;

            etag on;
            add_header Accept-Ranges bytes;

            location / {{
                root {root_path};
                autoindex off;
                try_files $uri =404;
            }}
        }}
        """
    ).strip()


def _ssl_paths_for_site(cfg: Dict[str, Any]) -> Tuple[str, str]:
    proxy = cfg.get("proxy") or {}
    tls_mode = (proxy.get("tls_mode") or "self_signed").lower()
    if tls_mode == "letsencrypt":
        server_name = proxy.get("server_name")
        if not server_name:
            raise RuntimeError("letsencrypt requires proxy.server_name for static site")
        cert_dir = f"/etc/letsencrypt/live/{server_name}"
        return (f"{cert_dir}/fullchain.pem", f"{cert_dir}/privkey.pem")
    # default to self-signed paths
    return ("/etc/nginx/ssl/nginx.crt", "/etc/nginx/ssl/nginx.key")


def ensure_hosting_config(config_path: str) -> None:
    """Configures additional static hosting if miner selects hosting.mode=static.

    Creates a separate TLS server on `hosting.static_public_port` (or proxy.public_port+10) serving `hosting.root_path`.
    Other hosting modes (s3/r2/minio) are control-plane only and require no local Nginx changes.
    """
    cfg = _load_yaml(config_path)
    role = cfg.get("role")
    if role != "miner":
        return
    hosting = cfg.get("hosting") or {}
    mode = (hosting.get("mode") or "static").lower()
    if mode != "static":
        return

    root_path = hosting.get("root_path")
    if not root_path:
        return
    os.makedirs(root_path, exist_ok=True)

    proxy = cfg.get("proxy") or {}
    server_name = proxy.get("server_name", "example.com")
    server_ip = proxy.get("server_ip", "127.0.0.1")
    base_public = int(proxy.get("public_port", 33333))
    static_port = int(hosting.get("static_public_port", base_public + 10))

    site_name = f"miner-static-{static_port}"
    site_path = f"/etc/nginx/sites-available/{site_name}"
    site_enabled = f"/etc/nginx/sites-enabled/{site_name}"

    cert_path, key_path = _ssl_paths_for_site(cfg)
    _write_file(site_path, _render_static_site(server_name, server_ip, static_port, root_path, ssl_cert_path=cert_path, ssl_key_path=key_path))
    if os.path.islink(site_enabled):
        os.remove(site_enabled)
    os.symlink(site_path, site_enabled)

    subprocess.call(["bash", "-lc", "sudo systemctl start nginx"])  # best-effort
    subprocess.check_call(["bash", "-lc", "sudo nginx -t"])  # ensure config valid
    subprocess.call(["bash", "-lc", "sudo systemctl reload nginx || sudo systemctl restart nginx"])  # best-effort


