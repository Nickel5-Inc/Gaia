### Miner runtime (Axon-as-adapter)

- We expose a single forward entry, conceptually like a single FastAPI route.
- `ForwardRouter` dispatches by `request.kind` (e.g., `"weather.forward"`).
- Axon adapts `bt.Synapse` ⇄ `ForwardRequest`/`ForwardResponse` and preserves signing/nonce protections.

### What you’ll set up

- TLS proxy (nginx) in front of Axon for HTTPS.
- Optional static file hosting (for self-hosted Zarr/Kerchunk data) over HTTPS.
- Miner handlers registered on the `ForwardRouter`.

### Prerequisites

- Ubuntu/Debian host with sudo.
- Python 3.10+.
- .env for any secrets (leave YAML free of secrets):
  - For S3/R2/MinIO: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or MinIO equivalents.

### Configure

- Edit `new/config/miner.config.yaml`:

```yaml
role: miner
proxy:
  enabled: true
  server_name: miner.example.com     # public DNS or IP
  server_ip: 127.0.0.1               # IP SAN for self-signed
  public_port: 44333                 # HTTPS port clients call
  forwarding_port: 44334             # local Axon port (HTTP)
  tls_mode: self_signed              # self_signed | letsencrypt | off
hosting:
  mode: static                       # static | s3 | r2 | minio
  root_path: /var/data/zarr          # required if mode=static
  # static_public_port: 44343        # optional, defaults to proxy.public_port+10
  # endpoint: https://s3.amazonaws.com
  # bucket: my-bucket
  # region: us-east-1
```

Notes

- `proxy` is auto-configured at boot: installs nginx, creates a self-signed cert (if selected), and proxies HTTPS `public_port` → local `forwarding_port`.
- If `proxy.tls_mode: letsencrypt`, issuance is automated with certbot (HTTP-01 by default). Requires public DNS and inbound port 80.
- If `hosting.mode: static`, an additional HTTPS virtual host serves files from `root_path` (use for self-hosted Zarr/Kerchunk).

### Start the miner

```python
from new.boot.loader import BootConfig, boot

boot(BootConfig(role="miner", config_path="/absolute/path/to/new/config/miner.config.yaml"))
```

What happens

- TLS proxy is ensured (self-signed by default).
- Optional static hosting vhost is created when `hosting.mode: static`.
- Axon is started and bound to `forwarding_port`; nginx publishes it over HTTPS at `public_port`.

### Public endpoint URL

- `https://<server_name>:<public_port>/` is the Axon HTTPS endpoint.
  - Example: `https://miner.example.com:44333/`

### Register handlers

- Handlers live in `new/miner/router/forward.py`. Register new kinds via the router:
  - `router.register("weather.forward", handle_weather_forward)`
  - Keep handlers async and idempotent.

### Hosting options

- Static (self-hosted): place files under `hosting.root_path`. URLs map to paths; clients should use presigned/signed URLs you return via Synapse.
- S3/R2/MinIO: store objects in a bucket and return short‑lived presigned HTTPS URLs via Synapse.
  - Always include `sha256` and `size` alongside each URL so validators can verify bytes.

### Troubleshooting

- Nginx config test: `sudo nginx -t`
- Reload nginx: `sudo systemctl reload nginx`
- Logs: `/var/log/nginx/error.log`, `/var/log/nginx/access.log`
- If the cert CN/SAN mismatches your hostname/IP, clients may warn; self-signed is fine for validator↔miner in controlled envs.
- For DNS-01 automation (behind NAT/no port 80), set `proxy.challenge: dns-01` and `proxy.dns_provider` (e.g., cloudflare). Provide provider API token via environment.
