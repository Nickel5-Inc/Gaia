### Validator runtime (Dendrite-as-adapter)

- `DendriteClientAdapter` sends `ForwardRequest` to miner Axon HTTPS endpoints and returns `ForwardResponse`.
- Business logic sees simple request/response; SDK details are hidden.

### What you’ll set up

- TLS proxy (nginx) in front of any local Axon you might run (validators can also serve, but not required to query).
- A static list of miner endpoints (HTTPS URLs) for querying.

### Configure

- Edit `new/config/validator.config.yaml`:

```yaml
role: validator
proxy:
  enabled: true
  server_name: validator.example.com
  server_ip: 127.0.0.1
  public_port: 33333
  forwarding_port: 33334
  tls_mode: self_signed
```

Notes

- Proxy is auto-configured at boot (self-signed supported). If you don’t serve an Axon from the validator, the proxy can remain unused.
- If `proxy.tls_mode: letsencrypt`, issuance is automated with certbot (HTTP-01). Requires public DNS and inbound port 80.
- Miner endpoints are not discovered dynamically; you provide them.

### Start the validator

```python
from new.boot.loader import BootConfig, boot

boot(BootConfig(role="validator", config_path="/absolute/path/to/new/config/validator.config.yaml"))
```

### Query miners

```python
from new.shared.networking.endpoint import Endpoint
from new.shared.networking.protocol import ForwardRequest
from new.validator.validator import query_miners
from new.core.utils.config import Config

endpoints = [
    Endpoint(url="https://miner.example.com:44333/"),
    # ...more miners
]
config = Config()
responses = await query_miners(endpoints, kind="weather.forward", payload={"x": 1}, config=config)
```

Best practices

- Use HTTPS endpoints only; avoid leaking presigned URLs or payloads over HTTP.
- Batch requests where possible; keep payloads small and include integrity fields for large data plans.
