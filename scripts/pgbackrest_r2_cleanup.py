#!/usr/bin/env python3
"""
Utility to list and remove pgBackRest resumable (partial) backup labels
stored in an S3-compatible repository (e.g., Cloudflare R2), using
credentials and settings read from /etc/pgbackrest/pgbackrest.conf.

Default behavior is read-only: it will list candidate labels that have
the marker file backup.manifest.copy. Pass --delete to perform deletion.

Examples:
  List partial labels for stanza:
    scripts/pgbackrest_r2_cleanup.py --stanza gaia-finney --list

  Delete a specific label:
    scripts/pgbackrest_r2_cleanup.py --stanza gaia-finney \
      --label 20251021-183245F --delete

  Scan and delete all partial labels in a stanza:
    scripts/pgbackrest_r2_cleanup.py --stanza gaia-finney --delete
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
except Exception as exc:  # pragma: no cover
    print("boto3/botocore is required in the active environment: {}".format(exc), file=sys.stderr)
    sys.exit(2)


@dataclass
class RepoConfig:
    bucket: str
    endpoint: str
    access_key: str
    secret_key: str
    repo_path: str = "/pgbackrest"
    region: str = "us-east-1"


PG_BACKREST_CONF_DEFAULT = "/etc/pgbackrest/pgbackrest.conf"


def parse_pgbackrest_conf(conf_path: str) -> Tuple[RepoConfig, List[str]]:
    """Parse minimal S3 repo and stanza info from pgBackRest config.

    Returns (repo_config, stanza_names)
    """
    if not os.path.isfile(conf_path):
        raise FileNotFoundError(conf_path)

    bucket = endpoint = access_key = secret_key = None
    repo_path = "/pgbackrest"
    region = "us-east-1"
    stanzas: List[str] = []

    section_re = re.compile(r"^\[(?P<name>[^\]]+)\]\s*$")
    current_section: Optional[str] = None

    with open(conf_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            sec = section_re.match(line)
            if sec:
                current_section = sec.group("name")
                # Heuristic: stanza sections are non-global and not repo definitions
                if current_section not in ("global",):
                    stanzas.append(current_section)
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip().lower()
            value = value.strip().strip("'\"")
            if key.startswith("repo") and "-s3-bucket" in key:
                bucket = value
            elif key.startswith("repo") and "-s3-endpoint" in key:
                endpoint = value
            elif key.startswith("repo") and key.endswith("-s3-key-secret"):
                secret_key = value
            elif key.startswith("repo") and key.endswith("-s3-key"):
                access_key = value
            elif key.startswith("repo") and key.endswith("-path"):
                repo_path = value or repo_path
            elif key.startswith("repo") and key.endswith("-s3-region"):
                if value and value.lower() != "auto":
                    region = value

    missing = [
        n
        for (n, v) in [
            ("bucket", bucket),
            ("endpoint", endpoint),
            ("access_key", access_key),
            ("secret_key", secret_key),
        ]
        if not v
    ]
    if missing:
        raise RuntimeError("Missing repo settings in {}: {}".format(conf_path, ", ".join(missing)))

    # Normalize endpoint to include scheme
    if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        endpoint = "https://" + endpoint

    repo_cfg = RepoConfig(
        bucket=bucket,
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        repo_path=repo_path or "/pgbackrest",
        region=region or "us-east-1",
    )

    # De-duplicate stanza names while preserving order
    seen = set()
    uniq_stanzas: List[str] = []
    for s in stanzas:
        if s not in seen:
            uniq_stanzas.append(s)
            seen.add(s)

    return repo_cfg, uniq_stanzas


def s3_client(cfg: RepoConfig):
    # Cloudflare R2 is S3-compatible; v4 sig and virtual addressing is fine
    session_cfg = Config(signature_version="s3v4", s3={"addressing_style": "virtual"})
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name=cfg.region,
        config=session_cfg,
    )


def join_key(*parts: str) -> str:
    sanitized = []
    for p in parts:
        if p is None:
            continue
        q = p.strip("/")
        if q:
            sanitized.append(q)
    return "/".join(sanitized)


def list_partial_labels(s3, cfg: RepoConfig, stanza: str) -> List[str]:
    """Return labels that appear to be partial: have backup.manifest.copy but not backup.manifest."""
    prefix = join_key(cfg.repo_path, "backup", stanza) + "/"
    paginator = s3.get_paginator("list_objects_v2")
    with_copy: set[str] = set()
    with_manifest: set[str] = set()
    try:
        for page in paginator.paginate(Bucket=cfg.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key", "")
                if not key.startswith(prefix):
                    continue
                parts = key.split("/")
                try:
                    idx = parts.index(stanza) + 1
                    label = parts[idx]
                except Exception:
                    continue
                if key.endswith("/backup.manifest.copy"):
                    with_copy.add(label)
                elif key.endswith("/backup.manifest"):
                    with_manifest.add(label)
    except ClientError as e:  # pragma: no cover
        raise RuntimeError(f"S3 list failed: {e}")
    candidates = sorted(l for l in with_copy if l not in with_manifest)
    return candidates


def delete_prefix(s3, cfg: RepoConfig, stanza: str, label: str) -> Tuple[int, int]:
    """Delete all objects under the label prefix. Returns (listed, deleted)."""
    prefix = join_key(cfg.repo_path, "backup", stanza, label)
    paginator = s3.get_paginator("list_objects_v2")
    listed = 0
    deleted = 0
    for page in paginator.paginate(Bucket=cfg.bucket, Prefix=prefix):
        objs = page.get("Contents", [])
        listed += len(objs)
        if not objs:
            continue
        batch = []
        for o in objs:
            batch.append({"Key": o["Key"]})
            if len(batch) == 1000:
                s3.delete_objects(Bucket=cfg.bucket, Delete={"Objects": batch, "Quiet": True})
                deleted += len(batch)
                batch = []
        if batch:
            s3.delete_objects(Bucket=cfg.bucket, Delete={"Objects": batch, "Quiet": True})
            deleted += len(batch)
    return listed, deleted


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Remove pgBackRest resumable backups from S3-compatible repo (e.g., R2)")
    ap.add_argument("--config", default=PG_BACKREST_CONF_DEFAULT, help="Path to pgbackrest.conf")
    ap.add_argument("--stanza", help="Stanza name (defaults to first non-global stanza in config)")
    ap.add_argument("--label", help="Specific backup label to delete (e.g., 20251021-183245F)")
    ap.add_argument("--list", action="store_true", help="List partial labels and exit")
    ap.add_argument("--delete", action="store_true", help="Perform deletion (otherwise dry-run)")

    args = ap.parse_args(argv)

    cfg, stanzas = parse_pgbackrest_conf(args.config)
    stanza = args.stanza or (stanzas[0] if stanzas else None)
    if not stanza:
        print("No stanza specified and none found in config", file=sys.stderr)
        return 2

    client = s3_client(cfg)

    # Determine target labels
    target_labels: List[str]
    if args.label:
        target_labels = [args.label]
    else:
        target_labels = list_partial_labels(client, cfg, stanza)

    result = {
        "bucket": cfg.bucket,
        "endpoint": cfg.endpoint,
        "repo_path": cfg.repo_path,
        "stanza": stanza,
        "labels": target_labels,
        "action": "delete" if args.delete else "list",
        "deleted": [],
    }

    if args.list and not args.delete:
        print(json.dumps(result, indent=2))
        return 0

    if not target_labels:
        print(json.dumps(result, indent=2))
        return 0

    # Execute deletions (or dry-run info when --delete is set without --list)
    for label in target_labels:
        listed, deleted = delete_prefix(client, cfg, stanza, label)
        result["deleted"].append({"label": label, "listed": listed, "deleted": deleted})

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())


