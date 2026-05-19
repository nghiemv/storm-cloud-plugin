"""Host-invoked CLI — runs inside the plugin image so the host only needs
Python + Docker.

Invoked from run.py via ``docker run --rm ... IMAGE python3.12 -m plugin.cli <subcmd>``.

Subcommands:
    list-payloads          stdout: "<uuid>\\t<iso-mtime>" per line, newest first.
    upload-batch <DIR>     each <DIR>/<jobname>/compute-manifest.json is staged to
                           s3://$CC_AWS_S3_BUCKET/manifests/<uuid>/payload.

All S3 wiring reads CC_AWS_* from the environment (forwarded by run.py).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ["CC_AWS_ENDPOINT"],
        aws_access_key_id=os.environ["CC_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["CC_AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("CC_AWS_DEFAULT_REGION", "us-east-1"),
    )


def _cmd_list_payloads() -> int:
    bucket = os.environ["CC_AWS_S3_BUCKET"]
    prefix = f"{os.environ.get('CC_ROOT', 'manifests')}/"
    s3 = _s3_client()
    payloads: dict[str, str] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        for obj in page.get("Contents") or []:
            # CC convention: keys look like <root>/<uuid>/payload
            rel = obj["Key"][len(prefix) :]
            parts = rel.split("/", 2)
            if len(parts) >= 2 and parts[1] == "payload":
                mtime = obj.get("LastModified")
                payloads[parts[0]] = mtime.isoformat() if mtime else ""
    for uuid, mtime in sorted(payloads.items(), key=lambda x: x[1], reverse=True):
        print(f"{uuid}\t{mtime}")
    return 0


def _cmd_upload_batch(batch_dir: str) -> int:
    s3 = _s3_client()
    bucket = os.environ["CC_AWS_S3_BUCKET"]
    root = Path(batch_dir)
    if not root.is_dir():
        print(f"error: batch dir not found: {batch_dir}", file=sys.stderr)
        return 1
    n = 0
    for sub in sorted(root.iterdir()):
        if not sub.is_dir():
            continue
        manifest = sub / "compute-manifest.json"
        if not manifest.is_file():
            continue
        try:
            data = json.loads(manifest.read_text())
        except json.JSONDecodeError as e:
            print(f"skipping {manifest}: {e}", file=sys.stderr)
            continue
        uuid = data.get("uuid") or data.get("id") or sub.name
        key = f"manifests/{uuid}/payload"
        print(f"  {sub.name} -> {key}")
        s3.upload_file(str(manifest), bucket, key)
        n += 1
    if n == 0:
        print(f"no jobs found in {batch_dir}", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    cmd, args = sys.argv[1], sys.argv[2:]
    if cmd == "list-payloads" and not args:
        return _cmd_list_payloads()
    if cmd == "upload-batch" and len(args) == 1:
        return _cmd_upload_batch(args[0])
    print(f"unknown command or wrong arg count: {cmd} {args}", file=sys.stderr)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
