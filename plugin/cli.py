"""Host-invoked CLI — runs inside the plugin image so the host only needs
Python + Docker.

Invoked from run.py via ``docker run --rm ... IMAGE python3.12 -m plugin.cli <subcmd>``.

Subcommands:
    list-payloads          stdout: JSON array of payload records, newest first.
                           Each record has: uuid, mtime, and (when the payload
                           body parses cleanly) catalog_id, catalog_description,
                           start_date, end_date, storm_duration, top_n_events.
    upload-batch <DIR>     each <DIR>/<jobname>/compute-manifest.json is staged
                           to s3://$CC_AWS_S3_BUCKET/manifests/<uuid>/payload.
    mirror [--year-start Y] [--year-end Y] [--dest-base URI] [--dry-run]
                           Mirror NOAA AORC zarr years into a private cache.
                           Reads from MIRROR_AWS_* env. Skips years already
                           present (checks for .zmetadata).

All S3 wiring reads CC_AWS_* from the environment (forwarded by run.py).
"""

from __future__ import annotations

import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
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


_PAYLOAD_ATTRS = (
    "catalog_id",
    "catalog_description",
    "start_date",
    "end_date",
    "storm_duration",
    "top_n_events",
)


def _cmd_list_payloads() -> int:
    bucket = os.environ["CC_AWS_S3_BUCKET"]
    prefix = f"{os.environ.get('CC_ROOT', 'manifests')}/"
    s3 = _s3_client()

    # Enumerate UUIDs via list_objects_v2 (cheap).
    found: dict[str, str] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        for obj in page.get("Contents") or []:
            # CC convention: keys look like <root>/<uuid>/payload
            rel = obj["Key"][len(prefix) :]
            parts = rel.split("/", 2)
            if len(parts) >= 2 and parts[1] == "payload":
                mtime = obj.get("LastModified")
                found[parts[0]] = mtime.isoformat() if mtime else ""

    # Fan-out GetObject calls in parallel so listing 50 payloads doesn't
    # serialize to 50 × round-trip-time. boto3 clients are thread-safe.
    def fetch(uuid: str) -> dict:
        rec: dict = {"uuid": uuid, "mtime": found[uuid]}
        try:
            obj = s3.get_object(Bucket=bucket, Key=f"{prefix}{uuid}/payload")
            attrs = json.loads(obj["Body"].read()).get("attributes") or {}
            for key in _PAYLOAD_ATTRS:
                rec[key] = attrs.get(key, "")
        except Exception as e:
            rec["error"] = str(e)
        return rec

    records: list[dict] = []
    if found:
        with ThreadPoolExecutor(max_workers=8) as pool:
            records = list(pool.map(fetch, found.keys()))
        records.sort(key=lambda r: r.get("mtime", ""), reverse=True)

    json.dump(records, sys.stdout)
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


def _cmd_mirror(argv: list[str]) -> int:
    """Mirror NOAA AORC zarr years to a private cache.

    Runs inside the plugin image (s3fs + xarray are baked in), so the host
    needs only Python + Docker. Reads ``MIRROR_AWS_ACCESS_KEY_ID`` /
    ``MIRROR_AWS_SECRET_ACCESS_KEY`` / ``MIRROR_AWS_ENDPOINT`` for the
    write target. Source is NOAA's public bucket (anonymous).

    The image-wide default scheduler is ``synchronous`` (set in Dockerfile
    to cap memory during the plugin's process-storms scan loop). For bulk
    mirroring we override to ``threads`` so dask reads/writes many zarr
    chunks in parallel — without this, sustained throughput is ~440 KB/s
    (one chunk RTT at a time) instead of tens of MB/s.
    """
    import argparse

    # Must be set BEFORE importing dask-using libs so the global scheduler
    # picks it up.
    os.environ["DASK_SCHEDULER"] = os.environ.get(
        "MIRROR_DASK_SCHEDULER", "threads"
    )

    import dask
    import s3fs
    import xarray as xr

    dask.config.set(scheduler=os.environ["DASK_SCHEDULER"])

    LON_MIN, LON_MAX = -97.2, -77.4
    LAT_MIN, LAT_MAX = 33.3, 44.1
    VARIABLES = ["APCP_surface", "TMP_2maboveground"]
    NOAA_BASE = "s3://noaa-nws-aorc-v1-1-1km"

    p = argparse.ArgumentParser(prog="plugin.cli mirror")
    p.add_argument("--year-start", type=int, default=1979)
    p.add_argument("--year-end", type=int, default=2024)
    p.add_argument("--dest-base", default="s3://storm-cloud/aorc-cache")
    p.add_argument("--dry-run", action="store_true")
    opts = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("mirror")

    key = os.environ["MIRROR_AWS_ACCESS_KEY_ID"]
    secret = os.environ["MIRROR_AWS_SECRET_ACCESS_KEY"]
    endpoint = os.environ.get("MIRROR_AWS_ENDPOINT", "https://s3.hecdev.net")

    src_fs = s3fs.S3FileSystem(anon=True, config_kwargs={"max_pool_connections": 50})
    dst_fs = s3fs.S3FileSystem(
        anon=False,
        key=key,
        secret=secret,
        endpoint_url=endpoint,
        config_kwargs={"max_pool_connections": 20},
    )

    def year_done(dest_path: str) -> bool:
        return dst_fs.exists(dest_path.replace("s3://", "") + "/.zmetadata")

    failed: list[int] = []
    for year in range(opts.year_start, opts.year_end + 1):
        src_path = f"{NOAA_BASE}/{year}.zarr"
        dst_path = f"{opts.dest_base}/{year}.zarr"
        try:
            if year_done(dst_path):
                log.info("[%d] already cached — skip", year)
                continue
            log.info("[%d] opening %s", year, src_path)
            ds = xr.open_dataset(
                s3fs.S3Map(root=src_path, s3=src_fs, check=False),
                engine="zarr",
                chunks="auto",
                consolidated=True,
            )
            ds = ds[VARIABLES].sel(
                longitude=slice(LON_MIN, LON_MAX),
                latitude=slice(LAT_MIN, LAT_MAX),
            )
            # Keep source-aligned chunking so to_zarr streams chunk-to-chunk
            # instead of buffering the whole year. NOAA uses (144, 128, 256);
            # rechunking to anything else (e.g. time=24, lat=full) forces
            # dask to read all source chunks before the first write — that's
            # what made the previous attempt sit at ~470 KB/s. We still pop
            # the encoded chunks so safe_chunks doesn't trip on a phantom
            # post-clip mismatch.
            ds = ds.chunk({"time": 144, "latitude": 128, "longitude": 256})
            for var in ds.data_vars:
                ds[var].encoding.pop("chunks", None)
            for coord in ds.coords:
                ds[coord].encoding.pop("chunks", None)
            log.info(
                "[%d] clipped: time=%d lat=%d lon=%d",
                year,
                ds.sizes.get("time", 0),
                ds.sizes.get("latitude", 0),
                ds.sizes.get("longitude", 0),
            )
            if opts.dry_run:
                log.info("[%d] dry-run — skip write", year)
                continue
            log.info("[%d] writing to %s", year, dst_path)
            ds.to_zarr(
                s3fs.S3Map(root=dst_path, s3=dst_fs, check=False),
                mode="w",
                consolidated=True,
                safe_chunks=False,
            )
            log.info("[%d] done", year)
        except Exception:
            log.exception("[%d] failed", year)
            failed.append(year)

    if failed:
        log.error("failed years: %s", failed)
        return 1
    log.info("mirror complete (%d–%d)", opts.year_start, opts.year_end)
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
    if cmd == "mirror":
        return _cmd_mirror(args)
    print(f"unknown command or wrong arg count: {cmd} {args}", file=sys.stderr)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
