#!/usr/bin/env python3
"""storm-cloud-plugin task runner.

One entry point for every workflow against this plugin.

Invocation:
    Linux / macOS:        ./run.py <cmd>
    Windows (cmd / PS):   python run.py <cmd>
    Anywhere portable:    python run.py <cmd>

Usage:
    run.py                  Smoke run with compute/smoke/fixtures/payload.json
    run.py PAYLOAD          Smoke run with a custom payload
    run.py hec              List payloads on HEC S3 and pick one interactively
    run.py hec list         Just list payloads (UUID + timestamp, tab-separated)
    run.py hec UUID [NAME]  Run a specific payload (NAME = output subdir name)
    run.py batch [DIR]      Multi-job HEC S3 driver (DIR holds one subdir per job)
    run.py build            docker build the plugin image
    run.py mirror [args]    One-shot AORC zarr mirror (NOAA -> private S3 cache)
    run.py lint             ruff check + format check
    run.py format           ruff format
    run.py freeze           Regenerate compute/constraints.txt from a built image
    run.py down             docker compose down
    run.py clean            Stop containers, drop volumes, clear compute/outputs/
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
COMPUTE = ROOT / "compute"
SMOKE = COMPUTE / "smoke"
HEC = COMPUTE / "hec-s3"
COMPOSE = ["docker", "compose", "-f", str(SMOKE / "compose.yaml")]
DEFAULT_PAYLOAD = SMOKE / "fixtures" / "payload.json"
IMAGE = "ghcr.io/usace/storm-cloud-plugin:latest"


def sh(args, env=None, check=True, **kwargs):
    merged = {**os.environ, **(env or {})}
    r = subprocess.run(args, env=merged, cwd=ROOT, **kwargs)
    if check and r.returncode != 0:
        sys.exit(r.returncode)
    return r


def sh_quiet(args):
    subprocess.run(
        args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ROOT
    )


# ─── smoke (local MinIO) ─────────────────────────────────────────────────────


def cmd_smoke(payload: Path = DEFAULT_PAYLOAD) -> None:
    """Run the plugin against the local MinIO stack."""
    if not payload.is_file():
        print(f"Error: payload not found: {payload}", file=sys.stderr)
        sys.exit(1)
    sh(["git", "submodule", "update", "--init"])
    sh_quiet([*COMPOSE, "down", "--remove-orphans"])
    (COMPUTE / "outputs" / "quick-test").mkdir(parents=True, exist_ok=True)
    # the seed service mounts compute/fixtures at /fixtures inside the container
    container_path = "/fixtures/" + payload.name
    print(f"Running smoke: {payload.name}")
    print("Progress streams to stdout; outputs land in compute/outputs/quick-test/\n")
    sh([*COMPOSE, "run", "--rm", "seed"], env={"PAYLOAD_FILE": container_path})
    sh([*COMPOSE, "run", "--rm", "storm-cloud-plugin"])


# ─── hec (production S3) ─────────────────────────────────────────────────────


_HEC_REQUIRED = ("CC_AWS_ACCESS_KEY_ID", "CC_AWS_SECRET_ACCESS_KEY",
                 "CC_AWS_ENDPOINT", "CC_AWS_S3_BUCKET")


def _load_hec_env() -> None:
    """Overlay compute/hec-s3/.env onto os.environ if it exists.

    Existing env vars win (so an explicit `export FOO=...` overrides the file).
    Quietly no-op if the .env file is absent — the user gets a clearer error
    from _require_hec_env() pointing at the README.
    """
    env_file = HEC / ".env"
    if not env_file.is_file():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _require_hec_env() -> None:
    _load_hec_env()
    missing = [k for k in _HEC_REQUIRED if not os.environ.get(k)]
    if missing:
        print(
            f"Error: missing HEC creds: {missing}\n"
            f"  Fix: cp compute/hec-s3/.env.example compute/hec-s3/.env, then fill it in.\n"
            f"  See compute/hec-s3/README.md.",
            file=sys.stderr,
        )
        sys.exit(1)


def _list_hec_payloads() -> list[tuple[str, str]]:
    """Return [(uuid, last_modified), ...] for payloads in s3://$BUCKET/$CC_ROOT/."""
    endpoint = os.environ["CC_AWS_ENDPOINT"]
    bucket = os.environ["CC_AWS_S3_BUCKET"]
    root = os.environ.get("CC_ROOT", "manifests")
    prefix = f"{root}/"

    r = subprocess.run(
        ["aws", "--endpoint-url", endpoint, "s3api", "list-objects-v2",
         "--bucket", bucket, "--prefix", prefix, "--output", "json"],
        capture_output=True, text=True, cwd=ROOT,
    )
    if r.returncode != 0:
        print(r.stderr, file=sys.stderr)
        sys.exit(r.returncode)

    data = json.loads(r.stdout or "{}")
    payloads: dict[str, str] = {}
    for obj in data.get("Contents") or []:
        # CC convention: keys look like <root>/<uuid>/payload
        rel = obj["Key"][len(prefix):]
        parts = rel.split("/", 2)
        if len(parts) >= 2 and parts[1] == "payload":
            payloads[parts[0]] = obj.get("LastModified", "")
    return sorted(payloads.items(), key=lambda x: x[1], reverse=True)


def _pick_hec_payload() -> str | None:
    """Print available payloads, prompt for selection. Returns chosen UUID or None."""
    payloads = _list_hec_payloads()
    bucket = os.environ["CC_AWS_S3_BUCKET"]
    root = os.environ.get("CC_ROOT", "manifests")
    if not payloads:
        print(f"No payloads found at s3://{bucket}/{root}/", file=sys.stderr)
        return None
    print(f"Payloads at s3://{bucket}/{root}/:\n")
    for i, (uuid, mtime) in enumerate(payloads, 1):
        print(f"  {i:3d}. {uuid}  ({mtime})")
    print()
    try:
        choice = input("Pick number (or paste a UUID; Enter to cancel): ").strip()
    except EOFError:
        return None
    if not choice:
        return None
    if choice.isdigit() and 1 <= int(choice) <= len(payloads):
        return payloads[int(choice) - 1][0]
    return choice  # raw UUID


def _run_hec_job(uuid: str, name: str | None = None) -> None:
    name = name or uuid
    (COMPUTE / "outputs" / name).mkdir(parents=True, exist_ok=True)
    print(f"Running HEC S3: payload={uuid} (results -> compute/outputs/{name}/)\n")
    forwarded = (
        *_HEC_REQUIRED,
        "CC_AWS_DEFAULT_REGION",
        "FFRD_AWS_ACCESS_KEY_ID", "FFRD_AWS_SECRET_ACCESS_KEY",
        "FFRD_AWS_DEFAULT_REGION", "FFRD_AWS_ENDPOINT", "FFRD_AWS_S3_BUCKET",
        "AORC_S3_BASE_URL", "AORC_S3_KEY", "AORC_S3_SECRET", "AORC_S3_ENDPOINT",
    )
    sh([
        "docker", "run", "--rm",
        "-v", f"{COMPUTE / 'outputs' / name}:/usr/src/app/Local",
        "-e", f"CC_PAYLOAD_ID={uuid}",
        "-e", f"CC_MANIFEST_ID={uuid}",
        "-e", f"CC_ROOT={os.environ.get('CC_ROOT', 'manifests')}",
        *[arg for k in forwarded if os.environ.get(k)
          for arg in ("-e", f"{k}={os.environ[k]}")],
        IMAGE,
    ])


def cmd_hec(args: list[str]) -> None:
    """Run the plugin against HEC S3.

    Usage:
      ./run.py hec              List payloads in S3 and pick one interactively
      ./run.py hec list         Just list payloads (machine-readable)
      ./run.py hec UUID [NAME]  Run that payload (NAME = output subdir name)
    """
    _require_hec_env()

    if not args:
        uuid = _pick_hec_payload()
        if uuid is None:
            return
        _run_hec_job(uuid)
        return

    if args[0] == "list":
        for uuid, mtime in _list_hec_payloads():
            print(f"{uuid}\t{mtime}")
        return

    uuid = args[0]
    name = args[1] if len(args) > 1 else uuid
    _run_hec_job(uuid, name)


# ─── build ───────────────────────────────────────────────────────────────────


def cmd_build() -> None:
    """Build the plugin image."""
    sh(["git", "submodule", "update", "--init"])
    sh([*COMPOSE, "build", "storm-cloud-plugin"])


# ─── batch (multi-job HEC driver) ─────────────────────────────────────────────


def cmd_batch(args: list[str]) -> None:
    """Multi-job HEC S3 driver.

    Reads each compute-manifest.json under DIR (default: compute/hec-s3/batch/),
    stages it to s3://$BUCKET/manifests/<uuid>/payload, then runs the plugin
    once per manifest. Set the same HEC creds as `./run.py hec`.
    """
    batch_dir = Path(args[0]) if args else HEC / "batch"
    if not batch_dir.is_dir():
        print(f"Error: batch dir not found: {batch_dir}", file=sys.stderr)
        print("Expected one subdir per job, each holding compute-manifest.json", file=sys.stderr)
        sys.exit(1)

    _require_hec_env()

    jobs = []
    for sub in sorted(batch_dir.iterdir()):
        if not sub.is_dir():
            continue
        manifest = sub / "compute-manifest.json"
        if not manifest.is_file():
            continue
        try:
            data = json.loads(manifest.read_text())
        except json.JSONDecodeError as e:
            print(f"Skipping {manifest}: {e}", file=sys.stderr)
            continue
        uuid = data.get("uuid") or data.get("id") or sub.name
        jobs.append((sub.name, uuid, manifest))

    if not jobs:
        print(f"No jobs found in {batch_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"=== Staging {len(jobs)} manifests to s3://{os.environ['CC_AWS_S3_BUCKET']}/manifests/ ===")
    endpoint = os.environ["CC_AWS_ENDPOINT"]
    for name, uuid, manifest in jobs:
        print(f"  {name} -> manifests/{uuid}/payload")
        sh([
            "aws", "--endpoint-url", endpoint,
            "s3", "cp", str(manifest),
            f"s3://{os.environ['CC_AWS_S3_BUCKET']}/manifests/{uuid}/payload",
        ])

    for name, uuid, _ in jobs:
        print(f"\n=== Starting: {name} (payload={uuid}) ===")
        _run_hec_job(uuid, name)
        print(f"=== Done: {name} ===")

    print("\nAll runs complete.")


# ─── mirror (AORC -> private S3) ──────────────────────────────────────────────


def cmd_mirror(args: list[str]) -> None:
    """Mirror AORC zarr data from NOAA public S3 to a private cache.

    One-shot. Subsequent runs skip years already mirrored. Required env:
    MIRROR_AWS_ACCESS_KEY_ID, MIRROR_AWS_SECRET_ACCESS_KEY,
    MIRROR_AWS_ENDPOINT (default: https://s3.hecdev.net).
    """
    import argparse as _ap

    LON_MIN, LON_MAX = -97.2, -77.4
    LAT_MIN, LAT_MAX = 33.3, 44.1
    VARIABLES = ["APCP_surface", "TMP_2maboveground"]
    NOAA_BASE = "s3://noaa-nws-aorc-v1-1-1km"
    YEAR_START, YEAR_END = 1979, 2024

    p = _ap.ArgumentParser(prog="./run.py mirror", description=cmd_mirror.__doc__,
                           formatter_class=_ap.RawDescriptionHelpFormatter)
    p.add_argument("--year-start", type=int, default=YEAR_START, metavar="YEAR")
    p.add_argument("--year-end", type=int, default=YEAR_END, metavar="YEAR")
    p.add_argument("--dest-base", default="s3://storm-cloud/aorc-cache", metavar="S3_URI")
    p.add_argument("--dry-run", action="store_true", help="plan without writing")
    opts = p.parse_args(args)

    _load_hec_env()  # picks up MIRROR_AWS_* if defined in compute/hec-s3/.env
    try:
        import s3fs
        import xarray as xr
    except ImportError:
        print(
            "./run.py mirror needs s3fs + xarray. Install with:\n"
            "    pip install s3fs xarray",
            file=sys.stderr,
        )
        sys.exit(1)

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
        anon=False, key=key, secret=secret, endpoint_url=endpoint,
        config_kwargs={"max_pool_connections": 20},
    )

    def year_done(dest_path):
        return dst_fs.exists(dest_path.replace("s3://", "") + "/.zmetadata")

    failed = []
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
                engine="zarr", chunks="auto", consolidated=True,
            )
            ds = ds[VARIABLES].sel(
                longitude=slice(LON_MIN, LON_MAX),
                latitude=slice(LAT_MIN, LAT_MAX),
            )
            n_lat = ds.sizes.get("latitude", 1)
            n_lon = ds.sizes.get("longitude", 1)
            ds = ds.chunk({"time": 24, "latitude": n_lat, "longitude": n_lon})
            log.info("[%d] clipped: time=%d lat=%d lon=%d",
                     year, ds.sizes.get("time", 0), n_lat, n_lon)
            if opts.dry_run:
                log.info("[%d] dry-run — skip write", year)
                continue
            log.info("[%d] writing to %s", year, dst_path)
            ds.to_zarr(
                s3fs.S3Map(root=dst_path, s3=dst_fs, check=False),
                mode="w", consolidated=True,
            )
            log.info("[%d] done", year)
        except Exception:
            log.exception("[%d] failed", year)
            failed.append(year)

    if failed:
        log.error("failed years: %s", failed)
        sys.exit(1)
    log.info("mirror complete (%d–%d)", opts.year_start, opts.year_end)


# ─── dev maintenance ─────────────────────────────────────────────────────────


def cmd_lint() -> None:
    sh(["ruff", "check", "plugin/"])
    sh(["ruff", "format", "--check", "plugin/"])


def cmd_format() -> None:
    sh(["ruff", "format", "plugin/"])


def cmd_freeze() -> None:
    cmd_build()
    r = subprocess.run(
        [*COMPOSE, "run", "--rm",
         "--entrypoint", "python3.12 -m pip freeze",
         "storm-cloud-plugin"],
        capture_output=True, text=True, cwd=ROOT,
    )
    if r.returncode != 0:
        print(r.stderr, file=sys.stderr)
        sys.exit(r.returncode)
    skip = ("-e ", "pkg_resources", "stormhub", "cc-py-sdk", "cc_py_sdk")
    lines = sorted(
        line for line in r.stdout.splitlines()
        if line.strip() and not any(line.startswith(s) for s in skip)
    )
    (COMPUTE / "constraints.txt").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    print("Updated compute/constraints.txt")


def cmd_down() -> None:
    sh_quiet([*COMPOSE, "down", "--remove-orphans"])


def cmd_clean() -> None:
    cmd_down()
    shutil.rmtree(COMPUTE / "outputs", ignore_errors=True)
    sh_quiet([*COMPOSE, "down", "-v", "--remove-orphans"])
    print("Cleaned.")


# ─── dispatch ────────────────────────────────────────────────────────────────


_NO_ARGS = {
    "build": cmd_build,
    "lint": cmd_lint,
    "format": cmd_format,
    "freeze": cmd_freeze,
    "down": cmd_down,
    "clean": cmd_clean,
}

_WITH_ARGS = {
    "hec": cmd_hec,
    "batch": cmd_batch,
    "mirror": cmd_mirror,
}


def main() -> None:
    if len(sys.argv) == 1:
        cmd_smoke()
        return

    arg = sys.argv[1]
    if arg in ("-h", "--help", "help"):
        print(__doc__)
        return

    if arg in _NO_ARGS:
        if len(sys.argv) > 2:
            print(f"./run.py {arg} takes no arguments", file=sys.stderr)
            sys.exit(1)
        _NO_ARGS[arg]()
        return

    if arg == "smoke":
        cmd_smoke()
        return

    if arg in _WITH_ARGS:
        _WITH_ARGS[arg](sys.argv[2:])
        return

    # Treat as payload path for a smoke run
    p = Path(arg)
    if p.is_file():
        cmd_smoke(p)
    else:
        print(f"Unknown command or missing payload file: {arg}", file=sys.stderr)
        print("Run ./run.py help for usage.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
