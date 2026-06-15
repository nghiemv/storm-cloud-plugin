#!/usr/bin/env python3
"""storm-cloud-plugin task runner.

One entry point for every workflow against this plugin.

Invocation:
    Linux / macOS:        ./run.py <cmd>
    Windows (cmd / PS):   python run.py <cmd>
    Anywhere portable:    python run.py <cmd>

Usage:
    run.py                  Local run with compute/local/sample/payload.json
    run.py PAYLOAD          Local run with a custom payload
    run.py hec              List payloads on HEC S3 and pick one interactively
    run.py hec list         Just list payloads (UUID + timestamp, tab-separated)
    run.py hec UUID [NAME]  Run a specific payload (NAME = output subdir name)
    run.py batch [DIR]      Multi-job HEC S3 driver (DIR holds one subdir per job)
    run.py build            docker build the plugin image
    run.py mirror [args]    One-shot AORC zarr mirror (NOAA -> private S3 cache)
    run.py web [--port N]   Browser UI for browsing payloads + launching runs
    run.py lint             ruff check + format check
    run.py format           ruff format
    run.py test [args...]   pytest plugin/tests/ (forwards extra args to pytest)
    run.py freeze           Regenerate compute/constraints.txt from a built image
    run.py down             docker compose down
    run.py clean            Stop containers, drop volumes, clear compute/outputs/
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

# Shared host plumbing lives in app.py (the single home for these), so the CLI
# launcher and the web server can't drift on env loading, name sanitisation, or
# the launch.json schema. app.py is stdlib-only and imports nothing from run.py.
from app import _load_hec_env, _safe_subdir, serve, write_launch_json

ROOT = Path(__file__).resolve().parent
COMPUTE = ROOT / "compute"
LOCAL = COMPUTE / "local"
COMPOSE = ["docker", "compose", "-f", str(LOCAL / "compose.yaml")]
DEFAULT_PAYLOAD = LOCAL / "sample" / "payload.json"
# Local tag — `./run.py build` produces this. For prod images pulled from
# GHCR, tag them locally: `docker tag ghcr.io/usace/storm-cloud-plugin:latest
# storm-cloud-plugin:latest`.
IMAGE = "storm-cloud-plugin:latest"


def sh(args, env=None, check=True, **kwargs):
    merged = {**os.environ, **(env or {})}
    r = subprocess.run(args, env=merged, cwd=ROOT, **kwargs)
    if check and r.returncode != 0:
        sys.exit(r.returncode)
    return r


def sh_quiet(args, env=None):
    merged = {**os.environ, **(env or {})}
    subprocess.run(
        args, env=merged, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ROOT
    )


def _running_container_for(name: str) -> str | None:
    """Return the id of a live plugin container for run ``name``, else None.

    Matches the ``storm-cloud-run=<name>`` label set on every ``docker run``
    below. Used to refuse a duplicate launch into the same output dir.
    """
    r = subprocess.run(
        ["docker", "ps", "-q", "--filter", f"label=storm-cloud-run={name}"],
        capture_output=True,
        text=True,
    )
    ids = (r.stdout or "").split()
    return ids[0] if ids else None


def _stdout_is(path: Path) -> bool:
    """True when our stdout already points at ``path`` (same dev+inode).

    app.py launches ``run.py`` with stdout redirected to ``launch.log``; the
    CLI leaves stdout on the terminal. The tee in ``_docker_run_logged`` uses
    this to avoid writing every line twice when stdout already *is* the log.
    """
    try:
        return os.path.samestat(os.fstat(sys.stdout.fileno()), os.stat(path))
    except (OSError, ValueError):
        return False


def _docker_run_logged(args, run_dir: Path) -> int:
    """Run ``args``, streaming combined output to ``run_dir/launch.log``.

    launch.log is what the web UI tails for a run's Details/Audit views, so it
    must be populated regardless of who started the run. app.py redirects our
    stdout into launch.log already; the CLI does not — so we always write the
    child's output to the log file ourselves, and additionally mirror it to our
    own stdout unless that stdout already *is* the log file (avoids duplicates).
    Returns the child's exit code.
    """
    log_path = run_dir / "launch.log"
    mirror = not _stdout_is(log_path)
    with open(log_path, "ab", buffering=0) as logf:
        proc = subprocess.Popen(
            args,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert proc.stdout is not None
        for raw in proc.stdout:
            logf.write(raw)
            if mirror:
                sys.stdout.buffer.write(raw)
                sys.stdout.flush()
        return proc.wait()


# ─── local (MinIO dev stack) ─────────────────────────────────────────────────


def cmd_local(payload: Path = DEFAULT_PAYLOAD) -> None:
    """Run the plugin against the local MinIO stack."""
    if not payload.is_file():
        print(f"Error: payload not found: {payload}", file=sys.stderr)
        sys.exit(1)
    # Derive the seed paths from the payload — the payload is the single
    # source of truth for where the plugin will look for inputs, so the
    # local seeder must put them in exactly that location.
    data = json.loads(payload.read_text())
    seed_env = {
        "FFRD_STORE_ROOT": data["stores"][0]["params"]["root"].lstrip("/"),
        "FFRD_INPUT_PATH": data["attributes"]["input_path"],
    }
    sh(["git", "submodule", "update", "--init"])
    sh_quiet([*COMPOSE, "down", "--remove-orphans"], env=seed_env)
    (COMPUTE / "outputs" / "quick-test").mkdir(parents=True, exist_ok=True)
    # the seed service mounts compute/local/sample at /sample inside the container
    container_path = "/sample/" + payload.name
    print(f"Running local: {payload.name}")
    print("Progress streams to stdout; outputs land in compute/outputs/quick-test/\n")
    sh(
        [*COMPOSE, "run", "--rm", "seed"],
        env={**seed_env, "PAYLOAD_FILE": container_path},
    )
    sh([*COMPOSE, "run", "--rm", "storm-cloud-plugin"], env=seed_env)


# ─── hec (production S3) ─────────────────────────────────────────────────────


_HEC_REQUIRED = (
    "CC_AWS_ACCESS_KEY_ID",
    "CC_AWS_SECRET_ACCESS_KEY",
    "CC_AWS_ENDPOINT",
    "CC_AWS_S3_BUCKET",
)


def _require_hec_env() -> None:
    _load_hec_env()
    missing = [k for k in _HEC_REQUIRED if not os.environ.get(k)]
    if missing:
        print(
            f"Error: missing HEC creds: {missing}\n"
            f"  Fix: cp compute/hec/env.example compute/hec/env, then fill it in.\n"
            f"  See the 'Running Against HEC S3' section of README.md.",
            file=sys.stderr,
        )
        sys.exit(1)


_FORWARDED_HEC_ENV = (
    *_HEC_REQUIRED,
    "CC_AWS_DEFAULT_REGION",
    "CC_ROOT",
)


def _docker_plugin_cli(
    subcmd: list[str],
    *,
    mounts: list[tuple[Path, str, str]] = (),
    capture: bool = True,
):
    """Run ``python3.12 -m plugin.cli <subcmd>`` inside the plugin image.

    Keeps the host stdlib-only — boto3 lives in the image, not on PATH.
    Forwards CC_AWS_* env so the in-container S3 client authenticates.

    Returns the subprocess return code when ``capture=False`` (output goes
    straight to the host terminal); otherwise the full ``CompletedProcess``.
    """
    args = [
        "docker",
        "run",
        "--rm",
        "--entrypoint",
        "python3.12",
        *[
            arg
            for k in _FORWARDED_HEC_ENV
            if os.environ.get(k)
            for arg in ("-e", f"{k}={os.environ[k]}")
        ],
        *[arg for h, c, m in mounts for arg in ("-v", f"{h}:{c}:{m}")],
        IMAGE,
        "-m",
        "plugin.cli",
        *subcmd,
    ]
    if capture:
        return subprocess.run(args, capture_output=True, text=True, cwd=ROOT)
    return subprocess.call(args, cwd=ROOT)


def _list_hec_payloads() -> list[dict]:
    """Return payload records sorted newest-first. Each record has at least
    ``uuid`` and ``mtime``; clean payloads also have ``catalog_id``,
    ``catalog_description``, ``start_date``, ``end_date``, ``storm_duration``,
    ``top_n_events``. Malformed payloads carry an ``error`` field.
    """
    r = _docker_plugin_cli(["list-payloads"])
    if r.returncode != 0:
        sys.stderr.write(r.stderr)
        sys.exit(r.returncode)
    return json.loads(r.stdout or "[]")


def _pick_hec_payload() -> tuple[str, str] | None:
    """Print available payloads + prompt. Returns (uuid, output-subdir-name) or None.

    The output-subdir-name is the catalog_id (sanitized) when available, else
    the raw UUID — gives users meaningful local directories rather than UUIDs.
    """
    payloads = _list_hec_payloads()
    bucket = os.environ["CC_AWS_S3_BUCKET"]
    root = os.environ.get("CC_ROOT", "manifests")
    if not payloads:
        print(f"No payloads found at s3://{bucket}/{root}/", file=sys.stderr)
        return None
    print(f"Payloads at s3://{bucket}/{root}/:\n")
    for i, p in enumerate(payloads, 1):
        cid = p.get("catalog_id") or "(unnamed)"
        desc = (p.get("catalog_description") or "").strip()
        start = p.get("start_date", "")
        end = p.get("end_date", "") or start
        dur = p.get("storm_duration", "")
        dates = start if start == end else f"{start} → {end}"
        line2 = "  ".join(s for s in (dates, f"{dur}h" if dur else "", desc) if s)
        print(f"  {i:3d}. {cid}")
        if line2:
            print(f"         {line2}")
        print(f"         uuid {p['uuid']}  ·  {p.get('mtime', '')}")
        print()
    try:
        choice = input("Pick number (or paste a UUID; Enter to cancel): ").strip()
    except EOFError:
        return None
    if not choice:
        return None
    if choice.isdigit() and 1 <= int(choice) <= len(payloads):
        p = payloads[int(choice) - 1]
        return p["uuid"], _safe_subdir(p.get("catalog_id") or p["uuid"])
    return choice, choice  # raw UUID — no catalog_id known


def _host_mem_total_mb() -> int | None:
    """Host RAM in MiB from /proc/meminfo, or None if unreadable."""
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) // 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def _docker_memory_flags() -> list[str]:
    """Return ``--memory`` / ``--memory-swap`` flags for ``docker run``.

    Prefers ``CC_MEMORY_GB`` when set. Otherwise computes 80% of host MemTotal
    (floor 4 GiB) so docker actually caps the cgroup — without this, the
    container's ``cgroup memory.max`` reads ``'max'`` and the kernel won't
    OOM-kill until host RAM is exhausted, taking other host processes with
    it. ``--memory-swap`` is set equal to ``--memory`` so the container can't
    silently dip into swap and tank performance.
    """
    override = os.environ.get("CC_MEMORY_GB", "").strip()
    if override:
        try:
            gb = max(1, int(override))
            return [f"--memory={gb}g", f"--memory-swap={gb}g"]
        except ValueError:
            print(
                f"warning: CC_MEMORY_GB={override!r} not an int — ignoring",
                file=sys.stderr,
            )
    host_mb = _host_mem_total_mb()
    if host_mb is None:
        return []
    safe_mb = max(4 * 1024, int(host_mb * 0.8))
    return [f"--memory={safe_mb}m", f"--memory-swap={safe_mb}m"]


def _run_hec_job(uuid: str, name: str | None = None) -> None:
    name = _safe_subdir(name or uuid)
    run_dir = COMPUTE / "outputs" / name
    run_dir.mkdir(parents=True, exist_ok=True)
    # Refuse to stack a second container on the same output dir. Two runs
    # sharing compute/outputs/<name>/ clobber each other's progress.json,
    # container.id, and launch.log, and double-write the S3 catalog — the
    # failure mode that orphaned a run on 2026-06-15. Stop the live one first.
    existing = _running_container_for(name)
    if existing:
        print(
            f"Error: a run for '{name}' is already active "
            f"(container {existing[:12]}).\n"
            f"Stop it first:  docker stop {existing[:12]}\n"
            f"Refusing to launch a duplicate into compute/outputs/{name}/.",
            file=sys.stderr,
        )
        sys.exit(1)
    # ``docker run --cidfile`` refuses to write to an existing path, so wipe
    # any stale id from a prior run before relaunching.
    cidfile = run_dir / "container.id"
    cidfile.unlink(missing_ok=True)
    # Write launch.json so app.py's _known_runs() (which filters by launch.json
    # presence) recognises CLI-launched runs. catalog_id defaults to the run
    # name; the plugin reads the real one from the S3 payload at runtime. We
    # record our own pid — this process IS the launcher the web UI monitors for
    # liveness (app.py Popens us) — and overwrite the launch.json app.py wrote;
    # without our pid the UI marks every progressed run "interrupted".
    write_launch_json(
        run_dir,
        args=sys.argv,
        pid=os.getpid(),
        payload_uuid=uuid,
        payload_attrs={"catalog_id": name},
        source="run.py hec",
    )
    print(f"Running HEC S3: payload={uuid} (results -> compute/outputs/{name}/)\n")
    forwarded = (
        *_HEC_REQUIRED,
        "CC_AWS_DEFAULT_REGION",
        "FFRD_AWS_ACCESS_KEY_ID",
        "FFRD_AWS_SECRET_ACCESS_KEY",
        "FFRD_AWS_DEFAULT_REGION",
        "FFRD_AWS_ENDPOINT",
        "FFRD_AWS_S3_BUCKET",
        # SC = StormHubStore profile. Payloads reference it as the input /
        # output store via {"stores":[{"profile":"SC", ...}]}. The CC SDK
        # composes env var names as "<profile>_AWS_<KEY>", so all five
        # SC_AWS_* must be in compute/hec/env or PluginManager.connect()
        # raises KeyError before the first action runs.
        "SC_AWS_ACCESS_KEY_ID",
        "SC_AWS_SECRET_ACCESS_KEY",
        "SC_AWS_DEFAULT_REGION",
        "SC_AWS_ENDPOINT",
        "SC_AWS_S3_BUCKET",
        "AORC_S3_BASE_URL",
        "AORC_S3_KEY",
        "AORC_S3_SECRET",
        "AORC_S3_ENDPOINT",
    )
    # The image defaults to DASK_SCHEDULER=synchronous (Dockerfile) so
    # per-worker memory stays predictable (1 dask thread × *_NUM_THREADS=1).
    # That makes every AORC zarr read serial — fine for the no-cache
    # baseline but a 5-10x throttle once the cache exists. Override to
    # "threads" so each storm-window read parallelizes its chunk fetches,
    # capped at DASK_NUM_WORKERS=4 so total threads = num_workers × 4
    # stays bounded. Operator can revert by setting CC_DASK_SCHEDULER=
    # synchronous in the env.
    dask_env = {
        "DASK_SCHEDULER": os.environ.get("CC_DASK_SCHEDULER", "threads"),
        "DASK_NUM_WORKERS": os.environ.get("CC_DASK_NUM_WORKERS", "4"),
    }
    # Opt-in vectorized scan in process-storms. Inherited from the host's
    # shell or compute/hec/env; defaults to off so existing behaviour is
    # unchanged until the operator flips it on.
    if os.environ.get("CC_VECTORIZED_SCAN"):
        dask_env["CC_VECTORIZED_SCAN"] = os.environ["CC_VECTORIZED_SCAN"]
    if os.environ.get("CC_CUMSUM_SCAN"):
        dask_env["CC_CUMSUM_SCAN"] = os.environ["CC_CUMSUM_SCAN"]
    # Override the auto-sized num_workers when a host operator knows the
    # bbox is large (cumsum's per-worker peak is ~5-6 GB for ~800×400
    # transposition domains, vs the workers.py budget of 4 GB).
    if os.environ.get("CC_NUM_WORKERS"):
        dask_env["CC_NUM_WORKERS"] = os.environ["CC_NUM_WORKERS"]

    # Cap the container's memory budget so a runaway worker can't push the
    # host into OOM territory (the kernel will SIGKILL one of our process
    # workers and trigger BrokenProcessPool across the whole scan). cumsum's
    # in-process auto-cap projects per-worker memory from bbox + snapshot
    # count and respects this docker-side limit via /proc/meminfo (which
    # reflects the cgroup when one is set). Default: 80% of host MemTotal,
    # min 4 GiB. Override with CC_MEMORY_GB.
    memory_flags = _docker_memory_flags()
    rc = _docker_run_logged(
        [
            "docker",
            "run",
            "--rm",
            *memory_flags,
            # cidfile lets the web UI `docker stop` this container later for
            # a clean pause; the plugin's signal handler unwinds the current
            # action and exits, preserving on-disk state for resume.
            "--cidfile",
            str(cidfile),
            # Label so we can also locate orphans (`docker ps --filter
            # label=storm-cloud-run=<name>`) if the cidfile is gone.
            "--label",
            f"storm-cloud-run={name}",
            "-v",
            f"{run_dir}:/usr/src/app/Local",
            "-e",
            f"CC_PAYLOAD_ID={uuid}",
            "-e",
            f"CC_MANIFEST_ID={uuid}",
            "-e",
            f"CC_ROOT={os.environ.get('CC_ROOT', 'manifests')}",
            *[arg for k, v in dask_env.items() for arg in ("-e", f"{k}={v}")],
            *[
                arg
                for k in forwarded
                if os.environ.get(k)
                for arg in ("-e", f"{k}={os.environ[k]}")
            ],
            IMAGE,
        ],
        run_dir,
    )
    if rc != 0:
        sys.exit(rc)


def cmd_hec(args: list[str]) -> None:
    """Run the plugin against HEC S3.

    Usage:
      ./run.py hec                       List payloads in S3 and pick one interactively
      ./run.py hec list                  TSV columns: uuid, catalog_id, start_date,
                                         storm_duration, mtime
      ./run.py hec list --json           Full structured JSON (all attributes)
      ./run.py hec promote <KEY>         Promote a catalog-prefix compute-manifest.json
                                         to manifests/<uuid>/payload. Prints the UUID
                                         on success. Idempotent. Used by the web UI.
      ./run.py hec UUID [NAME]           Run that payload (NAME = output subdir name;
                                         defaults to catalog_id from the payload, then UUID)
    """
    _require_hec_env()

    if not args:
        pick = _pick_hec_payload()
        if pick is None:
            return
        uuid, name = pick
        _run_hec_job(uuid, name)
        return

    if args[0] == "list":
        payloads = _list_hec_payloads()
        if "--json" in args[1:]:
            print(json.dumps(payloads))
            return
        for p in payloads:
            print(
                "\t".join(
                    [
                        p.get("uuid", ""),
                        p.get("catalog_id", ""),
                        p.get("start_date", ""),
                        p.get("storm_duration", ""),
                        p.get("mtime", ""),
                    ]
                )
            )
        return

    if args[0] == "promote":
        if len(args) != 2:
            print("usage: ./run.py hec promote <CATALOG_KEY>", file=sys.stderr)
            sys.exit(2)
        # plugin.cli prints "promoted: ..." / "already promoted: ..." to stdout,
        # followed by the UUID on the last line. Forward stdout verbatim so the
        # caller can scrape the UUID from the tail.
        r = _docker_plugin_cli(["promote-catalog", args[1]], capture=False)
        sys.exit(r)

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

    Reads each compute-manifest.json under DIR (default: compute/batch/),
    stages it to s3://$BUCKET/manifests/<uuid>/payload, then runs the plugin
    once per manifest. Set the same HEC creds as `./run.py hec`.
    """
    batch_dir = Path(args[0]) if args else COMPUTE / "batch"
    if not batch_dir.is_dir():
        print(f"Error: batch dir not found: {batch_dir}", file=sys.stderr)
        print(
            "Expected one subdir per job, each holding compute-manifest.json",
            file=sys.stderr,
        )
        sys.exit(1)

    _require_hec_env()

    # Walk the batch dir once on the host (stdlib only) so we have the
    # (name, uuid) map for the per-job runs below.
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

    # Stage all manifests in a single docker run — no host boto3 needed.
    print(
        f"=== Staging {len(jobs)} manifests to s3://{os.environ['CC_AWS_S3_BUCKET']}/manifests/ ==="
    )
    r = _docker_plugin_cli(
        ["upload-batch", "/batch"],
        mounts=[(batch_dir.resolve(), "/batch", "ro")],
    )
    sys.stdout.write(r.stdout)
    sys.stderr.write(r.stderr)
    if r.returncode != 0:
        sys.exit(r.returncode)

    for name, uuid, _ in jobs:
        print(f"\n=== Starting: {name} (payload={uuid}) ===")
        _run_hec_job(uuid, name)
        print(f"=== Done: {name} ===")

    print("\nAll runs complete.")


# ─── mirror (AORC -> private S3) ──────────────────────────────────────────────


def cmd_mirror(args: list[str]) -> None:
    """Mirror AORC zarr data from NOAA public S3 to a private cache.

    Delegates to ``python -m plugin.cli mirror`` inside the plugin image so
    the host only needs Python + Docker (s3fs + xarray live in the image,
    not on the host). Subsequent runs skip years already mirrored.

    Required env (read from compute/hec/env):
      MIRROR_AWS_ACCESS_KEY_ID, MIRROR_AWS_SECRET_ACCESS_KEY,
      MIRROR_AWS_ENDPOINT (default: https://s3.hecdev.net).
    """
    _load_hec_env()
    for required in ("MIRROR_AWS_ACCESS_KEY_ID", "MIRROR_AWS_SECRET_ACCESS_KEY"):
        if not os.environ.get(required):
            print(
                f"Error: {required} not set. Add it to compute/hec/env "
                "(typically mirror your SC_AWS_* values).",
                file=sys.stderr,
            )
            sys.exit(1)

    forwarded = (
        "MIRROR_AWS_ACCESS_KEY_ID",
        "MIRROR_AWS_SECRET_ACCESS_KEY",
        "MIRROR_AWS_ENDPOINT",
    )
    # Stream output live (no capture) so the user sees per-year progress
    # while the long-running mirror runs.
    docker_args = [
        "docker",
        "run",
        "--rm",
        "--entrypoint",
        "python3.12",
        *[
            arg
            for k in forwarded
            if os.environ.get(k)
            for arg in ("-e", f"{k}={os.environ[k]}")
        ],
        IMAGE,
        "-m",
        "plugin.cli",
        "mirror",
        *args,
    ]
    r = subprocess.run(docker_args, cwd=ROOT)
    if r.returncode != 0:
        sys.exit(r.returncode)


# ─── dev maintenance ─────────────────────────────────────────────────────────


def cmd_lint() -> None:
    sh(["ruff", "check", "plugin/"])
    sh(["ruff", "format", "--check", "plugin/"])


def cmd_format() -> None:
    sh(["ruff", "format", "plugin/"])


def cmd_test(args: list[str]) -> None:
    """Run plugin/tests/ via pytest. Extra args forward to pytest."""
    sh([sys.executable, "-m", "pytest", "plugin/tests/", *args])


def cmd_web(args: list[str]) -> None:
    """Browser UI: browse S3 payloads, launch runs, watch progress.

    Localhost-only. See app.py for the implementation.
    """
    import argparse as _ap

    p = _ap.ArgumentParser(prog="./run.py web")
    p.add_argument("--port", type=int, default=8744)
    p.add_argument("--host", default="127.0.0.1")
    opts = p.parse_args(args)
    serve(host=opts.host, port=opts.port)


def cmd_freeze() -> None:
    cmd_build()
    r = subprocess.run(
        [
            *COMPOSE,
            "run",
            "--rm",
            "--entrypoint",
            "python3.12 -m pip freeze",
            "storm-cloud-plugin",
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if r.returncode != 0:
        print(r.stderr, file=sys.stderr)
        sys.exit(r.returncode)
    skip = ("-e ", "pkg_resources", "stormhub", "cc-py-sdk", "cc_py_sdk")
    lines = sorted(
        line
        for line in r.stdout.splitlines()
        if line.strip() and not any(line.startswith(s) for s in skip)
    )
    (COMPUTE / "constraints.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
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
    "test": cmd_test,
    "web": cmd_web,
}


def main() -> None:
    if len(sys.argv) == 1:
        cmd_local()
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

    if arg in _WITH_ARGS:
        _WITH_ARGS[arg](sys.argv[2:])
        return

    # Treat as payload path for a local run
    p = Path(arg)
    if p.is_file():
        cmd_local(p)
    else:
        print(f"Unknown command or missing payload file: {arg}", file=sys.stderr)
        print("Run ./run.py help for usage.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
