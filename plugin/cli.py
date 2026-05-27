"""Host-invoked CLI — runs inside the plugin image so the host only needs
Python + Docker.

Invoked from run.py via ``docker run --rm ... IMAGE python3.12 -m plugin.cli <subcmd>``.

Subcommands:
    list-payloads          stdout: JSON array of payload records, newest first.
                           Each record has: uuid, mtime, source (``manifests``
                           or ``catalog-prefix``), and (when the payload body
                           parses cleanly) catalog_id, catalog_description,
                           start_date, end_date, storm_duration, top_n_events.
                           ``catalog-prefix`` entries also carry catalog_key.
                           Read-only — promote-catalog handles writes.
    promote-catalog <KEY>  Copy s3://$CC_AWS_S3_BUCKET/<KEY> (a catalog-prefix
                           compute-manifest.json) to manifests/<uuid>/payload,
                           rewriting it to CC-payload shape. Idempotent.
                           Prints the resolved UUID to stdout. Called by the
                           web app's launch path for catalog-prefix entries.
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
import uuid as _uuid
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

# Top-level bucket prefixes that are never catalogs. Used by catalog-prefix
# discovery below to skip system / cache namespaces.
_NON_CATALOG_PREFIXES = {"manifests", "aorc-cache", "aorc-cache-conus"}


def _stable_catalog_uuid(catalog_id: str) -> str:
    """Stable UUID for a catalog-prefix payload — same catalog_id always
    produces the same UUID, so repeated discovery doesn't create new keys
    and a later promotion step is idempotent.
    """
    return str(_uuid.uuid5(_uuid.NAMESPACE_OID, f"sc-catalog:{catalog_id}"))


def _cmd_list_payloads() -> int:
    """Discover both flavors of payload, emit a unified list.

    1. ``manifests/<uuid>/payload`` — the CC-standard location; entries
       carry ``source: "manifests"``. Ready to launch.

    2. ``<catalog>/compute-manifest.json`` — catalogs created via the UI's
       POST /api/catalog whose dual-write to ``manifests/`` never landed.
       Entries carry ``source: "catalog-prefix"`` and a ``catalog_key``
       pointing at the manifest object. **Read-only here** — the launch
       path is responsible for promoting before running.
    """
    bucket = os.environ["CC_AWS_S3_BUCKET"]
    prefix = f"{os.environ.get('CC_ROOT', 'manifests')}/"
    s3 = _s3_client()

    # Step 1: enumerate manifests/<uuid>/payload via list_objects_v2.
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

    # Step 2: fan-out GetObject for the manifests/ entries so listing 50
    # payloads doesn't serialize to 50 × round-trip-time.
    def fetch_manifest(uuid: str) -> dict:
        rec: dict = {"uuid": uuid, "mtime": found[uuid], "source": "manifests"}
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
            records = list(pool.map(fetch_manifest, found.keys()))

    # Step 3: discover catalog-prefix payloads. Skip any catalog_id that
    # already has a manifests/ entry — those are already runnable.
    already_listed = {r.get("catalog_id") for r in records if r.get("catalog_id")}

    def fetch_catalog_prefix(name: str) -> dict | None:
        try:
            obj = s3.get_object(
                Bucket=bucket, Key=f"{name}/compute-manifest.json"
            )
            mtime = obj.get("LastModified")
            manifest = json.loads(obj["Body"].read())
        except Exception:
            return None
        inputs = manifest.get("inputs") or {}
        attrs = (
            inputs.get("payload_attributes") or {}
            if isinstance(inputs, dict)
            else {}
        )
        catalog_id = attrs.get("catalog_id") or name
        if catalog_id in already_listed:
            return None
        rec: dict = {
            "uuid": _stable_catalog_uuid(catalog_id),
            "mtime": mtime.isoformat() if mtime else "",
            "source": "catalog-prefix",
            "catalog_key": f"{name}/compute-manifest.json",
        }
        for key in _PAYLOAD_ATTRS:
            rec[key] = attrs.get(key, "")
        return rec

    candidate_names: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes") or []:
            name = cp["Prefix"].rstrip("/")
            if name not in _NON_CATALOG_PREFIXES:
                candidate_names.append(name)

    if candidate_names:
        with ThreadPoolExecutor(max_workers=8) as pool:
            for rec in pool.map(fetch_catalog_prefix, candidate_names):
                if rec is not None:
                    records.append(rec)

    records.sort(key=lambda r: r.get("mtime", ""), reverse=True)
    json.dump(records, sys.stdout)
    return 0


def _cmd_promote_catalog(catalog_key: str) -> int:
    """Copy ``<catalog>/compute-manifest.json`` to ``manifests/<uuid>/payload``,
    rewriting the JSON to CC-payload shape. Idempotent — re-running is a
    no-op once the target key exists.

    The web app's launch path calls this for ``source: catalog-prefix``
    entries before kicking off the container, so the docker run sees a
    standard manifests/<uuid>/payload.
    """
    bucket = os.environ["CC_AWS_S3_BUCKET"]
    prefix = f"{os.environ.get('CC_ROOT', 'manifests')}/"
    s3 = _s3_client()

    try:
        obj = s3.get_object(Bucket=bucket, Key=catalog_key)
    except Exception as e:
        print(f"error: cannot read {catalog_key}: {e}", file=sys.stderr)
        return 1
    manifest = json.loads(obj["Body"].read())
    inputs = manifest.get("inputs") or {}
    attrs = (
        inputs.get("payload_attributes") or {}
        if isinstance(inputs, dict)
        else {}
    )
    catalog_id = attrs.get("catalog_id")
    if not catalog_id:
        print(f"error: {catalog_key} has no catalog_id in attributes", file=sys.stderr)
        return 1

    new_uuid = _stable_catalog_uuid(catalog_id)
    payload_key = f"{prefix}{new_uuid}/payload"
    try:
        s3.head_object(Bucket=bucket, Key=payload_key)
        print(f"already promoted: {payload_key}")
        print(new_uuid)
        return 0
    except Exception:
        pass

    # Compute-manifest shape -> CC payload shape: attributes lifted from
    # inputs.payload_attributes, inputs flattened to inputs.data_sources.
    payload_body = {
        "attributes": attrs,
        "stores": manifest.get("stores", []),
        "inputs": (inputs.get("data_sources") or [])
        if isinstance(inputs, dict)
        else inputs,
        "outputs": manifest.get("outputs", []),
        "actions": manifest.get("actions", []),
    }
    s3.put_object(
        Bucket=bucket,
        Key=payload_key,
        Body=json.dumps(payload_body).encode(),
        ContentType="application/json",
    )
    print(f"promoted: {catalog_key} -> {payload_key}")
    print(new_uuid)
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

    Runs inside the plugin image (s3fs + zarr are baked in), so the host
    needs only Python + Docker. Reads ``MIRROR_AWS_ACCESS_KEY_ID`` /
    ``MIRROR_AWS_SECRET_ACCESS_KEY`` / ``MIRROR_AWS_ENDPOINT`` for the
    write target. Source is NOAA's public bucket (anonymous).

    The mirror byte-copies zarr chunks (no decode/recode) and regenerates
    consolidated metadata at the end. See ``_mirror_one_year`` for details.
    Each year worker spawns ``MIRROR_COPY_THREADS`` (default 64) GET/PUT
    threads. With ``--parallel-years N``, multiple years run in subprocesses
    concurrently. Network is the bottleneck, not CPU, so push parallelism
    until you saturate the link rather than counting cores.
    """
    import argparse

    p = argparse.ArgumentParser(prog="plugin.cli mirror")
    p.add_argument("--year-start", type=int, default=1979)
    p.add_argument("--year-end", type=int, default=2024)
    p.add_argument("--dest-base", default="s3://storm-cloud/aorc-cache-conus")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--parallel-years",
        type=int,
        default=1,
        help="number of years to mirror concurrently (default 1)",
    )
    opts = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("mirror")

    years = list(range(opts.year_start, opts.year_end + 1))

    # Pre-list existing dst keys for each year, serially in the parent. We tried
    # listing inside each worker and hammered HEC's endpoint into timeouts when
    # many workers listed simultaneously; one connection at a time keeps it sane.
    # Workers receive their year's dict and skip uploads where dst has matching
    # size — lets us resume cleanly from a crashed prior run.
    import s3fs

    _mirror_log = logging.getLogger("mirror")
    _dst_fs = s3fs.S3FileSystem(
        anon=False,
        key=os.environ["MIRROR_AWS_ACCESS_KEY_ID"],
        secret=os.environ["MIRROR_AWS_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ.get(
            "MIRROR_AWS_ENDPOINT", "https://s3.hecdev.net"
        ),
    )
    existing_by_year: dict[int, dict[str, int]] = {}
    for year in years:
        dst_root = f"{opts.dest_base}/{year}.zarr".replace("s3://", "")
        try:
            detail = _dst_fs.find(dst_root, detail=True)
            existing_by_year[year] = (
                {p: i.get("size", -1) for p, i in detail.items()}
                if isinstance(detail, dict)
                else {}
            )
        except FileNotFoundError:
            existing_by_year[year] = {}
        except Exception as e:
            _mirror_log.warning(
                "pre-list failed for %d (worker will re-upload all): %s", year, e
            )
            existing_by_year[year] = {}
        n = len(existing_by_year[year])
        if n:
            _mirror_log.info("pre-listed %d: %d existing keys", year, n)

    if opts.parallel_years <= 1:
        results = [
            _mirror_one_year(year, opts.dest_base, opts.dry_run, existing_by_year[year])
            for year in years
        ]
    else:
        from concurrent.futures import ProcessPoolExecutor, as_completed

        log.info(
            "Mirroring %d years with %d parallel workers",
            len(years),
            opts.parallel_years,
        )
        results = []
        with ProcessPoolExecutor(max_workers=opts.parallel_years) as pool:
            futures = {
                pool.submit(
                    _mirror_one_year,
                    y,
                    opts.dest_base,
                    opts.dry_run,
                    existing_by_year[y],
                ): y
                for y in years
            }
            for fut in as_completed(futures):
                results.append(fut.result())

    failed = [year for year, ok in results if not ok]
    if failed:
        log.error("failed years: %s", sorted(failed))
        return 1
    log.info("mirror complete (%d–%d)", opts.year_start, opts.year_end)
    return 0


def _mirror_one_year(
    year: int,
    dest_base: str,
    dry_run: bool,
    existing_dst: dict[str, int] | None = None,
) -> tuple[int, bool]:
    """Mirror a single year by byte-copying zarr chunks. Returns (year, success).

    We keep the full NOAA extent and only filter variables, so no decode is
    needed — chunks ship from NOAA to HEC byte-for-byte. This bypasses the
    xarray/dask pipeline, eliminating CPU-bound decompress/recompress and
    letting a single year saturate the network with many parallel GETs/PUTs.

    Runnable in a subprocess (ProcessPoolExecutor) because every name it
    needs is either imported here or passed in.

    After all chunks land we run ``zarr.consolidate_metadata`` to rewrite
    ``.zmetadata`` over our filtered variable set — NOAA's ``.zmetadata``
    references vars we drop, so we cannot copy it verbatim.
    """
    import threading
    from queue import Queue

    import s3fs
    import zarr

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("mirror")

    KEEP_VARS = ("APCP_surface", "TMP_2maboveground")
    COORDS = ("time", "latitude", "longitude")
    NOAA_BASE = "s3://noaa-nws-aorc-v1-1-1km"
    # Decoupled GET/PUT pipeline: GET threads pull from NOAA into a bounded
    # in-memory queue; PUT threads drain the queue into HEC. This is ~2× faster
    # than a coupled GET-then-PUT-per-thread loop because GET and PUT no longer
    # block each other within a thread — each side runs at its independent
    # ceiling (NOAA ~127 MB/s, HEC ~100 MB/s) until the slower one caps us.
    # Tuned defaults reflect server-side per-IP throttles observed in testing.
    GET_THREADS = int(os.environ.get("MIRROR_GET_THREADS", "64"))
    PUT_THREADS = int(os.environ.get("MIRROR_PUT_THREADS", "16"))
    # Bound the in-flight queue to cap RAM: 64 × ~5 MB max = ~320 MB peak.
    QUEUE_DEPTH = int(os.environ.get("MIRROR_QUEUE_DEPTH", "64"))

    key = os.environ["MIRROR_AWS_ACCESS_KEY_ID"]
    secret = os.environ["MIRROR_AWS_SECRET_ACCESS_KEY"]
    endpoint = os.environ.get("MIRROR_AWS_ENDPOINT", "https://s3.hecdev.net")

    src_fs = s3fs.S3FileSystem(
        anon=True, config_kwargs={"max_pool_connections": GET_THREADS + 8}
    )
    dst_fs = s3fs.S3FileSystem(
        anon=False,
        key=key,
        secret=secret,
        endpoint_url=endpoint,
        config_kwargs={"max_pool_connections": PUT_THREADS + 8},
    )

    src_root = f"{NOAA_BASE}/{year}.zarr".replace("s3://", "")
    dst_root = f"{dest_base}/{year}.zarr".replace("s3://", "")
    marker = f"{dst_root}/.mirror-complete"
    try:
        if dst_fs.exists(marker):
            log.info("[%d] already cached — skip", year)
            return year, True

        log.info("[%d] enumerating keys under %s", year, src_root)
        # path -> size for every key we intend to copy
        src_entries: dict[str, int] = {}
        # Root-level metadata files (.zattrs, .zgroup). Skip NOAA's .zmetadata —
        # we'll regenerate one matching our filtered variable set.
        for entry in src_fs.ls(src_root, detail=True):
            name = entry["name"].rsplit("/", 1)[-1]
            if entry["type"] == "file" and name in (".zattrs", ".zgroup"):
                src_entries[entry["name"]] = entry["size"]
        for sub in (*COORDS, *KEEP_VARS):
            detail = src_fs.find(f"{src_root}/{sub}", detail=True)
            for path, info in detail.items():
                src_entries[path] = info["size"]
        log.info("[%d] %d keys to copy", year, len(src_entries))

        if dry_run:
            log.info("[%d] dry-run — skip copy", year)
            return year, True

        # ``existing_dst`` is pre-computed by the parent via a single serialized
        # listing pass — keeps the worker fan-out from hammering HEC with N
        # concurrent paginated list_objects requests.
        if existing_dst is None:
            existing_dst = {}

        # GET workers consume src keys, push (rel, data) tuples into pipe_q.
        # PUT workers drain pipe_q, write to HEC. Counters and errors guarded
        # by a single lock; errors fail the year fast.
        log.info(
            "[%d] copying via decoupled pipeline (%d GET + %d PUT threads, queue=%d)",
            year, GET_THREADS, PUT_THREADS, QUEUE_DEPTH,
        )
        get_q: Queue = Queue()
        pipe_q: Queue = Queue(maxsize=QUEUE_DEPTH)
        GET_DONE = object()
        PUT_DONE = object()
        for src_key in src_entries:
            get_q.put(src_key)
        for _ in range(GET_THREADS):
            get_q.put(GET_DONE)

        state = {"copied": 0, "skipped": 0, "done": 0, "errors": []}
        state_lock = threading.Lock()
        total = len(src_entries)

        def get_worker() -> None:
            while True:
                src_key = get_q.get()
                if src_key is GET_DONE:
                    return
                try:
                    rel = src_key[len(src_root) + 1 :]
                    dst_key = f"{dst_root}/{rel}"
                    if existing_dst.get(dst_key) == src_entries[src_key]:
                        with state_lock:
                            state["skipped"] += 1
                            state["done"] += 1
                            if state["done"] % 5000 == 0:
                                log.info(
                                    "[%d] %d/%d (uploaded %d, skipped %d)",
                                    year, state["done"], total,
                                    state["copied"], state["skipped"],
                                )
                        continue
                    data = src_fs.cat(src_key)
                    pipe_q.put((rel, data))
                except Exception as e:
                    with state_lock:
                        state["errors"].append((src_key, e))

        def put_worker() -> None:
            while True:
                item = pipe_q.get()
                if item is PUT_DONE:
                    return
                rel, data = item
                try:
                    with dst_fs.open(f"{dst_root}/{rel}", "wb") as f:
                        f.write(data)
                    with state_lock:
                        state["copied"] += 1
                        state["done"] += 1
                        if state["done"] % 5000 == 0:
                            log.info(
                                "[%d] %d/%d (uploaded %d, skipped %d)",
                                year, state["done"], total,
                                state["copied"], state["skipped"],
                            )
                except Exception as e:
                    with state_lock:
                        state["errors"].append((rel, e))

        get_ts = [threading.Thread(target=get_worker, daemon=True) for _ in range(GET_THREADS)]
        put_ts = [threading.Thread(target=put_worker, daemon=True) for _ in range(PUT_THREADS)]
        for t in get_ts + put_ts:
            t.start()
        # Wait for GETs to finish before signaling PUTs to drain.
        for t in get_ts:
            t.join()
        for _ in range(PUT_THREADS):
            pipe_q.put(PUT_DONE)
        for t in put_ts:
            t.join()

        if state["errors"]:
            first = state["errors"][0]
            raise RuntimeError(
                f"[{year}] {len(state['errors'])} transfer errors; first at "
                f"{first[0]}: {first[1]}"
            )
        log.info(
            "[%d] copy phase: %d uploaded, %d skipped",
            year, state["copied"], state["skipped"],
        )

        log.info("[%d] consolidating metadata", year)
        zarr.consolidate_metadata(dst_fs.get_mapper(dst_root))

        with dst_fs.open(marker, "wb") as f:
            f.write(b"")
        log.info("[%d] done (%d keys)", year, len(src_entries))
        return year, True
    except Exception:
        log.exception("[%d] failed", year)
        return year, False


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    cmd, args = sys.argv[1], sys.argv[2:]
    if cmd == "list-payloads" and not args:
        return _cmd_list_payloads()
    if cmd == "promote-catalog" and len(args) == 1:
        return _cmd_promote_catalog(args[0])
    if cmd == "upload-batch" and len(args) == 1:
        return _cmd_upload_batch(args[0])
    if cmd == "mirror":
        return _cmd_mirror(args)
    print(f"unknown command or wrong arg count: {cmd} {args}", file=sys.stderr)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
