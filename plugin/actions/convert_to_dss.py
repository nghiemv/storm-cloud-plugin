"""Action: convert-to-dss — Convert storm events from NOAA Zarr to HEC-DSS."""

from __future__ import annotations

import logging
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime
from typing import Optional

from plugin.lib import (
    RunContext,
    check_failure_ratio,
    dss_filename,
    parse_storm_datetime,
)
from plugin.progress import Progress

log = logging.getLogger(__name__)

MAX_FAILURE_RATIO = float(os.environ.get("DSS_MAX_FAILURE_RATIO", "0.5"))
# Default to 2 workers, not ``cpu_count``: each subprocess re-imports
# stormhub + dask + s3fs and loads the storm-duration precip slice into
# memory. With 8 cores and a 30 GB box, ``cpu_count`` workers OOM-killed
# the run twice mid-conversion. 2 keeps peak under ~5 GB while staying
# parallel enough that DSS-write I/O still overlaps with zarr reads.
# Override via ``DSS_WORKERS`` env to a higher value on a fatter host.
DSS_WORKERS = int(os.environ.get("DSS_WORKERS", "2"))

# Spawn, not fork: stormhub workers open s3fs, whose async event-loop thread
# doesn't survive fork() and deadlocks child Event.wait() on first S3 read.
# Same fix landed in stormhub's own pools on v0.5.0.
_SPAWN_CTX = multiprocessing.get_context("spawn")


def _convert_single_storm(
    output_path: str,
    transposition_file: str,
    catalog_id: str,
    storm_start_iso: str,
    storm_duration: int,
) -> Optional[str]:
    """Convert one storm to DSS. Returns error message on failure, None on success.

    Runs in a subprocess via ProcessPoolExecutor, so all args must be picklable.
    """
    from stormhub.met.zarr_to_dss import NOAADataVariable, noaa_zarr_to_dss

    try:
        noaa_zarr_to_dss(
            output_dss_path=output_path,
            aoi_geometry_gpkg_path=transposition_file,
            aoi_name=catalog_id,
            storm_start=datetime.fromisoformat(storm_start_iso),
            variable_duration_map={
                NOAADataVariable.APCP: storm_duration,
                NOAADataVariable.TMP: storm_duration,
            },
            output_resolution_km=4,
        )
        return None
    except Exception as e:
        return str(e)


def convert_to_dss(ctx: RunContext) -> None:
    if ctx.inputs is None or ctx.storms is None:
        raise RuntimeError(
            "convert-to-dss requires download-inputs and process-storms to run first"
        )

    catalog_id = ctx.payload.attributes["catalog_id"]
    storm_duration = ctx.storms.params["storm_duration"]
    transposition_file = str(ctx.inputs.transposition_path)

    dss_dir = ctx.local_root / catalog_id / "data"
    dss_dir.mkdir(parents=True, exist_ok=True)

    items = list(ctx.storms.collection.get_all_items())
    if not items:
        raise RuntimeError("No storm events found in collection — nothing to convert")

    log.info("Converting %d storm events to DSS", len(items))

    work: list[tuple[str, str, str]] = []  # (item_id, output_path, storm_start_iso)
    failed: list[str] = []

    for idx, item in enumerate(items, start=1):
        storm_start = parse_storm_datetime(item)
        if storm_start is None:
            log.warning("Skipping item %s: could not parse datetime", item.id)
            failed.append(item.id)
            continue

        out_name = dss_filename(storm_start, idx, storm_duration)
        out_path = dss_dir / out_name

        # Idempotency: skip if DSS file already exists
        if out_path.exists():
            log.info(
                "[%d/%d] Skipping %s — %s already exists",
                idx,
                len(items),
                item.id,
                out_name,
            )
            continue

        work.append((item.id, str(out_path), storm_start.isoformat()))

    if work:
        workers = min(len(work), max(1, DSS_WORKERS))
        log.info("Running %d conversions with %d workers", len(work), workers)
        progress = Progress(total=len(work), label="convert-to-dss")

        with ProcessPoolExecutor(max_workers=workers, mp_context=_SPAWN_CTX) as pool:
            futures = {
                pool.submit(
                    _convert_single_storm,
                    out_path,
                    transposition_file,
                    catalog_id,
                    start_iso,
                    storm_duration,
                ): item_id
                for item_id, out_path, start_iso in work
            }
            try:
                for future in as_completed(futures):
                    item_id = futures[future]
                    error = future.result()
                    if error:
                        log.error("Failed to convert %s: %s", item_id, error)
                        failed.append(item_id)
                    else:
                        log.info("  Converted %s", item_id)
                    progress.tick()
            except BrokenProcessPool as e:
                # A subprocess died abruptly (OOM, segfault, or external SIGKILL).
                # The pool is now unusable. Any DSS files completed by surviving
                # workers are already flushed to disk — they'll be picked up by
                # the idempotency check on the next resume. Cancel pending work
                # so we exit promptly with a clear signal.
                pending = sum(1 for f in futures if not f.done())
                for f in futures:
                    f.cancel()
                raise RuntimeError(
                    f"convert-to-dss process pool died (workers={workers}, "
                    f"~{pending} items pending). DSS files completed so far "
                    f"are preserved on disk; re-run will skip them via "
                    f"idempotency and resume the rest. If this recurs, "
                    f"lower DSS_WORKERS (currently {workers}) further or "
                    f"check container memory headroom."
                ) from e

    check_failure_ratio(
        failed, len(items), label="DSS conversion", max_ratio=MAX_FAILURE_RATIO
    )
    log.info("DSS conversion complete. Output: %s", dss_dir)
