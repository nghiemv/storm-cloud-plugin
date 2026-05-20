"""Cumsum-based AORC scan — O(area × T) instead of O(area × D × num_dates).

The upstream pipeline reads ``storm_duration`` hours of precip per
storm-date and sums them. With ``check_every_n_hours`` < ``storm_duration``
consecutive windows overlap heavily — each hour of data gets read and
summed roughly ``storm_duration / check_every_n_hours`` times.

This module sweeps each year once. For every storm-date we only need
two snapshots of the running cumulative sum (one at the window start,
one just past the window end), so the algorithm is:

1. For each year, compute the set of "times of interest" — for each
   storm-date ``d``, ``i_start = idx_of(d + 1h)`` and ``i_end1 = idx_of(d + D) + 1``.
2. Stream the year's transposition-bbox precip in time-chunks
   (default 720h = 1 month). Maintain a running cumulative sum
   ``(Y, X) float64``. Whenever the stream passes a time-of-interest,
   snapshot the running sum.
3. For each storm-date: ``window_sum = snapshot[i_end1] - snapshot[i_start]``,
   feed into the reused ``Transpose`` object, write CSV row.

Peak memory is bounded by one chunk + the snapshot pool (~3 GB for a
1000×1000 transposition bbox), not by the full-year cube (which would
be tens of GB for large domains).

Bit-equivalent to upstream at valid shifts. ``valid_shifts`` is
computed once from a 2D template that has the rio.clip NaN pattern.
At any valid shift the watershed window only overlaps cells that were
finite in the original data, so treating NaN as 0 in the running sum
cannot perturb any mean we'll actually emit.

Gating: opt-in via ``CC_CUMSUM_SCAN=1``. Chunk size: ``CC_CUMSUM_CHUNK_HOURS``
(default 720).
"""

from __future__ import annotations

import datetime
import gc
import logging
import os
import time
from collections import defaultdict
from typing import Any, Callable

import numpy as np

log = logging.getLogger(__name__)


def enabled() -> bool:
    """Whether the cumsum-scan path is active. Opt-in via env."""
    return os.environ.get("CC_CUMSUM_SCAN", "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


_CHUNK_HOURS = int(os.environ.get("CC_CUMSUM_CHUNK_HOURS", "720"))

_original_collect_event_stats: Callable | None = None


def install() -> None:
    """Replace ``stormhub.met.storm_catalog.collect_event_stats``."""
    global _original_collect_event_stats
    from stormhub.met import storm_catalog as sc_mod

    if _original_collect_event_stats is not None:
        return
    _original_collect_event_stats = sc_mod.collect_event_stats
    sc_mod.collect_event_stats = cumsum_collect_event_stats
    log.info(
        "Cumsum-based AORC scan installed (CC_CUMSUM_SCAN=1, chunk=%dh)",
        _CHUNK_HOURS,
    )


def restore() -> None:
    global _original_collect_event_stats
    from stormhub.met import storm_catalog as sc_mod

    if _original_collect_event_stats is None:
        return
    sc_mod.collect_event_stats = _original_collect_event_stats
    _original_collect_event_stats = None


def cumsum_collect_event_stats(
    event_dates: list,
    catalog: Any,
    collection_id: str | None = None,
    storm_duration: int = 72,
    num_workers: int | None = None,
    use_threads: bool = False,
    with_tb: bool = False,
    use_parallel_processing: bool = True,
) -> None:
    """Drop-in replacement for ``stormhub.met.storm_catalog.collect_event_stats``.

    Streams each year's transposition-bbox precip in 1-month chunks,
    accumulating a running cumulative sum and snapshotting it at the
    indices each storm-date needs. Memory bounded to ~one chunk's worth.
    """
    from shapely.geometry import shape
    from stormhub.met.consts import (
        AORC_PRECIP_VARIABLE,
        NOAA_AORC_S3_BASE_URL,
    )
    from stormhub.met.transpose import Transpose
    from stormhub.met.zarr_to_dss import open_aorc_zarr

    if not collection_id:
        collection_id = catalog.spm.storm_collection_id(storm_duration)
    collection_dir = catalog.spm.collection_dir(collection_id)
    os.makedirs(collection_dir, exist_ok=True)

    csv_path = os.path.join(collection_dir, "storm-stats.csv")
    if not os.path.exists(csv_path):
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("storm_date,min,mean,max,x,y\n")

    watershed_geom = shape(catalog.watershed.geometry)
    transposition_geom = shape(catalog.valid_transposition_region.geometry)
    bounds = transposition_geom.bounds

    by_year: dict[int, list] = defaultdict(list)
    for d in event_dates:
        by_year[d.year].append(d)

    total = len(event_dates)
    completed = 0
    skipped = 0
    t_overall = time.monotonic()

    for year in sorted(by_year.keys()):
        dates_in_year = sorted(by_year[year])
        t_year = time.monotonic()

        # Open year(s) — cross-year storm windows need year+1 too.
        max_end = max(d + datetime.timedelta(hours=storm_duration) for d in dates_in_year)
        years_needed = list(range(year, max_end.year + 1))
        paths = tuple(f"{NOAA_AORC_S3_BASE_URL}/{y}.zarr" for y in years_needed)
        log.info(
            "[cumsum-scan] year=%s opening %s for %d dates",
            year, paths, len(dates_in_year),
        )

        ds = open_aorc_zarr(paths)
        precip_da = ds[AORC_PRECIP_VARIABLE].sel(
            longitude=slice(bounds[0], bounds[2]),
            latitude=slice(bounds[1], bounds[3]),
        )
        # rio.clip is eager — masks outside-polygon cells with NaN but
        # the lazy structure is preserved if the source was dask-backed.
        precip_da = precip_da.rio.clip(
            [transposition_geom], drop=True, all_touched=True
        )

        T = precip_da.sizes["time"]
        Y = precip_da.sizes["latitude"]
        X = precip_da.sizes["longitude"]
        log.info(
            "[cumsum-scan] year=%d clipped time=%d lat=%d lon=%d (~%.1f GB cube avoided)",
            year, T, Y, X, T * Y * X * 4 / 1e9,
        )

        # Map zarr-time to integer index using a small probe.
        times_np = precip_da.time.values
        time_to_idx: dict[datetime.datetime, int] = {}
        for i, t in enumerate(times_np):
            ts = (t - np.datetime64("1970-01-01T00:00:00")) / np.timedelta64(1, "s")
            time_to_idx[datetime.datetime.utcfromtimestamp(float(ts))] = i

        # Build the set of cumsum-indices each date needs.
        date_to_io = {}
        toi: set[int] = {0}  # snapshot[0] = zeros (used as the lower edge for early-year windows)
        for d in dates_in_year:
            start_t = d + datetime.timedelta(hours=1)
            end_t = d + datetime.timedelta(hours=storm_duration)
            i0 = time_to_idx.get(start_t)
            i1 = time_to_idx.get(end_t)
            if i0 is None or i1 is None:
                continue
            date_to_io[d] = (i0, i1 + 1)
            toi.add(i0)
            toi.add(i1 + 1)
        toi_sorted = sorted(toi)
        log.info(
            "[cumsum-scan] year=%d %d snapshots over T=%d",
            year, len(toi_sorted), T,
        )

        # Stream the precip cube in CC_CUMSUM_CHUNK_HOURS slabs, maintain
        # a running cumulative sum, snapshot at each time-of-interest.
        running = np.zeros((Y, X), dtype=np.float64)
        snapshots: dict[int, np.ndarray] = {0: running.copy()}
        t_stream = time.monotonic()
        bytes_streamed = 0
        next_toi_idx = 1  # toi_sorted[0] == 0, already snapshotted

        chunk_h = _CHUNK_HOURS
        for chunk_start in range(0, T, chunk_h):
            chunk_end = min(chunk_start + chunk_h, T)
            t_chunk_load = time.monotonic()
            chunk = (
                precip_da.isel(time=slice(chunk_start, chunk_end))
                .compute()
                .values
            )  # (chunk_h, Y, X) float32
            bytes_streamed += chunk.nbytes

            chunk_filled = np.where(np.isfinite(chunk), chunk, 0.0).astype(np.float64)
            chunk_cum = np.cumsum(chunk_filled, axis=0)
            del chunk, chunk_filled

            # Snapshot any time-of-interest that falls in (chunk_start, chunk_end].
            # cumsum[t] = sum precip[0..t-1] = running (sum before chunk) + chunk_cum[t - chunk_start - 1].
            while next_toi_idx < len(toi_sorted):
                t_idx = toi_sorted[next_toi_idx]
                if t_idx <= chunk_start:
                    # already past, snapshot already taken (or t_idx=0)
                    next_toi_idx += 1
                    continue
                if t_idx > chunk_end:
                    break
                local = t_idx - chunk_start - 1  # 0..chunk_cum.shape[0]-1
                snapshots[t_idx] = running + chunk_cum[local]
                next_toi_idx += 1

            running = running + chunk_cum[-1]
            del chunk_cum

            log.debug(
                "[cumsum-scan] year=%d chunk %d-%d loaded in %.1fs",
                year, chunk_start, chunk_end, time.monotonic() - t_chunk_load,
            )

        log.info(
            "[cumsum-scan] year=%d stream done %.1fs (%.1f GB read, %d snapshots)",
            year,
            time.monotonic() - t_stream,
            bytes_streamed / 1e9,
            len(snapshots),
        )

        # Build Transpose from a 2D template with the rio.clip NaN pattern.
        # valid_shifts + watershed_mask are computed once and reused as we
        # swap in cumsum-derived window sums.
        template = precip_da.isel(time=0).compute()
        transpose_obj = Transpose(
            template, watershed_geom, "longitude", "latitude"
        )
        _ = transpose_obj.valid_shifts

        t_dates = time.monotonic()
        for storm_start in dates_in_year:
            io = date_to_io.get(storm_start)
            if io is None:
                skipped += 1
                continue
            i0, i1p1 = io
            window_sum = snapshots[i1p1] - snapshots[i0]

            transpose_obj._np_data_array = window_sum  # float64
            poly, _aff, stats = transpose_obj.max_transpose(_create_stats)
            centroid = poly.centroid

            line = (
                f"{storm_start.strftime('%Y-%m-%dT%H')},"
                f"{stats['min']},{stats['mean']},{stats['max']},"
                f"{centroid.x},{centroid.y}\n"
            )
            with open(csv_path, "a", encoding="utf-8") as f:
                f.write(line)
            completed += 1
            if completed % 500 == 0:
                rate = completed / (time.monotonic() - t_overall)
                eta_s = (total - completed) / rate if rate else 0
                log.info(
                    "[cumsum-scan] %d/%d (%.2f/s) ETA %.1fh skipped=%d",
                    completed, total, rate, eta_s / 3600, skipped,
                )

        log.info(
            "[cumsum-scan] year=%d per-date pass: %.1fs (%d dates, %.3fs/date) — year total %.1fs",
            year,
            time.monotonic() - t_dates,
            len(date_to_io),
            (time.monotonic() - t_dates) / max(1, len(date_to_io)),
            time.monotonic() - t_year,
        )

        del snapshots, running, transpose_obj
        gc.collect()

    log.info(
        "[cumsum-scan] DONE: %d/%d processed (skipped=%d) in %.1fs",
        completed, total, skipped, time.monotonic() - t_overall,
    )


def _create_stats(array: np.ndarray) -> dict:
    """Match ``stormhub.met.aorc.aorc.AORCItem._create_stats``."""
    from stormhub.met.consts import MM_TO_INCH_CONVERSION_FACTOR
    return {
        "min": round(float(np.nanmin(array)) * MM_TO_INCH_CONVERSION_FACTOR, 2),
        "mean": round(float(np.nanmean(array)) * MM_TO_INCH_CONVERSION_FACTOR, 2),
        "max": round(float(np.nanmax(array)) * MM_TO_INCH_CONVERSION_FACTOR, 2),
        "units": "inches",
    }
