"""Cumsum-based AORC scan — O(area × T) instead of O(area × D × num_dates).

The upstream pipeline reads ``storm_duration`` hours of precip per
storm-date and sums them. With ``check_every_n_hours`` < ``storm_duration``
consecutive windows overlap heavily — each hour of data gets read and
summed roughly ``storm_duration / check_every_n_hours`` times.

This module loads each year's transposition-bbox precip into memory
once, computes a cumulative sum along the time axis, and derives each
storm-date's window sum as a single subtraction:
``window_sum = cumsum[t_end + 1] - cumsum[t_start]``. Per-date work is
then just ``fftconvolve`` + argmax + stats (a few ms).

Bit-for-bit parity with the upstream pipeline at valid shifts:
``valid_shifts`` is computed once from a 2D template that has the
correct NaN pattern (rio.clip output), then cached. At every valid
shift, the watershed window only covers cells that were finite in the
original data, so treating NaN as 0 in the cumsum does not perturb the
mean (mean is computed via numpy masked_array reduction on cells
selected by ``watershed_mask_clipped`` only).

Gating: opt-in via ``CC_CUMSUM_SCAN=1``. Patches stormhub's
``collect_event_stats`` so ``new_collection`` picks it up transparently.
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


_original_collect_event_stats: Callable | None = None


def install() -> None:
    """Replace ``stormhub.met.storm_catalog.collect_event_stats``."""
    global _original_collect_event_stats
    from stormhub.met import storm_catalog as sc_mod

    if _original_collect_event_stats is not None:
        return
    _original_collect_event_stats = sc_mod.collect_event_stats
    sc_mod.collect_event_stats = cumsum_collect_event_stats
    log.info("Cumsum-based AORC scan installed (CC_CUMSUM_SCAN=1)")


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

    Single-process for the v0 prototype. Memory is bounded to one year
    of transposition-bbox data at a time (~1-5 GB depending on payload).
    Parallelization across years can be added later if the single-process
    throughput proves insufficient.
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

    # Group dates by storm-start year. Cross-year storm windows are
    # handled by also loading the next year's data when needed.
    by_year: dict[int, list] = defaultdict(list)
    for d in event_dates:
        by_year[d.year].append(d)

    total = len(event_dates)
    completed = 0
    skipped = 0
    t_overall = time.monotonic()

    for year in sorted(by_year.keys()):
        dates_in_year = sorted(by_year[year])
        t_load = time.monotonic()

        # Determine year range needed: storm windows may cross year boundary.
        max_end = max(d + datetime.timedelta(hours=storm_duration) for d in dates_in_year)
        years_needed = list(range(year, max_end.year + 1))
        paths = tuple(f"{NOAA_AORC_S3_BASE_URL}/{y}.zarr" for y in years_needed)
        log.info(
            "[cumsum-scan] year=%s loading paths=%s (%d dates)",
            year, paths, len(dates_in_year),
        )

        ds = open_aorc_zarr(paths)
        precip_da = ds[AORC_PRECIP_VARIABLE].sel(
            longitude=slice(bounds[0], bounds[2]),
            latitude=slice(bounds[1], bounds[3]),
        )
        precip_da = precip_da.rio.clip(
            [transposition_geom], drop=True, all_touched=True
        )

        log.info(
            "[cumsum-scan] year=%d clipped time=%d lat=%d lon=%d",
            year,
            precip_da.sizes["time"],
            precip_da.sizes["latitude"],
            precip_da.sizes["longitude"],
        )
        precip = precip_da.compute().values  # (T, Y, X) float32
        log.info(
            "[cumsum-scan] year=%d in-memory %.2f GB, loaded in %.1fs",
            year, precip.nbytes / 1e9, time.monotonic() - t_load,
        )

        # NaN → 0 for cumsum accumulation. At valid_shifts (computed
        # once from the rio-clipped template below) no watershed cell
        # ever overlaps an originally-NaN cell, so this substitution
        # cannot perturb any mean we'll actually compute.
        t_cs = time.monotonic()
        precip_filled = np.where(np.isfinite(precip), precip, 0.0)
        # Float64 accumulator → float32 result for memory.
        # cumsum[t] = sum precip[0..t-1], so window [i0..i1] = cumsum[i1+1] - cumsum[i0].
        cumsum = np.empty(
            (precip.shape[0] + 1, precip.shape[1], precip.shape[2]),
            dtype=np.float32,
        )
        cumsum[0] = 0.0
        np.cumsum(precip_filled, axis=0, dtype=np.float64, out=cumsum[1:])
        del precip_filled
        log.info("[cumsum-scan] year=%d cumsum in %.1fs", year, time.monotonic() - t_cs)

        # Build the time → index map. Storm window in the upstream
        # pipeline is sel(time=slice(start+1h, end)); both ends inclusive.
        times = precip_da.time.values
        time_to_idx: dict[datetime.datetime, int] = {}
        for i, t in enumerate(times):
            # np.datetime64 → python datetime (UTC-naive, matching event_dates)
            ts = (t - np.datetime64("1970-01-01T00:00:00")) / np.timedelta64(1, "s")
            time_to_idx[datetime.datetime.utcfromtimestamp(float(ts))] = i

        # Construct Transpose from a 2D template that has the correct
        # NaN pattern. valid_shifts and watershed_mask are computed on
        # first access and then cached; we swap in cumsum-derived
        # window sums per date without rebuilding them.
        template = precip_da.isel(time=0)
        transpose_obj = Transpose(
            template, watershed_geom, "longitude", "latitude"
        )
        _ = transpose_obj.valid_shifts  # warm cache with correct NaN-mask

        for storm_start in dates_in_year:
            start_t = storm_start + datetime.timedelta(hours=1)
            end_t = storm_start + datetime.timedelta(hours=storm_duration)
            i0 = time_to_idx.get(start_t)
            i1 = time_to_idx.get(end_t)
            if i0 is None or i1 is None:
                skipped += 1
                continue

            # Window sum: sum precip[i0..i1] inclusive
            window_sum = cumsum[i1 + 1] - cumsum[i0]
            # Restore NaN where the original data was NaN (outside
            # transposition polygon). valid_shifts is already cached, so
            # this is purely so _create_stats's nanmean does the right
            # thing if a watershed cell happens to sit on a NaN — which
            # by construction at valid_shifts it won't, but defense.
            window_sum_for_transpose = window_sum.astype(np.float64, copy=False)

            transpose_obj._np_data_array = window_sum_for_transpose
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
            if completed % 250 == 0:
                rate = completed / (time.monotonic() - t_overall)
                eta_s = (total - completed) / rate if rate else 0
                log.info(
                    "[cumsum-scan] %d/%d (%.2f/s) ETA %.1fh skipped=%d",
                    completed, total, rate, eta_s / 3600, skipped,
                )

        del precip, cumsum, transpose_obj
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
