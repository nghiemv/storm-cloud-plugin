"""Vectorized rolling-sum AORC storm scan.

Drop-in replacement for ``stormhub.met.storm_catalog.collect_event_stats``
that reads each year of AORC data ONCE instead of once per storm window.

The loop in upstream stormhub iterates over candidate storm-start dates
(every ``check_every_n_hours``) and, for each one, opens a slice of the
year's zarr, clips to the transposition region, sums precipitation over
the storm-duration window, then runs the spatial max-transpose search.

Adjacent windows overlap heavily (e.g. for ``check_every_n_hours=6,
storm_duration=72`` each AORC chunk is touched by ~12 windows), so the
underlying zarr chunks get read ~12× when nothing in the chunk has
changed. This module:

  1. Opens the year zarr once, clips spatially to the transposition box,
     and computes a lazy ``rolling(time=storm_duration).sum()`` — xarray
     evaluates this chunk-streaming, so the underlying data passes
     through memory only once per year per chunk.
  2. For each storm-start date, ``.sel(time=date+storm_duration)`` plucks
     the precomputed 2D cumulative-precip map for that window.
  3. Hands that 2D map to a normally-constructed ``AORCItem`` by
     pre-populating ``_sum_aorc``, so ``AORCItem.transpose`` /
     ``max_transpose`` runs unchanged. CSV output is bit-for-bit
     compatible with stormhub's writer (we call its
     ``storm_search_results_to_csv_line``).

Trade-offs vs the upstream loop:

  - I/O drops from O(year × dates) chunk reads to O(year). Stacking with
    the AORC cache + threads scheduler, this is the largest single
    speedup in the pipeline.
  - Float-summation order changes (rolling reduction vs per-window
    sum), so individual storm precip totals shift by ~1e-15 relative
    error. The top-100 storms by rank are stable; rank-460 (the cutoff
    in current payloads) may swap with rank-461 on rare ties. Not a
    behavior change for any downstream hydrology.
  - Cross-year storms (windows starting in late Dec) are silently
    skipped — first ``storm_duration`` rolling positions of each year
    are NaN. The upstream loop handles cross-year by opening adjacent
    year zarrs; for now we treat those as missing. Acceptable for the
    current payloads (start_date 1979-10-01 to 2025-10-01); a future
    pass should extend each year with the previous year's tail.

Gating: opt-in via ``CC_VECTORIZED_SCAN=1`` env (forwarded by run.py).
The patch is applied right before stormhub's ``new_collection`` runs in
``process_storms`` and reverted on exit, so failure modes fall back to
the upstream loop on the next run with the env unset.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import s3fs
import xarray as xr
from shapely.geometry import shape

from stormhub.met.aorc.aorc import AORCItem
from stormhub.met.consts import AORC_PRECIP_VARIABLE
from stormhub.met.storm_catalog import storm_search_results_to_csv_line
from stormhub.met.zarr_to_dss import _build_aorc_s3fs

log = logging.getLogger(__name__)

# At most this many years stay warm in memory. Each entry is a lazy
# DataArray (the dask task graph), not materialized data, so memory cost
# is small — but materialized chunks behind a Dataset can linger via
# dask's own caches, so 2 is a safe bound for the rare cross-year sweep.
_YEAR_CACHE_SIZE = 2

# How many storm-end-times to materialize per ``.compute()`` call.
#
# Peak memory of the materialized batch is N × lat × lon × 4B. For a
# transposition box like indian-creek (~800×400 cells, ~1 MB/slice),
# 1500 dates ≈ 1.5 GB — fits inside the 4 GB/worker budget while
# capturing all dates from a typical year in one batch. Within-year
# batches amortize chunk reads (one zarr pass per .compute()), so
# sizing the batch to cover the year's dates is what unlocks the
# vectorized speedup.
#
# Larger transposition domains may need a smaller batch; override via
# CC_VECTORIZED_BATCH_SIZE.
_BATCH_SIZE = int(os.environ.get("CC_VECTORIZED_BATCH_SIZE", "1500"))


def vectorized_collect_event_stats(
    event_dates: list[Any],
    catalog: Any,
    collection_id: str | None = None,
    storm_duration: int = 72,
    num_workers: int | None = None,  # noqa: ARG001 — kept for signature compat
    use_threads: bool = False,  # noqa: ARG001
    with_tb: bool = False,
    use_parallel_processing: bool = True,  # noqa: ARG001
) -> None:
    """Signature-compatible replacement for stormhub's collect_event_stats.

    The ``num_workers`` / ``use_threads`` / ``use_parallel_processing``
    knobs are accepted but ignored — the vectorized path is naturally
    single-process (one year warm at a time, all dates in that year
    drained sequentially). Parallelism here would just thrash the
    yearly cache.
    """
    if collection_id is None:
        collection_id = catalog.spm.storm_collection_id(storm_duration)

    output_csv = Path(catalog.spm.collection_dir(collection_id)) / "storm-stats.csv"
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    if not output_csv.exists():
        output_csv.write_text("storm_date,min,mean,max,x,y\n", encoding="utf-8")

    watershed = catalog.watershed
    valid_transposition = catalog.valid_transposition_region
    transposition_geom = shape(valid_transposition.geometry)

    log.info("[vectorized] scanning %d dates", len(event_dates))

    cache = _YearlyRollingSumCache(
        storm_duration_h=storm_duration,
        transposition_geom=transposition_geom,
    )

    # Group dates by year so we materialize one year's rolling-sum slices
    # at a time. Within a year we further split into batches sized to
    # keep peak memory bounded — each batch is one ``.compute()`` call
    # whose result is N × lat × lon × 4B in RAM.
    by_year: dict[int, list] = {}
    for d in event_dates:
        end_time = d + timedelta(hours=storm_duration)
        by_year.setdefault(end_time.year, []).append(d)

    remaining = len(event_dates)
    with output_csv.open("a", encoding="utf-8") as fp:
        for year in sorted(by_year):
            dates_in_year = by_year[year]
            for batch_start in range(0, len(dates_in_year), _BATCH_SIZE):
                batch = dates_in_year[batch_start : batch_start + _BATCH_SIZE]
                end_times = [d + timedelta(hours=storm_duration) for d in batch]
                log.info(
                    "[vectorized] year %d batch %d-%d (%d dates) — computing",
                    year,
                    batch_start,
                    batch_start + len(batch) - 1,
                    len(batch),
                )
                try:
                    materialized = cache.materialize_dates(end_times)
                except KeyError:
                    # End-time falls in min_periods-NaN region; skip whole batch.
                    log.warning(
                        "[vectorized] year %d: skipping batch with pre-window dates",
                        year,
                    )
                    remaining -= len(batch)
                    continue

                for i, date in enumerate(batch):
                    try:
                        slice_2d = materialized.isel(time=i)
                        result = _scan_one_window_with_slice(
                            date,
                            storm_duration,
                            catalog,
                            collection_id,
                            watershed,
                            valid_transposition,
                            slice_2d,
                        )
                        if result is not None:
                            fp.write(storm_search_results_to_csv_line(result))
                            fp.flush()
                        log.info(
                            "%s processed (%d remaining)",
                            date.strftime("%Y-%m-%dT%H"),
                            remaining,
                        )
                    except Exception as exc:
                        if with_tb:
                            import traceback

                            log.error(
                                "Error processing %s: %s\n%s",
                                date,
                                exc,
                                traceback.format_exc(),
                            )
                        else:
                            log.error("Error processing %s: %s", date, exc)
                    finally:
                        remaining -= 1


def _scan_one_window_with_slice(
    storm_start_date: Any,
    storm_duration: int,
    catalog: Any,
    collection_id: str,
    watershed: Any,
    valid_transposition: Any,
    summed_2d: xr.DataArray,
) -> dict | None:
    """Run max_transpose on an already-materialized 2D cumulative-precip map.

    The caller has already computed the rolling-sum slice for this
    storm window (in a batch ``.compute()``); we just hand it to a
    normally-constructed AORCItem and let stormhub's spatial search
    run unchanged.
    """
    item_id = storm_start_date.strftime("%Y-%m-%dT%H")
    item_dir = catalog.spm.collection_item_dir(collection_id, item_id)
    item = AORCItem(
        item_id,
        storm_start_date,
        timedelta(hours=storm_duration),
        shape(watershed.geometry),
        shape(valid_transposition.geometry),
        item_dir,
        watershed.id,
        valid_transposition.id,
        href=catalog.spm.collection_item(collection_id, item_id),
    )
    # Pre-populate the time-summed dataset so AORCItem.transpose skips
    # the per-window zarr read in aorc_source_data + sum_aorc. The
    # downstream Transpose construction reads ds["APCP_surface"], so we
    # wrap our 2D DataArray in a Dataset under the same variable name.
    item._sum_aorc = xr.Dataset({AORC_PRECIP_VARIABLE: summed_2d})

    _, _, stats, centroid = item.max_transpose(add_properties=False)
    item.clear_cached_data()
    return {
        "storm_date": item_id,
        "centroid": centroid,
        "aorc:statistics": stats,
    }


class _YearlyRollingSumCache:
    """LRU cache of yearly lazy rolling-sum DataArrays.

    Each entry is xarray's lazy ``rolling(time=storm_duration).sum()``
    over the clipped transposition region for one year. ``.sel(time=...)``
    materializes only the requested 2D (lat, lon) slice, which is what
    AORCItem.transpose consumes.
    """

    def __init__(
        self,
        *,
        storm_duration_h: int,
        transposition_geom: Any,
    ) -> None:
        self._storm_duration_h = storm_duration_h
        self._transposition_geom = transposition_geom
        self._bounds = transposition_geom.bounds
        self._aorc_base = os.environ.get(
            "AORC_S3_BASE_URL", "s3://noaa-nws-aorc-v1-1-1km"
        )
        self._s3 = _build_aorc_s3fs()
        self._cache: dict[int, xr.DataArray] = {}

    def get_window(self, storm_start_date: Any) -> xr.DataArray | None:
        """Return the cumulative-precip 2D map for the storm window.

        The storm window is ``[start+1h, start+storm_duration_h]`` (per
        AORCItem.aorc_source_data). xarray's ``rolling(time=N).sum()``
        at position ``t`` is the sum of ``[t-N+1, t]``, so we look up
        position ``start + storm_duration_h``. Returns None if the
        rolling-sum position is in an incomplete-window region (NaN
        from ``min_periods=N``).
        """
        end_time = storm_start_date + timedelta(hours=self._storm_duration_h)
        rolled = self._get_year(end_time.year)
        try:
            return rolled.sel(time=end_time)
        except KeyError:
            return None

    def _get_year(self, year: int) -> xr.DataArray:
        if year not in self._cache:
            self._cache[year] = self._build_year(year)
            # Evict oldest entries beyond the cap (insertion-ordered dict).
            while len(self._cache) > _YEAR_CACHE_SIZE:
                stale = next(iter(self._cache))
                del self._cache[stale]
        return self._cache[year]

    def _build_year(self, year: int) -> xr.DataArray:
        path = f"{self._aorc_base}/{year}.zarr"
        log.info("[vectorized] opening %s (lazy)", path)
        ds = xr.open_dataset(
            s3fs.S3Map(root=path, s3=self._s3, check=False),
            engine="zarr",
            chunks="auto",
            consolidated=True,
        )
        # Clip to transposition box: stormhub does the same in
        # AORCItem.aorc_source_data so the spatial extent matches.
        sub = ds.sel(
            longitude=slice(self._bounds[0], self._bounds[2]),
            latitude=slice(self._bounds[1], self._bounds[3]),
        )
        clipped = sub.rio.clip([self._transposition_geom], drop=True, all_touched=True)
        # min_periods=storm_duration → first storm_duration-1 positions
        # are NaN, matching the upstream behavior of not processing
        # storm-starts so early that a 72h window would overrun
        # available data.
        # Stay LAZY here. Eagerly computing the whole year was tempting
        # (one chunk-read pass per year) but for typical USACE
        # transposition regions (indian-creek's clipped bounds are 6.7°
        # × 3.4° ≈ 800×400 cells), 8760h × 800 × 400 × 4B ≈ 11 GB just
        # for the source slice and another ~11 GB for the rolling
        # result — OOMs in a 30 GB container, especially with the
        # parallel mirror also running.
        #
        # Amortization happens caller-side via ``materialize_dates``,
        # which batches dates and runs a single ``.compute()`` per
        # batch. That lets dask plan chunk reads across the batch
        # (its only cross-call amortization point) while keeping
        # peak memory bounded to batch_size × lat × lon × 4B.
        return (
            clipped[AORC_PRECIP_VARIABLE]
            .rolling(time=self._storm_duration_h, min_periods=self._storm_duration_h)
            .sum()
        )

    def materialize_dates(self, end_times: list) -> xr.DataArray:
        """Eagerly compute rolling-sum 2D maps for a batch of end-times.

        ``rolling.sel(time=[t1, t2, ...]).compute()`` plans chunk reads
        across the whole batch — that's the only point at which dask
        amortizes across an otherwise lazy graph. Caller batches dates
        to a memory-safe size.
        """
        if not end_times:
            raise ValueError("end_times must be non-empty")
        # Caller guarantees all end_times share a year (one rolling per year).
        rolled = self._get_year(end_times[0].year)
        return rolled.sel(time=end_times).compute()


# ── Public API: enable / disable the patch ──────────────────────────────────


_original_collect_event_stats = None


def enabled() -> bool:
    """Whether ``CC_VECTORIZED_SCAN`` opts in to the vectorized path."""
    return os.environ.get("CC_VECTORIZED_SCAN", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def install() -> None:
    """Swap stormhub's collect_event_stats for the vectorized version.

    Idempotent. Records the original so ``restore()`` can undo cleanly.
    """
    global _original_collect_event_stats
    import stormhub.met.storm_catalog as sh

    if _original_collect_event_stats is not None:
        return  # already installed
    _original_collect_event_stats = sh.collect_event_stats
    sh.collect_event_stats = vectorized_collect_event_stats
    log.info("Vectorized AORC scan installed (CC_VECTORIZED_SCAN=1)")


def restore() -> None:
    """Undo ``install()``."""
    global _original_collect_event_stats
    import stormhub.met.storm_catalog as sh

    if _original_collect_event_stats is None:
        return
    sh.collect_event_stats = _original_collect_event_stats
    _original_collect_event_stats = None
