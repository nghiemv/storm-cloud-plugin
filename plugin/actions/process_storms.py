"""Action: process-storms — Build a STAC catalog/collection from NOAA AORC data."""

from __future__ import annotations

import json
import logging
import os
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stormhub.met.storm_catalog import StormCatalog, new_catalog, new_collection

from plugin.actions import parallel_threads, vectorized_scan, vectorized_transpose
from plugin.lib import RunContext
from plugin.progress import StormhubProgressTracker
from plugin.workers import resolve_num_workers

log = logging.getLogger(__name__)


@dataclass
class StormState:
    """Catalog + parameters built by ``process-storms``."""

    collection: Any  # pystac.Collection
    params: dict[str, Any]


def _try_reload(
    catalog_dir: Path, catalog_id: str, storm_duration: int
) -> tuple[Any | None, Any | None]:
    """Return ``(catalog, collection)`` reloaded from disk if present.

    Either may be ``None``:
      - ``(None, None)`` when no ``catalog.json`` exists (fresh build needed).
      - ``(catalog, None)`` when the catalog exists but the collection for
        this storm_duration is missing or empty (skip new_catalog, build only
        the collection). This also sidesteps stormhub's ``valid_spaces_item``,
        which hardcodes ``anon=True`` s3fs against our private cache URL and
        fails to authenticate.
      - ``(catalog, collection)`` when both are reusable.
    """
    catalog_file = catalog_dir / catalog_id / "catalog.json"
    if not catalog_file.exists():
        return None, None
    try:
        catalog = StormCatalog.from_file(str(catalog_file))
        collection_id = catalog.spm.storm_collection_id(storm_duration)
        collection = catalog.get_child(collection_id)
        if collection is None or not list(collection.get_all_items()):
            collection = None
    except Exception as e:
        log.warning("Could not reload catalog — will rebuild: %s", e)
        return None, None
    if collection is not None:
        log.info("Reloaded existing collection %s from disk", collection_id)
    else:
        log.info("Reloaded catalog %s — collection missing, will rebuild it", catalog_id)
    return catalog, collection


def _vec_transpose_enabled() -> bool:
    """Vectorized max_transpose is on by default; opt out via env."""
    return os.environ.get("CC_VECTORIZED_TRANSPOSE", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _storm_params(attrs: dict[str, str]) -> dict[str, Any]:
    end_date = attrs.get("end_date") or attrs["start_date"]
    if not attrs.get("end_date"):
        log.info(
            "No end_date specified — defaulting to start_date (%s) for single-day scan",
            end_date,
        )
    return {
        "start_date": attrs["start_date"],
        "end_date": end_date,
        "storm_duration": int(attrs.get("storm_duration", "72")),
        "min_precip_threshold": float(attrs.get("min_precip_threshold", "0.0")),
        "top_n_events": int(attrs.get("top_n_events", "10")),
        "check_every_n_hours": int(attrs.get("check_every_n_hours", "24")),
        "num_workers": resolve_num_workers(attrs),
        # Processes (use_threads=False): the vectorized max_transpose and
        # batch_size fix are baked into vendored stormhub, so subprocess
        # workers inherit them. Threads were tried but the surrounding
        # per-date Python work is GIL-bound; processes hit cores directly.
        "use_threads": False,
        "specific_dates": (
            json.loads(attrs["specific_dates"]) if attrs.get("specific_dates") else []
        ),
    }


def process_storms(ctx: RunContext) -> None:
    if ctx.inputs is None:
        raise RuntimeError("process-storms requires download-inputs to run first")

    attrs = ctx.payload.attributes
    catalog_id = attrs["catalog_id"]
    params = _storm_params(attrs)

    catalog, collection = _try_reload(
        ctx.local_root, catalog_id, params["storm_duration"]
    )

    if collection is None:
        if catalog is None:
            catalog = new_catalog(
                catalog_id,
                str(ctx.inputs.config_path),
                local_directory=str(ctx.local_root),
                catalog_description=attrs["catalog_description"],
            )
        # Two independent stormhub patches, each gated by its own env:
        #
        # - vectorized_transpose (CC_VECTORIZED_TRANSPOSE=1, default ON)
        #   Replaces Transpose.max_transpose's Python loop with one
        #   scipy.signal.fftconvolve pass — bit-identical output,
        #   ~4× wall-clock win per storm date on indian-creek. Paired
        #   with use_threads=True (set in _storm_params) so the
        #   class-level patch stays visible to ThreadPoolExecutor
        #   workers (a ProcessPoolExecutor with 'spawn' would re-import
        #   stormhub fresh and skip the patch).
        #
        # - vectorized_scan (CC_VECTORIZED_SCAN=1, default OFF)
        #   Replaces collect_event_stats with a single-process loop
        #   over batched rolling-sum slices. Bit-identical but no net
        #   speedup in measured runs (dask's rolling().sum() doesn't
        #   amortize chunk reads the way the implementation assumes).
        #   Kept opt-in until that design is revisited.
        if _vec_transpose_enabled():
            vectorized_transpose.install()
            # Pair with the batch-size patch: with vec_transpose+threads,
            # upstream's batch_size=num_workers//3 leaves most threads idle.
            parallel_threads.install()
        if vectorized_scan.enabled():
            vectorized_scan.install()
        try:
            with StormhubProgressTracker(label="process-storms"):
                collection = new_collection(catalog, **params)
        except BrokenProcessPool as e:
            raise RuntimeError(
                f"Storm processing pool died with num_workers={params['num_workers']} "
                "(likely OOM). Lower via 'num_workers' payload attribute or "
                "CC_NUM_WORKERS env."
            ) from e
        finally:
            vectorized_scan.restore()
            vectorized_transpose.restore()
            parallel_threads.restore()
        if collection is None:
            raise RuntimeError("no storms found matching criteria")

    log.info("Catalog and collection ready")
    ctx.storms = StormState(collection=collection, params=params)
