"""Action: process-storms — Build a STAC catalog/collection from NOAA AORC data."""

from __future__ import annotations

import json
import logging
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Any

from stormhub.met.storm_catalog import StormCatalog, new_catalog, new_collection

from context import RunContext, StormState
from worker_sizing import resolve_num_workers

log = logging.getLogger(__name__)


def _load_existing_collection(
    catalog_dir: Path, catalog_id: str, storm_duration: int
) -> Any | None:
    """Return a previously-saved collection if present and non-empty, else None.

    Enables fast resumes on local re-runs: if a prior invocation left a saved
    catalog under ``catalog_dir / catalog_id``, reuse it instead of rebuilding.
    """
    catalog_file = catalog_dir / catalog_id / "catalog.json"
    if not catalog_file.exists():
        return None
    try:
        catalog = StormCatalog.from_file(str(catalog_file))
        collection_id = catalog.spm.storm_collection_id(storm_duration)
        collection = catalog.get_child(collection_id)
        if collection is None or not list(collection.get_all_items()):
            return None
    except Exception as e:
        log.warning("Could not reload collection — will rebuild: %s", e)
        return None
    log.info("Reloaded existing collection %s from disk", collection_id)
    return collection


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

    collection = _load_existing_collection(
        ctx.local_root, catalog_id, params["storm_duration"]
    )

    if collection is None:
        catalog = new_catalog(
            catalog_id,
            str(ctx.inputs.config_path),
            local_directory=str(ctx.local_root),
            catalog_description=attrs["catalog_description"],
        )
        try:
            collection = new_collection(catalog, **params)
        except BrokenProcessPool as e:
            raise RuntimeError(
                f"Storm processing pool died with num_workers={params['num_workers']} "
                "(likely OOM). Lower via 'num_workers' payload attribute or "
                "CC_NUM_WORKERS env."
            ) from e
        if collection is None:
            raise RuntimeError("no storms found matching criteria")

    log.info("Catalog and collection ready")
    ctx.storms = StormState(collection=collection, params=params)
