"""Cross-action plumbing: shared context, CC SDK I/O, payload validation,
DSS naming, and the soft failure-ratio policy.

Each section below is a single concern that's too small to deserve its own
file. They share no state — the grouping is purely "stuff every action
reaches for". Action-output schemas (``LocalInputs``, ``StormState``) live
with the actions that produce them.

Sections:
    1. RunContext       — typed bag passed between actions
    2. validate_payload — fail fast on malformed payloads
    3. S3 transfers     — exponential-backoff wrappers around cc-py-sdk
    4. DSS helpers      — filename derivation + STAC-item datetime parsing
                          + earliest-pathname scan of a HEC-DSS file
    5. check_failure_ratio — batch action's soft-failure policy
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from plugin.actions.process_storms import StormState
    from plugin.actions.download_inputs import LocalInputs

log = logging.getLogger(__name__)


# ─── 1. RunContext ────────────────────────────────────────────────────────────


@dataclass
class RunContext:
    pm: Any  # cc.plugin_manager.PluginManager
    payload: Any
    local_root: Path
    inputs: "LocalInputs | None" = None
    storms: "StormState | None" = None


# ─── 2. Payload validation ───────────────────────────────────────────────────
#
# CC SDK convention: every payload attribute value is a string. Enforce
# required keys and per-attribute formats so action handlers can trust their
# inputs without re-parsing.


REQUIRED_ATTRS = ("catalog_id", "catalog_description", "output_path", "start_date")
REQUIRED_INPUT_KEYS = ("watershed", "transposition")

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _is_iso_date(v: str) -> bool:
    return bool(_DATE_RE.fullmatch(v))


def _is_positive_int(v: str) -> bool:
    return v.isdigit() and int(v) > 0


def _is_non_negative_float(v: str) -> bool:
    try:
        return float(v) >= 0
    except ValueError:
        return False


def _is_json_string_list(v: str) -> bool:
    try:
        parsed = json.loads(v)
    except (json.JSONDecodeError, TypeError):
        return False
    return isinstance(parsed, list) and all(isinstance(d, str) for d in parsed)


_VALIDATORS: dict[str, tuple[Callable[[str], bool], str]] = {
    "start_date": (_is_iso_date, "YYYY-MM-DD date string"),
    "end_date": (_is_iso_date, "YYYY-MM-DD date string"),
    "storm_duration": (_is_positive_int, "positive integer string"),
    "top_n_events": (_is_positive_int, "positive integer string"),
    "check_every_n_hours": (_is_positive_int, "positive integer string"),
    "min_precip_threshold": (_is_non_negative_float, "non-negative numeric string"),
    "specific_dates": (_is_json_string_list, "JSON array of date strings"),
}


def validate_payload(payload: Any) -> None:
    """Raise ``ValueError`` with a clear message if the payload is misconfigured."""
    attrs = payload.attributes

    missing = [k for k in REQUIRED_ATTRS if k not in attrs]
    if missing:
        raise ValueError(f"Missing required payload attributes: {missing}")

    non_string = [k for k, v in attrs.items() if not isinstance(v, str)]
    if non_string:
        raise ValueError(
            "All payload attribute values must be strings (CC SDK convention), "
            f"but these are not: {non_string}"
        )

    errors: list[str] = []
    for key, (check_fn, description) in _VALIDATORS.items():
        value = attrs.get(key)
        if not value:
            continue
        if not check_fn(value):
            errors.append(f"  {key}={value!r} — expected {description}")
    if errors:
        raise ValueError("Invalid payload attribute values:\n" + "\n".join(errors))

    if not payload.outputs:
        raise ValueError("Payload has no outputs configured")
    if not payload.inputs:
        raise ValueError("Payload has no inputs configured")

    input_keys = payload.inputs[0].paths
    missing_keys = [k for k in REQUIRED_INPUT_KEYS if k not in input_keys]
    if missing_keys:
        raise ValueError(f"Missing required input path keys: {missing_keys}")


# ─── 3. S3 transfers ─────────────────────────────────────────────────────────
#
# Thin retry wrappers around ``PluginManager.copy_file_*`` so action handlers
# just say "download this key to this path" without restating the SDK shape
# or the retry policy on every call.


S3_MAX_RETRIES = 3
S3_INITIAL_DELAY = 2  # seconds, doubled each retry


def _with_retry(op: Callable[[], Any], *, description: str) -> Any:
    delay = S3_INITIAL_DELAY
    for attempt in range(1, S3_MAX_RETRIES + 1):
        try:
            return op()
        except Exception:
            if attempt == S3_MAX_RETRIES:
                raise
            log.warning(
                "%s attempt %d/%d failed, retrying in %ds",
                description,
                attempt,
                S3_MAX_RETRIES,
                delay,
            )
            time.sleep(delay)
            delay *= 2


def download_to_local(
    pm: Any, *, source_name: str, pathkey: str, local_path: Path, description: str
) -> None:
    from cc.plugin_manager import DataSourceOpInput

    op = DataSourceOpInput(name=source_name, pathkey=pathkey, datakey=None)
    _with_retry(
        lambda: pm.copy_file_to_local(ds=op, localpath=str(local_path)),
        description=description,
    )


def upload_from_local(
    pm: Any, *, source_name: str, pathkey: str, local_path: Path, description: str
) -> None:
    from cc.plugin_manager import DataSourceOpInput

    op = DataSourceOpInput(name=source_name, pathkey=pathkey, datakey=None)
    _with_retry(
        lambda: pm.copy_file_to_remote(ds=op, localpath=str(local_path)),
        description=description,
    )


# ─── 4. DSS helpers ──────────────────────────────────────────────────────────
#
# ``convert-to-dss`` and ``create-grid-file`` both derive DSS filenames from
# the same storm collection items; the derivation lives here so the two stay
# in lockstep — drift would silently mis-pair storms and DSS files.


def parse_storm_datetime(item: Any) -> datetime | None:
    """Parse a STAC item's storm start, tolerating tz-naive AORC timestamps."""
    try:
        dt = datetime.strptime(item.id, "%Y-%m-%dT%H")
    except ValueError:
        dt = getattr(item, "datetime", None)
    if dt is not None and dt.tzinfo is not None:
        # AORC zarr time coord is tz-naive (numpy datetime64); strip any tz
        # so ds.sel(time=slice(...)) doesn't raise on tz-aware/naive compare.
        dt = dt.replace(tzinfo=None)
    return dt


def storm_rank(item: Any, fallback: int) -> int:
    """Return a storm item's catalog rank (por_rank).

    ``storm_search`` encodes por_rank as the item id (``item_id = f"{por_rank}"``),
    so the rank is ``int(item.id)``. Fall back to ``fallback`` (e.g. the
    enumeration position) only when the id is not a plain rank integer — e.g.
    the ``%Y-%m-%dT%H`` datetime id used when a storm is searched without a rank.
    """
    try:
        return int(item.id)
    except (TypeError, ValueError):
        return fallback


def dss_filename(storm_start: datetime, rank: int, storm_duration: int) -> str:
    """Filename for storm ``rank`` (1-indexed) starting at ``storm_start``."""
    date_str = storm_start.strftime("%Y%m%d")
    return f"{date_str}_{storm_duration}hr_st1_r{rank:03d}.dss"


def earliest_dss_paths(dss_file: Path) -> tuple[str | None, str | None]:
    """Return earliest PRECIPITATION and TEMPERATURE pathnames in a DSS file."""
    from hecdss import HecDss  # runtime dep; not needed for pure-format tests

    precip_path: str | None = None
    temp_path: str | None = None
    earliest_precip: datetime | None = None
    earliest_temp: datetime | None = None

    with HecDss(str(dss_file)) as dss:
        for path_obj in dss.get_catalog():
            path_str = str(path_obj)
            parts = path_str.strip("/").split("/")
            if len(parts) < 6:
                continue
            part_c = parts[2].upper()
            try:
                dt = datetime.strptime(parts[3], "%d%b%Y:%H%M")
            except ValueError:
                continue
            if part_c == "PRECIPITATION":
                if earliest_precip is None or dt < earliest_precip:
                    precip_path, earliest_precip = path_str, dt
            elif part_c == "TEMPERATURE":
                if earliest_temp is None or dt < earliest_temp:
                    temp_path, earliest_temp = path_str, dt

    return precip_path, temp_path


# ─── 5. Soft failure-ratio policy ────────────────────────────────────────────
#
# ``convert-to-dss`` and ``create-grid-file`` produce one output per storm
# and tolerate a fraction of failures; this is the shared "warn / hard-fail
# if all failed / hard-fail if ratio exceeded" policy.


def check_failure_ratio(
    failed: list[str], total: int, *, label: str, max_ratio: float
) -> None:
    """Raise ``RuntimeError`` if failures exceed ``max_ratio`` (or all failed)."""
    n_failed = len(failed)
    if n_failed == 0:
        return
    log.warning("%s: %d/%d failed: %s", label, n_failed, total, failed)
    if n_failed == total:
        raise RuntimeError(f"All {total} {label} ops failed: {failed}")
    if n_failed / total > max_ratio:
        raise RuntimeError(
            f"{label} failure rate {n_failed}/{total} "
            f"({n_failed / total:.0%}) exceeds threshold ({max_ratio:.0%}): {failed}"
        )
