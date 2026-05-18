"""Shared DSS filename + storm datetime helpers.

Both ``convert-to-dss`` and ``create-grid-file`` derive DSS filenames from the
same storm collection items; the derivation lives here so the two stay in
lockstep — drift between them would silently mis-pair storms and DSS files.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


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


def dss_filename(storm_start: datetime, rank: int, storm_duration: int) -> str:
    """Filename for storm ``rank`` (1-indexed) starting at ``storm_start``."""
    date_str = storm_start.strftime("%Y%m%d")
    return f"{date_str}_{storm_duration}hr_st1_r{rank:03d}.dss"
