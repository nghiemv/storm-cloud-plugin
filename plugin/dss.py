"""DSS filename + STAC-item datetime + DSS catalog helpers.

``convert-to-dss`` and ``create-grid-file`` both derive DSS filenames from the
same storm collection items; the derivation lives here so the two stay in
lockstep — drift between them would silently mis-pair storms and DSS files.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
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
