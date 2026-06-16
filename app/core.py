"""Shared foundation for the app/ package: path constants + small helpers.

Stdlib-only leaf module (no imports from app), so maps/status/launch/
discovery and __init__ can all build on it without import cycles.
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any


# app/ is a package — __file__ is app/__init__.py, so go up twice to the
# repo root (was one level when this lived in app.py).
ROOT = Path(__file__).resolve().parent.parent
COMPUTE = ROOT / "compute"
OUTPUTS = COMPUTE / "outputs"
HEC_ENV = COMPUTE / "hec" / "env"
RUN_PY = ROOT / "run.py"
STATIC = ROOT / "static"  # dashboard + report markup (html/css/js)

# Files to mirror from each catalog's S3 prefix when downloading an audit.
_TOP_FILES = ("catalog.json", "catalog.grid")
_EVENTS_FILES = (
    "collection.json",
    "ranked-storms.csv",
    "storm-stats.csv",
    "max_precip_locations.geojson",
    "transposed_watershed_centroids.geojson",
)


def _load_hec_env() -> None:
    """Overlay compute/hec/env onto os.environ (existing vars win)."""
    if not HEC_ENV.is_file():
        return
    for line in HEC_ENV.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _safe_subdir(name: str) -> str:
    """Filesystem-safe coercion for a compute/outputs/<name>/ subdir.

    Must match run.py:_safe_subdir — the plugin writes progress.json to the
    dir this names, so a divergence silently breaks progress visibility.
    """
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
    return cleaned or "run"


def _read_json(p: Path) -> Any | None:
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def write_launch_json(
    run_dir: Path,
    *,
    args,
    pid: int,
    payload_uuid: str | None = None,
    payload_attrs: dict | None = None,
    source: str | None = None,
) -> None:
    """Write the launch record _list_runs() reads to recognise + monitor a run.

    Single source of truth for the schema, shared by app.py (web launch) and
    run.py (CLI launch) so the two can't drift — a mismatch silently breaks
    progress/ETA. ``pid`` is the launcher process the UI checks for liveness.
    """
    rec = {
        "launched_at": time.time(),
        "args": list(args),
        "pid": pid,
        "payload_uuid": payload_uuid,
        "payload_attrs": payload_attrs or {},
    }
    if source:
        rec["source"] = source
    (run_dir / "launch.json").write_text(json.dumps(rec))
