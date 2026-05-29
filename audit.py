#!/usr/bin/env python3
"""Audit storm-catalog outputs in HEC S3.

Downloads each catalog's metadata + thumbnails to
``compute/outputs/<name>/audit/`` and emits a static ``report.html`` per
catalog. Per-catalog checks: DSS file counts, grid-file integrity, outlier
DSS files (likely empty/partial), STAC item counts, storm date coverage,
centroid containment in the transposition domain.

Usage:
    ./audit.py                              Download + report + serve for all known runs
    ./audit.py download [NAME]              Just refresh artifacts from S3
    ./audit.py report   [NAME]              (Re)generate report.html (no S3 round-trips)
    ./audit.py serve [PORT] [--host HOST]   Open the reports in a browser via local HTTP
                                            (default 127.0.0.1:8745; use --host 0.0.0.0
                                            when browsing from another machine)

Reads HEC S3 creds from compute/hec/env (same convention as run.py).

stdlib only on the host except for ``mc`` (MinIO client) on PATH for fast
recursive downloads — falls back to a Python S3 client if ``boto3`` is
installed but ``mc`` isn't.
"""

from __future__ import annotations

import csv
import html
import json
import os
import shutil
import subprocess
import sys
import webbrowser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
COMPUTE = ROOT / "compute"
OUTPUTS = COMPUTE / "outputs"
HEC_ENV = COMPUTE / "hec" / "env"

# Files to mirror from each catalog's S3 prefix.
_TOP_FILES = ("catalog.json", "catalog.grid")
_EVENTS_FILES = (
    "collection.json",
    "ranked-storms.csv",
    "storm-stats.csv",
    "max_precip_locations.geojson",
    "transposed_watershed_centroids.geojson",
)


# ─── env / S3 plumbing ────────────────────────────────────────────────────────


def _load_hec_env() -> None:
    if not HEC_ENV.is_file():
        return
    for line in HEC_ENV.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _mc_available() -> bool:
    return shutil.which("mc") is not None


def _mc_alias_for_endpoint() -> str | None:
    """Find the mc alias that points at the configured CC endpoint."""
    if not _mc_available():
        return None
    endpoint = os.environ.get("CC_AWS_ENDPOINT", "").rstrip("/")
    if not endpoint:
        return None
    try:
        cfg_path = Path.home() / ".mc" / "config.json"
        cfg = json.loads(cfg_path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    for name, entry in (cfg.get("aliases") or {}).items():
        if entry.get("url", "").rstrip("/") == endpoint and entry.get(
            "accessKey"
        ) == os.environ.get("CC_AWS_ACCESS_KEY_ID"):
            return name
    return None


def _bucket() -> str:
    return os.environ["CC_AWS_S3_BUCKET"]


def _mc_cp(src: str, dst: Path, *, recursive: bool = False) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    alias = _mc_alias_for_endpoint()
    if not alias:
        raise RuntimeError(
            "no matching mc alias for CC_AWS_ENDPOINT/CC_AWS_ACCESS_KEY_ID; "
            "configure one with `mc alias set ...` or install boto3."
        )
    cmd = ["mc", "cp", "--quiet"]
    if recursive:
        cmd.append("--recursive")
    cmd.extend([f"{alias}/{_bucket()}/{src}", str(dst)])
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"mc cp failed for {src}: {r.stderr.strip()}")


def _mc_ls_lines(src: str) -> list[str]:
    alias = _mc_alias_for_endpoint()
    if not alias:
        raise RuntimeError("no mc alias available")
    r = subprocess.run(
        ["mc", "ls", f"{alias}/{_bucket()}/{src}"],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"mc ls failed for {src}: {r.stderr.strip()}")
    return [ln for ln in r.stdout.splitlines() if ln.strip()]


# ─── known runs (mined from compute/outputs/) ────────────────────────────────


def _known_runs() -> list[str]:
    """Subdirs of compute/outputs/ that have a launch.json (= a real run)."""
    if not OUTPUTS.is_dir():
        return []
    out = []
    for sub in sorted(OUTPUTS.iterdir()):
        if (sub / "launch.json").is_file():
            out.append(sub.name)
    return out


def _catalog_id_for(run_name: str) -> str:
    """Read catalog_id from compute/outputs/<run>/launch.json. Falls back to the
    run name when the launch record lacks it (older runs stored only the UUID).
    """
    lj = OUTPUTS / run_name / "launch.json"
    if lj.is_file():
        try:
            data = json.loads(lj.read_text())
            cid = (data.get("payload_attrs") or {}).get("catalog_id")
            if cid:
                return cid
        except (OSError, json.JSONDecodeError):
            pass
    return run_name


# ─── download ─────────────────────────────────────────────────────────────────


def _download_run(run_name: str) -> Path:
    """Mirror the catalog's S3 prefix into compute/outputs/<run>/audit/."""
    cid = _catalog_id_for(run_name)
    dst = OUTPUTS / run_name / "audit"
    dst.mkdir(parents=True, exist_ok=True)

    # Resolve events-prefix lazily — duration varies (72hr-events / 48hr-events).
    events_prefix = _resolve_events_prefix(cid)
    print(f"[{cid}] events prefix: {events_prefix}")

    for f in _TOP_FILES:
        print(f"  ↓ {f}")
        _mc_cp(f"{cid}/{f}", dst / f)

    (dst / "events").mkdir(exist_ok=True)
    for f in _EVENTS_FILES:
        print(f"  ↓ events/{f}")
        _mc_cp(f"{cid}/{events_prefix}/{f}", dst / "events" / f)

    # Items + thumbnails (recursive). Skip if already present and the user
    # just wants to regenerate the report — but most users re-run download
    # because S3 changed, so always run.
    print(f"  ↓ {events_prefix}/<N>/ (items + thumbnails, recursive)")
    _mc_cp(f"{cid}/{events_prefix}/", dst / "events", recursive=True)

    (dst / "hydro_domains").mkdir(exist_ok=True)
    for f in (
        f"{cid}-watershed.json",
        f"{cid}-transposition.json",
        f"{cid}-transposition_valid.json",
    ):
        print(f"  ↓ hydro_domains/{f}")
        try:
            _mc_cp(f"{cid}/hydro_domains/{f}", dst / "hydro_domains" / f)
        except RuntimeError as e:
            # transposition_valid is optional; keep going.
            print(f"    (skip: {e})")

    # Cache the data/ DSS listing so the report can audit sizes without
    # downloading the 600 MiB of DSS files themselves.
    print("  ↓ data/ listing (sizes only)")
    listing = _mc_ls_lines(f"{cid}/data/")
    (dst / "data-listing.txt").write_text("\n".join(listing) + "\n")

    # Stash the manifest payload too — older launch.json files lack
    # payload_attrs, so the audit falls back to this for top_n / duration etc.
    try:
        lj_path = OUTPUTS / run_name / "launch.json"
        uuid = json.loads(lj_path.read_text()).get("payload_uuid")
        if uuid:
            cc_root = os.environ.get("CC_ROOT", "manifests")
            print(f"  ↓ {cc_root}/{uuid}/payload (for top_n / duration fallback)")
            _mc_cp(
                f"{cc_root}/{uuid}/payload",
                dst / "payload.json",
            )
    except (OSError, json.JSONDecodeError, RuntimeError) as e:
        print(f"    (manifest fallback skipped: {e})")

    return dst


def _resolve_events_prefix(cid: str) -> str:
    """The events prefix is ``<duration>hr-events``; duration comes from the
    payload but only the catalog dir is known. List the prefix and pick the
    one ending in '-events'.
    """
    lines = _mc_ls_lines(f"{cid}/")
    for line in lines:
        tail = line.split()[-1]
        if tail.endswith("-events/"):
            return tail.rstrip("/")
    raise RuntimeError(f"no <duration>hr-events/ prefix found under {cid}/")


# ─── audit checks ─────────────────────────────────────────────────────────────


def _parse_mc_size(token: str) -> int:
    """Convert mc's human-readable size token (e.g. '1.4MiB', '123KiB', '434B')
    to a byte count. Returns -1 on parse failure.
    """
    # Longest suffix first so 'MiB' wins over 'B' (every "iB" form also
    # endswith('B')); GiB before MiB before KiB before bare B.
    units = (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024), ("B", 1))
    for suffix, mul in units:
        if token.endswith(suffix):
            try:
                return int(float(token[: -len(suffix)]) * mul)
            except ValueError:
                return -1
    return -1


def _parse_listing(audit_dir: Path) -> list[dict]:
    """Parse compute/outputs/<run>/audit/data-listing.txt → list of
    {name, size_bytes, size_token}.
    """
    out: list[dict] = []
    text = (audit_dir / "data-listing.txt").read_text(errors="replace")
    for line in text.splitlines():
        toks = line.split()
        if not toks:
            continue
        # mc ls format: "[DATE TIME] SIZE STORAGE NAME"
        # Locate the size token by walking from the right past the name + STANDARD.
        # Easier: name is the last token; size_token is 2 tokens before that.
        name = toks[-1]
        size_token = toks[-3] if len(toks) >= 3 else ""
        if "." not in name:
            continue
        out.append(
            {
                "name": name,
                "size_token": size_token,
                "size_bytes": _parse_mc_size(size_token),
            }
        )
    return out


def _audit(run_name: str) -> dict:
    """Run all programmatic checks for one catalog. Returns a dict the HTML
    report consumes.
    """
    audit_dir = OUTPUTS / run_name / "audit"
    launch = {}
    try:
        launch = json.loads((OUTPUTS / run_name / "launch.json").read_text())
    except (OSError, json.JSONDecodeError):
        pass
    progress = {}
    try:
        progress = json.loads((OUTPUTS / run_name / "progress.json").read_text())
    except (OSError, json.JSONDecodeError):
        pass

    attrs = (launch.get("payload_attrs") or {}) if launch else {}

    # Older launches (before payload_attrs was captured) leave attrs empty —
    # fall back to the cached manifest payload from S3.
    if not attrs:
        manifest_path = OUTPUTS / run_name / "audit" / "payload.json"
        if manifest_path.is_file():
            try:
                attrs = json.loads(manifest_path.read_text()).get("attributes") or {}
            except (OSError, json.JSONDecodeError):
                attrs = {}

    catalog_id = attrs.get("catalog_id") or run_name
    top_n = int(attrs.get("top_n_events") or 0)
    storm_duration = int(attrs.get("storm_duration") or 0)

    # Pull events from max_precip_locations.geojson — already has id, date,
    # mean/min/max, lonlat, season per event. Saves parsing 460 STAC items.
    geojson_path = audit_dir / "events" / "max_precip_locations.geojson"
    events: list[dict] = []
    if geojson_path.is_file():
        gj = json.loads(geojson_path.read_text())
        for feat in gj.get("features", []):
            props = feat.get("properties") or {}
            geom = feat.get("geometry") or {}
            coords = geom.get("coordinates") or [None, None]
            stats = props.get("aorc:statistics") or {}
            events.append(
                {
                    "id": props.get("id"),
                    "storm_start": props.get("storm_start_date"),
                    "season": props.get("season"),
                    "mean": stats.get("mean"),
                    "min": stats.get("min"),
                    "max": stats.get("max"),
                    "max_lon": coords[0],
                    "max_lat": coords[1],
                }
            )

    # ─── Enrich each event from its STAC item ────────────────────────────
    # Pulls bbox (DSS spatial coverage) and end_datetime (actual storm window
    # — lets us verify duration matches the declared `storm_duration`).
    # Reading 460 small JSONs is cheap (~30 ms total) and the visualization
    # needs the bbox to overlay coverage on the map.
    for ev in events:
        eid = ev.get("id")
        if not eid:
            continue
        item_path = audit_dir / "events" / str(eid) / f"{eid}.json"
        if not item_path.is_file():
            continue
        try:
            item = json.loads(item_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        bbox = item.get("bbox")
        if isinstance(bbox, list) and len(bbox) >= 4:
            ev["bbox"] = [float(bbox[0]), float(bbox[1]),
                          float(bbox[2]), float(bbox[3])]
        props = item.get("properties") or {}
        ev["storm_end"] = props.get("end_datetime")
        if not ev.get("storm_start"):
            ev["storm_start"] = props.get("start_datetime")

    # DSS listing
    dss = _parse_listing(audit_dir)
    median_bytes = (
        sorted(d["size_bytes"] for d in dss)[len(dss) // 2] if dss else 0
    )
    # Outlier = much smaller than median; thresholds tuned per typical sizes.
    # Median is ~1.4 MiB (72hr), ~1.7 MiB (48hr), ~3.8 MiB (120hr). Anything
    # < 50% of median is suspicious (the empirically-broken 123KiB files come
    # in at ~10% of median, the partial 1.0-1.4 MiB ones at ~25-35%).
    outlier_threshold = int(median_bytes * 0.5) if median_bytes else 0
    outlier_dss = (
        [d for d in dss if 0 < d["size_bytes"] < outlier_threshold]
        if outlier_threshold
        else []
    )

    # Grid file: parse names of Grid blocks.
    grid_path = audit_dir / "catalog.grid"
    grid_entries: list[dict] = []
    if grid_path.is_file():
        current = None
        for line in grid_path.read_text().splitlines():
            line_stripped = line.strip()
            if line_stripped.startswith("Grid: "):
                current = {
                    "name": line_stripped[len("Grid: ") :].strip(),
                    "grid_type": None,
                    "dss_file": None,
                    "dss_pathname": None,
                }
                grid_entries.append(current)
            elif current and line_stripped.startswith("Grid Type:"):
                current["grid_type"] = line_stripped.split(":", 1)[1].strip()
            elif current and line_stripped.startswith("DSS File Name:"):
                current["dss_file"] = line_stripped.split(":", 1)[1].strip()
            elif current and line_stripped.startswith("DSS Pathname:"):
                current["dss_pathname"] = line_stripped.split(":", 1)[1].strip()
            elif line_stripped == "End:":
                current = None

    # Cross-checks
    dss_names = {d["name"] for d in dss}
    dss_size_by_name = {d["name"]: d["size_bytes"] for d in dss}
    grid_dss_names = {
        Path(e["dss_file"]).name for e in grid_entries if e["dss_file"]
    }
    dss_without_grid = sorted(dss_names - grid_dss_names)
    grid_without_dss = sorted(grid_dss_names - dss_names)

    expected_grid_count = 2 * len(dss)  # precip + temperature
    grid_count_ok = len(grid_entries) == expected_grid_count

    # ─── Attach DSS metadata to each event ───────────────────────────────
    # Match by the `_r{NNN}` suffix in DSS filenames + grid block names.
    pathnames_by_id: dict[str, dict[str, str]] = {}
    dss_file_by_id: dict[str, str] = {}
    for e in grid_entries:
        nm = e.get("name") or ""
        if "_r" not in nm:
            continue
        try:
            eid = str(int(nm.rsplit("_r", 1)[1]))
        except (IndexError, ValueError):
            continue
        slot = pathnames_by_id.setdefault(eid, {})
        gt = (e.get("grid_type") or "").lower()
        if "precip" in gt:
            slot["precip"] = e.get("dss_pathname") or ""
        elif "temp" in gt:
            slot["temp"] = e.get("dss_pathname") or ""
        if e.get("dss_file"):
            dss_file_by_id[eid] = Path(e["dss_file"]).name

    dss_size_by_id: dict[str, int] = {}
    for nm, sz in dss_size_by_name.items():
        if "_r" not in nm:
            continue
        try:
            eid = str(int(nm.rsplit("_r", 1)[1].split(".", 1)[0]))
            dss_size_by_id[eid] = sz
            dss_file_by_id.setdefault(eid, nm)
        except (IndexError, ValueError):
            pass

    outlier_ids: set[str] = set()
    for d in outlier_dss:
        if "_r" not in d["name"]:
            continue
        try:
            outlier_ids.add(str(int(d["name"].rsplit("_r", 1)[1].split(".", 1)[0])))
        except (IndexError, ValueError):
            pass

    for ev in events:
        eid = str(ev.get("id"))
        ev["dss_file"] = dss_file_by_id.get(eid)
        ev["dss_size"] = dss_size_by_id.get(eid)
        pn = pathnames_by_id.get(eid) or {}
        ev["precip_pathname"] = pn.get("precip")
        ev["temp_pathname"] = pn.get("temp")
        ev["has_precip"] = bool(ev["precip_pathname"])
        ev["has_temp"] = bool(ev["temp_pathname"])
        ev["is_outlier"] = eid in outlier_ids

    # Centroid containment — naive bounding box check against the
    # transposition polygon. Good enough for sanity (full point-in-polygon
    # would need shapely; this is a stdlib host).
    bbox = _polygon_bbox(audit_dir / "hydro_domains" / f"{catalog_id}-transposition.json")
    out_of_box = []
    if bbox is not None:
        lon_min, lat_min, lon_max, lat_max = bbox
        for ev in events:
            lon, lat = ev.get("max_lon"), ev.get("max_lat")
            if lon is None or lat is None:
                continue
            if not (lon_min <= lon <= lon_max and lat_min <= lat <= lat_max):
                out_of_box.append(ev)

    # ─── Duration sanity: actual storm window vs declared storm_duration ─
    # Pathnames look like `…/PRECIPITATION/20APR1992:0600/20APR1992:0700/…`
    # which only spans 1 hour (the catalog.grid pointer at the last grid).
    # The real storm window is in the STAC item — already on ev as
    # storm_start / storm_end. Anything off by >1 hour gets flagged.
    duration_mismatches: list[dict] = []
    if storm_duration:
        from datetime import datetime, timezone

        for ev in events:
            s, e = ev.get("storm_start"), ev.get("storm_end")
            if not s or not e:
                continue
            try:
                ds = datetime.fromisoformat(s.replace("Z", "+00:00"))
                de = datetime.fromisoformat(e.replace("Z", "+00:00"))
            except ValueError:
                continue
            hours = (de - ds).total_seconds() / 3600.0
            ev["actual_hours"] = hours
            if abs(hours - storm_duration) > 1:
                duration_mismatches.append(
                    {"id": ev["id"], "actual_hours": hours, "expected": storm_duration}
                )

    # ─── DSS bbox outside transposition domain ───────────────────────────
    bbox_out_of_domain: list[dict] = []
    if bbox is not None:
        lon_min, lat_min, lon_max, lat_max = bbox
        for ev in events:
            ebb = ev.get("bbox")
            if not (isinstance(ebb, list) and len(ebb) == 4):
                continue
            # Flag if the storm bbox is completely outside the transposition
            # bbox (no overlap at all). Partial overlap is normal and fine.
            elon_min, elat_min, elon_max, elat_max = ebb
            if (elon_max < lon_min or elon_min > lon_max
                    or elat_max < lat_min or elat_min > lat_max):
                bbox_out_of_domain.append(ev)

    # Year distribution
    years: dict[int, int] = {}
    for ev in events:
        ts = ev.get("storm_start") or ""
        if len(ts) >= 4 and ts[:4].isdigit():
            y = int(ts[:4])
            years[y] = years.get(y, 0) + 1

    # Mean precip min/max
    means = [ev["mean"] for ev in events if isinstance(ev.get("mean"), (int, float))]
    mean_range = (min(means), max(means)) if means else (None, None)

    return {
        "run_name": run_name,
        "catalog_id": catalog_id,
        "storm_duration": storm_duration,
        "top_n": top_n,
        "attrs": attrs,
        "progress": progress,
        "events": events,
        "n_events": len(events),
        "dss_files": dss,
        "n_dss": len(dss),
        "median_dss_bytes": median_bytes,
        "outlier_dss": outlier_dss,
        "grid_entries": grid_entries,
        "n_grid": len(grid_entries),
        "expected_grid": expected_grid_count,
        "grid_count_ok": grid_count_ok,
        "dss_without_grid": dss_without_grid,
        "grid_without_dss": grid_without_dss,
        "transposition_bbox": bbox,
        "out_of_box": out_of_box,
        "duration_mismatches": duration_mismatches,
        "bbox_out_of_domain": bbox_out_of_domain,
        "years": years,
        "mean_range": mean_range,
    }


def _walk_coords(obj, out: list[tuple[float, float]]) -> None:
    """Depth-first collect (lon, lat) pairs from any nested GeoJSON shape."""
    if isinstance(obj, list):
        if len(obj) >= 2 and all(isinstance(x, (int, float)) for x in obj[:2]):
            out.append((float(obj[0]), float(obj[1])))
        else:
            for el in obj:
                _walk_coords(el, out)
    elif isinstance(obj, dict):
        for v in obj.values():
            _walk_coords(v, out)


def _polygon_bbox(p: Path) -> tuple[float, float, float, float] | None:
    """Loose bbox over a GeoJSON polygon/feature. None if unreadable."""
    if not p.is_file():
        return None
    try:
        data = json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    coords: list[tuple[float, float]] = []
    _walk_coords(data, coords)
    if not coords:
        return None
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return min(lons), min(lats), max(lons), max(lats)


# ─── SVG map renderer (offline, no CDN) ──────────────────────────────────────
#
# The original template loaded Leaflet + OSM tiles from public CDNs, which
# typically fail on locked-down corporate/.mil networks — the map div ends up
# empty. Render the watershed/transposition/centroid overlay as inline SVG
# instead: self-contained, no external requests, and crisp at any zoom.


def _projector(bbox: tuple[float, float, float, float], width: int, pad_pct: float = 0.06):
    """Equirectangular-ish projection. Scales longitudes by cos(center lat) so
    polygons stay near-round at mid-latitudes. Returns (project_fn, viewbox).
    """
    lon_min, lat_min, lon_max, lat_max = bbox
    import math

    pad_lon = (lon_max - lon_min) * pad_pct or 0.05
    pad_lat = (lat_max - lat_min) * pad_pct or 0.05
    lon_min -= pad_lon
    lon_max += pad_lon
    lat_min -= pad_lat
    lat_max += pad_lat

    center_lat = (lat_min + lat_max) / 2
    k = math.cos(math.radians(center_lat))
    lon_span = (lon_max - lon_min) * k or 1e-6
    lat_span = (lat_max - lat_min) or 1e-6
    height = max(1, round(width * (lat_span / lon_span)))

    def project(lon: float, lat: float) -> tuple[float, float]:
        x = (lon - lon_min) * k / lon_span * width
        y = (lat_max - lat) / lat_span * height  # flip Y so north is up
        return x, y

    return project, (width, height)


def _ring_to_d(ring: list, project) -> str:
    """One GeoJSON linear ring → SVG path 'd' segment (closed)."""
    pts = []
    for c in ring:
        if not (isinstance(c, list) and len(c) >= 2):
            continue
        x, y = project(float(c[0]), float(c[1]))
        pts.append(f"{x:.1f},{y:.1f}")
    if not pts:
        return ""
    return "M" + "L".join(pts) + "Z"


def _geom_to_path_d(geom: dict | None, project) -> str:
    """GeoJSON Polygon / MultiPolygon / nested-feature → composite SVG 'd'."""
    if not geom:
        return ""
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    if gtype == "Polygon":
        return " ".join(_ring_to_d(r, project) for r in (coords or []))
    if gtype == "MultiPolygon":
        out = []
        for poly in coords or []:
            out.append(" ".join(_ring_to_d(r, project) for r in poly))
        return " ".join(out)
    return ""


def _feature_geom(d: dict | None) -> dict | None:
    """Pull the geometry out of a Feature; tolerate raw geometries too."""
    if not d:
        return None
    if d.get("type") in ("Polygon", "MultiPolygon"):
        return d
    return d.get("geometry")


def _ring_signed_area_km2(ring: list, lat_ref: float) -> float:
    """Equirectangular shoelace area in km². Sign reflects ring orientation.

    Accurate to <1% at mid-latitudes for areas under ~1M km². Good enough for
    catalog-scale watersheds — the alternative (spherical excess) is overkill
    for sanity-check visuals.
    """
    import math

    k_lon = 111.32 * math.cos(math.radians(lat_ref))
    k_lat = 111.32
    s = 0.0
    n = len(ring)
    if n < 3:
        return 0.0
    for i in range(n):
        x1, y1 = ring[i][0] * k_lon, ring[i][1] * k_lat
        x2, y2 = ring[(i + 1) % n][0] * k_lon, ring[(i + 1) % n][1] * k_lat
        s += x1 * y2 - x2 * y1
    return s / 2.0


def _polygon_area_km2(geom: dict | None) -> float:
    """Polygon / MultiPolygon area in km². Handles holes."""
    if not geom:
        return 0.0
    # Reference latitude from the polygon centroid bbox
    pts: list[tuple[float, float]] = []
    _walk_coords(geom.get("coordinates"), pts)
    if not pts:
        return 0.0
    lat_ref = sum(p[1] for p in pts) / len(pts)

    total = 0.0
    if geom["type"] == "Polygon":
        rings = geom["coordinates"] or []
        if rings:
            total += abs(_ring_signed_area_km2(rings[0], lat_ref))
            for hole in rings[1:]:
                total -= abs(_ring_signed_area_km2(hole, lat_ref))
    elif geom["type"] == "MultiPolygon":
        for poly in geom["coordinates"] or []:
            if poly:
                total += abs(_ring_signed_area_km2(poly[0], lat_ref))
                for hole in poly[1:]:
                    total -= abs(_ring_signed_area_km2(hole, lat_ref))
    return max(0.0, total)


def _polygon_centroid(geom: dict | None) -> tuple[float, float] | None:
    """Loose centroid — average of vertex coords. Good enough for label
    placement; not the area-weighted centroid you'd want for analytic work.
    """
    if not geom:
        return None
    pts: list[tuple[float, float]] = []
    _walk_coords(geom.get("coordinates"), pts)
    if not pts:
        return None
    return (sum(p[0] for p in pts) / len(pts),
            sum(p[1] for p in pts) / len(pts))


_CONUS_OUTLINE: dict | None = None


def _conus_outline() -> dict | None:
    """Lazy-load the CONUS state-boundary GeoJSON shipped under
    ``audit_assets/``. Used as a world-map backdrop in the report so users can
    verify watershed / transposition coordinates against familiar geography.
    Cached — same payload reused across all reports in a single build.
    """
    global _CONUS_OUTLINE
    if _CONUS_OUTLINE is None:
        p = ROOT / "audit_assets" / "conus_outline.json"
        if not p.is_file():
            _CONUS_OUTLINE = {}
            return None
        try:
            _CONUS_OUTLINE = json.loads(p.read_text())
        except (OSError, json.JSONDecodeError):
            _CONUS_OUTLINE = {}
            return None
    return _CONUS_OUTLINE or None


def _build_conus_overview(
    watershed: dict | None,
    transposition: dict | None,
    width: int = 520,
) -> str:
    """A small CONUS reference map with the catalog's transposition bbox and
    watershed marker highlighted. Confirms at a glance that the configured
    geography lands where it should — Iowa, West Virginia, etc.
    """
    conus = _conus_outline()
    if not conus:
        return '<div class="conus-empty">CONUS outline data unavailable.</div>'

    # Fixed projection across all reports so visual scale stays comparable.
    bbox = (-125.0, 24.0, -66.5, 49.5)
    project, (w, h) = _projector(bbox, width=width, pad_pct=0.02)

    parts = [
        f'<svg class="conus-map" viewBox="0 0 {w} {h}" '
        f'xmlns="http://www.w3.org/2000/svg" '
        f'preserveAspectRatio="xMidYMid meet">',
        '<defs><linearGradient id="bg-c" x1="0" y1="0" x2="0" y2="1">'
        '<stop offset="0%" stop-color="#0c4a6e"/>'
        '<stop offset="100%" stop-color="#082f49"/>'
        '</linearGradient></defs>',
        f'<rect width="{w}" height="{h}" fill="url(#bg-c)"/>',
    ]

    # State outlines (gray fill, lighter strokes)
    parts.append('<g class="states">')
    for feat in (conus.get("features") or []):
        d = _geom_to_path_d(feat.get("geometry"), project)
        if d:
            parts.append(
                f'<path d="{d}" fill="#1e293b" fill-opacity="0.95" '
                f'stroke="#475569" stroke-width="0.5"/>'
            )
    parts.append("</g>")

    # Transposition domain footprint
    if transposition:
        d = _geom_to_path_d(_feature_geom(transposition), project)
        if d:
            parts.append(
                f'<path d="{d}" fill="#ea580c" fill-opacity="0.35" '
                f'stroke="#fb923c" stroke-width="1.5"/>'
            )

    # Watershed footprint (small but bold)
    if watershed:
        d = _geom_to_path_d(_feature_geom(watershed), project)
        if d:
            parts.append(
                f'<path d="{d}" fill="#2563eb" fill-opacity="0.85" '
                f'stroke="#60a5fa" stroke-width="1.5"/>'
            )
        wc = _polygon_centroid(_feature_geom(watershed))
        if wc:
            cx, cy = project(wc[0], wc[1])
            parts.append(
                f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="5" '
                f'fill="none" stroke="#facc15" stroke-width="2"/>'
                f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="2" fill="#facc15"/>'
            )

    parts.append("</svg>")
    return "".join(parts)


def _data_integrity_checks(
    catalog_id: str,
    watershed: dict | None,
    transposition: dict | None,
    transposition_valid: dict | None,
) -> list[dict]:
    """Auto-verify that the domain files are what they claim to be.

    Catches a class of subtle mis-routing bugs: watershed/transposition file
    contents getting swapped at the producer (so the "watershed" GeoJSON
    actually contains the transposition polygon, or vice versa). The user
    explicitly asked whether we might be doing that; this codifies the
    answer so every report re-verifies on rebuild.

    Each check returns {key, status: ok|warn|bad, detail}.
    """
    out: list[dict] = []

    expected = {
        "watershed": ("watershed", "Watershed file declares 'watershed'"),
        "transposition": ("transposition_region",
                           "Transposition file declares 'transposition_region'"),
        "transposition_valid": ("valid_transposition_region",
                                 "Valid file declares 'valid_transposition_region'"),
    }
    feats = {
        "watershed": watershed,
        "transposition": transposition,
        "transposition_valid": transposition_valid,
    }
    for slot, (expected_type, label) in expected.items():
        feat = feats[slot]
        if not feat:
            out.append({"key": slot + "_present", "status": "warn",
                         "detail": f"{label} — file missing"})
            continue
        declared = (feat.get("properties") or {}).get("hydro_domain:type")
        if declared == expected_type:
            out.append({"key": slot + "_type", "status": "ok",
                         "detail": label})
        else:
            out.append({"key": slot + "_type", "status": "bad",
                         "detail": f"{label} — found '{declared}'"})

    # Watershed must be smaller than transposition
    wa = _polygon_area_km2(_feature_geom(watershed))
    ta = _polygon_area_km2(_feature_geom(transposition))
    if wa and ta:
        if wa < ta:
            out.append({"key": "area_order", "status": "ok",
                         "detail": f"Watershed ({wa:,.0f} km²) < transposition "
                                   f"({ta:,.0f} km²)"})
        else:
            out.append({"key": "area_order", "status": "bad",
                         "detail": f"Watershed ({wa:,.0f} km²) ≥ transposition "
                                   f"({ta:,.0f} km²) — likely SWAPPED"})

    # Watershed centroid should be inside the transposition bbox
    wc = _polygon_centroid(_feature_geom(watershed))
    tb = transposition.get("bbox") if isinstance(transposition, dict) else None
    if wc and isinstance(tb, list) and len(tb) >= 4:
        inside = (tb[0] <= wc[0] <= tb[2] and tb[1] <= wc[1] <= tb[3])
        if inside:
            out.append({"key": "ws_in_tx", "status": "ok",
                         "detail": f"Watershed centroid ({wc[0]:.2f}, "
                                   f"{wc[1]:.2f}) is inside the transposition bbox"})
        else:
            out.append({"key": "ws_in_tx", "status": "bad",
                         "detail": f"Watershed centroid ({wc[0]:.2f}, "
                                   f"{wc[1]:.2f}) outside transposition bbox "
                                   f"{tb} — likely SWAPPED"})

    # IDs in the GeoJSON properties (extra cross-check: the STAC id should
    # contain the catalog name)
    for slot, feat in feats.items():
        if not feat:
            continue
        fid = (feat.get("id") or "")
        if not fid:
            continue
        if catalog_id.lower() in fid.lower():
            out.append({"key": slot + "_id", "status": "ok",
                         "detail": f"{slot} STAC id is '{fid}'"})
        else:
            out.append({"key": slot + "_id", "status": "warn",
                         "detail": f"{slot} STAC id '{fid}' doesn't reference "
                                   f"catalog '{catalog_id}'"})

    return out


def _build_watershed_zoom(
    watershed: dict | None, width: int = 280
) -> tuple[str, dict]:
    """Tight-cropped SVG of the watershed alone, with vertex markers visible.

    At the transposition-domain zoom level a watershed is a barely-visible
    blob — visually mistaken for a circle even when its 100+ vertices trace
    a real ridge-line. This dedicated zoom proves the actual shape, and the
    returned ``circularity`` metric (4πA/P², in [0, 1]) tells you whether
    the geometry is a synthetic buffer (≈1) or a real watershed (<0.8).
    """
    g = _feature_geom(watershed)
    if not g:
        return ("", {})

    pts: list[tuple[float, float]] = []
    _walk_coords(g.get("coordinates"), pts)
    if not pts:
        return ("", {})

    lons = [p[0] for p in pts]
    lats = [p[1] for p in pts]
    bbox = (min(lons), min(lats), max(lons), max(lats))
    project, (w, h) = _projector(bbox, width=width, pad_pct=0.10)

    parts = [
        f'<svg class="ws-zoom" viewBox="0 0 {w} {h}" '
        f'xmlns="http://www.w3.org/2000/svg" '
        f'preserveAspectRatio="xMidYMid meet">',
        f'<rect width="{w}" height="{h}" fill="#082f49"/>',
    ]

    # Faint graticule
    import math
    lat_min, lat_max = bbox[1], bbox[3]
    lon_min, lon_max = bbox[0], bbox[2]
    span = max(lon_max - lon_min, lat_max - lat_min)
    step = 0.05 if span < 0.5 else (0.1 if span < 1 else 0.25)
    g_lat = math.floor(lat_min / step) * step
    while g_lat <= lat_max:
        x0, y0 = project(lon_min, g_lat)
        x1, y1 = project(lon_max, g_lat)
        parts.append(
            f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" '
            f'stroke="#1e3a5f" stroke-width="0.5" opacity="0.5"/>'
        )
        g_lat += step
    g_lon = math.floor(lon_min / step) * step
    while g_lon <= lon_max:
        x0, y0 = project(g_lon, lat_min)
        x1, y1 = project(g_lon, lat_max)
        parts.append(
            f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" '
            f'stroke="#1e3a5f" stroke-width="0.5" opacity="0.5"/>'
        )
        g_lon += step

    # Watershed polygon with crisp stroke
    d = _geom_to_path_d(g, project)
    parts.append(
        f'<path d="{d}" fill="#60a5fa" fill-opacity="0.32" '
        f'stroke="#bfdbfe" stroke-width="1.6" stroke-linejoin="round"/>'
    )
    # Vertex markers so the actual shape can't be mistaken for a smoothed curve
    rings = g.get("coordinates") or []
    outer = rings[0] if rings and isinstance(rings[0], list) else []
    for v in outer:
        if not (isinstance(v, list) and len(v) >= 2):
            continue
        x, y = project(float(v[0]), float(v[1]))
        parts.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="1.2" '
            f'fill="#fef3c7" fill-opacity="0.85"/>'
        )

    # Shape metric — Polsby–Popper circularity (4πA / P²); a circle = 1.
    area_km2 = _polygon_area_km2(g)
    lat_ref = sum(lats) / len(lats)
    k_lon = math.cos(math.radians(lat_ref)) * 111.32
    k_lat = 111.32
    perim_km = 0.0
    if outer:
        for i in range(len(outer)):
            v1 = outer[i]
            v2 = outer[(i + 1) % len(outer)]
            dx = (v2[0] - v1[0]) * k_lon
            dy = (v2[1] - v1[1]) * k_lat
            perim_km += math.hypot(dx, dy)
    circularity = (4 * math.pi * area_km2 / (perim_km ** 2)) if perim_km else 0.0

    parts.append("</svg>")
    return "".join(parts), {
        "vertices": len(outer),
        "perim_km": perim_km,
        "area_km2": area_km2,
        "circularity": circularity,
    }


def _build_domain_section(
    watershed: dict | None,
    transposition: dict | None,
    transposition_valid: dict | None,
    width: int = 760,
) -> tuple[str, dict]:
    """Dedicated 'domain' SVG — polygons only, labeled, with scale bar.
    Returns (svg, stats) where stats has area_km2 + extent per layer.
    """
    pts: list[tuple[float, float]] = []
    for f in (watershed, transposition, transposition_valid):
        g = _feature_geom(f)
        if g:
            _walk_coords(g.get("coordinates"), pts)
    if not pts:
        return ("<div class='map-empty'>No domain geometry.</div>", {})

    lons = [p[0] for p in pts]
    lats = [p[1] for p in pts]
    bbox = (min(lons), min(lats), max(lons), max(lats))
    project, (w, h) = _projector(bbox, width=width, pad_pct=0.08)

    parts = [
        f'<svg class="domain-svg" viewBox="0 0 {w} {h}" '
        f'preserveAspectRatio="xMidYMid meet" '
        f'xmlns="http://www.w3.org/2000/svg" role="img" '
        f'aria-label="hydrologic domain">',
        '<defs><linearGradient id="bg-d" x1="0" y1="0" x2="0" y2="1">'
        '<stop offset="0%" stop-color="#1e293b"/>'
        '<stop offset="100%" stop-color="#0f172a"/>'
        '</linearGradient></defs>',
        f'<rect width="{w}" height="{h}" fill="url(#bg-d)"/>',
    ]

    # Light graticule for spatial reference
    import math
    lon_min, lat_min, lon_max, lat_max = bbox
    span = max(lon_max - lon_min, lat_max - lat_min)
    step = 0.25 if span < 2 else (0.5 if span < 5 else 1.0)
    g_lat = math.floor(lat_min / step) * step
    while g_lat <= lat_max:
        x0, y0 = project(lon_min, g_lat)
        x1, y1 = project(lon_max, g_lat)
        parts.append(
            f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" '
            f'stroke="#334155" stroke-width="0.5" opacity="0.55"/>'
            f'<text x="6" y="{y0:.1f}" fill="#64748b" font-size="10" '
            f'dominant-baseline="middle">{g_lat:.1f}°N</text>'
        )
        g_lat += step
    g_lon = math.floor(lon_min / step) * step
    while g_lon <= lon_max:
        x0, y0 = project(g_lon, lat_min)
        x1, y1 = project(g_lon, lat_max)
        parts.append(
            f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" '
            f'stroke="#334155" stroke-width="0.5" opacity="0.55"/>'
            f'<text x="{x0:.1f}" y="{h - 4:.1f}" fill="#64748b" font-size="10" '
            f'text-anchor="middle">{g_lon:.1f}°</text>'
        )
        g_lon += step

    # Layer rendering — outer-to-inner so labels are placed last.
    # Each tuple: (label, feature, stroke, fill, fill_opacity, stroke_width, dash).
    # Fill-opacity is critical: the valid region is *nested* inside the
    # transposition domain, so a solid fill would blanket everything below.
    parts.append(
        '<defs><pattern id="stripes-t" patternUnits="userSpaceOnUse" '
        'width="6" height="6" patternTransform="rotate(45)">'
        '<rect width="6" height="6" fill="#ea580c" fill-opacity="0.08"/>'
        '<line x1="0" y1="0" x2="0" y2="6" stroke="#ea580c" '
        'stroke-width="1.2" stroke-opacity="0.45"/></pattern></defs>'
    )
    layers: list[tuple[str, dict | None, str, str, float, float, str]] = [
        ("Transposition Domain", transposition,
         "#fb923c", "url(#stripes-t)", 1.0, 2.0, "6 4"),
        ("Valid Region", transposition_valid,
         "#22c55e", "#22c55e", 0.18, 1.4, ""),
        ("Watershed", watershed, "#60a5fa", "#2563eb", 0.55, 2.5, ""),
    ]

    stats: dict = {}
    for label, feat, stroke, fill, fo, sw, dash in layers:
        g = _feature_geom(feat)
        if not g:
            continue
        d = _geom_to_path_d(g, project)
        if not d:
            continue
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        # Pattern fills don't take fill-opacity (the pattern handles its own
        # transparency). Plain colors do.
        fo_attr = "" if fill.startswith("url(") else f' fill-opacity="{fo}"'
        parts.append(
            f'<path d="{d}" fill="{fill}"{fo_attr} '
            f'stroke="{stroke}" stroke-width="{sw}"{dash_attr}/>'
        )
        area = _polygon_area_km2(g)
        bbox_l = (feat.get("bbox") if isinstance(feat, dict) else None)
        stats[label] = {"area_km2": area, "bbox": bbox_l}

    # Label centroids
    for label, feat, _stroke, _fill, _fo, _sw, _dash in layers:
        g = _feature_geom(feat)
        c = _polygon_centroid(g) if g else None
        if not c:
            continue
        cx, cy = project(c[0], c[1])
        # White text with dark halo so it reads on either fill
        parts.append(
            f'<text x="{cx:.1f}" y="{cy:.1f}" fill="#0f172a" '
            f'stroke="#0f172a" stroke-width="3" paint-order="stroke" '
            f'font-size="13" font-weight="700" text-anchor="middle">{label}</text>'
            f'<text x="{cx:.1f}" y="{cy:.1f}" fill="#f8fafc" '
            f'font-size="13" font-weight="700" text-anchor="middle">{label}</text>'
        )

    # Scale bar — 50 km if span is reasonable, else 100/200/500 km
    span_km = (lat_max - lat_min) * 111.32
    if span_km < 100:
        bar_km = 10
    elif span_km < 300:
        bar_km = 50
    elif span_km < 800:
        bar_km = 100
    else:
        bar_km = 200
    k_lon = math.cos(math.radians((lat_min + lat_max) / 2)) * 111.32
    bar_deg = bar_km / k_lon
    bar_px = (bar_deg / (lon_max - lon_min) * w)
    sx, sy = w - bar_px - 20, h - 22
    parts.append(
        f'<g class="scale">'
        f'<rect x="{sx:.1f}" y="{sy:.1f}" width="{bar_px:.1f}" height="4" '
        f'fill="#f1f5f9"/>'
        f'<text x="{sx + bar_px / 2:.1f}" y="{sy - 4:.1f}" '
        f'fill="#cbd5e1" font-size="11" text-anchor="middle">{bar_km} km</text>'
        "</g>"
    )

    parts.append("</svg>")
    return "".join(parts), stats


def _color_for_precip(val: float, vmin: float, vmax: float) -> str:
    """Sequential viridis-like ramp (5 stops). Returns #RRGGBB.

    Used to color event dots by mean precipitation so the eye picks out the
    biggest storms instantly — a single color is the wrong choice when we have
    460 dots all jumbled together.
    """
    if vmax <= vmin:
        t = 0.5
    else:
        t = max(0.0, min(1.0, (val - vmin) / (vmax - vmin)))
    stops = [
        (0.0, (68, 1, 84)),     # deep purple
        (0.25, (59, 82, 139)),  # blue
        (0.5, (33, 145, 140)),  # teal
        (0.75, (94, 201, 98)),  # green
        (1.0, (253, 231, 37)),  # yellow
    ]
    for i in range(len(stops) - 1):
        t0, c0 = stops[i]
        t1, c1 = stops[i + 1]
        if t <= t1:
            f = (t - t0) / (t1 - t0) if t1 > t0 else 0
            r = int(c0[0] + (c1[0] - c0[0]) * f)
            g = int(c0[1] + (c1[1] - c0[1]) * f)
            b = int(c0[2] + (c1[2] - c0[2]) * f)
            return f"#{r:02x}{g:02x}{b:02x}"
    return "#fde725"


def _build_svg_map(
    watershed: dict | None,
    transposition: dict | None,
    transposition_valid: dict | None,
    events: list[dict],
    width: int = 900,
) -> tuple[str, dict]:
    """Build an inline-SVG map for the report.

    Returns (svg_markup, meta). meta carries per-event projected positions and
    color stops so the playback JS can re-use them without re-projecting.
    """
    pts: list[tuple[float, float]] = []
    for f in (watershed, transposition, transposition_valid):
        g = _feature_geom(f)
        if g:
            _walk_coords(g.get("coordinates"), pts)
    for ev in events:
        lon, lat = ev.get("max_lon"), ev.get("max_lat")
        if lon is not None and lat is not None:
            pts.append((float(lon), float(lat)))

    if not pts:
        return ("<div class='map-empty'>No geometry available.</div>", {})

    lons = [p[0] for p in pts]
    lats = [p[1] for p in pts]
    bbox = (min(lons), min(lats), max(lons), max(lats))
    project, (w, h) = _projector(bbox, width=width)

    parts = [
        f'<svg class="map-svg" viewBox="0 0 {w} {h}" '
        f'preserveAspectRatio="xMidYMid meet" '
        f'xmlns="http://www.w3.org/2000/svg" role="img" '
        f'aria-label="storm map">',
        # Subtle background gradient so empty map regions don't look broken.
        '<defs>'
        '<linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">'
        '<stop offset="0%" stop-color="#0f172a"/>'
        '<stop offset="100%" stop-color="#1e293b"/>'
        '</linearGradient>'
        '<filter id="glow"><feGaussianBlur stdDeviation="2.5" result="b"/>'
        '<feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/>'
        '</feMerge></filter>'
        '</defs>',
        f'<rect width="{w}" height="{h}" fill="url(#bg)"/>',
    ]

    # Graticule — light lat/lon grid every ~1° for spatial reference
    import math
    lon_min, lat_min, lon_max, lat_max = bbox
    span = max(lon_max - lon_min, lat_max - lat_min)
    step = 0.25 if span < 2 else (0.5 if span < 5 else 1.0)
    g_lat = math.floor(lat_min / step) * step
    while g_lat <= lat_max:
        x0, y0 = project(lon_min, g_lat)
        x1, y1 = project(lon_max, g_lat)
        parts.append(
            f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" '
            f'stroke="#334155" stroke-width="0.5" opacity="0.5"/>'
        )
        g_lat += step
    g_lon = math.floor(lon_min / step) * step
    while g_lon <= lon_max:
        x0, y0 = project(g_lon, lat_min)
        x1, y1 = project(g_lon, lat_max)
        parts.append(
            f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" '
            f'stroke="#334155" stroke-width="0.5" opacity="0.5"/>'
        )
        g_lon += step

    # Transposition domain (orange dashed) — biggest area, drawn first
    d = _geom_to_path_d(_feature_geom(transposition), project)
    if d:
        parts.append(
            f'<path d="{d}" fill="#ea580c" fill-opacity="0.07" '
            f'stroke="#ea580c" stroke-width="2" stroke-dasharray="6 4"/>'
        )

    # Valid-transposition region (green, lower-opacity fill)
    d = _geom_to_path_d(_feature_geom(transposition_valid), project)
    if d:
        parts.append(
            f'<path d="{d}" fill="#16a34a" fill-opacity="0.10" '
            f'stroke="#22c55e" stroke-width="1"/>'
        )

    # Watershed (blue solid)
    d = _geom_to_path_d(_feature_geom(watershed), project)
    if d:
        parts.append(
            f'<path d="{d}" fill="#2563eb" fill-opacity="0.22" '
            f'stroke="#60a5fa" stroke-width="2"/>'
        )

    # Event dots — sized & colored by mean precip; tag each dot with its event id
    means = [ev["mean"] for ev in events if isinstance(ev.get("mean"), (int, float))]
    vmin = min(means) if means else 0.0
    vmax = max(means) if means else 1.0
    dots_g = ['<g class="dots">']
    positions: dict[str, list[float]] = {}
    colors: dict[str, str] = {}
    bboxes_svg: dict[str, list[float]] = {}
    for ev in events:
        lon, lat = ev.get("max_lon"), ev.get("max_lat")
        if lon is None or lat is None:
            continue
        mean = ev.get("mean") if isinstance(ev.get("mean"), (int, float)) else vmin
        x, y = project(float(lon), float(lat))
        r = 3.0 + 5.0 * ((mean - vmin) / (vmax - vmin) if vmax > vmin else 0.5)
        color = _color_for_precip(float(mean), vmin, vmax)
        eid = str(ev.get("id"))
        positions[eid] = [round(x, 1), round(y, 1)]
        colors[eid] = color
        # Pre-project the DSS bounding box so JS can drop a rectangle on the
        # map without re-implementing the projection.
        ebb = ev.get("bbox")
        if isinstance(ebb, list) and len(ebb) == 4:
            x0, y0 = project(ebb[0], ebb[3])  # NW = (lon_min, lat_max)
            x1, y1 = project(ebb[2], ebb[1])  # SE = (lon_max, lat_min)
            bboxes_svg[eid] = [
                round(min(x0, x1), 1), round(min(y0, y1), 1),
                round(abs(x1 - x0), 1), round(abs(y1 - y0), 1),
            ]
        dots_g.append(
            f'<circle class="dot" data-id="{html.escape(eid)}" '
            f'cx="{x:.1f}" cy="{y:.1f}" r="{r:.1f}" '
            f'fill="{color}" fill-opacity="0.78" '
            f'stroke="#0b1220" stroke-width="0.6"/>'
        )
    dots_g.append("</g>")
    parts.extend(dots_g)

    # Active-event highlight (pulse) drawn on top, hidden by default
    parts.append(
        '<rect id="active-bbox" x="0" y="0" width="0" height="0" '
        'fill="#facc15" fill-opacity="0.08" stroke="#facc15" '
        'stroke-width="1.5" stroke-dasharray="4 3" style="display:none" '
        'pointer-events="none"/>'
    )
    parts.append(
        '<circle id="active-dot" cx="0" cy="0" r="0" fill="none" '
        'stroke="#facc15" stroke-width="2.5" filter="url(#glow)" '
        'style="display:none"/>'
    )

    # Legend (color ramp) bottom-left
    legend_w = 180
    legend_h = 10
    lx = 18
    ly = h - 30
    parts.append(
        f'<g class="legend" transform="translate({lx},{ly})">'
        '<defs><linearGradient id="ramp" x1="0" x2="1">'
        '<stop offset="0%" stop-color="#440154"/>'
        '<stop offset="25%" stop-color="#3b528b"/>'
        '<stop offset="50%" stop-color="#21918c"/>'
        '<stop offset="75%" stop-color="#5ec962"/>'
        '<stop offset="100%" stop-color="#fde725"/>'
        '</linearGradient></defs>'
        f'<rect width="{legend_w}" height="{legend_h}" fill="url(#ramp)" rx="2"/>'
        f'<text x="0" y="-4" fill="#cbd5e1" font-size="10">mean precip (in)</text>'
        f'<text x="0" y="{legend_h + 12}" fill="#cbd5e1" font-size="10">'
        f'{vmin:.1f}</text>'
        f'<text x="{legend_w}" y="{legend_h + 12}" fill="#cbd5e1" '
        f'font-size="10" text-anchor="end">{vmax:.1f}</text>'
        "</g>"
    )

    parts.append("</svg>")
    meta = {
        "positions": positions,
        "colors": colors,
        "bboxes": bboxes_svg,
        "viewbox": [w, h],
        "vmin": vmin,
        "vmax": vmax,
    }
    return "".join(parts), meta


# ─── HTML report ──────────────────────────────────────────────────────────────

_REPORT_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Audit — __CATALOG_ID__</title>
<style>
  :root {
    --bg: #f7f8fb; --card: #fff; --ink: #0f172a; --mute: #64748b;
    --line: #e2e8f0; --accent: #1e3a5f; --accent2: #2563eb;
    --ok: #059669; --warn: #d97706; --bad: #dc2626;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui,
                      sans-serif;
         color: var(--ink); background: var(--bg); line-height: 1.45; }
  header { background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 100%);
           color: #fff; padding: 22px 28px; }
  header h1 { margin: 0; font-size: 22px; letter-spacing: -0.01em; }
  header .sub { font-size: 13px; opacity: 0.85; margin-top: 6px;
                 font-variant-numeric: tabular-nums; }
  header .nav { font-size: 12px; margin-top: 10px; }
  header .nav a { color: #93c5fd; margin-right: 14px; text-decoration: none; }
  header .nav a:hover { text-decoration: underline; }
  main { max-width: 1400px; margin: 0 auto; padding: 22px 28px 60px; }
  h2 { font-size: 13px; margin: 30px 0 10px; padding-bottom: 6px;
       text-transform: uppercase; letter-spacing: 0.06em; color: var(--mute);
       border-bottom: 1px solid var(--line); }
  h2 .meta { font-weight: normal; text-transform: none; letter-spacing: 0;
             color: var(--mute); font-size: 12px; margin-left: 8px; }

  /* Summary cards */
  .stats { display: grid; gap: 12px;
           grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
           margin-bottom: 8px; }
  .stat { background: var(--card); border: 1px solid var(--line);
          padding: 12px 14px; border-radius: 8px;
          box-shadow: 0 1px 2px rgba(15,23,42,0.04); }
  .stat .label { font-size: 10px; color: var(--mute); text-transform: uppercase;
                 letter-spacing: 0.06em; }
  .stat .value { font-size: 20px; font-weight: 700; margin-top: 4px;
                 font-variant-numeric: tabular-nums; }
  .stat .sub { font-size: 11px; color: var(--mute); margin-top: 2px; }
  .stat.warn { border-color: #fcd34d; background: #fffbeb; }
  .stat.warn .value { color: #b45309; }
  .stat.ok { border-color: #6ee7b7; background: #ecfdf5; }
  .stat.ok .value { color: #047857; }

  /* Anomalies */
  .anomaly { background: #fff5f5; border-left: 3px solid #ef4444;
             padding: 10px 14px; border-radius: 4px; margin: 8px 0;
             box-shadow: 0 1px 2px rgba(15,23,42,0.04); }
  .anomaly h3 { font-size: 13px; margin: 0 0 6px; color: #b91c1c; }
  .anomaly code { background: #fff; padding: 1px 5px; border-radius: 3px;
                  border: 1px solid #fecaca; font-size: 11px; }
  .anomaly ul { margin: 6px 0 0; padding-left: 22px; font-size: 12px;
                color: #475569; }

  /* Data integrity */
  .integrity { display: grid;
               grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
               gap: 6px 14px; background: var(--card); border: 1px solid var(--line);
               border-radius: 8px; padding: 10px 14px; margin-bottom: 6px;
               box-shadow: 0 1px 2px rgba(15,23,42,0.04); }
  .integrity .row { display: flex; align-items: flex-start; gap: 8px;
                     font-size: 12px; line-height: 1.4; padding: 2px 0; }
  .integrity .ic { display: inline-block; width: 16px; flex: 0 0 16px;
                    text-align: center; font-weight: 700; }
  .integrity .ic.ok { color: #059669; }
  .integrity .ic.warn { color: #d97706; }
  .integrity .ic.bad { color: #dc2626; }
  .integrity .row.bad .d { color: #991b1b; font-weight: 600; }

  /* Domain section */
  .domain-wrap { display: grid; gap: 16px; align-items: start;
                  grid-template-columns: minmax(0, 1.8fr) minmax(220px, 1fr);
                  margin-bottom: 8px; }
  @media (max-width: 900px) { .domain-wrap { grid-template-columns: 1fr; } }
  .domain-map { background: #0f172a; border-radius: 10px; overflow: hidden;
                 box-shadow: 0 4px 14px rgba(15,23,42,0.15); }
  .domain-svg { display: block; width: 100%; height: auto; }
  .domain-side { display: flex; flex-direction: column; gap: 12px; }
  .conus-card { background: #0f172a; border-radius: 10px; padding: 10px;
                 box-shadow: 0 4px 14px rgba(15,23,42,0.15); }
  .conus-card .conus-title { color: #94a3b8; font-size: 10px;
                              text-transform: uppercase; letter-spacing: 0.06em;
                              margin-bottom: 6px; padding: 0 4px; }
  .conus-card .conus-inner { border-radius: 6px; overflow: hidden;
                              background: #082f49; }
  .conus-map { display: block; width: 100%; height: auto; }
  .conus-legend { color: #cbd5e1; font-size: 11px; margin-top: 6px;
                   padding: 0 4px; display: flex; gap: 12px; flex-wrap: wrap; }
  .conus-legend .sw { display: inline-block; width: 10px; height: 10px;
                       border-radius: 2px; vertical-align: middle;
                       margin-right: 5px; }
  .ws-card { background: #082f49; border-radius: 10px; padding: 10px;
              box-shadow: 0 4px 14px rgba(15,23,42,0.15); }
  .ws-card .t { color: #94a3b8; font-size: 10px; text-transform: uppercase;
                 letter-spacing: 0.06em; margin-bottom: 6px; padding: 0 4px;
                 display: flex; justify-content: space-between;
                 align-items: baseline; }
  .ws-card .t small { color: #cbd5e1; font-size: 10px;
                       text-transform: none; letter-spacing: 0; }
  .ws-card .ws-zoom { display: block; width: 100%; height: auto;
                       border-radius: 6px; background: #082f49; }
  .ws-card .metrics { display: grid; grid-template-columns: 1fr 1fr;
                       gap: 8px; margin-top: 8px; padding: 0 4px;
                       font-size: 11px; color: #cbd5e1;
                       font-variant-numeric: tabular-nums; }
  .ws-card .metrics .l { font-size: 9.5px; color: #64748b;
                          text-transform: uppercase; letter-spacing: 0.06em; }
  .ws-card .metrics .v { font-size: 13px; font-weight: 600;
                          color: #f1f5f9; margin-top: 1px; }
  .ws-card .metrics .v.warn { color: #fbbf24; }
  .domain-stats { display: grid; gap: 8px; }
  .domain-stat { background: var(--card); border: 1px solid var(--line);
                  border-radius: 8px; padding: 9px 11px;
                  border-left: 3px solid var(--accent2);
                  box-shadow: 0 1px 2px rgba(15,23,42,0.04); }
  .domain-stat.t { border-left-color: #ea580c; }
  .domain-stat.v { border-left-color: #22c55e; }
  .domain-stat .l { font-size: 10px; color: var(--mute);
                     text-transform: uppercase; letter-spacing: 0.06em; }
  .domain-stat .a { font-size: 17px; font-weight: 700; margin-top: 2px;
                     font-variant-numeric: tabular-nums; }
  .domain-stat .e { font-size: 11px; color: var(--mute); margin-top: 2px;
                     font-variant-numeric: tabular-nums; word-break: break-all; }

  /* Map */
  .map-wrap { position: relative; background: #0f172a; border-radius: 10px;
              overflow: hidden; box-shadow: 0 4px 14px rgba(15,23,42,0.15);
              margin-bottom: 8px; }
  .map-svg { display: block; width: 100%; height: auto; }
  .map-svg .dot { transition: r 120ms, fill-opacity 120ms; cursor: pointer; }
  .map-svg .dot:hover { fill-opacity: 1; stroke: #fff; stroke-width: 1.2; }
  .map-legend { position: absolute; top: 12px; right: 12px; color: #cbd5e1;
                font-size: 11px; background: rgba(15,23,42,0.7);
                padding: 8px 11px; border-radius: 6px;
                border: 1px solid rgba(148,163,184,0.25); }
  .map-legend .swatch { display: inline-block; width: 11px; height: 11px;
                         border-radius: 2px; vertical-align: middle;
                         margin-right: 6px; }
  .map-legend .row { margin: 2px 0; }

  /* Playback */
  .play { display: grid; grid-template-columns: 220px 1fr; gap: 18px;
          background: var(--card); border: 1px solid var(--line);
          border-radius: 10px; padding: 14px; margin-bottom: 8px;
          box-shadow: 0 1px 2px rgba(15,23,42,0.04); }
  .play .thumb-box { aspect-ratio: 1/1; background: #0f172a;
                     border-radius: 6px; overflow: hidden; position: relative; }
  .play .thumb-box img { width: 100%; height: 100%; object-fit: contain;
                          background: #0f172a; cursor: zoom-in; }
  .play .ctrl { font-variant-numeric: tabular-nums; }
  .play .row1 { display: flex; align-items: center; gap: 10px;
                margin-bottom: 6px; }
  .play .btn { background: var(--accent2); color: #fff; border: 0;
               padding: 7px 14px; border-radius: 999px; cursor: pointer;
               font-size: 13px; font-weight: 600; }
  .play .btn:hover { background: #1d4ed8; }
  .play .btn.pause { background: var(--mute); }
  .play .seek { flex: 1; -webkit-appearance: none; appearance: none;
                background: linear-gradient(90deg, var(--accent2) 0%,
                            var(--accent2) var(--pct, 0%),
                            #e2e8f0 var(--pct, 0%), #e2e8f0 100%);
                height: 5px; border-radius: 3px; outline: none; }
  .play .seek::-webkit-slider-thumb { -webkit-appearance: none;
                width: 14px; height: 14px; border-radius: 50%;
                background: var(--accent2); cursor: pointer; }
  .play .seek::-moz-range-thumb { width: 14px; height: 14px; border-radius: 50%;
                                   background: var(--accent2); cursor: pointer;
                                   border: 0; }
  .play .label { font-size: 11px; text-transform: uppercase;
                 letter-spacing: 0.06em; color: var(--mute); }
  .play .now { font-size: 18px; font-weight: 700; margin-top: 2px;
               font-variant-numeric: tabular-nums; }
  .play .now small { font-size: 11px; font-weight: 500; color: var(--mute);
                     margin-left: 6px; text-transform: uppercase;
                     letter-spacing: 0.04em; }
  .play .stats-row { display: grid; gap: 10px;
                     grid-template-columns: repeat(auto-fit, minmax(80px, 1fr));
                     margin-top: 10px; padding-top: 10px;
                     border-top: 1px solid var(--line); }
  .play .stats-row .label { display: block; }
  .play .stats-row .val { font-size: 14px; font-weight: 600;
                          font-variant-numeric: tabular-nums; }
  .play .speed { font-size: 11px; padding: 4px 8px; border-radius: 4px;
                 border: 1px solid var(--line); background: #fff;
                 cursor: pointer; }
  .play .dss-block { margin-top: 10px; padding-top: 10px;
                     border-top: 1px solid var(--line); display: grid;
                     gap: 5px; }
  .play .dss-row { display: flex; align-items: center; gap: 10px;
                    font-size: 12px; line-height: 1.4; }
  .play .dss-row .k { width: 60px; flex: 0 0 60px; color: var(--mute);
                       font-size: 10px; text-transform: uppercase;
                       letter-spacing: 0.06em; }
  .play .dss-row .v { font-variant-numeric: tabular-nums; }
  .play .dss-row code.v { background: #f1f5f9; padding: 2px 6px;
                           border-radius: 4px; font-size: 11px;
                           color: #1e3a5f; }
  .play .dss-row .path { font-size: 10.5px; word-break: break-all;
                          flex: 1; min-width: 0; }
  .play .dss-row .badge { font-size: 10.5px; padding: 1px 8px;
                           border-radius: 999px; background: #ecfdf5;
                           color: #047857; font-weight: 600; }
  .play .dss-row .badge.warn { background: #fffbeb; color: #b45309; }
  .play .dss-row .badge.bad { background: #fef2f2; color: #b91c1c; }
  .bbox-toggle { font-size: 11px; padding: 3px 9px; border-radius: 999px;
                  border: 1px solid var(--line); background: #fff;
                  cursor: pointer; }
  .bbox-toggle[aria-pressed="false"] { background: #f1f5f9; color: var(--mute); }

  /* DSS size strip */
  .dss-strip-wrap { background: var(--card); border: 1px solid var(--line);
                     border-radius: 10px; padding: 14px;
                     box-shadow: 0 1px 2px rgba(15,23,42,0.04); }
  .dss-strip { display: block; width: 100%; height: auto; }
  .dss-strip .b { transition: fill-opacity 100ms, transform 100ms; cursor: pointer; }
  .dss-strip .b:hover { fill-opacity: 1; }
  .dss-strip .b.active { stroke: #facc15; stroke-width: 1.4; }
  .dss-tip { position: fixed; pointer-events: none; background: #0f172a;
              color: #f1f5f9; font-size: 11px; padding: 6px 10px;
              border-radius: 5px; box-shadow: 0 6px 16px rgba(0,0,0,0.3);
              display: none; z-index: 10000; white-space: nowrap;
              font-variant-numeric: tabular-nums; }
  .dss-strip-legend { font-size: 11px; margin-top: 8px; color: var(--mute);
                       display: flex; gap: 14px; flex-wrap: wrap;
                       align-items: center; }
  .dss-strip-legend .sw { display: inline-block; width: 11px; height: 11px;
                           border-radius: 2px; vertical-align: middle;
                           margin-right: 5px; }

  /* Top gallery */
  .gallery { display: grid;
             grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
             gap: 12px; }
  .card { background: var(--card); border: 1px solid var(--line);
          border-radius: 8px; overflow: hidden; cursor: pointer;
          transition: transform 120ms, box-shadow 120ms; }
  .card:hover { transform: translateY(-2px);
                box-shadow: 0 6px 16px rgba(15,23,42,0.12); }
  .card .thumb { aspect-ratio: 1/1; background: #0f172a; }
  .card .thumb img { width: 100%; height: 100%; object-fit: contain; }
  .card .body { padding: 8px 10px; }
  .card .rank { font-size: 10px; color: var(--mute); text-transform: uppercase;
                letter-spacing: 0.06em; }
  .card .precip { font-size: 17px; font-weight: 700;
                  font-variant-numeric: tabular-nums; }
  .card .date { font-size: 11px; color: var(--mute); margin-top: 1px; }

  /* Season + month strip */
  .small-charts { display: grid; gap: 18px;
                   grid-template-columns: 220px 1fr; align-items: start; }
  @media (max-width: 700px) { .small-charts { grid-template-columns: 1fr; } }
  .donut-wrap { background: var(--card); border: 1px solid var(--line);
                border-radius: 10px; padding: 12px; }
  .donut-wrap .title { font-size: 11px; color: var(--mute);
                       text-transform: uppercase; letter-spacing: 0.06em;
                       margin-bottom: 6px; }
  .donut-legend { font-size: 12px; margin-top: 4px; }
  .donut-legend .row { display: flex; justify-content: space-between;
                        padding: 2px 0; }
  .donut-legend .row .left { display: flex; align-items: center; gap: 6px; }
  .donut-legend .row .left .sw { width: 10px; height: 10px; border-radius: 2px; }
  .donut-legend .row .ct { font-variant-numeric: tabular-nums;
                            color: var(--mute); }
  .month-strip { background: var(--card); border: 1px solid var(--line);
                  border-radius: 10px; padding: 14px; }
  .month-strip .row { display: grid; grid-template-columns: repeat(12, 1fr);
                       gap: 6px; align-items: end; height: 110px; }
  .month-strip .bar { background: linear-gradient(180deg, #60a5fa, #2563eb);
                       border-radius: 3px 3px 0 0; min-height: 2px;
                       position: relative; }
  .month-strip .bar:hover { background: linear-gradient(180deg, #facc15, #d97706); }
  .month-strip .bar .ct { position: absolute; top: -16px; left: 0; right: 0;
                           text-align: center; font-size: 10px;
                           color: var(--mute);
                           font-variant-numeric: tabular-nums; }
  .month-strip .labels { display: grid; grid-template-columns: repeat(12, 1fr);
                          gap: 6px; font-size: 10px; color: var(--mute);
                          text-align: center; margin-top: 6px;
                          text-transform: uppercase; }

  /* Year strip */
  .year-bars { display: flex; gap: 1px; align-items: flex-end;
               height: 70px; background: var(--card); border: 1px solid var(--line);
               padding: 8px 8px 20px; border-radius: 8px; position: relative; }
  .year-bars .bar { background: linear-gradient(180deg, #60a5fa, #1e3a5f);
                    min-width: 4px; flex: 1 1 auto; border-radius: 2px 2px 0 0;
                    position: relative; }
  .year-bars .bar:hover { background: linear-gradient(180deg, #facc15, #d97706); }
  .year-bars .bar span { position: absolute; bottom: -16px; left: 0; right: 0;
                          font-size: 9px; text-align: center; color: var(--mute);
                          font-variant-numeric: tabular-nums; }

  /* Table */
  table { width: 100%; border-collapse: collapse; font-size: 12px;
          background: var(--card); border: 1px solid var(--line);
          border-radius: 8px; overflow: hidden; }
  th, td { padding: 6px 10px; text-align: left;
           border-bottom: 1px solid var(--line); }
  th { background: #f1f5f9; cursor: pointer; user-select: none;
       position: sticky; top: 0; font-weight: 600;
       text-transform: uppercase; font-size: 11px; letter-spacing: 0.04em;
       color: var(--mute); }
  th:hover { background: #e2e8f0; }
  th[data-key]::after { content: ' ⇅'; opacity: 0.3; }
  th[data-key].asc::after { content: ' ↑'; opacity: 1; color: var(--accent2); }
  th[data-key].desc::after { content: ' ↓'; opacity: 1; color: var(--accent2); }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  tr.outlier { background: #fef2f2; }
  tr.outlier td:first-child { box-shadow: inset 3px 0 0 #dc2626; }
  tr:hover { background: #f8fafc; }
  tr.active-row { background: #fef3c7 !important; }
  img.thumb { height: 36px; width: 36px; object-fit: contain; background: #0f172a;
              border-radius: 3px; cursor: pointer;
              transition: transform 120ms; }
  img.thumb:hover { transform: scale(1.4); }

  /* Modal */
  #modal { display: none; position: fixed; inset: 0;
           background: rgba(15,23,42,0.92); z-index: 9999; cursor: pointer;
           backdrop-filter: blur(2px); }
  #modal img { max-width: 92vw; max-height: 88vh; margin: 4vh auto 0;
               display: block; background: #0f172a; padding: 8px;
               border-radius: 6px; }
  #modal .caption { color: #f1f5f9; text-align: center; margin-top: 14px;
                    font-size: 14px; font-variant-numeric: tabular-nums; }

  .controls { font-size: 12px; margin: 8px 0; display: flex; gap: 10px;
              align-items: center; }
  .controls input { font-size: 12px; padding: 5px 8px; width: 240px;
                     border: 1px solid var(--line); border-radius: 4px; }

  @keyframes pulse { 0%, 100% { r: 7; opacity: 1; }
                     50% { r: 14; opacity: 0.4; } }
  #active-dot.playing { animation: pulse 1s ease-in-out infinite; }
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <div class="sub">__SUBTITLE__</div>
  <div class="nav">__NAV_LINKS__</div>
</header>
<main>

<h2>Summary</h2>
<div class="stats">__SUMMARY_STATS__</div>

__ANOMALIES_HTML__

<h2>Data Integrity <span class="meta">cross-checks proving file contents match their labels</span></h2>
<div class="integrity">__INTEGRITY_HTML__</div>

<h2>Hydrologic Domain <span class="meta">watershed (blue), transposition region (orange hatched), valid sub-region (green) · CONUS overview right</span></h2>
<div class="domain-wrap">
  <div class="domain-map">__DOMAIN_SVG__</div>
  <div class="domain-side">
    <div class="conus-card">
      <div class="conus-title">CONUS context</div>
      <div class="conus-inner">__CONUS_SVG__</div>
      <div class="conus-legend">
        <span><span class="sw" style="background:#2563eb"></span>watershed</span>
        <span><span class="sw" style="background:#ea580c"></span>transposition</span>
      </div>
    </div>
    __WS_ZOOM_CARD__
    __DOMAIN_STATS__
  </div>
</div>

<h2>Storm Map <span class="meta">same geometry · 460 storms colored by mean precip · click a dot or row to focus</span></h2>
<div class="map-wrap">
  __SVG_MAP__
</div>

<h2>Storm Playback <span class="meta">__N_EVENTS__ events sorted chronologically</span></h2>
<div class="play">
  <div class="thumb-box">
    <img id="play-thumb" src="" alt="" onclick="showModal(this.src, document.getElementById('play-caption').textContent)">
  </div>
  <div class="ctrl">
    <div class="row1">
      <button class="btn" id="play-btn">▶ Play</button>
      <input class="seek" id="play-seek" type="range" min="0" max="0" value="0">
      <select class="speed" id="play-speed">
        <option value="800">1×</option>
        <option value="400">2×</option>
        <option value="200" selected>5×</option>
        <option value="100">10×</option>
        <option value="40">25×</option>
      </select>
    </div>
    <div class="label">Storm <span id="play-pos">0</span>/<span id="play-total">0</span></div>
    <div class="now" id="play-caption">—</div>
    <div class="stats-row">
      <div><span class="label">Item</span><span class="val" id="play-id">—</span></div>
      <div><span class="label">Season</span><span class="val" id="play-season">—</span></div>
      <div><span class="label">Mean</span><span class="val" id="play-mean">—</span></div>
      <div><span class="label">Min</span><span class="val" id="play-min">—</span></div>
      <div><span class="label">Max</span><span class="val" id="play-max">—</span></div>
      <div><span class="label">DSS size</span><span class="val" id="play-dss">—</span></div>
    </div>
    <div class="dss-block">
      <div class="dss-row">
        <span class="k">DSS file</span>
        <code class="v" id="play-dssfile">—</code>
        <span class="badge" id="play-dur">—</span>
      </div>
      <div class="dss-row">
        <span class="k">Window</span>
        <span class="v" id="play-window">—</span>
      </div>
      <div class="dss-row">
        <span class="k">Bbox</span>
        <span class="v" id="play-bbox">—</span>
        <button class="bbox-toggle" id="bbox-toggle" type="button" aria-pressed="true">Hide on map</button>
      </div>
      <div class="dss-row">
        <span class="k">Precip</span>
        <code class="v path" id="play-precip">—</code>
      </div>
      <div class="dss-row">
        <span class="k">Temp</span>
        <code class="v path" id="play-temp">—</code>
      </div>
    </div>
  </div>
</div>

<h2>DSS Files <span class="meta">__N_DSS__ files · hover bars for detail · click to focus</span></h2>
<div class="dss-strip-wrap">__DSS_STRIP__</div>

<h2>Top 12 Events <span class="meta">by mean precipitation</span></h2>
<div class="gallery" id="gallery">__GALLERY_HTML__</div>

<h2>Seasonality</h2>
<div class="small-charts">
  <div class="donut-wrap">
    <div class="title">By Season</div>
    __SEASON_DONUT__
    <div class="donut-legend">__SEASON_LEGEND__</div>
  </div>
  <div class="month-strip">
    <div class="row">__MONTH_BARS__</div>
    <div class="labels">__MONTH_LABELS__</div>
  </div>
</div>

<h2>Year Distribution</h2>
<div class="year-bars">__YEAR_BARS__</div>

<h2>All Events <span class="meta">click a header to sort, thumbnail to enlarge, row to focus on map</span></h2>
<div class="controls">
  Filter:
  <input id="filter" placeholder="date / season / id…">
  Showing <span id="shown">__N_EVENTS__</span> rows
</div>
<table id="events">
  <thead><tr>
    <th data-key="id" data-type="num">Item ID</th>
    <th data-key="storm_start" data-type="str">Storm Start (UTC)</th>
    <th data-key="season" data-type="str">Season</th>
    <th data-key="mean" data-type="num">Mean (in)</th>
    <th data-key="min" data-type="num">Min (in)</th>
    <th data-key="max" data-type="num">Max (in)</th>
    <th data-key="max_lat" data-type="num">Lat</th>
    <th data-key="max_lon" data-type="num">Lon</th>
    <th data-key="dss_size" data-type="num">DSS KiB</th>
    <th>Thumb</th>
  </tr></thead>
  <tbody id="events-body"></tbody>
</table>

</main>

<div id="modal" onclick="this.style.display='none'">
  <img id="modal-img" src="" alt="">
  <div class="caption" id="modal-caption"></div>
</div>

<script>
const events = __EVENTS_JSON__;
const dssSize = __DSS_SIZE_JSON__;
const outliers = new Set(__OUTLIERS_JSON__);
const mapMeta = __MAP_META_JSON__;
const eventsPrefix = __EVENTS_PREFIX_JSON__;

// ─── Map dot interactivity ───────────────────────────────────────────────
const mapSvg = document.querySelector('.map-svg');
const activeDot = document.getElementById('active-dot');
const activeBbox = document.getElementById('active-bbox');
const bboxToggle = document.getElementById('bbox-toggle');
let bboxVisible = true;
if (bboxToggle) {
  bboxToggle.addEventListener('click', () => {
    bboxVisible = !bboxVisible;
    bboxToggle.setAttribute('aria-pressed', bboxVisible);
    bboxToggle.textContent = bboxVisible ? 'Hide on map' : 'Show on map';
    if (!bboxVisible && activeBbox) activeBbox.style.display = 'none';
    else if (bboxVisible && activeBbox && activeBbox.dataset.pending) {
      activeBbox.style.display = 'block';
    }
  });
}

function focusOnMap(eid) {
  const pos = mapMeta.positions[eid];
  if (pos && activeDot) {
    activeDot.setAttribute('cx', pos[0]);
    activeDot.setAttribute('cy', pos[1]);
    activeDot.setAttribute('r', 8);
    activeDot.style.display = 'block';
  }
  const bb = mapMeta.bboxes ? mapMeta.bboxes[eid] : null;
  if (bb && activeBbox) {
    activeBbox.setAttribute('x', bb[0]);
    activeBbox.setAttribute('y', bb[1]);
    activeBbox.setAttribute('width', bb[2]);
    activeBbox.setAttribute('height', bb[3]);
    activeBbox.dataset.pending = '1';
    activeBbox.style.display = bboxVisible ? 'block' : 'none';
  } else if (activeBbox) {
    activeBbox.style.display = 'none';
  }
}

if (mapSvg) {
  mapSvg.addEventListener('click', e => {
    const t = e.target;
    if (t && t.classList.contains('dot')) {
      const id = t.getAttribute('data-id');
      scrubTo(events.findIndex(ev => String(ev.id) === id));
    }
  });
}

// ─── Storm playback ──────────────────────────────────────────────────────
const byDate = events.slice().sort((a, b) =>
  (a.storm_start || '').localeCompare(b.storm_start || '')
);
const seek = document.getElementById('play-seek');
const playBtn = document.getElementById('play-btn');
const speedSel = document.getElementById('play-speed');
seek.max = Math.max(0, byDate.length - 1);
document.getElementById('play-total').textContent = byDate.length;
let playIdx = 0;
let playTimer = null;

function thumbUrl(ev) {
  return `${eventsPrefix}/${ev.id}/${ev.id}.thumbnail.png`;
}

function scrubTo(i) {
  if (i < 0 || i >= byDate.length) return;
  playIdx = i;
  const ev = byDate[i];
  seek.value = i;
  seek.style.setProperty('--pct', (i / (byDate.length - 1 || 1) * 100).toFixed(1) + '%');
  document.getElementById('play-pos').textContent = i + 1;
  document.getElementById('play-thumb').src = thumbUrl(ev);
  document.getElementById('play-caption').textContent =
    `${ev.storm_start || '—'}  ·  Item ${ev.id}  ·  ${(ev.mean ?? '—')} in mean`;
  document.getElementById('play-id').textContent = ev.id;
  document.getElementById('play-season').textContent = ev.season || '—';
  document.getElementById('play-mean').textContent =
    ev.mean != null ? ev.mean.toFixed(2) : '—';
  document.getElementById('play-min').textContent =
    ev.min != null ? ev.min.toFixed(2) : '—';
  document.getElementById('play-max').textContent =
    ev.max != null ? ev.max.toFixed(2) : '—';
  const sz = dssSize[ev.id];
  document.getElementById('play-dss').textContent =
    sz != null ? `${Math.round(sz / 1024)} KiB` : '—';

  // DSS details
  document.getElementById('play-dssfile').textContent = ev.dss_file || '—';
  const dur = document.getElementById('play-dur');
  if (ev.actual_hours != null) {
    const h = ev.actual_hours;
    dur.textContent = `${h.toFixed(0)} hr`;
    dur.className = 'badge';
    if (expectedHours && Math.abs(h - expectedHours) > 1) {
      dur.className = 'badge bad';
      dur.textContent = `${h.toFixed(0)} hr (≠ ${expectedHours} hr)`;
    }
  } else {
    dur.textContent = '—';
    dur.className = 'badge warn';
  }
  document.getElementById('play-window').textContent =
    ev.storm_start && ev.storm_end
      ? `${ev.storm_start} → ${ev.storm_end}`
      : (ev.storm_start || '—');
  const bb = ev.bbox;
  document.getElementById('play-bbox').textContent = bb
    ? `lon ${bb[0].toFixed(2)} → ${bb[2].toFixed(2)},  lat ${bb[1].toFixed(2)} → ${bb[3].toFixed(2)}`
    : '—';
  document.getElementById('play-precip').textContent = ev.precip_pathname || '— missing —';
  document.getElementById('play-precip').className = 'v path' + (ev.precip_pathname ? '' : ' missing');
  document.getElementById('play-temp').textContent = ev.temp_pathname || '— missing —';
  document.getElementById('play-temp').className = 'v path' + (ev.temp_pathname ? '' : ' missing');

  focusOnMap(String(ev.id));

  // strip highlight
  document.querySelectorAll('.dss-strip .b.active').forEach(b => b.classList.remove('active'));
  const sb = document.querySelector(`.dss-strip .b[data-id="${ev.id}"]`);
  if (sb) sb.classList.add('active');

  // highlight active table row
  document.querySelectorAll('tr.active-row').forEach(r => r.classList.remove('active-row'));
  const r = document.querySelector(`tr[data-id="${ev.id}"]`);
  if (r) r.classList.add('active-row');
}

const expectedHours = __EXPECTED_HOURS__;

function stepPlay() {
  scrubTo((playIdx + 1) % byDate.length);
}

function startPlay() {
  if (playTimer) return;
  const ms = parseInt(speedSel.value, 10) || 200;
  playBtn.textContent = '⏸ Pause';
  playBtn.classList.add('pause');
  activeDot.classList.add('playing');
  playTimer = setInterval(stepPlay, ms);
}

function stopPlay() {
  if (!playTimer) return;
  clearInterval(playTimer);
  playTimer = null;
  playBtn.textContent = '▶ Play';
  playBtn.classList.remove('pause');
  activeDot.classList.remove('playing');
}

playBtn.addEventListener('click', () => playTimer ? stopPlay() : startPlay());
seek.addEventListener('input', e => { stopPlay(); scrubTo(parseInt(e.target.value, 10)); });
speedSel.addEventListener('change', () => { if (playTimer) { stopPlay(); startPlay(); } });

if (byDate.length) scrubTo(0);

// ─── Events table ────────────────────────────────────────────────────────
const tbody = document.getElementById('events-body');
const shown = document.getElementById('shown');
let sortKey = 'id', sortDir = 1, sortType = 'num';

function row(ev) {
  const sz = dssSize[ev.id] != null ? dssSize[ev.id] : null;
  const szKi = sz != null ? Math.round(sz / 1024) : '';
  const isOutlier = outliers.has(String(ev.id));
  const tu = thumbUrl(ev);
  return {
    ev, isOutlier, szBytes: sz,
    html: `<tr class="${isOutlier ? 'outlier' : ''}" data-id="${ev.id}">
      <td class="num">${ev.id}</td>
      <td>${ev.storm_start || ''}</td>
      <td>${ev.season || ''}</td>
      <td class="num">${ev.mean != null ? ev.mean.toFixed(2) : ''}</td>
      <td class="num">${ev.min != null ? ev.min.toFixed(2) : ''}</td>
      <td class="num">${ev.max != null ? ev.max.toFixed(2) : ''}</td>
      <td class="num">${ev.max_lat != null ? ev.max_lat.toFixed(3) : ''}</td>
      <td class="num">${ev.max_lon != null ? ev.max_lon.toFixed(3) : ''}</td>
      <td class="num">${szKi}</td>
      <td><img class="thumb" src="${tu}" alt="storm ${ev.id}" loading="lazy"
               onclick="event.stopPropagation(); showModal('${tu}', 'Storm ${ev.id} — ${ev.storm_start}')"></td>
    </tr>`
  };
}
const allRows = events.map(row);

function applySortIndicator() {
  document.querySelectorAll('th[data-key]').forEach(th => {
    th.classList.remove('asc', 'desc');
    if (th.dataset.key === sortKey) th.classList.add(sortDir > 0 ? 'asc' : 'desc');
  });
}

function render(filterText) {
  const f = (filterText || '').toLowerCase();
  let rows = allRows;
  if (f) rows = rows.filter(r =>
    String(r.ev.id).includes(f)
    || (r.ev.storm_start || '').toLowerCase().includes(f)
    || (r.ev.season || '').toLowerCase().includes(f)
  );
  rows = rows.slice().sort((a, b) => {
    let av = sortKey === 'dss_size' ? a.szBytes : a.ev[sortKey];
    let bv = sortKey === 'dss_size' ? b.szBytes : b.ev[sortKey];
    if (sortType === 'num') {
      av = av == null ? -Infinity : +av;
      bv = bv == null ? -Infinity : +bv;
    } else {
      av = av || '';
      bv = bv || '';
    }
    if (av < bv) return -1 * sortDir;
    if (av > bv) return 1 * sortDir;
    return 0;
  });
  tbody.innerHTML = rows.map(r => r.html).join('');
  shown.textContent = rows.length;
  applySortIndicator();
}

document.querySelectorAll('th[data-key]').forEach(th => {
  th.addEventListener('click', () => {
    const k = th.dataset.key;
    if (sortKey === k) sortDir *= -1;
    else { sortKey = k; sortDir = 1; sortType = th.dataset.type || 'str'; }
    render(document.getElementById('filter').value);
  });
});
document.getElementById('filter').addEventListener('input', e => render(e.target.value));

tbody.addEventListener('click', e => {
  const tr = e.target.closest('tr[data-id]');
  if (!tr) return;
  const i = byDate.findIndex(ev => String(ev.id) === tr.dataset.id);
  if (i >= 0) { stopPlay(); scrubTo(i); }
});

render();

// ─── Gallery click handlers ──────────────────────────────────────────────
document.getElementById('gallery').addEventListener('click', e => {
  const c = e.target.closest('.card[data-id]');
  if (!c) return;
  const i = byDate.findIndex(ev => String(ev.id) === c.dataset.id);
  if (i >= 0) { stopPlay(); scrubTo(i); window.scrollTo({top: 0, behavior: 'smooth'}); }
});

// ─── DSS strip interactivity ─────────────────────────────────────────────
const dssStrip = document.querySelector('.dss-strip');
const dssTip = document.createElement('div');
dssTip.className = 'dss-tip';
document.body.appendChild(dssTip);
if (dssStrip) {
  const eventById = {};
  for (const ev of events) eventById[String(ev.id)] = ev;
  dssStrip.addEventListener('mouseover', e => {
    const b = e.target.closest('.b[data-id]');
    if (!b) return;
    const ev = eventById[b.dataset.id];
    if (!ev) return;
    const sz = dssSize[ev.id];
    const kib = sz != null ? Math.round(sz / 1024) : '—';
    dssTip.innerHTML = `<b>Item ${ev.id}</b> · ${ev.storm_start || ''}` +
      `<br>${kib} KiB · mean ${ev.mean != null ? ev.mean.toFixed(2) : '—'} in` +
      (ev.is_outlier ? '<br><b style="color:#fca5a5">⚠ outlier</b>' : '');
    dssTip.style.display = 'block';
  });
  dssStrip.addEventListener('mousemove', e => {
    dssTip.style.left = (e.clientX + 12) + 'px';
    dssTip.style.top = (e.clientY + 12) + 'px';
  });
  dssStrip.addEventListener('mouseout', () => { dssTip.style.display = 'none'; });
  dssStrip.addEventListener('click', e => {
    const b = e.target.closest('.b[data-id]');
    if (!b) return;
    const i = byDate.findIndex(ev => String(ev.id) === b.dataset.id);
    if (i >= 0) { stopPlay(); scrubTo(i); }
  });
}

// ─── Modal ───────────────────────────────────────────────────────────────
function showModal(src, caption) {
  document.getElementById('modal-img').src = src;
  document.getElementById('modal-caption').textContent = caption;
  document.getElementById('modal').style.display = 'block';
}
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    document.getElementById('modal').style.display = 'none';
    stopPlay();
  }
  if (e.key === ' ' && e.target === document.body) {
    e.preventDefault();
    playTimer ? stopPlay() : startPlay();
  }
  if (e.key === 'ArrowRight') { stopPlay(); scrubTo((playIdx + 1) % byDate.length); }
  if (e.key === 'ArrowLeft') { stopPlay(); scrubTo((playIdx - 1 + byDate.length) % byDate.length); }
});
</script>
</body>
</html>
"""


def _index_html(runs: list[dict]) -> str:
    """Landing page: one hero card per catalog with a top-storm thumbnail.

    Card layout — thumbnail of the highest-mean event, key counts, and an
    anomaly summary. Visual at-a-glance picker for which catalog to drill into.
    """
    cards: list[str] = []
    for r in runs:
        cid = r["catalog_id"]
        attrs = r.get("attrs") or {}

        # Pick the top event by mean precip as the cover image.
        top_ev = None
        for ev in r["events"]:
            if isinstance(ev.get("mean"), (int, float)):
                if top_ev is None or ev["mean"] > top_ev["mean"]:
                    top_ev = ev
        cover = (
            f'{html.escape(r["run_name"])}/audit/events/'
            f'{html.escape(str(top_ev["id"]))}/{html.escape(str(top_ev["id"]))}'
            ".thumbnail.png"
            if top_ev else ""
        )

        anoms = []
        if r["outlier_dss"]:
            anoms.append(f"{len(r['outlier_dss'])} outlier DSS")
        if not r["grid_count_ok"]:
            anoms.append(f"grid {r['n_grid']}/{r['expected_grid']}")
        if r["out_of_box"]:
            anoms.append(f"{len(r['out_of_box'])} out-of-bbox")
        if anoms:
            anomaly_html = (
                '<div class="anom warn">⚠ ' + "; ".join(html.escape(a) for a in anoms)
                + "</div>"
            )
        else:
            anomaly_html = '<div class="anom ok">✓ clean</div>'

        mean_min, mean_max = r["mean_range"]
        mean_str = (
            f"{mean_min:.1f}–{mean_max:.1f} in"
            if isinstance(mean_min, (int, float)) else "—"
        )

        cards.append(
            f'<a class="catalog-card" '
            f'href="{html.escape(r["run_name"])}/audit/report.html">'
            f'<div class="cover">'
            + (
                f'<img src="{cover}" alt="top storm in {html.escape(cid)}">'
                if cover else ""
            )
            + f'<div class="duration">{attrs.get("storm_duration", "—")} hr</div>'
            + "</div>"
            f'<div class="body">'
            f'<div class="cid">{html.escape(cid)}</div>'
            f'<div class="metrics">'
            f'<div><div class="n">{r["n_events"]}</div>'
            f'<div class="l">events</div></div>'
            f'<div><div class="n">{r["n_dss"]}</div>'
            f'<div class="l">DSS</div></div>'
            f'<div><div class="n">{r["n_grid"]}</div>'
            f'<div class="l">grid</div></div>'
            f'</div>'
            f'<div class="precip-range">{mean_str} mean precip</div>'
            f'{anomaly_html}'
            f'</div></a>'
        )

    return (
        "<!doctype html><html><head><meta charset=utf-8>"
        "<title>Storm Catalog Audit Index</title>"
        "<style>"
        ":root{--ink:#0f172a;--mute:#64748b;--line:#e2e8f0;"
        "--ok:#059669;--warn:#d97706;--bad:#dc2626}"
        "*{box-sizing:border-box}body{margin:0;font-family:-apple-system,"
        "BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;color:var(--ink);"
        "background:#f7f8fb;line-height:1.45}"
        "header{background:linear-gradient(135deg,#0f172a,#1e3a5f);color:#fff;"
        "padding:24px 28px}"
        "header h1{margin:0;font-size:22px}"
        "header .sub{font-size:13px;opacity:.85;margin-top:4px}"
        "main{max-width:1300px;margin:0 auto;padding:24px 28px 60px}"
        ".grid{display:grid;gap:18px;grid-template-columns:repeat(auto-fit,minmax(280px,1fr))}"
        ".catalog-card{display:block;background:#fff;border:1px solid var(--line);"
        "border-radius:12px;overflow:hidden;text-decoration:none;color:var(--ink);"
        "box-shadow:0 2px 6px rgba(15,23,42,.05);transition:transform 150ms,box-shadow 150ms}"
        ".catalog-card:hover{transform:translateY(-3px);"
        "box-shadow:0 10px 22px rgba(15,23,42,.12)}"
        ".cover{position:relative;aspect-ratio:16/9;background:#0f172a;overflow:hidden}"
        ".cover img{width:100%;height:100%;object-fit:cover;opacity:.92}"
        ".cover .duration{position:absolute;top:10px;right:10px;"
        "background:rgba(15,23,42,.78);color:#fff;font-size:11px;padding:3px 9px;"
        "border-radius:999px;letter-spacing:.04em}"
        ".body{padding:14px 16px}"
        ".cid{font-size:15px;font-weight:700;margin-bottom:10px}"
        ".metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;"
        "margin-bottom:10px}"
        ".metrics .n{font-size:18px;font-weight:700;font-variant-numeric:tabular-nums}"
        ".metrics .l{font-size:10px;color:var(--mute);text-transform:uppercase;"
        "letter-spacing:.06em;margin-top:1px}"
        ".precip-range{font-size:12px;color:var(--mute);margin-bottom:8px}"
        ".anom{font-size:12px;padding:6px 10px;border-radius:6px;font-weight:600}"
        ".anom.ok{background:#ecfdf5;color:#047857}"
        ".anom.warn{background:#fffbeb;color:#b45309}"
        "</style></head><body>"
        "<header>"
        "<h1>Storm Catalog Audit</h1>"
        f'<div class="sub">{len(runs)} catalog(s) · click a card to open the report</div>'
        "</header>"
        '<main><div class="grid">'
        + "".join(cards) +
        "</div></main></body></html>"
    )


def _stat_html(label: str, value: str, *, warn: bool = False, ok: bool = False) -> str:
    cls = "stat" + (" warn" if warn else " ok" if ok else "")
    return (
        f'<div class="{cls}"><div class="label">{html.escape(label)}</div>'
        f'<div class="value">{html.escape(value)}</div></div>'
    )


_SEASON_COLORS = {
    "Spring": "#22c55e",
    "Summer": "#f59e0b",
    "Fall":   "#dc2626",
    "Autumn": "#dc2626",
    "Winter": "#3b82f6",
}


def _build_season_donut(events: list[dict]) -> tuple[str, str]:
    """SVG donut chart of event counts per season. Returns (svg, legend_html)."""
    import math

    counts: dict[str, int] = {}
    for ev in events:
        s = ev.get("season") or "Unknown"
        counts[s] = counts.get(s, 0) + 1
    total = sum(counts.values()) or 1
    # Stable order: Winter → Spring → Summer → Fall (calendar)
    order = ["Winter", "Spring", "Summer", "Fall", "Autumn", "Unknown"]
    items = [(s, counts[s]) for s in order if s in counts] + [
        (s, n) for s, n in counts.items() if s not in order
    ]

    cx, cy, r_outer, r_inner = 90, 90, 75, 48
    parts = [f'<svg viewBox="0 0 180 180" style="width:100%;max-width:180px;display:block;margin:auto">']

    # Background ring
    parts.append(
        f'<circle cx="{cx}" cy="{cy}" r="{(r_outer + r_inner) / 2:.1f}" '
        f'fill="none" stroke="#f1f5f9" stroke-width="{r_outer - r_inner}"/>'
    )

    angle = -math.pi / 2  # start at top
    circumference = 2 * math.pi * ((r_outer + r_inner) / 2)
    for s, n in items:
        frac = n / total
        color = _SEASON_COLORS.get(s, "#94a3b8")
        seg_len = circumference * frac
        # Use stroke-dasharray on a circle to draw the arc
        rot_deg = (angle + math.pi / 2) * 180 / math.pi
        parts.append(
            f'<circle cx="{cx}" cy="{cy}" r="{(r_outer + r_inner) / 2:.1f}" '
            f'fill="none" stroke="{color}" '
            f'stroke-width="{r_outer - r_inner}" '
            f'stroke-dasharray="{seg_len:.2f} {circumference - seg_len:.2f}" '
            f'transform="rotate({rot_deg:.2f} {cx} {cy})"/>'
        )
        angle += 2 * math.pi * frac

    parts.append(
        f'<text x="{cx}" y="{cy - 4}" text-anchor="middle" '
        f'font-size="22" font-weight="700" fill="#0f172a">{total}</text>'
        f'<text x="{cx}" y="{cy + 14}" text-anchor="middle" '
        f'font-size="10" fill="#64748b" text-transform="uppercase" '
        f'letter-spacing="1">events</text>'
    )
    parts.append("</svg>")

    legend_rows = []
    for s, n in items:
        color = _SEASON_COLORS.get(s, "#94a3b8")
        pct = 100 * n / total
        legend_rows.append(
            f'<div class="row"><div class="left">'
            f'<span class="sw" style="background:{color}"></span>'
            f'<span>{html.escape(s)}</span></div>'
            f'<span class="ct">{n} ({pct:.0f}%)</span></div>'
        )
    return "".join(parts), "".join(legend_rows)


def _build_month_bars(events: list[dict]) -> tuple[str, str]:
    """12 vertical bars showing event counts per calendar month."""
    counts = [0] * 12
    for ev in events:
        ts = ev.get("storm_start") or ""
        if len(ts) >= 7 and ts[5:7].isdigit():
            counts[int(ts[5:7]) - 1] += 1
    peak = max(counts) or 1
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    bars = []
    for i, c in enumerate(counts):
        h = round(95 * c / peak) if peak else 0
        bars.append(
            f'<div class="bar" style="height:{h}px" title="{months[i]}: {c} events">'
            f'<span class="ct">{c if c else ""}</span></div>'
        )
    labels = "".join(f'<div>{m}</div>' for m in months)
    return "".join(bars), labels


def _build_dss_strip(
    events: list[dict],
    dss_size_by_id: dict[str, int],
    median_bytes: int,
    width: int = 1320,
    height: int = 130,
) -> str:
    """460 vertical bars, one per storm, height proportional to DSS size.

    Sorted by storm date so the bars trace the catalog chronologically. Color
    encodes health: red = outlier (<50% median), gold = small (<80% median),
    blue = healthy. Bars are clickable + hoverable; the active storm gets a
    yellow stroke.
    """
    ranked = sorted(
        events, key=lambda e: (e.get("storm_start") or "")
    )
    if not ranked or not median_bytes:
        return '<div class="dss-empty">No DSS data.</div>'

    max_sz = max(
        (dss_size_by_id.get(str(ev["id"]), 0) for ev in ranked), default=0
    ) or 1
    n = len(ranked)
    pad_l, pad_r, pad_b = 40, 20, 26
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_b
    bw = max(1.5, plot_w / n - 0.5)
    step = plot_w / n

    parts = [
        f'<svg class="dss-strip" viewBox="0 0 {width} {height}" '
        f'preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">'
    ]
    # Threshold lines (median, 50% median = outlier cutoff)
    for level, label, color in [
        (median_bytes, "median", "#94a3b8"),
        (median_bytes // 2, "50%", "#fca5a5"),
    ]:
        y = plot_h - (level / max_sz) * plot_h
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{pad_l + plot_w}" y2="{y:.1f}" '
            f'stroke="{color}" stroke-width="0.8" stroke-dasharray="3 3"/>'
            f'<text x="{pad_l + plot_w + 4}" y="{y + 3:.1f}" '
            f'fill="{color}" font-size="10">{label}</text>'
        )

    # Y-axis tick labels (KiB)
    for frac in (0.25, 0.5, 0.75, 1.0):
        y = plot_h - frac * plot_h
        kib = int((max_sz * frac) / 1024)
        parts.append(
            f'<text x="{pad_l - 6}" y="{y + 3:.1f}" text-anchor="end" '
            f'fill="#94a3b8" font-size="10">{kib}</text>'
        )
    parts.append(
        f'<text x="6" y="{plot_h / 2:.1f}" fill="#94a3b8" font-size="10" '
        f'transform="rotate(-90 12 {plot_h / 2:.1f})">DSS size (KiB)</text>'
    )

    for i, ev in enumerate(ranked):
        eid = str(ev["id"])
        sz = dss_size_by_id.get(eid, 0)
        if not sz:
            continue
        h = (sz / max_sz) * plot_h
        x = pad_l + i * step
        if ev.get("is_outlier"):
            color = "#dc2626"
        elif sz < median_bytes * 0.8:
            color = "#f59e0b"
        else:
            color = "#2563eb"
        parts.append(
            f'<rect class="b" data-id="{html.escape(eid)}" '
            f'x="{x:.1f}" y="{plot_h - h:.1f}" '
            f'width="{bw:.1f}" height="{h:.1f}" '
            f'fill="{color}" fill-opacity="0.78"/>'
        )

    # X-axis year ticks — sample every ~5 years
    years_seen: list[tuple[int, float]] = []
    for i, ev in enumerate(ranked):
        ts = ev.get("storm_start") or ""
        if len(ts) >= 4 and ts[:4].isdigit():
            y = int(ts[:4])
            if not years_seen or y >= years_seen[-1][0] + 5:
                years_seen.append((y, pad_l + i * step + bw / 2))
    for y, xpos in years_seen:
        parts.append(
            f'<text x="{xpos:.1f}" y="{height - 8:.1f}" text-anchor="middle" '
            f'fill="#64748b" font-size="10">{y}</text>'
        )

    parts.append("</svg>")
    parts.append(
        '<div class="dss-strip-legend">'
        '<span><span class="sw" style="background:#2563eb"></span>healthy '
        '(≥80% median)</span>'
        '<span><span class="sw" style="background:#f59e0b"></span>small '
        '(50–80%)</span>'
        '<span><span class="sw" style="background:#dc2626"></span>outlier '
        '(&lt;50%)</span>'
        '<span>sorted chronologically · 1 bar per storm</span>'
        "</div>"
    )
    return "".join(parts)


def _build_gallery(events: list[dict], events_prefix: str, k: int = 12) -> str:
    """Top-K events by mean precip → gallery cards with thumbnails."""
    ranked = sorted(
        (ev for ev in events if isinstance(ev.get("mean"), (int, float))),
        key=lambda e: e["mean"],
        reverse=True,
    )[:k]
    cards = []
    for rank, ev in enumerate(ranked, 1):
        eid = html.escape(str(ev["id"]))
        date = (ev.get("storm_start") or "")[:10]
        cards.append(
            f'<div class="card" data-id="{eid}">'
            f'<div class="thumb">'
            f'<img src="{html.escape(events_prefix)}/{eid}/{eid}.thumbnail.png" '
            f'alt="storm {eid}" loading="lazy">'
            f'</div>'
            f'<div class="body">'
            f'<div class="rank">#{rank} · Item {eid}</div>'
            f'<div class="precip">{ev["mean"]:.2f} in</div>'
            f'<div class="date">{html.escape(date)} · {html.escape(ev.get("season") or "")}</div>'
            f'</div></div>'
        )
    return "".join(cards)


def _build_report(
    run_name: str, audit: dict, all_runs: list[dict], http_mode: bool = False
) -> Path | str:
    """Render a catalog's audit report.

    File mode (default): write ``audit/report.html`` with relative asset URLs
    and return the path — used by ``./audit.py report`` for offline sharing.

    HTTP mode: return the HTML string with absolute URLs rooted at the unified
    web app's routes (``/assets/<name>/...`` thumbnails, ``/audit/<other>`` nav)
    so ``web.py`` can serve the same report inline without writing a file.
    """
    audit_dir = OUTPUTS / run_name / "audit"
    events_prefix = f"/assets/{run_name}/events" if http_mode else "events"

    # Stats
    progress = audit.get("progress") or {}
    total_s = (progress.get("summary") or {}).get("total_s")
    median_kib = audit["median_dss_bytes"] // 1024 if audit["median_dss_bytes"] else 0
    mean_min, mean_max = audit["mean_range"]
    summary_chunks = [
        _stat_html("Storm Duration", f"{audit['storm_duration']} hr"),
        _stat_html("Top N events", str(audit["top_n"])),
        _stat_html(
            "Events found",
            f"{audit['n_events']}",
            ok=audit["n_events"] == audit["top_n"],
            warn=audit["n_events"] != audit["top_n"],
        ),
        _stat_html(
            "DSS files",
            f"{audit['n_dss']}",
            ok=audit["n_dss"] == audit["top_n"],
            warn=audit["n_dss"] != audit["top_n"],
        ),
        _stat_html(
            "Grid entries",
            f"{audit['n_grid']} / {audit['expected_grid']}",
            ok=audit["grid_count_ok"],
            warn=not audit["grid_count_ok"],
        ),
        _stat_html(
            "DSS outliers",
            str(len(audit["outlier_dss"])),
            ok=len(audit["outlier_dss"]) == 0,
            warn=len(audit["outlier_dss"]) > 0,
        ),
        _stat_html("Median DSS", f"{median_kib} KiB"),
        _stat_html(
            "Mean precip",
            f"{mean_min:.1f}–{mean_max:.1f} in"
            if isinstance(mean_min, (int, float)) else "—",
        ),
        _stat_html(
            "Date span",
            f"{audit['attrs'].get('start_date','?')} → {audit['attrs'].get('end_date','?')}",
        ),
        _stat_html(
            "Runtime",
            f"{int(total_s // 60)}m {int(total_s % 60)}s" if total_s else "—",
        ),
    ]

    # Anomalies
    anomalies = []
    if audit["outlier_dss"]:
        items = "".join(
            f"<li><code>{html.escape(d['name'])}</code> — {d['size_token']}</li>"
            for d in audit["outlier_dss"][:30]
        )
        more = (
            f"<li>… and {len(audit['outlier_dss']) - 30} more</li>"
            if len(audit["outlier_dss"]) > 30
            else ""
        )
        anomalies.append(
            "<div class='anomaly'><h3>Outlier DSS files (much smaller than median "
            f"{median_kib} KiB — likely partial or empty)</h3>"
            f"<ul>{items}{more}</ul></div>"
        )
    if not audit["grid_count_ok"]:
        anomalies.append(
            "<div class='anomaly'><h3>Grid file count mismatch</h3>"
            f"<p>Expected {audit['expected_grid']} entries (Precipitation + "
            f"Temperature per storm), got {audit['n_grid']}. "
            "Likely the smaller DSS files above lack one or both pathnames.</p></div>"
        )
    if audit["dss_without_grid"]:
        items = "".join(
            f"<li><code>{html.escape(n)}</code></li>"
            for n in audit["dss_without_grid"][:20]
        )
        anomalies.append(
            "<div class='anomaly'><h3>DSS files with no grid entries</h3>"
            f"<ul>{items}</ul></div>"
        )
    if audit["grid_without_dss"]:
        items = "".join(
            f"<li><code>{html.escape(n)}</code></li>"
            for n in audit["grid_without_dss"][:20]
        )
        anomalies.append(
            "<div class='anomaly'><h3>Grid entries pointing at missing DSS files</h3>"
            f"<ul>{items}</ul></div>"
        )
    if audit["out_of_box"]:
        items = "".join(
            f"<li>Item <code>{html.escape(str(ev['id']))}</code> "
            f"({ev.get('storm_start','?')}) at "
            f"({ev.get('max_lat')}, {ev.get('max_lon')})</li>"
            for ev in audit["out_of_box"][:10]
        )
        anomalies.append(
            "<div class='anomaly'><h3>Max-precip locations outside transposition "
            "bbox (loose check)</h3>"
            f"<ul>{items}</ul></div>"
        )
    if audit.get("duration_mismatches"):
        items = "".join(
            f"<li>Item <code>{html.escape(str(m['id']))}</code> · "
            f"actual {m['actual_hours']:.1f} hr vs declared "
            f"{m['expected']} hr</li>"
            for m in audit["duration_mismatches"][:15]
        )
        more = (
            f"<li>… and {len(audit['duration_mismatches']) - 15} more</li>"
            if len(audit["duration_mismatches"]) > 15 else ""
        )
        anomalies.append(
            "<div class='anomaly'><h3>Storm window doesn't match declared "
            f"{audit['storm_duration']}-hour duration</h3>"
            f"<ul>{items}{more}</ul></div>"
        )
    if audit.get("bbox_out_of_domain"):
        items = "".join(
            f"<li>Item <code>{html.escape(str(ev['id']))}</code> · "
            f"DSS bbox {ev.get('bbox')}</li>"
            for ev in audit["bbox_out_of_domain"][:10]
        )
        anomalies.append(
            "<div class='anomaly'><h3>DSS bbox entirely outside transposition "
            "domain</h3>"
            f"<ul>{items}</ul></div>"
        )

    # Year strip
    years = audit["years"]
    if years:
        ymin, ymax = min(years), max(years)
        peak = max(years.values()) or 1
        bars = []
        for y in range(ymin, ymax + 1):
            count = years.get(y, 0)
            h = int(round(56 * count / peak)) if peak else 0
            label = str(y) if y % 5 == 0 else ""
            bars.append(
                f'<div class="bar" style="height:{h}px" '
                f'title="{y}: {count} events"><span>{label}</span></div>'
            )
        year_bars_html = "".join(bars)
    else:
        year_bars_html = ""

    # Geometry payloads
    cid = audit["catalog_id"]
    watershed = _read_json(audit_dir / "hydro_domains" / f"{cid}-watershed.json")
    transposition = _read_json(
        audit_dir / "hydro_domains" / f"{cid}-transposition.json"
    )
    transposition_valid = _read_json(
        audit_dir / "hydro_domains" / f"{cid}-transposition_valid.json"
    )

    integrity_rows = _data_integrity_checks(
        cid, watershed, transposition, transposition_valid
    )
    ic = {"ok": "✓", "warn": "⚠", "bad": "✗"}
    integrity_html = "".join(
        f'<div class="row {r["status"]}">'
        f'<span class="ic {r["status"]}">{ic.get(r["status"], "·")}</span>'
        f'<span class="d">{html.escape(r["detail"])}</span></div>'
        for r in integrity_rows
    )

    svg_map, map_meta = _build_svg_map(
        watershed, transposition, transposition_valid, audit["events"]
    )
    domain_svg, domain_stats = _build_domain_section(
        watershed, transposition, transposition_valid
    )
    conus_svg = _build_conus_overview(watershed, transposition)
    ws_zoom_svg, ws_metrics = _build_watershed_zoom(watershed)
    if ws_zoom_svg:
        c = ws_metrics.get("circularity", 0)
        # Polsby–Popper: 1.0 = perfect circle, real watersheds typically < 0.8.
        # The shape can legitimately be a circle (e.g. a sliding-template
        # catalog uses a fixed-area circular search window), so describe the
        # shape without judging it.
        if c >= 0.95:
            note = '<small>circular template</small>'
        elif c >= 0.7:
            note = '<small>compact</small>'
        else:
            note = '<small>irregular</small>'
        warn_cls = ""
        ws_zoom_card = (
            '<div class="ws-card">'
            f'<div class="t"><span>Watershed shape</span>{note}</div>'
            f'{ws_zoom_svg}'
            '<div class="metrics">'
            '<div><div class="l">Vertices</div>'
            f'<div class="v">{ws_metrics["vertices"]}</div></div>'
            '<div><div class="l">Perimeter</div>'
            f'<div class="v">{ws_metrics["perim_km"]:,.1f} km</div></div>'
            '<div><div class="l">Circularity</div>'
            f'<div class="v{warn_cls}">{ws_metrics["circularity"]:.2f}</div></div>'
            '<div><div class="l">vs circle</div>'
            f'<div class="v">1.00 = circle</div></div>'
            '</div></div>'
        )
    else:
        ws_zoom_card = ""

    def _fmt_bbox(b: list | None) -> str:
        if not (isinstance(b, list) and len(b) >= 4):
            return ""
        return (f"lon {b[0]:.2f} → {b[2]:.2f}<br>"
                f"lat {b[1]:.2f} → {b[3]:.2f}")

    def _fmt_area(km2: float) -> str:
        mi2 = km2 * 0.3861
        if km2 < 1000:
            return f"{km2:,.1f} km² <span style='color:var(--mute);font-size:11px;font-weight:500'>({mi2:,.1f} mi²)</span>"
        return f"{km2:,.0f} km² <span style='color:var(--mute);font-size:11px;font-weight:500'>({mi2:,.0f} mi²)</span>"

    domain_stats_html_parts: list[str] = []
    ws_area = domain_stats.get("Watershed", {}).get("area_km2", 0.0)
    td_area = domain_stats.get("Transposition Domain", {}).get("area_km2", 0.0)
    vr_area = domain_stats.get("Valid Region", {}).get("area_km2", 0.0)
    sampling_ratio = (td_area / ws_area) if ws_area else 0.0

    if ws_area:
        ws_bbox = (_feature_geom(watershed) or {}).get("bbox") or (
            watershed.get("bbox") if isinstance(watershed, dict) else None
        )
        domain_stats_html_parts.append(
            f'<div class="domain-stat"><div class="l">Watershed area</div>'
            f'<div class="a">{_fmt_area(ws_area)}</div>'
            f'<div class="e">{_fmt_bbox(ws_bbox)}</div></div>'
        )
    if td_area:
        td_bbox = (
            transposition.get("bbox") if isinstance(transposition, dict) else None
        )
        domain_stats_html_parts.append(
            f'<div class="domain-stat t"><div class="l">Transposition area</div>'
            f'<div class="a">{_fmt_area(td_area)}</div>'
            f'<div class="e">{_fmt_bbox(td_bbox)}</div></div>'
        )
    if vr_area:
        vr_bbox = (
            transposition_valid.get("bbox") if isinstance(transposition_valid, dict) else None
        )
        domain_stats_html_parts.append(
            f'<div class="domain-stat v"><div class="l">Valid sub-region</div>'
            f'<div class="a">{_fmt_area(vr_area)}</div>'
            f'<div class="e">{_fmt_bbox(vr_bbox)}</div></div>'
        )
    if sampling_ratio > 1:
        domain_stats_html_parts.append(
            f'<div class="domain-stat"><div class="l">Sampling ratio</div>'
            f'<div class="a">{sampling_ratio:,.1f}×</div>'
            f'<div class="e">transposition ÷ watershed</div></div>'
        )
    domain_stats_html = (
        '<div class="domain-stats">' + "".join(domain_stats_html_parts) + "</div>"
    )

    # DSS size lookup by item id (parsed from filename's _r{NNN} suffix)
    dss_size_by_id: dict[str, int] = {}
    for d in audit["dss_files"]:
        name = d["name"]
        try:
            r_part = name.rsplit("_r", 1)[1].split(".", 1)[0]
            item_id = str(int(r_part))
            dss_size_by_id[item_id] = d["size_bytes"]
        except (IndexError, ValueError):
            pass

    outlier_ids = []
    for d in audit["outlier_dss"]:
        try:
            r_part = d["name"].rsplit("_r", 1)[1].split(".", 1)[0]
            outlier_ids.append(str(int(r_part)))
        except (IndexError, ValueError):
            pass

    season_svg, season_legend = _build_season_donut(audit["events"])
    month_bars, month_labels = _build_month_bars(audit["events"])
    gallery_html = _build_gallery(audit["events"], events_prefix)
    dss_strip_html = _build_dss_strip(
        audit["events"], dss_size_by_id, audit["median_dss_bytes"]
    )

    if http_mode:
        nav_links = " ".join(
            f'<a href="/audit/{html.escape(r["run_name"])}">{html.escape(r["catalog_id"])}</a>'
            for r in all_runs
            if r["run_name"] != run_name
        )
        nav_links = '<a href="/">Index</a> ' + nav_links
    else:
        nav_links = " ".join(
            f'<a href="../../{r["run_name"]}/audit/report.html">{html.escape(r["catalog_id"])}</a>'
            for r in all_runs
            if r["run_name"] != run_name
        )
        nav_links = '<a href="../../audit-index.html">Index</a> ' + nav_links

    # Pull the manifest's free-form description if cached. For these catalogs
    # the description encodes design intent ("maximizes over 1000 sq mi
    # circle") that explains otherwise-suspicious geometry, so it belongs in
    # the header where reviewers will see it first.
    catalog_desc = ""
    payload_path = audit_dir / "payload.json"
    if payload_path.is_file():
        try:
            mp = json.loads(payload_path.read_text())
            catalog_desc = (mp.get("attributes") or {}).get(
                "catalog_description", ""
            ) or ""
        except (OSError, json.JSONDecodeError):
            pass

    subtitle_core = (
        f"{audit['storm_duration']}-hour storms · "
        f"{audit['attrs'].get('start_date','?')} → "
        f"{audit['attrs'].get('end_date','?')} · "
        f"top {audit['top_n']}"
    )
    subtitle = (
        f"{html.escape(catalog_desc)}<br>{html.escape(subtitle_core)}"
        if catalog_desc else html.escape(subtitle_core)
    )

    # Sentinel replacement — keeps the JS/CSS in the template free of
    # `{{`/`}}` escaping that .format() would otherwise require.
    repl = {
        "__CATALOG_ID__": html.escape(cid),
        "__TITLE__": html.escape(f"Audit — {cid}"),
        # subtitle already contains escaped fragments + literal <br>
        "__SUBTITLE__": subtitle,
        "__NAV_LINKS__": nav_links,
        "__SUMMARY_STATS__": "".join(summary_chunks),
        "__ANOMALIES_HTML__": "\n".join(anomalies) if anomalies else "",
        "__INTEGRITY_HTML__": integrity_html,
        "__DOMAIN_SVG__": domain_svg,
        "__CONUS_SVG__": conus_svg,
        "__WS_ZOOM_CARD__": ws_zoom_card,
        "__DOMAIN_STATS__": domain_stats_html,
        "__SVG_MAP__": svg_map,
        "__GALLERY_HTML__": gallery_html,
        "__SEASON_DONUT__": season_svg,
        "__SEASON_LEGEND__": season_legend,
        "__MONTH_BARS__": month_bars,
        "__MONTH_LABELS__": month_labels,
        "__YEAR_BARS__": year_bars_html,
        "__DSS_STRIP__": dss_strip_html,
        "__N_EVENTS__": str(audit["n_events"]),
        "__N_DSS__": str(audit["n_dss"]),
        "__EXPECTED_HOURS__": json.dumps(audit["storm_duration"] or None),
        "__EVENTS_JSON__": json.dumps(audit["events"]),
        "__DSS_SIZE_JSON__": json.dumps(dss_size_by_id),
        "__OUTLIERS_JSON__": json.dumps(outlier_ids),
        "__MAP_META_JSON__": json.dumps(map_meta),
        "__EVENTS_PREFIX_JSON__": json.dumps(events_prefix),
    }
    report_html = _REPORT_TEMPLATE
    for k, v in repl.items():
        report_html = report_html.replace(k, v)

    if http_mode:
        return report_html
    out_path = audit_dir / "report.html"
    out_path.write_text(report_html, encoding="utf-8")
    return out_path


def _read_json(p: Path) -> Any | None:
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        return None


# ─── serve ────────────────────────────────────────────────────────────────────


def _serve(port: int = 8745, host: str = "127.0.0.1") -> None:
    os.chdir(OUTPUTS)
    httpd = ThreadingHTTPServer((host, port), SimpleHTTPRequestHandler)
    display_host = "127.0.0.1" if host in ("0.0.0.0", "::") else host
    url = f"http://{display_host}:{port}/audit-index.html"
    print(f"\nServing audit reports at {url}")
    if host in ("0.0.0.0", "::"):
        # When binding to all interfaces, surface the LAN-reachable URLs too —
        # the user is most likely browsing from another machine on the LAN.
        import socket

        for addr in _lan_addrs():
            print(f"  also reachable as http://{addr}:{port}/audit-index.html")
        print(
            "  (server is bound to all interfaces — anyone on the LAN can read these "
            "reports. Stop with Ctrl-C when done.)"
        )
    print("Ctrl-C to stop.\n")
    if host == "127.0.0.1":
        # Only auto-open when bound to loopback — otherwise we're on a headless
        # box and there's no local browser to spawn.
        try:
            webbrowser.open(url)
        except Exception:
            pass
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        httpd.server_close()


def _lan_addrs() -> list[str]:
    """LAN IPv4 addresses for this host, in a deterministic order. Skips
    loopback, docker bridges, and link-local. Used by _serve() to print
    every URL the user might reach the server on.
    """
    import socket

    out = []
    try:
        names = socket.getaddrinfo(
            socket.gethostname(), None, socket.AF_INET, socket.SOCK_STREAM
        )
        for n in names:
            ip = n[4][0]
            if ip.startswith(("127.", "172.17.", "172.18.", "169.254.")):
                continue
            if ip not in out:
                out.append(ip)
    except OSError:
        pass
    return out


# ─── CLI dispatch ─────────────────────────────────────────────────────────────


def _do_download(targets: list[str]) -> None:
    _load_hec_env()
    for t in targets:
        print(f"\n=== Downloading {t} ===")
        _download_run(t)


def _do_report(targets: list[str]) -> None:
    all_runs: list[dict] = []
    for t in targets:
        audit_dir = OUTPUTS / t / "audit"
        if not audit_dir.is_dir():
            print(f"  skip {t}: no audit/ — run `./audit.py download {t}` first")
            continue
        a = _audit(t)
        all_runs.append(a)
    if not all_runs:
        return
    for a in all_runs:
        path = _build_report(a["run_name"], a, all_runs)
        print(f"  ✓ wrote {path.relative_to(ROOT)}")
    index_path = OUTPUTS / "audit-index.html"
    index_path.write_text(_index_html(all_runs), encoding="utf-8")
    print(f"  ✓ wrote {index_path.relative_to(ROOT)}")


def main() -> None:
    argv = sys.argv[1:]
    if argv and argv[0] in ("-h", "--help", "help"):
        print(__doc__)
        return

    if not argv:
        targets = _known_runs()
        if not targets:
            print("No runs found in compute/outputs/.", file=sys.stderr)
            sys.exit(1)
        _do_download(targets)
        _do_report(targets)
        _serve()
        return

    cmd = argv[0]
    rest = argv[1:]

    if cmd == "download":
        targets = rest or _known_runs()
        _do_download(targets)
        return
    if cmd == "report":
        targets = rest or _known_runs()
        _do_report(targets)
        return
    if cmd == "serve":
        host = "127.0.0.1"
        port = 8745
        i = 0
        while i < len(rest):
            tok = rest[i]
            if tok == "--host" and i + 1 < len(rest):
                host = rest[i + 1]
                i += 2
            elif tok.isdigit():
                port = int(tok)
                i += 1
            else:
                print(f"unrecognized arg: {tok}", file=sys.stderr)
                sys.exit(2)
        # Auditing is now part of the unified web app (./run.py web, :8744),
        # which serves reports inline at /audit/<name>. Forward there instead
        # of standing up a second static server on :8745.
        print(
            "note: audit reports are now served by the unified web app. "
            f"Forwarding to ./run.py web on port {8744 if port == 8745 else port}.",
            file=sys.stderr,
        )
        from web import serve as _web_serve

        _web_serve(host=host, port=(8744 if port == 8745 else port))
        return
    print(f"Unknown command: {cmd}", file=sys.stderr)
    print(__doc__, file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
