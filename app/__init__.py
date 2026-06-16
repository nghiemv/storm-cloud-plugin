#!/usr/bin/env python3
"""storm-cloud-plugin web app: launch, monitor, and audit runs.

A stdlib-only JSON API + static file server (no host pip installs). Browse S3
payloads, launch/stop/resume runs against the local MinIO stack or HEC S3,
watch weighted progress, and view rich per-catalog audit reports inline.

Dashboard markup lives in static/ (index.html, style.css, app.js); the audit
report template lives in static/report.html. Run via ``./run.py web``.
Localhost-only, no auth.
"""

from __future__ import annotations

import html
import json
import mimetypes
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from app.maps import (
    _feature_geom,
    _geom_to_path_d,
    _polygon_area_km2,
    _polygon_bbox,
    _polygon_centroid,
    _projector,
    _walk_coords,
)
from app.status import (
    _derive_status,
    _overall_pct,
    _runtime_eta_s,
    _scan_launch_log,
    _tail_log,
)

from app.core import (
    HEC_ENV,
    OUTPUTS,
    ROOT,
    RUN_PY,
    STATIC,
    _EVENTS_FILES,
    _TOP_FILES,
    _load_hec_env,
    _read_json,
    _safe_subdir,
    write_launch_json as write_launch_json,
)
from app.launch import _launch_hec, _launch_local, _rerun, _stop


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
            ev["bbox"] = [
                float(bbox[0]),
                float(bbox[1]),
                float(bbox[2]),
                float(bbox[3]),
            ]
        props = item.get("properties") or {}
        ev["storm_end"] = props.get("end_datetime")
        if not ev.get("storm_start"):
            ev["storm_start"] = props.get("start_datetime")

    # DSS listing
    dss = _parse_listing(audit_dir)
    median_bytes = sorted(d["size_bytes"] for d in dss)[len(dss) // 2] if dss else 0
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
    grid_dss_names = {Path(e["dss_file"]).name for e in grid_entries if e["dss_file"]}
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
    bbox = _polygon_bbox(
        audit_dir / "hydro_domains" / f"{catalog_id}-transposition.json"
    )
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
        from datetime import datetime

        for ev in events:
            s, e = ev.get("storm_start"), ev.get("storm_end")
            if not s or not e:
                continue
            try:
                # storm_start_date (from the events geojson) is naive
                # ("%Y-%m-%dT%H") while the STAC end_datetime is tz-aware
                # ("…Z"). Normalize both to naive so the subtraction can't
                # raise "can't subtract offset-naive and offset-aware".
                ds = datetime.fromisoformat(s.replace("Z", "+00:00")).replace(
                    tzinfo=None
                )
                de = datetime.fromisoformat(e.replace("Z", "+00:00")).replace(
                    tzinfo=None
                )
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
            if (
                elon_max < lon_min
                or elon_min > lon_max
                or elat_max < lat_min
                or elat_min > lat_max
            ):
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
        "</linearGradient></defs>",
        f'<rect width="{w}" height="{h}" fill="url(#bg-c)"/>',
    ]

    # State outlines (gray fill, lighter strokes)
    parts.append('<g class="states">')
    for feat in conus.get("features") or []:
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
        "transposition": (
            "transposition_region",
            "Transposition file declares 'transposition_region'",
        ),
        "transposition_valid": (
            "valid_transposition_region",
            "Valid file declares 'valid_transposition_region'",
        ),
    }
    feats = {
        "watershed": watershed,
        "transposition": transposition,
        "transposition_valid": transposition_valid,
    }
    for slot, (expected_type, label) in expected.items():
        feat = feats[slot]
        if not feat:
            out.append(
                {
                    "key": slot + "_present",
                    "status": "warn",
                    "detail": f"{label} — file missing",
                }
            )
            continue
        declared = (feat.get("properties") or {}).get("hydro_domain:type")
        if declared == expected_type:
            out.append({"key": slot + "_type", "status": "ok", "detail": label})
        else:
            out.append(
                {
                    "key": slot + "_type",
                    "status": "bad",
                    "detail": f"{label} — found '{declared}'",
                }
            )

    # Watershed must be smaller than transposition
    wa = _polygon_area_km2(_feature_geom(watershed))
    ta = _polygon_area_km2(_feature_geom(transposition))
    if wa and ta:
        if wa < ta:
            out.append(
                {
                    "key": "area_order",
                    "status": "ok",
                    "detail": f"Watershed ({wa:,.0f} km²) < transposition "
                    f"({ta:,.0f} km²)",
                }
            )
        else:
            out.append(
                {
                    "key": "area_order",
                    "status": "bad",
                    "detail": f"Watershed ({wa:,.0f} km²) ≥ transposition "
                    f"({ta:,.0f} km²) — likely SWAPPED",
                }
            )

    # Watershed centroid should be inside the transposition bbox
    wc = _polygon_centroid(_feature_geom(watershed))
    tb = transposition.get("bbox") if isinstance(transposition, dict) else None
    if wc and isinstance(tb, list) and len(tb) >= 4:
        inside = tb[0] <= wc[0] <= tb[2] and tb[1] <= wc[1] <= tb[3]
        if inside:
            out.append(
                {
                    "key": "ws_in_tx",
                    "status": "ok",
                    "detail": f"Watershed centroid ({wc[0]:.2f}, "
                    f"{wc[1]:.2f}) is inside the transposition bbox",
                }
            )
        else:
            out.append(
                {
                    "key": "ws_in_tx",
                    "status": "bad",
                    "detail": f"Watershed centroid ({wc[0]:.2f}, "
                    f"{wc[1]:.2f}) outside transposition bbox "
                    f"{tb} — likely SWAPPED",
                }
            )

    # IDs in the GeoJSON properties (extra cross-check: the STAC id should
    # contain the catalog name)
    for slot, feat in feats.items():
        if not feat:
            continue
        fid = feat.get("id") or ""
        if not fid:
            continue
        if catalog_id.lower() in fid.lower():
            out.append(
                {
                    "key": slot + "_id",
                    "status": "ok",
                    "detail": f"{slot} STAC id is '{fid}'",
                }
            )
        else:
            out.append(
                {
                    "key": slot + "_id",
                    "status": "warn",
                    "detail": f"{slot} STAC id '{fid}' doesn't reference "
                    f"catalog '{catalog_id}'",
                }
            )

    return out


def _build_watershed_zoom(watershed: dict | None, width: int = 280) -> tuple[str, dict]:
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
    circularity = (4 * math.pi * area_km2 / (perim_km**2)) if perim_km else 0.0

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
        "</linearGradient></defs>",
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
        (
            "Transposition Domain",
            transposition,
            "#fb923c",
            "url(#stripes-t)",
            1.0,
            2.0,
            "6 4",
        ),
        ("Valid Region", transposition_valid, "#22c55e", "#22c55e", 0.18, 1.4, ""),
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
        bbox_l = feat.get("bbox") if isinstance(feat, dict) else None
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
    bar_px = bar_deg / (lon_max - lon_min) * w
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


def _stat_html(label: str, value: str, *, warn: bool = False, ok: bool = False) -> str:
    cls = "stat" + (" warn" if warn else " ok" if ok else "")
    return (
        f'<div class="{cls}"><div class="label">{html.escape(label)}</div>'
        f'<div class="value">{html.escape(value)}</div></div>'
    )


def _build_report(run_name: str, audit: dict, all_runs: list[dict]) -> str:
    """Render a catalog's audit report as an HTML string.

    Asset URLs are absolute, rooted at the app's routes (``/assets/<name>/...``
    thumbnails, ``/audit/<other>`` nav) so the server serves it inline.
    """
    audit_dir = OUTPUTS / run_name / "audit"
    events_prefix = f"/assets/{run_name}/events"

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
            if isinstance(mean_min, (int, float))
            else "—",
        ),
        _stat_html(
            "Date span",
            f"{audit['attrs'].get('start_date', '?')} → {audit['attrs'].get('end_date', '?')}",
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
            f"({ev.get('storm_start', '?')}) at "
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
            if len(audit["duration_mismatches"]) > 15
            else ""
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
            note = "<small>circular template</small>"
        elif c >= 0.7:
            note = "<small>compact</small>"
        else:
            note = "<small>irregular</small>"
        warn_cls = ""
        ws_zoom_card = (
            '<div class="ws-card">'
            f'<div class="t"><span>Watershed shape</span>{note}</div>'
            f"{ws_zoom_svg}"
            '<div class="metrics">'
            '<div><div class="l">Vertices</div>'
            f'<div class="v">{ws_metrics["vertices"]}</div></div>'
            '<div><div class="l">Perimeter</div>'
            f'<div class="v">{ws_metrics["perim_km"]:,.1f} km</div></div>'
            '<div><div class="l">Circularity</div>'
            f'<div class="v{warn_cls}">{ws_metrics["circularity"]:.2f}</div></div>'
            '<div><div class="l">vs circle</div>'
            f'<div class="v">1.00 = circle</div></div>'
            "</div></div>"
        )
    else:
        ws_zoom_card = ""

    def _fmt_bbox(b: list | None) -> str:
        if not (isinstance(b, list) and len(b) >= 4):
            return ""
        return f"lon {b[0]:.2f} → {b[2]:.2f}<br>lat {b[1]:.2f} → {b[3]:.2f}"

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
        td_bbox = transposition.get("bbox") if isinstance(transposition, dict) else None
        domain_stats_html_parts.append(
            f'<div class="domain-stat t"><div class="l">Transposition area</div>'
            f'<div class="a">{_fmt_area(td_area)}</div>'
            f'<div class="e">{_fmt_bbox(td_bbox)}</div></div>'
        )
    if vr_area:
        vr_bbox = (
            transposition_valid.get("bbox")
            if isinstance(transposition_valid, dict)
            else None
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

    nav_links = " ".join(
        f'<a href="/audit/{html.escape(r["run_name"])}">{html.escape(r["catalog_id"])}</a>'
        for r in all_runs
        if r["run_name"] != run_name
    )
    nav_links = '<a href="/">Index</a> ' + nav_links

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
        f"{audit['attrs'].get('start_date', '?')} → "
        f"{audit['attrs'].get('end_date', '?')} · "
        f"top {audit['top_n']}"
    )
    subtitle = (
        f"{html.escape(catalog_desc)}<br>{html.escape(subtitle_core)}"
        if catalog_desc
        else html.escape(subtitle_core)
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
        "__N_EVENTS__": str(audit["n_events"]),
        "__N_DSS__": str(audit["n_dss"]),
        "__EXPECTED_HOURS__": json.dumps(audit["storm_duration"] or None),
        "__MEDIAN_DSS__": json.dumps(audit["median_dss_bytes"]),
        "__EVENTS_JSON__": json.dumps(audit["events"]),
        "__DSS_SIZE_JSON__": json.dumps(dss_size_by_id),
        "__OUTLIERS_JSON__": json.dumps(outlier_ids),
        "__WATERSHED_GEOM_JSON__": json.dumps(_feature_geom(watershed)),
        "__TRANSPOSITION_GEOM_JSON__": json.dumps(_feature_geom(transposition)),
        "__TRANSPOSITION_VALID_GEOM_JSON__": json.dumps(
            _feature_geom(transposition_valid)
        ),
        "__EVENTS_PREFIX_JSON__": json.dumps(events_prefix),
    }
    report_html = (STATIC / "report.html").read_text(encoding="utf-8")
    for k, v in repl.items():
        report_html = report_html.replace(k, v)

    return report_html


_HIST_TTL_S = 10.0  # cache historical scan; the dashboard polls every 2s
_hist_cache: dict = {"at": 0.0, "data": None}


def _has_run_output(run_dir: Path) -> bool:
    """A run dir without launch/progress markers might still be a completed
    run from a CLI invocation. Recognize it by the presence of any file or
    subdir — stormhub leaves ``config.json``, catalog dirs, DSS files, etc.
    """
    try:
        for _ in run_dir.iterdir():
            return True
    except OSError:
        pass
    return False


def _list_runs() -> list[dict]:
    if not OUTPUTS.is_dir():
        return []
    runs: list[dict] = []
    entries = sorted(OUTPUTS.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    for run_dir in entries:
        if not run_dir.is_dir():
            continue
        progress = _read_json(run_dir / "progress.json")
        launch = _read_json(run_dir / "launch.json")
        has_output = _has_run_output(run_dir)
        if progress is None and launch is None and not has_output:
            continue
        status = _derive_status(progress, launch)
        if status == "unknown" and has_output and progress is None and launch is None:
            # Legacy CLI run: no markers but the dir holds plugin output.
            # Surface it as "done" so the user can re-run from the UI.
            status = "done"
        rec = {
            "name": run_dir.name,
            "status": status,
            "started_at": (progress or {}).get("started_at")
            or (launch or {}).get("launched_at"),
            "elapsed_s": (progress or {}).get("elapsed_s"),
            "current_step": (progress or {}).get("current_step"),
            "summary": (progress or {}).get("summary"),
            "plan": (progress or {}).get("plan", []),
            "action_progress": (progress or {}).get("action_progress", {}),
            "completed_steps": (progress or {}).get("completed_steps", []),
            # Used by the Re-run button. None when we don't know the
            # payload UUID (legacy CLI run without launch.json).
            "payload_uuid": (launch or {}).get("payload_uuid"),
            # Audit availability so the dashboard can link/trigger audits.
            "has_audit": _has_audit(run_dir.name),
        }
        dljob = _download_jobs.get(run_dir.name)
        if dljob and dljob.get("state") == "running":
            rec["audit_downloading"] = True
        if status == "running":
            # The cumsum process-storms step emits no action_progress; derive
            # its real sub-progress from launch.log's per-year lines and inject
            # a synthetic entry so the weighted bar + ETA pick it up naturally.
            cs = rec.get("current_step") or {}
            if (
                cs.get("name") == "process-storms"
                and "process-storms" not in rec["action_progress"]
            ):
                scan = _scan_launch_log(run_dir, cs.get("started_at"))
                if scan:
                    rec["action_progress"] = {
                        **rec["action_progress"],
                        "process-storms": scan,
                    }
            rec["eta_s"] = _runtime_eta_s(rec)
            rec["overall_pct"] = _overall_pct(rec)
        if status in ("failed", "interrupted"):
            rec["error_tail"] = _tail_log(run_dir / "launch.log")
        runs.append(rec)
    return runs


# ─── HEC S3 payload listing ──────────────────────────────────────────────────


def _list_payloads() -> dict:
    """Three distinct response shapes — the UI picks branches off them:
    {"state": "unconfigured"}              — no env file yet
    {"state": "error",  "detail": "..."}   — env present, listing failed
    {"state": "ok",     "payloads": [...]} — listing succeeded (may be empty)

    Each payload dict carries uuid, mtime, and (for parseable payloads)
    catalog_id, catalog_description, start_date, end_date, storm_duration,
    top_n_events. See plugin/cli.py:_cmd_list_payloads.
    """
    if not HEC_ENV.is_file():
        return {"state": "unconfigured"}
    r = subprocess.run(
        [sys.executable, str(RUN_PY), "hec", "list", "--json"],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if r.returncode != 0:
        return {"state": "error", "detail": (r.stderr or r.stdout).strip()}
    try:
        payloads = json.loads(r.stdout or "[]")
    except json.JSONDecodeError as e:
        return {"state": "error", "detail": f"could not parse list output: {e}"}
    # Annotate each payload with any existing local run for its catalog, so the
    # UI offers "Run" only for catalogs never run — an already-run catalog is
    # managed from Recent runs, not one-click re-launched from here.
    runs_by_name = {r["name"]: r["status"] for r in _list_runs()}
    for p in payloads:
        cid = p.get("catalog_id")
        nm = _safe_subdir(cid) if cid else None
        if nm and nm in runs_by_name:
            p["run_name"] = nm
            p["run_status"] = runs_by_name[nm]
    return {"state": "ok", "payloads": payloads}


# ─── audit integration (download + render reports inline) ───────────────────


def _has_audit(name: str) -> bool:
    return (OUTPUTS / name / "audit").is_dir()


def _audit_nav_runs() -> list[dict]:
    """Lightweight {run_name, catalog_id} list for report nav links — avoids a
    full ``_audit()`` per run (the report's nav loop only needs these two)."""
    runs = []
    for r in _known_runs():
        if _has_audit(r):
            runs.append({"run_name": r, "catalog_id": _catalog_id_for(r)})
    return runs


def _render_audit_html(name: str) -> tuple[str, int]:
    """(html, status) for GET /audit/<name>."""
    if not _has_audit(name):
        body = (
            "<!doctype html><meta charset=utf-8>"
            f"<title>Audit — {html.escape(name)}</title>"
            "<body style='font-family:system-ui;max-width:640px;margin:4rem auto'>"
            f"<h1>{html.escape(name)}</h1>"
            "<p>No audit artifacts downloaded yet for this catalog.</p>"
            "<p>Use the <b>Download audit</b> button on the dashboard to fetch "
            "them, then reload.</p><p><a href='/'>← back</a></p></body>"
        )
        return body, 200
    try:
        a = _audit(name)
        report = _build_report(name, a, _audit_nav_runs())
        return report, 200
    except Exception as e:  # noqa: BLE001 — surface render errors to the page
        return (
            f"<!doctype html><meta charset=utf-8><h1>Audit render error</h1>"
            f"<pre>{html.escape(repr(e))}</pre><p><a href='/'>← back</a></p>",
            500,
        )


def _audit_summary(name: str) -> dict:
    """Compact JSON summary for GET /api/audit/<name>."""
    if not _has_audit(name):
        return {"name": name, "state": "not-downloaded"}
    try:
        a = _audit(name)
    except Exception as e:  # noqa: BLE001
        return {"name": name, "state": "error", "error": repr(e)}
    n_anomalies = (
        len(a.get("outlier_dss") or [])
        + len(a.get("grid_without_dss") or [])
        + len(a.get("out_of_box") or [])
        + len(a.get("duration_mismatches") or [])
    )
    summary = {
        "name": name,
        "state": "downloaded",
        "catalog_id": a.get("catalog_id"),
        "n_events": a.get("n_events"),
        "n_dss": a.get("n_dss"),
        "top_n": a.get("top_n"),
        "n_anomalies": n_anomalies,
    }
    # Overlay an in-flight download job, if any.
    job = _download_jobs.get(name)
    if job and job.get("state") == "running":
        summary["download"] = "running"
    return summary


# ─── background audit download ───────────────────────────────────────────────
#
# Downloading a catalog's audit artifacts (~600 MB of prefixes via mc) must not
# block the request thread. We run it in a daemon thread and expose status via
# /api/audit/<name>; the dashboard polls and flips the button to "Downloading…".

_download_jobs: dict[str, dict] = {}
_download_lock = threading.Lock()


def _start_audit_download(name: str) -> dict:
    with _download_lock:
        existing = _download_jobs.get(name)
        if existing and existing.get("state") == "running":
            return existing
        job = {"state": "running", "started_at": time.time(), "error": None}
        _download_jobs[name] = job

    def _worker() -> None:
        try:
            _load_hec_env()
            _download_run(name)
            result = {"state": "done", "started_at": job["started_at"], "error": None}
        except Exception as e:  # noqa: BLE001 — surface to the dashboard
            result = {
                "state": "error",
                "started_at": job["started_at"],
                "error": repr(e),
            }
        with _download_lock:
            _download_jobs[name] = result

    threading.Thread(target=_worker, daemon=True).start()
    return job


# ─── unified S3-centric catalog discovery ────────────────────────────────────


# Top-level S3 prefixes that are infrastructure, not storm catalogs.
_NON_CATALOG_PREFIXES = {
    "manifests",
    "aorc-cache",
    "aorc-cache-conus",
    "diagnostic-throughput",
}


def _s3_output_catalogs() -> tuple[set[str], str | None]:
    """Catalog ids that have an output prefix in S3. Best-effort: one `mc ls`
    of the bucket root, infrastructure prefixes filtered out. Returns
    (catalog_ids, note) where note explains why the set is empty/partial."""
    try:
        _load_hec_env()
        lines = _mc_ls_lines("")
    except Exception as e:  # noqa: BLE001 — mc/alias may be absent; degrade
        return set(), f"S3 output listing unavailable: {e}"
    cids = set()
    for ln in lines:
        tok = ln.split()[-1] if ln.split() else ""
        name = tok.rstrip("/")
        if name and name not in _NON_CATALOG_PREFIXES:
            cids.add(name)
    return cids, None


def _list_catalogs() -> dict:
    """Unified, S3-centric catalog list keyed by catalog_id.

    Merges three sources: S3 manifest payloads (launchable), local runs
    (compute/outputs, with live progress), and S3 output prefixes (auditable
    even without a local run). HEC S3 is the source of truth for what exists;
    local progress is overlaid for runs executing on this machine.
    """
    by_cid: dict[str, dict] = {}

    def rec(cid: str) -> dict:
        return by_cid.setdefault(
            cid,
            {
                "catalog_id": cid,
                "uuid": None,
                "attrs": {},
                "predicted_s": None,
                "local_run": None,
                "s3_outputs": False,
                "audit": "none",
            },
        )

    payloads = _list_payloads()
    pstate = payloads.get("state")
    if pstate == "ok":
        for p in payloads.get("payloads", []):
            cid = p.get("catalog_id") or p.get("uuid")
            if not cid:
                continue
            r = rec(cid)
            r["uuid"] = p.get("uuid")
            r["predicted_s"] = p.get("predicted_s")
            r["attrs"] = {
                k: p.get(k)
                for k in (
                    "catalog_id",
                    "catalog_description",
                    "start_date",
                    "end_date",
                    "storm_duration",
                    "top_n_events",
                )
                if p.get(k) is not None
            }

    for run in _list_runs():
        cid = _catalog_id_for(run["name"])
        r = rec(cid)
        r["local_run"] = run
        if not r["uuid"] and run.get("payload_uuid"):
            r["uuid"] = run["payload_uuid"]
        if _has_audit(run["name"]):
            r["audit"] = "downloaded"

    s3_cids, s3_note = _s3_output_catalogs()
    for cid in s3_cids:
        r = rec(cid)
        r["s3_outputs"] = True
        if r["audit"] == "none":
            r["audit"] = "available"  # outputs exist in S3, can download to audit

    # Sort: active runs first, then by catalog_id.
    def _key(r: dict) -> tuple:
        lr = r.get("local_run") or {}
        active = 0 if lr.get("status") == "running" else 1
        return (active, r["catalog_id"].lower())

    return {
        "state": pstate,
        "catalogs": sorted(by_cid.values(), key=_key),
        "s3_note": s3_note,
    }


def _get_run(name: str) -> dict | None:
    for r in _list_runs():
        if r["name"] == name:
            return r
    return None


def _step_breakdown(run: dict) -> list[dict]:
    """Per-step rows for the detail view: name, even weight%, state, detail."""
    plan = run.get("plan") or []
    if not plan:
        return []
    pct = round(100 / len(plan), 1)
    completed = {s.get("name"): s for s in run.get("completed_steps", [])}
    cur = (run.get("current_step") or {}).get("name")
    rows = []
    for nm in plan:
        if nm in completed:
            state, detail = "done", f"{completed[nm].get('duration_s', 0):.1f}s"
        elif nm == cur:
            ap = (run.get("action_progress") or {}).get(nm) or {}
            state = "running"
            detail = f"{ap.get('done', 0)}/{ap['total']}" if ap.get("total") else "…"
        else:
            state, detail = "pending", "—"
        rows.append({"name": nm, "weight_pct": pct, "state": state, "detail": detail})
    return rows


def _render_run_detail_html(name: str) -> tuple[str, int]:
    run = _get_run(name)
    if not run:
        return (
            f"<!doctype html><meta charset=utf-8><h1>{html.escape(name)}</h1>"
            "<p>No such run.</p><p><a href='/'>← back</a></p>",
            404,
        )
    rows = _step_breakdown(run)
    pct = run.get("overall_pct")
    eta = run.get("eta_s")
    bar_rows = "".join(
        f"<tr class='st-{r['state']}'><td>{html.escape(r['name'])}</td>"
        f"<td style='text-align:right'>{r['weight_pct']}%</td>"
        f"<td>{r['state']}</td><td>{html.escape(str(r['detail']))}</td></tr>"
        for r in rows
    )
    log_tail = html.escape(_tail_log(OUTPUTS / name / "launch.log", 40))
    audit_link = (
        f"<a href='/audit/{html.escape(name)}'>View audit report →</a>"
        if _has_audit(name)
        else "<span class='muted'>No audit downloaded</span>"
    )
    body = f"""<!doctype html><meta charset=utf-8>
<title>{html.escape(name)} — run detail</title>
<style>
 body{{font-family:system-ui,sans-serif;max-width:880px;margin:2rem auto;padding:0 1rem;color:#1f2937}}
 h1{{margin-bottom:.2rem}} .muted{{color:#6b7280}}
 .bar{{height:14px;background:#e5e7eb;border-radius:7px;overflow:hidden;margin:.6rem 0}}
 .bar>div{{height:100%;background:#2563eb}}
 table{{border-collapse:collapse;width:100%;margin-top:1rem}}
 td,th{{padding:.4rem .6rem;border-bottom:1px solid #eee;text-align:left}}
 tr.st-done td{{color:#16a34a}} tr.st-running td{{font-weight:600;color:#2563eb}}
 tr.st-pending td{{color:#9ca3af}}
 pre{{background:#0b1021;color:#d6e2ff;padding:1rem;border-radius:8px;overflow:auto;font-size:12px;line-height:1.4}}
</style>
<p><a href="/">← dashboard</a></p>
<h1>{html.escape(name)}</h1>
<p class="muted">status: <b>{html.escape(run.get("status") or "?")}</b>
 · {audit_link}</p>
<div class="bar"><div style="width:{pct or 0}%"></div></div>
<p>{(str(pct) + "% complete") if pct is not None else ""}
 {("· ETA " + _fmt_dur(eta)) if eta else ""}</p>
<table><thead><tr><th>step</th><th style='text-align:right'>weight</th>
 <th>state</th><th>detail</th></tr></thead><tbody>{bar_rows}</tbody></table>
<h3>Recent log</h3><pre>{log_tail}</pre>
"""
    return body, 200


def _fmt_dur(s: float | None) -> str:
    if not s or s <= 0:
        return "—"
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {(s % 3600) // 60}m"


# ─── HTTP layer ──────────────────────────────────────────────────────────────


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, data, code: int = 200) -> None:
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html_str: str, code: int = 200) -> None:
        body = html_str.encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_asset(self, path: str) -> None:
        """Serve a file under compute/outputs/<name>/audit/ for /assets/<name>/...

        The only filesystem-exposed route. Guards against path traversal by
        resolving the target and asserting it stays within the run's audit dir.
        """
        rel = urllib.parse.unquote(path[len("/assets/") :])
        parts = rel.split("/", 1)
        if len(parts) != 2 or not parts[1]:
            self.send_error(404)
            return
        name, subpath = parts
        base = (OUTPUTS / name / "audit").resolve()
        try:
            target = (base / subpath).resolve()
        except (OSError, ValueError):
            self.send_error(404)
            return
        if base != target and base not in target.parents:
            self.send_error(403)
            return
        if not target.is_file():
            self.send_error(404)
            return
        ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        data = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_static(self, path: str) -> None:
        """Serve a dashboard asset from static/ for /static/<file>.

        Same path-traversal guard as _serve_asset. Sent with ``no-store`` so
        an edited style.css/app.js is never served stale from the browser
        cache during development.
        """
        rel = urllib.parse.unquote(path[len("/static/") :])
        base = STATIC.resolve()
        try:
            target = (base / rel).resolve()
        except (OSError, ValueError):
            self.send_error(404)
            return
        if base != target and base not in target.parents:
            self.send_error(403)
            return
        if not target.is_file():
            self.send_error(404)
            return
        ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        data = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path in ("/", "/index.html"):
            try:
                self._send_html((STATIC / "index.html").read_text(encoding="utf-8"))
            except OSError:
                self._send_html("<h1>static/index.html missing</h1>", 500)
        elif path.startswith("/static/"):
            self._serve_static(path)
        elif path == "/api/runs":
            self._send_json(_list_runs())
        elif path == "/api/payloads":
            self._send_json(_list_payloads())
        elif path == "/api/catalogs":
            self._send_json(_list_catalogs())
        elif path == "/api/health":
            self._send_json({"ok": True, "hec_configured": HEC_ENV.is_file()})
        elif path.startswith("/api/run/"):
            name = urllib.parse.unquote(path[len("/api/run/") :]).strip("/")
            run = _get_run(name) if name else None
            if run is None:
                self._send_json({"error": "no such run"}, 404)
            else:
                self._send_json(run)
        elif path.startswith("/run/"):
            name = urllib.parse.unquote(path[len("/run/") :]).strip("/")
            if not name:
                self.send_error(404)
                return
            body, code = _render_run_detail_html(name)
            self._send_html(body, code)
        elif path.startswith("/assets/"):
            self._serve_asset(path)
        elif path.startswith("/api/audit/"):
            name = urllib.parse.unquote(path[len("/api/audit/") :]).strip("/")
            if not name:
                self.send_error(404)
                return
            self._send_json(_audit_summary(name))
        elif path.startswith("/audit/"):
            name = urllib.parse.unquote(path[len("/audit/") :]).strip("/")
            if not name:
                self.send_error(404)
                return
            body, code = _render_audit_html(name)
            self._send_html(body, code)
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        try:
            if path == "/api/launch/local":
                self._send_json({"name": _launch_local()})
            elif path == "/api/launch/hec":
                data = json.loads(body or b"{}")
                uuid = data.get("uuid")
                if not uuid:
                    self._send_json({"error": "missing uuid"}, 400)
                    return
                attrs = data.get("attrs") or {}
                # ``catalog-prefix`` entries don't have a manifests/<uuid>/payload
                # yet — promote them first. ``run.py hec promote`` shells out to
                # plugin.cli inside Docker and is idempotent, so calling it for
                # an already-promoted catalog is a cheap no-op.
                catalog_key = data.get("catalog_key")
                if data.get("source") == "catalog-prefix" and catalog_key:
                    r = subprocess.run(
                        [sys.executable, str(RUN_PY), "hec", "promote", catalog_key],
                        capture_output=True,
                        text=True,
                        cwd=ROOT,
                    )
                    if r.returncode != 0:
                        self._send_json(
                            {
                                "error": f"promote failed: {(r.stderr or r.stdout).strip()}"
                            },
                            500,
                        )
                        return
                self._send_json(
                    {"name": _launch_hec(uuid, data.get("name"), payload_attrs=attrs)}
                )
            elif path == "/api/launch/rerun":
                data = json.loads(body or b"{}")
                name = data.get("name")
                if not name:
                    self._send_json({"error": "missing name"}, 400)
                    return
                new_name, err = _rerun(name)
                if err:
                    self._send_json({"error": err}, 400)
                    return
                self._send_json({"name": new_name})
            elif path == "/api/stop":
                data = json.loads(body or b"{}")
                name = data.get("name")
                if not name:
                    self._send_json({"error": "missing name"}, 400)
                    return
                stopped, err = _stop(name)
                if err:
                    self._send_json({"error": err}, 400)
                    return
                self._send_json({"stopped": stopped, "name": name})
            elif path.startswith("/api/audit/") and path.endswith("/download"):
                inner = path[len("/api/audit/") : -len("/download")]
                name = urllib.parse.unquote(inner).strip("/")
                if not name:
                    self._send_json({"error": "missing name"}, 400)
                    return
                self._send_json(_start_audit_download(name))
            else:
                self.send_error(404)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write("[web] " + (fmt % args) + "\n")


def serve(*, host: str = "127.0.0.1", port: int = 8744) -> None:
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    srv = ThreadingHTTPServer((host, port), Handler)
    print(f"web UI: http://{host}:{port}/  (Ctrl-C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(prog="./app.py")
    p.add_argument("--port", type=int, default=8744)
    p.add_argument("--host", default="127.0.0.1")
    opts = p.parse_args()
    serve(host=opts.host, port=opts.port)
