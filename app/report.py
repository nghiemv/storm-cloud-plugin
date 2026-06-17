"""Audit report: integrity checks, offline SVG maps, and HTML rendering.

Builds the per-catalog audit dict from downloaded artifacts (_audit) and
renders it to HTML (_build_report / _render_audit_html). Depends on app.core,
app.maps (geometry/SVG), and app.discovery (run lookup + listing parse).
"""

from __future__ import annotations

import html
import json
from pathlib import Path

from app.core import OUTPUTS, ROOT, STATIC, _read_json
from app.discovery import _catalog_id_for, _has_audit, _known_runs, _parse_listing
from app.maps import (
    _feature_geom,
    _geom_to_path_d,
    _polygon_area_km2,
    _polygon_bbox,
    _polygon_centroid,
    _projector,
    _walk_coords,
)


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


def _stat_html(label: str, value: str, *, warn: bool = False, ok: bool = False) -> str:
    cls = "stat" + (" warn" if warn else " ok" if ok else "")
    return (
        f'<div class="{cls}"><div class="label">{html.escape(label)}</div>'
        f'<div class="value">{html.escape(value)}</div></div>'
    )


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
    # Ignore unparseable sizes (_parse_mc_size returns -1): a negative entry
    # sorts to the front and can drag the median (and the outlier threshold
    # derived from it) negative, silently disabling outlier detection.
    _sizes = sorted(d["size_bytes"] for d in dss if d["size_bytes"] > 0)
    median_bytes = _sizes[len(_sizes) // 2] if _sizes else 0
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
