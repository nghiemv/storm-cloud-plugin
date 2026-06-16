"""Pure geometry + SVG-path helpers for the audit report maps.

No app state, no geo libraries — just GeoJSON math and SVG 'd' strings —
so these are importable and unit-tested in CI without the Docker image.
Consumed by the report/map composers in app/__init__.py.
"""

from __future__ import annotations

import json
from pathlib import Path


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


def _projector(
    bbox: tuple[float, float, float, float], width: int, pad_pct: float = 0.06
):
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
    return (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))
