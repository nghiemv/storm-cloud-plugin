"""Unit tests for app.maps — the pure geometry/SVG helpers extracted from app.py.

These power the audit report's offline maps (area/centroid labels, SVG polygon
paths, CONUS projection). They had no coverage while buried in the 2.5k-LOC
app.py monolith; isolating them into app/maps.py makes them testable here.
"""

from __future__ import annotations

import json

import pytest

from app import maps

# A 1°×1° square straddling the equator (GeoJSON lon/lat, closed ring).
SQUARE = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}


def test_walk_coords_flattens_nested_geojson():
    out: list[tuple[float, float]] = []
    maps._walk_coords(SQUARE["coordinates"], out)
    assert (0.0, 0.0) in out and (1.0, 1.0) in out
    assert all(isinstance(p, tuple) and len(p) == 2 for p in out)


def test_polygon_centroid_of_square():
    cx, cy = maps._polygon_centroid(SQUARE)
    assert cx == pytest.approx(0.4, abs=0.2)  # avg of ring vertices (incl. repeat)
    assert cy == pytest.approx(0.4, abs=0.2)
    assert maps._polygon_centroid(None) is None


def test_polygon_area_km2_one_degree_square():
    # ~111.32 km per degree → ~12390 km² near the equator. Allow 2%.
    area = maps._polygon_area_km2(SQUARE)
    assert area == pytest.approx(111.32 * 111.32, rel=0.02)
    assert maps._polygon_area_km2(None) == 0.0


def test_polygon_area_subtracts_holes():
    with_hole = {
        "type": "Polygon",
        "coordinates": [
            [[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]],  # outer 2°×2°
            [[0.5, 0.5], [1.5, 0.5], [1.5, 1.5], [0.5, 1.5], [0.5, 0.5]],  # 1° hole
        ],
    }
    outer = maps._polygon_area_km2(
        {"type": "Polygon", "coordinates": [with_hole["coordinates"][0]]}
    )
    net = maps._polygon_area_km2(with_hole)
    assert net < outer
    assert net == pytest.approx(outer * 0.75, rel=0.05)  # 4 - 1 of 4 = 3/4


def test_ring_signed_area_sign_follows_orientation():
    ccw = [[0, 0], [1, 0], [1, 1], [0, 1]]
    cw = list(reversed(ccw))
    a_ccw = maps._ring_signed_area_km2(ccw, lat_ref=0.5)
    a_cw = maps._ring_signed_area_km2(cw, lat_ref=0.5)
    assert a_ccw == pytest.approx(-a_cw)
    assert abs(a_ccw) > 0
    assert maps._ring_signed_area_km2([[0, 0], [1, 1]], 0.0) == 0.0  # <3 pts


def test_geom_to_path_d_polygon_and_multipolygon():
    proj = lambda lon, lat: (lon, lat)  # noqa: E731 — identity projector for the test
    d = maps._geom_to_path_d(SQUARE, proj)
    assert d.startswith("M") and d.endswith("Z") and "L" in d
    multi = {"type": "MultiPolygon", "coordinates": [SQUARE["coordinates"]]}
    assert maps._geom_to_path_d(multi, proj).count("Z") == 1
    assert maps._geom_to_path_d(None, proj) == ""
    assert maps._geom_to_path_d({"type": "Point", "coordinates": [0, 0]}, proj) == ""


def test_feature_geom_unwraps_feature_and_passes_raw():
    feat = {"type": "Feature", "geometry": SQUARE, "properties": {}}
    assert maps._feature_geom(feat) == SQUARE
    assert maps._feature_geom(SQUARE) == SQUARE  # raw geometry tolerated
    assert maps._feature_geom(None) is None


def test_projector_orients_north_up_and_fits_width():
    project, (w, h) = maps._projector((0, 0, 1, 1), width=100)
    assert w == 100 and h > 0
    x_min, y_top = project(0, 1)  # north edge → small y
    x_min2, y_bot = project(0, 0)  # south edge → large y
    assert y_top < y_bot  # north is up


def test_polygon_bbox_from_file(tmp_path):
    f = tmp_path / "poly.json"
    f.write_text(json.dumps(SQUARE))
    assert maps._polygon_bbox(f) == (0.0, 0.0, 1.0, 1.0)
    assert maps._polygon_bbox(tmp_path / "missing.json") is None
