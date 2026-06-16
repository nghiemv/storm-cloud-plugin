"""Render test for the audit report path (_audit → _build_report).

The ~1000-line report/audit composer in app/__init__.py was entirely uncovered,
and the web-boot smoke only hits / and /api/runs — not /audit/<name>. This
builds a realistic minimal audit fixture and renders it end-to-end. Crucially it
asserts status == 200 AND that the page isn't the caught-exception error page
(_render_audit_html swallows render crashes into a 500 error page), so a
regression in the report or the geometry helpers it calls fails the test.

This guards the planned extraction of the report code into app/report.py.
"""

from __future__ import annotations

import json

import app
from app import discovery

CID = "testcat"
_EVENTS = [(1, -95.0, 41.5), (2, -94.5, 41.8)]


def _poly_feature(bbox):
    x0, y0, x1, y1 = bbox
    return {
        "type": "Feature",
        "bbox": list(bbox),
        "properties": {},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]],
        },
    }


def _build_audit_fixture(base):
    """A small but representative downloaded-audit directory for CID."""
    run = base / CID
    audit = run / "audit"
    (audit / "events").mkdir(parents=True)
    (audit / "hydro_domains").mkdir(parents=True)

    (run / "launch.json").write_text(
        json.dumps(
            {
                "payload_attrs": {
                    "catalog_id": CID,
                    "top_n_events": "2",
                    "storm_duration": "72",
                }
            }
        )
    )
    (run / "progress.json").write_text(
        json.dumps({"summary": {"n_actions": 5, "total_s": 120.0}})
    )

    feats = [
        {
            "type": "Feature",
            "properties": {
                "id": eid,
                "storm_start_date": f"2001-0{eid}-01T00",
                "season": "spring",
                "aorc:statistics": {"mean": 10.0 + eid, "min": 1.0, "max": 50.0 + eid},
            },
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
        }
        for eid, lon, lat in _EVENTS
    ]
    (audit / "events" / "max_precip_locations.geojson").write_text(
        json.dumps({"type": "FeatureCollection", "features": feats})
    )
    for eid, lon, lat in _EVENTS:
        d = audit / "events" / str(eid)
        d.mkdir()
        (d / f"{eid}.json").write_text(
            json.dumps(
                {
                    "bbox": [lon - 0.5, lat - 0.5, lon + 0.5, lat + 0.5],
                    "properties": {
                        "start_datetime": f"2001-0{eid}-01T00:00:00Z",
                        "end_datetime": f"2001-0{eid}-04T00:00:00Z",
                    },
                }
            )
        )

    (audit / "data-listing.txt").write_text(
        "[2024-01-01 00:00:00] 1.4MiB STANDARD 1.dss\n"
        "[2024-01-01 00:00:00] 1.5MiB STANDARD 2.dss\n"
    )
    (audit / "catalog.grid").write_text(
        "Grid: STORM-1\nGrid Type: PER-CUM PRECIP\n"
        "DSS File Name: 1.dss\nDSS Pathname: /a/b\nEnd:\n"
    )
    (audit / "hydro_domains" / f"{CID}-watershed.json").write_text(
        json.dumps(_poly_feature((-95.5, 41.0, -94.0, 42.0)))
    )
    (audit / "hydro_domains" / f"{CID}-transposition.json").write_text(
        json.dumps(_poly_feature((-97.0, 40.0, -93.0, 43.0)))
    )
    return CID


def _patch_outputs(monkeypatch, tmp):
    """OUTPUTS is imported into several app submodules; patch every binding
    the audit path reads so the fixture dir is used everywhere."""
    for mod in (app, discovery):
        monkeypatch.setattr(mod, "OUTPUTS", tmp)


def test_render_audit_html_full_path(tmp_path, monkeypatch):
    _patch_outputs(monkeypatch, tmp_path)
    name = _build_audit_fixture(tmp_path)
    body, status = app._render_audit_html(name)
    assert status == 200, body[:800]
    assert "Audit render error" not in body, body[:800]
    assert CID in body
    # SVG maps actually rendered (geometry helpers exercised), not empty.
    assert "<svg" in body


def test_render_audit_html_missing_is_friendly(tmp_path, monkeypatch):
    _patch_outputs(monkeypatch, tmp_path)
    body, status = app._render_audit_html("never-downloaded")
    assert status == 200
    assert "No audit artifacts" in body


def test_audit_dict_shape(tmp_path, monkeypatch):
    """_audit builds the dict _build_report consumes — pin its core keys."""
    _patch_outputs(monkeypatch, tmp_path)
    name = _build_audit_fixture(tmp_path)
    a = app._audit(name)
    assert a["catalog_id"] == CID
    assert a["n_events"] == 2
    assert a["n_dss"] == 2
    assert a["top_n"] == 2
    assert a["storm_duration"] == 72
    assert a["transposition_bbox"] is not None
