"""Unit tests for plugin.web — state transitions + HTTP smoke."""

from __future__ import annotations

import json
import urllib.request

import pytest

from plugin import web


@pytest.fixture(autouse=True)
def _fresh_state():
    """Each test starts with a fresh _State so they don't leak through STATE."""
    old = web.STATE
    web.STATE = web._State()
    try:
        yield
    finally:
        web.STATE = old


def test_initial_snapshot_is_empty():
    snap = web.STATE.snapshot()
    assert snap["plan"] == []
    assert snap["current_step"] is None
    assert snap["action_progress"] == {}
    assert snap["completed_steps"] == []
    assert snap["summary"] is None


def test_step_lifecycle_clears_current_on_done():
    web.STATE.set_plan(["a", "b"])
    web.STATE.step_start(1, 2, "a")
    snap = web.STATE.snapshot()
    assert snap["current_step"]["name"] == "a"

    web.STATE.step_done(1, 2, "a", 3.5)
    snap = web.STATE.snapshot()
    assert snap["current_step"] is None
    assert snap["completed_steps"] == [
        {"i": 1, "n": 2, "name": "a", "duration_s": 3.5}
    ]


def test_progress_computes_percent():
    web.STATE.set_progress("foo", done=25, total=100, rate=2.5, eta_s=30.0)
    snap = web.STATE.snapshot()
    assert snap["action_progress"]["foo"]["pct"] == 25.0
    assert snap["action_progress"]["foo"]["rate"] == 2.5


def test_progress_overwrites_per_label():
    web.STATE.set_progress("foo", done=10, total=100, rate=1.0, eta_s=90.0)
    web.STATE.set_progress("foo", done=50, total=100, rate=2.0, eta_s=25.0)
    snap = web.STATE.snapshot()
    assert snap["action_progress"]["foo"]["done"] == 50


def test_summary_set():
    web.STATE.set_summary(5, 423.7)
    snap = web.STATE.snapshot()
    assert snap["summary"] == {"n_actions": 5, "total_s": 423.7}


# --- HTTP server smoke -----------------------------------------------------


@pytest.fixture
def running_server(monkeypatch):
    """Start the server on an OS-picked port so concurrent tests don't collide."""
    monkeypatch.setenv("CC_PROGRESS_PORT", "0")  # picked below
    # start_if_enabled() reads CC_PROGRESS_PORT; for the test we bypass it and
    # bind explicitly to port 0 to let the OS pick a free one.
    server = web._Server(("127.0.0.1", 0), web._Handler)
    port = server.server_address[1]
    import threading

    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        yield port
    finally:
        server.shutdown()
        server.server_close()


def test_html_page_served(running_server):
    port = running_server
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/") as r:
        assert r.status == 200
        body = r.read().decode("utf-8")
    assert "<title>Storm Cloud Plugin</title>" in body
    assert "/api/status" in body  # the page fetches it


def test_api_status_returns_snapshot(running_server):
    port = running_server
    web.STATE.set_plan(["x", "y"])
    web.STATE.step_start(1, 2, "x")
    web.STATE.set_progress("x", done=3, total=10, rate=0.5, eta_s=14.0)

    with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status") as r:
        assert r.status == 200
        data = json.loads(r.read())

    assert data["plan"] == ["x", "y"]
    assert data["current_step"]["name"] == "x"
    assert data["action_progress"]["x"]["done"] == 3


def test_404_for_unknown_path(running_server):
    port = running_server
    with pytest.raises(urllib.error.HTTPError) as exc:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/nope")
    assert exc.value.code == 404


def test_start_if_enabled_returns_none_when_disabled(monkeypatch):
    monkeypatch.setenv("CC_PROGRESS_PORT", "0")
    assert web.start_if_enabled() is None


def test_start_if_enabled_returns_none_for_invalid_port(monkeypatch):
    monkeypatch.setenv("CC_PROGRESS_PORT", "not-a-number")
    assert web.start_if_enabled() is None
