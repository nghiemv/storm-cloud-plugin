"""Unit tests for app.py's pure web-server logic.

app.py (the unified launch/monitor/audit server) had zero coverage. These pin
the UI-correctness core: the run-status state machine, the step-fraction
progress/ETA math, and the launch.log→progress synthesizer that keeps the
process-storms bar moving. These are exactly the bits behind the earlier
"interrupted" mislabelling, and they're pure functions over dicts/files.

app.py is stdlib-only, so it imports in CI without the Docker image.
"""

from __future__ import annotations

import re

import pytest

import app
import run
from app import discovery, status


# ─── status state machine ────────────────────────────────────────────────────


def test_derive_status_done_on_summary(monkeypatch):
    monkeypatch.setattr(status, "_pid_alive", lambda pid: False)
    assert status._derive_status({"summary": {"n_actions": 5}}, {"pid": 123}) == "done"


def test_derive_status_running_when_launcher_alive(monkeypatch):
    monkeypatch.setattr(status, "_pid_alive", lambda pid: True)
    progress = {"current_step": {"name": "process-storms"}}
    assert status._derive_status(progress, {"pid": 999}) == "running"


def test_derive_status_interrupted_vs_failed(monkeypatch):
    """Launcher dead + made progress → interrupted; dead + no progress → failed."""
    monkeypatch.setattr(status, "_pid_alive", lambda pid: False)
    made = {"current_step": {"name": "convert-to-dss"}}
    assert status._derive_status(made, {"pid": 1}) == "interrupted"
    none_yet = {}
    assert status._derive_status(none_yet, {"pid": 1}) == "failed"
    completed_only = {"completed_steps": [{"name": "download-inputs"}]}
    assert status._derive_status(completed_only, {"pid": 1}) == "interrupted"


def test_derive_status_starting_and_unknown(monkeypatch):
    monkeypatch.setattr(status, "_pid_alive", lambda pid: True)
    assert status._derive_status(None, {"pid": 5}) == "starting"
    assert status._derive_status(None, None) == "unknown"
    assert status._derive_status({"started_at": 1.0}, None) == "unknown"


# ─── progress / ETA math ─────────────────────────────────────────────────────


def test_overall_pct_completed_steps_plus_subfraction(monkeypatch):
    # Step 3 of 5, no fresh sub-progress → (3-1)/5 = 40%
    run_rec = {"current_step": {"i": 3, "n": 5}}
    assert status._overall_pct(run_rec) == 40.0
    # With 50% of the current step done → (2 + 0.5)/5 = 50%
    monkeypatch.setattr(status, "_within_step_frac", lambda r: 0.5)
    assert status._overall_pct(run_rec) == 50.0


def test_overall_pct_none_without_step_count():
    assert status._overall_pct({"current_step": {}}) is None
    assert status._overall_pct({}) is None


def test_within_step_frac_ignores_stale_subprogress(monkeypatch):
    monkeypatch.setattr(status.time, "time", lambda: 1000.0)
    fresh = {
        "current_step": {"name": "process-storms"},
        "action_progress": {"process-storms": {"pct": 73.0, "updated_at": 995.0}},
    }
    assert status._within_step_frac(fresh) == pytest.approx(0.73)
    stale = {
        "current_step": {"name": "process-storms"},
        "action_progress": {"process-storms": {"pct": 73.0, "updated_at": 800.0}},
    }
    assert status._within_step_frac(stale) == 0.0


def test_runtime_eta_extrapolates_from_elapsed(monkeypatch):
    # 25% done in 100s → 300s remaining
    monkeypatch.setattr(status, "_overall_pct", lambda r: 25.0)
    assert status._runtime_eta_s({"elapsed_s": 100.0}) == pytest.approx(300.0)
    monkeypatch.setattr(status, "_overall_pct", lambda r: 0.0)
    assert status._runtime_eta_s({"elapsed_s": 100.0}) is None


def test_maybe_int_unwraps_cc_string_attrs():
    assert status._maybe_int("48") == 48
    assert status._maybe_int(" 72 ") == 72
    assert status._maybe_int(None) is None
    assert status._maybe_int("") is None
    assert status._maybe_int("not-a-number") is None


# ─── launch.log → progress synthesizer ───────────────────────────────────────

_LOG = """\
2026-06-15 18:07:23 [INFO] [cumsum-scan] dispatching 47 years across 1 worker(s)
2026-06-15 18:09:21 [INFO] [cumsum-scan] year=1979 done (completed=184, skipped=0) — 1/47 years in 118.4s
2026-06-15 18:11:33 [INFO] [cumsum-scan] year=1980 done (completed=732, skipped=0) — 2/47 years in 249.6s
"""


def test_scan_launch_log_synthesizes_year_progress(tmp_path):
    (tmp_path / "launch.log").write_text(_LOG)
    entry = status._scan_launch_log(tmp_path)
    assert entry["done"] == 2
    assert entry["total"] == 47
    assert entry["pct"] == pytest.approx(round(100 * 2 / 47, 1))


def test_scan_launch_log_rate_and_eta_with_step_start(monkeypatch, tmp_path):
    (tmp_path / "launch.log").write_text(_LOG)
    monkeypatch.setattr(status.time, "time", lambda: 10_000.0)
    entry = status._scan_launch_log(tmp_path, step_started=10_000.0 - 200.0)
    assert entry["rate"] == pytest.approx(2 / 200.0)
    assert entry["eta_s"] == pytest.approx((47 - 2) / (2 / 200.0))


def test_scan_launch_log_none_without_year_lines(tmp_path):
    (tmp_path / "launch.log").write_text("nothing relevant here\n")
    assert status._scan_launch_log(tmp_path) is None
    assert status._scan_launch_log(tmp_path / "missing-dir") is None


def test_cumsum_year_regex_tolerates_log_prefix():
    line = "2026-06-15 18:09 [INFO] [cumsum-scan] year=2001 done (completed=5, skipped=0) — 23/47 years in 9s"
    m = status._CUMSUM_YEAR_RE.search(line)
    assert m and (m.group(1), m.group(2)) == ("23", "47")


# ─── parsing helpers ─────────────────────────────────────────────────────────


def test_parse_mc_size_units():
    assert discovery._parse_mc_size("434B") == 434
    assert discovery._parse_mc_size("123KiB") == 123 * 1024
    assert discovery._parse_mc_size("1.5MiB") == int(1.5 * 1024**2)
    assert discovery._parse_mc_size("2GiB") == 2 * 1024**3
    assert discovery._parse_mc_size("garbage") == -1


def test_fmt_dur_buckets():
    assert app._fmt_dur(None) == "—"
    assert app._fmt_dur(0) == "—"
    assert app._fmt_dur(45) == "45s"
    assert app._fmt_dur(125) == "2m 5s"
    assert app._fmt_dur(3725) == "1h 2m"


# ─── cross-module invariant ──────────────────────────────────────────────────


def test_safe_subdir_matches_run_py():
    """app._safe_subdir and run._safe_subdir must agree — progress/ETA break
    silently if the run-dir name differs between the launcher and the UI."""
    for name in [
        "duwamish",
        "Indian Creek 72hr",
        "a/b\\c..",
        "  ",
        "x" * 5,
        "café—dash",
    ]:
        assert app._safe_subdir(name) == run._safe_subdir(name)


def test_safe_subdir_examples():
    assert app._safe_subdir("Indian Creek 72hr") == "Indian-Creek-72hr"
    assert app._safe_subdir("") == "run"
    assert re.fullmatch(r"[A-Za-z0-9._-]+", app._safe_subdir("a/b c!@#"))
