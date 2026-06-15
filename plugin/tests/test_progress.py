"""Unit tests for plugin.progress."""

from __future__ import annotations

import logging
import re

from plugin.progress import Progress, StormhubProgressTracker, format_duration


def test_format_duration_seconds_only():
    assert format_duration(0) == "0s"
    assert format_duration(45.7) == "45s"


def test_format_duration_minutes():
    assert format_duration(75) == "1m15s"


def test_format_duration_hours():
    assert format_duration(3725) == "1h02m"


def test_format_duration_unknown():
    assert format_duration(float("inf")) == "unknown"
    assert format_duration(float("nan")) == "unknown"


def test_progress_emits_on_final_tick(caplog):
    p = Progress(total=3, label="test", log_every_n=None, log_every_s=None)
    with caplog.at_level(logging.INFO, logger="plugin.progress"):
        p.tick()
        p.tick()
        p.tick()
    msgs = [r.getMessage() for r in caplog.records]
    progress_msgs = [m for m in msgs if "[progress] test" in m]
    assert len(progress_msgs) == 1
    assert "3/3" in progress_msgs[0]
    assert "100.0%" in progress_msgs[0]


def test_progress_emits_on_every_n(caplog):
    p = Progress(total=10, label="test", log_every_n=2, log_every_s=None)
    with caplog.at_level(logging.INFO, logger="plugin.progress"):
        for _ in range(10):
            p.tick()
    progress_msgs = [
        r.getMessage() for r in caplog.records if "[progress] test" in r.getMessage()
    ]
    # ticks at 2,4,6,8,10 → 5 emits
    assert len(progress_msgs) == 5


def test_progress_includes_rate_and_eta(caplog):
    p = Progress(total=5, label="test", log_every_n=None, log_every_s=None)
    with caplog.at_level(logging.INFO, logger="plugin.progress"):
        for _ in range(5):
            p.tick()
    msg = next(
        r.getMessage() for r in caplog.records if "[progress] test" in r.getMessage()
    )
    assert re.search(r"\d+\.\d+/s", msg), msg
    assert "ETA" in msg


def test_stormhub_tracker_picks_up_remaining(caplog):
    """The tracker scrapes '(N remaining)' from stormhub-style log lines."""
    sub_log = logging.getLogger("stormhub.fake")
    with caplog.at_level(logging.INFO):
        with StormhubProgressTracker(label="proc", emit_every_s=3600) as t:
            sub_log.info("1980-01-01T00 processed (100 remaining)")
            sub_log.info("1980-01-01T06 processed (98 remaining)")
            # final emit happens on __exit__
        assert t._start_remaining == 100
        assert t._last_remaining == 98
    final = [
        r.getMessage() for r in caplog.records if "[progress] proc" in r.getMessage()
    ]
    assert any("2/100 complete in" in m for m in final), final


def test_stormhub_tracker_no_lines_no_emit(caplog):
    """If stormhub never logs '(N remaining)', tracker stays silent."""
    with caplog.at_level(logging.INFO):
        with StormhubProgressTracker(label="proc", emit_every_s=3600):
            pass
    progress_msgs = [
        r.getMessage() for r in caplog.records if "[progress] proc" in r.getMessage()
    ]
    assert progress_msgs == []
