"""Progress + ETA reporting for long-running actions.

Emits to two destinations:

  - structured log lines: ``[plan] / [step / [progress] / [summary]``.
    A grep on ``[progress] <action>`` gives one line per emit per action.
  - a JSON snapshot at the path configured via ``configure_state_file()``,
    convention ``compute/outputs/<run-name>/progress.json``. Useful for
    post-mortem inspection or for an external monitor to poll.

Two reporting flavors:

  - ``Progress``: drop-in for loops we own (convert-to-dss, uploads,
    grids). Call ``tick()`` after each unit of work.
  - ``StormhubProgressTracker``: scrapes stormhub's own
    ``"… processed (N remaining)"`` lines via a passive root-logger
    handler, so the otherwise-opaque ``new_collection()`` call also
    surfaces rate + ETA.

The JSON file is rewritten on every state change. Writes are
best-effort: a failed write logs once but never breaks the pipeline.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


# ─── Duration formatting ─────────────────────────────────────────────────────


def format_duration(seconds: float) -> str:
    """Render a duration as ``1h23m`` / ``4m12s`` / ``45s`` / ``unknown``."""
    if seconds != seconds or seconds == float("inf"):  # NaN or +inf
        return "unknown"
    if seconds < 0:
        return "0s"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{sec:02d}s"
    return f"{sec}s"


# ─── State + JSON snapshot ───────────────────────────────────────────────────
#
# Module-level state holder + file writer. Single instance — one pipeline
# per process, so one JSON snapshot per run.


_state_lock = threading.Lock()
_state: dict[str, Any] = {
    "started_at": time.time(),
    "plan": [],
    "current_step": None,
    "action_progress": {},
    "completed_steps": [],
    "summary": None,
}
_state_file: Path | None = None
_write_warned = False


def configure_state_file(path: Path) -> None:
    """Tell progress where to write its JSON snapshot. Idempotent."""
    global _state_file
    _state_file = Path(path)
    _state_file.parent.mkdir(parents=True, exist_ok=True)
    _flush()


def _flush() -> None:
    if _state_file is None:
        return
    with _state_lock:
        snap = dict(_state)
    snap["now"] = time.time()
    snap["elapsed_s"] = snap["now"] - snap["started_at"]
    try:
        # Write to a sibling temp file then rename — atomic from the reader's
        # POV, so no one ever observes a half-written JSON.
        tmp = _state_file.with_suffix(_state_file.suffix + ".tmp")
        tmp.write_text(json.dumps(snap))
        tmp.replace(_state_file)
    except OSError as e:
        global _write_warned
        if not _write_warned:
            log.warning("progress snapshot write failed (%s); won't warn again", e)
            _write_warned = True


def set_plan(plan: list[str]) -> None:
    with _state_lock:
        _state["plan"] = list(plan)
    _flush()


def step_start(i: int, n: int, name: str) -> None:
    with _state_lock:
        _state["current_step"] = {
            "i": i,
            "n": n,
            "name": name,
            "started_at": time.time(),
        }
    _flush()


def step_done(i: int, n: int, name: str, duration_s: float) -> None:
    with _state_lock:
        _state["completed_steps"].append(
            {"i": i, "n": n, "name": name, "duration_s": duration_s}
        )
        if (
            _state["current_step"] is not None
            and _state["current_step"]["name"] == name
        ):
            _state["current_step"] = None
    _flush()


def set_summary(n_actions: int, total_s: float) -> None:
    with _state_lock:
        _state["summary"] = {"n_actions": n_actions, "total_s": total_s}
    _flush()


def _push_progress(
    label: str, *, done: int, total: int, rate: float, eta_s: float
) -> None:
    with _state_lock:
        _state["action_progress"][label] = {
            "done": done,
            "total": total,
            "pct": (done / total * 100) if total > 0 else 100.0,
            "rate": rate,
            "eta_s": eta_s,
            "updated_at": time.time(),
        }
    _flush()


# ─── Per-loop progress (for actions we control) ──────────────────────────────


class Progress:
    """Track progress through a known-total loop and emit log + JSON updates."""

    def __init__(
        self,
        *,
        total: int,
        label: str,
        log_every_n: int | None = None,
        log_every_s: float | None = 30.0,
    ) -> None:
        self.total = total
        self.label = label
        self.log_every_n = log_every_n
        self.log_every_s = log_every_s
        self.done = 0
        self._start = time.monotonic()
        self._last_emit = self._start

    def tick(self, n: int = 1) -> None:
        self.done += n
        now = time.monotonic()
        if self._should_emit(now):
            self._emit(now)
            self._last_emit = now

    def _should_emit(self, now: float) -> bool:
        if self.done >= self.total:
            return True
        if self.log_every_n is not None and self.done % self.log_every_n == 0:
            return True
        if self.log_every_s is not None and (now - self._last_emit) >= self.log_every_s:
            return True
        return False

    def _emit(self, now: float) -> None:
        elapsed = now - self._start
        rate = self.done / elapsed if elapsed > 0 else 0.0
        remaining = max(0, self.total - self.done)
        eta_s = remaining / rate if rate > 0 else float("inf")
        pct = (self.done / self.total * 100) if self.total > 0 else 100.0
        log.info(
            "[progress] %s: %d/%d (%.1f%%) — %.2f/s — ETA %s",
            self.label,
            self.done,
            self.total,
            pct,
            rate,
            format_duration(eta_s),
        )
        _push_progress(
            self.label, done=self.done, total=self.total, rate=rate, eta_s=eta_s
        )


# ─── stormhub log-scraping tracker (for opaque library calls) ────────────────


class StormhubProgressTracker:
    """Surface ETA for stormhub's opaque ``new_collection`` work.

    Installs a passive logging handler on the root logger that scrapes
    ``"… (N remaining)"`` messages stormhub emits per processed window.
    A daemon thread publishes a ``[progress]`` line every ``emit_every_s``
    seconds, and a final ``[progress] … complete in <duration>`` line on
    context exit.

    A *Handler* (not a Filter) is required because filters on a logger only
    see records emitted *on that logger* — propagated records from
    descendant loggers bypass them. Handlers on root see every record
    that reaches root via propagation.
    """

    _REMAINING_RE = re.compile(r"\((\d+)\s+remaining\)")

    def __init__(self, *, label: str, emit_every_s: float = 30.0) -> None:
        self.label = label
        self.emit_every_s = emit_every_s
        self._lock = threading.Lock()
        self._start_remaining: int | None = None
        self._last_remaining: int | None = None
        self._start_time: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._handler: logging.Handler | None = None

    def __enter__(self) -> "StormhubProgressTracker":
        self._handler = _RecordWatcher(self)
        self._handler.setLevel(logging.NOTSET)
        logging.getLogger().addHandler(self._handler)
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if self._handler is not None:
            logging.getLogger().removeHandler(self._handler)
        self._emit_final()

    def _observe(self, msg: str) -> None:
        m = self._REMAINING_RE.search(msg)
        if not m:
            return
        n = int(m.group(1))
        with self._lock:
            if self._start_remaining is None:
                self._start_remaining = n
                self._start_time = time.monotonic()
            self._last_remaining = n

    def _loop(self) -> None:
        while not self._stop.wait(self.emit_every_s):
            self._emit_snapshot()

    def _emit_snapshot(self) -> None:
        with self._lock:
            start_rem = self._start_remaining
            last_rem = self._last_remaining
            start_t = self._start_time
        if start_rem is None or last_rem is None or start_t is None:
            return
        elapsed = time.monotonic() - start_t
        processed = start_rem - last_rem
        if processed <= 0 or elapsed <= 0:
            return
        rate = processed / elapsed
        eta_s = last_rem / rate if rate > 0 else float("inf")
        pct = processed / start_rem * 100 if start_rem > 0 else 100.0
        log.info(
            "[progress] %s: %d/%d (%.1f%%) — %.2f/s — ETA %s",
            self.label,
            processed,
            start_rem,
            pct,
            rate,
            format_duration(eta_s),
        )
        _push_progress(
            self.label, done=processed, total=start_rem, rate=rate, eta_s=eta_s
        )

    def _emit_final(self) -> None:
        with self._lock:
            start_rem = self._start_remaining
            last_rem = self._last_remaining
            start_t = self._start_time
        if start_rem is None or last_rem is None or start_t is None:
            return
        elapsed = time.monotonic() - start_t
        processed = start_rem - last_rem
        log.info(
            "[progress] %s: %d/%d complete in %s",
            self.label,
            processed,
            start_rem,
            format_duration(elapsed),
        )


class _RecordWatcher(logging.Handler):
    """Passive handler: forwards every record's message to the tracker."""

    def __init__(self, tracker: StormhubProgressTracker) -> None:
        super().__init__()
        self._tracker = tracker

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._tracker._observe(record.getMessage())
        except Exception:
            # never propagate logging errors back into application code
            pass
