"""Run status, progress, and ETA logic + launch.log parsing.

Pure functions over progress.json / launch.json dicts and run-dir paths —
no app globals, stdlib only — so they unit-test in CI without the Docker
image. Consumed by app/__init__.py's run listing.
"""

from __future__ import annotations

import os
import re
import time
from pathlib import Path


# Process-storms (the dominant step) runs the cumsum scan, which doesn't write
# progress.json action_progress but DOES log per-year completion to launch.log:
#   [cumsum-scan] dispatching 47 years ...
#   [cumsum-scan] year=1979 done (completed=184, skipped=0) — 1/47 years in 29.5s
# Parsing these gives the only real sub-progress for the step that otherwise
# freezes the bar through ~98% of the run. Bracket-anchored to tolerate the
# leading "<ts> [INFO] " logging prefix.
_CUMSUM_YEAR_RE = re.compile(r"\[cumsum-scan\] year=\d+ done .*?(\d+)/(\d+) years")

_CUMSUM_DISPATCH_RE = re.compile(r"\[cumsum-scan\] dispatching (\d+) years")


def _tail_bytes(path: Path, n: int = 65536) -> str:
    """Decode the last ``n`` bytes of a file (bounded read for large logs)."""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - n))
            return f.read().decode("utf-8", errors="replace")
    except OSError:
        return ""


def _tail_log(path: Path, max_lines: int = 12) -> str:
    """Read the last ``max_lines`` of a log file for surfacing failure context."""
    text = _tail_bytes(path)
    if not text:
        return ""
    return "\n".join(text.splitlines()[-max_lines:])


def _maybe_int(x: object) -> int | None:
    """Coerce CC-SDK string-attr values to int. Payload attrs come over as
    strings (CC convention), so storm_duration="48" needs unwrapping."""
    if x is None:
        return None
    try:
        s = str(x).strip()
        return int(s) if s else None
    except (ValueError, TypeError):
        return None


def _pid_alive(pid: int | None) -> bool:
    """POSIX + Windows ``os.kill(pid, 0)`` is a no-op that only checks reachability."""
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but we can't signal it — still counts as alive.
        return True
    except OSError:
        return False
    return True


def _within_step_frac(run: dict) -> float:
    """0..1 fraction of the current step complete, from its action_progress
    sub-loop (keyed by step name). 0 when no fresh sub-progress is reported.
    """
    cur = (run.get("current_step") or {}).get("name")
    a = (run.get("action_progress") or {}).get(cur) if cur else None
    if a and a.get("updated_at") and time.time() - a["updated_at"] <= 120:
        return max(0.0, min(100.0, a.get("pct") or 0)) / 100.0
    return 0.0


def _overall_pct(run: dict) -> float | None:
    """Pipeline completion 0..100: (completed steps + current sub-fraction) / N.

    Step-fraction model. The in-flight sub-loop (action_progress — including
    the process-storms per-year scan injected in _list_runs) keeps the bar
    moving smoothly through the long step, without a per-step weight table.
    """
    cs = run.get("current_step") or {}
    i, n = cs.get("i"), cs.get("n")
    if not n:
        return None
    overall = ((max(1, i or 1) - 1) + _within_step_frac(run)) / n
    return round(min(1.0, max(0.0, overall)) * 100, 1)


def _runtime_eta_s(run: dict) -> float | None:
    """Live ETA: extrapolate elapsed time over the completed fraction."""
    pct = _overall_pct(run)
    elapsed = run.get("elapsed_s") or 0.0
    if not pct or pct <= 0 or elapsed <= 0:
        return None
    frac = pct / 100.0
    return max(0.0, elapsed * (1.0 - frac) / frac)


def _derive_status(progress: dict | None, launch: dict | None) -> str:
    """Status precedence: done > interrupted > failed > running > starting > unknown.

    ``interrupted`` (made real progress, then lost the launcher — e.g. a
    machine reboot or a Stop) is distinguished from ``failed`` (launcher
    died before any step started, usually a docker / env error). The two
    look identical to the user but call for different remediations: a
    Retry on the interrupted run resumes via the plugin's disk-level
    idempotency, while a Retry on the failed run needs the underlying
    docker/env issue fixed first.
    """
    if progress and progress.get("summary"):
        return "done"
    launcher_alive = _pid_alive((launch or {}).get("pid"))
    if launch and not launcher_alive:
        # Launcher died. If progress.json has any step state (in flight or
        # completed), the run made real progress and can be resumed.
        made_progress = bool(
            progress
            and (progress.get("current_step") or progress.get("completed_steps"))
        )
        return "interrupted" if made_progress else "failed"
    if progress and progress.get("current_step"):
        return "running"
    if launch:
        return "starting"
    if progress:
        # progress.json exists but no current_step and no summary — a stale
        # initial snapshot. Treat as unknown rather than guessing.
        return "unknown"
    return "unknown"


def _scan_launch_log(run_dir: Path, step_started: float | None = None) -> dict | None:
    """Synthesize a process-storms action_progress entry from launch.log.

    Returns an entry shaped like progress.json's action_progress values
    ({done,total,pct,rate,eta_s,updated_at}) or None when no year count is
    determinable yet. Used only for a running process-storms step.
    """
    text = _tail_bytes(run_dir / "launch.log")
    if not text:
        return None
    years_total = None
    for m in _CUMSUM_DISPATCH_RE.finditer(text):
        years_total = int(m.group(1))
    last_year = None
    for m in _CUMSUM_YEAR_RE.finditer(text):
        last_year = m
    years_done = 0
    if last_year:
        years_done = int(last_year.group(1))
        years_total = years_total or int(last_year.group(2))
    if not years_total:
        return None
    pct = 100.0 * years_done / years_total
    entry = {
        "done": years_done,
        "total": years_total,
        "pct": round(pct, 1),
        "rate": None,
        "eta_s": None,
        "updated_at": time.time(),
    }
    if step_started and years_done > 0:
        elapsed = max(0.0, time.time() - step_started)
        if elapsed > 0:
            rate = years_done / elapsed  # years/sec
            entry["rate"] = rate
            entry["eta_s"] = (years_total - years_done) / rate if rate > 0 else None
    return entry
