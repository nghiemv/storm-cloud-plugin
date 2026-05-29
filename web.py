#!/usr/bin/env python3
"""Lightweight web UI for storm-cloud-plugin runs.

Started via: ``./run.py web [--port 8744]``
Visit:       http://localhost:8744/

Single-file, stdlib-only. Lists payloads in HEC S3, lists local runs in
``compute/outputs/``, peeks at each run's ``progress.json``, and kicks off
new runs in the background via ``subprocess.Popen``.

Localhost-only by default — no auth.
"""

from __future__ import annotations

import html
import json
import mimetypes
import os
import re
import statistics
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# Audit logic (download, quality checks, rich report rendering) lives in
# audit.py. We import it so the unified web app can serve audit reports inline
# at /audit/<name> instead of running a second server on :8745. audit.py has no
# module-level side effects and does not import web (its `serve` subcommand
# imports lazily), so this is not circular.
import audit

ROOT = Path(__file__).resolve().parent
OUTPUTS = ROOT / "compute" / "outputs"
HEC_ENV = ROOT / "compute" / "hec" / "env"
RUN_PY = ROOT / "run.py"


# ─── name sanitization ───────────────────────────────────────────────────────
#
# Keep in lockstep with run.py:_safe_subdir. If the two diverge, web.py will
# write launch.json to one directory while the plugin writes progress.json
# to another — silently breaking progress visibility.


def _safe_subdir(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
    return cleaned or "run"


# ─── progress + run discovery ────────────────────────────────────────────────


def _read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


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


# ─── Analytical ETA model ────────────────────────────────────────────────────
#
# Pre-launch ETA is derived from payload-attribute analysis — NO history,
# NO past-run measurements. The dominant cost in the pipeline is the
# process-storms scan loop, which iterates once per date emitted by
# ``generate_date_range(start, end, every_n_hours=check_every_n_hours)``
# in stormhub/met/storm_catalog.py:1605. The other actions scale linearly
# with ``top_n_events``.
#
# The per-unit-of-work constants below are coarse rules of thumb intended
# to place the estimate in the right order of magnitude (minutes vs hours),
# not to be precise. Once the run is in flight, the in-progress sub-loop's
# measured rate (action_progress) refines the ETA — see _runtime_eta_s.

_S_PER_SCAN_DATE = 0.5  # process-storms: one zarr window scan
_S_PER_EVENT_ITEM = 5.0  # process-storms: per top-N storm item creation
_S_PER_EVENT_DSS = 6.0  # convert-to-dss: per storm
_S_PER_EVENT_GRID = 2.5  # create-grid-file: per storm
_S_DOWNLOAD_FIXED = 10.0  # download-inputs: tiny geojson + config
_S_UPLOAD_FIXED = 30.0  # upload-outputs: STAC + DSS sync to S3
_S_OVERHEAD_FIXED = 15.0  # container start, plugin init, payload validate


def _work_units(attrs: dict) -> dict | None:
    """Translate a payload's attrs into work-unit counts.

    Returns None when required attrs are missing or unparseable — callers
    should fall back to "no estimate".
    """
    if not attrs:
        return None
    start = (attrs.get("start_date") or "").strip()
    end = (attrs.get("end_date") or start).strip()
    storm_dur = _maybe_int(attrs.get("storm_duration"))
    top_n = _maybe_int(attrs.get("top_n_events"))
    # CC payloads default check_every_n_hours to 24 in
    # plugin/actions/process_storms.py:_storm_params.
    check_every = _maybe_int(attrs.get("check_every_n_hours")) or 24
    if not start or not storm_dur or not top_n:
        return None
    try:
        from datetime import datetime

        d_start = datetime.fromisoformat(start)
        d_end = datetime.fromisoformat(end) if end else d_start
    except ValueError:
        return None
    span_h = max(0.0, (d_end - d_start).total_seconds() / 3600.0)
    n_dates = int(span_h / check_every) + 1
    return {
        "n_dates": n_dates,
        "n_events": top_n,
        "storm_duration_h": storm_dur,
        "check_every_n_hours": check_every,
        "span_hours": span_h,
    }


def _predict_action_seconds(work: dict | None) -> dict[str, float]:
    """Per-action predicted durations. Empty dict ⇒ no estimate."""
    if not work:
        return {}
    n_dates = work["n_dates"]
    n_events = work["n_events"]
    return {
        "download-inputs": _S_DOWNLOAD_FIXED,
        "process-storms": (n_dates * _S_PER_SCAN_DATE + n_events * _S_PER_EVENT_ITEM),
        "convert-to-dss": n_events * _S_PER_EVENT_DSS,
        "create-grid-file": n_events * _S_PER_EVENT_GRID,
        "upload-outputs": _S_UPLOAD_FIXED,
    }


def _predict_total_s(attrs: dict) -> float | None:
    """Pre-launch total ETA from payload analysis. ``None`` when undecidable."""
    breakdown = _predict_action_seconds(_work_units(attrs))
    if not breakdown:
        return None
    return sum(breakdown.values()) + _S_OVERHEAD_FIXED


# ─── duration-weighted step weights ──────────────────────────────────────────
#
# The pipeline's steps have wildly unequal durations — process-storms commonly
# runs ~98% of total wall time (e.g. 1730s of 1753s on a real run) while
# create-grid-file is milliseconds. Weighting each step as 1/N makes the bar
# misrepresent true progress, so we weight by *seconds*. Weights come, in
# priority order, from (1) historical medians measured on this machine, (2) the
# analytic per-unit prediction, (3) a small floor so no step is weightless.

_STEP_FLOOR_S = 2.0  # minimum weight so an unseen/instant step still shows
_HIST_TTL_S = 10.0  # cache historical scan; the dashboard polls every 2s
_hist_cache: dict = {"at": 0.0, "data": None}


def _historical_step_seconds() -> dict[str, float]:
    """Median completed-step durations learned from finished runs.

    Scans every ``compute/outputs/*/progress.json`` that has a ``summary``
    (i.e. completed) and returns ``{step_name: median_duration_s}`` for steps
    with >=2 samples. Memoized with a short TTL so the 2s poll doesn't rescan.
    """
    now = time.time()
    cached = _hist_cache["data"]
    if cached is not None and now - _hist_cache["at"] < _HIST_TTL_S:
        return cached
    samples: dict[str, list[float]] = {}
    if OUTPUTS.is_dir():
        for run_dir in OUTPUTS.iterdir():
            if not run_dir.is_dir():
                continue
            prog = _read_json(run_dir / "progress.json")
            if not prog or not prog.get("summary"):
                continue
            for s in prog.get("completed_steps") or []:
                nm, dur = s.get("name"), s.get("duration_s")
                if nm and isinstance(dur, (int, float)) and dur >= 0:
                    samples.setdefault(nm, []).append(float(dur))
    medians = {nm: statistics.median(v) for nm, v in samples.items() if len(v) >= 2}
    _hist_cache.update(at=now, data=medians)
    return medians


def weight_for_step(
    name: str, attrs: dict | None, predicted: dict[str, float] | None = None
) -> float:
    """Resolve a step's weight in seconds: historical median > predicted > floor."""
    hist = _historical_step_seconds()
    if name in hist:
        return max(_STEP_FLOOR_S, hist[name])
    if predicted is None:
        predicted = _predict_action_seconds(_work_units(attrs or {}))
    if predicted.get(name, 0) > 0:
        return max(_STEP_FLOOR_S, predicted[name])
    return _STEP_FLOOR_S


def _step_weights(plan: list[str], attrs: dict | None) -> dict[str, float]:
    """{step_name: weight_seconds} for every step in the run's plan."""
    predicted = _predict_action_seconds(_work_units(attrs or {}))
    return {nm: weight_for_step(nm, attrs, predicted) for nm in plan}


def _within_step_frac(run: dict, cs: dict, cur_weight: float) -> float:
    """0..1 fraction of the current step complete.

    ``action_progress`` is keyed by step name, so we look up the current step
    directly (not "freshest across all labels", which could surface a prior
    completed step's 100%). Falls back to a time-based estimate (in-step
    elapsed / expected step seconds, capped 0.95) so long steps without
    sub-progress reporting still advance. Phase 3 supplies a real fraction for
    process-storms via launch.log.
    """
    cur = cs.get("name")
    now = time.time()
    a = (run.get("action_progress") or {}).get(cur) if cur else None
    if a:
        ts = a.get("updated_at") or 0
        if ts and now - ts <= 120:
            return max(0.0, min(100.0, a.get("pct") or 0)) / 100.0
    started = cs.get("started_at")
    if started and cur_weight > 0:
        return min(0.95, max(0.0, now - started) / cur_weight)
    return 0.0


def _runtime_eta_s(run: dict, attrs: dict | None) -> float | None:
    """Live ETA for a running run.

    Composition:
      completed steps → actual durations
      current step    → live sub-loop ETA when available; else analytic
                        minus time spent in this step so far
      future steps    → analytic predictions

    Returns ``None`` when there's no analytic baseline AND no live signal.
    """
    plan = run.get("plan") or []
    completed = {s["name"] for s in run.get("completed_steps", [])}
    cs = run.get("current_step")
    if not plan:
        # No plan recorded — fall back to the pre-launch analytic estimate.
        breakdown = _predict_action_seconds(_work_units(attrs or {}))
        if not breakdown:
            return None
        total = sum(breakdown.values()) + _S_OVERHEAD_FIXED
        return max(0.0, total - (run.get("elapsed_s") or 0.0))

    weights = _step_weights(plan, attrs)
    if not cs:
        # Pre-step or post-summary: remaining = total weight minus elapsed.
        total = sum(weights.values())
        return max(0.0, total - (run.get("elapsed_s") or 0.0))

    current_name = cs.get("name")
    cur_w = weights.get(current_name, 0.0)
    eta = 0.0

    # Current step: prefer a live measured ETA; else weight × remaining fraction.
    live = _live_current_step_eta(run.get("action_progress") or {})
    if live is not None:
        eta += live
    else:
        within = _within_step_frac(run, cs, cur_w)
        eta += max(0.0, cur_w * (1.0 - within))

    # Future steps: every planned step not yet completed and not current.
    for name, w in weights.items():
        if name == current_name or name in completed:
            continue
        eta += w
    return eta


def _live_current_step_eta(action_progress: dict) -> float | None:
    """Pick freshest non-stale, non-complete action_progress eta_s."""
    now = time.time()
    best = None
    for _label, a in action_progress.items():
        ts = a.get("updated_at")
        if not ts or now - ts > 120:
            continue
        if (a.get("pct") or 0) >= 100:
            continue
        if best is None or ts > best.get("updated_at", 0):
            best = a
    if best is None:
        return None
    eta = best.get("eta_s")
    if eta is None or eta == float("inf"):
        return None
    return float(eta)


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


# Process-storms (the dominant step) runs the cumsum scan, which doesn't write
# progress.json action_progress but DOES log per-year completion to launch.log:
#   [cumsum-scan] dispatching 47 years ...
#   [cumsum-scan] year=1979 done (completed=184, skipped=0) — 1/47 years in 29.5s
# Parsing these gives the only real sub-progress for the step that otherwise
# freezes the bar through ~98% of the run. Bracket-anchored to tolerate the
# leading "<ts> [INFO] " logging prefix.
_CUMSUM_YEAR_RE = re.compile(r"\[cumsum-scan\] year=\d+ done .*?(\d+)/(\d+) years")
_CUMSUM_DISPATCH_RE = re.compile(r"\[cumsum-scan\] dispatching (\d+) years")


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
        attrs = (launch or {}).get("payload_attrs") or {}
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
            "predicted_total_s": _predict_total_s(attrs),
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
            rec["eta_s"] = _runtime_eta_s(rec, attrs)
            rec["overall_pct"] = _overall_pct(rec, attrs)
        if status in ("failed", "interrupted"):
            rec["error_tail"] = _tail_log(run_dir / "launch.log")
        runs.append(rec)
    return runs


def _overall_pct(run: dict, attrs: dict | None = None) -> float | None:
    """Fraction of the pipeline complete, 0..100 — weighted by step duration.

    Each step contributes its weight (historical median seconds, else analytic
    prediction, else a floor) rather than 1/N, so the bar tracks true cost.
    A run where process-storms is 98% of the time will show ~98% of the bar
    devoted to that step instead of a misleading 20%.
    """
    cs = run.get("current_step") or {}
    plan = run.get("plan") or []
    if not plan:
        # No plan — fall back to the coarse step counter.
        i, n = cs.get("i"), cs.get("n")
        if not n:
            return None
        return round(min(1.0, max(0.0, (max(1, i or 1) - 1) / n)) * 100, 1)

    weights = _step_weights(plan, attrs)
    total = sum(weights.values()) or 1.0
    completed = {s.get("name") for s in run.get("completed_steps", [])}
    done_weight = sum(w for nm, w in weights.items() if nm in completed)
    cur = cs.get("name")
    cur_w = weights.get(cur, 0.0) if cur else 0.0
    within = _within_step_frac(run, cs, cur_w) if cur else 0.0
    overall = (done_weight + within * cur_w) / total
    return round(min(1.0, max(0.0, overall)) * 100, 1)


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
    # Augment each payload with an analytical ETA derived from its attrs.
    for p in payloads:
        p["predicted_s"] = _predict_total_s(p)
    return {"state": "ok", "payloads": payloads}


# ─── launching ───────────────────────────────────────────────────────────────


# Cross-platform process detachment: POSIX uses setsid, Windows uses
# DETACHED_PROCESS + CREATE_NEW_PROCESS_GROUP so the child survives the
# parent's exit and isn't bound to its console.
if sys.platform == "win32":
    _DETACH_KWARGS: dict = {
        "creationflags": (
            subprocess.DETACHED_PROCESS  # type: ignore[attr-defined]
            | subprocess.CREATE_NEW_PROCESS_GROUP
        ),
    }
else:
    _DETACH_KWARGS = {"start_new_session": True}


def _launch(
    args: list[str],
    name: str,
    *,
    payload_uuid: str | None = None,
    payload_attrs: dict | None = None,
) -> str:
    """Detach a run in the background.

    Writes ``launch.json`` BEFORE Popen so the UI sees the launch even if
    the process dies in its first millisecond (the dead PID still tells us
    it failed). Stdout/stderr stream to ``launch.log``.
    """
    safe = _safe_subdir(name)
    run_dir = OUTPUTS / safe
    run_dir.mkdir(parents=True, exist_ok=True)
    # Clear stale progress.json from prior runs in this dir so the UI doesn't
    # show a stale "done"/"current_step" until the new run writes its own.
    (run_dir / "progress.json").unlink(missing_ok=True)
    log_file = (run_dir / "launch.log").open("ab", buffering=0)
    proc = subprocess.Popen(
        args,
        cwd=str(ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        **_DETACH_KWARGS,
    )
    (run_dir / "launch.json").write_text(
        json.dumps(
            {
                "launched_at": time.time(),
                "args": args,
                "pid": proc.pid,
                "payload_uuid": payload_uuid,
                "payload_attrs": payload_attrs or {},
            }
        )
    )
    return safe


def _launch_local() -> str:
    return _launch([sys.executable, str(RUN_PY)], "quick-test")


def _launch_hec(
    uuid: str,
    name: str | None = None,
    *,
    payload_attrs: dict | None = None,
) -> str:
    target = _safe_subdir(name or uuid)
    args = [sys.executable, str(RUN_PY), "hec", uuid]
    if target != uuid:
        args.append(target)
    return _launch(args, target, payload_uuid=uuid, payload_attrs=payload_attrs)


def _stop(run_name: str) -> tuple[bool, str | None]:
    """Gracefully stop a running compute. Returns ``(stopped, error)``.

    Strategy: ``docker stop`` the container via its cidfile so the plugin's
    SIGTERM handler unwinds the current action and exits cleanly,
    preserving on-disk state for resume. Falls back to looking up the
    container by label if the cidfile is missing (older runs, manual
    deletion).
    """
    safe = _safe_subdir(run_name)
    run_dir = OUTPUTS / safe
    cidfile = run_dir / "container.id"
    container_id: str | None = None
    if cidfile.is_file():
        try:
            container_id = cidfile.read_text().strip() or None
        except OSError:
            container_id = None
    if not container_id:
        # Fallback: locate by label set in run.py:_run_hec_job.
        r = subprocess.run(
            [
                "docker",
                "ps",
                "-q",
                "--filter",
                f"label=storm-cloud-run={safe}",
            ],
            capture_output=True,
            text=True,
        )
        container_id = (r.stdout or "").strip().splitlines()[0:1]
        container_id = container_id[0] if container_id else None
    if not container_id:
        return False, "no running container found for this run"
    # docker stop sends SIGTERM, waits up to --timeout seconds, then SIGKILLs.
    # Give the plugin enough time to finish the current AORC date scan
    # iteration and write its final progress.json before falling back to
    # SIGKILL.
    r = subprocess.run(
        ["docker", "stop", "--timeout", "30", container_id],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        # "No such container" is fine — the container already exited (e.g.,
        # finished or was stopped by another path). Treat as success.
        msg = (r.stderr or r.stdout or "").strip()
        if "No such container" in msg:
            return True, None
        return False, msg or "docker stop failed"
    return True, None


def _rerun(run_name: str) -> tuple[str | None, str | None]:
    """Re-launch the run at ``compute/outputs/<run_name>/``.

    Returns ``(name, error)``. Idempotent plugin actions skip work already
    on disk, so the practical effect is "resume". Refuses if a launcher is
    still alive or if we don't know the payload UUID.
    """
    run_dir = OUTPUTS / _safe_subdir(run_name)
    launch = _read_json(run_dir / "launch.json")
    if launch is None:
        return None, f"no launch.json in {run_dir.name} — can't determine payload"
    if _pid_alive(launch.get("pid")):
        return None, "run is still active — stop it before re-running"
    uuid = launch.get("payload_uuid")
    if not uuid:
        return None, "launch.json has no payload_uuid (legacy run)"
    # Carry forward the stashed payload attrs so post-resume ETA stays valid.
    attrs = launch.get("payload_attrs") or {}
    return _launch_hec(uuid, run_name, payload_attrs=attrs), None


# ─── HTML ────────────────────────────────────────────────────────────────────


HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>storm-cloud-plugin</title>
<style>
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
       max-width: 900px; margin: 2rem auto; padding: 0 1rem; color: #1a1a1a; }
h1 { font-size: 1.4rem; margin-top: 0; }
h2 { font-size: .85rem; text-transform: uppercase; letter-spacing: .06em; color: #555;
     border-bottom: 1px solid #ddd; padding-bottom: .3rem; margin-top: 2rem; }
h2 .meta { color: #888; font-weight: normal; margin-left: .5rem; }
.row { display: flex; justify-content: space-between; align-items: center;
       padding: .55rem 0; border-bottom: 1px solid #eee; gap: .8rem; }
.row .left { flex: 1; min-width: 0; }
.row .meta { color: #666; font-size: .85rem; margin-top: .15rem; }
.badge { display: inline-block; padding: .15rem .55rem; border-radius: 999px;
         font-size: .72rem; font-weight: 600; text-transform: uppercase;
         vertical-align: middle; margin-left: .35rem; }
.b-done     { background: #d1fae5; color: #065f46; }
.b-running  { background: #dbeafe; color: #1e40af; }
.b-starting { background: #fef3c7; color: #92400e; }
.b-failed   { background: #fee2e2; color: #991b1b; }
.b-interrupted { background: #ffedd5; color: #9a3412; }
.b-unknown  { background: #f3f4f6; color: #4b5563; }
button { padding: .35rem .9rem; border: 1px solid #2563eb; background: #2563eb;
         color: #fff; border-radius: 6px; cursor: pointer; font-size: .85rem;
         font-weight: 500; }
button:hover { background: #1d4ed8; }
button:disabled { background: #9ca3af; border-color: #9ca3af; cursor: not-allowed; }
button.secondary { background: #fff; color: #2563eb; }
button.secondary:hover { background: #eff6ff; }
code { background: #f3f4f6; padding: .12rem .35rem; border-radius: 4px;
       font-size: .85rem; font-family: ui-monospace, "SF Mono", Menlo, monospace; }
.muted { color: #888; font-style: italic; }
.uuid { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: .85rem; }
.bar { width: 100%; height: 6px; background: #eef2f7; border-radius: 3px;
       overflow: hidden; margin-top: .35rem; }
.bar > div { height: 100%; background: #2563eb; transition: width .3s ease; }
.bar.done > div { background: #10b981; }
.bar.failed > div { background: #dc2626; }
.bar.interrupted > div { background: #f97316; }
pre.errlog { background: #fef2f2; color: #7f1d1d; border: 1px solid #fecaca;
             padding: .4rem .55rem; border-radius: 4px; font-size: .75rem;
             margin: .35rem 0 0; max-height: 8rem; overflow: auto;
             white-space: pre-wrap; word-break: break-all; }
.toast { position: fixed; bottom: 1rem; right: 1rem; background: #1f2937;
         color: #fff; padding: .55rem .9rem; border-radius: 6px;
         font-size: .85rem; box-shadow: 0 4px 12px rgba(0,0,0,.15);
         opacity: 0; transition: opacity .2s ease; pointer-events: none; }
.toast.show { opacity: 1; }
.toast.err { background: #991b1b; }
.actions { display: flex; gap: .4rem; align-items: center; flex-shrink: 0; }
a.btnlink { padding: .35rem .9rem; border: 1px solid #2563eb; background: #fff;
            color: #2563eb; border-radius: 6px; font-size: .85rem; font-weight: 500;
            text-decoration: none; white-space: nowrap; }
a.btnlink:hover { background: #eff6ff; }
</style>
</head>
<body>
<h1>storm-cloud-plugin</h1>

<h2>Local run</h2>
<div class="row">
  <div class="left">
    <code>compute/local/sample/payload.json</code>
    <div class="meta">MinIO dev stack via <code>./run.py</code></div>
  </div>
  <button onclick="launchLocal(this)">Run</button>
</div>

<h2>HEC S3 payloads <span id="hec-count" class="meta"></span>
  <button class="secondary" style="float:right;font-size:.7rem;padding:.15rem .55rem"
          onclick="renderPayloads()">Refresh</button>
</h2>
<div id="payloads"><span class="muted">Loading…</span></div>

<h2>Recent runs <span id="runs-count" class="meta"></span></h2>
<div id="runs"><span class="muted">Loading…</span></div>

<div id="toast" class="toast"></div>

<script>
async function getJson(url, opts) {
  const r = await fetch(url, opts);
  let body = null;
  try { body = await r.json(); } catch {}
  if (!r.ok) {
    const msg = (body && body.error) || `${url}: ${r.status}`;
    throw new Error(msg);
  }
  return body || {};
}

function fmtDur(s) {
  if (s === null || s === undefined) return "—";
  s = Math.round(s);
  if (s < 60) return s + "s";
  const m = Math.floor(s / 60), sec = s % 60;
  if (m < 60) return m + "m" + String(sec).padStart(2, "0") + "s";
  const h = Math.floor(m / 60), mm = m % 60;
  return h + "h" + String(mm).padStart(2, "0") + "m";
}

function badge(status) {
  return `<span class="badge b-${status}">${status}</span>`;
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"
  }[c]));
}

let _toastTimer = null;
function toast(msg, kind) {
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.className = "toast show" + (kind === "err" ? " err" : "");
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => { el.className = "toast"; }, 4500);
}

async function renderPayloads() {
  const root = document.getElementById("payloads");
  const count = document.getElementById("hec-count");
  let resp;
  try {
    resp = await getJson("/api/payloads");
  } catch (e) {
    root.innerHTML = `<span class="muted">Couldn't reach server: ${esc(e.message)}</span>`;
    count.textContent = "";
    return;
  }
  if (resp.state === "unconfigured") {
    root.innerHTML = '<span class="muted">No <code>compute/hec/env</code> — fill it in to enable HEC S3 runs.</span>';
    count.textContent = "";
    return;
  }
  if (resp.state === "error") {
    root.innerHTML = `<div class="row"><div class="left"><strong style="color:#b91c1c">Listing failed</strong><pre class="errlog">${esc(resp.detail)}</pre></div><button class="secondary" onclick="renderPayloads()">Retry</button></div>`;
    count.textContent = "";
    return;
  }
  const payloads = resp.payloads || [];
  if (payloads.length === 0) {
    root.innerHTML = '<span class="muted">No payloads found in S3.</span>';
    count.textContent = "(0)";
    return;
  }
  count.textContent = `(${payloads.length})`;
  root.innerHTML = payloads.map(p => {
    const cid = p.catalog_id || "";
    const desc = p.catalog_description || "";
    const start = p.start_date || "";
    const end = p.end_date || start;
    const dates = (start && end && start !== end) ? `${start} → ${end}` : start;
    const dur = p.storm_duration ? `${p.storm_duration}h` : "";
    const tn = p.top_n_events ? `top ${p.top_n_events}` : "";
    const est = (p.predicted_s != null) ? `est ~${fmtDur(p.predicted_s)}` : "";
    const facts = [dates, dur, tn, est].filter(Boolean).join(" · ");
    const title = cid
      ? `<strong>${esc(cid)}</strong>`
      : `<em class="muted">(no catalog_id)</em>`;
    // JSON-encoded args interpolated into the onclick attribute must be
    // HTML-escaped — JSON's inner double-quotes would otherwise terminate
    // the attribute value and silently break the click handler.
    const uuidArg = esc(JSON.stringify(p.uuid));
    const cidArg = esc(JSON.stringify(cid));
    const attrsArg = esc(JSON.stringify({
      catalog_id: cid,
      start_date: p.start_date || "",
      end_date: p.end_date || "",
      storm_duration: p.storm_duration || "",
      top_n_events: p.top_n_events || "",
      check_every_n_hours: p.check_every_n_hours || "",
    }));
    // catalog-prefix entries get a small badge and pass their source +
    // catalog_key to launchHec so the backend can promote on click.
    const isUnpromoted = p.source === "catalog-prefix";
    const sourceArg = esc(JSON.stringify(p.source || "manifests"));
    const catKeyArg = esc(JSON.stringify(p.catalog_key || ""));
    const badge = isUnpromoted
      ? ` <span class="meta" title="will be promoted to manifests/ on first Run">[catalog-prefix]</span>`
      : "";
    return `
      <div class="row">
        <div class="left">
          ${title}${badge}
          ${desc ? `<div class="meta">${esc(desc)}</div>` : ""}
          ${facts ? `<div class="meta">${esc(facts)}</div>` : ""}
          <div class="meta uuid">${esc(p.uuid)}</div>
        </div>
        <button onclick="launchHec(this, ${uuidArg}, ${cidArg}, ${attrsArg}, ${sourceArg}, ${catKeyArg})">Run</button>
      </div>
    `;
  }).join("");
}

async function renderRuns() {
  let runs = [];
  try {
    runs = await getJson("/api/runs");
  } catch {
    // ignore — server may be restarting
    return;
  }
  const root = document.getElementById("runs");
  const count = document.getElementById("runs-count");
  count.textContent = runs.length ? `(${runs.length})` : "";
  if (runs.length === 0) {
    root.innerHTML = '<span class="muted">No runs yet.</span>';
    return;
  }
  root.innerHTML = runs.map(r => {
    let detail = "";
    let pct = null;
    let metaBits = [];
    if (r.status === "running" && r.current_step) {
      const cs = r.current_step;
      detail = `step ${cs.i}/${cs.n}: ${esc(cs.name)}`;
      const ap = pickActiveAction(r.action_progress);
      if (ap) {
        detail += ` — ${ap.done}/${ap.total}`;
        if (isFinite(ap.rate) && ap.rate > 0) metaBits.push(`${ap.rate.toFixed(1)}/s`);
      }
      // Server-computed overall pct (combines step counter + sub-loop pct
      // so the bar reflects whole-pipeline progress, not within-step).
      if (r.overall_pct != null) {
        pct = r.overall_pct;
        metaBits.unshift(`${pct.toFixed(0)}% complete`);
      } else if (cs.n > 0) {
        pct = ((cs.i - 1) / cs.n) * 100;
      }
      // ETA last — it's the most uncertain figure; elapsed + pct give the
      // user a grounded sense regardless of whether ETA is meaningful.
      if (r.eta_s != null && isFinite(r.eta_s)) {
        metaBits.push(`ETA ~${fmtDur(r.eta_s)}`);
      }
    } else if (r.status === "done" && r.summary) {
      detail = `${r.summary.n_actions} steps in ${fmtDur(r.summary.total_s)}`;
      pct = 100;
    } else if (r.status === "done") {
      detail = "completed (no progress snapshot)";
      pct = 100;
    } else if (r.status === "starting") {
      detail = r.predicted_total_s != null
        ? `container starting… (est ~${fmtDur(r.predicted_total_s)})`
        : "container starting…";
      pct = 0;
    } else if (r.status === "failed") {
      detail = "launcher exited before any progress — check log";
    } else if (r.status === "interrupted") {
      detail = "stopped mid-run — Retry to resume from disk state";
      // Best-effort: surface the last known overall pct so the user can
      // see how far we got. _overall_pct is only computed for status=running
      // server-side, so reconstruct minimally from current_step here.
      const cs = r.current_step;
      if (cs && cs.n > 0) pct = ((cs.i - 1) / cs.n) * 100;
    }
    const metaSuffix = metaBits.length ? ` · ${metaBits.join(" · ")}` : "";
    const barCls = r.status === "done" ? "bar done"
                 : r.status === "failed" ? "bar failed"
                 : r.status === "interrupted" ? "bar interrupted"
                 : "bar";
    const bar = pct === null ? "" :
      `<div class="${barCls}"><div style="width:${pct.toFixed(1)}%"></div></div>`;
    const errLog = (r.status === "failed" || r.status === "interrupted") && r.error_tail
      ? `<pre class="errlog">${esc(r.error_tail)}</pre>`
      : "";

    // Action buttons: Stop while running, Retry/Resume when stopped or
    // failed (only if we know the payload UUID — legacy CLI runs lack it).
    // HTML-escape the JSON-encoded name so its inner quotes don't break
    // the onclick attribute parse.
    const nameArg = esc(JSON.stringify(r.name));
    let actionBtn = "";
    if (r.status === "running" || r.status === "starting") {
      actionBtn = `<button class="secondary" onclick="stopRun(this, ${nameArg})">Stop</button>`;
    } else if ((r.status === "interrupted" || r.status === "failed") && r.payload_uuid) {
      const label = r.status === "interrupted" ? "Resume" : "Retry";
      actionBtn = `<button class="secondary" onclick="rerun(this, ${nameArg})">${label}</button>`;
    } else if (r.status === "done" && r.payload_uuid) {
      actionBtn = `<button class="secondary" onclick="rerun(this, ${nameArg})">Re-run</button>`;
    }

    // Audit action: view if downloaded, download if outputs likely exist,
    // disabled "Downloading…" while a background pull is in flight.
    let auditBtn = "";
    if (r.has_audit) {
      auditBtn = `<a class="btnlink" href="/audit/${encodeURIComponent(r.name)}">Audit</a>`;
    } else if (r.audit_downloading) {
      auditBtn = `<button class="secondary" disabled>Downloading…</button>`;
    } else if (r.status === "done") {
      auditBtn = `<button class="secondary" onclick="downloadAudit(this, ${nameArg})">Download audit</button>`;
    }
    const detailLink = `<a class="btnlink" href="/run/${encodeURIComponent(r.name)}">Details</a>`;

    return `
      <div class="row">
        <div class="left">
          <strong>${esc(r.name)}</strong>${badge(r.status)}
          <div class="meta">${detail || "&nbsp;"} · elapsed ${fmtDur(r.elapsed_s)}${metaSuffix}</div>
          ${bar}
          ${errLog}
        </div>
        <div class="actions">${detailLink}${auditBtn}${actionBtn}</div>
      </div>
    `;
  }).join("");
}

// Picks the freshest action_progress entry. Filters out stale ones (>2m
// since update) so a long-finished sub-loop doesn't keep showing its ETA.
function pickActiveAction(actions) {
  if (!actions) return null;
  const nowSec = Date.now() / 1000;
  let best = null;
  for (const label in actions) {
    const a = actions[label];
    if (!a.updated_at || nowSec - a.updated_at > 120) continue;
    if (a.pct >= 100) continue;
    if (!best || a.updated_at > best.updated_at) best = a;
  }
  return best;
}

async function launchLocal(btn) {
  btn.disabled = true;
  btn.textContent = "Launching…";
  try {
    const r = await getJson("/api/launch/local", {method: "POST"});
    toast(`Launched ${r.name}`);
    await renderRuns();
  } catch (e) {
    toast(`Launch failed: ${e.message}`, "err");
  } finally {
    btn.disabled = false;
    btn.textContent = "Run";
  }
}

async function downloadAudit(btn, name) {
  btn.disabled = true;
  btn.textContent = "Downloading…";
  try {
    await getJson(`/api/audit/${encodeURIComponent(name)}/download`,
                  {method: "POST"});
    toast(`Audit download started for ${name}`);
    // Re-render shortly so the row reflects the in-flight job, then again
    // once it completes (the row flips to a "View audit" link).
    setTimeout(renderRuns, 1500);
  } catch (e) {
    toast(`Download failed: ${e.message}`, "err");
    btn.disabled = false;
    btn.textContent = "Download audit";
  }
}

async function launchHec(btn, uuid, catalogId, attrs, source, catalogKey) {
  btn.disabled = true;
  btn.textContent = source === "catalog-prefix" ? "Promoting…" : "Launching…";
  try {
    const r = await getJson("/api/launch/hec", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        uuid,
        name: catalogId || undefined,
        attrs: attrs || {},
        source: source || "manifests",
        catalog_key: catalogKey || "",
      }),
    });
    toast(`Launched ${r.name}`);
    await renderRuns();
  } catch (e) {
    toast(`Launch failed: ${e.message}`, "err");
  } finally {
    btn.disabled = false;
    btn.textContent = "Run";
  }
}

async function rerun(btn, name) {
  btn.disabled = true;
  const orig = btn.textContent;
  btn.textContent = "Launching…";
  try {
    const r = await getJson("/api/launch/rerun", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({name}),
    });
    toast(`Re-launched ${r.name}`);
    await renderRuns();
  } catch (e) {
    toast(`Re-run failed: ${e.message}`, "err");
  } finally {
    btn.disabled = false;
    btn.textContent = orig;
  }
}

async function stopRun(btn, name) {
  if (!confirm(`Stop ${name}? On-disk state is preserved; Resume will pick up where it left off.`)) return;
  btn.disabled = true;
  btn.textContent = "Stopping…";
  try {
    await getJson("/api/stop", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({name}),
    });
    toast(`Stopped ${name} — Resume to continue`);
    await renderRuns();
  } catch (e) {
    toast(`Stop failed: ${e.message}`, "err");
  } finally {
    btn.disabled = false;
    btn.textContent = "Stop";
  }
}

renderPayloads();
renderRuns();
setInterval(renderRuns, 2000);
</script>
</body>
</html>
"""


# ─── audit integration (renders audit.py reports inline) ─────────────────────


def _has_audit(name: str) -> bool:
    return (OUTPUTS / name / "audit").is_dir()


def _audit_nav_runs() -> list[dict]:
    """Lightweight {run_name, catalog_id} list for report nav links — avoids a
    full ``_audit()`` per run (the report's nav loop only needs these two)."""
    runs = []
    for r in audit._known_runs():
        if _has_audit(r):
            runs.append({"run_name": r, "catalog_id": audit._catalog_id_for(r)})
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
            f"<p>Run <code>./audit.py download {html.escape(name)}</code> "
            "(or use the Download button on the dashboard) to fetch them, then "
            "reload.</p><p><a href='/'>← back</a></p></body>"
        )
        return body, 200
    try:
        a = audit._audit(name)
        report = audit._build_report(name, a, _audit_nav_runs(), http_mode=True)
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
        a = audit._audit(name)
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
            audit._load_hec_env()
            audit._download_run(name)
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
        audit._load_hec_env()
        lines = audit._mc_ls_lines("")
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
        cid = audit._catalog_id_for(run["name"])
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


def _step_breakdown(run: dict, attrs: dict | None) -> list[dict]:
    """Per-step rows for the detail view: name, weight%, state, duration/eta."""
    plan = run.get("plan") or []
    if not plan:
        return []
    weights = _step_weights(plan, attrs)
    total = sum(weights.values()) or 1.0
    completed = {s.get("name"): s for s in run.get("completed_steps", [])}
    cur = (run.get("current_step") or {}).get("name")
    rows = []
    for nm in plan:
        w = weights.get(nm, 0.0)
        if nm in completed:
            state, detail = "done", f"{completed[nm].get('duration_s', 0):.1f}s"
        elif nm == cur:
            ap = (run.get("action_progress") or {}).get(nm) or {}
            if ap.get("total"):
                state = "running"
                detail = f"{ap.get('done', 0)}/{ap['total']}"
            else:
                state, detail = "running", "…"
        else:
            state, detail = "pending", f"~{w:.0f}s"
        rows.append(
            {"name": nm, "weight_pct": round(100 * w / total, 1), "state": state, "detail": detail}
        )
    return rows


def _render_run_detail_html(name: str) -> tuple[str, int]:
    run = _get_run(name)
    if not run:
        return (
            f"<!doctype html><meta charset=utf-8><h1>{html.escape(name)}</h1>"
            "<p>No such run.</p><p><a href='/'>← back</a></p>",
            404,
        )
    attrs = {}  # detail view reuses run's recorded plan/progress; attrs optional
    lj = _read_json(OUTPUTS / name / "launch.json") or {}
    attrs = lj.get("payload_attrs") or {}
    rows = _step_breakdown(run, attrs)
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
<p class="muted">status: <b>{html.escape(run.get('status') or '?')}</b>
 · {audit_link}</p>
<div class="bar"><div style="width:{pct or 0}%"></div></div>
<p>{(str(pct) + '% complete') if pct is not None else ''}
 {('· ETA ' + _fmt_dur(eta)) if eta else ''}</p>
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

    def do_GET(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path in ("/", "/index.html"):
            self._send_html(HTML)
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
                        capture_output=True, text=True, cwd=ROOT,
                    )
                    if r.returncode != 0:
                        self._send_json(
                            {"error": f"promote failed: {(r.stderr or r.stdout).strip()}"},
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

    p = argparse.ArgumentParser(prog="./web.py")
    p.add_argument("--port", type=int, default=8744)
    p.add_argument("--host", default="127.0.0.1")
    opts = p.parse_args()
    serve(host=opts.host, port=opts.port)
