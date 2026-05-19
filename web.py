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

import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

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


def _runtime_eta_s(run: dict, attrs: dict | None) -> float | None:
    """Live ETA for a running run.

    Composition:
      completed steps → actual durations
      current step    → live sub-loop ETA when available; else analytic
                        minus time spent in this step so far
      future steps    → analytic predictions

    Returns ``None`` when there's no analytic baseline AND no live signal.
    """
    breakdown = _predict_action_seconds(_work_units(attrs or {}))
    completed = {s["name"] for s in run.get("completed_steps", [])}
    cs = run.get("current_step")
    if not cs:
        # Either pre-step or post-summary — fall back to predicted_total minus
        # elapsed if we have one, else nothing.
        if not breakdown:
            return None
        total = sum(breakdown.values()) + _S_OVERHEAD_FIXED
        return max(0.0, total - (run.get("elapsed_s") or 0.0))

    current_name = cs.get("name")
    eta = 0.0

    # Current step: prefer live measured ETA from action_progress (freshest,
    # non-stale, non-complete entry).
    current_step_eta = _live_current_step_eta(run.get("action_progress") or {})
    if current_step_eta is not None:
        eta += current_step_eta
    elif current_name in breakdown:
        step_started = cs.get("started_at") or 0
        in_step_elapsed = max(0.0, time.time() - step_started) if step_started else 0.0
        eta += max(0.0, breakdown[current_name] - in_step_elapsed)
    elif not breakdown:
        # No analytic model AND no live data — can't say.
        return None

    # Future steps (in breakdown but not completed and not current).
    for name, pred in breakdown.items():
        if name == current_name or name in completed:
            continue
        eta += pred

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


def _tail_log(path: Path, max_lines: int = 12) -> str:
    """Read the last ``max_lines`` of a log file for surfacing failure context."""
    try:
        data = path.read_bytes()
    except OSError:
        return ""
    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines()[-max_lines:]
    return "\n".join(lines)


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
        }
        if status == "running":
            rec["eta_s"] = _runtime_eta_s(rec, attrs)
            rec["overall_pct"] = _overall_pct(rec)
        if status in ("failed", "interrupted"):
            rec["error_tail"] = _tail_log(run_dir / "launch.log")
        runs.append(rec)
    return runs


def _overall_pct(run: dict) -> float | None:
    """Fraction of the pipeline complete, 0..100.

    Combines coarse step progress with the live sub-loop fraction so the
    bar reflects overall progress rather than within-step progress. We
    always have elapsed + this, even when ETA is too uncertain to display.
    """
    cs = run.get("current_step") or {}
    i, n = cs.get("i"), cs.get("n")
    if not n:
        return None
    completed_frac = max(0, (i or 1) - 1) / n
    # Within-step fraction from the freshest non-stale action_progress entry.
    within = 0.0
    ap = run.get("action_progress") or {}
    now = time.time()
    best_ts = -1.0
    for _label, a in ap.items():
        ts = a.get("updated_at") or 0
        if not ts or now - ts > 120:
            continue
        if ts > best_ts:
            best_ts = ts
            pct = a.get("pct") or 0
            within = max(0.0, min(100.0, pct)) / 100.0
    overall = completed_frac + within / n
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
    return `
      <div class="row">
        <div class="left">
          ${title}
          ${desc ? `<div class="meta">${esc(desc)}</div>` : ""}
          ${facts ? `<div class="meta">${esc(facts)}</div>` : ""}
          <div class="meta uuid">${esc(p.uuid)}</div>
        </div>
        <button onclick="launchHec(this, ${uuidArg}, ${cidArg}, ${attrsArg})">Run</button>
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

    return `
      <div class="row">
        <div class="left">
          <strong>${esc(r.name)}</strong>${badge(r.status)}
          <div class="meta">${detail || "&nbsp;"} · elapsed ${fmtDur(r.elapsed_s)}${metaSuffix}</div>
          ${bar}
          ${errLog}
        </div>
        ${actionBtn}
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

async function launchHec(btn, uuid, catalogId, attrs) {
  btn.disabled = true;
  btn.textContent = "Launching…";
  try {
    const r = await getJson("/api/launch/hec", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({uuid, name: catalogId || undefined, attrs: attrs || {}}),
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


# ─── HTTP layer ──────────────────────────────────────────────────────────────


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, data, code: int = 200) -> None:
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path in ("/", "/index.html"):
            self._send_html(HTML)
        elif path == "/api/runs":
            self._send_json(_list_runs())
        elif path == "/api/payloads":
            self._send_json(_list_payloads())
        elif path == "/api/health":
            self._send_json({"ok": True, "hec_configured": HEC_ENV.is_file()})
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
