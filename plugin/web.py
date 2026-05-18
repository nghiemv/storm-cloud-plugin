"""Tiny in-process HTTP server that surfaces live progress in a browser.

The plugin already emits ``[plan] / [step / [progress] / [summary]`` log
lines (see ``plugin.progress``); this module gives those same signals a
browser-visible face at ``http://localhost:<port>``. State is held in a
single ``_State`` instance updated by ``plugin.progress.Progress`` and
``StormhubProgressTracker`` on every tick.

Design notes
------------

- Port: ``CC_PROGRESS_PORT`` env var, default 8080. Set to 0 to disable.
- Server binds inside the container; whether it's reachable from the host
  depends on the docker port mapping (``docker run -p 8080:8080`` or the
  compose ``ports:`` block), so production deploys that don't publish the
  port have nothing exposed.
- Bind failures are non-fatal: a stale port just means no browser view.
- The thread is a daemon so the plugin can exit normally when work is done.
"""

from __future__ import annotations

import http.server
import json
import logging
import os
import socketserver
import threading
import time
from typing import Any, Optional

log = logging.getLogger(__name__)

_PORT_ENV = "CC_PROGRESS_PORT"
_DEFAULT_PORT = 8080


class _State:
    """Thread-safe holder for the live pipeline state.

    Single instance lives at module-level (``STATE``); everything writes
    through the typed methods so we can keep the locking discipline in
    one place.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.started_at = time.time()
        self.plan: list[str] = []
        self.current_step: Optional[dict[str, Any]] = None
        self.action_progress: dict[str, dict[str, Any]] = {}
        self.completed_steps: list[dict[str, Any]] = []
        self.summary: Optional[dict[str, Any]] = None

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "started_at": self.started_at,
                "now": time.time(),
                "elapsed_s": time.time() - self.started_at,
                "plan": list(self.plan),
                "current_step": self.current_step,
                "action_progress": dict(self.action_progress),
                "completed_steps": list(self.completed_steps),
                "summary": self.summary,
            }

    def set_plan(self, plan: list[str]) -> None:
        with self._lock:
            self.plan = list(plan)

    def step_start(self, i: int, n: int, name: str) -> None:
        with self._lock:
            self.current_step = {
                "i": i,
                "n": n,
                "name": name,
                "started_at": time.time(),
            }

    def step_done(self, i: int, n: int, name: str, duration_s: float) -> None:
        with self._lock:
            self.completed_steps.append(
                {"i": i, "n": n, "name": name, "duration_s": duration_s}
            )
            if self.current_step is not None and self.current_step.get("name") == name:
                self.current_step = None

    def set_progress(
        self,
        label: str,
        *,
        done: int,
        total: int,
        rate: float,
        eta_s: float,
    ) -> None:
        with self._lock:
            self.action_progress[label] = {
                "done": done,
                "total": total,
                "pct": (done / total * 100) if total > 0 else 100.0,
                "rate": rate,
                "eta_s": eta_s,
                "updated_at": time.time(),
            }

    def set_summary(self, n_actions: int, total_s: float) -> None:
        with self._lock:
            self.summary = {"n_actions": n_actions, "total_s": total_s}


STATE = _State()


# Inline HTML: keep it self-contained so no static-file serving is needed.
# Refresh every 2s via fetch; render minimal pipeline + progress + ETA.
_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Storm Cloud Plugin</title>
<style>
 body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; max-width: 880px; margin: 2em auto; padding: 0 1em; color: #222; }
 h1 { margin: 0 0 0.2em 0; }
 .meta { color: #666; font-size: 0.9em; }
 .pipeline { display: flex; flex-wrap: wrap; gap: 6px; margin: 1em 0; }
 .step { padding: 0.4em 0.7em; border: 1px solid #ccc; border-radius: 4px; font-size: 0.9em; background: #f8f9fa; color: #999; }
 .step.done    { background: #d4edda; color: #155724; border-color: #b7d8c0; }
 .step.current { background: #cce5ff; color: #004085; border-color: #9ec8ee; font-weight: 600; }
 .panel { background: #f6f8fa; border: 1px solid #e1e4e8; border-radius: 6px; padding: 1em; margin: 1em 0; }
 .bar { background: #e9ecef; border-radius: 3px; height: 18px; overflow: hidden; margin: 0.6em 0 0.3em 0; }
 .bar-fill { background: linear-gradient(90deg, #0366d6, #0681e7); height: 100%; transition: width 0.4s ease; }
 .row { display: flex; justify-content: space-between; font-size: 0.95em; }
 .stale { color: #cc6600; }
 .done { color: #155724; font-weight: 600; }
 code { background: #eef; padding: 0 0.3em; border-radius: 3px; }
</style>
</head><body>
<h1>Storm Cloud Plugin</h1>
<div class="meta"><span id="elapsed">…</span> elapsed</div>
<div id="root">Loading…</div>
<script>
async function refresh() {
  let s;
  try { s = await (await fetch('/api/status')).json(); }
  catch { document.getElementById('root').innerHTML = '<p class="stale">No connection — plugin may have exited.</p>'; return; }

  document.getElementById('elapsed').textContent = fmtDur(s.elapsed_s);

  const completedNames = new Set(s.completed_steps.map(x => x.name));
  const pipe = (s.plan || []).map((name, i) => {
    let cls = 'pending';
    if (completedNames.has(name)) cls = 'done';
    else if (s.current_step && s.current_step.name === name) cls = 'current';
    return `<div class="step ${cls}">${i+1}. ${name}</div>`;
  }).join('');

  let body = `<div class="pipeline">${pipe}</div>`;

  if (s.current_step) {
    const cur = s.current_step;
    const prog = s.action_progress[cur.name];
    body += `<div class="panel"><div class="row"><div><strong>Step ${cur.i}/${cur.n}: ${cur.name}</strong></div><div class="meta">${fmtDur(s.now - cur.started_at)} on step</div></div>`;
    if (prog) {
      const ago = s.now - prog.updated_at;
      const staleTag = ago > 60 ? ` <span class="stale">(stale ${fmtDur(ago)})</span>` : '';
      body += `
        <div class="row">
          <div>${prog.done.toLocaleString()} / ${prog.total.toLocaleString()} (${prog.pct.toFixed(1)}%)</div>
          <div class="meta">${prog.rate.toFixed(2)}/s — ETA ${fmtDur(prog.eta_s)}${staleTag}</div>
        </div>
        <div class="bar"><div class="bar-fill" style="width:${prog.pct}%"></div></div>`;
    } else {
      body += `<div class="meta">No per-item progress reported yet…</div>`;
    }
    body += `</div>`;
  }

  if (s.completed_steps.length) {
    body += `<div class="panel"><strong>Completed steps</strong><ul>` +
      s.completed_steps.map(c => `<li>${c.i}/${c.n} <code>${c.name}</code> — ${fmtDur(c.duration_s)}</li>`).join('') +
      `</ul></div>`;
  }

  if (s.summary) {
    body += `<p class="done">All ${s.summary.n_actions} actions completed in ${fmtDur(s.summary.total_s)}.</p>`;
  }

  document.getElementById('root').innerHTML = body;
}

function fmtDur(s) {
  if (s === null || s === undefined || !isFinite(s)) return '?';
  s = Math.max(0, Math.floor(s));
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60), sec = s%60;
  if (h) return `${h}h${String(m).padStart(2,'0')}m`;
  if (m) return `${m}m${String(sec).padStart(2,'0')}s`;
  return `${sec}s`;
}

refresh();
setInterval(refresh, 2000);
</script>
</body></html>
"""


class _Handler(http.server.BaseHTTPRequestHandler):
    """Two routes: HTML page at /, JSON status at /api/status."""

    def do_GET(self) -> None:  # noqa: N802 (stdlib name)
        if self.path in ("/", "/index.html"):
            self._reply(200, "text/html; charset=utf-8", _HTML.encode("utf-8"))
            return
        if self.path == "/api/status":
            body = json.dumps(STATE.snapshot()).encode("utf-8")
            self._reply(200, "application/json", body)
            return
        self._reply(404, "text/plain", b"not found")

    def _reply(self, code: int, ctype: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Browser closed the tab mid-response; harmless.
            pass

    def log_message(self, *_args, **_kwargs) -> None:
        # Don't let access logs drown the real pipeline logs.
        return


class _Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Threaded so a stuck client can't block /api/status."""

    daemon_threads = True
    allow_reuse_address = True


def start_if_enabled() -> Optional[_Server]:
    """Start the progress server on the configured port; no-op on failure.

    Returns the server (for tests) or None if disabled / port bind failed.
    """
    port_str = os.environ.get(_PORT_ENV, str(_DEFAULT_PORT))
    try:
        port = int(port_str)
    except ValueError:
        log.warning("Invalid %s=%r, disabling progress server", _PORT_ENV, port_str)
        return None
    if port <= 0:
        return None

    try:
        server = _Server(("0.0.0.0", port), _Handler)
    except OSError as e:
        log.warning(
            "Could not bind progress server to port %d: %s (continuing without it)",
            port,
            e,
        )
        return None

    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("[web] progress viewer: http://localhost:%d", port)
    return server
