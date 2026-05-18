#!/usr/bin/env python3
"""Host-side live progress viewer for storm-cloud-plugin runs.

Serves an auto-refreshing HTML page at http://localhost:8080 that shows
the current state of every run. The plugin writes progress snapshots to
``Local/progress.json`` inside its container; docker compose / batch_run
bind-mounts ``outputs/<run-name>/`` → ``Local/``, so each run's snapshot
ends up at ``outputs/<run-name>/progress.json`` on the host.

The viewer:
  - is always available (no container required to start)
  - shows a card per discovered run
  - works before, during, and after each run (post-mortem visible)

Usage:
    python dev/viewer.py              # serves on :8080
    VIEWER_PORT=9999 python dev/viewer.py
"""

from __future__ import annotations

import http.server
import json
import os
import socketserver
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
PORT = int(os.environ.get("VIEWER_PORT", "8080"))


_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Storm Cloud Plugin</title>
<style>
 body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; max-width: 920px; margin: 2em auto; padding: 0 1em; color: #222; }
 h1 { margin: 0 0 0.5em 0; }
 .empty { color: #888; font-style: italic; }
 .run { border: 1px solid #d0d7de; border-radius: 6px; padding: 1em 1.2em; margin: 1em 0; }
 .run h2 { margin: 0; display: flex; align-items: baseline; gap: 0.6em; }
 .badge { font-size: 0.75em; font-weight: 600; padding: 0.15em 0.5em; border-radius: 3px; text-transform: uppercase; letter-spacing: 0.5px; }
 .badge.running   { background: #cce5ff; color: #004085; }
 .badge.complete  { background: #d4edda; color: #155724; }
 .badge.idle      { background: #eee; color: #555; }
 .meta { color: #666; font-size: 0.85em; margin: 0.3em 0; }
 .pipeline { display: flex; flex-wrap: wrap; gap: 6px; margin: 0.7em 0; }
 .step { padding: 0.35em 0.7em; border: 1px solid #ccc; border-radius: 4px; font-size: 0.85em; background: #f8f9fa; color: #999; }
 .step.done    { background: #d4edda; color: #155724; border-color: #b7d8c0; }
 .step.current { background: #cce5ff; color: #004085; border-color: #9ec8ee; font-weight: 600; }
 .bar { background: #e9ecef; border-radius: 3px; height: 16px; overflow: hidden; margin: 0.5em 0 0.3em 0; }
 .bar-fill { background: linear-gradient(90deg, #0366d6, #0681e7); height: 100%; transition: width 0.4s ease; }
 .row { display: flex; justify-content: space-between; font-size: 0.9em; }
 .stale { color: #cc6600; }
</style>
</head><body>
<h1>Storm Cloud Plugin</h1>
<div id="root">Loading…</div>
<script>
async function refresh() {
  let s;
  try { s = await (await fetch('/api/status')).json(); }
  catch { document.getElementById('root').innerHTML = '<p class="stale">Viewer offline.</p>'; return; }

  if (!s.runs.length) {
    document.getElementById('root').innerHTML = `<p class="empty">No runs found under <code>outputs/</code>. Start one with <code>python dev/tasks.py</code>.</p>`;
    return;
  }

  document.getElementById('root').innerHTML = s.runs.map(renderRun).join('');
}

function renderRun(r) {
  const cs = r.current_step;
  const completed = new Set((r.completed_steps || []).map(x => x.name));
  const status = r.summary ? 'complete' : (cs ? 'running' : 'idle');
  const ageS = (Date.now() / 1000) - r.file_mtime;
  const staleSuffix = (status === 'running' && ageS > 60) ? ` <span class="stale">(snapshot ${fmtDur(ageS)} old)</span>` : '';

  const pipe = (r.plan || []).map((name, i) => {
    let cls = 'pending';
    if (completed.has(name)) cls = 'done';
    else if (cs && cs.name === name) cls = 'current';
    return `<div class="step ${cls}">${i+1}. ${name}</div>`;
  }).join('');

  let body = `<h2><code>${r.name}</code> <span class="badge ${status}">${status}</span></h2>`;
  body += `<div class="meta">started ${fmtDur(r.elapsed_s)} ago${staleSuffix}</div>`;
  body += `<div class="pipeline">${pipe}</div>`;

  if (cs) {
    const prog = r.action_progress[cs.name];
    body += `<div><strong>Step ${cs.i}/${cs.n}: ${cs.name}</strong></div>`;
    if (prog) {
      body += `
        <div class="row">
          <div>${prog.done.toLocaleString()} / ${prog.total.toLocaleString()} (${prog.pct.toFixed(1)}%)</div>
          <div class="meta">${prog.rate.toFixed(2)}/s — ETA ${fmtDur(prog.eta_s)}</div>
        </div>
        <div class="bar"><div class="bar-fill" style="width:${prog.pct}%"></div></div>`;
    }
  }
  if (r.summary) {
    body += `<p style="color:#155724">All ${r.summary.n_actions} actions completed in ${fmtDur(r.summary.total_s)}.</p>`;
  }
  return `<div class="run">${body}</div>`;
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


def collect_runs() -> dict[str, Any]:
    """Read every ``outputs/*/progress.json`` into a combined snapshot."""
    runs: list[dict[str, Any]] = []
    if OUTPUTS_DIR.exists():
        for d in sorted(OUTPUTS_DIR.iterdir()):
            if not d.is_dir():
                continue
            f = d / "progress.json"
            if not f.exists():
                continue
            try:
                snap = json.loads(f.read_text())
            except (json.JSONDecodeError, OSError):
                continue
            snap["name"] = d.name
            snap["file_mtime"] = f.stat().st_mtime
            runs.append(snap)
    return {"runs": runs}


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path in ("/", "/index.html"):
            self._reply(200, "text/html; charset=utf-8", _HTML.encode("utf-8"))
            return
        if self.path == "/api/status":
            body = json.dumps(collect_runs()).encode("utf-8")
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
            pass

    def log_message(self, *_args, **_kwargs) -> None:
        return  # quiet


class _Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main() -> None:
    with _Server(("0.0.0.0", PORT), _Handler) as server:
        print(f"Viewer: http://localhost:{PORT}", flush=True)
        print(f"Watching: {OUTPUTS_DIR}/*/progress.json", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print()  # newline after ^C


if __name__ == "__main__":
    sys.exit(main() or 0)
