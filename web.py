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


# ─── progress + run discovery ────────────────────────────────────────────────


def _read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _derive_status(progress: dict | None, launch: dict | None) -> str:
    if progress is None and launch is not None:
        return "starting"
    if progress is None:
        return "unknown"
    if progress.get("summary"):
        return "done"
    if progress.get("current_step"):
        return "running"
    return "starting"


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
        if progress is None and launch is None:
            continue
        status = _derive_status(progress, launch)
        runs.append(
            {
                "name": run_dir.name,
                "status": status,
                "started_at": (progress or {}).get("started_at")
                or (launch or {}).get("launched_at"),
                "elapsed_s": (progress or {}).get("elapsed_s"),
                "current_step": (progress or {}).get("current_step"),
                "summary": (progress or {}).get("summary"),
                "plan": (progress or {}).get("plan", []),
            }
        )
    return runs


# ─── HEC S3 payload listing ──────────────────────────────────────────────────


def _list_payloads() -> list[dict] | None:
    """Shell out to ``./run.py hec list``. Returns None if env not configured."""
    if not HEC_ENV.is_file():
        return None
    r = subprocess.run(
        [sys.executable, str(RUN_PY), "hec", "list"],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if r.returncode != 0:
        return None
    payloads = []
    for line in r.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t", 1)
        payloads.append({"uuid": parts[0], "mtime": parts[1] if len(parts) > 1 else ""})
    return payloads


# ─── launching ───────────────────────────────────────────────────────────────


def _launch(args: list[str], name: str) -> str:
    """Detach a run in the background. Logs to compute/outputs/<name>/launch.log."""
    run_dir = OUTPUTS / name
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "launch.json").write_text(
        json.dumps({"launched_at": time.time(), "args": args})
    )
    log_file = (run_dir / "launch.log").open("ab", buffering=0)
    subprocess.Popen(
        args,
        cwd=str(ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return name


def _launch_local() -> str:
    return _launch([sys.executable, str(RUN_PY)], "quick-test")


def _launch_hec(uuid: str, name: str | None = None) -> str:
    name = name or uuid
    args = [sys.executable, str(RUN_PY), "hec", uuid]
    if name != uuid:
        args.append(name)
    return _launch(args, name)


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
.b-unknown  { background: #f3f4f6; color: #4b5563; }
button { padding: .35rem .9rem; border: 1px solid #2563eb; background: #2563eb;
         color: #fff; border-radius: 6px; cursor: pointer; font-size: .85rem;
         font-weight: 500; }
button:hover { background: #1d4ed8; }
button:disabled { background: #9ca3af; border-color: #9ca3af; cursor: not-allowed; }
code { background: #f3f4f6; padding: .12rem .35rem; border-radius: 4px;
       font-size: .85rem; font-family: ui-monospace, "SF Mono", Menlo, monospace; }
.muted { color: #888; font-style: italic; }
.uuid { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: .85rem; }
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

<h2>HEC S3 payloads <span id="hec-count" class="meta"></span></h2>
<div id="payloads"><span class="muted">Loading…</span></div>

<h2>Recent runs <span id="runs-count" class="meta"></span></h2>
<div id="runs"><span class="muted">Loading…</span></div>

<script>
async function getJson(url, opts) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error(`${url}: ${r.status}`);
  return r.json();
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

async function renderPayloads() {
  let data;
  try {
    data = await getJson("/api/payloads");
  } catch {
    data = null;
  }
  const root = document.getElementById("payloads");
  const count = document.getElementById("hec-count");
  if (data === null) {
    root.innerHTML = '<span class="muted">No <code>compute/hec/env</code> — fill it in to enable HEC S3 runs.</span>';
    count.textContent = "";
    return;
  }
  if (data.length === 0) {
    root.innerHTML = '<span class="muted">No payloads found in S3.</span>';
    count.textContent = "(0)";
    return;
  }
  count.textContent = `(${data.length})`;
  root.innerHTML = data.map(p => `
    <div class="row">
      <div class="left">
        <span class="uuid">${esc(p.uuid)}</span>
        <div class="meta">${esc(p.mtime)}</div>
      </div>
      <button onclick="launchHec(this, '${esc(p.uuid)}')">Run</button>
    </div>
  `).join("");
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
    if (r.status === "running" && r.current_step) {
      detail = `step ${r.current_step.i}/${r.current_step.n}: ${esc(r.current_step.name)}`;
    } else if (r.status === "done" && r.summary) {
      detail = `${r.summary.n_actions} steps in ${fmtDur(r.summary.total_s)}`;
    } else if (r.status === "starting") {
      detail = "container starting…";
    }
    return `
      <div class="row">
        <div class="left">
          <strong>${esc(r.name)}</strong>${badge(r.status)}
          <div class="meta">${detail || "&nbsp;"} · elapsed ${fmtDur(r.elapsed_s)}</div>
        </div>
      </div>
    `;
  }).join("");
}

async function launchLocal(btn) {
  btn.disabled = true;
  btn.textContent = "Launching…";
  try {
    await getJson("/api/launch/local", {method: "POST"});
    await renderRuns();
  } finally {
    btn.disabled = false;
    btn.textContent = "Run";
  }
}

async function launchHec(btn, uuid) {
  btn.disabled = true;
  btn.textContent = "Launching…";
  try {
    await getJson("/api/launch/hec", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({uuid}),
    });
    await renderRuns();
  } finally {
    btn.disabled = false;
    btn.textContent = "Run";
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
                self._send_json({"name": _launch_hec(uuid, data.get("name"))})
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
