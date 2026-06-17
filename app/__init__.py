#!/usr/bin/env python3
"""storm-cloud-plugin web app: launch, monitor, and audit runs.

A stdlib-only JSON API + static file server (no host pip installs). Browse S3
payloads, launch/stop/resume runs against the local MinIO stack or HEC S3,
watch weighted progress, and view rich per-catalog audit reports inline.

Dashboard markup lives in static/ (index.html, style.css, app.js); the audit
report template lives in static/report.html. Run via ``./run.py web``.
Localhost-only, no auth.
"""

from __future__ import annotations

import html
import json
import mimetypes
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from app.status import (
    _derive_status,
    _overall_pct,
    _runtime_eta_s,
    _scan_launch_log,
    _tail_log,
)

from app.core import (
    HEC_ENV,
    OUTPUTS,
    ROOT,
    RUN_PY,
    STATIC,
    _EVENTS_FILES,
    _TOP_FILES,
    _load_hec_env,
    _read_json,
    _safe_subdir,
    write_launch_json as write_launch_json,
)
from app.launch import _launch_hec, _launch_local, _rerun, _stop
from app.discovery import (
    _catalog_id_for,
    _has_run_output,
    _has_audit,
)
from app.report import _audit, _render_audit_html


def _mc_available() -> bool:
    return shutil.which("mc") is not None


def _mc_alias_for_endpoint() -> str | None:
    """Find the mc alias that points at the configured CC endpoint."""
    if not _mc_available():
        return None
    endpoint = os.environ.get("CC_AWS_ENDPOINT", "").rstrip("/")
    if not endpoint:
        return None
    try:
        cfg_path = Path.home() / ".mc" / "config.json"
        cfg = json.loads(cfg_path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    for name, entry in (cfg.get("aliases") or {}).items():
        if entry.get("url", "").rstrip("/") == endpoint and entry.get(
            "accessKey"
        ) == os.environ.get("CC_AWS_ACCESS_KEY_ID"):
            return name
    return None


def _bucket() -> str:
    return os.environ["CC_AWS_S3_BUCKET"]


def _mc_cp(src: str, dst: Path, *, recursive: bool = False) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    alias = _mc_alias_for_endpoint()
    if not alias:
        raise RuntimeError(
            "no matching mc alias for CC_AWS_ENDPOINT/CC_AWS_ACCESS_KEY_ID; "
            "configure one with `mc alias set ...` or install boto3."
        )
    cmd = ["mc", "cp", "--quiet"]
    if recursive:
        cmd.append("--recursive")
    cmd.extend([f"{alias}/{_bucket()}/{src}", str(dst)])
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"mc cp failed for {src}: {r.stderr.strip()}")


def _mc_ls_lines(src: str) -> list[str]:
    alias = _mc_alias_for_endpoint()
    if not alias:
        raise RuntimeError("no mc alias available")
    r = subprocess.run(
        ["mc", "ls", f"{alias}/{_bucket()}/{src}"],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"mc ls failed for {src}: {r.stderr.strip()}")
    return [ln for ln in r.stdout.splitlines() if ln.strip()]


# ─── known runs (mined from compute/outputs/) ────────────────────────────────


# ─── download ─────────────────────────────────────────────────────────────────


def _download_run(run_name: str) -> Path:
    """Mirror the catalog's S3 prefix into compute/outputs/<run>/audit/."""
    cid = _catalog_id_for(run_name)
    dst = OUTPUTS / run_name / "audit"
    dst.mkdir(parents=True, exist_ok=True)

    # Resolve events-prefix lazily — duration varies (72hr-events / 48hr-events).
    events_prefix = _resolve_events_prefix(cid)
    print(f"[{cid}] events prefix: {events_prefix}")

    for f in _TOP_FILES:
        print(f"  ↓ {f}")
        _mc_cp(f"{cid}/{f}", dst / f)

    (dst / "events").mkdir(exist_ok=True)
    for f in _EVENTS_FILES:
        print(f"  ↓ events/{f}")
        _mc_cp(f"{cid}/{events_prefix}/{f}", dst / "events" / f)

    # Items + thumbnails (recursive). Skip if already present and the user
    # just wants to regenerate the report — but most users re-run download
    # because S3 changed, so always run.
    print(f"  ↓ {events_prefix}/<N>/ (items + thumbnails, recursive)")
    _mc_cp(f"{cid}/{events_prefix}/", dst / "events", recursive=True)

    (dst / "hydro_domains").mkdir(exist_ok=True)
    for f in (
        f"{cid}-watershed.json",
        f"{cid}-transposition.json",
        f"{cid}-transposition_valid.json",
    ):
        print(f"  ↓ hydro_domains/{f}")
        try:
            _mc_cp(f"{cid}/hydro_domains/{f}", dst / "hydro_domains" / f)
        except RuntimeError as e:
            # transposition_valid is optional; keep going.
            print(f"    (skip: {e})")

    # Cache the data/ DSS listing so the report can audit sizes without
    # downloading the 600 MiB of DSS files themselves.
    print("  ↓ data/ listing (sizes only)")
    listing = _mc_ls_lines(f"{cid}/data/")
    (dst / "data-listing.txt").write_text("\n".join(listing) + "\n")

    # Stash the manifest payload too — older launch.json files lack
    # payload_attrs, so the audit falls back to this for top_n / duration etc.
    try:
        lj_path = OUTPUTS / run_name / "launch.json"
        uuid = json.loads(lj_path.read_text()).get("payload_uuid")
        if uuid:
            cc_root = os.environ.get("CC_ROOT", "manifests")
            print(f"  ↓ {cc_root}/{uuid}/payload (for top_n / duration fallback)")
            _mc_cp(
                f"{cc_root}/{uuid}/payload",
                dst / "payload.json",
            )
    except (OSError, json.JSONDecodeError, RuntimeError) as e:
        print(f"    (manifest fallback skipped: {e})")

    return dst


def _resolve_events_prefix(cid: str) -> str:
    """The events prefix is ``<duration>hr-events``; duration comes from the
    payload but only the catalog dir is known. List the prefix and pick the
    one ending in '-events'.
    """
    lines = _mc_ls_lines(f"{cid}/")
    for line in lines:
        tail = line.split()[-1]
        if tail.endswith("-events/"):
            return tail.rstrip("/")
    raise RuntimeError(f"no <duration>hr-events/ prefix found under {cid}/")


# ─── audit checks ─────────────────────────────────────────────────────────────


_HIST_TTL_S = 10.0  # cache historical scan; the dashboard polls every 2s
_hist_cache: dict = {"at": 0.0, "data": None}


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
            rec["eta_s"] = _runtime_eta_s(rec)
            rec["overall_pct"] = _overall_pct(rec)
        if status in ("failed", "interrupted"):
            rec["error_tail"] = _tail_log(run_dir / "launch.log")
        runs.append(rec)
    return runs


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
    # Annotate each payload with any existing local run for its catalog, so the
    # UI offers "Run" only for catalogs never run — an already-run catalog is
    # managed from Recent runs, not one-click re-launched from here.
    runs_by_name = {r["name"]: r["status"] for r in _list_runs()}
    for p in payloads:
        cid = p.get("catalog_id")
        nm = _safe_subdir(cid) if cid else None
        if nm and nm in runs_by_name:
            p["run_name"] = nm
            p["run_status"] = runs_by_name[nm]
    return {"state": "ok", "payloads": payloads}


# ─── audit integration (download + render reports inline) ───────────────────


def _audit_summary(name: str) -> dict:
    """Compact JSON summary for GET /api/audit/<name>."""
    if not _has_audit(name):
        return {"name": name, "state": "not-downloaded"}
    try:
        a = _audit(name)
    except Exception as e:  # noqa: BLE001
        return {"name": name, "state": "error", "error": repr(e)}
    n_anomalies = (
        len(a.get("outlier_dss") or [])
        + len(a.get("grid_without_dss") or [])
        + len(a.get("dss_without_grid") or [])
        + len(a.get("out_of_box") or [])
        + len(a.get("duration_mismatches") or [])
        + len(a.get("bbox_out_of_domain") or [])
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
            _load_hec_env()
            _download_run(name)
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
        _load_hec_env()
        lines = _mc_ls_lines("")
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
        cid = _catalog_id_for(run["name"])
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


def _step_breakdown(run: dict) -> list[dict]:
    """Per-step rows for the detail view: name, even weight%, state, detail."""
    plan = run.get("plan") or []
    if not plan:
        return []
    pct = round(100 / len(plan), 1)
    completed = {s.get("name"): s for s in run.get("completed_steps", [])}
    cur = (run.get("current_step") or {}).get("name")
    rows = []
    for nm in plan:
        if nm in completed:
            state, detail = "done", f"{completed[nm].get('duration_s', 0):.1f}s"
        elif nm == cur:
            ap = (run.get("action_progress") or {}).get(nm) or {}
            state = "running"
            detail = f"{ap.get('done', 0)}/{ap['total']}" if ap.get("total") else "…"
        else:
            state, detail = "pending", "—"
        rows.append({"name": nm, "weight_pct": pct, "state": state, "detail": detail})
    return rows


def _render_run_detail_html(name: str) -> tuple[str, int]:
    run = _get_run(name)
    if not run:
        return (
            f"<!doctype html><meta charset=utf-8><h1>{html.escape(name)}</h1>"
            "<p>No such run.</p><p><a href='/'>← back</a></p>",
            404,
        )
    rows = _step_breakdown(run)
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
<p class="muted">status: <b>{html.escape(run.get("status") or "?")}</b>
 · {audit_link}</p>
<div class="bar"><div style="width:{pct or 0}%"></div></div>
<p>{(str(pct) + "% complete") if pct is not None else ""}
 {("· ETA " + _fmt_dur(eta)) if eta else ""}</p>
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

    def _serve_static(self, path: str) -> None:
        """Serve a dashboard asset from static/ for /static/<file>.

        Same path-traversal guard as _serve_asset. Sent with ``no-store`` so
        an edited style.css/app.js is never served stale from the browser
        cache during development.
        """
        rel = urllib.parse.unquote(path[len("/static/") :])
        base = STATIC.resolve()
        try:
            target = (base / rel).resolve()
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
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path in ("/", "/index.html"):
            try:
                self._send_html((STATIC / "index.html").read_text(encoding="utf-8"))
            except OSError:
                self._send_html("<h1>static/index.html missing</h1>", 500)
        elif path.startswith("/static/"):
            self._serve_static(path)
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
                        capture_output=True,
                        text=True,
                        cwd=ROOT,
                    )
                    if r.returncode != 0:
                        self._send_json(
                            {
                                "error": f"promote failed: {(r.stderr or r.stdout).strip()}"
                            },
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

    p = argparse.ArgumentParser(prog="./app.py")
    p.add_argument("--port", type=int, default=8744)
    p.add_argument("--host", default="127.0.0.1")
    opts = p.parse_args()
    serve(host=opts.host, port=opts.port)
