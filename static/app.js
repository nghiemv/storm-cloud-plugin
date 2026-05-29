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
    // No action button for finished runs — re-running a completed catalog is
    // rarely intended and an accidental click recomputes the whole thing. Only
    // in-flight runs (Stop) and incomplete ones (Resume/Retry) get an action;
    // a done run can still be re-launched from the HEC S3 payload list above.
    let actionBtn = "";
    if (r.status === "running" || r.status === "starting") {
      actionBtn = `<button class="secondary" onclick="stopRun(this, ${nameArg})">Stop</button>`;
    } else if ((r.status === "interrupted" || r.status === "failed") && r.payload_uuid) {
      const label = r.status === "interrupted" ? "Resume" : "Retry";
      actionBtn = `<button class="secondary" onclick="rerun(this, ${nameArg})">${label}</button>`;
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
  if (!confirm(`Re-launch ${name}? This starts a new compute run.`)) return;
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
