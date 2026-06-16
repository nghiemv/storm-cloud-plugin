#!/usr/bin/env python3
"""Syntax-check the dashboard/report client JavaScript with `node --check`.

The web app ships vanilla browser JS in static/app.js and an inline <script> in
static/report.html (the client-side audit charts/map render). Neither is covered
by the Python tests, and a syntax error white-screens the dashboard or audit
report. This is a zero-dependency guard — just the `node` binary, no npm/eslint
toolchain — run locally and in CI.

The report.html script reads server-injected values via __PLACEHOLDER__ tokens
(e.g. `const events = __EVENTS_JSON__;`); we replace those with `null` so the
extracted script is valid standalone JS for the syntax check.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "static"
_PLACEHOLDER = re.compile(r"__[A-Z0-9_]+__")


def _node_check(label: str, source: str) -> bool:
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(source)
        tmp = f.name
    try:
        r = subprocess.run(["node", "--check", tmp], capture_output=True, text=True)
    finally:
        Path(tmp).unlink(missing_ok=True)
    if r.returncode == 0:
        print(f"  ok  {label}")
        return True
    print(f"FAIL  {label}\n{r.stderr.strip()}", file=sys.stderr)
    return False


def _inline_scripts(html: str) -> list[str]:
    """Return the contents of each <script>…</script> block (no src attr)."""
    return re.findall(r"<script\b[^>]*>(.*?)</script>", html, re.DOTALL | re.IGNORECASE)


def main() -> int:
    ok = True

    app_js = STATIC / "app.js"
    if app_js.is_file():
        ok &= _node_check("static/app.js", app_js.read_text())

    report = STATIC / "report.html"
    if report.is_file():
        blocks = _inline_scripts(report.read_text())
        for i, block in enumerate(blocks):
            if not block.strip():
                continue
            # Neutralize server-side template tokens so the JS parses standalone.
            ok &= _node_check(
                f"static/report.html <script#{i}>", _PLACEHOLDER.sub("null", block)
            )

    if not ok:
        print("\nclient JS syntax check failed", file=sys.stderr)
        return 1
    print("client JS syntax OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
