"""Discovery primitives: locate runs/catalogs and parse S3 listings.

Leaf helpers over compute/outputs/ and mc-listing text — depend only on
app.core (paths), stdlib. Shared by __init__'s HTTP layer and the audit
report, so they live here to keep both import-cycle-free.
"""

from __future__ import annotations

import json
from pathlib import Path

from app.core import OUTPUTS


def _known_runs() -> list[str]:
    """Subdirs of compute/outputs/ that have a launch.json (= a real run)."""
    if not OUTPUTS.is_dir():
        return []
    out = []
    for sub in sorted(OUTPUTS.iterdir()):
        if (sub / "launch.json").is_file():
            out.append(sub.name)
    return out


def _catalog_id_for(run_name: str) -> str:
    """Read catalog_id from compute/outputs/<run>/launch.json. Falls back to the
    run name when the launch record lacks it (older runs stored only the UUID).
    """
    lj = OUTPUTS / run_name / "launch.json"
    if lj.is_file():
        try:
            data = json.loads(lj.read_text())
            cid = (data.get("payload_attrs") or {}).get("catalog_id")
            if cid:
                return cid
        except (OSError, json.JSONDecodeError):
            pass
    return run_name


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


def _has_audit(name: str) -> bool:
    return (OUTPUTS / name / "audit").is_dir()


def _parse_mc_size(token: str) -> int:
    """Convert mc's human-readable size token (e.g. '1.4MiB', '123KiB', '434B')
    to a byte count. Returns -1 on parse failure.
    """
    # Longest suffix first so 'MiB' wins over 'B' (every "iB" form also
    # endswith('B')); GiB before MiB before KiB before bare B.
    units = (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024), ("B", 1))
    for suffix, mul in units:
        if token.endswith(suffix):
            try:
                return int(float(token[: -len(suffix)]) * mul)
            except ValueError:
                return -1
    return -1


def _parse_listing(audit_dir: Path) -> list[dict]:
    """Parse compute/outputs/<run>/audit/data-listing.txt → list of
    {name, size_bytes, size_token}.
    """
    out: list[dict] = []
    text = (audit_dir / "data-listing.txt").read_text(errors="replace")
    for line in text.splitlines():
        toks = line.split()
        if not toks:
            continue
        # mc ls format: "[DATE TIME] SIZE STORAGE NAME"
        # Locate the size token by walking from the right past the name + STANDARD.
        # Easier: name is the last token; size_token is 2 tokens before that.
        name = toks[-1]
        size_token = toks[-3] if len(toks) >= 3 else ""
        if "." not in name:
            continue
        out.append(
            {
                "name": name,
                "size_token": size_token,
                "size_bytes": _parse_mc_size(size_token),
            }
        )
    return out
