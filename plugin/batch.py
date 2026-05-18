"""Soft failure-ratio policy for batch-style actions.

Both ``convert-to-dss`` and ``create-grid-file`` produce one output per storm
and tolerate a fraction of failures; this helper centralizes the "warn,
hard-fail if everything failed, hard-fail if ratio exceeded" policy.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def check_failure_ratio(
    failed: list[str], total: int, *, label: str, max_ratio: float
) -> None:
    """Raise ``RuntimeError`` if failures exceed ``max_ratio`` (or all failed)."""
    n_failed = len(failed)
    if n_failed == 0:
        return
    log.warning("%s: %d/%d failed: %s", label, n_failed, total, failed)
    if n_failed == total:
        raise RuntimeError(f"All {total} {label} ops failed: {failed}")
    if n_failed / total > max_ratio:
        raise RuntimeError(
            f"{label} failure rate {n_failed}/{total} "
            f"({n_failed / total:.0%}) exceeds threshold ({max_ratio:.0%}): {failed}"
        )
