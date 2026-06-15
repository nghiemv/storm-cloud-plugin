"""Pick a worker count the container can afford.

The vendored stormhub library defaults to ``os.cpu_count() - 2`` workers,
which inside a container reads the *host* CPU count and can exceed the
cgroup memory ceiling — causing OOM-driven ``BrokenProcessPool``. This
module picks a safe count from the cgroup limit, with operator overrides.

Memory budget scales with the dask scheduler in effect. The image sets
``DASK_SCHEDULER=synchronous`` by default (single dask thread per worker
× ``*_NUM_THREADS=1``); ``run.py`` flips it to ``threads`` for HEC runs
to parallelize zarr chunk reads when the AORC cache is available.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

# Per-worker memory budget.
#
# Synchronous scheduler: ~1.5 GB observed on a 72 hr AORC slice, 3 GB
#   absorbs transient spikes + headroom for larger domains.
# Threads scheduler (capped at DASK_NUM_WORKERS=4): each worker may hold
#   up to 4 decompressed AORC chunks in flight (~100 MB each), so reserve
#   4 GB to keep auto-sized count safely under the cgroup ceiling.
PER_WORKER_MB_SYNC = 3072
PER_WORKER_MB_THREADS = 4096

CGROUP_MEM_MAX = "/sys/fs/cgroup/memory.max"


def resolve_num_workers(attrs: dict) -> int:
    """Payload attribute > CC_NUM_WORKERS env > cgroup-derived > 1."""
    source, n = _resolve(attrs)
    log.info("num_workers=%d (%s)", n, source)
    return n


def _resolve(attrs: dict) -> tuple[str, int]:
    if attrs.get("num_workers"):
        return "from payload attribute", max(1, int(attrs["num_workers"]))
    if os.environ.get("CC_NUM_WORKERS"):
        return "from CC_NUM_WORKERS env", max(1, int(os.environ["CC_NUM_WORKERS"]))
    cpu_cap = max(1, (os.cpu_count() or 2) - 2)
    mem_mb = _cgroup_mem_limit_mb()
    if mem_mb is None:
        # No cgroup memory limit — fall back to CPU count. With threads,
        # workers share host memory; with subprocesses on a fat host it's
        # still safer to cap at cpu-2 than at 1.
        return "cgroup unset — capped at cpu-2", cpu_cap
    per_worker = (
        PER_WORKER_MB_THREADS
        if os.environ.get("DASK_SCHEDULER", "synchronous") == "threads"
        else PER_WORKER_MB_SYNC
    )
    return "auto-sized from cgroup", max(1, min(cpu_cap, mem_mb // per_worker))


def _cgroup_mem_limit_mb() -> int | None:
    """Read cgroup v2 ``memory.max`` in MiB, or None if unlimited/absent."""
    try:
        raw = Path(CGROUP_MEM_MAX).read_text(encoding="utf-8").strip()
    except (FileNotFoundError, OSError):
        return None
    if raw == "max":
        return None
    try:
        bytes_ = int(raw)
    except ValueError:
        return None
    # Kernel sentinels for "no limit" are huge.
    if bytes_ <= 0 or bytes_ >= (1 << 62):
        return None
    return bytes_ // (1024 * 1024)
