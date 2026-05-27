"""Regression tests for the bbox/snapshot-aware worker cap in cumsum_scan.

These guard against the OOM regression we hit on 2026-05-27 with indian-creek-72hr:
the auto-sized 6 workers exceeded the 30 GiB cgroup once snapshot pools grew on
the ~407×802 bbox. ``_cap_workers_by_memory`` must cap that to ~3.
"""

from __future__ import annotations

from collections import defaultdict
from unittest import mock

from plugin.actions import cumsum_scan as cs


def _by_year(n_dates_per_year: int, n_years: int = 47):
    """Build a fake by_year with ``n_dates_per_year`` items in each year."""
    out: dict[int, list] = defaultdict(list)
    for y in range(1979, 1979 + n_years):
        out[y] = list(range(n_dates_per_year))  # placeholder; only len() is used
    return out


def test_aorc_cells_from_bounds_whitehorse_scale():
    # Whitehorse-scale bbox: ~3° × ~3° = ~360×360 ≈ 130k cells
    cells = cs._aorc_cells_from_bounds((-119.5, 41.5, -116.5, 44.5))
    assert 100_000 < cells < 200_000, cells


def test_aorc_cells_from_bounds_indian_creek_scale():
    # Indian-creek-72hr's clipped bbox observed at 407×802 cells = ~3.4°×6.7°.
    cells = cs._aorc_cells_from_bounds((-90.0, 36.0, -83.0, 39.4))
    assert 250_000 < cells < 400_000, cells


def test_estimate_per_worker_floor():
    # Tiny bbox + tiny snapshot count still pays a 512 MiB floor.
    mb = cs._estimate_per_worker_mb(max_snapshots=10, bbox_cells=100, chunk_hours=720)
    assert mb == cs._MIN_PER_WORKER_MB


def test_estimate_per_worker_whitehorse_scale():
    # 110k cells × 735 snaps × float64 (~620 MiB) + 720h × 110k × 12B (~907 MiB)
    # Total raw ~1.5 GiB × 1.2 overhead = ~1.8 GiB.
    mb = cs._estimate_per_worker_mb(
        max_snapshots=735, bbox_cells=110_000, chunk_hours=720
    )
    assert 1500 < mb < 2200, mb


def test_estimate_per_worker_indian_creek_scale():
    # The OOM regression case. Per the audit on 2026-05-27, peak was ~5.7 GiB
    # at 6 workers; the estimator should land in that neighbourhood (within
    # 30%) so the cap calculation picks the right worker count.
    mb = cs._estimate_per_worker_mb(
        max_snapshots=1473, bbox_cells=326_000, chunk_hours=720
    )
    assert 6500 < mb < 9000, mb


def test_cap_workers_unset_cgroup_passes_through():
    with mock.patch.object(cs, "_cgroup_mem_mb", return_value=None):
        capped = cs._cap_workers_by_memory(
            requested=8,
            by_year=_by_year(735),
            bounds=(-119.5, 41.5, -116.5, 44.5),
            chunk_hours=720,
        )
        assert capped == 8


def test_cgroup_mem_mb_falls_back_to_meminfo_when_unbounded(tmp_path, monkeypatch):
    """docker run without --memory leaves cgroup memory.max as 'max'; we must
    still cap by host RAM (the kernel OOM-kills on host exhaustion either way).
    This is exactly the regression that defeated the first version of this
    cap: cgroup said 'max' so we returned None and didn't cap at all.
    """
    # Synth a fake /sys/fs/cgroup/memory.max -> 'max' and /proc/meminfo with 8 GiB.
    cgroup = tmp_path / "memory.max"
    cgroup.write_text("max\n")
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:    8388608 kB\nMemFree:    1234 kB\n")

    real_open = open

    def fake_open(path, *a, **kw):
        if path == "/sys/fs/cgroup/memory.max":
            return real_open(cgroup, *a, **kw)
        if path == "/proc/meminfo":
            return real_open(meminfo, *a, **kw)
        return real_open(path, *a, **kw)

    monkeypatch.setattr("builtins.open", fake_open)
    # 8 GiB MemTotal / 1024 = 8192 MiB
    assert cs._cgroup_mem_mb() == 8192


def test_cgroup_mem_mb_prefers_smaller_of_cgroup_and_host(tmp_path, monkeypatch):
    """When both cgroup and host RAM are bounded, take the tighter constraint."""
    cgroup = tmp_path / "memory.max"
    cgroup.write_text(str(4 * 1024 * 1024 * 1024) + "\n")  # 4 GiB cgroup
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:    16777216 kB\n")  # 16 GiB host
    real_open = open

    def fake_open(path, *a, **kw):
        if path == "/sys/fs/cgroup/memory.max":
            return real_open(cgroup, *a, **kw)
        if path == "/proc/meminfo":
            return real_open(meminfo, *a, **kw)
        return real_open(path, *a, **kw)

    monkeypatch.setattr("builtins.open", fake_open)
    assert cs._cgroup_mem_mb() == 4096  # cgroup is tighter


def test_cap_workers_whitehorse_does_not_cap():
    # 30 GiB cgroup, Whitehorse-scale bbox: ~1.8 GiB/worker, 27 GiB budget,
    # safe_max ~ 14 workers. Request 6 -> keeps 6.
    with mock.patch.object(cs, "_cgroup_mem_mb", return_value=30_720):
        capped = cs._cap_workers_by_memory(
            requested=6,
            by_year=_by_year(735),
            bounds=(-119.5, 41.5, -116.5, 44.5),
            chunk_hours=720,
        )
        assert capped == 6


def test_cap_workers_indian_creek_caps_to_safe():
    # 30 GiB cgroup, indian-creek-scale bbox: ~7.9 GiB/worker, 27 GiB budget,
    # safe_max=3. Request 6 -> capped to 3 (matches the observed safe count).
    with mock.patch.object(cs, "_cgroup_mem_mb", return_value=30_720):
        capped = cs._cap_workers_by_memory(
            requested=6,
            by_year=_by_year(1473),
            bounds=(-90.0, 36.0, -83.0, 39.4),
            chunk_hours=720,
        )
        assert capped == 3, capped


def test_cap_workers_respects_lower_request():
    # User explicitly asked for 2 workers — cap should not raise that to 3.
    with mock.patch.object(cs, "_cgroup_mem_mb", return_value=30_720):
        capped = cs._cap_workers_by_memory(
            requested=2,
            by_year=_by_year(1473),
            bounds=(-90.0, 36.0, -83.0, 39.4),
            chunk_hours=720,
        )
        assert capped == 2
