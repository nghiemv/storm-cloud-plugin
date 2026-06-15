"""Unit tests for plugin.workers.resolve_num_workers."""

from __future__ import annotations

import pytest

from plugin import workers


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("CC_NUM_WORKERS", raising=False)


@pytest.fixture
def no_cgroup(monkeypatch):
    monkeypatch.setattr(workers, "_cgroup_mem_limit_mb", lambda: None)


@pytest.fixture
def fake_cpu_count(monkeypatch):
    """Pin os.cpu_count() so tests don't depend on the host."""
    monkeypatch.setattr(workers.os, "cpu_count", lambda: 8)


def test_payload_attribute_wins(monkeypatch, no_cgroup):
    monkeypatch.setenv("CC_NUM_WORKERS", "7")
    assert workers.resolve_num_workers({"num_workers": "3"}) == 3


def test_payload_attribute_floors_at_one(no_cgroup):
    assert workers.resolve_num_workers({"num_workers": "0"}) == 1


def test_env_used_when_no_attribute(monkeypatch, no_cgroup):
    monkeypatch.setenv("CC_NUM_WORKERS", "5")
    assert workers.resolve_num_workers({}) == 5


def test_empty_attribute_falls_through_to_cpu_cap(no_cgroup, fake_cpu_count):
    # cgroup unset + 8 visible CPUs -> cpu_cap = max(1, 8-2) = 6.
    # Empty payload attribute is falsy, so the cgroup/cpu fallback wins.
    assert workers.resolve_num_workers({"num_workers": ""}) == 6


def test_auto_sizes_from_cgroup(monkeypatch, fake_cpu_count):
    monkeypatch.setattr(workers, "_cgroup_mem_limit_mb", lambda: 15000)
    # 15000 // 3072 == 4. Pin cpu_count (fake_cpu_count → 8, cpu_cap = 6) so
    # this exercises the memory cap deterministically; without it a low-core
    # CI runner's cpu_cap (max(1, cores-2)) would mask the result as 2.
    assert workers.resolve_num_workers({}) == 4


def test_auto_floors_at_one_when_budget_below_per_worker(monkeypatch):
    monkeypatch.setattr(workers, "_cgroup_mem_limit_mb", lambda: 2048)
    assert workers.resolve_num_workers({}) == 1


def test_fallback_to_cpu_cap_when_cgroup_unset(no_cgroup, fake_cpu_count):
    # Behavior changed in commit bd4e6f5 (perf: auto-size workers): an
    # unbounded host falls back to ``cpu_count - 2`` rather than a hardcoded
    # 1, on the theory that a fat host with no cgroup has the cores to spare.
    assert workers.resolve_num_workers({}) == 6


def _patch_cgroup_read(monkeypatch, contents):
    class FakePath:
        def __init__(self, *_):
            pass

        def read_text(self, **_):
            return contents

    monkeypatch.setattr(workers, "Path", FakePath)


def test_cgroup_max_means_unlimited(monkeypatch):
    _patch_cgroup_read(monkeypatch, "max\n")
    assert workers._cgroup_mem_limit_mb() is None


def test_cgroup_bytes_converted(monkeypatch):
    _patch_cgroup_read(monkeypatch, f"{3 * 1024 * 1024 * 1024}\n")
    assert workers._cgroup_mem_limit_mb() == 3072


def test_cgroup_huge_sentinel_treated_as_unlimited(monkeypatch):
    _patch_cgroup_read(monkeypatch, str(1 << 63))
    assert workers._cgroup_mem_limit_mb() is None


def test_cgroup_missing_returns_none(monkeypatch):
    class MissingPath:
        def __init__(self, *_):
            pass

        def read_text(self, **_):
            raise FileNotFoundError

    monkeypatch.setattr(workers, "Path", MissingPath)
    assert workers._cgroup_mem_limit_mb() is None


def test_cgroup_malformed_returns_none(monkeypatch):
    _patch_cgroup_read(monkeypatch, "garbage")
    assert workers._cgroup_mem_limit_mb() is None
