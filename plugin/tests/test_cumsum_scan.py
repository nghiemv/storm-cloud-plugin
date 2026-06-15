"""Parity tests for the cumsum streaming-snapshot kernel.

The cumsum scan computes each storm window's precip sum once per year as
``snapshots[b] - snapshots[a]`` instead of re-summing every window, and claims
the result is bit-identical (modulo float order) to the per-date sum. This pins
that: snapshot differences must equal a naive windowed sum, including across
chunk boundaries (the subtle part) and with NaNs treated as 0. A regression
here silently changes every catalog's per-storm statistics.
"""

from __future__ import annotations

import numpy as np
import pytest

from plugin.actions.cumsum_scan import _cumsum_snapshots


def _provider_from_cube(cube):
    """chunk_provider over an in-memory (T, Y, X) array."""
    return lambda cs, ce: cube[cs:ce]


def _toi_for(T, extra):
    toi = {0}
    toi.update(extra)
    return sorted(toi)


@pytest.mark.parametrize("chunk_hours", [1, 3, 5, 7, 50])
def test_snapshot_differences_match_naive_window_sums(chunk_hours):
    rng = np.random.default_rng(0)
    T, Y, X = 30, 4, 5
    cube = (rng.random((T, Y, X)) * 10.0).astype(np.float32)
    # Windows of interest expressed as (start_idx, stop_idx_exclusive).
    windows = [(1, 7), (0, 30), (5, 6), (12, 25), (0, 1), (28, 30)]
    toi = _toi_for(T, [a for a, _ in windows] + [b for _, b in windows])

    snapshots, nbytes = _cumsum_snapshots(
        _provider_from_cube(cube), T, (Y, X), toi, chunk_hours
    )
    assert nbytes == cube.nbytes

    cube64 = cube.astype(np.float64)
    for a, b in windows:
        cumsum_based = snapshots[b] - snapshots[a]
        naive = cube64[a:b].sum(axis=0)
        assert np.allclose(cumsum_based, naive, rtol=1e-9, atol=1e-9)


def test_chunk_size_invariance():
    """Snapshots must be identical regardless of chunk size (boundary logic)."""
    rng = np.random.default_rng(1)
    T, Y, X = 24, 3, 3
    cube = (rng.random((T, Y, X)) * 5.0).astype(np.float32)
    toi = _toi_for(T, [4, 9, 17, 24])

    ref, _ = _cumsum_snapshots(_provider_from_cube(cube), T, (Y, X), toi, 1)
    for ch in (2, 5, 6, 24, 100):
        other, _ = _cumsum_snapshots(_provider_from_cube(cube), T, (Y, X), toi, ch)
        assert ref.keys() == other.keys()
        for k in ref:
            assert np.array_equal(ref[k], other[k])


def test_nan_treated_as_zero():
    rng = np.random.default_rng(2)
    T, Y, X = 12, 2, 2
    cube = (rng.random((T, Y, X)) * 3.0).astype(np.float64)
    cube[3, 0, 0] = np.nan
    toi = _toi_for(T, [2, 8])

    snaps, _ = _cumsum_snapshots(_provider_from_cube(cube), T, (Y, X), toi, 4)
    naive = np.where(np.isfinite(cube[2:8]), cube[2:8], 0.0).sum(axis=0)
    assert np.allclose(snaps[8] - snaps[2], naive)


def test_snapshot_zero_is_zeros():
    cube = np.ones((5, 2, 2), dtype=np.float32)
    snaps, _ = _cumsum_snapshots(_provider_from_cube(cube), 5, (2, 2), [0, 5], 2)
    assert np.array_equal(snaps[0], np.zeros((2, 2)))
    assert np.allclose(snaps[5], np.full((2, 2), 5.0))
