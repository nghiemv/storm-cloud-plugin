"""Parity tests for the vectorized (fftconvolve) max-transpose kernel.

``vectorized_transpose`` replaces stormhub's per-shift ``nanmean`` loop with a
single cross-correlation and claims *bit-identical* rank selection. This guards
that claim: the kernel must pick the same best shift (and same mean, to float
tolerance) as a naive reference loop. A silent numerical regression here would
corrupt every storm catalog's ranking, so it's the highest-value unit to pin.
"""

from __future__ import annotations

import numpy as np
import pytest

from plugin.actions.vectorized_transpose import _best_shift_by_correlation


def _naive_best_shift(data, mask_clipped, valid_shifts, row0, col0):
    """Reference: upstream's per-shift masked-nanmean argmax loop."""
    h, w = mask_clipped.shape
    best_mean = None
    best_shift = None
    for x_delta, y_delta in valid_shifts:
        i = row0 + y_delta
        j = col0 + x_delta
        window = data[i : i + h, j : j + w]
        masked = np.where(mask_clipped, window, np.nan)
        mean = float(np.nanmean(masked))
        if best_mean is None or mean > best_mean:
            best_mean = mean
            best_shift = (x_delta, y_delta)
    return best_shift, best_mean


def _fixture(seed=0, dh=20, dw=24, mh=5, mw=6):
    rng = np.random.default_rng(seed)
    data = rng.random((dh, dw)) * 100.0
    mask = rng.random((mh, mw)) > 0.4  # ~60% cells active
    mask[0, 0] = True  # ensure non-empty
    row0, col0 = 2, 3
    # shifts kept in-bounds: i=row0+y in [0, dh-mh], j=col0+x in [0, dw-mw]
    valid_shifts = [(-3, -2), (0, 0), (4, 1), (10, 5), (15, 13), (-1, 8)]
    return data, mask, valid_shifts, row0, col0


def test_kernel_matches_naive_loop():
    data, mask, shifts, row0, col0 = _fixture()
    got_shift, got_mean = _best_shift_by_correlation(data, mask, shifts, row0, col0)
    exp_shift, exp_mean = _naive_best_shift(data, mask, shifts, row0, col0)
    assert got_shift == exp_shift
    assert got_mean == pytest.approx(exp_mean, rel=1e-9, abs=1e-9)


@pytest.mark.parametrize("seed", range(8))
def test_kernel_matches_naive_loop_many_seeds(seed):
    data, mask, shifts, row0, col0 = _fixture(seed=seed)
    assert (
        _best_shift_by_correlation(data, mask, shifts, row0, col0)[0]
        == _naive_best_shift(data, mask, shifts, row0, col0)[0]
    )


def test_nan_outside_mask_does_not_affect_valid_positions():
    """NaNs that never sit under the mask at a valid shift must not change
    the result — the kernel substitutes 0 for NaN by design."""
    data, mask, shifts, row0, col0 = _fixture(seed=3)
    base_shift, base_mean = _best_shift_by_correlation(data, mask, shifts, row0, col0)
    # Poke a NaN into a corner cell that no valid shift's mask window covers.
    data2 = data.copy()
    data2[-1, -1] = np.nan
    shift2, mean2 = _best_shift_by_correlation(data2, mask, shifts, row0, col0)
    assert shift2 == base_shift
    assert mean2 == pytest.approx(base_mean, rel=1e-9, abs=1e-9)


def test_empty_mask_raises():
    data, _, shifts, row0, col0 = _fixture()
    empty = np.zeros((5, 6), dtype=bool)
    with pytest.raises(ValueError, match="empty"):
        _best_shift_by_correlation(data, empty, shifts, row0, col0)


def test_no_valid_shifts_raises():
    data, mask, _, row0, col0 = _fixture()
    with pytest.raises(ValueError, match="No valid shifts"):
        _best_shift_by_correlation(data, mask, [], row0, col0)
