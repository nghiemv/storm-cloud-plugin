"""Vectorized replacement for ``stormhub.met.transpose.Transpose.max_transpose``.

The upstream method iterates ``self.valid_shifts`` (stride=4 over the
transposition region) and for each shift slices the 2D precip array,
applies the watershed mask, and computes ``np.nanmean``. For
indian-creek's geometry that's ~17 K iterations × ~100 µs each ≈ 2 s
per storm date — by far the dominant cost once the AORC cache is in
place (see the A/B/C benchmark in plugin/actions/vectorized_scan.py).

This module computes the same per-shift mean as a single C-coded 2D
cross-correlation via ``scipy.signal.correlate2d``. For 800×400 data
with a ~50×50 watershed mask that's milliseconds instead of seconds.

Output is numerically identical to the loop at valid shifts: by
construction (see ``Transpose.valid_shifts``) no NaN cell ever sits
under the watershed mask at a valid shift, so ``nanmean`` reduces to
``sum / count`` — exactly what correlate2d computes. Float-summation
order differs (direct correlation in C vs Python's left-to-right
loop) so individual means may differ by ~1e-15; top-rank stability
matches the same caveat documented in vectorized_scan.py.

Gating: shares ``CC_VECTORIZED_SCAN=1`` with the scan replacement —
both monkey-patches install/restore together because they target the
same overall optimization (collapse process-storms cost).
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import numpy as np
from scipy.signal import fftconvolve
from shapely.affinity import translate

log = logging.getLogger(__name__)


def _best_shift_by_correlation(
    np_data_array: np.ndarray,
    watershed_mask_clipped: np.ndarray,
    valid_shifts: list[tuple[int, int]],
    row0: int,
    col0: int,
) -> tuple[tuple[int, int], float]:
    """Return ``(best_shift, best_mean)`` over ``valid_shifts``.

    Pure NumPy/SciPy kernel — no geo deps — so it is importable and
    unit-testable in CI without the Docker image. ``corr[i, j]`` is the
    sum of ``data`` under the watershed mask placed at ``(i, j)``; the mean
    is ``corr / mask_count``. At valid shifts no NaN cell sits under the
    mask (``Transpose.valid_shifts`` guarantees this), so substituting 0
    for NaN and dividing the sum by the cell count equals the upstream
    ``nanmean`` loop — modulo C-vs-Python float-summation order (~1e-15).
    """
    data_for_corr = np.where(np.isfinite(np_data_array), np_data_array, 0.0)

    mask = watershed_mask_clipped.astype(np.float64)
    mask_count = float(mask.sum())
    if mask_count == 0.0:
        raise ValueError(
            "watershed_mask_clipped is empty — Transpose can't compute means"
        )

    # We want cross-correlation, not convolution:
    #   corr[i, j] = sum_{r, c} data[i+r, j+c] * mask[r, c]
    # ``fftconvolve(data, kernel, mode='valid')`` computes convolution,
    # which is correlation of the FLIPPED kernel — so we pre-flip the
    # mask. fftconvolve uses FFTs (O(N log N) regardless of kernel size)
    # vs scipy.signal.correlate2d's direct convolution (O(N × kernel²)),
    # which made the first cut of this patch ~100× slower than necessary.
    # For 800×400 data × 50×50 watershed: correlate2d ≈ 1.5–2 s, fftconvolve
    # ≈ 5–10 ms. Tiny float-precision delta (~1e-12 typical) vs the
    # direct version — same scale as the bit-noise we tolerate elsewhere.
    corr = fftconvolve(data_for_corr, mask[::-1, ::-1], mode="valid")

    # Walk the valid_shifts list and pick the argmax by mean. Same
    # tie-breaker as the upstream loop (strict >, first occurrence wins).
    best_mean: float | None = None
    best_shift: tuple[int, int] | None = None
    for x_delta, y_delta in valid_shifts:
        i = row0 + y_delta
        j = col0 + x_delta
        if not (0 <= i < corr.shape[0] and 0 <= j < corr.shape[1]):
            continue  # defensive — valid_shifts already bounds-checks
        mean = corr[i, j] / mask_count
        if best_mean is None or mean > best_mean:
            best_mean = mean
            best_shift = (x_delta, y_delta)

    if best_shift is None:
        raise ValueError("No valid shifts to maximize over")
    return best_shift, best_mean


def vectorized_max_transpose(self, func: Callable | None = None) -> tuple:
    """Drop-in replacement for ``Transpose.max_transpose``.

    Computes mean(precip × watershed_mask) at every (x_delta, y_delta)
    in ``self.valid_shifts`` via one ``scipy.signal.correlate2d`` pass
    instead of the upstream Python loop.

    The return signature matches the original:
    ``(translated_watershed_poly, affine_transform, results_from_func)``.
    """
    from affine import Affine

    original_window_row_slice, original_window_col_slice = (
        self.watershed_window.toslices()
    )
    row0 = original_window_row_slice.start
    col0 = original_window_col_slice.start
    h = original_window_row_slice.stop - row0
    w = original_window_col_slice.stop - col0

    # NaN handling and the correlation-vs-loop equivalence live in the pure
    # kernel (unit-tested in test_vectorized_transpose.py).
    best_shift, _ = _best_shift_by_correlation(
        self.np_data_array,
        self.watershed_mask_clipped,
        self.valid_shifts,
        row0,
        col0,
    )

    x_delta, y_delta = best_shift
    # Rebuild the masked array at the winning shift so ``func`` (the
    # _create_stats callback) gets exactly what the upstream loop
    # would have handed it.
    adjusted_row_start = row0 + y_delta
    adjusted_row_stop = adjusted_row_start + h
    adjusted_col_start = col0 + x_delta
    adjusted_col_stop = adjusted_col_start + w
    data_clipped = self.np_data_array[
        adjusted_row_start:adjusted_row_stop,
        adjusted_col_start:adjusted_col_stop,
    ]
    data_clipped_masked = np.ma.masked_array(data_clipped, ~self.watershed_mask_clipped)

    max_shift_xy = (
        float(x_delta * self.x_cellsize),
        float(y_delta * self.y_cellsize),
    )
    results: Any = func(data_clipped_masked) if func else None

    poly = self._array_to_polygon(self.watershed_mask)
    poly = translate(poly, *max_shift_xy)
    aff = Affine.translation(*max_shift_xy)
    return poly, aff, results


# ── Public API: monkey-patch install / restore ──────────────────────────────


_original_max_transpose: Callable | None = None


def install() -> None:
    """Swap ``Transpose.max_transpose`` for the vectorized version. Idempotent."""
    global _original_max_transpose
    from stormhub.met.transpose import Transpose

    if _original_max_transpose is not None:
        return  # already installed
    _original_max_transpose = Transpose.max_transpose
    Transpose.max_transpose = vectorized_max_transpose
    log.info("Vectorized Transpose.max_transpose installed (scipy.signal.fftconvolve)")


def restore() -> None:
    """Undo ``install()``."""
    global _original_max_transpose
    from stormhub.met.transpose import Transpose

    if _original_max_transpose is None:
        return
    Transpose.max_transpose = _original_max_transpose
    _original_max_transpose = None
