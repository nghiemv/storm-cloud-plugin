"""Patch stormhub's thread batch_size so ``num_workers`` actually parallelizes.

Upstream ``multi_processor`` does::

    batch_size = max(min(num_workers // 3, 10), 2) if use_threads else num_workers

The conservative ``// 3`` cap was added to prevent OOM when each thread
held a full lazy-loaded AORC slice. With our AORC mirror + the
vectorized ``max_transpose`` (fftconvolve releases GIL on the hot path)
the memory regime is different, and capping a 6-worker pool at
batch_size=2 leaves 4 of 6 threads idle.

This patch flips the threaded formula to match the process formula
(``batch_size = num_workers``), restoring full parallelism. Install /
restore on the same gate as ``vectorized_transpose``.
"""

from __future__ import annotations

import logging
from typing import Callable

log = logging.getLogger(__name__)

_original_multi_processor: Callable | None = None


def _patched_multi_processor(*args, **kwargs):
    """Drop-in replacement: forces ``batch_size = num_workers`` when threading."""
    import gc
    import os
    import traceback
    from concurrent.futures import (
        ProcessPoolExecutor,
        ThreadPoolExecutor,
        as_completed,
    )

    from stormhub.met.storm_catalog import (
        _SPAWN_CTX,
        storm_search_results_to_csv_line,
    )

    func = kwargs.get("func") or args[0]
    catalog = kwargs.get("catalog") or args[1]
    storm_duration = kwargs.get("storm_duration") or args[2]
    output_csv = kwargs.get("output_csv") or args[3]
    event_dates = kwargs.get("event_dates") or args[4]
    num_workers = kwargs.get("num_workers") or (args[5] if len(args) > 5 else None)
    use_threads = (
        kwargs.get("use_threads", False)
        if "use_threads" in kwargs
        else (args[6] if len(args) > 6 else False)
    )
    with_tb = (
        kwargs.get("with_tb", False)
        if "with_tb" in kwargs
        else (args[7] if len(args) > 7 else False)
    )

    executor_class = ThreadPoolExecutor if use_threads else ProcessPoolExecutor

    if not os.path.exists(output_csv):
        with open(output_csv, "w", encoding="utf-8") as f:
            f.write("storm_date,min,mean,max,x,y\n")

    count = len(event_dates)
    batch_size = num_workers  # ← the only change vs upstream
    logging.info("Processing in batches of %d (plugin-patched)", batch_size)

    executor_kwargs = {"max_workers": num_workers}
    if not use_threads:
        executor_kwargs["mp_context"] = _SPAWN_CTX

    with executor_class(**executor_kwargs) as executor:
        for i in range(0, len(event_dates), batch_size):
            batch = event_dates[i : i + batch_size]
            futures = [
                executor.submit(func, catalog, date, storm_duration) for date in batch
            ]
            with open(output_csv, "a", encoding="utf-8") as f:
                for future in as_completed(futures):
                    count -= 1
                    try:
                        r = future.result()
                        f.write(storm_search_results_to_csv_line(r))
                        f.flush()
                        logging.info(
                            "%s processed (%d remaining)", r["storm_date"], count
                        )
                        del r
                    except Exception as e:
                        if with_tb:
                            tb = traceback.format_exc()
                            logging.error("Error processing: %s\n%s", e, tb)
                        else:
                            logging.error("Error processing: %s", e)
            del futures
            del batch
            gc.collect()


def install() -> None:
    global _original_multi_processor
    from stormhub.met import storm_catalog as sc_mod

    if _original_multi_processor is not None:
        return
    _original_multi_processor = sc_mod.multi_processor
    sc_mod.multi_processor = _patched_multi_processor
    log.info(
        "Patched multi_processor: batch_size = num_workers (full thread parallelism)"
    )


def restore() -> None:
    global _original_multi_processor
    from stormhub.met import storm_catalog as sc_mod

    if _original_multi_processor is None:
        return
    sc_mod.multi_processor = _original_multi_processor
    _original_multi_processor = None
