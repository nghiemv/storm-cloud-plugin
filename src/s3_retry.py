"""Exponential-backoff retry wrapper for S3 transfers via cc-py-sdk."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

log = logging.getLogger(__name__)

MAX_RETRIES = 3
INITIAL_DELAY = 2  # seconds, doubled each retry


def with_retry(op: Callable[[], Any], *, description: str) -> Any:
    """Run ``op``, retrying transient failures with exponential backoff."""
    delay = INITIAL_DELAY
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return op()
        except Exception:
            if attempt == MAX_RETRIES:
                raise
            log.warning(
                "%s attempt %d/%d failed, retrying in %ds",
                description,
                attempt,
                MAX_RETRIES,
                delay,
            )
            time.sleep(delay)
            delay *= 2
