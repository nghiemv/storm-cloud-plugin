"""CC SDK transfer boundary.

Wraps the ``PluginManager.copy_file_*`` calls with exponential-backoff retry
and the boilerplate ``DataSourceOpInput`` construction, so action handlers
just say "download this key to this path" without restating the SDK shape or
the retry policy on every call.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Callable

from cc.plugin_manager import DataSourceOpInput

log = logging.getLogger(__name__)

MAX_RETRIES = 3
INITIAL_DELAY = 2  # seconds, doubled each retry


def _with_retry(op: Callable[[], Any], *, description: str) -> Any:
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


def download_to_local(
    pm: Any, *, source_name: str, pathkey: str, local_path: Path, description: str
) -> None:
    op = DataSourceOpInput(name=source_name, pathkey=pathkey, datakey=None)
    _with_retry(
        lambda: pm.copy_file_to_local(ds=op, localpath=str(local_path)),
        description=description,
    )


def upload_from_local(
    pm: Any, *, source_name: str, pathkey: str, local_path: Path, description: str
) -> None:
    op = DataSourceOpInput(name=source_name, pathkey=pathkey, datakey=None)
    _with_retry(
        lambda: pm.copy_file_to_remote(ds=op, localpath=str(local_path)),
        description=description,
    )
