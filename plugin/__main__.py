"""Plugin entry point — invoked as ``python -m plugin``.

Initializes the PluginManager, validates the payload, then dispatches actions
in the order declared by the payload. Actions are idempotent at the
file-system level (they skip work already done on disk), so re-runs after a
partial failure simply pick up where the prior run left off.
"""

from __future__ import annotations

import logging
import logging.config
import multiprocessing
import os
import shutil
import signal
import sys
import time
from enum import IntEnum
from pathlib import Path
from typing import Any, Callable

from cc.plugin_manager import PluginManager
from stormhub.logger import initialize_logger

from plugin.actions import (
    convert_to_dss,
    create_grid_file,
    download_inputs,
    process_storms,
    upload_outputs,
)
from plugin import web
from plugin.context import RunContext
from plugin.payload import validate_payload
from plugin.progress import format_duration

ACTION_DISPATCH: dict[str, Callable[[RunContext], None]] = {
    "download-inputs": download_inputs,
    "process-storms": process_storms,
    "convert-to-dss": convert_to_dss,
    "create-grid-file": create_grid_file,
    "upload-outputs": upload_outputs,
}


class ExitCode(IntEnum):
    SUCCESS = 0
    CRITICAL = 1
    INVALID_PAYLOAD = 2
    DOWNLOAD_ERROR = 3
    PROCESSING_ERROR = 4


def _configure_logging() -> None:
    """JSON for production (``LOG_FORMAT=json``), plain text otherwise."""
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    if os.environ.get("LOG_FORMAT", "").lower() == "json":
        logging.config.dictConfig(
            {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "json": {
                        "format": '{"time":"%(asctime)s","level":"%(levelname)s",'
                        '"logger":"%(name)s","message":"%(message)s"}',
                        "datefmt": "%Y-%m-%dT%H:%M:%S",
                    },
                },
                "handlers": {
                    "console": {
                        "class": "logging.StreamHandler",
                        "formatter": "json",
                        "stream": "ext://sys.stdout",
                    },
                },
                "root": {"level": level, "handlers": ["console"]},
            }
        )
    else:
        initialize_logger(level=getattr(logging, level, logging.INFO))


_configure_logging()
log = logging.getLogger(__name__)

try:
    multiprocessing.set_start_method("spawn")
except RuntimeError:
    pass


def run_actions(pm: PluginManager, payload: Any) -> None:
    """Dispatch each action declared by the payload, in order."""
    local_root = Path("Local")
    local_root.mkdir(parents=True, exist_ok=True)
    ctx = RunContext(pm=pm, payload=payload, local_root=local_root)

    interrupted = False

    def handle_signal(signum: int, frame: Any) -> None:
        nonlocal interrupted
        log.warning("Received signal %d, will shut down after current action", signum)
        interrupted = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    succeeded = False
    start_time = time.monotonic()
    n_actions = len(payload.actions)
    action_names = [a.name for a in payload.actions]
    log.info("[plan] pipeline: %s", " → ".join(action_names))
    web.STATE.set_plan(action_names)
    durations: list[float] = []

    try:
        for i, action in enumerate(payload.actions, start=1):
            if interrupted:
                log.warning("Shutdown requested, aborting before action %d", i)
                raise KeyboardInterrupt

            handler = ACTION_DISPATCH.get(action.name)
            if handler is None:
                raise ValueError(
                    f"Unknown action: {action.name} "
                    f"(available: {list(ACTION_DISPATCH)})"
                )

            log.info("[step %d/%d] start %s", i, n_actions, action.name)
            web.STATE.step_start(i, n_actions, action.name)
            t0 = time.monotonic()
            handler(ctx)
            elapsed = time.monotonic() - t0
            durations.append(elapsed)
            log.info(
                "[step %d/%d] done %s in %s",
                i,
                n_actions,
                action.name,
                format_duration(elapsed),
            )
            web.STATE.step_done(i, n_actions, action.name, elapsed)

        succeeded = True
        total_elapsed = time.monotonic() - start_time
        log.info(
            "[summary] all %d actions completed in %s (%s)",
            n_actions,
            format_duration(total_elapsed),
            ", ".join(
                f"{n}={format_duration(d)}" for n, d in zip(action_names, durations)
            ),
        )
        web.STATE.set_summary(n_actions, total_elapsed)
    finally:
        if succeeded:
            shutil.rmtree(local_root, ignore_errors=True)
            log.info("Cleaned up %s", local_root)
        else:
            log.warning(
                "Preserving %s for debugging (run failed or interrupted)", local_root
            )


def main() -> None:
    web.start_if_enabled()
    pm = PluginManager()
    payload = pm.get_payload()
    validate_payload(payload)
    run_actions(pm, payload)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Plugin interrupted")
        sys.exit(ExitCode.CRITICAL)
    except ValueError as e:
        log.error("Invalid payload: %s", e)
        sys.exit(ExitCode.INVALID_PAYLOAD)
    except FileNotFoundError as e:
        log.error("Missing file: %s", e)
        sys.exit(ExitCode.DOWNLOAD_ERROR)
    except Exception as e:
        log.error("Plugin failed: %s", e, exc_info=True)
        sys.exit(ExitCode.PROCESSING_ERROR)
