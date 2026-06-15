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

from plugin.actions.convert_to_dss import convert_to_dss
from plugin.actions.create_grid_file import create_grid_file
from plugin.actions.download_inputs import download_inputs
from plugin.actions.process_storms import process_storms
from plugin.actions.upload_outputs import upload_outputs
from plugin import progress
from plugin.lib import RunContext, validate_payload
from plugin.progress import format_duration

_ACTIONS: tuple[Callable[[RunContext], None], ...] = (
    download_inputs,
    process_storms,
    convert_to_dss,
    create_grid_file,
    upload_outputs,
)

# Payloads use kebab-case action names ("download-inputs"); the Python
# function uses snake_case. They are 1:1 by convention — keep them so.
ACTION_DISPATCH: dict[str, Callable[[RunContext], None]] = {
    fn.__name__.replace("_", "-"): fn for fn in _ACTIONS
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
        if interrupted:
            # Second signal: the operator (or orchestrator escalating SIGTERM →
            # SIGKILL) wants out *now*, not after the current action. The first
            # signal only takes effect between actions, so a stop during the
            # multi-hour process-storms step would otherwise be deferred until
            # that step finished. Restore the default disposition and re-raise
            # so the process terminates immediately with correct signal
            # semantics. On-disk state is left intact for an idempotent resume.
            log.warning("Received signal %d again — exiting immediately", signum)
            signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)
            return
        log.warning("Received signal %d, will shut down after current action", signum)
        interrupted = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    succeeded = False
    start_time = time.monotonic()
    n_actions = len(payload.actions)
    action_names = [a.name for a in payload.actions]
    log.info("[plan] pipeline: %s", " → ".join(action_names))
    progress.set_plan(action_names)
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
            progress.step_start(i, n_actions, action.name)
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
            progress.step_done(i, n_actions, action.name, elapsed)

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
        progress.set_summary(n_actions, total_elapsed)
    finally:
        if succeeded:
            # Drop intermediate outputs (DSS, grids, downloaded inputs) but
            # keep the run-metadata files so the web UI can still show the
            # final summary, the launch command, and the launch log after
            # the run completes.
            preserved = {"progress.json", "launch.json", "launch.log"}
            for item in local_root.iterdir():
                if item.name in preserved:
                    continue
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    try:
                        item.unlink()
                    except OSError:
                        pass
            log.info(
                "Cleaned up %s (kept %s)", local_root, ", ".join(sorted(preserved))
            )
        else:
            log.warning(
                "Preserving %s for debugging (run failed or interrupted)", local_root
            )


def main() -> None:
    # Surface progress at Local/progress.json. The compose stack bind-mounts
    # Local/ to compute/outputs/<run>/ on the host so the snapshot is visible
    # without needing a port published from the container.
    progress.configure_state_file(Path("Local") / "progress.json")
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
