"""Run lifecycle: launch, stop, and resume compute runs.

Detaches runs to the background (survives the web process exiting) and
stops/resumes them via the container cidfile + plugin idempotency.
"""

from __future__ import annotations

import subprocess
import sys

from app.core import (
    OUTPUTS,
    ROOT,
    RUN_PY,
    _read_json,
    _safe_subdir,
    write_launch_json,
)
from app.status import _pid_alive


# ─── launching ───────────────────────────────────────────────────────────────


# Cross-platform process detachment: POSIX uses setsid, Windows uses
# DETACHED_PROCESS + CREATE_NEW_PROCESS_GROUP so the child survives the
# parent's exit and isn't bound to its console.
if sys.platform == "win32":
    _DETACH_KWARGS: dict = {
        "creationflags": (
            subprocess.DETACHED_PROCESS  # type: ignore[attr-defined]
            | subprocess.CREATE_NEW_PROCESS_GROUP
        ),
    }
else:
    _DETACH_KWARGS = {"start_new_session": True}


def _launch(
    args: list[str],
    name: str,
    *,
    payload_uuid: str | None = None,
    payload_attrs: dict | None = None,
) -> str:
    """Detach a run in the background.

    Writes ``launch.json`` BEFORE Popen so the UI sees the launch even if
    the process dies in its first millisecond (the dead PID still tells us
    it failed). Stdout/stderr stream to ``launch.log``.
    """
    safe = _safe_subdir(name)
    run_dir = OUTPUTS / safe
    run_dir.mkdir(parents=True, exist_ok=True)
    # Clear stale progress.json from prior runs in this dir so the UI doesn't
    # show a stale "done"/"current_step" until the new run writes its own.
    (run_dir / "progress.json").unlink(missing_ok=True)
    log_file = (run_dir / "launch.log").open("ab", buffering=0)
    proc = subprocess.Popen(
        args,
        cwd=str(ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        **_DETACH_KWARGS,
    )
    write_launch_json(
        run_dir,
        args=args,
        pid=proc.pid,
        payload_uuid=payload_uuid,
        payload_attrs=payload_attrs,
    )
    return safe


def _launch_local() -> str:
    return _launch([sys.executable, str(RUN_PY)], "quick-test")


def _launch_hec(
    uuid: str,
    name: str | None = None,
    *,
    payload_attrs: dict | None = None,
) -> str:
    target = _safe_subdir(name or uuid)
    args = [sys.executable, str(RUN_PY), "hec", uuid]
    if target != uuid:
        args.append(target)
    return _launch(args, target, payload_uuid=uuid, payload_attrs=payload_attrs)


def _stop(run_name: str) -> tuple[bool, str | None]:
    """Gracefully stop a running compute. Returns ``(stopped, error)``.

    Strategy: ``docker stop`` the container via its cidfile so the plugin's
    SIGTERM handler unwinds the current action and exits cleanly,
    preserving on-disk state for resume. Falls back to looking up the
    container by label if the cidfile is missing (older runs, manual
    deletion).
    """
    safe = _safe_subdir(run_name)
    run_dir = OUTPUTS / safe
    cidfile = run_dir / "container.id"
    container_id: str | None = None
    if cidfile.is_file():
        try:
            container_id = cidfile.read_text().strip() or None
        except OSError:
            container_id = None
    if not container_id:
        # Fallback: locate by label set in run.py:_run_hec_job.
        r = subprocess.run(
            [
                "docker",
                "ps",
                "-q",
                "--filter",
                f"label=storm-cloud-run={safe}",
            ],
            capture_output=True,
            text=True,
        )
        container_id = (r.stdout or "").strip().splitlines()[0:1]
        container_id = container_id[0] if container_id else None
    if not container_id:
        return False, "no running container found for this run"
    # docker stop sends SIGTERM, waits up to --timeout seconds, then SIGKILLs.
    # Give the plugin enough time to finish the current AORC date scan
    # iteration and write its final progress.json before falling back to
    # SIGKILL.
    r = subprocess.run(
        ["docker", "stop", "--timeout", "30", container_id],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        # "No such container" is fine — the container already exited (e.g.,
        # finished or was stopped by another path). Treat as success.
        msg = (r.stderr or r.stdout or "").strip()
        if "No such container" in msg:
            return True, None
        return False, msg or "docker stop failed"
    return True, None


def _rerun(run_name: str) -> tuple[str | None, str | None]:
    """Re-launch the run at ``compute/outputs/<run_name>/``.

    Returns ``(name, error)``. Idempotent plugin actions skip work already
    on disk, so the practical effect is "resume". Refuses if a launcher is
    still alive or if we don't know the payload UUID.
    """
    run_dir = OUTPUTS / _safe_subdir(run_name)
    launch = _read_json(run_dir / "launch.json")
    if launch is None:
        return None, f"no launch.json in {run_dir.name} — can't determine payload"
    if _pid_alive(launch.get("pid")):
        return None, "run is still active — stop it before re-running"
    uuid = launch.get("payload_uuid")
    if not uuid:
        return None, "launch.json has no payload_uuid (legacy run)"
    # Carry forward the stashed payload attrs so post-resume ETA stays valid.
    attrs = launch.get("payload_attrs") or {}
    return _launch_hec(uuid, run_name, payload_attrs=attrs), None
