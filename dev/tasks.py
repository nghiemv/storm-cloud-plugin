#!/usr/bin/env python3
"""Local dev task runner. Requires Python 3 and Docker.

Run from the project root: ``python dev/tasks.py [command|payload]``.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PAYLOAD = "tests/examples/payload.json"
COMPOSE = ["docker", "compose", "-f", "docker/docker-compose.yaml"]


def run_cmd(args: list[str], env: dict[str, str] | None = None, **kwargs) -> None:
    merged_env = {**os.environ, **(env or {})}
    result = subprocess.run(args, env=merged_env, cwd=PROJECT_ROOT, **kwargs)
    if result.returncode != 0:
        sys.exit(result.returncode)


def run_quiet(args: list[str]) -> None:
    subprocess.run(
        args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=PROJECT_ROOT
    )


def cmd_build() -> None:
    """Init submodule + build Docker image."""
    run_cmd(["git", "submodule", "update", "--init"])
    run_cmd([*COMPOSE, "build", "storm-cloud-plugin"])


def cmd_package() -> None:
    """Build Docker image and save as storm-cloud-plugin.tar."""
    cmd_build()
    image = "ghcr.io/usace/storm-cloud-plugin:latest"
    out = PROJECT_ROOT / "storm-cloud-plugin.tar"
    print(f"Saving {image} -> {out}")
    run_cmd(["docker", "save", "-o", str(out), image])
    print(f"Done: {out} ({out.stat().st_size // 1024 // 1024} MB)")


def cmd_lint() -> None:
    """Ruff linter + format check."""
    run_cmd(["ruff", "check", "plugin/"])
    run_cmd(["ruff", "format", "--check", "plugin/"])


def cmd_format() -> None:
    """Auto-format with ruff."""
    run_cmd(["ruff", "format", "plugin/"])


def cmd_freeze() -> None:
    """Regenerate constraints.txt."""
    cmd_build()
    result = subprocess.run(
        [
            *COMPOSE, "run", "--rm",
            "--entrypoint", "python3.12 -m pip freeze",
            "storm-cloud-plugin",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(result.returncode)

    skip = ("-e ", "pkg_resources", "stormhub", "cc-py-sdk", "cc_py_sdk")
    lines = sorted(
        line
        for line in result.stdout.splitlines()
        if line.strip() and not any(line.startswith(s) for s in skip)
    )
    (PROJECT_ROOT / "constraints.txt").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    print("Updated constraints.txt")


def cmd_down() -> None:
    """Stop containers."""
    run_quiet([*COMPOSE, "down", "--remove-orphans"])


def cmd_clean() -> None:
    """Remove containers, volumes, Local/."""
    cmd_down()
    shutil.rmtree(PROJECT_ROOT / "Local", ignore_errors=True)
    run_quiet([*COMPOSE, "down", "-v", "--remove-orphans"])
    print("Cleaned.")


def cmd_viewer() -> None:
    """Serve the live progress viewer at http://localhost:8080."""
    run_cmd([sys.executable, str(PROJECT_ROOT / "dev" / "viewer.py")])


def cmd_run(payload_file: str) -> None:
    run_cmd(["git", "submodule", "update", "--init"])
    cmd_down()

    # Container path: tests/ is mounted as /inputs/ in the seed service
    container_path = "/inputs/" + payload_file.replace("\\", "/").split("tests/", 1)[-1]

    # Local/ in the container is bind-mounted to outputs/quick-test/ on the
    # host (see docker/docker-compose.yaml). Pre-create the host dir so the
    # mount doesn't show up as root-owned and dev/viewer.py can read it.
    (PROJECT_ROOT / "outputs" / "quick-test").mkdir(parents=True, exist_ok=True)

    print(f"Running: {payload_file}")
    print("Progress: http://localhost:8080  (start `python dev/tasks.py viewer` "
          "in another terminal if not already running)\n")
    run_cmd(
        [*COMPOSE, "run", "--rm", "seed"],
        env={"PAYLOAD_FILE": container_path},
    )
    run_cmd([*COMPOSE, "run", "--rm", "storm-cloud-plugin"])


TASK_COMMANDS = {
    "build": cmd_build,
    "package": cmd_package,
    "lint": cmd_lint,
    "format": cmd_format,
    "freeze": cmd_freeze,
    "viewer": cmd_viewer,
    "down": cmd_down,
    "clean": cmd_clean,
}


def main() -> None:
    arg = sys.argv[1] if len(sys.argv) > 1 else ""

    if arg in ("-h", "--help", "help"):
        print("Usage: python dev/tasks.py [PAYLOAD | command]\n")
        print("  (no args)    Run with tests/examples/payload.json")
        print("  PAYLOAD      Run with a custom payload file\n")
        for name in TASK_COMMANDS:
            print(f"  {name:<12} {TASK_COMMANDS[name].__doc__ or ''}")
        return

    if arg in TASK_COMMANDS:
        TASK_COMMANDS[arg]()
        return

    if arg:
        if not Path(arg).is_file():
            print(f"Error: file not found: {arg}", file=sys.stderr)
            sys.exit(1)
        payload_file = arg
    else:
        payload_file = DEFAULT_PAYLOAD

    cmd_run(payload_file)


if __name__ == "__main__":
    main()
