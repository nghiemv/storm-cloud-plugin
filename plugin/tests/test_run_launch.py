"""Tests for run.py launch robustness: duplicate-run guard + launch.log tee.

These guard the two regressions from the 2026-06-15 incident:
  - a second run stacked on the same output dir (two containers clobbering
    one compute/outputs/<name>/), and
  - launch.log left stale because a CLI-launched run's docker output never
    reached it (only app.py-launched runs were captured).
"""

from __future__ import annotations

import sys

import pytest

import run


class _StdoutTo:
    """Minimal stand-in for sys.stdout backed by a real file object.

    ``_stdout_is`` only needs ``fileno()``; the tee's mirror path also touches
    ``buffer`` and ``flush``.
    """

    def __init__(self, fileobj):
        self._f = fileobj
        self.buffer = fileobj

    def fileno(self):
        return self._f.fileno()

    def flush(self):
        self._f.flush()


def test_docker_run_logged_writes_child_output_to_launch_log(tmp_path):
    rc = run._docker_run_logged(
        [sys.executable, "-c", "print('hello-from-child')"], tmp_path
    )
    assert rc == 0
    assert "hello-from-child" in (tmp_path / "launch.log").read_text()


def test_docker_run_logged_propagates_exit_code(tmp_path):
    rc = run._docker_run_logged(
        [sys.executable, "-c", "import sys; sys.exit(7)"], tmp_path
    )
    assert rc == 7


def test_docker_run_logged_no_double_write_when_stdout_is_log(tmp_path, monkeypatch):
    """app.py points our stdout at launch.log; the tee must not duplicate."""
    log_path = tmp_path / "launch.log"
    with open(log_path, "ab") as redirected:
        monkeypatch.setattr(sys, "stdout", _StdoutTo(redirected))
        rc = run._docker_run_logged([sys.executable, "-c", "print('once')"], tmp_path)
    assert rc == 0
    assert log_path.read_text().count("once") == 1


def test_run_hec_job_refuses_duplicate(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(run, "COMPUTE", tmp_path)
    (tmp_path / "outputs").mkdir()
    monkeypatch.setattr(run, "_running_container_for", lambda name: "abc123def456ff")

    with pytest.raises(SystemExit) as ei:
        run._run_hec_job("some-uuid", "duwamish")

    assert ei.value.code == 1
    err = capsys.readouterr().err
    assert "already active" in err
    assert "docker stop abc123def456" in err  # first 12 chars of the id


def test_run_hec_job_proceeds_when_no_live_container(tmp_path, monkeypatch):
    """No live container → guard passes and we reach the docker launch."""
    monkeypatch.setattr(run, "COMPUTE", tmp_path)
    (tmp_path / "outputs").mkdir()
    monkeypatch.setattr(run, "_running_container_for", lambda name: None)
    monkeypatch.setattr(run, "write_launch_json", lambda *a, **k: None)

    called = {}

    def fake_run(args, run_dir):
        called["args"] = args
        called["run_dir"] = run_dir
        return 0

    monkeypatch.setattr(run, "_docker_run_logged", fake_run)

    run._run_hec_job("some-uuid", "duwamish")

    assert "docker" in called["args"] and "run" in called["args"]
    assert "storm-cloud-run=duwamish" in called["args"]
