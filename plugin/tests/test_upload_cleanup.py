"""Regression tests for upload_outputs._cleanup_stale_remote_keys.

Guards the post-upload reconcile that catches the rank-mismatch class of bug
we hit on 2026-05-27 (re-running a catalog left DSS files from the previous
run's different top-N ranking in S3, breaking grid-entry counts).
"""

from __future__ import annotations

from unittest import mock

import pytest

from plugin.actions import upload_outputs as uo


@pytest.fixture
def s3_env(monkeypatch):
    monkeypatch.setenv("CC_AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("CC_AWS_SECRET_ACCESS_KEY", "s")
    monkeypatch.setenv("CC_AWS_ENDPOINT", "http://localhost")
    monkeypatch.setenv("CC_AWS_S3_BUCKET", "test-bucket")


def _fake_paginator(keys):
    """Build a fake boto3 paginator that returns ``keys`` in one page."""
    paginator = mock.Mock()
    paginator.paginate.return_value = [{"Contents": [{"Key": k} for k in keys]}]
    return paginator


def test_skip_when_env_unset(monkeypatch, caplog):
    for k in (
        "CC_AWS_ACCESS_KEY_ID",
        "CC_AWS_SECRET_ACCESS_KEY",
        "CC_AWS_ENDPOINT",
        "CC_AWS_S3_BUCKET",
    ):
        monkeypatch.delenv(k, raising=False)
    with caplog.at_level("INFO"):
        uo._cleanup_stale_remote_keys("Whitehorse", {"Whitehorse/a"})
    assert "skipping" in caplog.text


def test_deletes_only_stale_keys(s3_env, caplog):
    s3 = mock.Mock()
    s3.get_paginator.return_value = _fake_paginator(
        [
            "Whitehorse/data/19800112_24hr_st1_r003.dss",  # expected
            "Whitehorse/data/19800112_24hr_st1_r068.dss",  # stale
            "Whitehorse/catalog.json",  # expected
            "Whitehorse/_archive.zip",  # expected
            "Whitehorse/data/orphan.dss",  # stale
        ]
    )
    s3.delete_objects.return_value = {}  # boto3 returns a dict; mock doesn't by default
    expected = {
        "Whitehorse/data/19800112_24hr_st1_r003.dss",
        "Whitehorse/catalog.json",
        "Whitehorse/_archive.zip",
    }
    with mock.patch("boto3.client", return_value=s3), caplog.at_level("INFO"):
        uo._cleanup_stale_remote_keys("Whitehorse", expected)

    s3.delete_objects.assert_called_once()
    call_kwargs = s3.delete_objects.call_args.kwargs
    deleted_keys = {o["Key"] for o in call_kwargs["Delete"]["Objects"]}
    assert deleted_keys == {
        "Whitehorse/data/19800112_24hr_st1_r068.dss",
        "Whitehorse/data/orphan.dss",
    }
    assert "deleted 2 stale keys" in caplog.text


def test_noop_when_nothing_stale(s3_env, caplog):
    s3 = mock.Mock()
    s3.get_paginator.return_value = _fake_paginator(
        ["Whitehorse/data/a.dss", "Whitehorse/catalog.json"]
    )
    expected = {"Whitehorse/data/a.dss", "Whitehorse/catalog.json"}
    with mock.patch("boto3.client", return_value=s3), caplog.at_level("INFO"):
        uo._cleanup_stale_remote_keys("Whitehorse", expected)
    s3.delete_objects.assert_not_called()
    assert "0 stale to delete" in caplog.text


def test_refuses_to_delete_majority_safety_guard(s3_env, caplog):
    """Defensive: if >50% of keys would be deleted, the expected-set was
    probably built wrong (e.g. a sink with empty paths). Refuse rather than
    nuke real data."""
    actual_keys = [f"Whitehorse/data/{i}.dss" for i in range(100)]
    expected = {"Whitehorse/data/0.dss"}  # only 1 expected
    s3 = mock.Mock()
    s3.get_paginator.return_value = _fake_paginator(actual_keys)
    with mock.patch("boto3.client", return_value=s3), caplog.at_level("ERROR"):
        uo._cleanup_stale_remote_keys("Whitehorse", expected)
    s3.delete_objects.assert_not_called()
    assert "REFUSED" in caplog.text


def test_failure_does_not_raise(s3_env, caplog):
    """Hygiene step must not break the run."""
    with (
        mock.patch("boto3.client", side_effect=RuntimeError("boom")),
        caplog.at_level("WARNING"),
    ):
        uo._cleanup_stale_remote_keys("Whitehorse", {"Whitehorse/a"})
    assert "cleanup failed" in caplog.text


def test_safety_guard_allows_modest_cleanup(s3_env, caplog):
    """The safety threshold is len(actual)//2 (with a floor of 10) — sub-half
    cleanups go through."""
    actual_keys = [f"Whitehorse/data/{i}.dss" for i in range(100)]
    expected = {
        f"Whitehorse/data/{i}.dss" for i in range(60)
    }  # 40% stale, under threshold
    s3 = mock.Mock()
    s3.get_paginator.return_value = _fake_paginator(actual_keys)
    s3.delete_objects.return_value = {}
    with mock.patch("boto3.client", return_value=s3), caplog.at_level("INFO"):
        uo._cleanup_stale_remote_keys("Whitehorse", expected)
    s3.delete_objects.assert_called_once()
    assert "deleted 40 stale keys" in caplog.text
