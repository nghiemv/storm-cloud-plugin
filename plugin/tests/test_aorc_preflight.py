"""Regression tests for aorc_preflight.assert_years_available.

Guards the fail-fast cache check that prevents the 30-minute-then-crash
behaviour we hit on 2026-05-27 when 2025.zarr wasn't mirrored.
"""

from __future__ import annotations

from unittest import mock

import pytest

from plugin.actions import aorc_preflight as ap


def test_required_years_single_year():
    assert ap.required_years("1979-10-01", "1979-12-31") == [1979]


def test_required_years_multi_year_inclusive():
    # 1979..2025 inclusive = 47 years.
    years = ap.required_years("1979-10-01", "2025-10-01")
    assert years == list(range(1979, 2026))


def test_required_years_late_year_window_pulls_in_next():
    # 2025-12-31 + 120h crosses into 2026 -> include 2026.
    years = ap.required_years("2025-01-01", "2025-12-31", storm_duration_hours=120)
    assert years[-1] == 2026
    assert 2025 in years


def test_required_years_handles_missing_end_date():
    assert ap.required_years("2020-06-15", None) == [2020]


def test_verify_skips_when_base_url_unset(monkeypatch):
    monkeypatch.delenv("AORC_S3_BASE_URL", raising=False)
    assert ap.verify_aorc_cache_years([1979, 1980, 1981]) == []


def test_verify_missing_returns_only_missing_years(monkeypatch):
    monkeypatch.setenv("AORC_S3_BASE_URL", "s3://test-bucket/aorc-cache")
    monkeypatch.delenv("AORC_S3_KEY", raising=False)
    monkeypatch.delenv("AORC_S3_SECRET", raising=False)
    monkeypatch.delenv("AORC_S3_ENDPOINT", raising=False)

    s3 = mock.Mock()

    def fake_head(*, Bucket, Key):
        if "2025" in Key:
            raise RuntimeError("NoSuchKey")
        return {}

    s3.head_object.side_effect = fake_head
    with mock.patch("boto3.client", return_value=s3):
        missing = ap.verify_aorc_cache_years([2023, 2024, 2025])
    assert missing == [2025]


def test_assert_raises_with_mirror_command(monkeypatch):
    """The error must point at exactly the mirror invocation to run."""
    monkeypatch.setenv("AORC_S3_BASE_URL", "s3://test-bucket/aorc-cache")
    with mock.patch.object(ap, "verify_aorc_cache_years", return_value=[2024, 2025]):
        with pytest.raises(RuntimeError) as excinfo:
            ap.assert_years_available(
                start_date="1979-10-01", end_date="2025-10-01", storm_duration_hours=72
            )
    msg = str(excinfo.value)
    assert "missing" in msg
    assert "./run.py mirror --year-start 2024 --year-end 2025" in msg


def test_assert_does_not_raise_when_all_present(monkeypatch, caplog):
    monkeypatch.setenv("AORC_S3_BASE_URL", "s3://test-bucket/aorc-cache")
    with mock.patch.object(ap, "verify_aorc_cache_years", return_value=[]):
        with caplog.at_level("INFO"):
            ap.assert_years_available(
                start_date="2020-01-01", end_date="2020-12-31", storm_duration_hours=24
            )
    assert "present in cache" in caplog.text


def test_assert_no_op_on_empty_range(monkeypatch):
    """Empty year range (impossible in practice, but defensive): no raise."""
    with mock.patch.object(ap, "required_years", return_value=[]):
        ap.assert_years_available(
            start_date="2020-01-01", end_date="2020-01-01", storm_duration_hours=0
        )


def test_verify_parses_bucket_and_prefix_correctly(monkeypatch):
    """Regression: AORC_S3_BASE_URL=s3://storm-cloud/aorc-cache-conus must
    head bucket=storm-cloud key=aorc-cache-conus/1979.zarr/.zmetadata, not
    bucket=aorc-cache-conus key=1979.zarr/.zmetadata."""
    monkeypatch.setenv("AORC_S3_BASE_URL", "s3://storm-cloud/aorc-cache-conus")
    s3 = mock.Mock()
    s3.head_object.return_value = {}
    seen_calls = []

    def capture(**kw):
        seen_calls.append(kw)
        return {}

    s3.head_object.side_effect = capture
    with mock.patch("boto3.client", return_value=s3):
        ap.verify_aorc_cache_years([1979])

    assert len(seen_calls) == 1
    assert seen_calls[0]["Bucket"] == "storm-cloud"
    assert seen_calls[0]["Key"] == "aorc-cache-conus/1979.zarr/.zmetadata"
