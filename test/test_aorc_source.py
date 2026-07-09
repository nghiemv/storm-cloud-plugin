"""Unit tests for aorc_source.configure_aorc_source (payload -> AORC_S3_* env)."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from aorc_source import configure_aorc_source  # noqa: E402

_STORMHUB_KEYS = (
    "AORC_S3_BASE_URL",
    "AORC_S3_ACCESS_KEY_ID",
    "AORC_S3_SECRET_ACCESS_KEY",
    "AORC_S3_ENDPOINT_URL",
    "AORC_S3_REGION",
)

_MIRROR_ENV = {
    "HECDEV_AWS_ACCESS_KEY_ID": "AK",
    "HECDEV_AWS_SECRET_ACCESS_KEY": "SK",
    "HECDEV_AWS_ENDPOINT": "https://s3.hecdev.net",
    "HECDEV_AWS_DEFAULT_REGION": "us-east-1",
}


def test_no_source_leaves_contract_unset():
    with mock.patch.dict(os.environ, {}, clear=True):
        configure_aorc_source({})
        for k in _STORMHUB_KEYS:
            assert k not in os.environ


def test_mirror_profile_maps_to_stormhub_contract():
    attrs = {"aorc_source": "hecdev", "aorc_base_url": "s3://storm-cloud/aorc-cache-conus"}
    with mock.patch.dict(os.environ, _MIRROR_ENV, clear=True):
        configure_aorc_source(attrs)
        assert os.environ["AORC_S3_BASE_URL"] == "s3://storm-cloud/aorc-cache-conus"
        assert os.environ["AORC_S3_ACCESS_KEY_ID"] == "AK"
        assert os.environ["AORC_S3_SECRET_ACCESS_KEY"] == "SK"
        assert os.environ["AORC_S3_ENDPOINT_URL"] == "https://s3.hecdev.net"
        assert os.environ["AORC_S3_REGION"] == "us-east-1"


def test_missing_credential_group_is_a_clear_error():
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="HECDEV_AWS_ACCESS_KEY_ID"):
            configure_aorc_source({"aorc_source": "hecdev"})


def test_base_url_and_endpoint_optional():
    env = {"MIRROR_AWS_ACCESS_KEY_ID": "AK", "MIRROR_AWS_SECRET_ACCESS_KEY": "SK"}
    with mock.patch.dict(os.environ, env, clear=True):
        configure_aorc_source({"aorc_source": "mirror"})
        assert os.environ["AORC_S3_ACCESS_KEY_ID"] == "AK"
        # No base_url / endpoint / region provided -> not set (stormhub defaults).
        assert "AORC_S3_BASE_URL" not in os.environ
        assert "AORC_S3_ENDPOINT_URL" not in os.environ
        assert "AORC_S3_REGION" not in os.environ
