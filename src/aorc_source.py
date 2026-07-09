"""Resolve the payload's AORC source into the env contract stormhub reads.

stormhub reads AORC credentials/location from a single env contract
(``AORC_S3_BASE_URL`` + ``AORC_S3_ACCESS_KEY_ID`` / ``_SECRET_ACCESS_KEY`` /
``_ENDPOINT_URL`` / ``_REGION``) and builds one filesystem from it. This module
is the *only* place that contract is written, translating the payload's choice
into it before any action runs.

Credentials are supplied the same way as every other bucket in this plugin: as
a ``<PROFILE>_AWS_*`` environment group injected by the deployment (compare
``CC_AWS_*``, ``FFRD_AWS_*``, ``SC_AWS_*``). The payload only names the profile
and the (non-secret) bucket — no secrets in the payload.

Default (no ``aorc_source`` attribute): NOAA public bucket, anonymous.
"""

from __future__ import annotations

import logging
import os
from typing import Any

log = logging.getLogger(__name__)


def configure_aorc_source(attrs: dict[str, Any]) -> None:
    """Export the AORC_S3_* env contract from the payload attributes.

    ``aorc_source`` names a credential profile the deployment supplies as a
    ``<PROFILE>_AWS_ACCESS_KEY_ID`` / ``_SECRET_ACCESS_KEY`` / ``_ENDPOINT`` /
    ``_DEFAULT_REGION`` group. ``aorc_base_url`` (optional) selects the bucket.
    No ``aorc_source`` -> leave the contract unset so stormhub reads NOAA anon.
    """
    profile = (attrs.get("aorc_source") or "").strip()
    if not profile:
        log.info("AORC source: NOAA public (anonymous) — no aorc_source configured")
        return

    base_url = (attrs.get("aorc_base_url") or "").strip()
    if base_url:
        os.environ["AORC_S3_BASE_URL"] = base_url

    p = profile.upper()
    key = os.environ.get(f"{p}_AWS_ACCESS_KEY_ID")
    if not key:
        raise RuntimeError(
            f"aorc_source={profile!r} but {p}_AWS_ACCESS_KEY_ID is not set — "
            f"the deployment must supply the {p}_AWS_* credential group."
        )
    try:
        secret = os.environ[f"{p}_AWS_SECRET_ACCESS_KEY"]
    except KeyError as e:
        raise RuntimeError(
            f"aorc_source={profile!r}: {p}_AWS_SECRET_ACCESS_KEY is not set."
        ) from e

    os.environ["AORC_S3_ACCESS_KEY_ID"] = key
    os.environ["AORC_S3_SECRET_ACCESS_KEY"] = secret

    endpoint = os.environ.get(f"{p}_AWS_ENDPOINT")
    if endpoint:
        os.environ["AORC_S3_ENDPOINT_URL"] = endpoint
    region = os.environ.get(f"{p}_AWS_DEFAULT_REGION")
    if region:
        os.environ["AORC_S3_REGION"] = region

    log.info(
        "AORC source: mirror profile %s (bucket=%s, endpoint=%s)",
        profile,
        os.environ.get("AORC_S3_BASE_URL", "<default>"),
        endpoint or "<default>",
    )
