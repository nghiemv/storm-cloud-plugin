"""Typed shared context for action handlers.

Actions communicate by populating typed fields on a single ``RunContext``
instance — no ad-hoc string keys, no untyped ``dict[str, Any]`` bag. Each
action documents its prerequisites by checking the fields it reads.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class LocalInputs:
    """Files materialized by ``download-inputs`` for downstream actions."""

    watershed_path: Path
    transposition_path: Path
    config_path: Path


@dataclass
class StormState:
    """Catalog + parameters built by ``process-storms``."""

    collection: Any  # pystac.Collection
    params: dict[str, Any]


@dataclass
class RunContext:
    pm: Any  # cc.plugin_manager.PluginManager
    payload: Any
    local_root: Path
    inputs: LocalInputs | None = None
    storms: StormState | None = None
