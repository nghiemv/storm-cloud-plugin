"""Shared run context passed between action handlers.

Action-output schemas (``LocalInputs``, ``StormState``) live with the actions
that produce them — see ``plugin/actions/download_inputs.py`` and
``plugin/actions/catalog.py``. Only the cross-action plumbing lives here.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from plugin.actions.catalog import StormState
    from plugin.actions.download_inputs import LocalInputs


@dataclass
class RunContext:
    pm: Any  # cc.plugin_manager.PluginManager
    payload: Any
    local_root: Path
    inputs: "LocalInputs | None" = None
    storms: "StormState | None" = None
