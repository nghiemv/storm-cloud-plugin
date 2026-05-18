"""Pytest config — put the project root on sys.path so ``plugin`` imports work.

Local dev runs `pytest` from the repo root without installing the package; in
the Docker image, the workdir already contains `plugin/` so this is harmless.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
