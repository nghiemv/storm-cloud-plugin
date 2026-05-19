"""Pytest config — put the project root on sys.path so ``plugin`` imports work."""

from __future__ import annotations

import sys
from pathlib import Path

# plugin/tests/conftest.py -> repo root is parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
