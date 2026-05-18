"""Payload validation.

CC SDK convention: every payload attribute value is a string. This module
enforces required keys and per-attribute formats so the action handlers can
trust their inputs without re-parsing.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable

REQUIRED_ATTRS = ("catalog_id", "catalog_description", "output_path", "start_date")
REQUIRED_INPUT_KEYS = ("watershed", "transposition")

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _is_iso_date(v: str) -> bool:
    return bool(_DATE_RE.fullmatch(v))


def _is_positive_int(v: str) -> bool:
    return v.isdigit() and int(v) > 0


def _is_non_negative_float(v: str) -> bool:
    try:
        return float(v) >= 0
    except ValueError:
        return False


def _is_json_string_list(v: str) -> bool:
    try:
        parsed = json.loads(v)
    except (json.JSONDecodeError, TypeError):
        return False
    return isinstance(parsed, list) and all(isinstance(d, str) for d in parsed)


_VALIDATORS: dict[str, tuple[Callable[[str], bool], str]] = {
    "start_date": (_is_iso_date, "YYYY-MM-DD date string"),
    "end_date": (_is_iso_date, "YYYY-MM-DD date string"),
    "storm_duration": (_is_positive_int, "positive integer string"),
    "top_n_events": (_is_positive_int, "positive integer string"),
    "check_every_n_hours": (_is_positive_int, "positive integer string"),
    "min_precip_threshold": (_is_non_negative_float, "non-negative numeric string"),
    "specific_dates": (_is_json_string_list, "JSON array of date strings"),
}


def validate_payload(payload: Any) -> None:
    """Raise ``ValueError`` with a clear message if the payload is misconfigured."""
    attrs = payload.attributes

    missing = [k for k in REQUIRED_ATTRS if k not in attrs]
    if missing:
        raise ValueError(f"Missing required payload attributes: {missing}")

    non_string = [k for k, v in attrs.items() if not isinstance(v, str)]
    if non_string:
        raise ValueError(
            "All payload attribute values must be strings (CC SDK convention), "
            f"but these are not: {non_string}"
        )

    errors: list[str] = []
    for key, (check_fn, description) in _VALIDATORS.items():
        value = attrs.get(key)
        if not value:
            continue
        if not check_fn(value):
            errors.append(f"  {key}={value!r} — expected {description}")
    if errors:
        raise ValueError("Invalid payload attribute values:\n" + "\n".join(errors))

    if not payload.outputs:
        raise ValueError("Payload has no outputs configured")
    if not payload.inputs:
        raise ValueError("Payload has no inputs configured")

    input_keys = payload.inputs[0].paths
    missing_keys = [k for k in REQUIRED_INPUT_KEYS if k not in input_keys]
    if missing_keys:
        raise ValueError(f"Missing required input path keys: {missing_keys}")
