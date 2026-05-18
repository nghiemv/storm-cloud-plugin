"""Action: download-inputs — Materialize watershed/transposition GeoJSON locally."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from plugin.cc_io import download_to_local
from plugin.context import RunContext
from plugin.progress import Progress

log = logging.getLogger(__name__)


@dataclass
class LocalInputs:
    """Files materialized by ``download-inputs`` for downstream actions."""

    watershed_path: Path
    transposition_path: Path
    config_path: Path


_GEOJSON_TYPES = frozenset(
    (
        "Feature",
        "FeatureCollection",
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
        "GeometryCollection",
    )
)


def _validate_geojson(path: Path, key: str) -> None:
    """Validate, and if needed unwrap, a downloaded geometry file.

    StormCloud UI stores geometries as
    ``{"catalog_name": ..., "geometry": "<json-string>"}`` — unwrap that
    envelope back to plain GeoJSON in place.
    """
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"Input '{key}' is not valid JSON: {path} — {e}") from e

    if "geometry" in data and isinstance(data["geometry"], str) and "type" not in data:
        try:
            inner = json.loads(data["geometry"])
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Input '{key}' has a geometry wrapper but its inner value is "
                f"not valid JSON: {path} — {e}"
            ) from e
        path.write_text(json.dumps(inner), encoding="utf-8")
        data = inner
        log.info("Unwrapped StormCloud geometry envelope for '%s': %s", key, path)

    geo_type = data.get("type", "")
    if geo_type not in _GEOJSON_TYPES:
        raise ValueError(
            f"Input '{key}' is not valid GeoJSON (type={geo_type!r}): {path}"
        )


def download_inputs(ctx: RunContext) -> None:
    pm = ctx.pm
    payload = ctx.payload
    local_root = ctx.local_root

    transfers = [
        (source, key, remote_path)
        for source in payload.inputs
        for key, remote_path in source.paths.items()
    ]
    progress = Progress(total=len(transfers), label="download-inputs", log_every_n=1)

    for source, key, remote_path in transfers:
        local_path = local_root / Path(remote_path).name
        log.info("Downloading %s -> %s", remote_path, local_path)
        try:
            download_to_local(
                pm,
                source_name=source.name,
                pathkey=key,
                local_path=local_path,
                description=f"S3 download {remote_path}",
            )
            _validate_geojson(local_path, key)
        finally:
            progress.tick()

    catalog_id = payload.attributes["catalog_id"]
    input_paths = payload.inputs[0].paths
    watershed = local_root / Path(input_paths["watershed"]).name
    transposition = local_root / Path(input_paths["transposition"]).name

    config = {
        "watershed": {
            "id": f"{catalog_id}-watershed",
            "geometry_file": str(watershed),
            "description": "Watershed for storm catalog",
        },
        "transposition_region": {
            "id": f"{catalog_id}-transposition",
            "geometry_file": str(transposition),
            "description": "Transposition domain for storm catalog",
        },
    }
    config_path = local_root / "config.json"
    config_path.write_text(json.dumps(config, indent=4), encoding="utf-8")
    log.info("Config file created at %s", config_path)

    ctx.inputs = LocalInputs(
        watershed_path=watershed,
        transposition_path=transposition,
        config_path=config_path,
    )
