"""Action: upload-outputs — Upload all processed files to remote storage."""

from __future__ import annotations

import json
import logging
import time

from plugin.lib import RunContext, upload_from_local
from plugin.progress import Progress

log = logging.getLogger(__name__)

# Sidecar that records ``{sink_name: {rel_path: epoch_ts}}`` for files already
# uploaded. Lets resumed runs skip the S3 PUTs they've already completed —
# safe because outputs are produced deterministically by upstream actions.
_MARKER_NAME = ".uploaded.json"


def upload_outputs(ctx: RunContext) -> None:
    pm = ctx.pm
    payload = ctx.payload

    catalog_id = payload.attributes["catalog_id"]
    remote_base = payload.attributes["output_path"]
    output_dir = ctx.local_root / catalog_id

    if not output_dir.exists():
        raise FileNotFoundError(f"Output directory not found: {output_dir}")

    files = [f for f in output_dir.rglob("*") if f.is_file()]
    if not files:
        raise FileNotFoundError(f"No output files found in: {output_dir}")

    marker_path = ctx.local_root / _MARKER_NAME
    try:
        marker: dict[str, dict[str, float]] = json.loads(marker_path.read_text())
        if not isinstance(marker, dict):
            marker = {}
    except (FileNotFoundError, json.JSONDecodeError):
        marker = {}

    total_transfers = len(payload.outputs) * len(files)
    log.info(
        "Uploading %d files to %s (%d total transfers across %d sinks)",
        len(files),
        remote_base,
        total_transfers,
        len(payload.outputs),
    )
    progress = Progress(total=total_transfers, label="upload-outputs")

    for output_source in payload.outputs:
        sink_done = marker.setdefault(output_source.name, {})
        for file in files:
            rel_path = str(file.relative_to(output_dir))
            remote_path = f"{remote_base}/{rel_path}"
            output_source.paths[rel_path] = remote_path
            if rel_path in sink_done:
                log.info(
                    "  [%s] skip %s — already uploaded", output_source.name, file.name
                )
                progress.tick()
                continue
            log.info("  [%s] %s -> %s", output_source.name, file.name, remote_path)
            try:
                upload_from_local(
                    pm,
                    source_name=output_source.name,
                    pathkey=rel_path,
                    local_path=file,
                    description=f"S3 upload {file.name}",
                )
                sink_done[rel_path] = time.time()
                # Persist after every successful upload so a crash mid-loop
                # doesn't force re-uploading what we already did.
                marker_path.write_text(json.dumps(marker))
            finally:
                progress.tick()
