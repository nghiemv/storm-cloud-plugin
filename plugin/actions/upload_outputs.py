"""Action: upload-outputs — Upload all processed files to remote storage."""

from __future__ import annotations

import logging

from plugin.cc_io import upload_from_local
from plugin.context import RunContext
from plugin.progress import Progress

log = logging.getLogger(__name__)


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
        for file in files:
            rel_path = str(file.relative_to(output_dir))
            remote_path = f"{remote_base}/{rel_path}"
            output_source.paths[rel_path] = remote_path
            log.info("  [%s] %s -> %s", output_source.name, file.name, remote_path)
            try:
                upload_from_local(
                    pm,
                    source_name=output_source.name,
                    pathkey=rel_path,
                    local_path=file,
                    description=f"S3 upload {file.name}",
                )
            finally:
                progress.tick()
