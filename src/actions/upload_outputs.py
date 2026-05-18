"""Action: upload-outputs — Upload all processed files to remote storage."""

from __future__ import annotations

import logging

from cc.plugin_manager import DataSourceOpInput

from context import RunContext
from s3_retry import with_retry

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

    log.info("Uploading %d files to %s", len(files), remote_base)

    for output_source in payload.outputs:
        for file in files:
            rel_path = str(file.relative_to(output_dir))
            remote_path = f"{remote_base}/{rel_path}"
            output_source.paths[rel_path] = remote_path
            op = DataSourceOpInput(
                name=output_source.name, pathkey=rel_path, datakey=None
            )
            log.info("  [%s] %s -> %s", output_source.name, file.name, remote_path)
            with_retry(
                lambda op=op, f=file: pm.copy_file_to_remote(ds=op, localpath=str(f)),
                description=f"S3 upload {file.name}",
            )
