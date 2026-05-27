"""Action: upload-outputs — Upload all processed files to remote storage."""

from __future__ import annotations

import json
import logging
import time
import zipfile
from pathlib import Path
from typing import Any

from plugin.lib import RunContext, upload_from_local
from plugin.progress import Progress

log = logging.getLogger(__name__)

# Sidecar that records ``{sink_name: {rel_path: epoch_ts}}`` for files already
# uploaded. Lets resumed runs skip the S3 PUTs they've already completed —
# safe because outputs are produced deterministically by upstream actions.
_MARKER_NAME = ".uploaded.json"

# One-shot archive of the whole catalog uploaded alongside the per-file
# objects so download consumers (UI download button, sync jobs) fetch one
# key instead of paginating through ~1400 small keys. Underscore prefix
# marks it as an internal artifact, easy to filter when iterating catalog
# contents.
_ARCHIVE_NAME = "_archive.zip"


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

    # Single-archive write — runs after all per-file uploads succeed so the
    # archive reflects the final catalog state.
    _build_and_upload_archive(
        pm=pm,
        output_dir=output_dir,
        remote_base=remote_base,
        outputs=payload.outputs,
        marker=marker,
        marker_path=marker_path,
    )


def _build_and_upload_archive(
    *,
    pm: Any,
    output_dir: Path,
    remote_base: str,
    outputs: list,
    marker: dict,
    marker_path: Path,
) -> None:
    """Zip the entire catalog locally and upload once per output sink.

    Idempotent: skips the rebuild + upload when every sink already records
    ``_archive.zip`` in the marker and the local file exists. Compression
    is ``ZIP_DEFLATED`` level 1 — catalog payloads (PNG thumbnails, binary
    DSS, geotiff) are already compressed, so higher levels burn CPU for
    ~1% size reduction.
    """
    archive_local = output_dir / _ARCHIVE_NAME

    pending = [s for s in outputs if _ARCHIVE_NAME not in marker.get(s.name, {})]
    if not pending and archive_local.exists():
        log.info("Archive %s already uploaded to all sinks — skipping", _ARCHIVE_NAME)
        return

    if not archive_local.exists():
        log.info("Building catalog archive: %s", archive_local)
        t0 = time.time()
        total_bytes = 0
        file_count = 0
        with zipfile.ZipFile(
            archive_local,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=1,
        ) as zf:
            for f in sorted(output_dir.rglob("*")):
                if not f.is_file() or f == archive_local:
                    continue
                # Catalog-relative path — archive root is the catalog itself.
                arcname = str(f.relative_to(output_dir))
                zf.write(f, arcname=arcname)
                total_bytes += f.stat().st_size
                file_count += 1
        log.info(
            "Built archive: %d files, %d -> %d bytes in %.1fs",
            file_count, total_bytes, archive_local.stat().st_size, time.time() - t0,
        )

    archive_size = archive_local.stat().st_size
    for sink in pending:
        remote_path = f"{remote_base}/{_ARCHIVE_NAME}"
        sink.paths[_ARCHIVE_NAME] = remote_path
        log.info("  [%s] %s -> %s (%d bytes)", sink.name, _ARCHIVE_NAME, remote_path, archive_size)
        upload_from_local(
            pm,
            source_name=sink.name,
            pathkey=_ARCHIVE_NAME,
            local_path=archive_local,
            description=f"S3 upload {_ARCHIVE_NAME}",
        )
        marker.setdefault(sink.name, {})[_ARCHIVE_NAME] = time.time()
        marker_path.write_text(json.dumps(marker))
