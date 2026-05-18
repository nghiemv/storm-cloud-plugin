#!/usr/bin/env python3
"""Mirror AORC zarr data from NOAA public S3 to a private S3 cache.

Clips to the union bounding box of the Indian Creek and Kanawha watersheds
and retains only APCP_surface and TMP_2maboveground (the two variables used
by stormhub).  Run once; subsequent runs skip years that already exist.

Required env vars (mirror profile credentials):
  MIRROR_AWS_ACCESS_KEY_ID
  MIRROR_AWS_SECRET_ACCESS_KEY
  MIRROR_AWS_ENDPOINT           (default: https://s3.hecdev.net)

Usage:
  python dev/mirror_aorc.py
  python dev/mirror_aorc.py --year-start 2000 --year-end 2005
  python dev/mirror_aorc.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import s3fs
import xarray as xr

log = logging.getLogger(__name__)

# Union bounding box of Indian Creek + Kanawha watersheds with 0.5-deg buffer
LON_MIN = -97.2
LON_MAX = -77.4
LAT_MIN = 33.3
LAT_MAX = 44.1

VARIABLES = ["APCP_surface", "TMP_2maboveground"]

NOAA_BASE = "s3://noaa-nws-aorc-v1-1-1km"
YEAR_START = 1979
YEAR_END = 2024


def _noaa_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(anon=True, config_kwargs={"max_pool_connections": 50})


def _dest_fs(key: str, secret: str, endpoint: str) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        anon=False,
        key=key,
        secret=secret,
        endpoint_url=endpoint,
        config_kwargs={"max_pool_connections": 20},
    )


def _year_done(fs: s3fs.S3FileSystem, dest_path: str) -> bool:
    """Consolidated zarr marker means the write completed cleanly."""
    check = dest_path.replace("s3://", "") + "/.zmetadata"
    return fs.exists(check)


def mirror_year(
    year: int,
    src_fs: s3fs.S3FileSystem,
    dst_fs: s3fs.S3FileSystem,
    dest_base: str,
    dry_run: bool,
) -> None:
    src_path = f"{NOAA_BASE}/{year}.zarr"
    dst_path = f"{dest_base}/{year}.zarr"

    if _year_done(dst_fs, dst_path):
        log.info("[%d] already cached — skip", year)
        return

    log.info("[%d] opening %s", year, src_path)
    src_store = s3fs.S3Map(root=src_path, s3=src_fs, check=False)
    ds = xr.open_dataset(src_store, engine="zarr", chunks="auto", consolidated=True)

    ds = ds[VARIABLES]
    ds = ds.sel(longitude=slice(LON_MIN, LON_MAX), latitude=slice(LAT_MIN, LAT_MAX))

    # Rechunk: full clipped spatial extent per chunk, 24-hour time slices
    n_lat = ds.sizes.get("latitude", 1)
    n_lon = ds.sizes.get("longitude", 1)
    ds = ds.chunk({"time": 24, "latitude": n_lat, "longitude": n_lon})

    log.info(
        "[%d] clipped dims: time=%d lat=%d lon=%d",
        year,
        ds.sizes.get("time", 0),
        n_lat,
        n_lon,
    )

    if dry_run:
        log.info("[%d] dry-run — skipping write", year)
        return

    dst_store = s3fs.S3Map(root=dst_path, s3=dst_fs, check=False)
    log.info("[%d] writing to %s", year, dst_path)
    ds.to_zarr(dst_store, mode="w", consolidated=True)
    log.info("[%d] done", year)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--year-start", type=int, default=YEAR_START, metavar="YEAR")
    parser.add_argument("--year-end", type=int, default=YEAR_END, metavar="YEAR")
    parser.add_argument("--dest-base", default="s3://storm-cloud/aorc-cache", metavar="S3_URI")
    parser.add_argument("--dry-run", action="store_true", help="plan without writing")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )

    key = os.environ["MIRROR_AWS_ACCESS_KEY_ID"]
    secret = os.environ["MIRROR_AWS_SECRET_ACCESS_KEY"]
    endpoint = os.environ.get("MIRROR_AWS_ENDPOINT", "https://s3.hecdev.net")

    src_fs = _noaa_fs()
    dst_fs = _dest_fs(key, secret, endpoint)

    failed: list[int] = []
    for year in range(args.year_start, args.year_end + 1):
        try:
            mirror_year(year, src_fs, dst_fs, args.dest_base, args.dry_run)
        except Exception:
            log.exception("[%d] failed", year)
            failed.append(year)

    if failed:
        log.error("failed years: %s", failed)
        sys.exit(1)

    log.info("mirror complete (%d–%d)", args.year_start, args.year_end)


if __name__ == "__main__":
    main()
