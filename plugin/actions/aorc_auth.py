"""Authenticate ``stormhub.met.aorc.aorc.valid_spaces_item`` against our
private AORC cache.

Upstream stormhub hard-codes ``s3fs.S3FileSystem(anon=True)`` for the
sample-window read inside ``valid_spaces_item``. That works against NOAA's
public bucket but 403s against our private ``s3://storm-cloud/aorc-cache-conus``,
so a fresh ``new_catalog`` call (Whitehorse, Christmas_Valley, anything
without a cached ``catalog.json``) crashes during init.

When ``AORC_S3_KEY`` is set, install a patched ``valid_spaces_item`` that
builds an authenticated s3fs. Identical to upstream otherwise — same
sample window, same Transpose computation, same return type.

``install()`` is idempotent and a no-op when no creds are present (anon
still works for NOAA public).
"""

from __future__ import annotations

import os

_original = None


def install() -> None:
    global _original
    if _original is not None:
        return
    if not os.environ.get("AORC_S3_KEY"):
        return

    import datetime as _dt

    import s3fs
    import xarray as xr
    from shapely.geometry import shape

    import stormhub.met.aorc.aorc as _aorc_mod
    from stormhub.met.aorc.aorc import (
        AORC_PRECIP_VARIABLE,
        AORC_X_VAR,
        AORC_Y_VAR,
        Transpose,
    )
    from stormhub.met.consts import NOAA_AORC_S3_BASE_URL

    _original = _aorc_mod.valid_spaces_item

    def patched(watershed, transposition_region, storm_duration: int = 72):
        s3 = s3fs.S3FileSystem(
            anon=False,
            key=os.environ["AORC_S3_KEY"],
            secret=os.environ["AORC_S3_SECRET"],
            endpoint_url=os.environ.get("AORC_S3_ENDPOINT"),
            config_kwargs={"max_pool_connections": 50},
        )
        start_time = _dt.datetime(1980, 5, 1)
        sample_data = s3fs.S3Map(
            root=f"{NOAA_AORC_S3_BASE_URL}/{start_time.year}.zarr", s3=s3
        )
        ds = xr.open_dataset(
            sample_data, engine="zarr", chunks="auto", consolidated=True
        )
        bounds = shape(transposition_region.geometry).bounds
        subset = ds.sel(
            time=slice(start_time, start_time + _dt.timedelta(hours=storm_duration)),
            longitude=slice(bounds[0], bounds[2]),
            latitude=slice(bounds[1], bounds[3]),
        )
        clipped = subset.rio.clip(
            [shape(transposition_region.geometry)], drop=True, all_touched=True
        )
        transpose = Transpose(
            clipped[AORC_PRECIP_VARIABLE].sum(dim="time", skipna=True, min_count=1),
            shape(watershed.geometry),
            AORC_X_VAR,
            AORC_Y_VAR,
        )
        return transpose.valid_spaces_polygon

    _aorc_mod.valid_spaces_item = patched
    # storm_catalog.py imports the symbol by name, not module attribute,
    # so we have to patch its local reference too.
    import stormhub.met.storm_catalog as _sc_mod

    if getattr(_sc_mod, "valid_spaces_item", None) is _original:
        _sc_mod.valid_spaces_item = patched


def restore() -> None:
    global _original
    if _original is None:
        return
    import stormhub.met.aorc.aorc as _aorc_mod
    import stormhub.met.storm_catalog as _sc_mod

    _aorc_mod.valid_spaces_item = _original
    if getattr(_sc_mod, "valid_spaces_item", None) is not _original:
        _sc_mod.valid_spaces_item = _original
    _original = None
