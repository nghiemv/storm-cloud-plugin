"""Unit tests for plugin.lib.storm_rank and dss_filename naming.

Regression guard for the convert-to-dss mislabeling bug: DSS files must be
named by the storm's true catalog rank (por_rank, encoded as the STAC item id),
not by the enumeration position of get_all_items(), which is not rank-sorted.
"""

from __future__ import annotations

from datetime import datetime

from plugin.lib import dss_filename, storm_rank


class _Item:
    def __init__(self, item_id):
        self.id = item_id


def test_storm_rank_uses_item_id_not_position():
    # get_all_items() order is arbitrary; the 3rd item can be por_rank 441.
    item = _Item("441")
    assert storm_rank(item, fallback=3) == 441


def test_storm_rank_falls_back_for_non_numeric_id():
    # storm_search uses a %Y-%m-%dT%H id when searched without a rank.
    item = _Item("1982-06-07T06")
    assert storm_rank(item, fallback=7) == 7


def test_storm_rank_falls_back_for_none_id():
    assert storm_rank(_Item(None), fallback=12) == 12


def test_dss_filename_encodes_true_rank():
    # por_rank 441 starting 1982-06-07 -> r441, never r003.
    name = dss_filename(datetime(1982, 6, 7, 6), storm_rank(_Item("441"), 3), 72)
    assert name == "19820607_72hr_st1_r441.dss"
