import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config import ENERGY_RESULTS_SCHEMA  # noqa: E402
import logic  # noqa: E402


def rec(bid, ts, actual, baseline, anomaly=None):
    residual = actual - baseline
    if anomaly is None:
        anomaly = abs(residual) > baseline * 0.28
    return {
        "building_id": bid,
        "timestamp": pd.Timestamp(ts, tz="UTC"),
        "meter_reading": float(actual),
        "baseline": float(baseline),
        "residual": float(residual),
        "is_anomaly": anomaly,
    }


def frame(rows):
    return pd.DataFrame(rows, columns=[c for c, _ in ENERGY_RESULTS_SCHEMA])


# ---- prepare -------------------------------------------------------------

def test_prepare_dedupes_and_sorts():
    df = frame([
        rec("A", "2017-01-01 02:00", 100, 100),
        rec("A", "2017-01-01 01:00", 100, 100),
        rec("A", "2017-01-01 01:00", 110, 100),  # replay duplicate, later row wins
    ])
    out = logic.prepare(df)
    assert list(out["timestamp"].dt.hour) == [1, 2]
    assert out.iloc[0]["meter_reading"] == 110


# ---- null handling in prepare ---------------------------------------------

def test_prepare_residual_pct():
    out = logic.prepare(frame([rec("A", "2017-01-01 01:00", 130, 100), rec("B", "2017-01-01 01:00", 70, 100)]))
    assert list(out["residual_pct"]) == pytest.approx([30.0, -30.0])


@pytest.mark.parametrize("baseline", [0, -5, None, float("nan")])
def test_prepare_residual_pct_is_nan_for_unusable_baseline(baseline):
    row = rec("A", "2017-01-01 01:00", 10, 100)
    row["baseline"] = baseline
    out = logic.prepare(frame([row]))
    assert pd.isna(out.iloc[0]["residual_pct"])


@pytest.mark.parametrize("value", [None, pd.NA, float("nan")])
def test_prepare_null_is_anomaly_becomes_false(value):
    row = rec("A", "2017-01-01 01:00", 100, 100)
    row["is_anomaly"] = value
    out = logic.prepare(frame([row, rec("B", "2017-01-01 01:00", 300, 100)]))
    assert out["is_anomaly"].dtype == bool
    assert list(out["is_anomaly"]) == [False, True]


# ---- stale tracking and status --------------------------------------------

def latest_of(rows):
    return logic.latest_per_building(logic.prepare(frame(rows)))


def test_latest_per_building():
    latest = latest_of([
        rec("A", "2017-01-01 01:00", 100, 100),
        rec("A", "2017-01-01 02:00", 200, 100),
        rec("B", "2017-01-01 01:00", 100, 100),
    ])
    assert set(latest["building_id"]) == {"A", "B"}
    assert latest.set_index("building_id").loc["A", "meter_reading"] == 200


def test_first_sight_is_not_stale():
    latest = latest_of([rec("A", "2017-01-01 01:00", 100, 100)])
    seen = {}
    logic.update_seen(seen, latest, now=1000.0)
    table = logic.building_table(latest, seen, now=1000.0, stale_after=30)
    assert table.iloc[0]["status"] == "NORMAL"


def test_status_anomaly_normal():
    latest = latest_of([
        rec("A", "2017-01-01 01:00", 200, 100),
        rec("B", "2017-01-01 01:00", 100, 100),
    ])
    seen = {}
    logic.update_seen(seen, latest, now=0.0)
    table = logic.building_table(latest, seen, now=1.0, stale_after=30)
    status = dict(zip(table["building_id"], table["status"]))
    assert status == {"A": "ANOMALY", "B": "NORMAL"}


def test_stale_when_timestamp_does_not_advance():
    rows1 = [rec("A", "2017-01-01 01:00", 100, 100), rec("B", "2017-01-01 01:00", 100, 100)]
    seen = {}
    logic.update_seen(seen, latest_of(rows1), now=0.0)

    # B advances, A does not
    rows2 = rows1 + [rec("B", "2017-01-01 02:00", 100, 100)]
    latest2 = latest_of(rows2)
    logic.update_seen(seen, latest2, now=40.0)
    table = logic.building_table(latest2, seen, now=40.0, stale_after=30)
    status = dict(zip(table["building_id"], table["status"]))
    assert status == {"A": "STALE", "B": "NORMAL"}


def test_stale_recovers_when_new_data_arrives():
    seen = {}
    logic.update_seen(seen, latest_of([rec("A", "2017-01-01 01:00", 100, 100)]), now=0.0)
    latest = latest_of([
        rec("A", "2017-01-01 01:00", 100, 100),
        rec("A", "2017-01-01 02:00", 100, 100),
    ])
    logic.update_seen(seen, latest, now=100.0)
    table = logic.building_table(latest, seen, now=100.0, stale_after=30)
    assert table.iloc[0]["status"] == "NORMAL"


def test_stale_beats_anomaly():
    latest = latest_of([rec("A", "2017-01-01 01:00", 300, 100)])
    seen = {}
    logic.update_seen(seen, latest, now=0.0)
    table = logic.building_table(latest, seen, now=60.0, stale_after=30)
    assert table.iloc[0]["status"] == "STALE"


def test_table_sort_order_and_zero_baseline_note():
    latest = latest_of([
        rec("C", "2017-01-01 01:00", 100, 100),
        rec("B", "2017-01-01 01:00", 50, 0, anomaly=True),
        rec("A", "2017-01-01 01:00", 300, 100),
    ])
    seen = {}
    logic.update_seen(seen, latest, now=0.0)
    table = logic.building_table(latest, seen, now=0.0, stale_after=30)
    assert list(table["building_id"]) == ["A", "B", "C"]  # ANOMALY first, then id
    row_b = table.set_index("building_id").loc["B"]
    assert pd.isna(row_b["residual_pct"])
    assert row_b["note"] == "baseline <= 0"
    assert table.set_index("building_id").loc["A", "residual_pct"] == pytest.approx(200.0)


def test_summary_counts():
    latest = latest_of([
        rec("A", "2017-01-01 01:00", 300, 100),
        rec("B", "2017-01-01 01:00", 100, 100),
        rec("C", "2017-01-01 01:00", 100, 100),
    ])
    seen = {}
    logic.update_seen(seen, latest, now=0.0)
    table = logic.building_table(latest, seen, now=0.0, stale_after=30)
    assert logic.summary(table) == {"ANOMALY": 1, "NORMAL": 2, "STALE": 0}


# ---- chart and event log --------------------------------------------------

def test_chart_data_last_n_for_one_building():
    df = logic.prepare(frame(
        [rec("A", f"2017-01-01 {h:02d}:00", 100, 100) for h in range(10)]
        + [rec("B", "2017-01-01 00:00", 1, 1)]
    ))
    out = logic.chart_data(df, "A", 4)
    assert list(out["timestamp"].dt.hour) == [6, 7, 8, 9]
    assert set(out["building_id"]) == {"A"}


def test_event_log_newest_first_limited_and_filtered():
    df = logic.prepare(frame([
        rec("A", "2017-01-01 01:00", 300, 100),
        rec("A", "2017-01-01 02:00", 100, 100),
        rec("A", "2017-01-01 03:00", 300, 100),
        rec("B", "2017-01-01 04:00", 300, 100),
    ]))
    everything = logic.event_log(df, None, limit=10)
    assert list(everything["building_id"]) == ["B", "A", "A"]
    assert list(everything["timestamp"].dt.hour) == [4, 3, 1]
    assert len(logic.event_log(df, None, limit=2)) == 2
    only_a = logic.event_log(df, "A", limit=10)
    assert set(only_a["building_id"]) == {"A"}
    assert "residual_pct" in only_a.columns


def test_timestamp_going_backwards_after_restart_counts_as_new_data():
    """After reset_demo.sh the replay restarts at an older timestamp; that is a live feed, not STALE."""
    seen = {}
    logic.update_seen(seen, latest_of([rec("A", "2017-01-03 00:00", 100, 100)]), now=0.0)
    restarted = latest_of([rec("A", "2017-01-01 00:00", 100, 100)])
    logic.update_seen(seen, restarted, now=100.0)
    table = logic.building_table(restarted, seen, now=100.0, stale_after=30)
    assert table.iloc[0]["status"] == "NORMAL"


# ---- header, stale note, filter ---------------------------------------------

@pytest.mark.parametrize("seconds,text", [(5, "5 s"), (41, "41 s"), (89, "89 s"), (120, "2 min"), (7200, "2 h")])
def test_format_age(seconds, text):
    assert logic.format_age(seconds) == text


def test_stale_note_says_how_long():
    latest = latest_of([rec("A", "2017-01-01 01:00", 100, 100)])
    seen = {}
    logic.update_seen(seen, latest, now=0.0)
    table = logic.building_table(latest, seen, now=41.0, stale_after=30)
    assert table.iloc[0]["note"] == "no data for 41 s"


def test_data_age_is_time_since_any_building_got_new_data():
    assert logic.data_age({}, now=10.0) is None
    seen = {"A": (None, 0.0), "B": (None, 10.0)}
    assert logic.data_age(seen, now=50.0) == pytest.approx(40.0)


def test_filter_table():
    latest = latest_of([
        rec("A", "2017-01-01 01:00", 300, 100),
        rec("B", "2017-01-01 01:00", 100, 100),
    ])
    seen = {}
    logic.update_seen(seen, latest, now=0.0)
    table = logic.building_table(latest, seen, now=0.0, stale_after=30)
    assert list(logic.filter_table(table, "ANOMALY")["building_id"]) == ["A"]
    assert list(logic.filter_table(table, "NORMAL")["building_id"]) == ["B"]
    assert len(logic.filter_table(table, "ALL")) == 2
