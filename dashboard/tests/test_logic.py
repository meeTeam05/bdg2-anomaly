import pandas as pd
import pytest
from deltalake import DeltaTable

from common.config import THRESHOLD_RATIO
from dashboard import logic
from dashboard.mock_stream import write_batch
from dashboard.tests.support import frame, latest_of, make_table, rec, status_of

# ---- prepare -------------------------------------------------------------------------------


def test_prepare_dedupes_and_sorts():
    df = frame(
        [
            rec("A", "2017-01-01 02:00", 100, 100),
            rec("A", "2017-01-01 01:00", 100, 100),
            rec("A", "2017-01-01 01:00", 110, 100),  # replay duplicate, later row wins
        ]
    )
    out = logic.prepare(df)
    assert list(out["timestamp"].dt.hour) == [1, 2]
    assert out.iloc[0]["meter_reading"] == 110


def test_prepare_keeps_meter_types_apart():
    out = logic.prepare(
        frame(
            [
                rec("A", "2017-01-01 01:00", 100, 100),
                rec("A", "2017-01-01 01:00", 7, 5, meter_type="gas"),
            ]
        )
    )
    assert len(out) == 2
    assert len(logic.latest_per_meter(out)) == 2


def test_prepare_residual_pct():
    out = logic.prepare(
        frame([rec("A", "2017-01-01 01:00", 130, 100), rec("B", "2017-01-01 01:00", 70, 100)])
    )
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


def test_prepare_attaches_per_meter_threshold_with_fallback():
    thresholds = pd.DataFrame(
        {"building_id": ["A"], "meter_type": ["electricity"], "anomaly_threshold": [0.5]}
    )
    out = logic.prepare(
        frame([rec("A", "2017-01-01 01:00", 100, 100), rec("B", "2017-01-01 01:00", 100, 100)]),
        thresholds,
    )
    assert list(out["threshold_pct"]) == pytest.approx([50.0, THRESHOLD_RATIO * 100])
    assert list(logic.prepare(frame([rec("A", "2017-01-01 01:00", 1, 1)]))["threshold_pct"]) == [
        pytest.approx(THRESHOLD_RATIO * 100)
    ]


# ---- stale tracking and status -------------------------------------------------------------


def test_latest_per_meter():
    latest = latest_of(
        [
            rec("A", "2017-01-01 01:00", 100, 100),
            rec("A", "2017-01-01 02:00", 200, 100),
            rec("B", "2017-01-01 01:00", 100, 100),
        ]
    )
    assert set(latest["building_id"]) == {"A", "B"}
    assert latest.set_index("building_id").loc["A", "meter_reading"] == 200


def test_first_sight_is_not_stale():
    _, table = make_table([rec("A", "2017-01-01 01:00", 100, 100)], now=1000.0)
    assert table.iloc[0]["status"] == "NORMAL"


def test_status_anomaly_normal():
    _, table = make_table(
        [rec("A", "2017-01-01 01:00", 200, 100), rec("B", "2017-01-01 01:00", 100, 100)],
        advance=1.0,
    )
    assert status_of(table) == {"A": "ANOMALY", "B": "NORMAL"}


def test_stale_when_timestamp_does_not_advance():
    rows1 = [rec("A", "2017-01-01 01:00", 100, 100), rec("B", "2017-01-01 01:00", 100, 100)]
    seen = {}
    logic.update_seen(seen, latest_of(rows1), now=0.0)

    # B advances, A does not
    latest2 = latest_of([*rows1, rec("B", "2017-01-01 02:00", 100, 100)])
    logic.update_seen(seen, latest2, now=40.0)
    table = logic.meter_table(latest2, seen, now=40.0, stale_after=30)
    assert status_of(table) == {"A": "STALE", "B": "NORMAL"}


def test_stale_recovers_when_new_data_arrives():
    seen = {}
    logic.update_seen(seen, latest_of([rec("A", "2017-01-01 01:00", 100, 100)]), now=0.0)
    latest = latest_of(
        [rec("A", "2017-01-01 01:00", 100, 100), rec("A", "2017-01-01 02:00", 100, 100)]
    )
    logic.update_seen(seen, latest, now=100.0)
    table = logic.meter_table(latest, seen, now=100.0, stale_after=30)
    assert table.iloc[0]["status"] == "NORMAL"


def test_stale_beats_anomaly():
    _, table = make_table([rec("A", "2017-01-01 01:00", 300, 100)], advance=60.0)
    assert table.iloc[0]["status"] == "STALE"


def test_timestamp_going_backwards_after_restart_counts_as_new_data():
    """After `make reset` the replay restarts at an older timestamp: a live feed, not STALE."""
    seen = {}
    logic.update_seen(seen, latest_of([rec("A", "2017-01-03 00:00", 100, 100)]), now=0.0)
    restarted = latest_of([rec("A", "2017-01-01 00:00", 100, 100)])
    logic.update_seen(seen, restarted, now=100.0)
    table = logic.meter_table(restarted, seen, now=100.0, stale_after=30)
    assert table.iloc[0]["status"] == "NORMAL"


# ---- meter table: Top-N order, notes, filters, sites ---------------------------------------


def test_table_sort_order_and_zero_baseline_note():
    _, table = make_table(
        [
            rec("C", "2017-01-01 01:00", 100, 100),
            rec("B", "2017-01-01 01:00", 50, 0, anomaly=True),
            rec("A", "2017-01-01 01:00", 300, 100),
            rec("D", "2017-01-01 01:00", 900, 100),
        ]
    )
    # ANOMALY first, the largest |residual %| first, unusable baseline last in its status
    assert list(table["building_id"]) == ["D", "A", "B", "C"]
    row_b = table.set_index("building_id").loc["B"]
    assert pd.isna(row_b["residual_pct"])
    assert row_b["note"] == "baseline <= 0"
    assert table.set_index("building_id").loc["A", "residual_pct"] == pytest.approx(200.0)


def test_summary_counts():
    _, table = make_table(
        [
            rec("A", "2017-01-01 01:00", 300, 100),
            rec("B", "2017-01-01 01:00", 100, 100),
            rec("C", "2017-01-01 01:00", 100, 100),
        ]
    )
    assert logic.summary(table) == {"ANOMALY": 1, "NORMAL": 2, "STALE": 0}


def test_site_summary_puts_sites_with_anomalies_first():
    _, table = make_table(
        [
            rec("A", "2017-01-01 01:00", 100, 100, site="Fox"),
            rec("B", "2017-01-01 01:00", 300, 100, site="Rat"),
            rec("C", "2017-01-01 01:00", 100, 100, site="Rat"),
        ]
    )
    sites = logic.site_summary(table)
    assert list(sites["site_id"]) == ["Rat", "Fox"]
    rat = sites.iloc[0]
    assert (rat["meters"], rat["ANOMALY"], rat["STALE"], rat["NORMAL"]) == (2, 1, 0, 1)


def test_filter_table_by_status_and_site():
    _, table = make_table(
        [
            rec("A", "2017-01-01 01:00", 300, 100, site="Fox"),
            rec("B", "2017-01-01 01:00", 100, 100, site="Fox"),
            rec("C", "2017-01-01 01:00", 300, 100, site="Rat"),
        ]
    )
    assert list(logic.filter_table(table, "ANOMALY")["building_id"]) == ["A", "C"]
    assert list(logic.filter_table(table, "NORMAL")["building_id"]) == ["B"]
    assert list(logic.filter_table(table, "ANOMALY", "Rat")["building_id"]) == ["C"]
    assert len(logic.filter_table(table, "ALL")) == 3


@pytest.mark.parametrize(
    "seconds,text", [(5, "5 s"), (41, "41 s"), (89, "89 s"), (120, "2 min"), (7200, "2 h")]
)
def test_format_age(seconds, text):
    assert logic.format_age(seconds) == text


def test_stale_note_says_how_long():
    _, table = make_table([rec("A", "2017-01-01 01:00", 100, 100)], advance=41.0)
    assert table.iloc[0]["note"] == "no data for 41 s"


def test_data_age_is_time_since_any_meter_got_new_data():
    assert logic.data_age({}, now=10.0) is None
    seen = {("A", "electricity"): (None, 0.0), ("B", "electricity"): (None, 10.0)}
    assert logic.data_age(seen, now=50.0) == pytest.approx(40.0)


# ---- chart and event log -------------------------------------------------------------------


def test_meter_rows_for_one_meter():
    df = logic.prepare(
        frame(
            [rec("A", f"2017-01-01 {h:02d}:00", 100, 100) for h in range(10)]
            + [
                rec("B", "2017-01-01 00:00", 1, 1),
                rec("A", "2017-01-01 00:00", 1, 1, None, "S1", "gas"),
            ]
        )
    )
    out = logic.meter_rows(df, ("A", "electricity"))
    assert list(out["timestamp"].dt.hour) == list(range(10))
    assert set(out["meter_type"]) == {"electricity"}


def test_event_log_newest_first_limited_and_filtered():
    df = logic.prepare(
        frame(
            [
                rec("A", "2017-01-01 01:00", 300, 100),
                rec("A", "2017-01-01 02:00", 100, 100),
                rec("A", "2017-01-01 03:00", 300, 100),
                rec("B", "2017-01-01 04:00", 300, 100),
            ]
        )
    )
    everything = logic.event_log(df, None, limit=10)
    assert list(everything["building_id"]) == ["B", "A", "A"]
    assert list(everything["timestamp"].dt.hour) == [4, 3, 1]
    assert len(logic.event_log(df, None, limit=2)) == 2
    only_a = logic.event_log(df, ("A", "electricity"), limit=10)
    assert set(only_a["building_id"]) == {"A"}
    assert {"residual_pct", "site_id"} <= set(only_a.columns)


# ---- reading Delta -------------------------------------------------------------------------


def test_read_window_only_keeps_the_last_hours(tmp_path):
    path = tmp_path / "energy_results"
    for h in range(10):  # one Delta file per hour, like one micro-batch per tick
        write_batch(path, frame([rec("A", f"2017-01-01 {h:02d}:00", 100, 100)]))
    out = logic.read_window(str(path), hours=3)
    assert sorted(out["timestamp"].dt.hour) == [7, 8, 9]
    newest = logic.latest_timestamp(DeltaTable(str(path)))
    assert newest == pd.Timestamp("2017-01-01 09:00", tz="UTC")


def test_readers_handle_missing_inputs(tmp_path):
    missing = str(tmp_path / "nope")
    assert logic.table_key(missing) is None
    assert logic.read_window(missing, 24) is None
    assert logic.read_thresholds(missing) is None
    assert logic.read_metrics(tmp_path / "batch_metrics.csv") is None


def test_read_metrics_returns_last_batch(tmp_path):
    path = tmp_path / "batch_metrics.csv"
    path.write_text("batch_id,rows_per_sec,max_latency_sec\n1,100.0,5.0\n2,380.5,7.1\n")
    assert logic.read_metrics(path) == {
        "batch_id": 2,
        "rows_per_sec": 380.5,
        "max_latency_sec": 7.1,
    }
