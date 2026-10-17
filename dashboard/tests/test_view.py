import json
import re
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import logic  # noqa: E402
import view  # noqa: E402
from config import DASHBOARD_REFRESH_SEC, ENERGY_RESULTS_SCHEMA, THRESHOLD_RATIO  # noqa: E402

EVIL = "<img src=x onerror=alert(1)>"


def table_rows(html, status):
    """Number of building-table rows with this status (the panel also carries data-status)."""
    return len(re.findall(rf'class="bdg-tr[^"]*" data-status="{status}"', html))


def rec(bid, ts, actual, baseline, anomaly=None):
    residual = actual - baseline
    if anomaly is None:
        anomaly = abs(residual) > baseline * THRESHOLD_RATIO
    return {
        "building_id": bid, "timestamp": pd.Timestamp(ts, tz="UTC"), "meter_reading": float(actual),
        "baseline": float(baseline), "residual": float(residual), "is_anomaly": anomaly,
    }


def make_table(rows, now=0.0, advance=0.0):
    prepared = logic.prepare(pd.DataFrame(rows, columns=[c for c, _ in ENERGY_RESULTS_SCHEMA]))
    latest = logic.latest_per_building(prepared)
    seen = {}
    logic.update_seen(seen, latest, now)
    return prepared, logic.building_table(latest, seen, now + advance, stale_after=30)


def first_row(table, bid):
    return next(table[table["building_id"] == bid].itertuples(index=False))


def test_building_ids_are_escaped_everywhere():
    prepared, table = make_table([rec(EVIL, "2017-01-01 01:00", 300, 100)])
    events = logic.event_log(prepared, None, 10)
    for out in (
        view.buildings_html(table, EVIL),
        view.panel_html(first_row(table, EVIL)),
        view.events_html(events, EVIL),
    ):
        assert EVIL not in out
        assert "&lt;img" in out


def test_waiting_path_is_escaped():
    assert "<script>" not in view.waiting_html("<script>x</script>")


def test_table_shows_status_chips_na_and_stale_note():
    prepared, table = make_table([
        rec("A", "2017-01-01 01:00", 300, 100),
        rec("B", "2017-01-01 01:00", 5, 0, anomaly=True),
        rec("C", "2017-01-01 01:00", 100, 100),
    ], advance=45)
    # everything is stale after 45 s: stale rows show "-" and how long
    out = view.buildings_html(table, "A")
    assert table_rows(out, "STALE") == 3
    assert "no data for 45 s" in out
    assert "<b>-</b>" in out

    _, fresh = make_table([
        rec("A", "2017-01-01 01:00", 300, 100),
        rec("B", "2017-01-01 01:00", 5, 0, anomaly=True),
        rec("C", "2017-01-01 01:00", 100, 100),
    ])
    out = view.buildings_html(fresh, "A")
    assert table_rows(out, "ANOMALY") == 2
    assert table_rows(out, "NORMAL") == 1
    assert "<i>n/a</i>" in out
    assert "baseline &lt;= 0" in out
    assert "+200.0%" in out


def test_band_in_legend_and_table_uses_config_ratio():
    _, table = make_table([rec("A", "2017-01-01 01:00", 100, 100)])
    assert f"±{THRESHOLD_RATIO * 100:g}% of baseline" in view.buildings_html(table, "A")
    assert f"±{THRESHOLD_RATIO * 100:g}% of baseline" in view.legend_html(200)


def test_header_states():
    counts = {"ANOMALY": 2, "NORMAL": 6, "STALE": 0}
    ts = pd.Timestamp("2017-01-02 15:00", tz="UTC")
    live = view.header_html("live", counts, ts, "")
    assert "needs attention" in live and "Live" in live and f"refreshes every {DASHBOARD_REFRESH_SEC} s" in live
    assert "2017-01-02" in live and "15:00" in live
    assert f"&gt; {THRESHOLD_RATIO * 100:g}%" in live
    assert "Data source stopped sending" not in live

    calm = view.header_html("live", {"ANOMALY": 0, "NORMAL": 8, "STALE": 0}, ts, "")
    assert "none right now" in calm

    stopped = view.header_html("allstale", {"ANOMALY": 0, "NORMAL": 0, "STALE": 8}, ts, "41 s")
    assert "Data source stopped sending" in stopped
    assert "All 8 buildings" in stopped
    assert "No new data for 41 s" in stopped

    waiting = view.header_html("waiting", {"ANOMALY": 0, "NORMAL": 0, "STALE": 0}, None, "")
    assert "No data yet" in waiting
    assert "Data source stopped sending" not in waiting


def test_panel_labels_depend_on_status():
    _, fresh = make_table([rec("A", "2017-01-01 01:00", 300, 100)])
    assert "As of (UTC)" in view.panel_html(first_row(fresh, "A"))
    _, stale = make_table([rec("A", "2017-01-01 01:00", 300, 100)], advance=60)
    assert "Last record (UTC)" in view.panel_html(first_row(stale, "A"))


def test_events_html_empty():
    prepared, _ = make_table([rec("A", "2017-01-01 01:00", 100, 100)])
    assert "No anomaly events." in view.events_html(logic.event_log(prepared, None, 10), "A")


def test_sgn_uses_real_minus():
    assert view.sgn(-6.5) == "−6.5"
    assert view.sgn(0) == "+0.0"


def chart_spec(stale):
    prepared, _ = make_table([
        rec("A", f"2017-01-01 {h:02d}:00", 300 if h > 4 else 100, 100) for h in range(8)
    ])
    return json.dumps(view.chart(prepared, stale=stale).to_dict())


def test_chart_marks_end_of_data_only_for_stale():
    assert "no data after this" in chart_spec(True)
    assert "no data after this" not in chart_spec(False)


def test_chart_tooltip_is_utc_text_and_band_uses_ratio():
    spec = chart_spec(False)
    assert "Time (UTC)" in spec and "1.28" not in spec  # band values are data columns, not constants
    prepared, _ = make_table([rec("A", "2017-01-01 01:00", 100, 100)])
    data = view.chart(prepared, stale=False).to_dict()["datasets"]
    rows = next(iter(data.values()))
    assert rows[0]["lo"] == pytest.approx(100 * (1 - THRESHOLD_RATIO))
    assert rows[0]["hi"] == pytest.approx(100 * (1 + THRESHOLD_RATIO))
    assert rows[0]["Time (UTC)"] == "2017-01-01 01:00"
