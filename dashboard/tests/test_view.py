import json
import re

import pandas as pd
import pytest

from common.config import ANOMALY_PERSISTENCE_HOURS, DASHBOARD_REFRESH_SEC
from dashboard import logic, view
from dashboard.tests.support import EVIL, first_row, make_table, rec

A = ("A", "electricity")
THRESHOLDS = pd.DataFrame(
    {"building_id": ["A"], "meter_type": ["electricity"], "anomaly_threshold": [0.5]}
)


def table_rows(html, status):
    """Number of table rows with this status (the panel also carries data-status)."""
    return len(re.findall(rf'class="bdg-tr[^"]*" data-status="{status}"', html))


def test_data_is_escaped_everywhere():
    prepared, table = make_table([rec(EVIL, "2017-01-01 01:00", 300, 100, site=EVIL)])
    events = logic.event_log(prepared, None, 10)
    selected = (EVIL, "electricity")
    for out in (
        view.meters_html(table, selected, 1),
        view.sites_html(logic.site_summary(table)),
        view.panel_html(first_row(table, EVIL)),
        view.events_html(events, selected),
    ):
        assert EVIL not in out
        assert "&lt;img" in out


def test_waiting_path_is_escaped():
    assert "<script>" not in view.waiting_html("<script>x</script>")


def test_meter_table_shows_status_chips_na_and_stale_note():
    rows = [
        rec("A", "2017-01-01 01:00", 300, 100),
        rec("B", "2017-01-01 01:00", 5, 0, anomaly=True),
        rec("C", "2017-01-01 01:00", 100, 100),
    ]
    # everything is stale after 45 s: stale rows show "-" and how long
    _, stale = make_table(rows, advance=45)
    out = view.meters_html(stale, A, 3)
    assert table_rows(out, "STALE") == 3
    assert "no data for 45 s" in out
    assert "<b>-</b>" in out

    _, fresh = make_table(rows)
    out = view.meters_html(fresh, A, 3)
    assert table_rows(out, "ANOMALY") == 2
    assert table_rows(out, "NORMAL") == 1
    assert "<i>n/a</i>" in out
    assert "baseline &lt;= 0" in out
    assert "+200.0%" in out


def test_meter_table_says_top_n_of_total():
    _, table = make_table([rec(b, "2017-01-01 01:00", 100, 100) for b in "ABCDE"])
    assert "Top 2 of 5" in view.meters_html(table.head(2), A, 5)


def test_residual_bar_band_is_the_meters_threshold():
    _, table = make_table([rec("A", "2017-01-01 01:00", 100, 100)], thresholds=THRESHOLDS)
    # 50% of a 24 px half-bar: the band spans 12 px on each side of the centre
    assert "left:12.0px;width:24.0px" in view.meters_html(table, A, 1)
    assert "±50% of baseline" in view.legend_html(168, 50.0)


def test_sites_table_rows():
    _, table = make_table(
        [rec("A", "2017-01-01 01:00", 300, 100, site="Rat"), rec("B", "2017-01-01 01:00", 1, 1)]
    )
    out = view.sites_html(logic.site_summary(table))
    assert table_rows(out, "ANOMALY") == 1
    assert table_rows(out, "NORMAL") == 1
    assert out.index("Rat") < out.index("S1")


def test_header_states():
    counts = {"ANOMALY": 2, "NORMAL": 6, "STALE": 0}
    ts = pd.Timestamp("2017-01-02 15:00", tz="UTC")
    live = view.header_html("live", counts, ts, "")
    assert "needs attention" in live and f"refreshes every {DASHBOARD_REFRESH_SEC} s" in live
    assert "2017-01-02" in live and "15:00" in live
    assert f"for {ANOMALY_PERSISTENCE_HOURS} h in a row" in live
    assert "Data source stopped sending" not in live

    calm = view.header_html("live", {"ANOMALY": 0, "NORMAL": 8, "STALE": 0}, ts, "")
    assert "none right now" in calm

    stopped = view.header_html("allstale", {"ANOMALY": 0, "NORMAL": 0, "STALE": 8}, ts, "41 s")
    assert "Data source stopped sending" in stopped
    assert "All 8 meters" in stopped
    assert "No new data for 41 s" in stopped

    waiting = view.header_html("waiting", {"ANOMALY": 0, "NORMAL": 0, "STALE": 0}, None, "")
    assert "No data yet" in waiting
    assert "Data source stopped sending" not in waiting


def test_header_shows_stream_metrics():
    ts = pd.Timestamp("2017-01-02 15:00", tz="UTC")
    counts = {"ANOMALY": 0, "NORMAL": 1, "STALE": 0}
    metrics = {"rows_per_sec": 380.4, "max_latency_sec": 7.14}
    assert "380 rows/s · latency 7.1 s" in view.header_html("live", counts, ts, "", metrics)
    assert "Replayed 2017 history" in view.header_html("live", counts, ts, "")


def test_panel_labels_depend_on_status():
    _, fresh = make_table([rec("A", "2017-01-01 01:00", 300, 100)], thresholds=THRESHOLDS)
    panel = view.panel_html(first_row(fresh, "A"))
    assert "As of (UTC)" in panel and "±50%" in panel and "electricity meter" in panel
    _, stale = make_table([rec("A", "2017-01-01 01:00", 300, 100)], advance=60)
    assert "Last record (UTC)" in view.panel_html(first_row(stale, "A"))


def test_events_html_empty():
    prepared, _ = make_table([rec("A", "2017-01-01 01:00", 100, 100)])
    assert "No anomaly events." in view.events_html(logic.event_log(prepared, None, 10), A)


def test_sgn_uses_real_minus():
    assert view.sgn(-6.5) == "−6.5"
    assert view.sgn(0) == "+0.0"


def chart_of(rows, stale, thresholds=None):
    prepared, _ = make_table(rows, thresholds=thresholds)
    return view.chart(prepared, stale=stale).to_dict()


def test_chart_marks_end_of_data_only_for_stale():
    rows = [rec("A", f"2017-01-01 {h:02d}:00", 300 if h > 4 else 100, 100) for h in range(8)]
    assert "no data after this" in json.dumps(chart_of(rows, True))
    assert "no data after this" not in json.dumps(chart_of(rows, False))


def test_chart_tooltip_is_utc_text_and_band_uses_meter_threshold():
    spec = chart_of([rec("A", "2017-01-01 01:00", 100, 100)], False, THRESHOLDS)
    first = next(iter(spec["datasets"].values()))[0]
    assert first["lo"] == pytest.approx(50.0)
    assert first["hi"] == pytest.approx(150.0)
    assert first["Time (UTC)"] == "2017-01-01 01:00"
