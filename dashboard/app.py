"""Task 4 dashboard: reads the Task 3 Gold table and shows sites, the most urgent meters,
one meter's actual vs baseline, and the anomaly event log.

    streamlit run dashboard/app.py          # from the repo root, http://localhost:8501
    ENERGY_RESULTS_PATH=<path> streamlit run dashboard/app.py

Each refresh reads only the last DASHBOARD_WINDOW_HOURS of replayed time, and only when the
Delta table has a new version.
"""

import os
import time
from pathlib import Path

import streamlit as st

from common.config import (
    DASHBOARD_REFRESH_SEC,
    DASHBOARD_WINDOW_HOURS,
    ENERGY_RESULTS_PATH,
    EVENT_LOG_LIMIT,
    METER_THRESHOLDS_PATH,
    STALE_AFTER_SEC,
    TOP_N,
)
from dashboard import logic, view

RESULTS_PATH = os.environ.get("ENERGY_RESULTS_PATH", ENERGY_RESULTS_PATH)
THRESHOLDS_PATH = os.environ.get("METER_THRESHOLDS_PATH", METER_THRESHOLDS_PATH)
METRICS_PATH = Path(RESULTS_PATH).parent / "batch_metrics.csv"  # written by streaming_job.py
EMPTY_COUNTS = {"ANOMALY": 0, "NORMAL": 0, "STALE": 0}
FILTERS = ["ALL", "ANOMALY", "STALE", "NORMAL"]
ALL_SITES = "All sites"


@st.cache_resource
def _seen():
    """Shared by all sessions: {meter: (newest timestamp, wall clock when first seen)}."""
    return {}


@st.cache_data(show_spinner=False, max_entries=1)
def _thresholds(path):
    return logic.read_thresholds(path)


@st.cache_data(show_spinner=False, max_entries=2)
def _load(path, key):
    """Re-read only when the table or its version changed (key = logic.table_key)."""
    rows = logic.read_window(path, DASHBOARD_WINDOW_HOURS)
    return logic.prepare(rows, _thresholds(THRESHOLDS_PATH))


def _meter_label(meter, status_of, several_types):
    building_id, meter_type = meter
    name = f"{building_id} · {meter_type}" if several_types else building_id
    return f"{name}  ·  {status_of[meter]}"


@st.fragment(run_every=DASHBOARD_REFRESH_SEC)
def live():
    key = logic.table_key(RESULTS_PATH)
    prepared = _load(RESULTS_PATH, key) if key is not None else None
    if prepared is None or prepared.empty:
        st.html(view.header_html("waiting", EMPTY_COUNTS, None, ""))
        st.html(view.waiting_html(RESULTS_PATH))
        return

    now = time.time()
    latest = logic.latest_per_meter(prepared)
    seen = _seen()
    logic.update_seen(seen, latest, now)
    table = logic.meter_table(latest, seen, now, STALE_AFTER_SEC)
    counts = logic.summary(table)
    kind = "allstale" if counts["STALE"] == len(table) else "live"
    age = logic.data_age(seen, now)
    metrics = logic.read_metrics(METRICS_PATH)
    st.html(
        view.header_html(kind, counts, prepared["timestamp"].max(), logic.format_age(age), metrics)
    )

    meters = logic.meter_ids(table)
    status_of = dict(zip(meters, table["status"], strict=True))
    site_of = dict(zip(meters, table["site_id"], strict=True))
    sites = sorted(set(table["site_id"]))
    several_types = table["meter_type"].nunique() > 1
    # First render: open the most urgent meter (the table is sorted ANOMALY first).
    if st.session_state.get("meter") not in meters:
        st.session_state["meter"] = meters[0]
        st.session_state["site"] = site_of[meters[0]]
    selected = st.session_state["meter"]

    left, right = st.columns([1, 2.4], gap="medium")
    with left, st.container(border=True):
        st.subheader("Sites")
        st.html(view.sites_html(logic.site_summary(table)))
    with right, st.container(border=True):
        st.subheader(f"Top {TOP_N} meters")
        status_col, site_col = st.columns([3, 2], vertical_alignment="center")
        shown = status_col.segmented_control(
            "Filter",
            FILTERS,
            default="ALL",
            key="filter",
            label_visibility="collapsed",
            format_func=lambda f: f"{f.title()} {len(table) if f == 'ALL' else counts[f]}",
        )
        site = site_col.selectbox(
            "Site", [ALL_SITES, *sites], key="table_site", label_visibility="collapsed"
        )
        filtered = logic.filter_table(table, shown or "ALL", None if site == ALL_SITES else site)
        st.html(view.meters_html(filtered.head(TOP_N), selected, len(filtered)))

    with st.container(border=True):
        st.subheader("Actual vs baseline")
        site_pick, meter_pick = st.columns([1, 3])
        detail_site = site_pick.selectbox("Site", sites, key="site")
        in_site = [m for m in meters if site_of[m] == detail_site]
        if st.session_state["meter"] not in in_site:
            st.session_state["meter"] = in_site[0]  # most urgent meter of the new site
        meter = meter_pick.selectbox(
            "Building",
            in_site,
            key="meter",
            format_func=lambda m: _meter_label(m, status_of, several_types),
        )
        row = next(logic.meter_rows(table, meter).itertuples(index=False))
        points = logic.meter_rows(prepared, meter)
        st.html(view.panel_html(row))
        st.html(view.legend_html(len(points), row.threshold_pct))
        st.altair_chart(
            view.chart(points, stale=row.status == "STALE"), width="stretch", theme=None
        )

    with st.container(border=True):
        title, check = st.columns([3, 2], vertical_alignment="center")
        title.subheader("Anomaly events")
        only_selected = check.checkbox("Only the selected meter", key="only_selected")
        events = logic.event_log(prepared, meter if only_selected else None, EVENT_LOG_LIMIT)
        st.html(view.events_html(events, meter))


st.set_page_config(page_title="Building Energy Monitor", layout="wide")
st.html(view.GLOBAL_CSS)
live()
