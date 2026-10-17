"""Task 4 dashboard: reads the Task 3 Delta table and shows building status, chart and event log.

    streamlit run dashboard.py            # http://localhost:8501
    ENERGY_RESULTS_PATH=<path> streamlit run dashboard.py
"""
import os
import sys
import time
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (  # noqa: E402
    CHART_POINTS,
    DASHBOARD_REFRESH_SEC,
    EVENT_LOG_LIMIT,
    STALE_AFTER_SEC,
)
import logic  # noqa: E402
import view  # noqa: E402

RESULTS_PATH = os.environ.get(
    "ENERGY_RESULTS_PATH",
    str(Path(__file__).resolve().parents[1] / "streaming" / "output" / "energy_results"),
)
EMPTY_COUNTS = {"ANOMALY": 0, "NORMAL": 0, "STALE": 0}
FILTERS = ["ALL", "ANOMALY", "STALE", "NORMAL"]


@st.cache_resource
def _seen():
    """Shared by all sessions: {building_id: (newest timestamp, wall clock when first seen)}."""
    return {}


@st.cache_data(show_spinner=False, max_entries=2)
def _load(path, key):
    """Re-read only when the table or its version changed (key = logic.table_key)."""
    return logic.prepare(logic.read_results(path))


@st.fragment(run_every=DASHBOARD_REFRESH_SEC)
def live():
    key = logic.table_key(RESULTS_PATH)
    prepared = _load(RESULTS_PATH, key) if key is not None else None
    if prepared is None or prepared.empty:
        st.html(view.header_html("waiting", EMPTY_COUNTS, None, ""))
        st.html(view.waiting_html(RESULTS_PATH))
        return

    now = time.time()
    latest = logic.latest_per_building(prepared)
    seen = _seen()
    logic.update_seen(seen, latest, now)
    table = logic.building_table(latest, seen, now, STALE_AFTER_SEC)
    counts = logic.summary(table)
    kind = "allstale" if counts["STALE"] == len(table) else "live"
    age = logic.data_age(seen, now)

    st.html(view.header_html(kind, counts, prepared["timestamp"].max(), logic.format_age(age)))

    ids = list(table["building_id"])
    # First render: open the most urgent building (the table is sorted ANOMALY first).
    if st.session_state.get("building") not in ids:
        st.session_state["building"] = ids[0]
    selected = st.session_state["building"]
    status_of = dict(zip(table["building_id"], table["status"]))

    left, right = st.columns([1, 1], gap="medium")
    with left, st.container(border=True):
        st.subheader("Buildings")
        shown = st.segmented_control(
            "Filter", FILTERS, default="ALL", key="filter", label_visibility="collapsed",
            format_func=lambda f: f"{f.title()} {len(table) if f == 'ALL' else counts[f]}",
        )
        st.html(view.buildings_html(logic.filter_table(table, shown or "ALL"), selected))
    with right, st.container(border=True):
        st.subheader("Actual vs baseline")
        building = st.selectbox(
            "Building", ids, key="building",
            format_func=lambda b: f"{b}  ·  {status_of[b]}",
        )
        row = next(table[table["building_id"] == building].itertuples(index=False))
        points = logic.chart_data(prepared, building, CHART_POINTS)
        st.html(view.panel_html(row))
        st.html(view.legend_html(len(points)))
        st.altair_chart(
            view.chart(points, stale=row.status == "STALE"), width="stretch", theme=None,
        )

    with st.container(border=True):
        title, check = st.columns([3, 2], vertical_alignment="center")
        title.subheader("Anomaly events")
        only_selected = check.checkbox("Only the selected building", key="only_selected")
        events = logic.event_log(prepared, building if only_selected else None, EVENT_LOG_LIMIT)
        st.html(view.events_html(events, building))


st.set_page_config(page_title="Building Energy Monitor", layout="wide")
st.html(view.GLOBAL_CSS)
live()
